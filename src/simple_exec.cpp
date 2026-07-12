#include "simple_exec.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <strings.h>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "errors.h"
#include "execution/execution_sort.h"
#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "execution/runtime_filter_cache.h"
#include "record_printer.h"

namespace {

enum class TokenKind { Ident, Number, String, Symbol, End };

struct SimpleFallback {};

struct Token {
    TokenKind kind;
    std::string text;
};

bool same_word(const std::string &lhs, const char *rhs) {
    return strcasecmp(lhs.c_str(), rhs) == 0;
}

bool is_stop_word(const std::string &word) {
    return same_word(word, "where") || same_word(word, "order") || same_word(word, "limit") ||
           same_word(word, "group") || same_word(word, "having") || same_word(word, "join") ||
           same_word(word, "on") || same_word(word, "and") || same_word(word, "from");
}

class Lexer {
   public:
    explicit Lexer(const std::string &sql) : sql_(sql) {}

    std::vector<Token> scan() {
        std::vector<Token> tokens;
        while (pos_ < sql_.size() && sql_[pos_] != '\0') {
            unsigned char ch = static_cast<unsigned char>(sql_[pos_]);
            if (std::isspace(ch)) {
                pos_++;
                continue;
            }
            if (std::isalpha(ch) || ch == '_') {
                tokens.push_back(read_ident());
                continue;
            }
            if (std::isdigit(ch) || (ch == '.' && pos_ + 1 < sql_.size() &&
                                     std::isdigit(static_cast<unsigned char>(sql_[pos_ + 1])))) {
                tokens.push_back(read_number());
                continue;
            }
            if (ch == '\'') {
                tokens.push_back(read_string());
                continue;
            }
            tokens.push_back(read_symbol());
        }
        tokens.push_back({TokenKind::End, ""});
        return tokens;
    }

   private:
    Token read_ident() {
        size_t begin = pos_;
        pos_++;
        while (pos_ < sql_.size()) {
            unsigned char ch = static_cast<unsigned char>(sql_[pos_]);
            if (!std::isalnum(ch) && ch != '_') {
                break;
            }
            pos_++;
        }
        return {TokenKind::Ident, sql_.substr(begin, pos_ - begin)};
    }

    Token read_number() {
        size_t begin = pos_;
        bool seen_dot = false;
        bool seen_exp = false;
        while (pos_ < sql_.size()) {
            char ch = sql_[pos_];
            if (std::isdigit(static_cast<unsigned char>(ch))) {
                pos_++;
                continue;
            }
            if (ch == '.' && !seen_dot && !seen_exp) {
                seen_dot = true;
                pos_++;
                continue;
            }
            if ((ch == 'e' || ch == 'E') && !seen_exp) {
                seen_exp = true;
                pos_++;
                if (pos_ < sql_.size() && (sql_[pos_] == '+' || sql_[pos_] == '-')) {
                    pos_++;
                }
                continue;
            }
            break;
        }
        return {TokenKind::Number, sql_.substr(begin, pos_ - begin)};
    }

    Token read_string() {
        pos_++;
        std::string value;
        while (pos_ < sql_.size()) {
            char ch = sql_[pos_++];
            if (ch == '\'') {
                if (pos_ < sql_.size() && sql_[pos_] == '\'') {
                    value.push_back('\'');
                    pos_++;
                    continue;
                }
                return {TokenKind::String, value};
            }
            value.push_back(ch);
        }
        throw SimpleFallback();
    }

    Token read_symbol() {
        char ch = sql_[pos_++];
        if (pos_ < sql_.size()) {
            char next = sql_[pos_];
            if ((ch == '<' && (next == '=' || next == '>')) || (ch == '>' && next == '=') ||
                (ch == '!' && next == '=')) {
                pos_++;
                return {TokenKind::Symbol, std::string{ch, next}};
            }
        }
        return {TokenKind::Symbol, std::string(1, ch)};
    }

    const std::string &sql_;
    size_t pos_ = 0;
};

class Parser {
   public:
    explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

    const Token &peek(size_t off = 0) const {
        size_t idx = pos_ + off;
        if (idx >= tokens_.size()) {
            return tokens_.back();
        }
        return tokens_[idx];
    }

    bool eof() const { return peek().kind == TokenKind::End; }

    bool consume_word(const char *word) {
        if (peek().kind == TokenKind::Ident && same_word(peek().text, word)) {
            pos_++;
            return true;
        }
        return false;
    }

    bool consume_symbol(const char *sym) {
        if (peek().kind == TokenKind::Symbol && peek().text == sym) {
            pos_++;
            return true;
        }
        return false;
    }

    bool expect_word(const char *word) {
        if (!consume_word(word)) {
            return false;
        }
        return true;
    }

    bool expect_symbol(const char *sym) {
        if (!consume_symbol(sym)) {
            return false;
        }
        return true;
    }

    bool ident(std::string *out) {
        if (peek().kind != TokenKind::Ident) {
            return false;
        }
        *out = peek().text;
        pos_++;
        return true;
    }

    bool finish() {
        while (consume_symbol(";")) {
        }
        return eof();
    }

    void advance() {
        if (!eof()) {
            pos_++;
        }
    }

    size_t mark() const { return pos_; }
    void reset(size_t pos) { pos_ = pos; }

   private:
    std::vector<Token> tokens_;
    size_t pos_ = 0;
};

struct SelectItemSpec {
    bool star = false;
    bool aggregate = false;
    bool distinct = false;
    AggType agg = AGG_COUNT;
    TabCol col;
    std::string output_name;
};

struct TableSpec {
    std::string real_name;
    std::string visible_name;
};

Value cast_value(Value val, ColType target_type) {
    if (val.type == target_type) {
        return val;
    }
    if (target_type == TYPE_FLOAT && val.type == TYPE_INT) {
        Value out;
        out.set_float(static_cast<float>(val.int_val));
        return out;
    }
    if (target_type == TYPE_INT && val.type == TYPE_FLOAT) {
        Value out;
        out.set_int(static_cast<int>(val.float_val));
        return out;
    }
    throw IncompatibleTypeError(coltype2str(target_type), coltype2str(val.type));
}

void write_value_to_record(Value val, const ColMeta &col, char *dest) {
    val = cast_value(std::move(val), col.type);
    if (col.type == TYPE_INT) {
        memcpy(dest, &val.int_val, sizeof(int));
        return;
    }
    if (col.type == TYPE_FLOAT) {
        memcpy(dest, &val.float_val, sizeof(float));
        return;
    }
    if (col.type == TYPE_STRING) {
        if (val.str_val.size() > static_cast<size_t>(col.len)) {
            throw StringOverflowError();
        }
        memset(dest, 0, col.len);
        memcpy(dest, val.str_val.data(), val.str_val.size());
        return;
    }
    throw InternalError("Unexpected column type");
}

Value parse_value(Parser *parser) {
    bool negative = false;
    bool positive = false;
    if (parser->peek().kind == TokenKind::Symbol &&
        (parser->peek().text == "-" || parser->peek().text == "+") &&
        parser->peek(1).kind == TokenKind::Number) {
        negative = parser->peek().text == "-";
        positive = parser->peek().text == "+";
        (void)positive;
        parser->consume_symbol(parser->peek().text.c_str());
    }

    const Token &tok = parser->peek();
    Value val;
    if (tok.kind == TokenKind::String) {
        val.set_str(tok.text);
        parser->advance();
        return val;
    }
    if (tok.kind != TokenKind::Number) {
        throw SimpleFallback();
    }
    std::string text = tok.text;
    parser->advance();
    if (negative) {
        text.insert(text.begin(), '-');
    }
    if (text.find('.') != std::string::npos || text.find('e') != std::string::npos ||
        text.find('E') != std::string::npos) {
        val.set_float(std::stof(text));
    } else {
        val.set_int(std::stoi(text));
    }
    return val;
}

bool try_parse_value(Parser *parser, Value *value) {
    size_t pos = parser->mark();
    try {
        *value = parse_value(parser);
        return true;
    } catch (SimpleFallback &) {
        parser->reset(pos);
        return false;
    } catch (RMDBError &) {
        parser->reset(pos);
        return false;
    }
}

TabCol parse_col_ref(Parser *parser) {
    std::string first;
    if (!parser->ident(&first)) {
        throw SimpleFallback();
    }
    if (parser->consume_symbol(".")) {
        std::string second;
        if (!parser->ident(&second)) {
            throw SimpleFallback();
        }
        return {.tab_name = first, .col_name = second};
    }
    return {.tab_name = "", .col_name = first};
}

std::vector<ColMeta>::iterator find_col(TabMeta &tab, const std::string &name) {
    return std::find_if(tab.cols.begin(), tab.cols.end(), [&](const ColMeta &col) { return col.name == name; });
}

ColMeta resolve_col(TabMeta &tab, const std::string &real_tab, const std::string &visible_tab, TabCol *col) {
    if (!col->tab_name.empty() && col->tab_name != visible_tab && col->tab_name != real_tab) {
        throw ColumnNotFoundError(col->tab_name + "." + col->col_name);
    }
    auto it = find_col(tab, col->col_name);
    if (it == tab.cols.end()) {
        throw ColumnNotFoundError(col->col_name);
    }
    col->tab_name = visible_tab;
    return *it;
}

ColMeta resolve_col(SmManager *sm_manager, const std::vector<TableSpec> &tables, TabCol *col) {
    if (!col->tab_name.empty()) {
        for (const auto &table : tables) {
            if (col->tab_name != table.visible_name && col->tab_name != table.real_name) {
                continue;
            }
            TabMeta &tab = sm_manager->db_.get_table(table.real_name);
            auto it = find_col(tab, col->col_name);
            if (it == tab.cols.end()) {
                throw ColumnNotFoundError(col->tab_name + "." + col->col_name);
            }
            ColMeta meta = *it;
            meta.tab_name = table.visible_name;
            col->tab_name = table.visible_name;
            return meta;
        }
        throw ColumnNotFoundError(col->tab_name + "." + col->col_name);
    }

    bool found = false;
    ColMeta result;
    std::string visible;
    for (const auto &table : tables) {
        TabMeta &tab = sm_manager->db_.get_table(table.real_name);
        auto it = find_col(tab, col->col_name);
        if (it == tab.cols.end()) {
            continue;
        }
        if (found) {
            throw AmbiguousColumnError(col->col_name);
        }
        found = true;
        result = *it;
        visible = table.visible_name;
    }
    if (!found) {
        throw ColumnNotFoundError(col->col_name);
    }
    result.tab_name = visible;
    col->tab_name = visible;
    return result;
}

ColMeta resolve_col(const std::vector<ColMeta> &cols, TabCol *col) {
    if (!col->tab_name.empty()) {
        auto it = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &meta) {
            return meta.tab_name == col->tab_name && meta.name == col->col_name;
        });
        if (it == cols.end()) {
            throw ColumnNotFoundError(col->tab_name + "." + col->col_name);
        }
        return *it;
    }

    auto found = cols.end();
    for (auto it = cols.begin(); it != cols.end(); ++it) {
        if (it->name != col->col_name) {
            continue;
        }
        if (found != cols.end()) {
            throw AmbiguousColumnError(col->col_name);
        }
        found = it;
    }
    if (found == cols.end()) {
        throw ColumnNotFoundError(col->col_name);
    }
    col->tab_name = found->tab_name;
    return *found;
}

CompOp parse_comp(Parser *parser) {
    const Token &tok = parser->peek();
    if (tok.kind != TokenKind::Symbol) {
        throw SimpleFallback();
    }
    if (tok.text == "=") {
        parser->consume_symbol("=");
        return OP_EQ;
    }
    if (tok.text == "<") {
        parser->consume_symbol("<");
        return OP_LT;
    }
    if (tok.text == ">") {
        parser->consume_symbol(">");
        return OP_GT;
    }
    if (tok.text == "<=") {
        parser->consume_symbol("<=");
        return OP_LE;
    }
    if (tok.text == ">=") {
        parser->consume_symbol(">=");
        return OP_GE;
    }
    if (tok.text == "<>" || tok.text == "!=") {
        parser->consume_symbol(tok.text.c_str());
        return OP_NE;
    }
    throw SimpleFallback();
}

Condition parse_condition(Parser *parser, TabMeta &tab, const std::string &real_tab, const std::string &visible_tab) {
    Condition cond;
    cond.lhs_col = parse_col_ref(parser);
    ColMeta lhs_meta = resolve_col(tab, real_tab, visible_tab, &cond.lhs_col);
    cond.op = parse_comp(parser);

    Value rhs_value;
    size_t rhs_pos = parser->mark();
    if (try_parse_value(parser, &rhs_value)) {
        cond.is_rhs_val = true;
        cond.rhs_val = cast_value(std::move(rhs_value), lhs_meta.type);
        cond.rhs_val.init_raw(lhs_meta.len);
        return cond;
    }

    parser->reset(rhs_pos);
    cond.is_rhs_val = false;
    cond.rhs_col = parse_col_ref(parser);
    ColMeta rhs_meta = resolve_col(tab, real_tab, visible_tab, &cond.rhs_col);
    if (lhs_meta.type != rhs_meta.type) {
        throw IncompatibleTypeError(coltype2str(lhs_meta.type), coltype2str(rhs_meta.type));
    }
    return cond;
}

Condition parse_condition(Parser *parser, SmManager *sm_manager, const std::vector<TableSpec> &tables) {
    Condition cond;
    cond.lhs_col = parse_col_ref(parser);
    ColMeta lhs_meta = resolve_col(sm_manager, tables, &cond.lhs_col);
    cond.op = parse_comp(parser);

    Value rhs_value;
    size_t rhs_pos = parser->mark();
    if (try_parse_value(parser, &rhs_value)) {
        cond.is_rhs_val = true;
        cond.rhs_val = cast_value(std::move(rhs_value), lhs_meta.type);
        cond.rhs_val.init_raw(lhs_meta.len);
        return cond;
    }

    parser->reset(rhs_pos);
    cond.is_rhs_val = false;
    cond.rhs_col = parse_col_ref(parser);
    ColMeta rhs_meta = resolve_col(sm_manager, tables, &cond.rhs_col);
    if (lhs_meta.type != rhs_meta.type) {
        throw IncompatibleTypeError(coltype2str(lhs_meta.type), coltype2str(rhs_meta.type));
    }
    return cond;
}

std::vector<Condition> parse_where(Parser *parser, TabMeta &tab, const std::string &real_tab,
                                   const std::string &visible_tab) {
    std::vector<Condition> conds;
    if (!parser->consume_word("where")) {
        return conds;
    }
    for (;;) {
        conds.push_back(parse_condition(parser, tab, real_tab, visible_tab));
        if (!parser->consume_word("and")) {
            break;
        }
    }
    return conds;
}

std::vector<Condition> parse_cond_list(Parser *parser, SmManager *sm_manager,
                                       const std::vector<TableSpec> &tables) {
    std::vector<Condition> conds;
    for (;;) {
        conds.push_back(parse_condition(parser, sm_manager, tables));
        if (!parser->consume_word("and")) {
            break;
        }
    }
    return conds;
}

std::vector<Condition> parse_where(Parser *parser, SmManager *sm_manager,
                                   const std::vector<TableSpec> &tables) {
    if (!parser->consume_word("where")) {
        return {};
    }
    return parse_cond_list(parser, sm_manager, tables);
}

bool is_single_table_cond(const Condition &cond) {
    return cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name;
}

std::set<std::string> condition_tables(const Condition &cond) {
    std::set<std::string> tabs;
    tabs.insert(cond.lhs_col.tab_name);
    if (!cond.is_rhs_val) {
        tabs.insert(cond.rhs_col.tab_name);
    }
    return tabs;
}

bool cond_tables_subset(const Condition &cond, const std::set<std::string> &tables) {
    auto refs = condition_tables(cond);
    return std::includes(tables.begin(), tables.end(), refs.begin(), refs.end());
}

bool cond_can_join_current(const Condition &cond, const std::set<std::string> &joined,
                           const std::string &candidate) {
    if (cond.is_rhs_val) {
        return false;
    }
    bool lhs_joined = joined.find(cond.lhs_col.tab_name) != joined.end();
    bool rhs_joined = joined.find(cond.rhs_col.tab_name) != joined.end();
    return (lhs_joined && cond.rhs_col.tab_name == candidate) ||
           (rhs_joined && cond.lhs_col.tab_name == candidate);
}

void add_needed_col(std::map<std::string, std::vector<TabCol>> *needed, const TabCol &col) {
    if (col.tab_name.empty()) {
        return;
    }
    auto &cols = (*needed)[col.tab_name];
    auto it = std::find_if(cols.begin(), cols.end(), [&](const TabCol &item) {
        return item.tab_name == col.tab_name && item.col_name == col.col_name;
    });
    if (it == cols.end()) {
        cols.push_back(col);
    }
}

bool can_push_join_projection(const std::vector<SelectItemSpec> &items) {
    for (const auto &item : items) {
        if (item.star && !item.aggregate) {
            return false;
        }
    }
    return true;
}

std::map<std::string, std::vector<TabCol>>
collect_join_projection_cols(const std::vector<SelectItemSpec> &items,
                             const std::vector<Condition> &conds,
                             const std::vector<OrderByCol> &order_cols) {
    std::map<std::string, std::vector<TabCol>> needed;
    for (const auto &item : items) {
        if (!item.star) {
            add_needed_col(&needed, item.col);
        }
    }
    for (const auto &order_col : order_cols) {
        add_needed_col(&needed, order_col.col);
    }
    for (const auto &cond : conds) {
        if (!cond.is_rhs_val) {
            add_needed_col(&needed, cond.lhs_col);
            add_needed_col(&needed, cond.rhs_col);
        }
    }
    return needed;
}

std::unique_ptr<AbstractExecutor>
project_scan_if_needed(std::unique_ptr<AbstractExecutor> scan, const std::string &visible_tab,
                       const std::map<std::string, std::vector<TabCol>> *needed_cols) {
    if (needed_cols == nullptr) {
        return scan;
    }
    auto it = needed_cols->find(visible_tab);
    if (it == needed_cols->end() || it->second.empty()) {
        return scan;
    }
    return std::make_unique<ProjectionExecutor>(std::move(scan), it->second);
}

std::vector<std::string> choose_index_cols(TabMeta &tab, const std::string &visible_tab,
                                           const std::vector<Condition> &conds,
                                           size_t *best_prefix_out = nullptr) {
    std::vector<std::string> result;
    size_t best_prefix = 0;
    const IndexMeta *best = nullptr;
    for (const auto &index : tab.indexes) {
        size_t prefix = 0;
        bool range_seen = false;
        for (const auto &idx_col : index.cols) {
            bool has_eq = false;
            bool has_range = false;
            for (const auto &cond : conds) {
                if (!cond.is_rhs_val || cond.lhs_col.tab_name != visible_tab ||
                    cond.lhs_col.col_name != idx_col.name || cond.op == OP_NE) {
                    continue;
                }
                if (cond.op == OP_EQ) {
                    has_eq = true;
                } else {
                    has_range = true;
                }
            }
            if ((!has_eq && !has_range) || range_seen) {
                break;
            }
            prefix++;
            if (!has_eq && has_range) {
                range_seen = true;
            }
        }
        if (prefix > best_prefix) {
            best_prefix = prefix;
            best = &index;
        }
    }
    if (best_prefix_out != nullptr) {
        *best_prefix_out = best_prefix;
    }
    if (best == nullptr || best_prefix == 0) {
        return result;
    }
    for (const auto &col : best->cols) {
        result.push_back(col.name);
    }
    return result;
}

const Condition *find_eq_value_cond(const std::vector<Condition> &conds, const std::string &visible_tab,
                                    const std::string &col_name) {
    for (const auto &cond : conds) {
        if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name == visible_tab &&
            cond.lhs_col.col_name == col_name) {
            return &cond;
        }
    }
    return nullptr;
}

bool try_collect_exact_index_rids(SmManager *sm_manager, const std::string &tab_name,
                                  const std::string &visible_tab, const std::vector<Condition> &conds,
                                  Context *context, std::vector<Rid> *rids) {
    if (rids == nullptr || conds.empty()) {
        return false;
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);
    const IndexMeta *best = nullptr;
    size_t best_cols = 0;
    for (const auto &index : tab.indexes) {
        bool full_eq = true;
        for (const auto &col : index.cols) {
            if (find_eq_value_cond(conds, visible_tab, col.name) == nullptr) {
                full_eq = false;
                break;
            }
        }
        if (full_eq && index.cols.size() > best_cols) {
            best = &index;
            best_cols = index.cols.size();
        }
    }
    if (best == nullptr) {
        return false;
    }

    std::vector<char> key(best->col_tot_len);
    int offset = 0;
    for (const auto &col : best->cols) {
        const Condition *cond = find_eq_value_cond(conds, visible_tab, col.name);
        if (cond == nullptr || cond->rhs_val.raw == nullptr) {
            return false;
        }
        memcpy(key.data() + offset, cond->rhs_val.raw->data, col.len);
        offset += col.len;
    }

    std::vector<Rid> candidates;
    auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, best->cols)).get();
    ih->get_value(key.data(), &candidates, context == nullptr ? nullptr : context->txn_);

    if (context != nullptr && context->txn_mgr_ != nullptr &&
        context->txn_mgr_->IsMvccTxn(context->txn_)) {
        std::vector<std::string> index_col_names;
        index_col_names.reserve(best->cols.size());
        for (const auto &col : best->cols) {
            index_col_names.push_back(col.name);
        }
        context->txn_mgr_->CollectVisibleVersionRids(tab_name, index_col_names, key, false, key, false,
                                                     conds, tab.cols, context->txn_, &candidates);
    }
    if (candidates.empty()) {
        rids->clear();
        return true;
    }

    bool need_dedup = candidates.size() > 1;
    std::vector<Rid> seen;
    if (need_dedup) {
        seen.reserve(candidates.size());
    }
    rids->clear();
    rids->reserve(candidates.size());
    for (const auto &rid : candidates) {
        if (need_dedup) {
            bool duplicate = false;
            for (const auto &existing : seen) {
                if (existing == rid) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) {
                continue;
            }
            seen.push_back(rid);
        }
        rids->push_back(rid);
    }
    return true;
}

std::vector<std::string> choose_order_limit_index_cols(TabMeta &tab, const std::string &visible_tab,
                                                       const std::vector<Condition> &conds,
                                                       const TabCol &order_col) {
    if (order_col.tab_name != visible_tab) {
        return {};
    }

    const IndexMeta *best = nullptr;
    size_t best_eq_prefix = 0;
    for (const auto &index : tab.indexes) {
        size_t eq_prefix = 0;
        while (eq_prefix < index.cols.size() &&
               find_eq_value_cond(conds, visible_tab, index.cols[eq_prefix].name) != nullptr) {
            eq_prefix++;
        }
        if (eq_prefix >= index.cols.size() || index.cols[eq_prefix].name != order_col.col_name) {
            continue;
        }
        if (best == nullptr || eq_prefix > best_eq_prefix) {
            best = &index;
            best_eq_prefix = eq_prefix;
        }
    }

    std::vector<std::string> cols;
    if (best == nullptr) {
        return cols;
    }
    cols.reserve(best->cols.size());
    for (const auto &col : best->cols) {
        cols.push_back(col.name);
    }
    return cols;
}

bool is_eq_join_with_table(const Condition &cond, const std::set<std::string> &joined,
                           const std::string &candidate, std::string *candidate_col = nullptr) {
    if (cond.is_rhs_val || cond.op != OP_EQ) {
        return false;
    }
    if (cond.lhs_col.tab_name == candidate && joined.find(cond.rhs_col.tab_name) != joined.end()) {
        if (candidate_col != nullptr) {
            *candidate_col = cond.lhs_col.col_name;
        }
        return true;
    }
    if (cond.rhs_col.tab_name == candidate && joined.find(cond.lhs_col.tab_name) != joined.end()) {
        if (candidate_col != nullptr) {
            *candidate_col = cond.rhs_col.col_name;
        }
        return true;
    }
    return false;
}

std::vector<std::string> choose_join_index_cols(TabMeta &tab, const std::vector<Condition> &filters,
                                                const std::vector<Condition> &join_conds,
                                                const std::set<std::string> &joined,
                                                const std::string &candidate);

size_t local_filter_score(TabMeta &tab, const std::string &visible_tab,
                          const std::vector<Condition> &filters) {
    size_t prefix = 0;
    (void)choose_index_cols(tab, visible_tab, filters, &prefix);
    std::vector<ColMeta> cache_cols;
    if (RuntimeFilterCache::choose_columns(tab, visible_tab, filters, &cache_cols)) {
        prefix = std::max(prefix, cache_cols.size());
    }
    size_t eq_count = 0;
    for (const auto &cond : filters) {
        if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name == visible_tab) {
            eq_count++;
        }
    }
    return prefix * 8 + eq_count;
}

size_t join_filter_score(TabMeta &tab, const std::vector<Condition> &filters,
                         const std::vector<Condition> &join_conds,
                         const std::set<std::string> &joined,
                         const std::string &candidate) {
    auto index_cols = choose_join_index_cols(tab, filters, join_conds, joined, candidate);
    size_t join_eqs = 0;
    for (const auto &cond : join_conds) {
        if (is_eq_join_with_table(cond, joined, candidate)) {
            join_eqs++;
        }
    }
    return index_cols.size() * 16 + join_eqs * 4 + local_filter_score(tab, candidate, filters);
}

std::vector<std::string> choose_join_index_cols(TabMeta &tab, const std::vector<Condition> &filters,
                                                const std::vector<Condition> &join_conds,
                                                const std::set<std::string> &joined,
                                                const std::string &candidate) {
    std::set<std::string> eq_cols;
    for (const auto &cond : join_conds) {
        std::string col;
        if (is_eq_join_with_table(cond, joined, candidate, &col)) {
            eq_cols.insert(col);
        }
    }

    const IndexMeta *best = nullptr;
    size_t best_prefix = 0;
    for (const auto &index : tab.indexes) {
        size_t prefix = 0;
        bool range_seen = false;
        for (const auto &col : index.cols) {
            bool has_eq = eq_cols.find(col.name) != eq_cols.end();
            bool has_range = false;
            for (const auto &cond : filters) {
                if (!cond.is_rhs_val || cond.lhs_col.tab_name != candidate ||
                    cond.lhs_col.col_name != col.name || cond.op == OP_NE) {
                    continue;
                }
                if (cond.op == OP_EQ) {
                    has_eq = true;
                } else {
                    has_range = true;
                }
            }
            if ((!has_eq && !has_range) || range_seen) {
                break;
            }
            prefix++;
            if (!has_eq && has_range) {
                range_seen = true;
            }
        }
        if (prefix > best_prefix) {
            best_prefix = prefix;
            best = &index;
        }
    }
    if (best == nullptr || best_prefix == 0) {
        return {};
    }
    std::vector<std::string> cols;
    cols.reserve(best->cols.size());
    for (const auto &col : best->cols) {
        cols.push_back(col.name);
    }
    return cols;
}

std::unique_ptr<AbstractExecutor> make_scan(SmManager *sm_manager, const std::string &tab_name,
                                            const std::string &alias, std::vector<Condition> conds,
                                            Context *context,
                                            const std::vector<std::string> &forced_index_cols = {},
                                            int scan_limit = -1, bool ordered_scan = false,
                                            bool scan_desc = false) {
    TabMeta &tab = sm_manager->db_.get_table(tab_name);
    std::string visible = alias.empty() ? tab_name : alias;
    size_t index_prefix = 0;
    std::vector<std::string> index_cols;
    if (!forced_index_cols.empty()) {
        index_cols = forced_index_cols;
        index_prefix = forced_index_cols.size();
    } else {
        index_cols = choose_index_cols(tab, visible, conds, &index_prefix);
    }
    std::vector<ColMeta> cache_cols;
    if (!ordered_scan && RuntimeFilterCache::choose_columns(tab, visible, conds, &cache_cols) &&
        cache_cols.size() > index_prefix) {
        return std::make_unique<SeqScanExecutor>(sm_manager, tab_name, visible, std::move(conds), context);
    }
    if (!index_cols.empty()) {
        return std::make_unique<IndexScanExecutor>(sm_manager, tab_name, visible, std::move(conds), index_cols,
                                                   context, scan_limit, ordered_scan, scan_desc);
    }
    return std::make_unique<SeqScanExecutor>(sm_manager, tab_name, visible, std::move(conds), context);
}

std::unique_ptr<AbstractExecutor> make_scan_with_index(SmManager *sm_manager, const TableSpec &table,
                                                       std::vector<Condition> conds,
                                                       std::vector<std::string> index_cols,
                                                       Context *context) {
    if (!index_cols.empty()) {
        return std::make_unique<IndexScanExecutor>(sm_manager, table.real_name, table.visible_name,
                                                   std::move(conds), index_cols, context);
    }
    return make_scan(sm_manager, table.real_name, table.visible_name, std::move(conds), context);
}

std::unique_ptr<AbstractExecutor> make_join_tree(SmManager *sm_manager, const std::vector<TableSpec> &tables,
                                                 const std::vector<Condition> &conds, Context *context,
                                                 const std::map<std::string, std::vector<TabCol>> *needed_cols = nullptr) {
    std::map<std::string, std::vector<Condition>> filters;
    std::vector<Condition> join_conds;
    for (const auto &cond : conds) {
        if (is_single_table_cond(cond)) {
            filters[cond.lhs_col.tab_name].push_back(cond);
        } else {
            join_conds.push_back(cond);
        }
    }
    if (tables.empty()) {
        throw RMDBError("No table in select");
    }

    std::vector<bool> used(tables.size(), false);
    size_t first = 0;
    size_t best_first_score = 0;
    for (size_t i = 0; i < tables.size(); ++i) {
        TabMeta &tab = sm_manager->db_.get_table(tables[i].real_name);
        size_t score = local_filter_score(tab, tables[i].visible_name, filters[tables[i].visible_name]);
        if (i == 0 || score > best_first_score) {
            first = i;
            best_first_score = score;
        }
    }

    auto root = make_scan(sm_manager, tables[first].real_name, tables[first].visible_name,
                          filters[tables[first].visible_name], context);
    root = project_scan_if_needed(std::move(root), tables[first].visible_name, needed_cols);
    std::set<std::string> joined;
    joined.insert(tables[first].visible_name);
    used[first] = true;

    for (size_t joined_count = 1; joined_count < tables.size(); ++joined_count) {
        size_t chosen = tables.size();
        size_t best_score = 0;
        for (size_t i = 0; i < tables.size(); ++i) {
            if (used[i]) {
                continue;
            }
            TabMeta &tab = sm_manager->db_.get_table(tables[i].real_name);
            size_t score = join_filter_score(tab, filters[tables[i].visible_name], join_conds, joined,
                                             tables[i].visible_name);
            if (chosen == tables.size() || score > best_score) {
                chosen = i;
                best_score = score;
            }
        }
        if (chosen == tables.size()) {
            throw SimpleFallback();
        }

        const auto &table = tables[chosen];
        std::set<std::string> future = joined;
        future.insert(table.visible_name);
        std::vector<Condition> current_join_conds;
        for (auto it = join_conds.begin(); it != join_conds.end();) {
            if (cond_can_join_current(*it, joined, table.visible_name) || cond_tables_subset(*it, future)) {
                current_join_conds.push_back(*it);
                it = join_conds.erase(it);
            } else {
                ++it;
            }
        }

        TabMeta &tab = sm_manager->db_.get_table(table.real_name);
        auto index_cols = choose_join_index_cols(tab, filters[table.visible_name], current_join_conds, joined,
                                                 table.visible_name);
        auto right = make_scan_with_index(sm_manager, table, filters[table.visible_name], std::move(index_cols),
                                          context);
        right = project_scan_if_needed(std::move(right), table.visible_name, needed_cols);
        root = std::make_unique<NestedLoopJoinExecutor>(std::move(root), std::move(right),
                                                        std::move(current_join_conds));
        joined.insert(table.visible_name);
        used[chosen] = true;
    }

    if (!join_conds.empty()) {
        throw SimpleFallback();
    }
    return root;
}

std::string raw_value_to_string(const char *data, const ColMeta &col) {
    if (col.type == TYPE_INT) {
        return std::to_string(*reinterpret_cast<const int *>(data));
    }
    if (col.type == TYPE_FLOAT) {
        return std::to_string(*reinterpret_cast<const float *>(data));
    }
    std::string out(data, col.len);
    out.resize(strlen(out.c_str()));
    return out;
}

void print_rows(const std::vector<ColMeta> &cols, const std::vector<std::vector<std::string>> &rows,
                Context *context) {
    RecordPrinter printer(cols.size());
    std::vector<std::string> captions;
    captions.reserve(cols.size());
    for (const auto &col : cols) {
        captions.push_back(col.name);
    }

    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);

    std::fstream outfile;
    bool write_file = output_file_enabled.load();
    if (write_file) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto &caption : captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }

    for (const auto &row : rows) {
        printer.print_record(row, context);
        if (write_file) {
            outfile << "|";
            for (const auto &field : row) {
                outfile << " " << field << " |";
            }
            outfile << "\n";
        }
    }
    if (write_file) {
        outfile.close();
    }
    printer.print_separator(context);
    RecordPrinter::print_record_count(rows.size(), context);
}

std::string agg_name(AggType type) {
    switch (type) {
        case AGG_COUNT:
            return "COUNT";
        case AGG_MAX:
            return "MAX";
        case AGG_MIN:
            return "MIN";
        case AGG_SUM:
            return "SUM";
        case AGG_AVG:
            return "AVG";
        default:
            throw InternalError("Unexpected aggregate type");
    }
}

bool parse_agg_type(const std::string &name, AggType *type) {
    if (same_word(name, "count")) {
        *type = AGG_COUNT;
        return true;
    }
    if (same_word(name, "max")) {
        *type = AGG_MAX;
        return true;
    }
    if (same_word(name, "min")) {
        *type = AGG_MIN;
        return true;
    }
    if (same_word(name, "sum")) {
        *type = AGG_SUM;
        return true;
    }
    if (same_word(name, "avg")) {
        *type = AGG_AVG;
        return true;
    }
    return false;
}

bool parse_select_item(Parser *parser, SelectItemSpec *item) {
    if (parser->consume_symbol("*")) {
        item->star = true;
        return true;
    }
    if (parser->peek().kind == TokenKind::Ident && parser->peek(1).kind == TokenKind::Symbol &&
        parser->peek(1).text == "(") {
        AggType type;
        if (!parse_agg_type(parser->peek().text, &type)) {
            return false;
        }
        std::string func_name;
        parser->ident(&func_name);
        parser->expect_symbol("(");
        item->aggregate = true;
        item->agg = type;
        if (parser->consume_word("distinct")) {
            item->distinct = true;
        }
        if (parser->consume_symbol("*")) {
            if (item->distinct) {
                return false;
            }
            item->star = true;
        } else {
            item->col = parse_col_ref(parser);
        }
        if (!parser->expect_symbol(")")) {
            return false;
        }
        return true;
    }
    item->col = parse_col_ref(parser);
    return true;
}

std::string distinct_key(const char *data, const ColMeta &col) {
    return std::string(data + col.offset, data + col.offset + col.len);
}

void run_aggregate(std::unique_ptr<AbstractExecutor> root, std::vector<SelectItemSpec> items, Context *context) {
    struct State {
        bool seen = false;
        double sum = 0;
        int count = 0;
        std::vector<char> value;
        ColMeta col;
        std::unordered_set<std::string> seen_values;
    };

    std::vector<State> states(items.size());
    std::vector<ColMeta> out_cols;
    out_cols.reserve(items.size());
    const auto &root_cols = root->cols();
    for (size_t i = 0; i < items.size(); ++i) {
        auto &item = items[i];
        ColMeta out;
        out.tab_name = "";
        if (item.agg == AGG_COUNT && item.star) {
            out.name = item.output_name.empty() ? "COUNT(*)" : item.output_name;
            out.type = TYPE_INT;
            out.len = sizeof(int);
        } else {
            ColMeta src = resolve_col(root_cols, &item.col);
            if ((item.agg == AGG_SUM || item.agg == AGG_AVG) && src.type == TYPE_STRING) {
                throw RMDBError("Aggregate function only supports numeric column");
            }
            states[i].col = src;
            out.name = item.output_name.empty()
                           ? agg_name(item.agg) + "(" + item.col.tab_name + "." + item.col.col_name + ")"
                           : item.output_name;
            if (item.agg == AGG_COUNT) {
                out.type = TYPE_INT;
                out.len = sizeof(int);
            } else if (item.agg == AGG_AVG) {
                out.type = TYPE_FLOAT;
                out.len = sizeof(float);
            } else {
                out.type = src.type;
                out.len = src.len;
            }
        }
        out.offset = i == 0 ? 0 : out_cols.back().offset + out_cols.back().len;
        out_cols.push_back(out);
    }

    bool count_star_only = std::all_of(items.begin(), items.end(), [](const SelectItemSpec &item) {
        return item.aggregate && item.agg == AGG_COUNT && item.star && !item.distinct;
    });
    if (count_star_only) {
        int count = 0;
        context->is_select_ = true;
        root->beginTuple();
        for (; !root->is_end(); root->nextTuple()) {
            count++;
        }
        context->is_select_ = false;
        std::vector<std::string> row(items.size(), std::to_string(count));
        print_rows(out_cols, {row}, context);
        return;
    }

    context->is_select_ = true;
    root->beginTuple();
    for (; !root->is_end(); root->nextTuple()) {
        auto rec = root->Next();
        for (size_t i = 0; i < items.size(); ++i) {
            auto &item = items[i];
            auto &state = states[i];
            const ColMeta &col = state.col;
            if (item.distinct) {
                auto key = distinct_key(rec->data, col);
                if (!state.seen_values.insert(std::move(key)).second) {
                    continue;
                }
            }
            if (item.agg == AGG_COUNT) {
                state.count++;
                continue;
            }
            const char *data = rec->data + col.offset;
            state.count++;
            if (item.agg == AGG_SUM || item.agg == AGG_AVG) {
                state.sum += col.type == TYPE_INT ? *reinterpret_cast<const int *>(data)
                                                   : *reinterpret_cast<const float *>(data);
                continue;
            }
            if (!state.seen) {
                state.value.assign(data, data + col.len);
                state.seen = true;
                continue;
            }
            int cmp = AbstractExecutor::compare_raw(data, state.value.data(), col.type, col.len);
            if ((item.agg == AGG_MIN && cmp < 0) || (item.agg == AGG_MAX && cmp > 0)) {
                state.value.assign(data, data + col.len);
            }
        }
    }
    context->is_select_ = false;

    std::vector<std::string> row;
    row.reserve(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        auto &item = items[i];
        auto &state = states[i];
        if (item.agg == AGG_COUNT) {
            row.push_back(std::to_string(state.count));
        } else if (item.agg == AGG_SUM) {
            if (state.col.type == TYPE_INT) {
                row.push_back(std::to_string(static_cast<int>(state.sum)));
            } else {
                row.push_back(std::to_string(static_cast<float>(state.sum)));
            }
        } else if (item.agg == AGG_AVG) {
            float avg = state.count == 0 ? 0.0f : static_cast<float>(state.sum / state.count);
            row.push_back(std::to_string(avg));
        } else if (!state.seen) {
            row.emplace_back("");
        } else {
            row.push_back(raw_value_to_string(state.value.data(), state.col));
        }
    }
    print_rows(out_cols, {row}, context);
}

bool exec_insert(Parser *parser, SmManager *sm_manager, Context *context) {
    if (!parser->expect_word("insert") || !parser->expect_word("into")) {
        return false;
    }
    std::string tab_name;
    if (!parser->ident(&tab_name) || !parser->expect_word("values") || !parser->expect_symbol("(")) {
        return false;
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);
    auto record = std::make_unique<RmRecord>(sm_manager->fhs_.at(tab_name)->get_file_hdr().record_size);
    size_t col_idx = 0;
    for (;;) {
        if (col_idx >= tab.cols.size()) {
            throw InvalidValueCountError();
        }
        write_value_to_record(parse_value(parser), tab.cols[col_idx],
                              record->data + tab.cols[col_idx].offset);
        col_idx++;
        if (parser->consume_symbol(",")) {
            continue;
        }
        break;
    }
    if (!parser->expect_symbol(")") || !parser->finish()) {
        return false;
    }
    if (col_idx != tab.cols.size()) {
        throw InvalidValueCountError();
    }
    InsertExecutor executor(sm_manager, tab_name, std::move(record), context);
    executor.Next();
    return true;
}

SetOp token_to_set_op(const std::string &op) {
    if (op == "+") {
        return SET_OP_ADD;
    }
    if (op == "-") {
        return SET_OP_SUB;
    }
    if (op == "*") {
        return SET_OP_MUL;
    }
    if (op == "/") {
        return SET_OP_DIV;
    }
    return SET_OP_VALUE;
}

bool exec_update(Parser *parser, SmManager *sm_manager, Context *context) {
    if (!parser->expect_word("update")) {
        return false;
    }
    std::string tab_name;
    if (!parser->ident(&tab_name) || !parser->expect_word("set")) {
        return false;
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);
    std::vector<SetClause> sets;
    for (;;) {
        std::string lhs_name;
        if (!parser->ident(&lhs_name) || !parser->expect_symbol("=")) {
            return false;
        }
        auto lhs_col = find_col(tab, lhs_name);
        if (lhs_col == tab.cols.end()) {
            throw ColumnNotFoundError(lhs_name);
        }

        SetClause set;
        set.lhs = {.tab_name = tab_name, .col_name = lhs_name};
        Value value;
        size_t value_pos = parser->mark();
        if (try_parse_value(parser, &value)) {
            set.op = SET_OP_VALUE;
            set.rhs = cast_value(std::move(value), lhs_col->type);
            set.rhs.init_raw(lhs_col->len);
        } else {
            parser->reset(value_pos);
            std::string rhs_col_name;
            if (!parser->ident(&rhs_col_name)) {
                return false;
            }
            auto rhs_col = find_col(tab, rhs_col_name);
            if (rhs_col == tab.cols.end()) {
                throw ColumnNotFoundError(rhs_col_name);
            }
            if (parser->peek().kind != TokenKind::Symbol) {
                return false;
            }
            std::string op = parser->peek().text;
            if (op != "+" && op != "-" && op != "*" && op != "/") {
                return false;
            }
            parser->consume_symbol(op.c_str());
            value = parse_value(parser);
            if ((lhs_col->type != TYPE_INT && lhs_col->type != TYPE_FLOAT) ||
                (rhs_col->type != TYPE_INT && rhs_col->type != TYPE_FLOAT) ||
                (value.type != TYPE_INT && value.type != TYPE_FLOAT)) {
                throw IncompatibleTypeError(coltype2str(rhs_col->type), coltype2str(value.type));
            }
            set.op = token_to_set_op(op);
            set.rhs_col = {.tab_name = tab_name, .col_name = rhs_col_name};
            set.rhs = cast_value(std::move(value), rhs_col->type);
            set.rhs.init_raw(rhs_col->len);
        }
        sets.push_back(std::move(set));
        if (!parser->consume_symbol(",")) {
            break;
        }
    }

    auto conds = parse_where(parser, tab, tab_name, tab_name);
    if (!parser->finish()) {
        return false;
    }
    std::vector<Rid> rids;
    if (!try_collect_exact_index_rids(sm_manager, tab_name, tab_name, conds, context, &rids)) {
        auto scan = make_scan(sm_manager, tab_name, tab_name, conds, context);
        for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
            rids.push_back(scan->rid());
        }
    }
    UpdateExecutor executor(sm_manager, tab_name, std::move(sets), conds, std::move(rids), context);
    executor.Next();
    return true;
}

bool exec_delete(Parser *parser, SmManager *sm_manager, Context *context) {
    if (!parser->expect_word("delete") || !parser->expect_word("from")) {
        return false;
    }
    std::string tab_name;
    if (!parser->ident(&tab_name)) {
        return false;
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);
    auto conds = parse_where(parser, tab, tab_name, tab_name);
    if (!parser->finish()) {
        return false;
    }
    std::vector<Rid> rids;
    if (!try_collect_exact_index_rids(sm_manager, tab_name, tab_name, conds, context, &rids)) {
        auto scan = make_scan(sm_manager, tab_name, tab_name, conds, context);
        for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
            rids.push_back(scan->rid());
        }
    }
    DeleteExecutor executor(sm_manager, tab_name, conds, std::move(rids), context);
    executor.Next();
    return true;
}

bool exec_select(Parser *parser, SmManager *sm_manager, QlManager *ql_manager, Context *context) {
    if (!parser->expect_word("select")) {
        return false;
    }

    std::vector<SelectItemSpec> items;
    for (;;) {
        SelectItemSpec item;
        if (!parse_select_item(parser, &item)) {
            return false;
        }
        if (parser->consume_word("as")) {
            if (!parser->ident(&item.output_name)) {
                return false;
            }
        } else if (parser->peek().kind == TokenKind::Ident && !is_stop_word(parser->peek().text)) {
            parser->ident(&item.output_name);
        }
        items.push_back(std::move(item));
        if (!parser->consume_symbol(",")) {
            break;
        }
    }
    if (!parser->expect_word("from")) {
        return false;
    }

    std::vector<TableSpec> tables;
    auto parse_table_ref = [&]() -> bool {
        std::string real_name;
        if (!parser->ident(&real_name)) {
            return false;
        }
        (void)sm_manager->db_.get_table(real_name);
        std::string visible_name = real_name;
        if (parser->consume_word("as")) {
            if (!parser->ident(&visible_name)) {
                return false;
            }
        } else if (parser->peek().kind == TokenKind::Ident && !is_stop_word(parser->peek().text)) {
            parser->ident(&visible_name);
        }
        tables.push_back({real_name, visible_name});
        return true;
    };

    if (!parse_table_ref()) {
        return false;
    }

    std::vector<Condition> conds;
    for (;;) {
        if (parser->consume_symbol(",")) {
            if (!parse_table_ref()) {
                return false;
            }
            continue;
        }
        if (parser->consume_word("join")) {
            if (!parse_table_ref()) {
                return false;
            }
            if (parser->consume_word("on")) {
                auto on_conds = parse_cond_list(parser, sm_manager, tables);
                conds.insert(conds.end(), on_conds.begin(), on_conds.end());
            }
            continue;
        }
        break;
    }

    auto where_conds = parse_where(parser, sm_manager, tables);
    conds.insert(conds.end(), where_conds.begin(), where_conds.end());

    std::vector<OrderByCol> order_cols;
    if (parser->consume_word("order")) {
        if (!parser->expect_word("by")) {
            return false;
        }
        for (;;) {
            TabCol col = parse_col_ref(parser);
            resolve_col(sm_manager, tables, &col);
            bool desc = false;
            if (parser->consume_word("desc")) {
                desc = true;
            } else {
                parser->consume_word("asc");
            }
            order_cols.push_back({.col = col, .is_desc = desc});
            if (!parser->consume_symbol(",")) {
                break;
            }
        }
    }

    int limit = -1;
    if (parser->consume_word("limit")) {
        Value lim = parse_value(parser);
        if (lim.type != TYPE_INT) {
            return false;
        }
        limit = lim.int_val;
    }
    if (parser->peek().kind == TokenKind::Ident &&
        (same_word(parser->peek().text, "group") || same_word(parser->peek().text, "having"))) {
        return false;
    }
    if (!parser->finish()) {
        return false;
    }

    for (auto &item : items) {
        if (item.aggregate && !item.star) {
            resolve_col(sm_manager, tables, &item.col);
        } else if (!item.aggregate && !item.star) {
            resolve_col(sm_manager, tables, &item.col);
        }
    }

    bool has_agg = false;
    bool has_plain = false;
    for (const auto &item : items) {
        has_agg = has_agg || item.aggregate;
        has_plain = has_plain || !item.aggregate;
    }
    if (has_agg && has_plain) {
        return false;
    }

    std::map<std::string, std::vector<TabCol>> join_needed_cols;
    const bool push_join_projection = tables.size() > 1 && can_push_join_projection(items);
    if (push_join_projection) {
        join_needed_cols = collect_join_projection_cols(items, conds, order_cols);
    }

    std::vector<std::string> ordered_index_cols;
    int scan_limit = -1;
    bool ordered_scan = false;
    bool scan_desc = false;
    bool order_limit_pushed = false;
    if (tables.size() == 1) {
        const auto &table = tables.front();
        TabMeta &tab = sm_manager->db_.get_table(table.real_name);
        if (!has_agg && order_cols.size() == 1 && limit > 0) {
            ordered_index_cols = choose_order_limit_index_cols(tab, table.visible_name, conds, order_cols.front().col);
            if (!ordered_index_cols.empty()) {
                scan_limit = limit;
                ordered_scan = true;
                scan_desc = order_cols.front().is_desc;
                order_limit_pushed = true;
            }
        } else if (has_agg && items.size() == 1 && items.front().aggregate && !items.front().star &&
                   (items.front().agg == AGG_MIN || items.front().agg == AGG_MAX)) {
            ordered_index_cols = choose_order_limit_index_cols(tab, table.visible_name, conds, items.front().col);
            if (!ordered_index_cols.empty()) {
                scan_limit = 1;
                ordered_scan = true;
                scan_desc = items.front().agg == AGG_MAX;
            }
        }
    }

    std::unique_ptr<AbstractExecutor> root;
    if (tables.size() == 1) {
        const auto &table = tables.front();
        root = make_scan(sm_manager, table.real_name, table.visible_name, conds, context,
                         ordered_index_cols, scan_limit, ordered_scan, scan_desc);
    } else {
        root = make_join_tree(sm_manager, tables, conds, context,
                              push_join_projection ? &join_needed_cols : nullptr);
    }
    if (has_agg) {
        run_aggregate(std::move(root), std::move(items), context);
        return true;
    }

    if (!order_limit_pushed && (!order_cols.empty() || limit >= 0)) {
        root = std::make_unique<SortExecutor>(std::move(root), order_cols, limit);
    }

    std::vector<TabCol> display_cols;
    if (items.size() == 1 && items[0].star) {
        for (const auto &table : tables) {
            TabMeta &tab = sm_manager->db_.get_table(table.real_name);
            for (const auto &col : tab.cols) {
                display_cols.push_back({.tab_name = table.visible_name, .col_name = col.name});
            }
        }
    } else {
        std::vector<TabCol> select_cols;
        std::vector<std::string> output_names;
        for (auto &item : items) {
            if (item.star) {
                return false;
            }
            select_cols.push_back(item.col);
            output_names.push_back(item.output_name);
            display_cols.push_back({.tab_name = item.output_name.empty() ? item.col.tab_name : "",
                                    .col_name = item.output_name.empty() ? item.col.col_name : item.output_name});
        }
        root = std::make_unique<ProjectionExecutor>(std::move(root), select_cols, output_names);
    }

    ql_manager->select_from(std::move(root), std::move(display_cols), context);
    return true;
}

}  // namespace

bool try_execute_simple_sql(const std::string &sql, SmManager *sm_manager, QlManager *ql_manager,
                            Context *context) {
    try {
        Lexer lexer(sql);
        Parser parser(lexer.scan());
        if (parser.peek().kind != TokenKind::Ident) {
            return false;
        }
        if (same_word(parser.peek().text, "insert")) {
            return exec_insert(&parser, sm_manager, context);
        }
        if (same_word(parser.peek().text, "update")) {
            return exec_update(&parser, sm_manager, context);
        }
        if (same_word(parser.peek().text, "delete")) {
            return exec_delete(&parser, sm_manager, context);
        }
        if (same_word(parser.peek().text, "select")) {
            return exec_select(&parser, sm_manager, ql_manager, context);
        }
    } catch (SimpleFallback &) {
        return false;
    }
    return false;
}
