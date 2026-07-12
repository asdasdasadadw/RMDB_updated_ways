/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

#include <algorithm>
#include <cctype>

namespace {

bool same_col(const TabCol &lhs, const TabCol &rhs) {
    return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
}

bool contains_col(const std::vector<TabCol> &cols, const TabCol &target) {
    return std::find_if(cols.begin(), cols.end(), [&](const TabCol &col) {
        return same_col(col, target);
    }) != cols.end();
}

std::string agg_type_name(AggType type) {
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

std::string make_agg_output_name(AggType type, bool is_star, const TabCol &col) {
    if (is_star) {
        return agg_type_name(type) + "(*)";
    }
    std::string col_name = col.tab_name.empty() ? col.col_name : col.tab_name + "." + col.col_name;
    return agg_type_name(type) + "(" + col_name + ")";
}

bool same_agg_call(const AggCall &lhs, const AggCall &rhs) {
    return lhs.type == rhs.type && lhs.is_star == rhs.is_star && same_col(lhs.col, rhs.col);
}

size_t add_or_get_agg_call(std::vector<AggCall> &agg_calls, AggCall call) {
    for (size_t i = 0; i < agg_calls.size(); ++i) {
        if (same_agg_call(agg_calls[i], call)) {
            return i;
        }
    }
    agg_calls.push_back(std::move(call));
    return agg_calls.size() - 1;
}

bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

SetOp convert_set_op(char op) {
    switch (op) {
        case 0:
            return SET_OP_VALUE;
        case '+':
            return SET_OP_ADD;
        case '-':
            return SET_OP_SUB;
        case '*':
            return SET_OP_MUL;
        case '/':
            return SET_OP_DIV;
        default:
            throw InternalError("Unexpected update operator");
    }
}

ColMeta promote_union_col(ColMeta current, const ColMeta &next) {
    if (current.type == TYPE_STRING && next.type == TYPE_STRING) {
        current.len = std::max(current.len, next.len);
        return current;
    }
    if (current.type == next.type) {
        return current;
    }
    if (is_numeric_type(current.type) && is_numeric_type(next.type)) {
        current.type = TYPE_FLOAT;
        current.len = sizeof(float);
        return current;
    }
    throw IncompatibleTypeError(coltype2str(current.type), coltype2str(next.type));
}

}  // namespace

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::ExplainAnalyze>(parse))
    {
        query->explain_analyze = true;
        analyze_select(x->select, query);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        analyze_select(x, query);
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        if (!sm_manager_->db_.is_table(x->tab_name)) {
            throw TableNotFoundError(x->tab_name);
        }
        query->tables = {x->tab_name};
        query->table_refs = {{x->tab_name, x->tab_name}};
        get_clause(x->conds, query->conds);
        check_clause(query->table_refs, query->conds);

        TabMeta &tab = sm_manager_->db_.get_table(x->tab_name);
        for (auto &sv_set_clause : x->set_clauses) {
            auto col = tab.get_col(sv_set_clause->col_name);
            Value rhs = convert_sv_value(sv_set_clause->val);
            SetOp set_op = convert_set_op(sv_set_clause->op);
            TabCol rhs_col;
            if (set_op == SET_OP_VALUE) {
                if (col->type != rhs.type) {
                    rhs = cast_value_to_type(rhs, col->type);
                }
                rhs.init_raw(col->len);
            } else {
                auto src_col = tab.get_col(sv_set_clause->rhs_col_name);
                if (!is_numeric_type(col->type) || !is_numeric_type(src_col->type)) {
                    throw IncompatibleTypeError(coltype2str(col->type), coltype2str(src_col->type));
                }
                if (!is_numeric_type(rhs.type)) {
                    throw IncompatibleTypeError(coltype2str(src_col->type), coltype2str(rhs.type));
                }
                if (src_col->type != rhs.type) {
                    rhs = cast_value_to_type(rhs, src_col->type);
                }
                rhs.init_raw(src_col->len);
                rhs_col = {.tab_name = x->tab_name, .col_name = sv_set_clause->rhs_col_name};
            }
            SetClause set_clause{.lhs = {.tab_name = x->tab_name, .col_name = sv_set_clause->col_name},
                                  .rhs = rhs,
                                  .op = set_op,
                                  .rhs_col = rhs_col};
            query->set_clauses.push_back(set_clause);
        }
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        if (!sm_manager_->db_.is_table(x->tab_name)) {
            throw TableNotFoundError(x->tab_name);
        }
        query->tables = {x->tab_name};
        query->table_refs = {{x->tab_name, x->tab_name}};
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->table_refs, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        if (!sm_manager_->db_.is_table(x->tab_name)) {
            throw TableNotFoundError(x->tab_name);
        }
        query->tables = {x->tab_name};
        query->table_refs = {{x->tab_name, x->tab_name}};
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
        TabMeta &tab = sm_manager_->db_.get_table(x->tab_name);
        if (query->values.size() != tab.cols.size()) {
            throw InvalidValueCountError();
        }
        for (size_t i = 0; i < query->values.size(); ++i) {
            if (query->values[i].type != tab.cols[i].type) {
                query->values[i] = cast_value_to_type(query->values[i], tab.cols[i].type);
            }
        }
    } else {
        // do nothing
    }
    if (query->parse == nullptr) {
        query->parse = std::move(parse);
    }
    return query;
}

void Analyze::analyze_select(const std::shared_ptr<ast::SelectStmt> &select, std::shared_ptr<Query> &query) {
    query->select_all = select->items.empty();
    query->tables.clear();
    query->table_refs.clear();
    for (auto &tab_ref : select->tabs) {
        std::string alias = tab_ref->alias.empty() ? tab_ref->tab_name : tab_ref->alias;
        if (tab_ref->is_derived() && alias.empty()) {
            throw RMDBError("Derived table requires alias");
        }
        for (auto &existing : query->table_refs) {
            if (existing.alias == alias) {
                throw RMDBError("Duplicate table alias: " + alias);
            }
        }
        Query::TableRef analyzed_ref;
        analyzed_ref.tab_name = tab_ref->tab_name;
        analyzed_ref.alias = alias;
        if (tab_ref->is_derived()) {
            analyzed_ref.is_derived = true;
            analyzed_ref.derived_query = analyze_union(tab_ref->union_query, alias);
            analyzed_ref.derived_cols = analyzed_ref.derived_query->output_cols;
        } else {
            if (!sm_manager_->db_.is_table(tab_ref->tab_name)) {
                throw TableNotFoundError(tab_ref->tab_name);
            }
        }
        query->tables.push_back(alias);
        query->table_refs.push_back(std::move(analyzed_ref));
    }

    std::vector<ColMeta> all_cols;
    get_all_cols(query->table_refs, all_cols);

    get_clause(select->conds, query->conds);
    check_clause(query->table_refs, query->conds);

    query->group_cols.clear();
    for (auto &sv_group_col : select->group_cols) {
        query->group_cols.push_back(analyze_col_expr(sv_group_col, all_cols));
    }

    query->cols.clear();
    query->select_items.clear();
    query->agg_calls.clear();
    query->has_agg = false;
    query->limit = select->limit;

    if (query->select_all) {
        if (!query->group_cols.empty()) {
            throw RMDBError("SELECT * is not supported with GROUP BY");
        }
        for (auto &col : all_cols) {
            TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
            query->cols.push_back(sel_col);
            query->select_items.push_back({sel_col, col.name, false});
        }
    } else {
        for (auto &sv_item : select->items) {
            if (auto sv_col = std::dynamic_pointer_cast<ast::Col>(sv_item->expr)) {
                TabCol sel_col = analyze_col_expr(sv_col, all_cols);
                if (!query->group_cols.empty() && !contains_col(query->group_cols, sel_col)) {
                    throw RMDBError("Non-aggregate column must appear in GROUP BY");
                }
                std::string output_name = sv_item->alias.empty() ? sel_col.col_name : sv_item->alias;
                query->cols.push_back(sel_col);
                query->select_items.push_back({sel_col, output_name, false});
            } else if (auto sv_agg = std::dynamic_pointer_cast<ast::AggExpr>(sv_item->expr)) {
                std::string agg_name = agg_expr_to_string(sv_agg);
                std::string output_name = sv_item->alias.empty() ? agg_name : sv_item->alias;
                AggCall call = analyze_agg_expr(sv_agg, all_cols, agg_name);
                size_t idx = add_or_get_agg_call(query->agg_calls, call);
                query->has_agg = true;
                TabCol output_col = {.tab_name = "", .col_name = query->agg_calls[idx].output_name};
                query->cols.push_back(output_col);
                query->select_items.push_back({output_col, output_name, true});
            } else {
                throw RMDBError("Unsupported select expression");
            }
        }
    }

    get_having_clause(select->having_conds, query->having_conds, query->agg_calls, all_cols);
    if (!query->having_conds.empty()) {
        query->has_agg = true;
    }
    if (query->has_agg && query->select_all) {
        throw RMDBError("SELECT * is not supported with aggregate functions");
    }

    query->order_cols.clear();
    for (auto &sv_order : select->orders) {
        TabCol order_col;
        bool resolved_from_output = false;
        auto order_ast_col = std::dynamic_pointer_cast<ast::Col>(sv_order->expr);
        auto order_ast_agg = std::dynamic_pointer_cast<ast::AggExpr>(sv_order->expr);
        if (query->has_agg || !query->group_cols.empty()) {
            if (order_ast_col != nullptr) {
                for (auto &sel_item : query->select_items) {
                    if (order_ast_col->tab_name.empty() && sel_item.output_name == order_ast_col->col_name) {
                        order_col = {.tab_name = "", .col_name = sel_item.output_name};
                        resolved_from_output = true;
                        break;
                    }
                }
            } else if (order_ast_agg != nullptr) {
                AggCall call = analyze_agg_expr(order_ast_agg, all_cols, agg_expr_to_string(order_ast_agg));
                size_t idx = add_or_get_agg_call(query->agg_calls, call);
                order_col = {.tab_name = "", .col_name = query->agg_calls[idx].output_name};
                resolved_from_output = true;
            }
        }
        if (!resolved_from_output && !(query->has_agg || !query->group_cols.empty())) {
            if (order_ast_agg != nullptr) {
                throw RMDBError("Aggregate functions are not allowed in ORDER BY without aggregate query");
            }
            for (auto &sel_item : query->select_items) {
                if (order_ast_col != nullptr && order_ast_col->tab_name.empty() && sel_item.output_name == order_ast_col->col_name) {
                    order_col = sel_item.source_col;
                    resolved_from_output = true;
                    break;
                }
            }
        }
        if (!resolved_from_output) {
            if (order_ast_col == nullptr) {
                throw RMDBError("Unsupported ORDER BY expression");
            }
            order_col = analyze_col_expr(order_ast_col, all_cols);
        }
        if ((query->has_agg || !query->group_cols.empty()) && !resolved_from_output &&
            !contains_col(query->group_cols, order_col) && !contains_col(query->cols, order_col)) {
            throw RMDBError("ORDER BY column must be selected or grouped");
        }
        query->order_cols.push_back({order_col, sv_order->orderby_dir == ast::OrderBy_DESC});
    }

    if (query->has_agg && query->group_cols.empty()) {
        for (auto &item : query->select_items) {
            if (!item.is_agg) {
                throw RMDBError("Non-aggregate column must appear in GROUP BY");
            }
        }
    }
    if (query->has_agg || !query->group_cols.empty()) {
        for (auto &cond : query->having_conds) {
            if (!cond.lhs_col.tab_name.empty() && !contains_col(query->group_cols, cond.lhs_col)) {
                throw RMDBError("Non-aggregate HAVING column must appear in GROUP BY");
            }
            if (!cond.is_rhs_val && !cond.rhs_col.tab_name.empty() && !contains_col(query->group_cols, cond.rhs_col)) {
                throw RMDBError("Non-aggregate HAVING column must appear in GROUP BY");
            }
        }
    }

    build_select_output_cols(query, all_cols);
    query->parse = select;
}

std::shared_ptr<Query> Analyze::analyze_union(const std::shared_ptr<ast::UnionStmt> &union_stmt,
                                             const std::string &alias) {
    if (union_stmt == nullptr || union_stmt->selects.size() < 2) {
        throw RMDBError("UNION requires at least two SELECT statements");
    }

    auto query = std::make_shared<Query>();
    query->is_union = true;
    query->parse = union_stmt;

    std::vector<ColMeta> common_cols;
    for (size_t branch_idx = 0; branch_idx < union_stmt->selects.size(); ++branch_idx) {
        auto branch_query = std::make_shared<Query>();
        analyze_select(union_stmt->selects[branch_idx], branch_query);
        if (branch_query->output_cols.empty()) {
            throw RMDBError("UNION branch has no output column");
        }

        if (branch_idx == 0) {
            common_cols = branch_query->output_cols;
        } else {
            if (branch_query->output_cols.size() != common_cols.size()) {
                throw RMDBError("UNION branch column count mismatch");
            }
            for (size_t i = 0; i < common_cols.size(); ++i) {
                common_cols[i] = promote_union_col(common_cols[i], branch_query->output_cols[i]);
            }
        }
        query->union_branches.push_back(std::move(branch_query));
    }

    query->output_cols.clear();
    query->cols.clear();
    query->select_items.clear();
    int offset = 0;
    for (auto col : common_cols) {
        col.tab_name = alias;
        col.offset = offset;
        col.index = false;
        offset += col.len;
        query->output_cols.push_back(col);
        TabCol tab_col{.tab_name = alias, .col_name = col.name};
        query->cols.push_back(tab_col);
        query->select_items.push_back({tab_col, col.name, false});
    }
    query->select_all = true;
    return query;
}

void Analyze::build_select_output_cols(std::shared_ptr<Query> &query, const std::vector<ColMeta> &all_cols) {
    query->output_cols.clear();
    int offset = 0;
    auto add_col = [&](std::string name, ColType type, int len) {
        ColMeta col;
        col.tab_name = "";
        col.name = std::move(name);
        col.type = type;
        col.len = len;
        col.offset = offset;
        col.index = false;
        offset += col.len;
        query->output_cols.push_back(std::move(col));
    };

    if (query->select_all) {
        for (const auto &col : all_cols) {
            add_col(col.name, col.type, col.len);
        }
        return;
    }

    for (const auto &item : query->select_items) {
        if (item.is_agg) {
            auto it = std::find_if(query->agg_calls.begin(), query->agg_calls.end(), [&](const AggCall &call) {
                return call.output_name == item.source_col.col_name;
            });
            if (it == query->agg_calls.end()) {
                throw ColumnNotFoundError(item.source_col.col_name);
            }
            add_col(item.output_name, it->result_type, it->result_len);
        } else {
            auto meta = find_col_meta(all_cols, item.source_col);
            add_col(item.output_name, meta.type, meta.len);
        }
    }
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        auto pos = std::find_if(all_cols.begin(), all_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == all_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
        }
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<Query::TableRef> &table_refs, std::vector<ColMeta> &all_cols) {
    for (auto &table_ref : table_refs) {
        if (table_ref.is_derived) {
            for (auto col : table_ref.derived_cols) {
                col.tab_name = table_ref.alias;
                all_cols.push_back(std::move(col));
            }
            continue;
        }
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(table_ref.tab_name).cols;
        for (auto col : sel_tab_cols) {
            col.tab_name = table_ref.alias;
            all_cols.push_back(col);
        }
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        auto lhs_col = std::dynamic_pointer_cast<ast::Col>(expr->lhs);
        if (lhs_col == nullptr || std::dynamic_pointer_cast<ast::AggExpr>(expr->rhs) != nullptr) {
            throw RMDBError("Aggregate functions are not allowed in WHERE");
        }
        cond.lhs_col = {.tab_name = lhs_col->tab_name, .col_name = lhs_col->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        } else {
            throw RMDBError("Unsupported WHERE expression");
        }
        conds.push_back(cond);
    }
}

void Analyze::get_having_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds,
                                std::vector<Condition> &conds, std::vector<AggCall> &agg_calls,
                                const std::vector<ColMeta> &all_cols) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        if (auto lhs_col = std::dynamic_pointer_cast<ast::Col>(expr->lhs)) {
            cond.lhs_col = analyze_col_expr(lhs_col, all_cols);
        } else if (auto lhs_agg = std::dynamic_pointer_cast<ast::AggExpr>(expr->lhs)) {
            AggCall call = analyze_agg_expr(lhs_agg, all_cols, agg_expr_to_string(lhs_agg));
            size_t idx = add_or_get_agg_call(agg_calls, call);
            cond.lhs_col = {.tab_name = "", .col_name = agg_calls[idx].output_name};
        } else {
            throw RMDBError("Unsupported HAVING expression");
        }

        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = analyze_col_expr(rhs_col, all_cols);
        } else if (auto rhs_agg = std::dynamic_pointer_cast<ast::AggExpr>(expr->rhs)) {
            AggCall call = analyze_agg_expr(rhs_agg, all_cols, agg_expr_to_string(rhs_agg));
            size_t idx = add_or_get_agg_call(agg_calls, call);
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = "", .col_name = agg_calls[idx].output_name};
        } else {
            throw RMDBError("Unsupported HAVING expression");
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<Query::TableRef> &table_refs, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(table_refs, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        auto lhs_col = std::find_if(all_cols.begin(), all_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == cond.lhs_col.tab_name && col.name == cond.lhs_col.col_name;
        });
        if (lhs_col == all_cols.end()) {
            throw ColumnNotFoundError(cond.lhs_col.tab_name + "." + cond.lhs_col.col_name);
        }
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
        } else {
            auto rhs_col = std::find_if(all_cols.begin(), all_cols.end(), [&](const ColMeta &col) {
                return col.tab_name == cond.rhs_col.tab_name && col.name == cond.rhs_col.col_name;
            });
            if (rhs_col == all_cols.end()) {
                throw ColumnNotFoundError(cond.rhs_col.tab_name + "." + cond.rhs_col.col_name);
            }
            rhs_type = rhs_col->type;
        }
        if (lhs_type != rhs_type) {
            if (cond.is_rhs_val) {
                cond.rhs_val = cast_value_to_type(cond.rhs_val, lhs_type);
            } else {
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }
        }
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
        }
    }
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    val.raw_text = sv_val->raw_text;
    return val;
}

Value Analyze::cast_value_to_type(Value val, ColType target_type) {
    if (val.type == target_type) {
        return val;
    }
    if (target_type == TYPE_FLOAT && val.type == TYPE_INT) {
        Value casted;
        casted.set_float(static_cast<float>(val.int_val));
        casted.raw_text = val.raw_text;
        return casted;
    }
    if (target_type == TYPE_INT && val.type == TYPE_FLOAT) {
        Value casted;
        casted.set_int(static_cast<int>(val.float_val));
        casted.raw_text = val.raw_text;
        return casted;
    }
    throw IncompatibleTypeError(coltype2str(target_type), coltype2str(val.type));
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}

AggType Analyze::convert_sv_agg_type(ast::AggFuncType type) {
    switch (type) {
        case ast::AGG_FUNC_COUNT:
            return AGG_COUNT;
        case ast::AGG_FUNC_MAX:
            return AGG_MAX;
        case ast::AGG_FUNC_MIN:
            return AGG_MIN;
        case ast::AGG_FUNC_SUM:
            return AGG_SUM;
        case ast::AGG_FUNC_AVG:
            return AGG_AVG;
        default:
            throw InternalError("Unexpected aggregate type");
    }
}

std::string Analyze::agg_expr_to_string(const std::shared_ptr<ast::AggExpr> &expr) {
    AggType type = convert_sv_agg_type(expr->func);
    TabCol col;
    if (!expr->is_star && expr->col != nullptr) {
        col = {.tab_name = expr->col->tab_name, .col_name = expr->col->col_name};
    }
    return make_agg_output_name(type, expr->is_star, col);
}

AggCall Analyze::analyze_agg_expr(const std::shared_ptr<ast::AggExpr> &expr, const std::vector<ColMeta> &all_cols,
                                  const std::string &output_name) {
    AggCall call;
    call.type = convert_sv_agg_type(expr->func);
    call.is_star = expr->is_star;
    call.output_name = output_name;
    if (!call.is_star) {
        call.col = analyze_col_expr(expr->col, all_cols);
        auto col_meta = find_col_meta(all_cols, call.col);
        if ((call.type == AGG_SUM || call.type == AGG_AVG) && col_meta.type == TYPE_STRING) {
            throw RMDBError("Aggregate function only supports numeric column");
        }
    } else if (call.type != AGG_COUNT) {
        throw RMDBError("Only COUNT supports *");
    }

    if (call.type == AGG_COUNT) {
        call.result_type = TYPE_INT;
        call.result_len = sizeof(int);
    } else if (call.type == AGG_AVG) {
        call.result_type = TYPE_FLOAT;
        call.result_len = sizeof(float);
    } else if (!call.is_star) {
        auto col_meta = find_col_meta(all_cols, call.col);
        call.result_type = col_meta.type;
        call.result_len = col_meta.len;
    } else {
        call.result_type = TYPE_FLOAT;
        call.result_len = sizeof(float);
    }
    return call;
}

TabCol Analyze::analyze_col_expr(const std::shared_ptr<ast::Col> &expr, const std::vector<ColMeta> &all_cols) {
    if (expr == nullptr) {
        throw RMDBError("Column expected");
    }
    return check_column(all_cols, {.tab_name = expr->tab_name, .col_name = expr->col_name});
}

ColMeta Analyze::find_col_meta(const std::vector<ColMeta> &all_cols, const TabCol &col) {
    auto it = std::find_if(all_cols.begin(), all_cols.end(), [&](const ColMeta &meta) {
        return meta.tab_name == col.tab_name && meta.name == col.col_name;
    });
    if (it == all_cols.end()) {
        throw ColumnNotFoundError(col.tab_name + "." + col.col_name);
    }
    return *it;
}
