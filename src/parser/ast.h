/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#pragma once

#include <vector>
#include <string>
#include <memory>

enum JoinType {
    INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN
};
namespace ast {

enum SvType {
    SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL
};

enum SvCompOp {
    SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE
};

enum OrderByDir {
    OrderBy_DEFAULT,
    OrderBy_ASC,
    OrderBy_DESC
};

enum SetKnobType {
    EnableNestLoop, EnableSortMerge
};

enum TxnIsolationLevel {
    SnapshotIsolation, Serializable
};

enum AggFuncType {
    AGG_FUNC_COUNT,
    AGG_FUNC_MAX,
    AGG_FUNC_MIN,
    AGG_FUNC_SUM,
    AGG_FUNC_AVG
};

// Base class for tree nodes
struct TreeNode {
    virtual ~TreeNode() = default;  // enable polymorphism
};

struct Help : public TreeNode {
};

struct ShowTables : public TreeNode {
};

struct ShowIndex : public TreeNode {
    std::string tab_name;

    ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct TxnBegin : public TreeNode {
};

struct TxnCommit : public TreeNode {
};

struct TxnAbort : public TreeNode {
};

struct TxnRollback : public TreeNode {
};

struct TypeLen : public TreeNode {
    SvType type;
    int len;

    TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
};

struct Field : public TreeNode {
};

struct ColDef : public Field {
    std::string col_name;
    std::shared_ptr<TypeLen> type_len;

    ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_) :
            col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
};

struct CreateTable : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Field>> fields;

    CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_) :
            tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

struct DropTable : public TreeNode {
    std::string tab_name;

    DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct DescTable : public TreeNode {
    std::string tab_name;

    DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct CreateIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    CreateIndex(std::string tab_name_, std::vector<std::string> col_names_) :
            tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct DropIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    DropIndex(std::string tab_name_, std::vector<std::string> col_names_) :
            tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct Expr : public TreeNode {
};

struct Value : public Expr {
    std::string raw_text;
};

struct IntLit : public Value {
    int val;

    IntLit(int val_, std::string raw_text_ = "") : val(val_) {
        raw_text = std::move(raw_text_);
    }
};

struct FloatLit : public Value {
    float val;

    FloatLit(float val_, std::string raw_text_ = "") : val(val_) {
        raw_text = std::move(raw_text_);
    }
};

struct StringLit : public Value {
    std::string val;

    StringLit(std::string val_, std::string raw_text_ = "") : val(std::move(val_)) {
        raw_text = std::move(raw_text_);
    }
};

struct BoolLit : public Value {
    bool val;

    BoolLit(bool val_, std::string raw_text_ = "") : val(val_) {
        raw_text = std::move(raw_text_);
    }
};

struct Col : public Expr {
    std::string tab_name;
    std::string col_name;

    Col(std::string tab_name_, std::string col_name_) :
            tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {}
};

struct AggExpr : public Expr {
    AggFuncType func;
    bool is_star;
    std::shared_ptr<Col> col;

    AggExpr(AggFuncType func_, bool is_star_, std::shared_ptr<Col> col_) :
            func(func_), is_star(is_star_), col(std::move(col_)) {}
};

struct SelectItem : public TreeNode {
    std::shared_ptr<Expr> expr;
    std::string alias;

    SelectItem(std::shared_ptr<Expr> expr_, std::string alias_) :
            expr(std::move(expr_)), alias(std::move(alias_)) {}
};

struct SetClause : public TreeNode {
    std::string col_name;
    std::shared_ptr<Value> val;
    std::string rhs_col_name;
    char op;

    SetClause(std::string col_name_, std::shared_ptr<Value> val_) :
            col_name(std::move(col_name_)), val(std::move(val_)), op(0) {}

    SetClause(std::string col_name_, std::string rhs_col_name_, char op_, std::shared_ptr<Value> val_) :
            col_name(std::move(col_name_)), val(std::move(val_)), rhs_col_name(std::move(rhs_col_name_)), op(op_) {}
};

struct BinaryExpr : public TreeNode {
    std::shared_ptr<Expr> lhs;
    SvCompOp op;
    std::shared_ptr<Expr> rhs;

    BinaryExpr(std::shared_ptr<Expr> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) :
            lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct OrderBy : public TreeNode
{
    std::shared_ptr<Expr> expr;
    OrderByDir orderby_dir;
    OrderBy( std::shared_ptr<Expr> expr_, OrderByDir orderby_dir_) :
       expr(std::move(expr_)), orderby_dir(std::move(orderby_dir_)) {}
};

struct InsertStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Value>> vals;

    InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_) :
            tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

struct DeleteStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_) :
            tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<SetClause>> set_clauses;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    UpdateStmt(std::string tab_name_,
               std::vector<std::shared_ptr<SetClause>> set_clauses_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_) :
            tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
    std::string left;
    std::string right;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    JoinType type;

    JoinExpr(std::string left_, std::string right_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_) :
            left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
};

struct UnionStmt;

struct TableRef : public TreeNode {
    std::string tab_name;
    std::string alias;
    std::shared_ptr<UnionStmt> union_query;

    TableRef(std::string tab_name_, std::string alias_) :
            tab_name(std::move(tab_name_)), alias(std::move(alias_)) {}

    TableRef(std::shared_ptr<UnionStmt> union_query_, std::string alias_) :
            alias(std::move(alias_)), union_query(std::move(union_query_)) {}

    bool is_derived() const { return union_query != nullptr; }
};

struct FromClause : public TreeNode {
    std::vector<std::shared_ptr<TableRef>> tabs;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
};

struct SelectStmt : public TreeNode {
    std::vector<std::shared_ptr<SelectItem>> items;
    std::vector<std::shared_ptr<TableRef>> tabs;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    std::vector<std::shared_ptr<Col>> group_cols;
    std::vector<std::shared_ptr<BinaryExpr>> having_conds;
    std::vector<std::shared_ptr<JoinExpr>> jointree;

    
    bool has_sort;
    std::vector<std::shared_ptr<OrderBy>> orders;
    int limit;


    SelectStmt(std::vector<std::shared_ptr<SelectItem>> items_,
               std::vector<std::shared_ptr<TableRef>> tabs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_,
               std::vector<std::shared_ptr<Col>> group_cols_,
               std::vector<std::shared_ptr<BinaryExpr>> having_conds_,
               std::vector<std::shared_ptr<OrderBy>> orders_,
               int limit_) :
            items(std::move(items_)), tabs(std::move(tabs_)), conds(std::move(conds_)),
            group_cols(std::move(group_cols_)), having_conds(std::move(having_conds_)),
            orders(std::move(orders_)), limit(limit_) {
                has_sort = !orders.empty();
            }
};

struct UnionStmt : public TreeNode {
    std::vector<std::shared_ptr<SelectStmt>> selects;

    UnionStmt(std::vector<std::shared_ptr<SelectStmt>> selects_) : selects(std::move(selects_)) {}
};

struct ExplainAnalyze : public TreeNode {
    std::shared_ptr<SelectStmt> select;

    ExplainAnalyze(std::shared_ptr<SelectStmt> select_) : select(std::move(select_)) {}
};

// set enable_nestloop
struct SetStmt : public TreeNode {
    SetKnobType set_knob_type_;
    bool bool_val_;

    SetStmt(SetKnobType &type, bool bool_value) : 
        set_knob_type_(type), bool_val_(bool_value) { }
};

struct SetTransactionIsolation : public TreeNode {
    TxnIsolationLevel isolation_level_;

    SetTransactionIsolation(TxnIsolationLevel isolation_level) : isolation_level_(isolation_level) {}
};

// Semantic value
struct SemValue {
    SemValue() : sv_int(0), sv_float(0), sv_bool(false), sv_orderby_dir(OrderBy_DEFAULT),
                 sv_comp_op(SV_OP_EQ), sv_setKnobType(EnableNestLoop),
                 sv_txn_isolation(SnapshotIsolation) {}

    int sv_int;
    float sv_float;
    std::string sv_str;
    std::string sv_raw_str;
    bool sv_bool;
    OrderByDir sv_orderby_dir;
    std::vector<std::string> sv_strs;

    std::shared_ptr<TreeNode> sv_node;

    SvCompOp sv_comp_op;

    std::shared_ptr<TypeLen> sv_type_len;

    std::shared_ptr<Field> sv_field;
    std::vector<std::shared_ptr<Field>> sv_fields;

    std::shared_ptr<Expr> sv_expr;

    std::shared_ptr<Value> sv_val;
    std::vector<std::shared_ptr<Value>> sv_vals;

    std::shared_ptr<TableRef> sv_table_ref;
    std::vector<std::shared_ptr<TableRef>> sv_table_refs;
    std::shared_ptr<FromClause> sv_from;

    std::shared_ptr<Col> sv_col;
    std::vector<std::shared_ptr<Col>> sv_cols;

    std::shared_ptr<AggExpr> sv_agg_expr;
    AggFuncType sv_agg_func_type;
    std::shared_ptr<SelectItem> sv_select_item;
    std::vector<std::shared_ptr<SelectItem>> sv_select_items;

    std::shared_ptr<SetClause> sv_set_clause;
    std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

    std::shared_ptr<BinaryExpr> sv_cond;
    std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

    std::shared_ptr<OrderBy> sv_orderby;
    std::vector<std::shared_ptr<OrderBy>> sv_orderbys;

    SetKnobType sv_setKnobType;
    TxnIsolationLevel sv_txn_isolation;
};

extern std::shared_ptr<ast::TreeNode> parse_tree;

}

#define YYSTYPE ast::SemValue
