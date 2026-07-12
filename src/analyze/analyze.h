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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "parser/parser.h"
#include "system/sm.h"
#include "common/common.h"

class Query{
    public:
    struct TableRef {
        std::string tab_name;
        std::string alias;
        bool is_derived = false;
        std::shared_ptr<Query> derived_query;
        std::vector<ColMeta> derived_cols;
    };

    std::shared_ptr<ast::TreeNode> parse;
    bool is_union = false;
    bool explain_analyze = false;
    bool select_all = false;
    // TODO jointree
    // where条件
    std::vector<Condition> conds;
    std::vector<Condition> having_conds;
    // 投影列
    std::vector<TabCol> cols;
    std::vector<SelectItem> select_items;
    std::vector<ColMeta> output_cols;
    std::vector<TabCol> group_cols;
    std::vector<AggCall> agg_calls;
    std::vector<OrderByCol> order_cols;
    bool has_agg = false;
    int limit = -1;
    // 表名
    std::vector<std::string> tables;
    std::vector<TableRef> table_refs;
    std::vector<std::shared_ptr<Query>> union_branches;
    // update 的set 值
    std::vector<SetClause> set_clauses;
    //insert 的values值
    std::vector<Value> values;

    Query(){}

};

class Analyze
{
private:
    SmManager *sm_manager_;
public:
    Analyze(SmManager *sm_manager) : sm_manager_(sm_manager){}
    ~Analyze(){}

    std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

private:
    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);
    void get_all_cols(const std::vector<Query::TableRef> &table_refs, std::vector<ColMeta> &all_cols);
    void analyze_select(const std::shared_ptr<ast::SelectStmt> &select, std::shared_ptr<Query> &query);
    std::shared_ptr<Query> analyze_union(const std::shared_ptr<ast::UnionStmt> &union_stmt,
                                         const std::string &alias);
    void build_select_output_cols(std::shared_ptr<Query> &query, const std::vector<ColMeta> &all_cols);
    void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);
    void get_having_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds,
                           std::vector<Condition> &conds, std::vector<AggCall> &agg_calls,
                           const std::vector<ColMeta> &all_cols);
    void check_clause(const std::vector<Query::TableRef> &table_refs, std::vector<Condition> &conds);
    Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);
    Value cast_value_to_type(Value val, ColType target_type);
    CompOp convert_sv_comp_op(ast::SvCompOp op);
    AggType convert_sv_agg_type(ast::AggFuncType type);
    std::string agg_expr_to_string(const std::shared_ptr<ast::AggExpr> &expr);
    AggCall analyze_agg_expr(const std::shared_ptr<ast::AggExpr> &expr, const std::vector<ColMeta> &all_cols,
                             const std::string &output_name);
    TabCol analyze_col_expr(const std::shared_ptr<ast::Col> &expr, const std::vector<ColMeta> &all_cols);
    ColMeta find_col_meta(const std::vector<ColMeta> &all_cols, const TabCol &col);
};
