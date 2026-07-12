/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner.h"

#include <algorithm>
#include <set>
#include <memory>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "execution/runtime_filter_cache.h"
#include "index/ix.h"
#include "record_printer.h"

bool Planner::get_index_cols(const std::string &real_tab_name, const std::string &visible_tab_name,
                             const std::vector<Condition> &curr_conds,
                             std::vector<std::string>& index_col_names) {
    index_col_names.clear();
    if (!sm_manager_->db_.is_table(real_tab_name)) {
        return false;
    }
    TabMeta& tab = sm_manager_->db_.get_table(real_tab_name);
    std::map<std::string, std::vector<CompOp>> cond_map;
    for (auto &cond : curr_conds) {
        if (cond.is_rhs_val && cond.lhs_col.tab_name == visible_tab_name && cond.op != OP_NE) {
            cond_map[cond.lhs_col.col_name].push_back(cond.op);
        }
    }

    size_t best_prefix = 0;
    const IndexMeta *best_index = nullptr;
    for (auto &index : tab.indexes) {
        size_t prefix = 0;
        bool range_seen = false;
        for (auto &col : index.cols) {
            auto it = cond_map.find(col.name);
            if (it == cond_map.end() || range_seen) {
                break;
            }
            bool has_eq = false;
            bool has_range = false;
            for (auto op : it->second) {
                if (op == OP_EQ) {
                    has_eq = true;
                } else if (op == OP_LT || op == OP_LE || op == OP_GT || op == OP_GE) {
                    has_range = true;
                }
            }
            if (!has_eq && !has_range) {
                break;
            }
            prefix++;
            if (!has_eq && has_range) {
                range_seen = true;
            }
        }
        if (prefix > best_prefix) {
            best_prefix = prefix;
            best_index = &index;
        }
    }
    if (best_index == nullptr || best_prefix == 0) {
        return false;
    }
    for (auto &col : best_index->cols) {
        index_col_names.push_back(col.name);
    }
    return true;
}

namespace {

std::string table_name_of(const Query::TableRef &ref) {
    return ref.alias.empty() ? ref.tab_name : ref.alias;
}

std::set<std::string> condition_tables(const Condition &cond) {
    std::set<std::string> tabs;
    tabs.insert(cond.lhs_col.tab_name);
    if (!cond.is_rhs_val) {
        tabs.insert(cond.rhs_col.tab_name);
    }
    return tabs;
}

bool is_single_table_cond(const Condition &cond) {
    return cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name;
}

std::string plan_first_table(const std::shared_ptr<Plan> &plan) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        return scan->tab_name_;
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        return plan_first_table(filter->subplan_);
    }
    if (auto project = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        return plan_first_table(project->subplan_);
    }
    if (auto agg = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
        return plan_first_table(agg->subplan_);
    }
    if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        return plan_first_table(join->left_);
    }
    return "";
}

bool plan_has_table(const std::shared_ptr<Plan> &plan, const std::string &table) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        return scan->tab_name_ == table;
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        return plan_has_table(filter->subplan_, table);
    }
    if (auto project = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        return plan_has_table(project->subplan_, table);
    }
    if (auto agg = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
        return plan_has_table(agg->subplan_, table);
    }
    if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        return plan_has_table(join->left_, table) || plan_has_table(join->right_, table);
    }
    return false;
}

std::set<std::string> plan_tables(const std::shared_ptr<Plan> &plan) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        return {scan->tab_name_};
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        return plan_tables(filter->subplan_);
    }
    if (auto project = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        return plan_tables(project->subplan_);
    }
    if (auto agg = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
        return plan_tables(agg->subplan_);
    }
    if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        auto tabs = plan_tables(join->left_);
        auto right = plan_tables(join->right_);
        tabs.insert(right.begin(), right.end());
        return tabs;
    }
    return {};
}

bool cond_tables_subset(const Condition &cond, const std::set<std::string> &tabs) {
    auto refs = condition_tables(cond);
    return std::includes(tabs.begin(), tabs.end(), refs.begin(), refs.end());
}

bool cond_can_join_current(const Condition &cond, const std::set<std::string> &joined, const std::string &candidate) {
    if (cond.is_rhs_val) {
        return false;
    }
    bool lhs_joined = joined.find(cond.lhs_col.tab_name) != joined.end();
    bool rhs_joined = joined.find(cond.rhs_col.tab_name) != joined.end();
    return (lhs_joined && cond.rhs_col.tab_name == candidate) || (rhs_joined && cond.lhs_col.tab_name == candidate);
}

bool is_eq_join_with_table(const Condition &cond, const std::set<std::string> &joined, const std::string &candidate,
                           std::string *candidate_col = nullptr) {
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

std::vector<std::string> best_join_index_cols(const TabMeta &tab, const std::vector<Condition> &conds,
                                              const std::set<std::string> &joined,
                                              const std::string &candidate) {
    std::set<std::string> eq_cols;
    for (const auto &cond : conds) {
        std::string col;
        if (is_eq_join_with_table(cond, joined, candidate, &col)) {
            eq_cols.insert(col);
        }
    }

    const IndexMeta *best = nullptr;
    size_t best_prefix = 0;
    for (const auto &index : tab.indexes) {
        size_t prefix = 0;
        for (const auto &col : index.cols) {
            if (eq_cols.find(col.name) == eq_cols.end()) {
                break;
            }
            prefix++;
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

ScanPlan *find_single_scan_plan(const std::shared_ptr<Plan> &plan) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        return scan.get();
    }
    if (auto project = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        return find_single_scan_plan(project->subplan_);
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        return find_single_scan_plan(filter->subplan_);
    }
    return nullptr;
}

void add_unique_col(std::vector<TabCol> &cols, const TabCol &col) {
    auto it = std::find_if(cols.begin(), cols.end(), [&](const TabCol &existing) {
        return existing.tab_name == col.tab_name && existing.col_name == col.col_name;
    });
    if (it == cols.end()) {
        cols.push_back(col);
    }
}

}  // namespace

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names) {
    // auto has_tab = [&](const std::string &tab_name) {
    //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    // };
    std::vector<Condition> solved_conds;
    auto it = conds.begin();
    while (it != conds.end()) {
        if ((tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_val) || (it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0)) {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        } else {
            it++;
        }
    }
    return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan)
{
    if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan))
    {
        if(x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if(x->tab_name_.compare(cond->rhs_col.tab_name) == 0){
            return 2;
        } else {
            return 0;
        }
    }
    else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan))
    {
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if(left_res == 3){
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if(right_res == 3){
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if(left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if(left_res == 2) {
            // 需要将左右两边的条件变换位置
            std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables, 
                std::vector<std::shared_ptr<Plan>> plans)
{
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if(x->tab_name_.compare(table) == 0)
        {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}


std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context)
{
    
    //TODO 实现逻辑优化规则

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plan = make_one_rel(query, context);
    
    // 其他物理优化

    return plan;
}



std::shared_ptr<Plan> Planner::make_scan_plan(const Query::TableRef &table_ref,
                                              const std::vector<Condition> &single_table_conds,
                                              const std::vector<TabCol> &needed_cols,
                                              bool push_project, Context *context)
{
    if (table_ref.is_derived) {
        std::shared_ptr<Plan> plan = generate_union_plan(table_ref.derived_query);
        if (!single_table_conds.empty()) {
            plan = std::make_shared<FilterPlan>(T_Filter, plan, single_table_conds);
        }
        if (push_project && !needed_cols.empty()) {
            std::vector<TabCol> cols = needed_cols;
            std::sort(cols.begin(), cols.end());
            plan = std::make_shared<ProjectionPlan>(T_Projection, plan, cols);
        }
        return plan;
    }

    std::string alias = table_name_of(table_ref);
    std::vector<std::string> index_col_names;
    std::vector<Condition> scan_conds = single_table_conds;
    bool index_exist = get_index_cols(table_ref.tab_name, alias, scan_conds, index_col_names);
    if (index_exist) {
        std::vector<ColMeta> cache_cols;
        TabMeta &tab = sm_manager_->db_.get_table(table_ref.tab_name);
        if (RuntimeFilterCache::choose_columns(tab, alias, scan_conds, &cache_cols) &&
            cache_cols.size() > index_col_names.size()) {
            index_exist = false;
        }
    }
    if (!index_exist) {
        index_col_names.clear();
    }
    auto scan = std::make_shared<ScanPlan>(index_exist ? T_IndexScan : T_SeqScan, sm_manager_,
                                           table_ref.tab_name, alias, scan_conds, index_col_names);
    std::shared_ptr<Plan> plan = scan;
    if (push_project && !needed_cols.empty()) {
        std::vector<TabCol> cols = needed_cols;
        std::sort(cols.begin(), cols.end());
        plan = std::make_shared<ProjectionPlan>(T_Projection, plan, cols);
    }
    return plan;
}

std::vector<TabCol> Planner::collect_needed_cols(const std::shared_ptr<Query> &query,
                                                 const std::vector<Condition> &join_conds) {
    std::vector<TabCol> needed_cols;
    for (auto &col : query->cols) {
        if (!col.tab_name.empty()) {
            add_unique_col(needed_cols, col);
        }
    }
    for (auto &col : query->group_cols) {
        add_unique_col(needed_cols, col);
    }
    for (auto &agg : query->agg_calls) {
        if (!agg.is_star) {
            add_unique_col(needed_cols, agg.col);
        }
    }
    for (auto &cond : query->having_conds) {
        if (!cond.lhs_col.tab_name.empty()) {
            add_unique_col(needed_cols, cond.lhs_col);
        }
        if (!cond.is_rhs_val && !cond.rhs_col.tab_name.empty()) {
            add_unique_col(needed_cols, cond.rhs_col);
        }
    }
    for (auto &order : query->order_cols) {
        if (!order.col.tab_name.empty()) {
            add_unique_col(needed_cols, order.col);
        }
    }
    for (auto &cond : join_conds) {
        add_unique_col(needed_cols, cond.lhs_col);
        add_unique_col(needed_cols, cond.rhs_col);
    }
    return needed_cols;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context *context)
{
    std::vector<Query::TableRef> table_refs = query->table_refs;
    std::vector<Condition> single_conds;
    std::vector<Condition> join_conds;
    for (auto cond : query->conds) {
        if (is_single_table_cond(cond)) {
            single_conds.push_back(cond);
        } else {
            join_conds.push_back(cond);
        }
    }

    std::map<std::string, std::vector<Condition>> filters_by_table;
    for (auto &cond : single_conds) {
        filters_by_table[cond.lhs_col.tab_name].push_back(cond);
    }

    bool push_project = query->table_refs.size() > 1 && !query->select_all;
    std::map<std::string, std::vector<TabCol>> needed_cols;
    if (push_project) {
        for (auto &col : collect_needed_cols(query, join_conds)) {
            add_unique_col(needed_cols[col.tab_name], col);
        }
    }

    std::map<std::string, std::shared_ptr<Plan>> scans;
    for (auto &table_ref : table_refs) {
        std::string alias = table_name_of(table_ref);
        scans[alias] = make_scan_plan(table_ref, filters_by_table[alias], needed_cols[alias], push_project, context);
    }

    if (table_refs.empty()) {
        throw RMDBError("No table in select");
    }
    if (table_refs.size() == 1) {
        return scans[table_name_of(table_refs[0])];
    }

    std::set<std::string> joined;
    std::shared_ptr<Plan> root = scans[table_name_of(table_refs[0])];
    joined.insert(table_name_of(table_refs[0]));

    auto make_join_with = [&](size_t idx) {
        std::string candidate = table_name_of(table_refs[idx]);
        std::set<std::string> future = joined;
        future.insert(candidate);
        std::vector<Condition> conds;
        for (auto it = join_conds.begin(); it != join_conds.end();) {
            if (cond_can_join_current(*it, joined, candidate) || cond_tables_subset(*it, future)) {
                conds.push_back(*it);
                it = join_conds.erase(it);
            } else {
                ++it;
            }
        }
        auto scan = find_single_scan_plan(scans[candidate]);
        if (scan != nullptr) {
            auto index_cols = best_join_index_cols(sm_manager_->db_.get_table(scan->real_tab_name_), conds, joined,
                                                   candidate);
            if (!index_cols.empty()) {
                scan->tag = T_IndexScan;
                scan->index_col_names_ = std::move(index_cols);
            }
        }
        root = std::make_shared<JoinPlan>(T_NestLoop, root, scans[candidate], conds);
        joined.insert(candidate);
    };

    for (size_t i = 1; i < table_refs.size(); ++i) {
        make_join_with(i);
    }

    if (!join_conds.empty()) {
        auto existing = std::dynamic_pointer_cast<JoinPlan>(root);
        if (existing != nullptr) {
            existing->conds_.insert(existing->conds_.end(), join_conds.begin(), join_conds.end());
        }
    }

    return root;
}

std::shared_ptr<Plan> Planner::generate_union_plan(std::shared_ptr<Query> query)
{
    if (query == nullptr || !query->is_union) {
        throw InternalError("Expected UNION query");
    }
    std::vector<std::shared_ptr<Plan>> subplans;
    subplans.reserve(query->union_branches.size());
    for (auto &branch : query->union_branches) {
        subplans.push_back(generate_select_plan(branch, nullptr));
    }
    return std::make_shared<UnionPlan>(T_Union, std::move(subplans), query->output_cols);
}


std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan)
{
    if(query->order_cols.empty() && query->limit < 0) {
        return plan;
    }
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), query->order_cols, query->limit);
}


/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    if (query->is_union) {
        return generate_union_plan(std::move(query));
    }

    //逻辑优化
    query = logical_optimization(std::move(query), context);

    //物理优化
    auto sel_cols = query->cols;
    auto select_items = query->select_items;
    std::vector<std::string> output_names;
    for (auto &item : select_items) {
        output_names.push_back(item.output_name);
    }
    bool display_all = query->select_all;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    if (query->has_agg || !query->group_cols.empty()) {
        std::vector<TabCol> final_cols;
        for (auto &item : select_items) {
            final_cols.push_back({.tab_name = "", .col_name = item.output_name});
        }
        plannerRoot = std::make_shared<AggregatePlan>(T_Aggregate, std::move(plannerRoot), query->group_cols,
                                                      query->agg_calls, select_items, query->having_conds);
        plannerRoot = generate_sort_plan(query, std::move(plannerRoot));
        plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot),
                                                       std::move(final_cols), display_all, output_names);
    } else {
        plannerRoot = generate_sort_plan(query, std::move(plannerRoot));
        plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot),
                                                       std::move(sel_cols), display_all,
                                                       display_all ? std::vector<std::string>() : output_names);
    }

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field : x->fields) {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
                ColDef col_def = {.name = sv_col_def->col_name,
                                  .type = interp_sv_type(sv_col_def->type_len->type),
                                  .len = sv_col_def->type_len->len};
                col_defs.push_back(col_def);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
        // create index;
        plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(),  x->tab_name,  
                                                    query->values, std::vector<Condition>(), std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, x->tab_name, query->conds, index_col_names);
        
        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, x->tab_name, query->conds, index_col_names);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name,  
                                                std::vector<Value>(), query->conds, std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, x->tab_name, query->conds, index_col_names);

        if (index_exist == false) {  // 该表没有索引
        index_col_names.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, x->tab_name, query->conds, index_col_names);
        }
        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name,
                                                     std::vector<Value>(), query->conds, 
                                                     query->set_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {

        bool explain_analyze = query->explain_analyze;
        bool select_all = query->select_all;
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                                    std::vector<Condition>(), std::vector<SetClause>());
        auto dml = std::dynamic_pointer_cast<DMLPlan>(plannerRoot);
        dml->explain_analyze_ = explain_analyze;
        dml->select_all_ = select_all;
    } else {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}
