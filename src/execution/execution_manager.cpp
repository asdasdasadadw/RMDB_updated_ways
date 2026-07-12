/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include <algorithm>
#include <set>
#include <sstream>

#include "executor_delete.h"
#include "executor_filter.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"
#include "common/config.h"

namespace {

std::string op_to_string(CompOp op) {
    switch (op) {
        case OP_EQ:
            return "=";
        case OP_NE:
            return "<>";
        case OP_LT:
            return "<";
        case OP_GT:
            return ">";
        case OP_LE:
            return "<=";
        case OP_GE:
            return ">=";
        default:
            return "";
    }
}

std::string value_to_string(const Value &val) {
    if (!val.raw_text.empty()) {
        return val.raw_text;
    }
    if (val.type == TYPE_INT) {
        return std::to_string(val.int_val);
    }
    if (val.type == TYPE_FLOAT) {
        std::ostringstream ss;
        ss << val.float_val;
        return ss.str();
    }
    return "'" + val.str_val + "'";
}

std::string col_to_string(const TabCol &col) {
    return col.tab_name + "." + col.col_name;
}

std::string cond_to_string(const Condition &cond) {
    std::string rhs = cond.is_rhs_val ? value_to_string(cond.rhs_val) : col_to_string(cond.rhs_col);
    return col_to_string(cond.lhs_col) + op_to_string(cond.op) + rhs;
}

template <typename T>
std::string join_strings(const std::vector<T> &items) {
    std::ostringstream ss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << items[i];
    }
    return ss.str();
}

std::set<std::string> plan_tables_for_print(const std::shared_ptr<Plan> &plan) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        return {scan->real_tab_name_};
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        return plan_tables_for_print(filter->subplan_);
    }
    if (auto project = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        return plan_tables_for_print(project->subplan_);
    }
    if (auto agg = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
        return plan_tables_for_print(agg->subplan_);
    }
    if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        auto tabs = plan_tables_for_print(join->left_);
        auto right = plan_tables_for_print(join->right_);
        tabs.insert(right.begin(), right.end());
        return tabs;
    }
    if (auto sort = std::dynamic_pointer_cast<SortPlan>(plan)) {
        return plan_tables_for_print(sort->subplan_);
    }
    if (auto union_plan = std::dynamic_pointer_cast<UnionPlan>(plan)) {
        std::set<std::string> tabs;
        for (auto &subplan : union_plan->subplans_) {
            auto sub_tabs = plan_tables_for_print(subplan);
            tabs.insert(sub_tabs.begin(), sub_tabs.end());
        }
        return tabs;
    }
    return {};
}

void collect_executor_rows(const std::shared_ptr<Plan> &plan, AbstractExecutor *exec) {
    if (plan == nullptr || exec == nullptr) {
        return;
    }
    plan->rows_ = exec->rows();
    if (auto project_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        collect_executor_rows(project_plan->subplan_, exec->child_at(0));
    } else if (auto agg_plan = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
        collect_executor_rows(agg_plan->subplan_, exec->child_at(0));
    } else if (auto filter_plan = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        collect_executor_rows(filter_plan->subplan_, exec->child_at(0));
    } else if (auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        collect_executor_rows(join_plan->left_, exec->child_at(0));
        collect_executor_rows(join_plan->right_, exec->child_at(1));
    } else if (auto sort_plan = std::dynamic_pointer_cast<SortPlan>(plan)) {
        collect_executor_rows(sort_plan->subplan_, exec->child_at(0));
    } else if (auto union_plan = std::dynamic_pointer_cast<UnionPlan>(plan)) {
        for (size_t i = 0; i < union_plan->subplans_.size(); ++i) {
            collect_executor_rows(union_plan->subplans_[i], exec->child_at(i));
        }
    }
}

void write_context(Context *context, const std::string &str) {
    if (context->data_send_ != nullptr && context->offset_ != nullptr) {
        memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
        *(context->offset_) += str.length();
    }
}

std::string explain_plan_to_string(const std::shared_ptr<Plan> &plan, int depth = 0) {
    std::ostringstream ss;
    std::string indent(depth, '\t');
    if (auto project = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        std::vector<std::string> cols;
        if (project->display_all_) {
            cols.push_back("*");
        } else {
            for (auto col : project->sel_cols_) {
                cols.push_back(col_to_string(col));
            }
        }
        std::sort(cols.begin(), cols.end());
        ss << indent << "Project(columns=[" << join_strings(cols) << "], rows=" << project->rows_ << ")\n";
        ss << explain_plan_to_string(project->subplan_, depth + 1);
    } else if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        std::vector<std::string> conds;
        for (auto cond : filter->conds_) {
            conds.push_back(cond_to_string(cond));
        }
        std::sort(conds.begin(), conds.end());
        ss << indent << "Filter(condition=[" << join_strings(conds) << "], rows=" << filter->rows_ << ")\n";
        ss << explain_plan_to_string(filter->subplan_, depth + 1);
    } else if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        std::vector<std::string> tables;
        auto table_set = plan_tables_for_print(plan);
        tables.assign(table_set.begin(), table_set.end());
        std::vector<std::string> conds;
        for (auto cond : join->conds_) {
            conds.push_back(cond_to_string(cond));
        }
        std::sort(conds.begin(), conds.end());
        ss << indent << "Join(tables=[" << join_strings(tables) << "], condition=[" << join_strings(conds)
           << "], rows=" << join->rows_ << ")\n";
        ss << explain_plan_to_string(join->left_, depth + 1);
        ss << explain_plan_to_string(join->right_, depth + 1);
    } else if (auto agg = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
        ss << indent << "Aggregate(rows=" << agg->rows_ << ")\n";
        ss << explain_plan_to_string(agg->subplan_, depth + 1);
    } else if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        ss << indent << "Scan(table=" << scan->real_tab_name_ << ", type=";
        if (scan->tag == T_IndexScan) {
            ss << "IndexScan, using_index=(" << join_strings(scan->index_col_names_) << ")";
        } else {
            ss << "SeqScan";
        }
        ss << ", rows=" << scan->rows_ << ")\n";
    } else if (auto sort = std::dynamic_pointer_cast<SortPlan>(plan)) {
        ss << explain_plan_to_string(sort->subplan_, depth);
    } else if (auto union_plan = std::dynamic_pointer_cast<UnionPlan>(plan)) {
        ss << indent << "Union(rows=" << union_plan->rows_ << ")\n";
        for (auto &subplan : union_plan->subplans_) {
            ss << explain_plan_to_string(subplan, depth + 1);
        }
    }
    return ss.str();
}

}  // namespace

const char *help_info = "Supported SQL syntax:\n"
                   "  command ;\n"
                   "command:\n"
                   "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                   "  DROP TABLE table_name\n"
                   "  CREATE INDEX table_name (column_name)\n"
                   "  DROP INDEX table_name (column_name)\n"
                   "  INSERT INTO table_name VALUES (value [, value ...])\n"
                   "  DELETE FROM table_name [WHERE where_clause]\n"
                   "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
                   "  SELECT selector FROM table_name [WHERE where_clause]\n"
                   "type:\n"
                   "  {INT | FLOAT | CHAR(n)}\n"
                   "where_clause:\n"
                   "  condition [AND condition ...]\n"
                   "condition:\n"
                   "  column op {column | value}\n"
                   "column:\n"
                   "  [table_name.]column_name\n"
                   "op:\n"
                   "  {= | <> | < | > | <= | >=}\n"
                   "selector:\n"
                   "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context){
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch(x->tag) {
            case T_CreateTable:
            {
                sm_manager_->create_table(x->tab_name_, x->cols_, context);
                break;
            }
            case T_DropTable:
            {
                sm_manager_->drop_table(x->tab_name_, context);
                break;
            }
            case T_CreateIndex:
            {
                sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            case T_DropIndex:
            {
                sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;  
        }
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch(x->tag) {
            case T_Help:
            {
                memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
                *(context->offset_) = strlen(help_info);
                break;
            }
            case T_ShowTable:
            {
                sm_manager_->show_tables(context);
                break;
            }
            case T_ShowIndex:
            {
                sm_manager_->show_index(x->tab_name_, context);
                break;
            }
            case T_DescTable:
            {
                sm_manager_->desc_table(x->tab_name_, context);
                break;
            }
            case T_Transaction_begin:
            {
                // 显示开启一个事务
                context->txn_->set_txn_mode(true);
                break;
            }  
            case T_Transaction_commit:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->commit(context->txn_, context->log_mgr_);
                *txn_id = INVALID_TXN_ID;
                break;
            }
            case T_Transaction_rollback:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_, context->log_mgr_);
                *txn_id = INVALID_TXN_ID;
                break;
            }
            case T_Transaction_abort:
            {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_, context->log_mgr_);
                *txn_id = INVALID_TXN_ID;
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;                        
        }

    } else if(auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
        switch (x->set_knob_type_)
        {
        case ast::SetKnobType::EnableNestLoop: {
            planner_->set_enable_nestedloop_join(x->bool_value_);
            break;
        }
        case ast::SetKnobType::EnableSortMerge: {
            planner_->set_enable_sortmerge_join(x->bool_value_);
            break;
        }
        default: {
            throw RMDBError("Not implemented!\n");
            break;
        }
        }
    } else if (auto x = std::dynamic_pointer_cast<SetTransactionIsolationPlan>(plan)) {
        if (context->session_isolation_level_ != nullptr) {
            *context->session_isolation_level_ =
                x->isolation_level_ == ast::TxnIsolationLevel::SnapshotIsolation
                    ? IsolationLevel::SNAPSHOT_ISOLATION
                    : IsolationLevel::SERIALIZABLE;
        }
    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, 
                            Context *context) {
    context->is_select_ = true;
    executorTreeRoot->beginTuple();

    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.col_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    bool write_output_file = output_file_enabled.load();
    if (write_output_file) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for(size_t i = 0; i < captions.size(); ++i) {
            outfile << " " << captions[i] << " |";
        }
        outfile << "\n";
    }

    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    for (; !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int *)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float *)rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char *)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            }
            columns.push_back(col_str);
        }
        // print record into buffer
        rec_printer.print_record(columns, context);
        // print record into file
        if (write_output_file) {
            outfile << "|";
            for(size_t i = 0; i < columns.size(); ++i) {
                outfile << " " << columns[i] << " |";
            }
            outfile << "\n";
        }
        num_rec++;
    }
    if (write_output_file) {
        outfile.close();
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
    context->is_select_ = false;
}

void QlManager::explain_analyze(std::shared_ptr<Plan> plan, std::unique_ptr<AbstractExecutor> executorTreeRoot,
                                Context *context) {
    auto dml = std::dynamic_pointer_cast<DMLPlan>(plan);
    std::shared_ptr<Plan> select_plan = dml == nullptr ? plan : dml->subplan_;

    executorTreeRoot->beginTuple();
    while (!executorTreeRoot->is_end()) {
        auto tuple = executorTreeRoot->Next();
        (void)tuple;
        executorTreeRoot->nextTuple();
    }
    collect_executor_rows(select_plan, executorTreeRoot.get());
    std::string output = explain_plan_to_string(select_plan);
    write_context(context, output);

    if (output_file_enabled.load()) {
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << output;
        outfile.close();
    }
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec){
    exec->Next();
}
