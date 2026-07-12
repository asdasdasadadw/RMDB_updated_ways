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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "runtime_filter_cache.h"
#include "system/sm.h"

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <utility>

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta *tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

    struct IndexPlan {
        const IndexMeta *meta;
        IxIndexHandle *handle;
        std::string lock_prefix;
    };

    struct UpdateAction {
        SetOp op;
        int dst_offset;
        int src_offset;
        int len;
        ColType dst_type;
        ColType src_type;
        const Value *value;
    };

    std::vector<IndexPlan> index_plans_;
    std::vector<UpdateAction> update_actions_;
    std::vector<FastCondition> fast_conds_;
    std::unordered_set<std::string> indexed_cols_;
    bool can_rebase_cached_ = false;
    bool touches_index_col_cached_ = false;

    static double read_number(const char *data, ColType type) {
        if (type == TYPE_INT) {
            return static_cast<double>(*reinterpret_cast<const int *>(data));
        }
        if (type == TYPE_FLOAT) {
            return static_cast<double>(*reinterpret_cast<const float *>(data));
        }
        throw IncompatibleTypeError("NUMERIC", coltype2str(type));
    }

    static void write_number(char *data, ColType type, double value) {
        if (type == TYPE_INT) {
            int int_value = static_cast<int>(value);
            memcpy(data, &int_value, sizeof(int));
            return;
        }
        if (type == TYPE_FLOAT) {
            float float_value = static_cast<float>(value);
            memcpy(data, &float_value, sizeof(float));
            return;
        }
        throw IncompatibleTypeError("NUMERIC", coltype2str(type));
    }

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = &sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
        collect_indexed_cols();
        build_update_actions();
        fast_conds_ = compile_conds(conds_, tab_->cols);
        touches_index_col_cached_ = touches_index_col();
        if (touches_index_col_cached_) {
            build_index_plans();
        }
        can_rebase_cached_ = can_rebase_update();
    }
    std::unique_ptr<RmRecord> Next() override {
        bool mvcc_update = context_->txn_mgr_ != nullptr && context_->txn_mgr_->IsMvccTxn(context_->txn_);
        bool can_rebase = mvcc_update && can_rebase_cached_;
        bool track_serializable = need_serializable_tracking(context_);
        for (const auto &rid : rids_) {
            std::shared_lock<std::shared_mutex> gc_lock;
            std::unique_lock<std::mutex> record_lock;
            for (;;) {
                if (context_->txn_mgr_ != nullptr) {
                    gc_lock = std::shared_lock<std::shared_mutex>(context_->txn_mgr_->GetMvccGcLatch());
                    record_lock = std::unique_lock<std::mutex>(
                        context_->txn_mgr_->GetRecordWriteLatch(tab_name_, rid));
                }
                bool wait = false;
                if (context_->txn_mgr_ != nullptr) {
                    wait = context_->txn_mgr_->CheckWriteConflict(tab_name_, rid, context_->txn_, can_rebase);
                }
                if (!wait) {
                    break;
                }
                record_lock.unlock();
                gc_lock.unlock();
                std::this_thread::yield();
            }
            std::unique_ptr<RmRecord> old_rec;
            if (can_rebase) {
                old_rec = fh_->get_record(rid, context_);
            } else if (context_->txn_mgr_ != nullptr) {
                old_rec = context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, rid, context_->txn_);
            } else {
                old_rec = fh_->get_record(rid, context_);
            }
            if (old_rec == nullptr || !eval_compiled_conds(fast_conds_, old_rec->data)) {
                continue;
            }
            RmRecord new_rec(*old_rec);

            for (const auto &action : update_actions_) {
                const auto &val = *action.value;
                if (action.op != SET_OP_VALUE) {
                    double lhs = read_number(old_rec->data + action.src_offset, action.src_type);
                    double rhs = read_number(val.raw->data, val.type);
                    double result = lhs;
                    switch (action.op) {
                        case SET_OP_ADD:
                            result = lhs + rhs;
                            break;
                        case SET_OP_SUB:
                            result = lhs - rhs;
                            break;
                        case SET_OP_MUL:
                            result = lhs * rhs;
                            break;
                        case SET_OP_DIV:
                            if (rhs == 0) {
                                throw RMDBError("Division by zero");
                            }
                            result = lhs / rhs;
                            break;
                        default:
                            throw InternalError("Unexpected update operator");
                    }
                    write_number(new_rec.data + action.dst_offset, action.dst_type, result);
                    continue;
                }
                memcpy(new_rec.data + action.dst_offset, val.raw->data, action.len);
            }

            std::vector<std::tuple<IxIndexHandle *, std::vector<char>, std::vector<char>>> index_entries;
            std::vector<std::string> index_lock_keys;
            index_entries.reserve(index_plans_.size());
            index_lock_keys.reserve(index_plans_.size() * 2);
            if (!can_rebase && touches_index_col_cached_) {
                for (auto &plan : index_plans_) {
                    const auto &index = *plan.meta;
                    std::vector<char> old_key(index.col_tot_len);
                    std::vector<char> new_key(index.col_tot_len);
                    int offset = 0;
                    for (auto &col : index.cols) {
                        memcpy(old_key.data() + offset, old_rec->data + col.offset, col.len);
                        memcpy(new_key.data() + offset, new_rec.data + col.offset, col.len);
                        offset += col.len;
                    }
                    if (memcmp(old_key.data(), new_key.data(), index.col_tot_len) == 0) {
                        continue;
                    }
                    index_lock_keys.push_back(make_index_lock_key(plan.lock_prefix, old_key));
                    index_lock_keys.push_back(make_index_lock_key(plan.lock_prefix, new_key));
                    index_entries.emplace_back(plan.handle, std::move(old_key), std::move(new_key));
                }
            }
            auto key_locks = lock_index_keys(context_->txn_mgr_, index_lock_keys);
            if (!index_entries.empty() && mvcc_update) {
                context_->txn_mgr_->CheckInsertConflict(tab_name_, new_rec, context_->txn_, &rid);
            }
            for (auto &entry : index_entries) {
                auto ih = std::get<0>(entry);
                auto &new_key = std::get<2>(entry);
                std::vector<Rid> existing;
                if (ih->get_value(new_key.data(), &existing, context_->txn_)) {
                    if (!mvcc_update) {
                        throw UniqueIndexViolationError();
                    }
                    for (const auto &existing_rid : existing) {
                        if (existing_rid == rid) {
                            continue;
                        }
                        context_->txn_mgr_->CheckWriteConflict(tab_name_, existing_rid, context_->txn_);
                        auto visible =
                            context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, existing_rid, context_->txn_);
                        if (visible != nullptr) {
                            throw UniqueIndexViolationError();
                        }
                    }
                }
            }

            if (context_->txn_ != nullptr) {
                context_->txn_->append_write_record(WType::UPDATE_TUPLE, tab_name_, rid, *old_rec);
            }
            if (context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->RecordUpdate(tab_name_, rid, *old_rec, new_rec, context_->txn_);
                if (track_serializable) {
                    context_->txn_mgr_->CheckSerializableWrite(context_->txn_, tab_name_, rid, old_rec.get(), &new_rec,
                                                               tab_->cols);
                }
            }
            if (context_->txn_ != nullptr && context_->log_mgr_ != nullptr) {
                Rid log_rid = rid;
                UpdateLogRecord log_record(context_->txn_->get_transaction_id(), *old_rec, new_rec, log_rid,
                                           tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
            }

            fh_->update_record(rid, new_rec.data, context_);

            for (auto &entry : index_entries) {
                auto ih = std::get<0>(entry);
                auto &old_key = std::get<1>(entry);
                auto &new_key = std::get<2>(entry);
                ih->delete_entry(old_key.data(), context_->txn_);
                ih->insert_entry(new_key.data(), rid, context_->txn_);
            }
            if (!mvcc_update) {
                RuntimeFilterCache::Get().update_record(tab_name_, rid, *old_rec, new_rec, *tab_, true);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

   private:
    void collect_indexed_cols() {
        indexed_cols_.clear();
        for (const auto &index : tab_->indexes) {
            for (const auto &col : index.cols) {
                indexed_cols_.insert(col.name);
            }
        }
    }

    void build_index_plans() {
        index_plans_.clear();
        index_plans_.reserve(tab_->indexes.size());
        for (const auto &index : tab_->indexes) {
            auto ih = sm_manager_->ihs_.at(
                sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            IndexPlan plan;
            plan.meta = &index;
            plan.handle = ih;
            plan.lock_prefix = make_index_lock_prefix(tab_name_, index);
            index_plans_.push_back(std::move(plan));
        }
    }

    void build_update_actions() {
        update_actions_.clear();
        update_actions_.reserve(set_clauses_.size());
        for (auto &set_clause : set_clauses_) {
            auto dst_col = tab_->get_col(set_clause.lhs.col_name);
            UpdateAction action;
            action.op = set_clause.op;
            action.dst_offset = dst_col->offset;
            action.src_offset = 0;
            action.len = dst_col->len;
            action.dst_type = dst_col->type;
            action.src_type = dst_col->type;
            action.value = &set_clause.rhs;
            if (set_clause.op == SET_OP_VALUE) {
                if (dst_col->type != set_clause.rhs.type) {
                    throw IncompatibleTypeError(coltype2str(dst_col->type), coltype2str(set_clause.rhs.type));
                }
                if (set_clause.rhs.raw == nullptr) {
                    set_clause.rhs.init_raw(dst_col->len);
                }
            } else {
                auto src_col = tab_->get_col(set_clause.rhs_col.col_name);
                action.src_offset = src_col->offset;
                action.src_type = src_col->type;
                if (set_clause.rhs.raw == nullptr) {
                    int rhs_len = set_clause.rhs.type == TYPE_INT ? static_cast<int>(sizeof(int)) :
                                  set_clause.rhs.type == TYPE_FLOAT ? static_cast<int>(sizeof(float)) :
                                  dst_col->len;
                    set_clause.rhs.init_raw(rhs_len);
                }
            }
            update_actions_.push_back(action);
        }
    }

    bool can_rebase_update() {
        if (set_clauses_.empty()) {
            return false;
        }
        std::unordered_set<std::string> updated_cols;
        for (const auto &set_clause : set_clauses_) {
            if (set_clause.op != SET_OP_ADD && set_clause.op != SET_OP_SUB) {
                return false;
            }
            if (set_clause.lhs.tab_name != set_clause.rhs_col.tab_name ||
                set_clause.lhs.col_name != set_clause.rhs_col.col_name) {
                return false;
            }
            auto col = tab_->get_col(set_clause.lhs.col_name);
            if (col->type != TYPE_INT && col->type != TYPE_FLOAT) {
                return false;
            }
            if (indexed_col(set_clause.lhs.col_name)) {
                return false;
            }
            updated_cols.insert(set_clause.lhs.col_name);
        }
        return rebase_conditions_stable(updated_cols);
    }

    bool is_this_table_col(const TabCol &col) const {
        return col.tab_name.empty() || col.tab_name == tab_name_;
    }

    bool indexed_col(const std::string &col_name) const {
        return indexed_cols_.find(col_name) != indexed_cols_.end();
    }

    bool touches_index_col() const {
        for (const auto &set_clause : set_clauses_) {
            if (indexed_col(set_clause.lhs.col_name)) {
                return true;
            }
        }
        return false;
    }

    bool rebase_condition_col_ok(const TabCol &col, const std::unordered_set<std::string> &updated_cols) const {
        if (!is_this_table_col(col)) {
            return true;
        }
        return updated_cols.find(col.col_name) == updated_cols.end();
    }

    bool rebase_conditions_stable(const std::unordered_set<std::string> &updated_cols) const {
        if (conds_.empty()) {
            return false;
        }
        for (const auto &cond : conds_) {
            if (is_this_table_col(cond.lhs_col) && (cond.op != OP_EQ || !cond.is_rhs_val)) {
                return false;
            }
            if (!rebase_condition_col_ok(cond.lhs_col, updated_cols)) {
                return false;
            }
            if (!cond.is_rhs_val) {
                if (is_this_table_col(cond.rhs_col)) {
                    return false;
                }
                if (!rebase_condition_col_ok(cond.rhs_col, updated_cols)) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string make_index_lock_prefix(const std::string &tab_name, const IndexMeta &index) {
        std::string result = tab_name;
        result.push_back('\0');
        for (const auto &col : index.cols) {
            result.append(col.name);
            result.push_back('\0');
        }
        return result;
    }

    static std::string make_index_lock_key(const std::string &prefix, const std::vector<char> &key) {
        std::string result = prefix;
        result.append(key.data(), key.size());
        return result;
    }

    static std::vector<std::unique_lock<std::mutex>> lock_index_keys(
        TransactionManager *txn_mgr, std::vector<std::string> keys) {
        if (txn_mgr == nullptr) {
            return {};
        }
        return txn_mgr->LockIndexKeys(std::move(keys));
    }
};
