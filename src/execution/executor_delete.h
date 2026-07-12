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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        bool mvcc_delete = context_->txn_mgr_ != nullptr && context_->txn_mgr_->IsMvccTxn(context_->txn_);
        for (const auto &rid : rids_) {
            std::shared_lock<std::shared_mutex> gc_lock;
            std::unique_lock<std::mutex> write_lock;
            std::unique_lock<std::mutex> record_lock;
            if (context_->txn_mgr_ != nullptr) {
                gc_lock = std::shared_lock<std::shared_mutex>(context_->txn_mgr_->GetMvccGcLatch());
                if (!mvcc_delete) {
                    write_lock = std::unique_lock<std::mutex>(context_->txn_mgr_->GetTableWriteLatch(tab_name_));
                }
                record_lock = std::unique_lock<std::mutex>(
                    context_->txn_mgr_->GetRecordWriteLatch(tab_name_, rid));
            }
            if (context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->CheckWriteConflict(tab_name_, rid, context_->txn_);
            }
            std::unique_ptr<RmRecord> old_rec;
            if (context_->txn_mgr_ != nullptr) {
                old_rec = context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, rid, context_->txn_);
            } else {
                old_rec = fh_->get_record(rid, context_);
            }
            if (old_rec == nullptr) {
                continue;
            }
            std::vector<std::pair<IxIndexHandle *, std::vector<char>>> index_entries;
            std::vector<std::string> index_lock_keys;
            for (auto &index : tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                std::vector<char> key(index.col_tot_len);
                int offset = 0;
                for (auto &col : index.cols) {
                    memcpy(key.data() + offset, old_rec->data + col.offset, col.len);
                    offset += col.len;
                }
                index_lock_keys.push_back(make_index_lock_key(tab_name_, index, key));
                index_entries.emplace_back(ih, std::move(key));
            }
            auto key_locks = lock_index_keys(context_->txn_mgr_, index_lock_keys);
            if (context_->txn_ != nullptr) {
                context_->txn_->append_write_record(new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *old_rec));
            }
            if (context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->CheckSerializableWrite(context_->txn_, tab_name_, rid, old_rec.get(), nullptr,
                                                           tab_.cols);
                context_->txn_mgr_->RecordDelete(tab_name_, rid, *old_rec, context_->txn_);
            }
            if (context_->txn_ != nullptr && context_->log_mgr_ != nullptr) {
                Rid log_rid = rid;
                DeleteLogRecord log_record(context_->txn_->get_transaction_id(), *old_rec, log_rid, tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
            }
            for (auto &entry : index_entries) {
                entry.first->delete_entry(entry.second.data(), context_->txn_);
            }
            if (!mvcc_delete) {
                fh_->delete_record(rid, context_);
                RuntimeFilterCache::Get().delete_record(tab_name_, rid, *old_rec, tab_, false);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

   private:
    static std::string make_index_lock_key(const std::string &tab_name, const IndexMeta &index,
                                           const std::vector<char> &key) {
        std::string result = tab_name;
        result.push_back('\0');
        for (const auto &col : index.cols) {
            result.append(col.name);
            result.push_back('\0');
        }
        result.append(key.data(), key.size());
        return result;
    }

    static std::vector<std::unique_lock<std::mutex>> lock_index_keys(
        TransactionManager *txn_mgr, std::vector<std::string> keys) {
        std::vector<std::unique_lock<std::mutex>> locks;
        if (txn_mgr == nullptr || keys.empty()) {
            return locks;
        }
        std::sort(keys.begin(), keys.end());
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
        locks.reserve(keys.size());
        for (const auto &key : keys) {
            locks.emplace_back(txn_mgr->GetIndexKeyLatch(key));
        }
        return locks;
    }
};
