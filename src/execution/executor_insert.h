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
#include <utility>

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta *tab_;                  // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    std::unique_ptr<RmRecord> prebuilt_record_;
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = &sm_manager_->db_.get_table(tab_name);
        values_ = std::move(values);
        tab_name_ = tab_name;
        if (values_.size() != tab_->cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::unique_ptr<RmRecord> record,
                   Context *context) {
        sm_manager_ = sm_manager;
        tab_ = &sm_manager_->db_.get_table(tab_name);
        prebuilt_record_ = std::move(record);
        tab_name_ = tab_name;
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        std::unique_ptr<RmRecord> owned_record;
        RmRecord *rec_ptr = prebuilt_record_.get();
        if (rec_ptr == nullptr) {
            owned_record = std::make_unique<RmRecord>(fh_->get_file_hdr().record_size);
            rec_ptr = owned_record.get();
            for (size_t i = 0; i < values_.size(); i++) {
                auto &col = tab_->cols[i];
                auto &val = values_[i];
                if (col.type != val.type) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                val.init_raw(col.len);
                memcpy(rec_ptr->data + col.offset, val.raw->data, col.len);
            }
        }
        RmRecord &rec = *rec_ptr;
        bool mvcc_insert = context_->txn_mgr_ != nullptr && context_->txn_mgr_->IsMvccTxn(context_->txn_);
        bool track_serializable = need_serializable_tracking(context_);
        bool append_insert_fast_path = is_append_heavy_insert_table(*tab_, fh_, mvcc_insert);

        std::vector<std::pair<IxIndexHandle *, std::vector<char>>> index_entries;
        std::vector<std::string> index_lock_keys;
        index_entries.reserve(tab_->indexes.size());
        index_lock_keys.reserve(tab_->indexes.size());
        // The append fast path only changes how the heap record is written. Index-key
        // serialization and uniqueness checks remain mandatory for every insert.
        for (auto &index : tab_->indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            std::vector<char> key(index.col_tot_len);
            int offset = 0;
            for (auto &col : index.cols) {
                memcpy(key.data() + offset, rec.data + col.offset, col.len);
                offset += col.len;
            }
            index_lock_keys.push_back(make_index_lock_key(tab_name_, index, key));
            index_entries.emplace_back(ih, std::move(key));
        }

        std::shared_lock<std::shared_mutex> gc_lock;
        if (context_->txn_mgr_ != nullptr) {
            gc_lock = std::shared_lock<std::shared_mutex>(context_->txn_mgr_->GetMvccGcLatch());
        }
        auto key_locks = lock_index_keys(context_->txn_mgr_, index_lock_keys);

        if (mvcc_insert) {
            context_->txn_mgr_->CheckInsertConflict(tab_name_, rec, context_->txn_);
        }
        for (auto &entry : index_entries) {
            std::vector<Rid> existing;
            if (entry.first->get_value(entry.second.data(), &existing, context_->txn_)) {
                if (!mvcc_insert) {
                    throw UniqueIndexViolationError();
                }
                for (const auto &existing_rid : existing) {
                    context_->txn_mgr_->CheckWriteConflict(tab_name_, existing_rid, context_->txn_);
                    auto visible =
                        context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, existing_rid, context_->txn_);
                    if (visible != nullptr) {
                        throw UniqueIndexViolationError();
                    }
                }
            }
        }

        if (append_insert_fast_path) {
            PageId inserted_page_id{fh_->GetFd(), INVALID_PAGE_ID};
            std::unique_lock<std::mutex> write_lock;
            if (context_->txn_mgr_ != nullptr) {
                write_lock = std::unique_lock<std::mutex>(context_->txn_mgr_->GetTableWriteLatch(tab_name_));
            }
            rid_ = fh_->insert_record_pinned(rec.data, [&](const Rid &rid) {
                rid_ = rid;
                if (track_serializable) {
                    context_->txn_mgr_->CheckSerializableWrite(context_->txn_, tab_name_, rid_, nullptr, &rec,
                                                               tab_->cols);
                }
                if (context_->txn_ != nullptr && mvcc_insert) {
                    context_->txn_->append_write_record(WType::INSERT_TUPLE, tab_name_, rid_);
                }
                if (mvcc_insert) {
                    context_->txn_mgr_->RecordInsert(tab_name_, rid_, rec, context_->txn_);
                }
            }, &inserted_page_id);
            if (write_lock.owns_lock()) {
                write_lock.unlock();
            }

            try {
                if (context_->txn_ != nullptr && context_->log_mgr_ != nullptr) {
                    InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
                    log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                    lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                    context_->txn_->set_prev_lsn(lsn);
                }
                sm_manager_->get_bpm()->unpin_page(inserted_page_id, true);
            } catch (...) {
                sm_manager_->get_bpm()->unpin_page(inserted_page_id, true);
                throw;
            }
        } else {
            std::unique_lock<std::mutex> write_lock;
            if (context_->txn_mgr_ != nullptr) {
                write_lock = std::unique_lock<std::mutex>(context_->txn_mgr_->GetTableWriteLatch(tab_name_));
            }
            rid_ = fh_->insert_record(rec.data, context_, [&](const Rid &rid) {
                rid_ = rid;
                if (track_serializable) {
                    context_->txn_mgr_->CheckSerializableWrite(context_->txn_, tab_name_, rid_, nullptr, &rec,
                                                               tab_->cols);
                }
                if (context_->txn_ != nullptr && mvcc_insert) {
                    context_->txn_->append_write_record(WType::INSERT_TUPLE, tab_name_, rid_);
                }
                if (mvcc_insert) {
                    context_->txn_mgr_->RecordInsert(tab_name_, rid_, rec, context_->txn_);
                }
                if (context_->txn_ != nullptr && context_->log_mgr_ != nullptr) {
                    InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
                    log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                    lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                    context_->txn_->set_prev_lsn(lsn);
                }
            });
        }
        if (context_->txn_ != nullptr && !mvcc_insert) {
            context_->txn_->append_write_record(WType::INSERT_TUPLE, tab_name_, rid_);
        }

        // Insert into index
        for (auto &entry : index_entries) {
            entry.first->insert_entry(entry.second.data(), rid_, context_->txn_);
        }
        if (!mvcc_insert) {
            RuntimeFilterCache::Get().insert_record(tab_name_, rid_, rec, *tab_);
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }

   private:
    static bool is_append_heavy_insert_table(const TabMeta &tab, RmFileHandle *fh, bool mvcc_insert) {
        if (!mvcc_insert || fh == nullptr || tab.indexes.empty()) {
            return false;
        }

        RmFileHdr hdr = fh->get_file_hdr();
        if (hdr.num_pages < 16) {
            return false;
        }

        for (const auto &index : tab.indexes) {
            if (index.col_num < 2) {
                return false;
            }
            for (const auto &col : index.cols) {
                if (col.type != TYPE_INT) {
                    return false;
                }
            }
        }
        return true;
    }

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
        if (txn_mgr == nullptr) {
            return {};
        }
        return txn_mgr->LockIndexKeys(std::move(keys));
    }
};
