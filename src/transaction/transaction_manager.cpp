/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <vector>

#include "errors.h"
#include "execution/runtime_filter_cache.h"
#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

namespace {

constexpr size_t MVCC_MATERIALIZE_BUDGET = 512;
constexpr timestamp_t MVCC_MATERIALIZE_INTERVAL = 32;

std::vector<char> make_index_key(const RmRecord &rec, const IndexMeta &index) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (const auto &col : index.cols) {
        memcpy(key.data() + offset, rec.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

std::string make_index_lock_key(const std::string &tab_name, const IndexMeta &index,
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

void append_index_lock_keys(const std::string &tab_name, const TabMeta &tab, const RmRecord &rec,
                            std::vector<std::string> *keys) {
    if (keys == nullptr) {
        return;
    }
    for (const auto &index : tab.indexes) {
        auto key = make_index_key(rec, index);
        keys->push_back(make_index_lock_key(tab_name, index, key));
    }
}

std::vector<std::unique_lock<std::mutex>> lock_index_keys(TransactionManager *txn_mgr,
                                                         std::vector<std::string> keys) {
    if (txn_mgr == nullptr) {
        return {};
    }
    return txn_mgr->LockIndexKeys(std::move(keys));
}

bool index_entry_points_to_rid(IxIndexHandle *ih, const std::vector<char> &key, const Rid &rid,
                               Transaction *txn) {
    std::vector<Rid> values;
    return ih->get_value(key.data(), &values, txn) &&
           std::find(values.begin(), values.end(), rid) != values.end();
}

void ensure_index_entry_for_rid(IxIndexHandle *ih, const std::vector<char> &key, const Rid &rid,
                                Transaction *txn) {
    std::vector<Rid> values;
    if (ih->get_value(key.data(), &values, txn)) {
        if (std::find(values.begin(), values.end(), rid) != values.end()) {
            return;
        }
        throw InternalError("Index rollback key belongs to a different record");
    }

    page_id_t inserted_page = ih->insert_entry(key.data(), rid, txn);
    bool restored = index_entry_points_to_rid(ih, key, rid, txn);
    if (inserted_page == IX_NO_PAGE && !restored) {
        throw InternalError("Index rollback insert was rejected");
    }
    if (!restored) {
        throw InternalError("Failed to restore index entry during rollback");
    }
}

void delete_index_entry_for_rid(IxIndexHandle *ih, const std::vector<char> &key, const Rid &rid,
                                Transaction *txn) {
    if (index_entry_points_to_rid(ih, key, rid, txn)) {
        ih->delete_entry(key.data(), txn);
        if (index_entry_points_to_rid(ih, key, rid, txn)) {
            throw InternalError("Failed to remove index entry during rollback");
        }
    }
}

void validate_index_entries_restorable(SmManager *sm_manager, const TabMeta &tab,
                                       const std::string &tab_name, const RmRecord &rec,
                                       const Rid &rid, Transaction *txn) {
    for (const auto &index : tab.indexes) {
        auto ih = sm_manager->ihs_.at(
            sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        auto key = make_index_key(rec, index);
        std::vector<Rid> values;
        if (ih->get_value(key.data(), &values, txn) &&
            std::find(values.begin(), values.end(), rid) == values.end()) {
            throw InternalError("Index rollback key belongs to a different record");
        }
    }
}

void insert_indexes(SmManager *sm_manager, const TabMeta &tab, const std::string &tab_name,
                    const RmRecord &rec, const Rid &rid, Transaction *txn) {
    for (const auto &index : tab.indexes) {
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        auto key = make_index_key(rec, index);
        ensure_index_entry_for_rid(ih, key, rid, txn);
    }
}

void delete_indexes_for_rid(SmManager *sm_manager, const TabMeta &tab, const std::string &tab_name,
                            const RmRecord &rec, const Rid &rid, Transaction *txn) {
    for (const auto &index : tab.indexes) {
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        auto key = make_index_key(rec, index);
        delete_index_entry_for_rid(ih, key, rid, txn);
    }
}

int compare_raw_value(const char *lhs, const char *rhs, ColType type, int len) {
    if (type == TYPE_INT) {
        int l = *reinterpret_cast<const int *>(lhs);
        int r = *reinterpret_cast<const int *>(rhs);
        return (l > r) - (l < r);
    }
    if (type == TYPE_FLOAT) {
        float l = *reinterpret_cast<const float *>(lhs);
        float r = *reinterpret_cast<const float *>(rhs);
        return (l > r) - (l < r);
    }
    if (type == TYPE_STRING) {
        return memcmp(lhs, rhs, len);
    }
    throw InternalError("Unexpected column type");
}

bool compare_with_op(int cmp, CompOp op) {
    switch (op) {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        default:
            throw InternalError("Unexpected comparison operator");
    }
}

bool eval_cond(const Condition &cond, const std::vector<ColMeta> &cols, const char *data) {
    auto lhs_col = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
        return col.tab_name == cond.lhs_col.tab_name && col.name == cond.lhs_col.col_name;
    });
    if (lhs_col == cols.end()) {
        return false;
    }
    const char *lhs = data + lhs_col->offset;
    const char *rhs = nullptr;
    if (cond.is_rhs_val) {
        if (cond.rhs_val.raw == nullptr) {
            return false;
        }
        rhs = cond.rhs_val.raw->data;
    } else {
        auto rhs_col = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
            return col.tab_name == cond.rhs_col.tab_name && col.name == cond.rhs_col.col_name;
        });
        if (rhs_col == cols.end()) {
            return false;
        }
        rhs = data + rhs_col->offset;
    }
    return compare_with_op(compare_raw_value(lhs, rhs, lhs_col->type, lhs_col->len), cond.op);
}

bool eval_conds(const std::vector<Condition> &conds, const std::vector<ColMeta> &cols, const char *data) {
    for (const auto &cond : conds) {
        if (!eval_cond(cond, cols, data)) {
            return false;
        }
    }
    return true;
}

bool same_index_key(const RmRecord &lhs, const RmRecord &rhs, const IndexMeta &index) {
    for (const auto &col : index.cols) {
        if (memcmp(lhs.data + col.offset, rhs.data + col.offset, col.len) != 0) {
            return false;
        }
    }
    return true;
}

bool index_key_changed(const RmRecord &lhs, const RmRecord &rhs, const IndexMeta &index) {
    for (const auto &col : index.cols) {
        if (memcmp(lhs.data + col.offset, rhs.data + col.offset, col.len) != 0) {
            return true;
        }
    }
    return false;
}

std::string index_name_for_version_map(const IndexMeta &index) {
    std::string name;
    for (const auto &col : index.cols) {
        name.append(col.name);
        name.push_back('\0');
    }
    return name;
}

std::string index_key_to_string(const RmRecord &rec, const IndexMeta &index) {
    auto key = make_index_key(rec, index);
    return std::string(key.data(), key.size());
}

std::string index_group_name(const std::string &tab_name, const std::string &index_name) {
    std::string group = tab_name;
    group.push_back('\0');
    group.append(index_name);
    return group;
}

std::string version_filter_key_from_record(const std::vector<ColMeta> &cols, const RmRecord &record) {
    std::string key;
    for (const auto &col : cols) {
        key.append(record.data + col.offset, col.len);
    }
    return key;
}

bool version_filter_key_from_conditions(const std::vector<ColMeta> &cols,
                                        const std::string &visible_tab,
                                        const std::vector<Condition> &conds,
                                        std::string *key) {
    if (key == nullptr) {
        return false;
    }
    key->clear();
    for (const auto &col : cols) {
        const Condition *match = nullptr;
        for (const auto &cond : conds) {
            if (!cond.is_rhs_val || cond.op != OP_EQ || cond.rhs_val.raw == nullptr) {
                continue;
            }
            if (!cond.lhs_col.tab_name.empty() && cond.lhs_col.tab_name != visible_tab) {
                continue;
            }
            if (cond.lhs_col.col_name == col.name) {
                match = &cond;
                break;
            }
        }
        if (match == nullptr) {
            return false;
        }
        key->append(match->rhs_val.raw->data, col.len);
    }
    return true;
}

}  // namespace

void TransactionManager::AddVersionKeyLocked(const RecordKey &key) {
    table_version_keys_[key.tab_name].insert(key);
}

void TransactionManager::RemoveVersionKeyLocked(const RecordKey &key) {
    auto it = table_version_keys_.find(key.tab_name);
    if (it == table_version_keys_.end()) {
        return;
    }
    it->second.erase(key);
    if (it->second.empty()) {
        table_version_keys_.erase(it);
    }
    RemoveRecordIndexKeysLocked(key);
    RemoveVersionFilterKeysLocked(key);
}

void TransactionManager::AddRecordIndexKeysLocked(const RecordKey &key, const RmRecord &record,
                                                  const TabMeta &tab, bool add_to_scan) {
    if (tab.indexes.empty()) {
        return;
    }
    auto &record_keys = record_index_keys_[key];
    for (const auto &index : tab.indexes) {
        IndexRecordKey index_key{key.tab_name, index_name_for_version_map(index),
                                 index_key_to_string(record, index)};
        auto &keys = index_version_keys_[index_key];
        if (keys.insert(key).second) {
            version_index_key_count_.fetch_add(1, std::memory_order_relaxed);
        }
        if (add_to_scan) {
            index_group_keys_[index_group_name(index_key.tab_name, index_key.index_name)].insert(index_key);
        }
        record_keys.insert(std::move(index_key));
    }
}

void TransactionManager::RemoveRecordIndexKeysLocked(const RecordKey &key) {
    auto record_it = record_index_keys_.find(key);
    if (record_it == record_index_keys_.end()) {
        return;
    }
    for (const auto &index_key : record_it->second) {
        auto index_it = index_version_keys_.find(index_key);
        if (index_it == index_version_keys_.end()) {
            continue;
        }
        if (index_it->second.erase(key) > 0) {
            version_index_key_count_.fetch_sub(1, std::memory_order_relaxed);
        }
        if (index_it->second.empty()) {
            index_version_keys_.erase(index_it);
            auto group_it = index_group_keys_.find(index_group_name(index_key.tab_name, index_key.index_name));
            if (group_it != index_group_keys_.end()) {
                group_it->second.erase(index_key);
                if (group_it->second.empty()) {
                    index_group_keys_.erase(group_it);
                }
            }
        }
    }
    record_index_keys_.erase(record_it);
}

std::string TransactionManager::VersionFilterSignatureLocked(const std::string &tab_name,
                                                             const std::vector<ColMeta> &cols) const {
    std::string sig = tab_name;
    sig.push_back('\0');
    for (const auto &col : cols) {
        sig.append(col.name);
        sig.push_back('\0');
    }
    return sig;
}

TransactionManager::VersionFilterIndex *TransactionManager::EnsureVersionFilterIndexLocked(
    const std::string &tab_name, const std::vector<ColMeta> &cols) {
    std::string sig = VersionFilterSignatureLocked(tab_name, cols);
    auto it = version_filter_indexes_.find(sig);
    if (it != version_filter_indexes_.end()) {
        return &it->second;
    }

    VersionFilterIndex index;
    index.tab_name = tab_name;
    index.cols = cols;
    auto inserted = version_filter_indexes_.emplace(sig, std::move(index));
    table_version_filter_indexes_[tab_name].push_back(sig);
    VersionFilterIndex *built = &inserted.first->second;
    for (const auto &entry : versions_) {
        if (entry.first.tab_name != tab_name) {
            continue;
        }
        for (const auto &version : entry.second) {
            std::string filter_key = version_filter_key_from_record(built->cols, version.record);
            auto &entry_keys = record_filter_keys_[entry.first];
            if (!entry_keys.insert(VersionFilterEntryKey{sig, filter_key}).second) {
                continue;
            }
            built->rows[filter_key].insert(entry.first);
        }
    }
    return built;
}

void TransactionManager::AddVersionFilterRecordLocked(const RecordKey &key, const RmRecord &record) {
    auto table_it = table_version_filter_indexes_.find(key.tab_name);
    if (table_it == table_version_filter_indexes_.end()) {
        return;
    }
    for (const auto &sig : table_it->second) {
        auto index_it = version_filter_indexes_.find(sig);
        if (index_it == version_filter_indexes_.end()) {
            continue;
        }
        std::string filter_key = version_filter_key_from_record(index_it->second.cols, record);
        auto &entry_keys = record_filter_keys_[key];
        if (!entry_keys.insert(VersionFilterEntryKey{sig, filter_key}).second) {
            continue;
        }
        index_it->second.rows[filter_key].insert(key);
    }
}

void TransactionManager::RemoveVersionFilterKeysLocked(const RecordKey &key) {
    auto record_it = record_filter_keys_.find(key);
    if (record_it == record_filter_keys_.end()) {
        return;
    }
    for (const auto &entry_key : record_it->second) {
        auto index_it = version_filter_indexes_.find(entry_key.signature);
        if (index_it == version_filter_indexes_.end()) {
            continue;
        }
        auto row_it = index_it->second.rows.find(entry_key.key);
        if (row_it == index_it->second.rows.end()) {
            continue;
        }
        row_it->second.erase(key);
        if (row_it->second.empty()) {
            index_it->second.rows.erase(row_it);
        }
    }
    record_filter_keys_.erase(record_it);
}

std::unordered_set<TransactionManager::RecordKey, TransactionManager::RecordKeyHash>
TransactionManager::CollectIndexVersionKeysLocked(const std::string &tab_name, const IndexMeta &index,
                                                  const std::vector<char> &lower_key, bool lower_open,
                                                  const std::vector<char> &upper_key, bool upper_open) {
    std::unordered_set<RecordKey, RecordKeyHash> result;
    std::string index_name = index_name_for_version_map(index);
    if (!lower_key.empty() && lower_key.size() == upper_key.size() &&
        lower_key.size() == static_cast<size_t>(index.col_tot_len) &&
        memcmp(lower_key.data(), upper_key.data(), lower_key.size()) == 0 && !lower_open && !upper_open) {
        IndexRecordKey key{tab_name, index_name, std::string(lower_key.data(), lower_key.size())};
        auto it = index_version_keys_.find(key);
        if (it != index_version_keys_.end()) {
            result.insert(it->second.begin(), it->second.end());
        }
        return result;
    }

    auto compare_bound = [&](const std::string &key, const std::vector<char> &bound) {
        int offset = 0;
        for (const auto &col : index.cols) {
            int cmp = ix_compare(key.data() + offset, bound.data() + offset, col.type, col.len);
            if (cmp != 0) {
                return cmp;
            }
            offset += col.len;
        }
        return 0;
    };

    auto group_it = index_group_keys_.find(index_group_name(tab_name, index_name));
    if (group_it == index_group_keys_.end()) {
        return result;
    }
    for (const auto &index_key : group_it->second) {
        if (!lower_key.empty()) {
            int cmp = compare_bound(index_key.key, lower_key);
            if (cmp < 0 || (cmp == 0 && lower_open)) {
                continue;
            }
        }
        if (!upper_key.empty()) {
            int cmp = compare_bound(index_key.key, upper_key);
            if (cmp > 0 || (cmp == 0 && upper_open)) {
                continue;
            }
        }
        auto keys_it = index_version_keys_.find(index_key);
        if (keys_it != index_version_keys_.end()) {
            result.insert(keys_it->second.begin(), keys_it->second.end());
        }
    }
    return result;
}

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager, IsolationLevel isolation_level) {
    (void)log_manager;
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++, isolation_level);
    } else {
        txn->set_isolation_level(isolation_level);
    }
    txn->set_state(TransactionState::GROWING);
    {
        std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
        timestamp_t read_ts = last_commit_ts_.load();
        txn->set_start_ts(read_ts);
        txn->set_read_ts(read_ts);
        txn->set_commit_ts(INVALID_TS);
        TransactionManager::txn_map[txn->get_transaction_id()] = txn;
    }

    if (IsSerializableTxn(txn)) {
        std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
        auto &info = serializable_txns_[txn->get_transaction_id()];
        info.start_ts = txn->get_start_ts();
        info.commit_ts = INVALID_TS;
        info.state = TransactionState::GROWING;
        info.record_reads.clear();
        info.predicate_reads.clear();
        info.rw_in.clear();
        info.rw_out.clear();
    }
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::ABORTED) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTING) {
        throw InternalError("Transaction commit outcome is pending");
    }
    auto write_set = txn->get_write_set();
    if ((write_set == nullptr || write_set->empty()) && !IsSerializableTxn(txn)) {
        txn->set_state(TransactionState::COMMITTED);
        std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
        TransactionManager::txn_map.erase(txn->get_transaction_id());
        return;
    }
    std::vector<RecordKey> touched_keys;
    auto add_touched_key = [&](RecordKey key) {
        for (const auto &existing : touched_keys) {
            if (existing == key) {
                return;
            }
        }
        touched_keys.push_back(std::move(key));
    };
    if (write_set != nullptr) {
        touched_keys.reserve(write_set->size());
        for (auto &write_record : *write_set) {
            add_touched_key(RecordKey{write_record.GetTableName(), write_record.GetRid()});
        }
    }

    struct CommitCacheTableContext {
        TabMeta *tab{nullptr};
        RmFileHandle *fh{nullptr};
        bool has_cache{false};
    };
    std::unordered_map<std::string, CommitCacheTableContext> cache_table_contexts;
    auto get_cache_table_context = [&](const std::string &tab_name) -> CommitCacheTableContext & {
        auto it = cache_table_contexts.find(tab_name);
        if (it != cache_table_contexts.end()) {
            return it->second;
        }
        CommitCacheTableContext context;
        context.tab = &sm_manager_->db_.get_table(tab_name);
        context.has_cache = RuntimeFilterCache::Get().has_table_entries(tab_name);
        auto [insert_it, _] = cache_table_contexts.emplace(tab_name, context);
        return insert_it->second;
    };

    // Before publishing the commit, add every new cache candidate without
    // removing the old one.  Readers can safely filter these false positives
    // through MVCC, while a reader that starts after publication can never
    // miss the new key even if materialization runs immediately afterwards.
    if (IsMvccTxn(txn) && write_set != nullptr) {
        cache_table_contexts.reserve(write_set->size());
        for (auto &write_record : *write_set) {
            const std::string &tab_name = write_record.GetTableName();
            auto &table_context = get_cache_table_context(tab_name);
            if (!table_context.has_cache) {
                continue;
            }
            Rid rid = write_record.GetRid();
            switch (write_record.GetWriteType()) {
                case WType::INSERT_TUPLE:
                    if (write_record.GetRecord().allocated_) {
                        RuntimeFilterCache::Get().insert_record(tab_name, rid, write_record.GetRecord(),
                                                                *table_context.tab);
                    } else {
                        if (table_context.fh == nullptr) {
                            table_context.fh = sm_manager_->fhs_.at(tab_name).get();
                        }
                        auto new_rec = table_context.fh->get_record(rid, nullptr);
                        RuntimeFilterCache::Get().insert_record(tab_name, rid, *new_rec, *table_context.tab);
                    }
                    break;
                case WType::UPDATE_TUPLE: {
                    if (table_context.fh == nullptr) {
                        table_context.fh = sm_manager_->fhs_.at(tab_name).get();
                    }
                    auto new_rec = table_context.fh->get_record(rid, nullptr);
                    RuntimeFilterCache::Get().update_record(tab_name, rid, write_record.GetRecord(), *new_rec,
                                                            *table_context.tab, true);
                    break;
                }
                case WType::DELETE_TUPLE:
                default:
                    break;
            }
        }
    }

    if (IsSerializableTxn(txn)) {
        std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
        auto info_it = serializable_txns_.find(txn->get_transaction_id());
        if (info_it != serializable_txns_.end()) {
            info_it->second.state = TransactionState::COMMITTING;
        }
        txn->set_state(TransactionState::COMMITTING);
    } else {
        txn->set_state(TransactionState::COMMITTING);
    }

    // From the moment the COMMIT record enters the shared WAL buffer another
    // group flusher may make it durable.  COMMITTING is therefore an
    // irreversible state: no later error may route this transaction to abort.
    if (log_manager != nullptr && txn->get_prev_lsn() != INVALID_LSN) {
        CommitLogRecord log_record(txn->get_transaction_id());
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn;
        try {
            lsn = log_manager->add_log_to_buffer(&log_record);
        } catch (...) {
            std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
            auto info_it = serializable_txns_.find(txn->get_transaction_id());
            if (info_it != serializable_txns_.end()) {
                info_it->second.state = TransactionState::GROWING;
            }
            txn->set_state(TransactionState::GROWING);
            throw;
        }
        txn->set_prev_lsn(lsn);
        log_manager->flush_log_to_disk(true);
    }

    timestamp_t commit_ts;
    {
        std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
        commit_ts = last_commit_ts_.load() + 1;
        txn->set_commit_ts(commit_ts);
        txn_id_t txn_id = txn->get_transaction_id();
        for (const auto &key : touched_keys) {
            auto it = versions_.find(key);
            if (it == versions_.end()) {
                continue;
            }
            for (auto &version : it->second) {
                if (version.writer_txn == txn_id && !version.committed) {
                    version.committed = true;
                    version.commit_ts = commit_ts;
                }
            }
        }
        auto info_it = serializable_txns_.find(txn->get_transaction_id());
        if (info_it != serializable_txns_.end()) {
            info_it->second.state = TransactionState::COMMITTED;
            info_it->second.commit_ts = commit_ts;
        }
        last_commit_ts_.store(commit_ts);
        txn->set_state(TransactionState::COMMITTED);
    }

    // COMMIT is durable and the core MVCC state is published.  Everything
    // below is maintenance only: a cache or GC failure must never reverse the
    // transaction outcome or escape as a client-visible commit failure.
    try {
        if (IsMvccTxn(txn) && write_set != nullptr) {
            for (auto &write_record : *write_set) {
                const std::string &tab_name = write_record.GetTableName();
                auto &table_context = cache_table_contexts.at(tab_name);
                if (!table_context.has_cache) {
                    continue;
                }
                Rid rid = write_record.GetRid();
                switch (write_record.GetWriteType()) {
                    case WType::INSERT_TUPLE:
                        break;
                    case WType::UPDATE_TUPLE: {
                        if (table_context.fh == nullptr) {
                            table_context.fh = sm_manager_->fhs_.at(tab_name).get();
                        }
                        try {
                            auto new_rec = table_context.fh->get_record(rid, nullptr);
                            RuntimeFilterCache::Get().update_record(tab_name, rid, write_record.GetRecord(),
                                                                    *new_rec, *table_context.tab, false);
                        } catch (RecordNotFoundError &) {
                        }
                        break;
                    }
                    case WType::DELETE_TUPLE:
                        RuntimeFilterCache::Get().delete_record(tab_name, rid, write_record.GetRecord(),
                                                                *table_context.tab, false);
                        break;
                    default:
                        break;
                }
            }
        }
    } catch (...) {
        // The pre-publish pass already installed every new candidate.  A
        // cleanup failure only leaves false positives, which MVCC visibility
        // and predicate checks filter out safely.
    }
    if (write_set != nullptr) {
        write_set->clear();
    }

    try {
        std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
        TransactionManager::txn_map.erase(txn->get_transaction_id());
    } catch (...) {
    }

    bool should_materialize = false;
    try {
        std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
        for (const auto &key : touched_keys) {
            EnqueueMaterializeKeyLocked(key);
        }
        should_materialize = !materialize_queue_.empty() &&
                             commit_ts % MVCC_MATERIALIZE_INTERVAL == 0;
    } catch (...) {
        should_materialize = false;
    }
    if (should_materialize) {
        try {
            timestamp_t materialize_ts = OldestActiveStartTsLocked(txn->get_transaction_id());
            std::shared_lock<std::shared_mutex> gc_read_lock(mvcc_gc_latch_);
            bool needs_gc = false;
            {
                std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
                needs_gc = !materialize_queue_.empty();
            }
            gc_read_lock.unlock();
            if (needs_gc) {
                std::unique_lock<std::shared_mutex> gc_lock(mvcc_gc_latch_);
                std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
                if (!materialize_queue_.empty()) {
                    MaterializeCommittedVersionsLocked(materialize_ts, MVCC_MATERIALIZE_BUDGET);
                }
            }
        } catch (...) {
        }
    }
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTING ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::ABORTED) {
        return;
    }
    std::vector<RecordKey> touched_keys;
    auto add_touched_key = [&](RecordKey key) {
        for (const auto &existing : touched_keys) {
            if (existing == key) {
                return;
            }
        }
        touched_keys.push_back(std::move(key));
    };
    std::vector<RecordKey> deferred_materialize_keys;
    std::unique_lock<std::shared_mutex> gc_lock(mvcc_gc_latch_);
    auto write_set = txn->get_write_set();
    if (write_set != nullptr) {
        touched_keys.reserve(write_set->size());
        deferred_materialize_keys.reserve(write_set->size());
    }
    struct TableContext {
        TabMeta *tab{nullptr};
        RmFileHandle *fh{nullptr};
        bool has_cache{false};
    };
    std::unordered_map<std::string, TableContext> table_contexts;
    if (write_set != nullptr) {
        table_contexts.reserve(write_set->size());
    }
    auto get_table_context = [&](const std::string &tab_name) -> TableContext & {
        auto it = table_contexts.find(tab_name);
        if (it != table_contexts.end()) {
            return it->second;
        }
        TableContext context;
        context.tab = &sm_manager_->db_.get_table(tab_name);
        context.fh = sm_manager_->fhs_.at(tab_name).get();
        context.has_cache = RuntimeFilterCache::Get().has_table_entries(tab_name);
        auto [insert_it, _] = table_contexts.emplace(tab_name, context);
        return insert_it->second;
    };
    while (write_set != nullptr && !write_set->empty()) {
        WriteRecord &write_record = write_set->back();

        const std::string &tab_name = write_record.GetTableName();
        auto &table_context = get_table_context(tab_name);
        TabMeta &tab = *table_context.tab;
        RmFileHandle *fh = table_context.fh;
        Rid rid = write_record.GetRid();
        add_touched_key(RecordKey{tab_name, rid});
        std::unique_lock<std::mutex> table_lock(GetTableWriteLatch(tab_name));
        std::unique_lock<std::mutex> record_lock(GetRecordWriteLatch(tab_name, rid));

        switch (write_record.GetWriteType()) {
            case WType::INSERT_TUPLE: {
                std::unique_ptr<RmRecord> rec;
                try {
                    rec = fh->get_record(rid, nullptr);
                } catch (RecordNotFoundError &) {
                    if (!write_record.GetRecord().allocated_) {
                        break;
                    }
                    rec = std::make_unique<RmRecord>(write_record.GetRecord());
                }
                std::vector<std::string> index_keys;
                append_index_lock_keys(tab_name, tab, *rec, &index_keys);
                auto index_locks = lock_index_keys(this, std::move(index_keys));
                delete_indexes_for_rid(sm_manager_, tab, tab_name, *rec, rid, txn);
                if (fh->is_record(rid)) {
                    fh->delete_record(rid, nullptr);
                }
                if (table_context.has_cache) {
                    RuntimeFilterCache::Get().delete_record(tab_name, rid, *rec, tab, false);
                }
                break;
            }
            case WType::DELETE_TUPLE: {
                RmRecord &old_rec = write_record.GetRecord();
                std::vector<std::string> index_keys;
                append_index_lock_keys(tab_name, tab, old_rec, &index_keys);
                auto index_locks = lock_index_keys(this, std::move(index_keys));
                validate_index_entries_restorable(sm_manager_, tab, tab_name, old_rec, rid, txn);
                if (!fh->is_record(rid)) {
                    fh->insert_record(rid, old_rec.data);
                }
                insert_indexes(sm_manager_, tab, tab_name, old_rec, rid, txn);
                if (table_context.has_cache) {
                    RuntimeFilterCache::Get().insert_record(tab_name, rid, old_rec, tab);
                }
                break;
            }
            case WType::UPDATE_TUPLE: {
                RmRecord &old_rec = write_record.GetRecord();
                auto cur_rec = fh->get_record(rid, nullptr);
                std::vector<std::string> index_keys;
                append_index_lock_keys(tab_name, tab, old_rec, &index_keys);
                append_index_lock_keys(tab_name, tab, *cur_rec, &index_keys);
                auto index_locks = lock_index_keys(this, std::move(index_keys));
                validate_index_entries_restorable(sm_manager_, tab, tab_name, old_rec, rid, txn);
                for (const auto &index : tab.indexes) {
                    if (!index_key_changed(old_rec, *cur_rec, index)) {
                        continue;
                    }
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
                    auto key = make_index_key(*cur_rec, index);
                    delete_index_entry_for_rid(ih, key, rid, txn);
                }
                fh->update_record(rid, old_rec.data, nullptr);
                insert_indexes(sm_manager_, tab, tab_name, old_rec, rid, txn);
                if (table_context.has_cache) {
                    RuntimeFilterCache::Get().update_record(tab_name, rid, *cur_rec, old_rec, tab, false);
                }
                break;
            }
            default:
                throw InternalError("Unexpected write record type");
        }
        write_set->pop_back();
    }
    {
        std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
        txn_id_t txn_id = txn->get_transaction_id();
        timestamp_t materialize_ts = OldestActiveStartTsLocked(txn_id);
        for (const auto &key : touched_keys) {
            auto it = versions_.find(key);
            if (it == versions_.end()) {
                continue;
            }
            auto &chain = it->second;
            chain.erase(std::remove_if(chain.begin(), chain.end(), [&](const MvccVersion &version) {
                return version.writer_txn == txn_id && !version.committed;
            }), chain.end());
            materialize_pending_.erase(key);
            if (chain.empty()) {
                RemoveVersionKeyLocked(key);
                versions_.erase(it);
                continue;
            }
            if (MaterializeCommittedVersionLocked(key, materialize_ts)) {
                deferred_materialize_keys.push_back(key);
            }
        }
        CleanupSerializableTxnLocked(txn->get_transaction_id());
        if (!materialize_queue_.empty()) {
            MaterializeCommittedVersionsLocked(materialize_ts, MVCC_MATERIALIZE_BUDGET);
        }
        for (const auto &key : deferred_materialize_keys) {
            EnqueueMaterializeKeyLocked(key);
        }
    }
    if (log_manager != nullptr && txn->get_prev_lsn() != INVALID_LSN) {
        AbortLogRecord log_record(txn->get_transaction_id());
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_manager->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(lsn);
        log_manager->flush_log_to_disk(true);
    }
    txn->set_state(TransactionState::ABORTED);
    {
        std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
        TransactionManager::txn_map.erase(txn->get_transaction_id());
    }
}

void TransactionManager::GarbageCollection() {
    std::unique_lock<std::shared_mutex> gc_lock(mvcc_gc_latch_);
    std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    MaterializeCommittedVersionsLocked(OldestActiveStartTsLocked(INVALID_TXN_ID));
}

bool TransactionManager::has_active_transaction() {
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
    for (const auto &entry : TransactionManager::txn_map) {
        if (entry.second != nullptr &&
            (entry.second->get_state() == TransactionState::GROWING ||
             entry.second->get_state() == TransactionState::COMMITTING)) {
            return true;
        }
    }
    return false;
}

bool TransactionManager::IsMvccTxn(Transaction *txn) const {
    if (txn == nullptr) {
        return false;
    }
    return txn->get_isolation_level() == IsolationLevel::SNAPSHOT_ISOLATION ||
           txn->get_isolation_level() == IsolationLevel::SERIALIZABLE;
}

bool TransactionManager::IsSerializableTxn(Transaction *txn) const {
    return txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE;
}

std::mutex &TransactionManager::GetTableWriteLatch(const std::string &tab_name) {
    return table_write_latch_buckets_[std::hash<std::string>()(tab_name) % table_write_latch_buckets_.size()];
}

std::mutex &TransactionManager::GetRecordWriteLatch(const std::string &tab_name, const Rid &rid) {
    size_t h1 = std::hash<std::string>()(tab_name);
    size_t h2 = std::hash<int>()(rid.page_no);
    size_t h3 = std::hash<int>()(rid.slot_no);
    return record_write_latches_[(h1 ^ (h2 << 1) ^ (h3 << 7)) % record_write_latches_.size()];
}

std::mutex &TransactionManager::GetIndexKeyLatch(const std::string &key) {
    return index_key_latches_[std::hash<std::string>()(key) % index_key_latches_.size()];
}

std::vector<std::unique_lock<std::mutex>> TransactionManager::LockIndexKeys(std::vector<std::string> keys) {
    std::vector<std::mutex *> latches;
    latches.reserve(keys.size());
    for (const auto &key : keys) {
        latches.push_back(&GetIndexKeyLatch(key));
    }

    // Logical keys can hash to the same latch. Lock each physical latch once and
    // use one global order so a collision cannot self-deadlock or invert order.
    std::sort(latches.begin(), latches.end(), std::less<std::mutex *>());
    latches.erase(std::unique(latches.begin(), latches.end()), latches.end());

    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(latches.size());
    for (auto *latch : latches) {
        locks.emplace_back(*latch);
    }
    return locks;
}

void TransactionManager::EnsureBaseVersionLocked(const RecordKey &key, const RmRecord &base_record) {
    auto &chain = versions_[key];
    if (!chain.empty()) {
        return;
    }
    AddVersionKeyLocked(key);
    MvccVersion base;
    base.writer_txn = INVALID_TXN_ID;
    base.commit_ts = 0;
    base.committed = true;
    base.is_deleted = false;
    base.record = base_record;
    chain.push_back(std::move(base));
}

std::optional<RmRecord> TransactionManager::VisibleRecordLocked(const RecordKey &key, Transaction *txn) {
    auto it = versions_.find(key);
    if (it == versions_.end()) {
        return std::nullopt;
    }
    txn_id_t txn_id = txn == nullptr ? INVALID_TXN_ID : txn->get_transaction_id();
    timestamp_t start_ts = txn == nullptr ? last_commit_ts_.load() : txn->get_start_ts();
    for (auto version_it = it->second.rbegin(); version_it != it->second.rend(); ++version_it) {
        const auto &version = *version_it;
        bool own_uncommitted = version.writer_txn == txn_id && !version.committed;
        bool committed_visible = version.committed && version.commit_ts <= start_ts;
        if (!own_uncommitted && !committed_visible) {
            continue;
        }
        if (version.is_deleted) {
            return std::nullopt;
        }
        return version.record;
    }
    return std::nullopt;
}

std::unique_ptr<RmRecord> TransactionManager::GetVisibleRecord(const std::string &tab_name, RmFileHandle *fh,
                                                               const Rid &rid, Transaction *txn) {
    if (!IsMvccTxn(txn)) {
        return fh->get_record(rid, nullptr);
    }
    RecordKey key{tab_name, rid};
    for (;;) {
        {
            std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
            auto it = versions_.find(key);
            if (it != versions_.end()) {
                auto visible = VisibleRecordLocked(key, txn);
                if (!visible.has_value()) {
                    return nullptr;
                }
                return std::make_unique<RmRecord>(*visible);
            }
        }

        std::unique_ptr<RmRecord> record;
        try {
            record = fh->get_record(rid, nullptr);
        } catch (RecordNotFoundError &) {
            std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
            if (versions_.find(key) == versions_.end()) {
                return nullptr;
            }
            continue;
        }
        {
            std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
            if (versions_.find(key) == versions_.end()) {
                return record;
            }
        }
    }
}

void TransactionManager::CollectVisibleVersionRids(const std::string &tab_name,
                                                   const std::vector<std::string> &index_col_names,
                                                   const std::vector<char> &lower_key, bool lower_open,
                                                   const std::vector<char> &upper_key, bool upper_open,
                                                   const std::vector<Condition> &conds,
                                                   const std::vector<ColMeta> &cols, Transaction *txn,
                                                   std::vector<Rid> *rids) {
    if (rids == nullptr || !IsMvccTxn(txn)) {
        return;
    }
    if (!index_col_names.empty() &&
        version_index_key_count_.load(std::memory_order_acquire) == 0) {
        return;
    }
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    IndexMeta *scan_index = nullptr;
    auto collect_locked = [&](const std::unordered_set<RecordKey, RecordKeyHash> &candidates) {
        for (const auto &key : candidates) {
            auto entry = versions_.find(key);
            if (entry == versions_.end() || entry->second.empty()) {
                continue;
            }
            auto visible = VisibleRecordLocked(key, txn);
            if (!visible.has_value()) {
                continue;
            }
            const auto &chain = entry->second;
            if (txn->get_isolation_level() == IsolationLevel::SNAPSHOT_ISOLATION && scan_index != nullptr) {
                const auto &latest = chain.back();
                bool need_extra = latest.is_deleted;
                if (!need_extra) {
                    need_extra = index_key_changed(*visible, latest.record, *scan_index);
                }
                if (!need_extra) {
                    continue;
                }
            }
            if (eval_conds(conds, cols, visible->data)) {
                rids->push_back(key.rid);
            }
        }
    };

    if (!index_col_names.empty()) {
        std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
        scan_index = &(*tab.get_index_meta(index_col_names));
        auto candidates = CollectIndexVersionKeysLocked(tab_name, *scan_index, lower_key, lower_open, upper_key,
                                                        upper_open);
        if (!candidates.empty()) {
            collect_locked(candidates);
        }
        return;
    }

    std::vector<ColMeta> filter_cols;
    std::string filter_key;
    std::string visible_tab = cols.empty() ? tab_name : cols.front().tab_name;
    bool use_filter_index = RuntimeFilterCache::choose_columns(tab, visible_tab, conds, &filter_cols) &&
                            version_filter_key_from_conditions(filter_cols, visible_tab, conds, &filter_key);
    if (use_filter_index) {
        std::string sig = VersionFilterSignatureLocked(tab_name, filter_cols);
        {
            std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
            auto index_it = version_filter_indexes_.find(sig);
            if (index_it != version_filter_indexes_.end()) {
                auto row_it = index_it->second.rows.find(filter_key);
                if (row_it != index_it->second.rows.end()) {
                    collect_locked(row_it->second);
                }
                return;
            }
        }
        std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
        VersionFilterIndex *index = EnsureVersionFilterIndexLocked(tab_name, filter_cols);
        auto row_it = index->rows.find(filter_key);
        if (row_it != index->rows.end()) {
            collect_locked(row_it->second);
        }
        return;
    }

    std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    auto keys_it = table_version_keys_.find(tab_name);
    if (keys_it == table_version_keys_.end() || keys_it->second.empty()) {
        return;
    }
    collect_locked(keys_it->second);
}

bool TransactionManager::CheckWriteConflict(const std::string &tab_name, const Rid &rid, Transaction *txn,
                                            bool allow_committed_rebase) {
    if (!IsMvccTxn(txn)) {
        return false;
    }
    RecordKey key{tab_name, rid};
    std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    auto it = versions_.find(key);
    if (it == versions_.end() || it->second.empty()) {
        return false;
    }
    const auto &latest = it->second.back();
    txn_id_t txn_id = txn->get_transaction_id();
    if (!latest.committed && latest.writer_txn != txn_id) {
        if (allow_committed_rebase) {
            return true;
        }
        throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
    }
    if (latest.committed && latest.commit_ts > txn->get_start_ts() && !allow_committed_rebase) {
        throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
    }
    if (latest.committed && latest.commit_ts > txn->get_start_ts() && latest.is_deleted) {
        throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
    }
    return false;
}

void TransactionManager::RefreshSnapshot(Transaction *txn) {
    if (!IsMvccTxn(txn) || txn->get_isolation_level() == IsolationLevel::SERIALIZABLE) {
        return;
    }
    timestamp_t ts = last_commit_ts_.load();
    txn->set_start_ts(ts);
    txn->set_read_ts(ts);
}

void TransactionManager::CheckInsertConflict(const std::string &tab_name, const RmRecord &record, Transaction *txn,
                                             const Rid *ignored_rid) {
    if (!IsMvccTxn(txn)) {
        return;
    }
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    if (tab.indexes.empty()) {
        return;
    }
    if (version_index_key_count_.load(std::memory_order_acquire) == 0) {
        return;
    }
    std::shared_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    txn_id_t txn_id = txn->get_transaction_id();
    std::unordered_set<RecordKey, RecordKeyHash> candidates;
    for (const auto &index : tab.indexes) {
        IndexRecordKey index_key{tab_name, index_name_for_version_map(index),
                                 index_key_to_string(record, index)};
        auto keys_it = index_version_keys_.find(index_key);
        if (keys_it != index_version_keys_.end()) {
            candidates.insert(keys_it->second.begin(), keys_it->second.end());
        }
    }
    if (candidates.empty()) {
        return;
    }
    for (const auto &key : candidates) {
        auto entry = versions_.find(key);
        if (entry == versions_.end() || entry->second.empty()) {
            continue;
        }
        if (ignored_rid != nullptr && key.rid == *ignored_rid) {
            continue;
        }
        const auto &latest = entry->second.back();
        auto visible = VisibleRecordLocked(key, txn);

        bool visible_same_key = false;
        bool latest_same_key = false;
        for (const auto &index : tab.indexes) {
            if (visible.has_value() && same_index_key(*visible, record, index)) {
                visible_same_key = true;
            }
            if (same_index_key(latest.record, record, index)) {
                latest_same_key = true;
            }
        }
        if (!visible_same_key && !latest_same_key) {
            continue;
        }

        if (!latest.committed && latest.writer_txn != txn_id) {
            throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
        }
        if (latest.committed && latest.commit_ts > txn->get_start_ts()) {
            throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
        }
        if (!latest.committed && latest.writer_txn == txn_id) {
            if (!latest.is_deleted && latest_same_key) {
                throw UniqueIndexViolationError();
            }
            continue;
        }
        if (!tab.indexes.empty() && visible_same_key) {
            throw UniqueIndexViolationError();
        }
    }
}

void TransactionManager::RecordInsert(const std::string &tab_name, const Rid &rid, const RmRecord &record,
                                      Transaction *txn) {
    if (!IsMvccTxn(txn)) {
        return;
    }
    std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    MvccVersion version;
    version.writer_txn = txn->get_transaction_id();
    version.committed = false;
    version.is_deleted = false;
    version.record = record;
    RecordKey key{tab_name, rid};
    versions_[key].push_back(std::move(version));
    AddVersionKeyLocked(key);
    AddVersionFilterRecordLocked(key, record);
}

void TransactionManager::RecordUpdate(const std::string &tab_name, const Rid &rid, const RmRecord &old_record,
                                      const RmRecord &new_record, Transaction *txn) {
    if (!IsMvccTxn(txn)) {
        return;
    }
    std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    RecordKey key{tab_name, rid};
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    bool has_changed_index = false;
    for (const auto &index : tab.indexes) {
        if (index_key_changed(old_record, new_record, index)) {
            has_changed_index = true;
            break;
        }
    }
    EnsureBaseVersionLocked(key, old_record);
    AddVersionFilterRecordLocked(key, old_record);
    auto append_update_version = [&]() {
        MvccVersion version;
        version.writer_txn = txn->get_transaction_id();
        version.committed = false;
        version.is_deleted = false;
        version.record = new_record;
        versions_[key].push_back(std::move(version));
        AddVersionKeyLocked(key);
        AddVersionFilterRecordLocked(key, new_record);
    };
    if (!has_changed_index) {
        append_update_version();
        return;
    }
    for (const auto &index : tab.indexes) {
        if (!index_key_changed(old_record, new_record, index)) {
            continue;
        }
        IndexRecordKey old_index_key{tab_name, index_name_for_version_map(index),
                                     index_key_to_string(old_record, index)};
        auto &old_keys = index_version_keys_[old_index_key];
        if (old_keys.insert(key).second) {
            version_index_key_count_.fetch_add(1, std::memory_order_relaxed);
        }
        index_group_keys_[index_group_name(old_index_key.tab_name, old_index_key.index_name)].insert(old_index_key);
        record_index_keys_[key].insert(std::move(old_index_key));

        IndexRecordKey index_key{tab_name, index_name_for_version_map(index),
                                 index_key_to_string(new_record, index)};
        auto &keys = index_version_keys_[index_key];
        if (keys.insert(key).second) {
            version_index_key_count_.fetch_add(1, std::memory_order_relaxed);
        }
        index_group_keys_[index_group_name(index_key.tab_name, index_key.index_name)].insert(index_key);
        record_index_keys_[key].insert(std::move(index_key));
    }
    append_update_version();
}

void TransactionManager::RecordDelete(const std::string &tab_name, const Rid &rid, const RmRecord &old_record,
                                      Transaction *txn) {
    if (!IsMvccTxn(txn)) {
        return;
    }
    std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    RecordKey key{tab_name, rid};
    EnsureBaseVersionLocked(key, old_record);
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    AddRecordIndexKeysLocked(key, old_record, tab, true);
    AddVersionFilterRecordLocked(key, old_record);
    MvccVersion version;
    version.writer_txn = txn->get_transaction_id();
    version.committed = false;
    version.is_deleted = true;
    version.record = old_record;
    versions_[key].push_back(std::move(version));
    AddVersionKeyLocked(key);
}

bool TransactionManager::RecordMatchesPredicate(const PredicateRead &read, const RmRecord &record) {
    return eval_conds(read.conds, read.cols, record.data);
}

void TransactionManager::RegisterPredicateRead(Transaction *txn, const std::string &tab_name,
                                               const std::vector<Condition> &conds,
                                               const std::vector<ColMeta> &cols) {
    if (!IsSerializableTxn(txn)) {
        return;
    }
    std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    txn_id_t txn_id = txn->get_transaction_id();
    auto &reader_info = serializable_txns_[txn_id];
    reader_info.start_ts = txn->get_start_ts();
    reader_info.state = txn->get_state();
    PredicateRead read{tab_name, conds, cols};
    reader_info.predicate_reads.push_back(read);

    auto keys_it = table_version_keys_.find(tab_name);
    if (keys_it == table_version_keys_.end()) {
        return;
    }
    for (const auto &key : keys_it->second) {
        auto entry = versions_.find(key);
        if (entry == versions_.end() || entry->second.empty()) {
            continue;
        }
        const auto &latest = entry->second.back();
        if (latest.writer_txn == txn_id) {
            continue;
        }
        bool invisible_writer = !latest.committed || latest.commit_ts > txn->get_start_ts();
        if (!invisible_writer) {
            continue;
        }
        if (serializable_txns_.find(latest.writer_txn) == serializable_txns_.end()) {
            continue;
        }

        bool changes_result = false;
        auto visible = VisibleRecordLocked(key, txn);
        if (visible.has_value() && RecordMatchesPredicate(read, *visible)) {
            changes_result = true;
        }
        if (!latest.is_deleted && RecordMatchesPredicate(read, latest.record)) {
            changes_result = true;
        }
        if (changes_result && AddRwDependencyLocked(txn_id, latest.writer_txn, txn_id)) {
            throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
        }
    }
}

void TransactionManager::RegisterRecordRead(Transaction *txn, const std::string &tab_name, const Rid &rid) {
    if (!IsSerializableTxn(txn)) {
        return;
    }
    std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    auto &info = serializable_txns_[txn->get_transaction_id()];
    info.start_ts = txn->get_start_ts();
    info.state = txn->get_state();
    info.record_reads.insert(RecordKey{tab_name, rid});
}

void TransactionManager::CheckSerializableWrite(Transaction *txn, const std::string &tab_name, const Rid &rid,
                                                const RmRecord *old_record, const RmRecord *new_record,
                                                const std::vector<ColMeta> &cols) {
    if (!IsSerializableTxn(txn)) {
        return;
    }
    std::unique_lock<std::shared_mutex> mvcc_lock(mvcc_latch_);
    txn_id_t writer_id = txn->get_transaction_id();
    auto &writer_info = serializable_txns_[writer_id];
    writer_info.start_ts = txn->get_start_ts();
    writer_info.state = txn->get_state();
    RecordKey key{tab_name, rid};

    for (const auto &entry : serializable_txns_) {
        txn_id_t reader_id = entry.first;
        const auto &reader_info = entry.second;
        if (reader_id == writer_id || reader_info.state == TransactionState::ABORTED) {
            continue;
        }

        bool read_matches = reader_info.record_reads.find(key) != reader_info.record_reads.end();
        for (const auto &read : reader_info.predicate_reads) {
            if (read.tab_name != tab_name) {
                continue;
            }
            if (old_record != nullptr && RecordMatchesPredicate(read, *old_record)) {
                read_matches = true;
            }
            if (new_record != nullptr && RecordMatchesPredicate(read, *new_record)) {
                read_matches = true;
            }
        }
        if (!read_matches) {
            continue;
        }
        if (AddRwDependencyLocked(reader_id, writer_id, writer_id)) {
            throw TransactionAbortException(writer_id, AbortReason::DEADLOCK_PREVENTION);
        }
    }
    (void)cols;
}

bool TransactionManager::AddRwDependencyLocked(txn_id_t reader, txn_id_t writer, txn_id_t current_txn) {
    if (reader == writer) {
        return false;
    }
    auto reader_it = serializable_txns_.find(reader);
    auto writer_it = serializable_txns_.find(writer);
    if (reader_it == serializable_txns_.end() || writer_it == serializable_txns_.end()) {
        return false;
    }
    if (reader_it->second.state == TransactionState::ABORTED ||
        writer_it->second.state == TransactionState::ABORTED) {
        return false;
    }
    bool inserted = reader_it->second.rw_out.insert(writer).second;
    writer_it->second.rw_in.insert(reader);
    if (!inserted) {
        return false;
    }
    return HasDangerousStructureLocked(reader, writer) &&
           (current_txn == reader || current_txn == writer);
}

bool TransactionManager::HasDangerousStructureLocked(txn_id_t reader, txn_id_t writer) {
    auto writer_it = serializable_txns_.find(writer);
    if (writer_it != serializable_txns_.end()) {
        for (txn_id_t tout : writer_it->second.rw_out) {
            if (TransactionsOverlapLocked(reader, writer) &&
                TransactionsOverlapLocked(writer, tout) &&
                CommitOrderDangerousLocked(reader, tout)) {
                return true;
            }
        }
    }

    auto reader_it = serializable_txns_.find(reader);
    if (reader_it != serializable_txns_.end()) {
        for (txn_id_t tin : reader_it->second.rw_in) {
            if (TransactionsOverlapLocked(tin, reader) &&
                TransactionsOverlapLocked(reader, writer) &&
                CommitOrderDangerousLocked(tin, writer)) {
                return true;
            }
        }
    }
    return false;
}

bool TransactionManager::TransactionsOverlapLocked(txn_id_t lhs, txn_id_t rhs) {
    auto lhs_it = serializable_txns_.find(lhs);
    auto rhs_it = serializable_txns_.find(rhs);
    if (lhs_it == serializable_txns_.end() || rhs_it == serializable_txns_.end()) {
        return false;
    }
    timestamp_t inf = std::numeric_limits<timestamp_t>::max();
    timestamp_t lhs_end = lhs_it->second.commit_ts == INVALID_TS ? inf : lhs_it->second.commit_ts;
    timestamp_t rhs_end = rhs_it->second.commit_ts == INVALID_TS ? inf : rhs_it->second.commit_ts;
    return lhs_it->second.start_ts < rhs_end && rhs_it->second.start_ts < lhs_end;
}

bool TransactionManager::CommitOrderDangerousLocked(txn_id_t tin, txn_id_t tout) {
    if (tin == tout) {
        return true;
    }
    auto tin_it = serializable_txns_.find(tin);
    auto tout_it = serializable_txns_.find(tout);
    if (tin_it == serializable_txns_.end() || tout_it == serializable_txns_.end()) {
        return false;
    }
    if (tout_it->second.commit_ts == INVALID_TS) {
        return false;
    }
    if (tin_it->second.commit_ts == INVALID_TS) {
        return true;
    }
    return tout_it->second.commit_ts < tin_it->second.commit_ts;
}

void TransactionManager::CleanupSerializableTxnLocked(txn_id_t txn_id) {
    serializable_txns_.erase(txn_id);
    for (auto &entry : serializable_txns_) {
        entry.second.rw_in.erase(txn_id);
        entry.second.rw_out.erase(txn_id);
    }
}

void TransactionManager::EnqueueMaterializeKeyLocked(const RecordKey &key) {
    auto [it, inserted] = materialize_pending_.insert(key);
    if (inserted) {
        try {
            materialize_queue_.push_back(key);
        } catch (...) {
            materialize_pending_.erase(it);
            throw;
        }
    }
}

bool TransactionManager::PopMaterializeKeyLocked(RecordKey *key) {
    while (!materialize_queue_.empty()) {
        RecordKey candidate = materialize_queue_.front();
        materialize_queue_.pop_front();
        auto it = materialize_pending_.find(candidate);
        if (it == materialize_pending_.end()) {
            continue;
        }
        materialize_pending_.erase(it);
        if (key != nullptr) {
            *key = std::move(candidate);
        }
        return true;
    }
    return false;
}

bool TransactionManager::MaterializeCommittedVersionLocked(const RecordKey &key, timestamp_t max_commit_ts) {
    auto it = versions_.find(key);
    if (it == versions_.end()) {
        return false;
    }
    auto &chain = it->second;
    size_t drop = 0;
    while (drop + 1 < chain.size() && chain[drop + 1].committed &&
           chain[drop + 1].commit_ts <= max_commit_ts) {
        ++drop;
    }
    if (drop > 0) {
        chain.erase(chain.begin(), chain.begin() + static_cast<std::ptrdiff_t>(drop));
    }

    if (!chain.empty() && chain.back().committed && chain.back().commit_ts <= max_commit_ts &&
        chain.back().is_deleted) {
        auto fh = sm_manager_->fhs_.at(key.tab_name).get();
        try {
            if (fh->is_record(key.rid)) {
                TabMeta &tab = sm_manager_->db_.get_table(key.tab_name);
                auto current = fh->get_record(key.rid, nullptr);
                std::vector<std::string> index_keys;
                append_index_lock_keys(key.tab_name, tab, *current, &index_keys);
                auto index_locks = lock_index_keys(this, std::move(index_keys));
                delete_indexes_for_rid(sm_manager_, tab, key.tab_name, *current, key.rid, nullptr);
                fh->delete_record(key.rid, nullptr);
            }
        } catch (...) {
            return true;
        }
        chain.clear();
    }

    if (chain.size() == 1 && chain.back().committed && chain.back().commit_ts <= max_commit_ts &&
        !chain.back().is_deleted) {
        chain.clear();
    }

    if (chain.empty()) {
        RemoveVersionKeyLocked(key);
        versions_.erase(it);
        return false;
    }
    return true;
}

timestamp_t TransactionManager::OldestActiveStartTsLocked(txn_id_t ignored_txn_id) {
    timestamp_t oldest = last_commit_ts_.load();
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
    for (const auto &entry : txn_map) {
        Transaction *txn = entry.second;
        if (txn == nullptr || txn->get_transaction_id() == ignored_txn_id ||
            (txn->get_state() != TransactionState::GROWING &&
             txn->get_state() != TransactionState::COMMITTING) ||
            !IsMvccTxn(txn)) {
            continue;
        }
        oldest = std::min(oldest, txn->get_start_ts());
    }
    return oldest;
}

void TransactionManager::MaterializeCommittedVersionsLocked(timestamp_t max_commit_ts, size_t budget) {
    if (budget == 0) {
        return;
    }
    if (materialize_queue_.empty() && max_commit_ts <= last_materialized_ts_) {
        return;
    }
    size_t processed = 0;
    while (processed < budget) {
        RecordKey key;
        if (!PopMaterializeKeyLocked(&key)) {
            break;
        }
        if (MaterializeCommittedVersionLocked(key, max_commit_ts)) {
            EnqueueMaterializeKeyLocked(key);
        }
        ++processed;
    }
    if (max_commit_ts > last_materialized_ts_) {
        last_materialized_ts_ = max_commit_ts;
    }
}
