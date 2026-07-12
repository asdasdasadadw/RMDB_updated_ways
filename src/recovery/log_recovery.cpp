/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <vector>

#include "errors.h"
namespace {

std::shared_ptr<LogRecord> make_log_record(LogType type) {
    switch (type) {
        case LogType::UPDATE:
            return std::make_shared<UpdateLogRecord>();
        case LogType::INSERT:
            return std::make_shared<InsertLogRecord>();
        case LogType::DELETE:
            return std::make_shared<DeleteLogRecord>();
        case LogType::begin:
            return std::make_shared<BeginLogRecord>();
        case LogType::commit:
            return std::make_shared<CommitLogRecord>();
        case LogType::ABORT:
            return std::make_shared<AbortLogRecord>();
        case LogType::CHECKPOINT:
            return std::make_shared<CheckpointLogRecord>();
        default:
            throw InternalError("Unknown log type");
    }
}

lsn_t read_checkpoint_offset(DiskManager *disk_manager) {
    if (!disk_manager->is_file(CHECKPOINT_FILE_NAME)) {
        return 0;
    }
    std::ifstream input(CHECKPOINT_FILE_NAME);
    lsn_t offset = 0;
    input >> offset;
    return std::max<lsn_t>(0, offset);
}

std::unique_ptr<RmRecord> get_record_if_exists(RmFileHandle *fh, const Rid &rid) {
    try {
        return fh->get_record(rid, nullptr);
    } catch (RMDBError &) {
        return nullptr;
    }
}

bool same_record_value(const RmRecord &lhs, const RmRecord &rhs) {
    return lhs.size == rhs.size && memcmp(lhs.data, rhs.data, lhs.size) == 0;
}

void redo_insert(SmManager *sm_manager, const InsertLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto current = get_record_if_exists(fh, log.rid_);
    if (current != nullptr) {
        if (same_record_value(*current, log.insert_value_)) {
            return;
        }
        fh->update_record(log.rid_, const_cast<char *>(log.insert_value_.data), nullptr);
    } else {
        fh->insert_record(log.rid_, const_cast<char *>(log.insert_value_.data));
    }
}

void undo_insert(SmManager *sm_manager, const InsertLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto current = get_record_if_exists(fh, log.rid_);
    if (current == nullptr) {
        return;
    }
    fh->delete_record(log.rid_, nullptr);
}

void redo_delete(SmManager *sm_manager, const DeleteLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto current = get_record_if_exists(fh, log.rid_);
    if (current == nullptr) {
        return;
    }
    fh->delete_record(log.rid_, nullptr);
}

void undo_delete(SmManager *sm_manager, const DeleteLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto current = get_record_if_exists(fh, log.rid_);
    if (current != nullptr) {
        if (same_record_value(*current, log.delete_value_)) {
            return;
        }
        fh->update_record(log.rid_, const_cast<char *>(log.delete_value_.data), nullptr);
    } else {
        fh->insert_record(log.rid_, const_cast<char *>(log.delete_value_.data));
    }
}

void redo_update(SmManager *sm_manager, const UpdateLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto current = get_record_if_exists(fh, log.rid_);
    if (current != nullptr) {
        if (same_record_value(*current, log.new_value_)) {
            return;
        }
        fh->update_record(log.rid_, const_cast<char *>(log.new_value_.data), nullptr);
    } else {
        fh->insert_record(log.rid_, const_cast<char *>(log.new_value_.data));
    }
}

void undo_update(SmManager *sm_manager, const UpdateLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto current = get_record_if_exists(fh, log.rid_);
    if (current != nullptr) {
        if (same_record_value(*current, log.old_value_)) {
            return;
        }
        fh->update_record(log.rid_, const_cast<char *>(log.old_value_.data), nullptr);
    } else {
        fh->insert_record(log.rid_, const_cast<char *>(log.old_value_.data));
    }
}

}  // namespace

void RecoveryManager::analyze() {
    logs_.clear();
    redo_txns_.clear();
    undo_txns_.clear();
    max_txn_id_ = INVALID_TXN_ID;

    lsn_t offset = read_checkpoint_offset(disk_manager_);
    if (!disk_manager_->is_file(LOG_FILE_NAME)) {
        return;
    }
    std::ifstream input(LOG_FILE_NAME, std::ios::binary | std::ios::ate);
    if (!input.is_open()) {
        return;
    }
    std::streamoff file_size = input.tellg();
    if (offset < 0 || static_cast<std::streamoff>(offset) >= file_size) {
        return;
    }
    input.seekg(offset, std::ios::beg);
    std::vector<char> raw(static_cast<size_t>(file_size - offset));
    input.read(raw.data(), static_cast<std::streamsize>(raw.size()));
    size_t bytes_read = static_cast<size_t>(input.gcount());
    raw.resize(bytes_read);

    size_t pos = 0;
    while (pos + LOG_HEADER_SIZE <= raw.size()) {
        const char *log_data = raw.data() + pos;
        auto type = *reinterpret_cast<const LogType *>(log_data + OFFSET_LOG_TYPE);
        auto total_len = *reinterpret_cast<const uint32_t *>(log_data + OFFSET_LOG_TOT_LEN);
        if (total_len < LOG_HEADER_SIZE || pos + static_cast<size_t>(total_len) > raw.size()) {
            break;
        }
        auto log = make_log_record(type);
        log->deserialize(log_data);
        logs_.push_back(log);
        if (log->log_tid_ != INVALID_TXN_ID) {
            max_txn_id_ = std::max(max_txn_id_, log->log_tid_);
        }

        switch (log->log_type_) {
            case LogType::begin:
                undo_txns_.insert(log->log_tid_);
                break;
            case LogType::INSERT:
            case LogType::DELETE:
            case LogType::UPDATE:
                if (redo_txns_.find(log->log_tid_) == redo_txns_.end()) {
                    undo_txns_.insert(log->log_tid_);
                }
                break;
            case LogType::commit:
                undo_txns_.erase(log->log_tid_);
                redo_txns_.insert(log->log_tid_);
                break;
            case LogType::ABORT:
                // There are no compensation log records and abort does not
                // force the rollback pages to disk.  A steal before abort can
                // therefore leave an uncommitted value on disk, so an aborted
                // writer must still be undone during crash recovery.
                undo_txns_.insert(log->log_tid_);
                break;
            default:
                break;
        }
        pos += static_cast<size_t>(total_len);
    }
}

void RecoveryManager::redo() {
    for (const auto &log : logs_) {
        if (redo_txns_.find(log->log_tid_) == redo_txns_.end()) {
            continue;
        }
        switch (log->log_type_) {
            case LogType::INSERT:
                redo_insert(sm_manager_, *std::static_pointer_cast<InsertLogRecord>(log));
                break;
            case LogType::DELETE:
                redo_delete(sm_manager_, *std::static_pointer_cast<DeleteLogRecord>(log));
                break;
            case LogType::UPDATE:
                redo_update(sm_manager_, *std::static_pointer_cast<UpdateLogRecord>(log));
                break;
            default:
                break;
        }
    }
}

void RecoveryManager::undo() {
    for (auto it = logs_.rbegin(); it != logs_.rend(); ++it) {
        const auto &log = *it;
        if (undo_txns_.find(log->log_tid_) == undo_txns_.end()) {
            continue;
        }
        switch (log->log_type_) {
            case LogType::INSERT:
                undo_insert(sm_manager_, *std::static_pointer_cast<InsertLogRecord>(log));
                break;
            case LogType::DELETE:
                undo_delete(sm_manager_, *std::static_pointer_cast<DeleteLogRecord>(log));
                break;
            case LogType::UPDATE:
                undo_update(sm_manager_, *std::static_pointer_cast<UpdateLogRecord>(log));
                break;
            default:
                break;
        }
    }
}
