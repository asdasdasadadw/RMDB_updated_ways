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
#include <array>
#include <cstring>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "errors.h"

namespace {

constexpr size_t WAL_SCAN_CHUNK_SIZE = 8U * 1024U * 1024U;

struct WalRecordHeader {
    LogType type{LogType::CHECKPOINT};
    lsn_t lsn{INVALID_LSN};
    uint32_t total_len{0};
    txn_id_t txn_id{INVALID_TXN_ID};
    lsn_t prev_lsn{INVALID_LSN};
};

template <typename T>
T read_scalar(const char *src, size_t offset) {
    T value{};
    memcpy(&value, src + offset, sizeof(T));
    return value;
}

bool is_valid_log_type(LogType type) {
    int raw = static_cast<int>(type);
    return raw >= static_cast<int>(LogType::UPDATE) &&
           raw <= static_cast<int>(LogType::CHECKPOINT);
}

bool is_dml(LogType type) {
    return type == LogType::INSERT || type == LogType::DELETE || type == LogType::UPDATE;
}

WalRecordHeader decode_header(const char *data, lsn_t physical_offset) {
    uint32_t magic = read_scalar<uint32_t>(data, OFFSET_LOG_MAGIC);
    uint32_t version = read_scalar<uint32_t>(data, OFFSET_LOG_VERSION);
    if (magic != LOG_RECORD_MAGIC || version != LOG_FORMAT_VERSION) {
        throw InternalError("Unsupported or corrupt WAL format");
    }

    WalRecordHeader header;
    header.type = read_scalar<LogType>(data, OFFSET_LOG_TYPE);
    header.lsn = read_scalar<lsn_t>(data, OFFSET_LSN);
    header.total_len = read_scalar<uint32_t>(data, OFFSET_LOG_TOT_LEN);
    header.txn_id = read_scalar<txn_id_t>(data, OFFSET_LOG_TID);
    header.prev_lsn = read_scalar<lsn_t>(data, OFFSET_PREV_LSN);

    if (!is_valid_log_type(header.type)) {
        throw InternalError("Unknown WAL record type");
    }
    if (header.total_len < LOG_HEADER_SIZE || header.total_len > LOG_BUFFER_SIZE) {
        throw InternalError("Invalid WAL record length");
    }
    if (header.lsn != physical_offset) {
        throw InternalError("WAL LSN does not match its physical offset");
    }
    if (header.prev_lsn != INVALID_LSN &&
        (header.prev_lsn < 0 || header.prev_lsn >= header.lsn)) {
        throw InternalError("Invalid WAL previous-LSN chain");
    }
    if (header.type == LogType::CHECKPOINT) {
        if (header.txn_id != INVALID_TXN_ID || header.prev_lsn != INVALID_LSN) {
            throw InternalError("Invalid WAL checkpoint identity");
        }
    } else if (header.txn_id < 0) {
        throw InternalError("Invalid WAL transaction id");
    }
    return header;
}

bool consume_bytes(size_t *cursor, size_t amount, size_t total) {
    if (cursor == nullptr || *cursor > total || amount > total - *cursor) {
        return false;
    }
    *cursor += amount;
    return true;
}

bool consume_record(const char *data, size_t total, size_t *cursor) {
    if (cursor == nullptr || !consume_bytes(cursor, sizeof(int), total)) {
        return false;
    }
    int record_size = read_scalar<int>(data, *cursor - sizeof(int));
    if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
        return false;
    }
    return consume_bytes(cursor, static_cast<size_t>(record_size), total);
}

bool consume_string(const char *data, size_t total, size_t *cursor) {
    if (cursor == nullptr || !consume_bytes(cursor, sizeof(uint32_t), total)) {
        return false;
    }
    uint32_t len = read_scalar<uint32_t>(data, *cursor - sizeof(uint32_t));
    return consume_bytes(cursor, static_cast<size_t>(len), total);
}

bool valid_payload(const char *data, const WalRecordHeader &header) {
    size_t cursor = LOG_HEADER_SIZE;
    size_t total = header.total_len;
    switch (header.type) {
        case LogType::INSERT:
        case LogType::DELETE:
            return consume_record(data, total, &cursor) &&
                   consume_bytes(&cursor, sizeof(Rid), total) &&
                   consume_string(data, total, &cursor) && cursor == total;
        case LogType::UPDATE:
            return consume_record(data, total, &cursor) &&
                   consume_record(data, total, &cursor) &&
                   consume_bytes(&cursor, sizeof(Rid), total) &&
                   consume_string(data, total, &cursor) && cursor == total;
        case LogType::begin:
        case LogType::commit:
        case LogType::ABORT:
        case LogType::CHECKPOINT:
            return cursor == total;
    }
    return false;
}

class WalScanner {
   public:
    WalScanner(const std::string &file_name, lsn_t begin, lsn_t end)
        : input_(file_name, std::ios::binary), buffer_(WAL_SCAN_CHUNK_SIZE + LOG_BUFFER_SIZE),
          buffer_start_(begin), limit_(end) {
        if (!input_.is_open() || begin < 0 || end < begin) {
            throw InternalError("Failed to open WAL for recovery");
        }
        input_.seekg(static_cast<std::streamoff>(begin), std::ios::beg);
        if (!input_) {
            throw InternalError("Failed to seek WAL for recovery");
        }
    }

    lsn_t offset() const { return buffer_start_ + static_cast<lsn_t>(pos_); }
    lsn_t limit() const { return limit_; }
    const char *data() const { return buffer_.data() + pos_; }

    bool ensure(size_t needed) {
        if (needed > buffer_.size()) {
            throw InternalError("WAL record exceeds recovery buffer");
        }
        while (end_ - pos_ < needed) {
            compact();
            lsn_t loaded_end = buffer_start_ + static_cast<lsn_t>(end_);
            if (loaded_end >= limit_) {
                break;
            }
            size_t capacity = buffer_.size() - end_;
            lsn_t remaining = limit_ - loaded_end;
            size_t to_read = static_cast<size_t>(std::min<lsn_t>(remaining, capacity));
            input_.read(buffer_.data() + end_, static_cast<std::streamsize>(to_read));
            if (input_.gcount() != static_cast<std::streamsize>(to_read)) {
                throw InternalError("Short WAL read during recovery");
            }
            end_ += to_read;
        }
        return end_ - pos_ >= needed;
    }

    void advance(size_t amount) {
        if (amount > end_ - pos_) {
            throw InternalError("Recovery scanner advanced past buffered WAL");
        }
        pos_ += amount;
    }

   private:
    void compact() {
        if (pos_ == 0) {
            return;
        }
        size_t remaining = end_ - pos_;
        if (remaining > 0) {
            memmove(buffer_.data(), buffer_.data() + pos_, remaining);
        }
        buffer_start_ += static_cast<lsn_t>(pos_);
        pos_ = 0;
        end_ = remaining;
    }

    std::ifstream input_;
    std::vector<char> buffer_;
    lsn_t buffer_start_{0};
    lsn_t limit_{0};
    size_t pos_{0};
    size_t end_{0};
};

bool next_record(WalScanner *scanner, WalRecordHeader *header, bool *torn_tail) {
    if (scanner == nullptr || header == nullptr || torn_tail == nullptr) {
        throw InternalError("Invalid WAL scanner state");
    }
    *torn_tail = false;
    lsn_t offset = scanner->offset();
    lsn_t remaining = scanner->limit() - offset;
    if (remaining == 0) {
        return false;
    }
    if (remaining < LOG_HEADER_SIZE) {
        // A v2 write may tear after only part of its header.  If enough bytes
        // survived to identify the format, reject a v1/corrupt file instead
        // of silently truncating it as though it were a v2 tail.
        size_t marker_size = static_cast<size_t>(
            std::min<lsn_t>(remaining, 2 * static_cast<lsn_t>(sizeof(uint32_t))));
        if (marker_size >= sizeof(uint32_t)) {
            if (!scanner->ensure(marker_size)) {
                throw InternalError("Short WAL format marker");
            }
            uint32_t magic = read_scalar<uint32_t>(scanner->data(), OFFSET_LOG_MAGIC);
            if (magic != LOG_RECORD_MAGIC) {
                throw InternalError("Unsupported or corrupt WAL format");
            }
            if (marker_size >= 2 * sizeof(uint32_t)) {
                uint32_t version = read_scalar<uint32_t>(scanner->data(), OFFSET_LOG_VERSION);
                if (version != LOG_FORMAT_VERSION) {
                    throw InternalError("Unsupported or corrupt WAL format");
                }
            }
        }
        *torn_tail = true;
        return false;
    }
    if (!scanner->ensure(LOG_HEADER_SIZE)) {
        *torn_tail = true;
        return false;
    }

    *header = decode_header(scanner->data(), offset);
    if (static_cast<lsn_t>(header->total_len) > remaining) {
        *torn_tail = true;
        return false;
    }
    if (!scanner->ensure(header->total_len)) {
        throw InternalError("Short WAL record read during recovery");
    }
    if (!valid_payload(scanner->data(), *header)) {
        throw InternalError("Invalid WAL record payload");
    }
    return true;
}

std::unique_ptr<LogRecord> make_log_record(LogType type) {
    switch (type) {
        case LogType::UPDATE:
            return std::make_unique<UpdateLogRecord>();
        case LogType::INSERT:
            return std::make_unique<InsertLogRecord>();
        case LogType::DELETE:
            return std::make_unique<DeleteLogRecord>();
        case LogType::begin:
            return std::make_unique<BeginLogRecord>();
        case LogType::commit:
            return std::make_unique<CommitLogRecord>();
        case LogType::ABORT:
            return std::make_unique<AbortLogRecord>();
        case LogType::CHECKPOINT:
            return std::make_unique<CheckpointLogRecord>();
    }
    throw InternalError("Unknown WAL record type");
}

std::unique_ptr<LogRecord> deserialize_log_record(const char *data, const WalRecordHeader &header) {
    auto log = make_log_record(header.type);
    log->deserialize(data);
    return log;
}

void validate_replay_target(SmManager *sm_manager, const std::string &table_name, const Rid &rid,
                            std::initializer_list<const RmRecord *> records) {
    if (sm_manager == nullptr || !sm_manager->db_.is_table(table_name)) {
        throw InternalError("WAL references an unknown table");
    }
    auto fh_it = sm_manager->fhs_.find(table_name);
    if (fh_it == sm_manager->fhs_.end() || fh_it->second == nullptr) {
        throw InternalError("WAL table file is not open");
    }
    RmFileHdr file_hdr = fh_it->second->get_file_hdr();
    if (file_hdr.record_size < 1 || file_hdr.record_size > RM_MAX_RECORD_SIZE ||
        file_hdr.num_records_per_page < 1) {
        throw InternalError("Invalid table metadata during WAL recovery");
    }
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.slot_no < 0 ||
        rid.slot_no >= file_hdr.num_records_per_page) {
        throw InternalError("Invalid RID in WAL record");
    }
    for (const RmRecord *record : records) {
        if (record == nullptr || !record->allocated_ || record->data == nullptr ||
            record->size != file_hdr.record_size) {
            throw InternalError("WAL record size does not match table schema");
        }
    }
}

void validate_replay_record(SmManager *sm_manager, const LogRecord &log) {
    switch (log.log_type_) {
        case LogType::INSERT: {
            const auto &insert_log = static_cast<const InsertLogRecord &>(log);
            validate_replay_target(sm_manager, insert_log.table_name_, insert_log.rid_,
                                   {&insert_log.insert_value_});
            return;
        }
        case LogType::DELETE: {
            const auto &delete_log = static_cast<const DeleteLogRecord &>(log);
            validate_replay_target(sm_manager, delete_log.table_name_, delete_log.rid_,
                                   {&delete_log.delete_value_});
            return;
        }
        case LogType::UPDATE: {
            const auto &update_log = static_cast<const UpdateLogRecord &>(log);
            validate_replay_target(sm_manager, update_log.table_name_, update_log.rid_,
                                   {&update_log.old_value_, &update_log.new_value_});
            return;
        }
        default:
            return;
    }
}

lsn_t read_checkpoint_offset() {
    std::ifstream input(CHECKPOINT_FILE_NAME);
    lsn_t offset = 0;
    if (!input.is_open() || !(input >> offset) || offset < 0) {
        return 0;
    }
    return offset;
}

bool checkpoint_is_valid(lsn_t checkpoint, lsn_t file_size) {
    if (checkpoint <= 0) {
        return checkpoint == 0;
    }
    if (checkpoint > file_size || file_size - checkpoint < LOG_HEADER_SIZE) {
        return false;
    }
    try {
        WalScanner scanner(LOG_FILE_NAME, checkpoint, file_size);
        WalRecordHeader header;
        bool torn_tail = false;
        return next_record(&scanner, &header, &torn_tail) &&
               header.type == LogType::CHECKPOINT && header.lsn == checkpoint;
    } catch (RMDBError &) {
        return false;
    }
}

std::unique_ptr<LogRecord> read_record_at(std::ifstream *input, lsn_t offset, lsn_t valid_end,
                                         std::vector<char> *scratch, WalRecordHeader *header) {
    if (input == nullptr || scratch == nullptr || header == nullptr || offset < 0 ||
        offset > valid_end || valid_end - offset < LOG_HEADER_SIZE) {
        throw InternalError("Invalid WAL random-read offset");
    }

    std::array<char, LOG_HEADER_SIZE> header_bytes{};
    input->clear();
    input->seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!*input) {
        throw InternalError("Failed to seek WAL undo record");
    }
    input->read(header_bytes.data(), static_cast<std::streamsize>(header_bytes.size()));
    if (input->gcount() != static_cast<std::streamsize>(header_bytes.size())) {
        throw InternalError("Short WAL undo header");
    }

    *header = decode_header(header_bytes.data(), offset);
    if (static_cast<lsn_t>(header->total_len) > valid_end - offset) {
        throw InternalError("WAL undo record extends past valid tail");
    }
    scratch->resize(header->total_len);
    memcpy(scratch->data(), header_bytes.data(), header_bytes.size());
    size_t payload_size = header->total_len - LOG_HEADER_SIZE;
    if (payload_size > 0) {
        input->read(scratch->data() + LOG_HEADER_SIZE, static_cast<std::streamsize>(payload_size));
        if (input->gcount() != static_cast<std::streamsize>(payload_size)) {
            throw InternalError("Short WAL undo record");
        }
    }
    if (!valid_payload(scratch->data(), *header)) {
        throw InternalError("Invalid WAL undo payload");
    }
    return deserialize_log_record(scratch->data(), *header);
}

std::unique_ptr<RmRecord> get_record_if_exists(RmFileHandle *fh, const Rid &rid) {
    try {
        return fh->get_record(rid, nullptr);
    } catch (RecordNotFoundError &) {
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
    if (current != nullptr) {
        fh->delete_record(log.rid_, nullptr);
    }
}

void redo_delete(SmManager *sm_manager, const DeleteLogRecord &log) {
    auto fh = sm_manager->fhs_.at(log.table_name_).get();
    auto current = get_record_if_exists(fh, log.rid_);
    if (current != nullptr) {
        fh->delete_record(log.rid_, nullptr);
    }
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
    txn_states_.clear();
    max_txn_id_ = INVALID_TXN_ID;
    scan_start_ = 0;
    valid_end_ = 0;
    has_recovery_work_ = false;

    if (!disk_manager_->is_file(LOG_FILE_NAME)) {
        return;
    }
    wal_offset_t file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        return;
    }

    lsn_t checkpoint = read_checkpoint_offset();
    scan_start_ = checkpoint_is_valid(checkpoint, file_size) ? checkpoint : 0;
    valid_end_ = scan_start_;

    {
        WalScanner scanner(LOG_FILE_NAME, scan_start_, file_size);
        while (scanner.offset() < scanner.limit()) {
            WalRecordHeader header;
            bool torn_tail = false;
            if (!next_record(&scanner, &header, &torn_tail)) {
                if (!torn_tail && scanner.offset() != scanner.limit()) {
                    throw InternalError("WAL scan stopped before valid end");
                }
                break;
            }

            if (header.txn_id != INVALID_TXN_ID) {
                max_txn_id_ = std::max(max_txn_id_, header.txn_id);
                auto &state = txn_states_[header.txn_id];
                if (header.prev_lsn != state.last_lsn) {
                    throw InternalError("WAL transaction chain is incomplete");
                }
                if (state.terminal != TxnTerminalState::NONE) {
                    throw InternalError("WAL transaction continues after a terminal record");
                }
                switch (header.type) {
                    case LogType::begin:
                        if (state.last_lsn != INVALID_LSN) {
                            throw InternalError("Duplicate WAL transaction begin");
                        }
                        state.undo = true;
                        break;
                    case LogType::INSERT:
                    case LogType::DELETE:
                    case LogType::UPDATE:
                        has_recovery_work_ = true;
                        if (!state.redo) {
                            state.undo = true;
                        }
                        break;
                    case LogType::commit:
                        state.undo = false;
                        state.redo = true;
                        state.terminal = TxnTerminalState::COMMITTED;
                        break;
                    case LogType::ABORT:
                        state.redo = false;
                        state.undo = true;
                        state.terminal = TxnTerminalState::ABORTED;
                        break;
                    case LogType::CHECKPOINT:
                        throw InternalError("Checkpoint WAL record has a transaction id");
                }
                state.last_lsn = header.lsn;
            }

            scanner.advance(header.total_len);
            valid_end_ = scanner.offset();
        }
    }

    // Even when no tail is torn, the accepted bytes may only have survived in
    // the kernel page cache of a killed process.  Make that recovery boundary
    // durable before replay can dirty and flush data pages, and align the next
    // append offset with the validated end.
    disk_manager_->truncate_log(valid_end_);
}

void RecoveryManager::undo() {
    using UndoItem = std::pair<lsn_t, txn_id_t>;
    std::priority_queue<UndoItem> pending;
    for (const auto &entry : txn_states_) {
        if (entry.second.undo && entry.second.last_lsn >= scan_start_) {
            pending.emplace(entry.second.last_lsn, entry.first);
        }
    }
    if (pending.empty()) {
        return;
    }

    std::ifstream input(LOG_FILE_NAME, std::ios::binary);
    if (!input.is_open()) {
        throw InternalError("Failed to open WAL for undo");
    }
    std::vector<char> scratch;
    while (!pending.empty()) {
        auto [lsn, txn_id] = pending.top();
        pending.pop();

        WalRecordHeader header;
        auto log = read_record_at(&input, lsn, valid_end_, &scratch, &header);
        if (header.txn_id != txn_id) {
            throw InternalError("WAL undo chain crosses transaction boundary");
        }
        validate_replay_record(sm_manager_, *log);

        switch (header.type) {
            case LogType::INSERT:
                undo_insert(sm_manager_, *static_cast<InsertLogRecord *>(log.get()));
                break;
            case LogType::DELETE:
                undo_delete(sm_manager_, *static_cast<DeleteLogRecord *>(log.get()));
                break;
            case LogType::UPDATE:
                undo_update(sm_manager_, *static_cast<UpdateLogRecord *>(log.get()));
                break;
            default:
                break;
        }

        if (header.prev_lsn != INVALID_LSN && header.prev_lsn >= scan_start_) {
            pending.emplace(header.prev_lsn, txn_id);
        }
    }
}

void RecoveryManager::redo() {
    if (valid_end_ <= scan_start_) {
        return;
    }

    WalScanner scanner(LOG_FILE_NAME, scan_start_, valid_end_);
    while (scanner.offset() < scanner.limit()) {
        WalRecordHeader header;
        bool torn_tail = false;
        if (!next_record(&scanner, &header, &torn_tail) || torn_tail) {
            throw InternalError("Validated WAL changed before redo");
        }

        auto state_it = txn_states_.find(header.txn_id);
        bool should_redo = state_it != txn_states_.end() && state_it->second.redo && is_dml(header.type);
        if (should_redo) {
            auto log = deserialize_log_record(scanner.data(), header);
            validate_replay_record(sm_manager_, *log);
            switch (header.type) {
                case LogType::INSERT:
                    redo_insert(sm_manager_, *static_cast<InsertLogRecord *>(log.get()));
                    break;
                case LogType::DELETE:
                    redo_delete(sm_manager_, *static_cast<DeleteLogRecord *>(log.get()));
                    break;
                case LogType::UPDATE:
                    redo_update(sm_manager_, *static_cast<UpdateLogRecord *>(log.get()));
                    break;
                default:
                    break;
            }
        }
        scanner.advance(header.total_len);
    }

    std::unordered_map<txn_id_t, TxnRecoveryState>().swap(txn_states_);
    has_recovery_work_ = false;
}
