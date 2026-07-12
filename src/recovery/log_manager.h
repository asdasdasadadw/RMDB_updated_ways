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

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>
#include <iostream>
#include "log_defs.h"
#include "common/config.h"
#include "errors.h"
#include "record/rm_defs.h"

/* 日志记录对应操作的类型 */
enum LogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    begin,
    commit,
    ABORT,
    CHECKPOINT
};
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT",
    "CHECKPOINT"
};

inline uint32_t serialized_string_len(const std::string &str) {
    return static_cast<uint32_t>(sizeof(uint32_t) + str.size());
}

inline void serialize_string(char *dest, int &offset, const std::string &str) {
    uint32_t len = static_cast<uint32_t>(str.size());
    memcpy(dest + offset, &len, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (len > 0) {
        memcpy(dest + offset, str.data(), len);
        offset += len;
    }
}

inline std::string deserialize_string(const char *src, int &offset) {
    uint32_t len = 0;
    memcpy(&len, src + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    std::string str(src + offset, src + offset + len);
    offset += len;
    return str;
}

inline uint32_t serialized_record_len(const RmRecord &record) {
    return static_cast<uint32_t>(sizeof(int) + record.size);
}

inline void serialize_record(char *dest, int &offset, const RmRecord &record) {
    memcpy(dest + offset, &record.size, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, record.data, record.size);
    offset += record.size;
}

inline RmRecord deserialize_record(const char *src, int &offset) {
    int size = 0;
    memcpy(&size, src + offset, sizeof(int));
    offset += sizeof(int);
    RmRecord record(size, const_cast<char *>(src + offset));
    offset += size;
    return record;
}

class LogRecord {
public:
    virtual ~LogRecord() = default;

    LogType log_type_;         /* 日志对应操作的类型 */
    lsn_t lsn_;                /* 当前日志的lsn */
    uint32_t log_tot_len_;     /* 整个日志记录的长度 */
    txn_id_t log_tid_;         /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */

    // 把日志记录序列化到dest中
    virtual void serialize (char* dest) const {
        memcpy(dest + OFFSET_LOG_MAGIC, &LOG_RECORD_MAGIC, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_VERSION, &LOG_FORMAT_VERSION, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        uint32_t magic = 0;
        uint32_t version = 0;
        memcpy(&magic, src + OFFSET_LOG_MAGIC, sizeof(uint32_t));
        memcpy(&version, src + OFFSET_LOG_VERSION, sizeof(uint32_t));
        if (magic != LOG_RECORD_MAGIC || version != LOG_FORMAT_VERSION) {
            throw InternalError("Unsupported WAL format");
        }
        memcpy(&log_type_, src + OFFSET_LOG_TYPE, sizeof(LogType));
        memcpy(&lsn_, src + OFFSET_LSN, sizeof(lsn_t));
        memcpy(&log_tot_len_, src + OFFSET_LOG_TOT_LEN, sizeof(uint32_t));
        memcpy(&log_tid_, src + OFFSET_LOG_TID, sizeof(txn_id_t));
        memcpy(&prev_lsn_, src + OFFSET_PREV_LSN, sizeof(lsn_t));
    }
    // used for debug
    virtual void format_print() {
        std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        std::cout << "lsn: " << lsn_ << "\n";
        printf("log_tot_len: %u\n", log_tot_len_);
        printf("log_tid: %ld\n", static_cast<long>(log_tid_));
        std::cout << "prev_lsn: " << prev_lsn_ << "\n";
    }
};

class BeginLogRecord: public LogRecord {
public:
    BeginLogRecord() {
        log_type_ = LogType::begin;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() {
        log_tid_ = txn_id;
    }
    // 序列化Begin日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Begin日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);   
    }
    virtual void format_print() override {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
};

class CommitLogRecord: public LogRecord {
public:
    CommitLogRecord() {
        log_type_ = LogType::commit;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() { log_tid_ = txn_id; }
};

class AbortLogRecord: public LogRecord {
public:
    AbortLogRecord() {
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit AbortLogRecord(txn_id_t txn_id) : AbortLogRecord() { log_tid_ = txn_id; }
};

class CheckpointLogRecord: public LogRecord {
public:
    CheckpointLogRecord() {
        log_type_ = LogType::CHECKPOINT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
};

class InsertLogRecord: public LogRecord {
public:
    InsertLogRecord() {
        log_type_ = LogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    InsertLogRecord(txn_id_t txn_id, RmRecord& insert_value, Rid& rid, std::string table_name)
        : InsertLogRecord() {
        log_tid_ = txn_id;
        insert_value_ = insert_value;
        rid_ = rid;
        log_tot_len_ += serialized_record_len(insert_value_);
        log_tot_len_ += sizeof(Rid);
        table_name_ = std::move(table_name);
        log_tot_len_ += serialized_string_len(table_name_);
    }

    // 把insert日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        serialize_record(dest, offset, insert_value_);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        serialize_string(dest, offset, table_name_);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        insert_value_ = deserialize_record(src, offset);
        memcpy(&rid_, src + offset, sizeof(Rid));
        offset += sizeof(Rid);
        table_name_ = deserialize_string(src, offset);
    }
    void format_print() override {
        printf("insert record\n");
        LogRecord::format_print();
        printf("insert rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_.c_str());
    }

    RmRecord insert_value_;     // 插入的记录
    Rid rid_;                   // 记录插入的位置
    std::string table_name_;    // 插入记录的表名称
};

class DeleteLogRecord: public LogRecord {
public:
    DeleteLogRecord() {
        log_type_ = LogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    DeleteLogRecord(txn_id_t txn_id, RmRecord &delete_value, Rid &rid, std::string table_name)
        : DeleteLogRecord() {
        log_tid_ = txn_id;
        delete_value_ = delete_value;
        rid_ = rid;
        table_name_ = std::move(table_name);
        log_tot_len_ += serialized_record_len(delete_value_);
        log_tot_len_ += sizeof(Rid);
        log_tot_len_ += serialized_string_len(table_name_);
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        serialize_record(dest, offset, delete_value_);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        serialize_string(dest, offset, table_name_);
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        delete_value_ = deserialize_record(src, offset);
        memcpy(&rid_, src + offset, sizeof(Rid));
        offset += sizeof(Rid);
        table_name_ = deserialize_string(src, offset);
    }

    RmRecord delete_value_;
    Rid rid_;
    std::string table_name_;
};

class UpdateLogRecord: public LogRecord {
public:
    UpdateLogRecord() {
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    UpdateLogRecord(txn_id_t txn_id, RmRecord &old_value, RmRecord &new_value, Rid &rid, std::string table_name)
        : UpdateLogRecord() {
        log_tid_ = txn_id;
        old_value_ = old_value;
        new_value_ = new_value;
        rid_ = rid;
        table_name_ = std::move(table_name);
        log_tot_len_ += serialized_record_len(old_value_);
        log_tot_len_ += serialized_record_len(new_value_);
        log_tot_len_ += sizeof(Rid);
        log_tot_len_ += serialized_string_len(table_name_);
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        serialize_record(dest, offset, old_value_);
        serialize_record(dest, offset, new_value_);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        serialize_string(dest, offset, table_name_);
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        old_value_ = deserialize_record(src, offset);
        new_value_ = deserialize_record(src, offset);
        memcpy(&rid_, src + offset, sizeof(Rid));
        offset += sizeof(Rid);
        table_name_ = deserialize_string(src, offset);
    }

    RmRecord old_value_;
    RmRecord new_value_;
    Rid rid_;
    std::string table_name_;
};

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */

class LogBuffer {
public:
    LogBuffer() { offset_ = 0; }

    bool is_full(int append_size) const { return offset_ + append_size > LOG_BUFFER_SIZE; }

    void reset() { offset_ = 0; }

    char buffer_[LOG_BUFFER_SIZE+1];
    int offset_;    // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager {
public:
    LogManager(DiskManager* disk_manager) { disk_manager_ = disk_manager; }
    
    lsn_t add_log_to_buffer(LogRecord* log_record);
    void flush_log_to_disk(bool durable = false);

    LogBuffer* get_log_buffer() { return active_buffer_; }

private:
    void flush_log_to_disk_locked(std::unique_lock<std::mutex> *lock, bool durable);
    void init_lsn_locked();
    bool prepare_flush_locked(LogBuffer **flush_buffer, int *flush_size, lsn_t *flush_lsn);

    std::mutex latch_;                  // 用于对log_buffer_的互斥访问
    std::condition_variable flush_cv_;
    LogBuffer log_buffer_a_;
    LogBuffer log_buffer_b_;
    LogBuffer *active_buffer_{&log_buffer_a_};
    LogBuffer *flush_buffer_{&log_buffer_b_};
    lsn_t persist_lsn_{INVALID_LSN};    // 记录已经持久化到磁盘中的最后一条日志的日志号
    lsn_t durable_lsn_{INVALID_LSN};    // 已经通过 fdatasync 持久化的最后一条日志的日志号
    lsn_t next_lsn_{INVALID_LSN};       // 下一条日志在文件中的字节偏移
    bool flushing_{false};
    DiskManager* disk_manager_;
};
