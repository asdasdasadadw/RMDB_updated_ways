/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_manager.h"

#include <algorithm>
#include <chrono>
#include <cstring>

namespace {

constexpr auto GROUP_COMMIT_DELAY = std::chrono::microseconds(200);

}  // namespace

void LogManager::init_lsn_locked() {
    if (next_lsn_ != INVALID_LSN) {
        return;
    }
    int file_size = disk_manager_->is_file(LOG_FILE_NAME) ? disk_manager_->get_file_size(LOG_FILE_NAME) : 0;
    next_lsn_ = std::max(0, file_size);
    persist_lsn_ = next_lsn_ == 0 ? INVALID_LSN : next_lsn_ - 1;
}

void LogManager::flush_log_to_disk_locked(std::unique_lock<std::mutex> *lock, bool durable) {
    LogBuffer *flush_buffer = nullptr;
    int flush_size = 0;
    lsn_t flush_lsn = INVALID_LSN;
    if (!prepare_flush_locked(&flush_buffer, &flush_size, &flush_lsn)) {
        if (durable) {
            if (lock != nullptr && lock->owns_lock()) {
                lock->unlock();
                disk_manager_->sync_log();
                lock->lock();
            } else {
                disk_manager_->sync_log();
            }
        }
        return;
    }
    if (lock != nullptr && lock->owns_lock()) {
        lock->unlock();
    }
    disk_manager_->write_log(flush_buffer->buffer_, flush_size);
    if (durable) {
        disk_manager_->sync_log();
    }
    if (lock != nullptr) {
        lock->lock();
    }
    flush_buffer->reset();
    persist_lsn_ = std::max(persist_lsn_, flush_lsn);
    flushing_ = false;
    flush_cv_.notify_all();
}

bool LogManager::prepare_flush_locked(LogBuffer **flush_buffer, int *flush_size, lsn_t *flush_lsn) {
    if (active_buffer_->offset_ == 0) {
        return false;
    }
    init_lsn_locked();
    std::swap(active_buffer_, flush_buffer_);
    *flush_buffer = flush_buffer_;
    *flush_size = flush_buffer_->offset_;
    *flush_lsn = next_lsn_ - 1;
    active_buffer_->reset();
    flushing_ = true;
    return true;
}

lsn_t LogManager::add_log_to_buffer(LogRecord *log_record) {
    if (log_record == nullptr) {
        return INVALID_LSN;
    }
    std::unique_lock<std::mutex> lock(latch_);
    if (log_record->log_tot_len_ > LOG_BUFFER_SIZE) {
        throw InternalError("Log record is too large");
    }
    if (active_buffer_->is_full(static_cast<int>(log_record->log_tot_len_))) {
        while (flushing_) {
            flush_cv_.wait(lock);
        }
        flush_log_to_disk_locked(&lock, false);
    }
    init_lsn_locked();
    log_record->lsn_ = next_lsn_;
    log_record->serialize(active_buffer_->buffer_ + active_buffer_->offset_);
    active_buffer_->offset_ += static_cast<int>(log_record->log_tot_len_);
    next_lsn_ += static_cast<lsn_t>(log_record->log_tot_len_);
    return log_record->lsn_;
}

void LogManager::flush_log_to_disk(bool durable) {
    std::unique_lock<std::mutex> lock(latch_);
    init_lsn_locked();
    lsn_t target_lsn = next_lsn_ == 0 ? INVALID_LSN : next_lsn_ - 1;
    if (target_lsn == INVALID_LSN || persist_lsn_ >= target_lsn) {
        return;
    }

    while (persist_lsn_ < target_lsn) {
        if (flushing_) {
            flush_cv_.wait(lock, [&] { return !flushing_ || persist_lsn_ >= target_lsn; });
            continue;
        }
        flush_cv_.wait_for(lock, GROUP_COMMIT_DELAY);
        if (flushing_ || persist_lsn_ >= target_lsn) {
            continue;
        }
        flush_log_to_disk_locked(&lock, durable);
    }
    if (durable) {
        lock.unlock();
        disk_manager_->sync_log();
    }
}
