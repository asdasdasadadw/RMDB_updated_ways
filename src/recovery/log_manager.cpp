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
#include <exception>
#include <limits>

namespace {

constexpr auto GROUP_COMMIT_DELAY = std::chrono::microseconds(200);

}  // namespace

void LogManager::init_lsn_locked() {
    if (next_lsn_ != INVALID_LSN) {
        return;
    }
    wal_offset_t file_size =
        disk_manager_->is_file(LOG_FILE_NAME) ? disk_manager_->get_file_size(LOG_FILE_NAME) : 0;
    if (file_size < 0) {
        throw InternalError("Failed to determine WAL size");
    }
    next_lsn_ = file_size;
    persist_lsn_ = next_lsn_ == 0 ? INVALID_LSN : next_lsn_ - 1;
    durable_lsn_ = persist_lsn_;
}

void LogManager::flush_log_to_disk_locked(std::unique_lock<std::mutex> *lock, bool durable) {
    LogBuffer *flush_buffer = nullptr;
    int flush_size = 0;
    lsn_t flush_lsn = INVALID_LSN;
    if (!prepare_flush_locked(&flush_buffer, &flush_size, &flush_lsn)) {
        if (durable) {
            lsn_t sync_lsn = persist_lsn_;
            try {
                if (lock != nullptr && lock->owns_lock()) {
                    lock->unlock();
                    disk_manager_->sync_log();
                    lock->lock();
                } else {
                    disk_manager_->sync_log();
                }
            } catch (...) {
                // Once WAL I/O has started its outcome is unknown.  Continuing
                // would either strand flushing_ or allow pages/transactions to
                // advance past an unverified WAL boundary, so recovery must
                // resolve the outcome in a fresh process.
                std::terminate();
            }
            durable_lsn_ = std::max(durable_lsn_, sync_lsn);
        }
        return;
    }
    if (lock != nullptr && lock->owns_lock()) {
        lock->unlock();
    }
    try {
        disk_manager_->write_log(flush_buffer->buffer_, flush_size);
        if (durable) {
            disk_manager_->sync_log();
        }
    } catch (...) {
        std::terminate();
    }
    if (lock != nullptr) {
        lock->lock();
    }
    flush_buffer->reset();
    persist_lsn_ = std::max(persist_lsn_, flush_lsn);
    if (durable) {
        durable_lsn_ = std::max(durable_lsn_, flush_lsn);
    }
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
    if (next_lsn_ > std::numeric_limits<lsn_t>::max() - log_record->log_tot_len_) {
        throw InternalError("WAL LSN overflow");
    }
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
    if (target_lsn == INVALID_LSN) {
        return;
    }
    if (!durable && persist_lsn_ >= target_lsn) {
        return;
    }
    if (durable && durable_lsn_ >= target_lsn) {
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
    if (durable && durable_lsn_ < target_lsn) {
        lsn_t sync_lsn = persist_lsn_;
        lock.unlock();
        try {
            disk_manager_->sync_log();
        } catch (...) {
            std::terminate();
        }
        lock.lock();
        durable_lsn_ = std::max(durable_lsn_, sync_lsn);
    }
}
