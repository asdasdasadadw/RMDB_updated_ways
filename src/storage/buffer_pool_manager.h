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
#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "disk_manager.h"
#include "errors.h"
#include "page.h"

class LogManager;

class BufferPoolManager {
   private:
    struct BufferPoolShard {
        size_t pool_size_;
        Page *pages_;
        std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;
        std::list<frame_id_t> free_list_;
        std::unique_ptr<std::atomic<uint8_t>[]> clock_ref_;
        frame_id_t clock_hand_{0};
        std::shared_mutex latch_;

        explicit BufferPoolShard(size_t pool_size) : pool_size_(pool_size) {
            pages_ = new Page[pool_size_];
            clock_ref_ = std::make_unique<std::atomic<uint8_t>[]>(pool_size_);
            for (size_t i = 0; i < pool_size_; ++i) {
                clock_ref_[i].store(0, std::memory_order_relaxed);
                free_list_.emplace_back(static_cast<frame_id_t>(i));
            }
            page_table_.reserve(pool_size_ < 20000 ? pool_size_ : 20000);
        }

        ~BufferPoolShard() {
            delete[] pages_;
        }
    };

    static constexpr size_t MAX_SHARD_COUNT = 16;
    static constexpr size_t MIN_SHARDED_POOL_SIZE = 4096;
    static constexpr size_t PAGE_ALLOCATION_LATCH_COUNT = 256;

    size_t pool_size_;
    DiskManager *disk_manager_;
    std::vector<std::unique_ptr<BufferPoolShard>> shards_;
    size_t shard_count_{1};
    std::array<std::mutex, PAGE_ALLOCATION_LATCH_COUNT> page_allocation_latches_;
    LogManager *log_manager_{nullptr};

   public:
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
        shard_count_ = pool_size_ >= MIN_SHARDED_POOL_SIZE ? MAX_SHARD_COUNT : 1;
        if (pool_size_ < shard_count_) {
            shard_count_ = 1;
        }

        shards_.reserve(shard_count_);
        size_t base_size = pool_size_ / shard_count_;
        size_t remainder = pool_size_ % shard_count_;
        for (size_t i = 0; i < shard_count_; ++i) {
            size_t shard_size = base_size + (i < remainder ? 1 : 0);
            shards_.push_back(std::make_unique<BufferPoolShard>(shard_size));
        }
    }

    ~BufferPoolManager() = default;

    static void mark_dirty(Page *page) { page->is_dirty_.store(true, std::memory_order_relaxed); }

    void set_log_manager(LogManager *log_manager) { log_manager_ = log_manager; }

   public:
    Page *fetch_page(PageId page_id);

    bool unpin_page(PageId page_id, bool is_dirty);

    bool flush_page(PageId page_id);

    Page *new_page(PageId *page_id);

    bool delete_page(PageId page_id);

    void flush_all_pages(int fd);

   private:
    BufferPoolShard &get_shard(PageId page_id);

    bool find_victim_page(BufferPoolShard &shard, frame_id_t *frame_id);

    void update_page(BufferPoolShard &shard, Page *page, PageId new_page_id, frame_id_t new_frame_id);
};
