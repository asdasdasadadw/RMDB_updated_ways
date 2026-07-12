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

#include <cassert>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "disk_manager.h"
#include "errors.h"
#include "page.h"
#include "replacer/lru_replacer.h"
#include "replacer/replacer.h"

class LogManager;

/**
 * @description: Sharded buffer pool manager.
 *                Pages are hashed into one of NUM_SHARDS independent buffer
 *                pool instances. Each instance has its own frames, page table,
 *                free list, replacer and mutex, so concurrent accesses to
 *                different pages rarely contend on the same lock.
 */
class BufferPoolManager {
   private:
    static constexpr size_t MAX_SHARDS = 16;          // must be power of 2
    static constexpr size_t SHARD_MASK = MAX_SHARDS - 1;
    static constexpr size_t MIN_FRAMES_PER_SHARD = 4;

    /**
     * @description: One independent buffer pool shard.
     */
    struct Shard {
        size_t pool_size_;      // number of frames in this shard
        Page *pages_;           // frame array for this shard
        std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;
        std::list<frame_id_t> free_list_;
        Replacer *replacer_;
        std::mutex latch_;
        DiskManager *disk_manager_;
        LogManager *log_manager_{nullptr};

        Shard(size_t pool_size, DiskManager *disk_manager)
            : pool_size_(pool_size), disk_manager_(disk_manager) {
            pages_ = new Page[pool_size_];
            if (REPLACER_TYPE.compare("LRU") == 0 || REPLACER_TYPE.compare("CLOCK") == 0) {
                replacer_ = new LRUReplacer(pool_size_);
            } else {
                replacer_ = new LRUReplacer(pool_size_);
            }
            for (size_t i = 0; i < pool_size_; ++i) {
                free_list_.emplace_back(static_cast<frame_id_t>(i));
            }
        }

        ~Shard() {
            delete[] pages_;
            delete replacer_;
        }

        Page *fetch_page(PageId page_id);
        bool unpin_page(PageId page_id, bool is_dirty);
        bool flush_page(PageId page_id);
        Page *new_page(PageId *page_id);
        bool delete_page(PageId page_id);
        void flush_all_pages(int fd);

       private:
        bool find_victim_page(frame_id_t *frame_id);
        void update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id);
    };

   private:
    DiskManager *disk_manager_;
    std::vector<std::unique_ptr<Shard>> shards_;
    size_t num_shards_;
    LogManager *log_manager_{nullptr};

   public:
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : disk_manager_(disk_manager) {
        // Use a single shard for tiny pools (unit tests); otherwise shard.
        size_t max_shards_by_size = pool_size / MIN_FRAMES_PER_SHARD;
        num_shards_ = 1;
        for (size_t s = 2; s <= MAX_SHARDS; s *= 2) {
            if (s <= max_shards_by_size) {
                num_shards_ = s;
            }
        }
        size_t shard_size = pool_size / num_shards_;
        for (size_t i = 0; i < num_shards_; ++i) {
            shards_.emplace_back(std::make_unique<Shard>(shard_size, disk_manager_));
        }
    }

    ~BufferPoolManager() = default;

    static void mark_dirty(Page *page) { page->is_dirty_ = true; }

    void set_log_manager(LogManager *log_manager) {
        log_manager_ = log_manager;
        for (auto &shard : shards_) {
            shard->log_manager_ = log_manager_;
        }
    }

    Page *fetch_page(PageId page_id) {
        return shards_[shard_id(page_id)]->fetch_page(page_id);
    }

    bool unpin_page(PageId page_id, bool is_dirty) {
        return shards_[shard_id(page_id)]->unpin_page(page_id, is_dirty);
    }

    bool flush_page(PageId page_id) {
        return shards_[shard_id(page_id)]->flush_page(page_id);
    }

    Page *new_page(PageId *page_id) {
        if (page_id == nullptr) {
            return nullptr;
        }
        // After disk_manager allocates a page_no, the shard is fixed.
        page_id_t new_page_no = disk_manager_->allocate_page(page_id->fd);
        page_id->page_no = new_page_no;
        return shards_[shard_id(*page_id)]->new_page(page_id);
    }

    bool delete_page(PageId page_id) {
        return shards_[shard_id(page_id)]->delete_page(page_id);
    }

    void flush_all_pages(int fd) {
        for (auto &shard : shards_) {
            shard->flush_all_pages(fd);
        }
    }

   private:
    size_t shard_id(PageId page_id) const {
        return PageIdHash{}(page_id) & (num_shards_ - 1);
    }
};
