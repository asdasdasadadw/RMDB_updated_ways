/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

#include "recovery/log_manager.h"

BufferPoolManager::BufferPoolShard &BufferPoolManager::get_shard(PageId page_id) {
    size_t shard_no = PageIdHash{}(page_id) % shard_count_;
    return *shards_[shard_no];
}

bool BufferPoolManager::find_victim_page(BufferPoolShard &shard, frame_id_t *frame_id) {
    if (frame_id == nullptr) {
        return false;
    }
    if (!shard.free_list_.empty()) {
        *frame_id = shard.free_list_.front();
        shard.free_list_.pop_front();
        return true;
    }

    if (shard.pool_size_ == 0) {
        return false;
    }
    const size_t max_steps = shard.pool_size_ * 2;
    for (size_t step = 0; step < max_steps; ++step) {
        frame_id_t candidate = shard.clock_hand_;
        shard.clock_hand_ = static_cast<frame_id_t>((shard.clock_hand_ + 1) % shard.pool_size_);

        Page *page = &shard.pages_[candidate];
        if (page->pin_count_.load(std::memory_order_acquire) != 0) {
            continue;
        }
        if (shard.clock_ref_[candidate].load(std::memory_order_relaxed) != 0) {
            shard.clock_ref_[candidate].store(0, std::memory_order_relaxed);
            continue;
        }
        *frame_id = candidate;
        return true;
    }
    return false;
}

void BufferPoolManager::update_page(BufferPoolShard &shard, Page *page, PageId new_page_id,
                                    frame_id_t new_frame_id) {
    if (page == nullptr) {
        return;
    }
    if (page->is_dirty_.load(std::memory_order_acquire) && page->id_.page_no != INVALID_PAGE_ID) {
        if (log_manager_ != nullptr) {
            log_manager_->flush_log_to_disk(true);
        }
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_.store(false, std::memory_order_release);
    }
    if (page->id_.page_no != INVALID_PAGE_ID) {
        auto it = shard.page_table_.find(page->id_);
        if (it != shard.page_table_.end() && it->second == new_frame_id) {
            shard.page_table_.erase(it);
        }
    }
    page->reset_memory();
    page->id_ = new_page_id;
    page->pin_count_.store(0, std::memory_order_release);
    shard.clock_ref_[new_frame_id].store(0, std::memory_order_relaxed);
    if (new_page_id.page_no != INVALID_PAGE_ID) {
        shard.page_table_[new_page_id] = new_frame_id;
    }
}

Page *BufferPoolManager::fetch_page(PageId page_id) {
    BufferPoolShard &shard = get_shard(page_id);
    {
        std::shared_lock read_lock{shard.latch_};
        auto it = shard.page_table_.find(page_id);
        if (it != shard.page_table_.end()) {
            frame_id_t frame_id = it->second;
            Page *page = &shard.pages_[frame_id];
            page->pin_count_.fetch_add(1, std::memory_order_acq_rel);
            return page;
        }
    }

    std::unique_lock write_lock{shard.latch_};
    auto it = shard.page_table_.find(page_id);
    if (it != shard.page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page *page = &shard.pages_[frame_id];
        page->pin_count_.fetch_add(1, std::memory_order_acq_rel);
        return page;
    }

    frame_id_t victim_frame = INVALID_FRAME_ID;
    if (!find_victim_page(shard, &victim_frame)) {
        return nullptr;
    }

    Page *victim_page = &shard.pages_[victim_frame];
    update_page(shard, victim_page, page_id, victim_frame);
    disk_manager_->read_page(page_id.fd, page_id.page_no, victim_page->data_, PAGE_SIZE);
    victim_page->pin_count_.store(1, std::memory_order_release);
    return victim_page;
}

bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    BufferPoolShard &shard = get_shard(page_id);
    std::shared_lock lock{shard.latch_};

    auto it = shard.page_table_.find(page_id);
    if (it == shard.page_table_.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    Page *page = &shard.pages_[frame_id];
    int prev_pin = page->pin_count_.fetch_sub(1, std::memory_order_acq_rel);
    if (prev_pin <= 0) {
        page->pin_count_.fetch_add(1, std::memory_order_acq_rel);
        return false;
    }
    if (is_dirty) {
        page->is_dirty_.store(true, std::memory_order_release);
    }
    if (prev_pin == 1) {
        shard.clock_ref_[frame_id].store(1, std::memory_order_relaxed);
    }
    return true;
}

bool BufferPoolManager::flush_page(PageId page_id) {
    BufferPoolShard &shard = get_shard(page_id);
    std::unique_lock lock{shard.latch_};

    if (page_id.page_no == INVALID_PAGE_ID) {
        return false;
    }
    auto it = shard.page_table_.find(page_id);
    if (it == shard.page_table_.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    Page *page = &shard.pages_[frame_id];
    if (log_manager_ != nullptr && page->is_dirty_.load(std::memory_order_acquire)) {
        log_manager_->flush_log_to_disk(true);
    }
    disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    page->is_dirty_.store(false, std::memory_order_release);
    return true;
}

Page *BufferPoolManager::new_page(PageId *page_id) {
    if (page_id == nullptr) {
        return nullptr;
    }
    int fd = page_id->fd;
    if (fd < 0) {
        return nullptr;
    }

    auto &allocation_latch = page_allocation_latches_[static_cast<size_t>(fd) % PAGE_ALLOCATION_LATCH_COUNT];
    std::scoped_lock new_page_lock{allocation_latch};
    PageId predicted_id{fd, disk_manager_->get_fd2pageno(fd)};
    BufferPoolShard &shard = get_shard(predicted_id);
    std::scoped_lock shard_lock{shard.latch_};

    frame_id_t victim_frame = INVALID_FRAME_ID;
    if (!find_victim_page(shard, &victim_frame)) {
        return nullptr;
    }

    page_id_t new_page_no = disk_manager_->allocate_page(fd);
    PageId new_id{fd, new_page_no};

    Page *page = &shard.pages_[victim_frame];
    update_page(shard, page, new_id, victim_frame);
    page->pin_count_.store(1, std::memory_order_release);
    *page_id = new_id;
    return page;
}

bool BufferPoolManager::delete_page(PageId page_id) {
    BufferPoolShard &shard = get_shard(page_id);
    std::unique_lock lock{shard.latch_};

    auto it = shard.page_table_.find(page_id);
    if (it == shard.page_table_.end()) {
        return true;
    }
    frame_id_t frame_id = it->second;
    Page *page = &shard.pages_[frame_id];
    if (page->pin_count_.load(std::memory_order_acquire) != 0) {
        return false;
    }

    if (page->is_dirty_.load(std::memory_order_acquire)) {
        if (log_manager_ != nullptr) {
            log_manager_->flush_log_to_disk(true);
        }
        disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_.store(false, std::memory_order_release);
    }

    shard.page_table_.erase(it);
    page->reset_memory();
    page->id_ = PageId{0, INVALID_PAGE_ID};
    page->pin_count_.store(0, std::memory_order_release);
    shard.clock_ref_[frame_id].store(0, std::memory_order_relaxed);
    shard.free_list_.push_back(frame_id);
    return true;
}

void BufferPoolManager::flush_all_pages(int fd) {
    for (auto &shard_ptr : shards_) {
        BufferPoolShard &shard = *shard_ptr;
        std::scoped_lock lock{shard.latch_};
        for (size_t i = 0; i < shard.pool_size_; i++) {
            Page *page = &shard.pages_[i];
            if (page->id_.page_no == INVALID_PAGE_ID) {
                continue;
            }
            if (page->id_.fd != fd) {
                continue;
            }
            if (!page->is_dirty_.load(std::memory_order_acquire)) {
                continue;
            }
            if (log_manager_ != nullptr) {
                log_manager_->flush_log_to_disk(true);
            }
            disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
            page->is_dirty_.store(false, std::memory_order_release);
        }
    }
}
