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

bool BufferPoolManager::Shard::find_victim_page(frame_id_t *frame_id) {
    if (frame_id == nullptr) {
        return false;
    }
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    return replacer_->victim(frame_id);
}

void BufferPoolManager::Shard::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    if (page == nullptr) {
        return;
    }
    if (page->is_dirty_ && page->id_.page_no != INVALID_PAGE_ID) {
        if (log_manager_ != nullptr) {
            log_manager_->flush_log_to_disk();
        }
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }
    if (page->id_.page_no != INVALID_PAGE_ID) {
        auto it = page_table_.find(page->id_);
        if (it != page_table_.end() && it->second == new_frame_id) {
            page_table_.erase(it);
        }
    }
    page->reset_memory();
    page->id_ = new_page_id;
    page->pin_count_ = 0;
    if (new_page_id.page_no != INVALID_PAGE_ID) {
        page_table_[new_page_id] = new_frame_id;
    }
}

Page *BufferPoolManager::Shard::fetch_page(PageId page_id) {
    std::scoped_lock lock{latch_};
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page *page = &pages_[frame_id];
        page->pin_count_++;
        replacer_->pin(frame_id);
        return page;
    }

    frame_id_t victim_frame = INVALID_FRAME_ID;
    if (!find_victim_page(&victim_frame)) {
        return nullptr;
    }

    Page *victim_page = &pages_[victim_frame];
    update_page(victim_page, page_id, victim_frame);
    disk_manager_->read_page(page_id.fd, page_id.page_no, victim_page->data_, PAGE_SIZE);
    victim_page->pin_count_ = 1;
    replacer_->pin(victim_frame);
    return victim_page;
}

bool BufferPoolManager::Shard::unpin_page(PageId page_id, bool is_dirty) {
    std::scoped_lock lock{latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    if (page->pin_count_ <= 0) {
        return false;
    }
    page->pin_count_--;
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }
    return true;
}

bool BufferPoolManager::Shard::flush_page(PageId page_id) {
    std::scoped_lock lock{latch_};
    if (page_id.page_no == INVALID_PAGE_ID) {
        return false;
    }
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    if (log_manager_ != nullptr && page->is_dirty_) {
        log_manager_->flush_log_to_disk();
    }
    disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    page->is_dirty_ = false;
    return true;
}

Page *BufferPoolManager::Shard::new_page(PageId *page_id) {
    if (page_id == nullptr) {
        return nullptr;
    }
    int fd = page_id->fd;
    if (fd < 0) {
        return nullptr;
    }

    std::scoped_lock lock{latch_};
    frame_id_t victim_frame = INVALID_FRAME_ID;
    if (!find_victim_page(&victim_frame)) {
        return nullptr;
    }

    PageId new_id{fd, page_id->page_no};
    Page *page = &pages_[victim_frame];
    update_page(page, new_id, victim_frame);
    page->pin_count_ = 1;
    replacer_->pin(victim_frame);
    return page;
}

bool BufferPoolManager::Shard::delete_page(PageId page_id) {
    std::scoped_lock lock{latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return true;
    }
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    if (page->pin_count_ != 0) {
        return false;
    }

    replacer_->pin(frame_id);

    if (page->is_dirty_) {
        if (log_manager_ != nullptr) {
            log_manager_->flush_log_to_disk();
        }
        disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }
    page_table_.erase(it);
    page->reset_memory();
    page->id_ = PageId{0, INVALID_PAGE_ID};
    page->pin_count_ = 0;
    free_list_.push_back(frame_id);
    return true;
}

void BufferPoolManager::Shard::flush_all_pages(int fd) {
    std::scoped_lock lock{latch_};
    for (size_t i = 0; i < pool_size_; i++) {
        Page *page = &pages_[i];
        if (page->id_.page_no == INVALID_PAGE_ID) {
            continue;
        }
        if (page->id_.fd != fd) {
            continue;
        }
        if (!page->is_dirty_) {
            continue;
        }
        if (log_manager_ != nullptr) {
            log_manager_->flush_log_to_disk();
        }
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }
}
