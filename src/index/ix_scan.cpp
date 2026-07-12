/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"

IxScan::~IxScan() {
    release_node();
}

void IxScan::release_node() {
    if (node_ != nullptr) {
        ih_->release_node(node_, false);
        node_ = nullptr;
    }
}

/**
 * @brief
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::next() {
    assert(!is_end());
    assert(node_ != nullptr);
    assert(node_->is_leaf_page());
    assert(iid_.slot_no < node_->get_size());
    // increment slot no
    iid_.slot_no++;
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node_->get_size()) {
        // go to next leaf
        page_id_t next = node_->get_next_leaf();
        release_node();
        iid_.slot_no = 0;
        iid_.page_no = next;
        if (!is_end()) {
            node_ = ih_->fetch_node(iid_.page_no);
        }
    } else if (is_end()) {
        release_node();
    }
}

Rid IxScan::rid() const {
    assert(!is_end());
    assert(node_ != nullptr);
    assert(iid_.page_no == node_->get_page_no());
    return *node_->get_rid(iid_.slot_no);
}
