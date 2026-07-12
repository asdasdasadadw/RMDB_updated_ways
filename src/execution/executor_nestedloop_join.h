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
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;
    std::unique_ptr<RmRecord> current_tuple_;
    std::unique_ptr<RmRecord> current_left_tuple_;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    void beginTuple() override {
        left_->beginTuple();
        isend = false;
        current_tuple_.reset();
        current_left_tuple_.reset();
        if (!left_->is_end()) {
            current_left_tuple_ = left_->Next();
            open_right_for_current_left();
        }
        find_next_valid();
    }

    void nextTuple() override {
        if (isend) {
            return;
        }
        right_->nextTuple();
        find_next_valid();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (isend || current_tuple_ == nullptr) {
            return nullptr;
        }
        rows_++;
        return std::make_unique<RmRecord>(*current_tuple_);
    }

    bool is_end() const override { return isend; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "NestedLoopJoinExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return _abstract_rid; }

    size_t rows() const override { return rows_; }

    void reset_rows() override {
        rows_ = 0;
        left_->reset_rows();
        right_->reset_rows();
    }

    AbstractExecutor *child_at(size_t idx) override {
        if (idx == 0) {
            return left_.get();
        }
        if (idx == 1) {
            return right_.get();
        }
        return nullptr;
    }

   private:
    std::unique_ptr<RmRecord> make_join_record(const RmRecord &left_rec, const RmRecord &right_rec) const {
        auto rec = std::make_unique<RmRecord>(len_);
        memcpy(rec->data, left_rec.data, left_->tupleLen());
        memcpy(rec->data + left_->tupleLen(), right_rec.data, right_->tupleLen());
        return rec;
    }

    void find_next_valid() {
        while (!left_->is_end()) {
            if (current_left_tuple_ == nullptr) {
                current_left_tuple_ = left_->Next();
                open_right_for_current_left();
            }
            while (!right_->is_end()) {
                auto right_rec = right_->Next();
                auto joined = make_join_record(*current_left_tuple_, *right_rec);
                if (eval_conds(fed_conds_, cols_, joined->data)) {
                    current_tuple_ = std::move(joined);
                    isend = false;
                    return;
                }
                right_->nextTuple();
            }
            left_->nextTuple();
            current_left_tuple_.reset();
        }
        current_tuple_.reset();
        isend = true;
    }

    void open_right_for_current_left() {
        right_->set_outer_tuple(fed_conds_, left_->cols(),
                                current_left_tuple_ == nullptr ? nullptr : current_left_tuple_->data);
        right_->beginTuple();
    }
};
