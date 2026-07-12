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
#include <algorithm>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<OrderByCol> order_cols_;
    std::vector<ColMeta> key_cols_;
    size_t tuple_num;
    int limit_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    std::vector<ColMeta> result_cols_;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t cursor_;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<OrderByCol> order_cols, int limit) {
        prev_ = std::move(prev);
        order_cols_ = std::move(order_cols);
        for (auto &order_col : order_cols_) {
            key_cols_.push_back(prev_->get_col_offset(order_col.col));
        }
        result_cols_ = prev_->cols();
        limit_ = limit;
        tuple_num = 0;
        used_tuple.clear();
        cursor_ = 0;
    }

    void beginTuple() override {
        tuples_.clear();
        prev_->beginTuple();
        if (!key_cols_.empty() && limit_ > 0) {
            load_topn();
        } else {
            while (!prev_->is_end()) {
                tuples_.push_back(prev_->Next());
                prev_->nextTuple();
            }
        }
        if (!key_cols_.empty()) {
            std::stable_sort(tuples_.begin(), tuples_.end(), [&](const std::unique_ptr<RmRecord> &lhs,
                                                                 const std::unique_ptr<RmRecord> &rhs) {
                return compare_tuple(lhs.get(), rhs.get()) < 0;
            });
        }
        if (limit_ >= 0 && tuples_.size() > static_cast<size_t>(limit_)) {
            tuples_.resize(static_cast<size_t>(limit_));
        }
        tuple_num = tuples_.size();
        cursor_ = 0;
    }

    void nextTuple() override {
        if (cursor_ < tuples_.size()) {
            cursor_++;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        rows_++;
        return std::make_unique<RmRecord>(*tuples_[cursor_]);
    }

    bool is_end() const override { return cursor_ >= tuples_.size(); }

    size_t tupleLen() const override { return prev_->tupleLen(); }

    const std::vector<ColMeta> &cols() const override { return result_cols_; }

    std::string getType() override { return "SortExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(result_cols_, target); }

    Rid &rid() override { return _abstract_rid; }

    size_t rows() const override { return rows_; }

    void reset_rows() override {
        rows_ = 0;
        prev_->reset_rows();
    }

    AbstractExecutor *child_at(size_t idx) override { return idx == 0 ? prev_.get() : nullptr; }

   private:
    int compare_tuple(const RmRecord *lhs, const RmRecord *rhs) const {
        for (size_t i = 0; i < key_cols_.size(); ++i) {
            const auto &col = key_cols_[i];
            int cmp = compare_raw(lhs->data + col.offset, rhs->data + col.offset, col.type, col.len);
            if (cmp != 0) {
                return order_cols_[i].is_desc ? -cmp : cmp;
            }
        }
        return 0;
    }

    void load_topn() {
        size_t keep = static_cast<size_t>(limit_);
        auto worse_first = [&](const std::unique_ptr<RmRecord> &lhs,
                               const std::unique_ptr<RmRecord> &rhs) {
            return compare_tuple(lhs.get(), rhs.get()) < 0;
        };
        while (!prev_->is_end()) {
            auto rec = prev_->Next();
            if (tuples_.size() < keep) {
                tuples_.push_back(std::move(rec));
                if (tuples_.size() == keep) {
                    std::make_heap(tuples_.begin(), tuples_.end(), worse_first);
                }
            } else if (compare_tuple(rec.get(), tuples_.front().get()) < 0) {
                std::pop_heap(tuples_.begin(), tuples_.end(), worse_first);
                tuples_.back() = std::move(rec);
                std::push_heap(tuples_.begin(), tuples_.end(), worse_first);
            }
            prev_->nextTuple();
        }
    }
};
