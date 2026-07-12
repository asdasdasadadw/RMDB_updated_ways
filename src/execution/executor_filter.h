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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"

class FilterExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<Condition> conds_;
    std::unique_ptr<RmRecord> current_tuple_;

   public:
    FilterExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<Condition> conds) {
        prev_ = std::move(prev);
        conds_ = std::move(conds);
    }

    void beginTuple() override {
        current_tuple_.reset();
        prev_->beginTuple();
        find_next_valid();
    }

    void nextTuple() override {
        if (is_end()) {
            return;
        }
        prev_->nextTuple();
        find_next_valid();
    }

    void set_outer_tuple(const std::vector<Condition> &conds, const std::vector<ColMeta> &outer_cols,
                         const char *outer_data) override {
        prev_->set_outer_tuple(conds, outer_cols, outer_data);
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*current_tuple_);
    }

    bool is_end() const override { return current_tuple_ == nullptr; }

    size_t tupleLen() const override { return prev_->tupleLen(); }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    std::string getType() override { return "FilterExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return prev_->get_col_offset(target); }

    Rid &rid() override { return prev_->rid(); }

    size_t rows() const override { return rows_; }

    void reset_rows() override {
        rows_ = 0;
        prev_->reset_rows();
    }

    AbstractExecutor *child_at(size_t idx) override { return idx == 0 ? prev_.get() : nullptr; }

   private:
    void find_next_valid() {
        current_tuple_.reset();
        while (!prev_->is_end()) {
            auto rec = prev_->Next();
            if (eval_conds(conds_, prev_->cols(), rec->data)) {
                current_tuple_ = std::move(rec);
                rows_++;
                return;
            }
            prev_->nextTuple();
        }
    }
};
