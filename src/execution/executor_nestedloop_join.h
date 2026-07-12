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

    struct JoinCondition {
        bool lhs_left;
        int lhs_offset;
        ColType type;
        int len;
        CompOp op;
        bool rhs_is_value;
        bool rhs_left;
        int rhs_offset;
        const char *rhs_value;
    };
    std::vector<JoinCondition> fast_conds_;

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
        fast_conds_ = compile_join_conds(left_->cols(), right_->cols(), fed_conds_);
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
                if (eval_join_conds(current_left_tuple_->data, right_rec->data)) {
                    current_tuple_ = make_join_record(*current_left_tuple_, *right_rec);
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

    static bool find_join_col(const std::vector<ColMeta> &left_cols, const std::vector<ColMeta> &right_cols,
                              const TabCol &target, bool *from_left, const ColMeta **col) {
        auto left = std::find_if(left_cols.begin(), left_cols.end(), [&](const ColMeta &item) {
            return item.tab_name == target.tab_name && item.name == target.col_name;
        });
        if (left != left_cols.end()) {
            *from_left = true;
            *col = &(*left);
            return true;
        }
        auto right = std::find_if(right_cols.begin(), right_cols.end(), [&](const ColMeta &item) {
            return item.tab_name == target.tab_name && item.name == target.col_name;
        });
        if (right != right_cols.end()) {
            *from_left = false;
            *col = &(*right);
            return true;
        }
        return false;
    }

    static std::vector<JoinCondition> compile_join_conds(const std::vector<ColMeta> &left_cols,
                                                         const std::vector<ColMeta> &right_cols,
                                                         const std::vector<Condition> &conds) {
        std::vector<JoinCondition> fast;
        fast.reserve(conds.size());
        for (const auto &cond : conds) {
            bool lhs_left = true;
            const ColMeta *lhs_col = nullptr;
            if (!find_join_col(left_cols, right_cols, cond.lhs_col, &lhs_left, &lhs_col)) {
                throw ColumnNotFoundError(cond.lhs_col.tab_name + "." + cond.lhs_col.col_name);
            }

            JoinCondition item{};
            item.lhs_left = lhs_left;
            item.lhs_offset = lhs_col->offset;
            item.type = lhs_col->type;
            item.len = lhs_col->len;
            item.op = cond.op;
            item.rhs_is_value = cond.is_rhs_val;
            item.rhs_left = true;
            item.rhs_offset = 0;
            item.rhs_value = nullptr;
            if (cond.is_rhs_val) {
                item.rhs_value = cond.rhs_val.raw->data;
            } else {
                bool rhs_left = true;
                const ColMeta *rhs_col = nullptr;
                if (!find_join_col(left_cols, right_cols, cond.rhs_col, &rhs_left, &rhs_col)) {
                    throw ColumnNotFoundError(cond.rhs_col.tab_name + "." + cond.rhs_col.col_name);
                }
                item.rhs_left = rhs_left;
                item.rhs_offset = rhs_col->offset;
            }
            fast.push_back(item);
        }
        return fast;
    }

    bool eval_join_conds(const char *left_data, const char *right_data) const {
        for (const auto &cond : fast_conds_) {
            const char *lhs_base = cond.lhs_left ? left_data : right_data;
            const char *rhs = cond.rhs_value;
            if (!cond.rhs_is_value) {
                const char *rhs_base = cond.rhs_left ? left_data : right_data;
                rhs = rhs_base + cond.rhs_offset;
            }
            if (!compare_result(compare_raw(lhs_base + cond.lhs_offset, rhs, cond.type, cond.len), cond.op)) {
                return false;
            }
        }
        return true;
    }
};
