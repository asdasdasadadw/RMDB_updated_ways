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
#include <cstring>
#include <memory>
#include <unordered_set>
#include <vector>

#include "errors.h"
#include "execution_defs.h"
#include "executor_abstract.h"

class UnionExecutor : public AbstractExecutor {
   private:
    std::vector<std::unique_ptr<AbstractExecutor>> children_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t cursor_ = 0;

   public:
    UnionExecutor(std::vector<std::unique_ptr<AbstractExecutor>> children, std::vector<ColMeta> cols) {
        children_ = std::move(children);
        cols_ = std::move(cols);
        len_ = 0;
        for (auto &col : cols_) {
            col.offset = static_cast<int>(len_);
            len_ += col.len;
        }
    }

    void beginTuple() override {
        tuples_.clear();
        cursor_ = 0;
        std::unordered_set<std::string> seen;
        for (auto &child : children_) {
            child->beginTuple();
            while (!child->is_end()) {
                auto child_rec = child->Next();
                auto tuple = cast_tuple(child->cols(), child_rec->data);
                std::string key(tuple->data, tuple->size);
                if (seen.insert(key).second) {
                    tuples_.push_back(std::move(tuple));
                }
                child->nextTuple();
            }
        }
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

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "UnionExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return _abstract_rid; }

    size_t rows() const override { return rows_; }

    void reset_rows() override {
        rows_ = 0;
        for (auto &child : children_) {
            child->reset_rows();
        }
    }

    AbstractExecutor *child_at(size_t idx) override {
        return idx < children_.size() ? children_[idx].get() : nullptr;
    }

   private:
    std::unique_ptr<RmRecord> cast_tuple(const std::vector<ColMeta> &child_cols, const char *child_data) const {
        if (child_cols.size() != cols_.size()) {
            throw RMDBError("UNION branch output column count mismatch");
        }
        auto tuple = std::make_unique<RmRecord>(static_cast<int>(len_));
        memset(tuple->data, 0, len_);
        for (size_t i = 0; i < cols_.size(); ++i) {
            const auto &src = child_cols[i];
            const auto &dst = cols_[i];
            const char *src_data = child_data + src.offset;
            char *dst_data = tuple->data + dst.offset;
            if (src.type == dst.type) {
                if (dst.type == TYPE_STRING) {
                    memcpy(dst_data, src_data, std::min(src.len, dst.len));
                } else {
                    memcpy(dst_data, src_data, dst.len);
                }
            } else if (src.type == TYPE_INT && dst.type == TYPE_FLOAT) {
                float value = static_cast<float>(*reinterpret_cast<const int *>(src_data));
                memcpy(dst_data, &value, sizeof(float));
            } else {
                throw IncompatibleTypeError(coltype2str(dst.type), coltype2str(src.type));
            }
        }
        return tuple;
    }
};
