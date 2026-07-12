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

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class AbstractExecutor {
   public:
    struct FastCondition {
        int lhs_offset;
        ColType type;
        int len;
        CompOp op;
        bool rhs_is_value;
        int rhs_offset;
        const char *rhs_value;
    };

    Rid _abstract_rid;

    Context *context_;
    size_t rows_ = 0;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual void set_outer_tuple(const std::vector<Condition> &, const std::vector<ColMeta> &, const char *) {}

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    virtual size_t rows() const { return rows_; }

    virtual void reset_rows() { rows_ = 0; }

    virtual AbstractExecutor *child_at(size_t) { return nullptr; }

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    static int compare_raw(const char *lhs, const char *rhs, ColType type, int len) {
        if (type == TYPE_INT) {
            int l = *reinterpret_cast<const int *>(lhs);
            int r = *reinterpret_cast<const int *>(rhs);
            return (l > r) - (l < r);
        }
        if (type == TYPE_FLOAT) {
            float l = *reinterpret_cast<const float *>(lhs);
            float r = *reinterpret_cast<const float *>(rhs);
            return (l > r) - (l < r);
        }
        if (type == TYPE_STRING) {
            return memcmp(lhs, rhs, len);
        }
        throw InternalError("Unexpected column type");
    }

    static bool compare_result(int cmp, CompOp op) {
        switch (op) {
            case OP_EQ:
                return cmp == 0;
            case OP_NE:
                return cmp != 0;
            case OP_LT:
                return cmp < 0;
            case OP_GT:
                return cmp > 0;
            case OP_LE:
                return cmp <= 0;
            case OP_GE:
                return cmp >= 0;
            default:
                throw InternalError("Unexpected comparison operator");
        }
    }

    static bool eval_cond(const Condition &cond, const std::vector<ColMeta> &rec_cols, const char *rec_data) {
        auto lhs_col = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == cond.lhs_col.tab_name && col.name == cond.lhs_col.col_name;
        });
        if (lhs_col == rec_cols.end()) {
            throw ColumnNotFoundError(cond.lhs_col.tab_name + "." + cond.lhs_col.col_name);
        }

        const char *lhs = rec_data + lhs_col->offset;
        const char *rhs = nullptr;
        if (cond.is_rhs_val) {
            rhs = cond.rhs_val.raw->data;
        } else {
            auto rhs_col = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
                return col.tab_name == cond.rhs_col.tab_name && col.name == cond.rhs_col.col_name;
            });
            if (rhs_col == rec_cols.end()) {
                throw ColumnNotFoundError(cond.rhs_col.tab_name + "." + cond.rhs_col.col_name);
            }
            rhs = rec_data + rhs_col->offset;
        }
        return compare_result(compare_raw(lhs, rhs, lhs_col->type, lhs_col->len), cond.op);
    }

    static bool eval_conds(const std::vector<Condition> &conds, const std::vector<ColMeta> &rec_cols,
                           const char *rec_data) {
        for (const auto &cond : conds) {
            if (!eval_cond(cond, rec_cols, rec_data)) {
                return false;
            }
        }
        return true;
    }

    static std::vector<FastCondition> compile_conds(const std::vector<Condition> &conds,
                                                    const std::vector<ColMeta> &rec_cols) {
        std::vector<FastCondition> fast;
        fast.reserve(conds.size());
        for (const auto &cond : conds) {
            auto lhs_col = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
                return col.tab_name == cond.lhs_col.tab_name && col.name == cond.lhs_col.col_name;
            });
            if (lhs_col == rec_cols.end()) {
                throw ColumnNotFoundError(cond.lhs_col.tab_name + "." + cond.lhs_col.col_name);
            }

            FastCondition item{};
            item.lhs_offset = lhs_col->offset;
            item.type = lhs_col->type;
            item.len = lhs_col->len;
            item.op = cond.op;
            item.rhs_is_value = cond.is_rhs_val;
            item.rhs_offset = 0;
            item.rhs_value = nullptr;
            if (cond.is_rhs_val) {
                item.rhs_value = cond.rhs_val.raw->data;
            } else {
                auto rhs_col = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
                    return col.tab_name == cond.rhs_col.tab_name && col.name == cond.rhs_col.col_name;
                });
                if (rhs_col == rec_cols.end()) {
                    throw ColumnNotFoundError(cond.rhs_col.tab_name + "." + cond.rhs_col.col_name);
                }
                item.rhs_offset = rhs_col->offset;
            }
            fast.push_back(item);
        }
        return fast;
    }

    static bool eval_compiled_conds(const std::vector<FastCondition> &conds, const char *rec_data) {
        for (const auto &cond : conds) {
            const char *lhs = rec_data + cond.lhs_offset;
            const char *rhs = cond.rhs_is_value ? cond.rhs_value : rec_data + cond.rhs_offset;
            if (!compare_result(compare_raw(lhs, rhs, cond.type, cond.len), cond.op)) {
                return false;
            }
        }
        return true;
    }
};
