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

#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "execution_defs.h"
#include "executor_abstract.h"

class AggregateExecutor : public AbstractExecutor {
   private:
    struct AggState {
        int count = 0;
        double sum = 0;
        bool initialized = false;
        int int_val = 0;
        float float_val = 0;
        std::vector<char> str_val;
    };

    struct GroupState {
        std::unique_ptr<RmRecord> key;
        std::vector<AggState> aggs;
    };

    enum SlotKind { SLOT_GROUP, SLOT_AGG, SLOT_SELECT };

    struct OutputSlot {
        SlotKind kind;
        size_t index;
        bool select_is_agg;
    };

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<TabCol> group_cols_;
    std::vector<AggCall> agg_calls_;
    std::vector<SelectItem> output_items_;
    std::vector<Condition> having_conds_;
    std::vector<ColMeta> group_metas_;
    std::vector<ColMeta> agg_src_metas_;
    std::vector<ColMeta> cols_;
    std::vector<OutputSlot> slots_;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t len_ = 0;
    size_t group_len_ = 0;
    size_t cursor_ = 0;

   public:
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> group_cols,
                      std::vector<AggCall> agg_calls, std::vector<SelectItem> output_items,
                      std::vector<Condition> having_conds) {
        prev_ = std::move(prev);
        group_cols_ = std::move(group_cols);
        agg_calls_ = std::move(agg_calls);
        output_items_ = std::move(output_items);
        having_conds_ = std::move(having_conds);
        prepare_metadata();
        prepare_having_conditions();
    }

    void beginTuple() override {
        tuples_.clear();
        cursor_ = 0;
        std::vector<GroupState> groups;
        std::unordered_map<std::string, size_t> group_map;
        prev_->beginTuple();
        while (!prev_->is_end()) {
            auto rec = prev_->Next();
            auto key = make_group_key(rec->data);
            size_t group_idx = groups.size();
            if (group_len_ == 0 && !groups.empty()) {
                group_idx = 0;
            } else if (group_len_ > 0) {
                std::string map_key(key->data, group_len_);
                auto it = group_map.find(map_key);
                if (it != group_map.end()) {
                    group_idx = it->second;
                } else {
                    group_map.emplace(std::move(map_key), group_idx);
                }
            }
            if (group_idx == groups.size()) {
                GroupState group;
                group.key = std::move(key);
                group.aggs.resize(agg_calls_.size());
                groups.push_back(std::move(group));
            }
            update_group(groups[group_idx], rec->data);
            prev_->nextTuple();
        }
        if (group_cols_.empty() && groups.empty()) {
            GroupState group;
            group.key = std::make_unique<RmRecord>(0);
            group.aggs.resize(agg_calls_.size());
            groups.push_back(std::move(group));
        }
        for (const auto &group : groups) {
            auto tuple = make_output_tuple(group);
            if (eval_conds(having_conds_, cols_, tuple->data)) {
                tuples_.push_back(std::move(tuple));
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

    std::string getType() override { return "AggregateExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return _abstract_rid; }

    size_t rows() const override { return rows_; }

    void reset_rows() override {
        rows_ = 0;
        prev_->reset_rows();
    }

    AbstractExecutor *child_at(size_t idx) override { return idx == 0 ? prev_.get() : nullptr; }

   private:
    void add_result_col(ColMeta col, OutputSlot slot) {
        col.offset = static_cast<int>(len_);
        len_ += col.len;
        cols_.push_back(std::move(col));
        slots_.push_back(slot);
    }

    void prepare_metadata() {
        group_len_ = 0;
        for (auto &group_col : group_cols_) {
            auto meta = prev_->get_col_offset(group_col);
            group_metas_.push_back(meta);
            group_len_ += meta.len;
            add_result_col(meta, {SLOT_GROUP, group_metas_.size() - 1, false});
        }

        for (auto &agg : agg_calls_) {
            ColMeta src_meta;
            if (!agg.is_star) {
                src_meta = prev_->get_col_offset(agg.col);
            }
            agg_src_metas_.push_back(src_meta);

            ColMeta out_meta;
            out_meta.tab_name = "";
            out_meta.name = agg.output_name;
            out_meta.type = agg.result_type;
            out_meta.len = agg.result_len;
            add_result_col(out_meta, {SLOT_AGG, agg_src_metas_.size() - 1, true});
        }

        for (auto &item : output_items_) {
            ColMeta visible;
            visible.tab_name = "";
            visible.name = item.output_name;
            visible.type = TYPE_INT;
            visible.len = sizeof(int);
            size_t source_idx = 0;
            if (item.is_agg) {
                source_idx = find_agg_index(item.source_col.col_name);
                visible.type = agg_calls_[source_idx].result_type;
                visible.len = agg_calls_[source_idx].result_len;
            } else {
                source_idx = find_group_index(item.source_col);
                visible.type = group_metas_[source_idx].type;
                visible.len = group_metas_[source_idx].len;
            }
            add_result_col(visible, {SLOT_SELECT, source_idx, item.is_agg});
        }
    }

    size_t find_agg_index(const std::string &name) const {
        for (size_t i = 0; i < agg_calls_.size(); ++i) {
            if (agg_calls_[i].output_name == name) {
                return i;
            }
        }
        throw ColumnNotFoundError(name);
    }

    size_t find_group_index(const TabCol &col) const {
        for (size_t i = 0; i < group_metas_.size(); ++i) {
            if (group_metas_[i].tab_name == col.tab_name && group_metas_[i].name == col.col_name) {
                return i;
            }
        }
        throw ColumnNotFoundError(col.tab_name + "." + col.col_name);
    }

    void prepare_having_conditions() {
        for (auto &cond : having_conds_) {
            auto lhs = get_col(cols_, cond.lhs_col);
            if (cond.is_rhs_val) {
                if (lhs->type == TYPE_FLOAT && cond.rhs_val.type == TYPE_INT) {
                    Value casted;
                    casted.set_float(static_cast<float>(cond.rhs_val.int_val));
                    casted.raw_text = cond.rhs_val.raw_text;
                    cond.rhs_val = casted;
                } else if (lhs->type == TYPE_INT && cond.rhs_val.type == TYPE_FLOAT) {
                    Value casted;
                    casted.set_int(static_cast<int>(cond.rhs_val.float_val));
                    casted.raw_text = cond.rhs_val.raw_text;
                    cond.rhs_val = casted;
                } else if (lhs->type != cond.rhs_val.type) {
                    throw IncompatibleTypeError(coltype2str(lhs->type), coltype2str(cond.rhs_val.type));
                }
                cond.rhs_val.init_raw(lhs->len);
            } else {
                auto rhs = get_col(cols_, cond.rhs_col);
                if (lhs->type != rhs->type) {
                    throw IncompatibleTypeError(coltype2str(lhs->type), coltype2str(rhs->type));
                }
            }
        }
    }

    std::unique_ptr<RmRecord> make_group_key(const char *rec_data) const {
        auto key = std::make_unique<RmRecord>(group_len_);
        size_t offset = 0;
        for (auto &meta : group_metas_) {
            memcpy(key->data + offset, rec_data + meta.offset, meta.len);
            offset += meta.len;
        }
        return key;
    }

    size_t find_group(const std::vector<GroupState> &groups, const RmRecord *key) const {
        for (size_t i = 0; i < groups.size(); ++i) {
            if (group_len_ == 0 || memcmp(groups[i].key->data, key->data, group_len_) == 0) {
                return i;
            }
        }
        return groups.size();
    }

    void update_group(GroupState &group, const char *rec_data) {
        for (size_t i = 0; i < agg_calls_.size(); ++i) {
            auto &call = agg_calls_[i];
            auto &state = group.aggs[i];
            state.count++;
            if (call.type == AGG_COUNT) {
                continue;
            }

            const auto &src = agg_src_metas_[i];
            if (src.type == TYPE_INT) {
                int val = *reinterpret_cast<const int *>(rec_data + src.offset);
                state.sum += val;
                if (!state.initialized || val > state.int_val) {
                    if (call.type == AGG_MAX) {
                        state.int_val = val;
                    }
                }
                if (!state.initialized || val < state.int_val) {
                    if (call.type == AGG_MIN) {
                        state.int_val = val;
                    }
                }
                if (!state.initialized && (call.type == AGG_SUM || call.type == AGG_AVG)) {
                    state.int_val = 0;
                }
            } else if (src.type == TYPE_FLOAT) {
                float val = *reinterpret_cast<const float *>(rec_data + src.offset);
                state.sum += val;
                if (!state.initialized || val > state.float_val) {
                    if (call.type == AGG_MAX) {
                        state.float_val = val;
                    }
                }
                if (!state.initialized || val < state.float_val) {
                    if (call.type == AGG_MIN) {
                        state.float_val = val;
                    }
                }
            } else if (src.type == TYPE_STRING) {
                const char *val = rec_data + src.offset;
                if (!state.initialized) {
                    state.str_val.assign(val, val + src.len);
                } else {
                    int cmp = compare_raw(state.str_val.data(), val, TYPE_STRING, src.len);
                    if ((call.type == AGG_MAX && cmp < 0) || (call.type == AGG_MIN && cmp > 0)) {
                        state.str_val.assign(val, val + src.len);
                    }
                }
            }
            state.initialized = true;
        }
    }

    void write_group_value(char *dst, const GroupState &group, size_t group_idx, int len) const {
        size_t offset = 0;
        for (size_t i = 0; i < group_idx; ++i) {
            offset += group_metas_[i].len;
        }
        memcpy(dst, group.key->data + offset, len);
    }

    void write_agg_value(char *dst, const GroupState &group, size_t agg_idx) const {
        const auto &call = agg_calls_[agg_idx];
        const auto &state = group.aggs[agg_idx];
        if (call.result_type == TYPE_INT) {
            int val = 0;
            if (call.type == AGG_COUNT) {
                val = state.count;
            } else if (call.type == AGG_SUM) {
                val = static_cast<int>(state.sum);
            } else {
                val = state.initialized ? state.int_val : 0;
            }
            *reinterpret_cast<int *>(dst) = val;
        } else if (call.result_type == TYPE_STRING) {
            if (state.initialized) {
                memcpy(dst, state.str_val.data(), call.result_len);
            } else {
                memset(dst, 0, call.result_len);
            }
        } else {
            float val = 0;
            if (call.type == AGG_AVG) {
                val = state.count == 0 ? 0 : static_cast<float>(state.sum / state.count);
            } else if (call.type == AGG_SUM) {
                val = static_cast<float>(state.sum);
            } else if (call.type == AGG_MAX || call.type == AGG_MIN) {
                if (agg_src_metas_[agg_idx].type == TYPE_INT) {
                    val = state.initialized ? static_cast<float>(state.int_val) : 0;
                } else {
                    val = state.initialized ? state.float_val : 0;
                }
            }
            *reinterpret_cast<float *>(dst) = val;
        }
    }

    std::unique_ptr<RmRecord> make_output_tuple(const GroupState &group) const {
        auto rec = std::make_unique<RmRecord>(len_);
        for (size_t i = 0; i < slots_.size(); ++i) {
            const auto &slot = slots_[i];
            char *dst = rec->data + cols_[i].offset;
            if (slot.kind == SLOT_GROUP) {
                write_group_value(dst, group, slot.index, cols_[i].len);
            } else if (slot.kind == SLOT_AGG) {
                write_agg_value(dst, group, slot.index);
            } else if (slot.select_is_agg) {
                write_agg_value(dst, group, slot.index);
            } else {
                write_group_value(dst, group, slot.index, cols_[i].len);
            }
        }
        return rec;
    }
};
