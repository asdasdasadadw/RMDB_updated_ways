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
#include "index/ix.h"
#include "record/rm_scan.h"
#include "system/sm.h"

#include <algorithm>
#include <limits>
#include <unordered_set>

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    std::string visible_tab_name_;
    TabMeta *tab_;                              // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同
    std::vector<FastCondition> fast_conds_;

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    const IndexMeta *index_meta_;               // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RmRecord> current_tuple_;
    std::vector<Rid> index_rids_;
    size_t index_pos_ = 0;

    SmManager *sm_manager_;
    std::vector<char> lower_key_;
    std::vector<char> upper_key_;
    std::vector<char> lower_key_template_;
    std::vector<char> upper_key_template_;
    bool lower_open_ = false;
    bool upper_open_ = false;
    int matched_prefix_len_ = 0;
    bool has_outer_tuple_ = false;
    const std::vector<Condition> *outer_conds_ = nullptr;
    const std::vector<ColMeta> *outer_cols_ = nullptr;
    const char *outer_data_ = nullptr;
    std::vector<Rid> mvcc_extra_rids_;
    size_t mvcc_extra_pos_ = 0;
    std::unordered_set<uint64_t> seen_rids_;
    bool bounds_empty_ = false;
    std::vector<ColType> index_col_types_;
    std::vector<int> index_col_lens_;
    std::vector<int> index_col_offsets_;
    bool track_serializable_ = false;
    int scan_limit_ = -1;
    bool ordered_scan_ = false;
    bool scan_desc_ = false;
    size_t emitted_ = 0;

    struct OrderedRow {
        Rid rid;
        std::unique_ptr<RmRecord> record;
    };
    std::vector<OrderedRow> ordered_rows_;
    size_t ordered_pos_ = 0;
    bool use_ordered_rows_ = false;

    struct StaticBound {
        bool has_eq = false;
        std::vector<char> eq;
        bool has_lower = false;
        bool lower_open = false;
        std::vector<char> lower;
        bool has_upper = false;
        bool upper_open = false;
        std::vector<char> upper;
    };
    std::vector<StaticBound> static_bounds_;
    std::vector<int> outer_eq_offsets_;
    const std::vector<Condition> *compiled_outer_conds_ = nullptr;
    const std::vector<ColMeta> *compiled_outer_cols_ = nullptr;
    bool outer_bounds_ready_ = false;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::string alias, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context, int scan_limit = -1, bool ordered_scan = false, bool scan_desc = false) {
        sm_manager_ = sm_manager;
        context_ = context;
        scan_limit_ = scan_limit;
        ordered_scan_ = ordered_scan;
        scan_desc_ = scan_desc;
        tab_name_ = std::move(tab_name);
        visible_tab_name_ = alias.empty() ? tab_name_ : std::move(alias);
        tab_ = &sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = &(*tab_->get_index_meta(index_col_names_));
        int index_offset = 0;
        for (auto &col : index_meta_->cols) {
            index_col_types_.push_back(col.type);
            index_col_lens_.push_back(col.len);
            index_col_offsets_.push_back(index_offset);
            index_offset += col.len;
        }
        lower_key_template_.assign(index_meta_->col_tot_len, 0);
        upper_key_template_.assign(index_meta_->col_tot_len, 0);
        for (size_t i = 0; i < index_meta_->cols.size(); ++i) {
            auto &col = index_meta_->cols[i];
            int offset = index_col_offsets_[i];
            fill_lowest(lower_key_template_.data() + offset, col);
            fill_highest(upper_key_template_.data() + offset, col);
        }
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_->cols;
        for (auto &col : cols_) {
            col.tab_name = visible_tab_name_;
        }
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != visible_tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == visible_tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
        fast_conds_ = compile_conds(fed_conds_, cols_);
        compile_static_bounds();
    }

    void set_outer_tuple(const std::vector<Condition> &conds, const std::vector<ColMeta> &outer_cols,
                         const char *outer_data) override {
        if (outer_conds_ != &conds || outer_cols_ != &outer_cols) {
            outer_bounds_ready_ = false;
        }
        outer_conds_ = &conds;
        outer_cols_ = &outer_cols;
        outer_data_ = outer_data;
        has_outer_tuple_ = outer_data != nullptr;
    }

    void beginTuple() override {
        current_tuple_.reset();
        index_rids_.clear();
        index_pos_ = 0;
        mvcc_extra_rids_.clear();
        mvcc_extra_pos_ = 0;
        seen_rids_.clear();
        ordered_rows_.clear();
        ordered_pos_ = 0;
        use_ordered_rows_ = false;
        emitted_ = 0;
        track_serializable_ = context_ != nullptr && context_->is_select_ && need_serializable_tracking(context_);
        if (track_serializable_) {
            context_->txn_mgr_->RegisterPredicateRead(context_->txn_, tab_name_, fed_conds_, cols_);
        }
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_->cols)).get();
        build_index_bounds();
        if (!bounds_empty_) {
            ih->collect_range(lower_key_.data(), lower_open_, upper_key_.data(), upper_open_, &index_rids_);
        }
        if (ordered_scan_ && scan_desc_) {
            std::reverse(index_rids_.begin(), index_rids_.end());
        }
        if (context_ != nullptr && context_->txn_mgr_ != nullptr &&
            context_->txn_mgr_->IsMvccTxn(context_->txn_)) {
            context_->txn_mgr_->CollectVisibleVersionRids(tab_name_, index_col_names_, lower_key_, lower_open_,
                                                          upper_key_, upper_open_, fed_conds_, cols_, context_->txn_,
                                                          &mvcc_extra_rids_);
        }
        if (!mvcc_extra_rids_.empty()) {
            seen_rids_.reserve(index_rids_.size() + mvcc_extra_rids_.size());
        }
        if (ordered_scan_ && scan_limit_ >= 0 && !mvcc_extra_rids_.empty()) {
            materialize_ordered_limit();
        }
        find_next_valid();
    }

    void nextTuple() override {
        find_next_valid();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*current_tuple_);
    }

    bool is_end() const override { return current_tuple_ == nullptr; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "IndexScanExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return rid_; }

   private:
    static uint64_t rid_key(const Rid &rid) {
        return (static_cast<uint64_t>(static_cast<uint32_t>(rid.page_no)) << 32) |
               static_cast<uint32_t>(rid.slot_no);
    }

    void find_next_valid() {
        current_tuple_.reset();
        if (scan_limit_ >= 0 && emitted_ >= static_cast<size_t>(scan_limit_)) {
            return;
        }
        if (use_ordered_rows_) {
            if (ordered_pos_ >= ordered_rows_.size()) {
                return;
            }
            rid_ = ordered_rows_[ordered_pos_].rid;
            current_tuple_ = std::make_unique<RmRecord>(*ordered_rows_[ordered_pos_].record);
            ordered_pos_++;
            emitted_++;
            if (track_serializable_) {
                context_->txn_mgr_->RegisterRecordRead(context_->txn_, tab_name_, rid_);
            }
            return;
        }
        while (index_pos_ < index_rids_.size()) {
            rid_ = index_rids_[index_pos_++];
            if (!mvcc_extra_rids_.empty()) {
                seen_rids_.insert(rid_key(rid_));
            }
            rows_++;
            std::unique_ptr<RmRecord> rec;
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                rec = context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, rid_, context_->txn_);
            } else {
                rec = fh_->get_record(rid_, context_);
            }
            if (rec == nullptr) {
                continue;
            }
            if (eval_compiled_conds(fast_conds_, rec->data)) {
                if (track_serializable_) {
                    context_->txn_mgr_->RegisterRecordRead(context_->txn_, tab_name_, rid_);
                }
                current_tuple_ = std::move(rec);
                emitted_++;
                return;
            }
        }
        while (mvcc_extra_pos_ < mvcc_extra_rids_.size()) {
            rid_ = mvcc_extra_rids_[mvcc_extra_pos_++];
            if (!seen_rids_.empty() && !seen_rids_.insert(rid_key(rid_)).second) {
                continue;
            }
            rows_++;
            std::unique_ptr<RmRecord> rec;
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                rec = context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, rid_, context_->txn_);
            } else {
                rec = fh_->get_record(rid_, context_);
            }
            if (rec == nullptr || !eval_compiled_conds(fast_conds_, rec->data)) {
                continue;
            }
            if (track_serializable_) {
                context_->txn_mgr_->RegisterRecordRead(context_->txn_, tab_name_, rid_);
            }
            current_tuple_ = std::move(rec);
            emitted_++;
            return;
        }
    }

    int compare_index_records(const RmRecord *lhs, const RmRecord *rhs) const {
        for (const auto &col : index_meta_->cols) {
            int cmp = compare_raw(lhs->data + col.offset, rhs->data + col.offset, col.type, col.len);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    void materialize_ordered_limit() {
        use_ordered_rows_ = true;
        std::unordered_set<uint64_t> seen;
        seen.reserve(index_rids_.size() + mvcc_extra_rids_.size());

        auto add_candidate = [&](const Rid &rid) {
            if (!seen.insert(rid_key(rid)).second) {
                return;
            }
            rows_++;
            std::unique_ptr<RmRecord> rec;
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                rec = context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, rid, context_->txn_);
            } else {
                rec = fh_->get_record(rid, context_);
            }
            if (rec == nullptr || !eval_compiled_conds(fast_conds_, rec->data)) {
                return;
            }
            ordered_rows_.push_back({rid, std::move(rec)});
        };

        for (const auto &rid : index_rids_) {
            add_candidate(rid);
        }
        for (const auto &rid : mvcc_extra_rids_) {
            add_candidate(rid);
        }
        std::sort(ordered_rows_.begin(), ordered_rows_.end(), [&](const OrderedRow &lhs, const OrderedRow &rhs) {
            int cmp = compare_index_records(lhs.record.get(), rhs.record.get());
            return scan_desc_ ? cmp > 0 : cmp < 0;
        });
        if (scan_limit_ >= 0 && ordered_rows_.size() > static_cast<size_t>(scan_limit_)) {
            ordered_rows_.resize(static_cast<size_t>(scan_limit_));
        }
        index_pos_ = index_rids_.size();
        mvcc_extra_pos_ = mvcc_extra_rids_.size();
    }

    void fill_lowest(char *dest, const ColMeta &col) {
        switch (col.type) {
            case TYPE_INT: {
                int v = std::numeric_limits<int>::min();
                memcpy(dest, &v, sizeof(int));
                break;
            }
            case TYPE_FLOAT: {
                float v = -std::numeric_limits<float>::max();
                memcpy(dest, &v, sizeof(float));
                break;
            }
            case TYPE_STRING:
                memset(dest, 0, col.len);
                break;
            default:
                throw InternalError("Unexpected data type");
        }
    }

    void fill_highest(char *dest, const ColMeta &col) {
        switch (col.type) {
            case TYPE_INT: {
                int v = std::numeric_limits<int>::max();
                memcpy(dest, &v, sizeof(int));
                break;
            }
            case TYPE_FLOAT: {
                float v = std::numeric_limits<float>::max();
                memcpy(dest, &v, sizeof(float));
                break;
            }
            case TYPE_STRING:
                memset(dest, 0xff, col.len);
                break;
            default:
                throw InternalError("Unexpected data type");
        }
    }

    void build_index_bounds() {
        lower_open_ = false;
        upper_open_ = false;
        bounds_empty_ = false;
        matched_prefix_len_ = 0;
        lower_key_ = lower_key_template_;
        upper_key_ = upper_key_template_;
        if (has_outer_tuple_ && (!outer_bounds_ready_ || compiled_outer_conds_ != outer_conds_ ||
                                 compiled_outer_cols_ != outer_cols_)) {
            compile_outer_bounds();
        }

        bool range_seen = false;
        for (size_t i = 0; i < index_meta_->cols.size(); ++i) {
            auto &col = index_meta_->cols[i];
            int offset = index_col_offsets_[i];
            if (range_seen) {
                break;
            }
            const char *outer_eq = nullptr;
            if (has_outer_tuple_ && i < outer_eq_offsets_.size() && outer_eq_offsets_[i] >= 0) {
                outer_eq = outer_data_ + outer_eq_offsets_[i];
            }
            const auto &bound = static_bounds_[i];
            if (outer_eq != nullptr) {
                memcpy(lower_key_.data() + offset, outer_eq, col.len);
                memcpy(upper_key_.data() + offset, outer_eq, col.len);
                matched_prefix_len_++;
            } else if (bound.has_eq) {
                memcpy(lower_key_.data() + offset, bound.eq.data(), col.len);
                memcpy(upper_key_.data() + offset, bound.eq.data(), col.len);
                matched_prefix_len_++;
            } else {
                bool has_range = false;
                if (bound.has_lower) {
                    memcpy(lower_key_.data() + offset, bound.lower.data(), col.len);
                    lower_open_ = bound.lower_open;
                    has_range = true;
                }
                if (bound.has_upper) {
                    memcpy(upper_key_.data() + offset, bound.upper.data(), col.len);
                    upper_open_ = bound.upper_open;
                    has_range = true;
                }
                if (!has_range) {
                    break;
                }
                range_seen = true;
                matched_prefix_len_++;
            }
        }
        int cmp = ix_compare(lower_key_.data(), upper_key_.data(), index_col_types_, index_col_lens_);
        bounds_empty_ = cmp > 0 || (cmp == 0 && (lower_open_ || upper_open_));
    }

    bool find_index_col_pos(const std::string &col_name, size_t *pos) const {
        for (size_t i = 0; i < index_meta_->cols.size(); ++i) {
            if (index_meta_->cols[i].name == col_name) {
                *pos = i;
                return true;
            }
        }
        return false;
    }

    void compile_static_bounds() {
        static_bounds_.assign(index_meta_->cols.size(), StaticBound{});
        for (auto &cond : conds_) {
            if (!cond.is_rhs_val || cond.lhs_col.tab_name != visible_tab_name_) {
                continue;
            }
            size_t pos = 0;
            if (!find_index_col_pos(cond.lhs_col.col_name, &pos)) {
                continue;
            }
            const auto &col = index_meta_->cols[pos];
            if (cond.rhs_val.raw == nullptr) {
                cond.rhs_val.init_raw(col.len);
            }
            auto &bound = static_bounds_[pos];
            switch (cond.op) {
                case OP_EQ:
                    bound.has_eq = true;
                    bound.eq.assign(cond.rhs_val.raw->data, cond.rhs_val.raw->data + col.len);
                    break;
                case OP_GT:
                case OP_GE:
                    if (!bound.has_lower ||
                        ix_compare(cond.rhs_val.raw->data, bound.lower.data(), col.type, col.len) > 0) {
                        bound.has_lower = true;
                        bound.lower_open = cond.op == OP_GT;
                        bound.lower.assign(cond.rhs_val.raw->data, cond.rhs_val.raw->data + col.len);
                    }
                    break;
                case OP_LT:
                case OP_LE:
                    if (!bound.has_upper ||
                        ix_compare(cond.rhs_val.raw->data, bound.upper.data(), col.type, col.len) < 0) {
                        bound.has_upper = true;
                        bound.upper_open = cond.op == OP_LT;
                        bound.upper.assign(cond.rhs_val.raw->data, cond.rhs_val.raw->data + col.len);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    void compile_outer_bounds() {
        outer_eq_offsets_.assign(index_meta_->cols.size(), -1);
        compiled_outer_conds_ = outer_conds_;
        compiled_outer_cols_ = outer_cols_;
        outer_bounds_ready_ = true;
        if (outer_conds_ == nullptr || outer_cols_ == nullptr) {
            return;
        }
        for (const auto &cond : *outer_conds_) {
            if (cond.op != OP_EQ || cond.is_rhs_val) {
                continue;
            }
            size_t pos = 0;
            const TabCol *outer_ref = nullptr;
            if (cond.lhs_col.tab_name == visible_tab_name_ &&
                find_index_col_pos(cond.lhs_col.col_name, &pos)) {
                outer_ref = &cond.rhs_col;
            } else if (cond.rhs_col.tab_name == visible_tab_name_ &&
                       find_index_col_pos(cond.rhs_col.col_name, &pos)) {
                outer_ref = &cond.lhs_col;
            }
            if (outer_ref == nullptr) {
                continue;
            }
            auto outer_col = get_col(*outer_cols_, *outer_ref);
            outer_eq_offsets_[pos] = outer_col->offset;
        }
    }
};
