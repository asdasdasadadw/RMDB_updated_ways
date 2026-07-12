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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_scan.h"
#include "runtime_filter_cache.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同
    std::vector<FastCondition> fast_conds_;

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator
    std::unique_ptr<RmRecord> current_tuple_;
    std::vector<Rid> cached_rids_;
    size_t cached_pos_ = 0;
    bool use_cached_rids_ = false;

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::string alias, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        std::string visible_tab_name = alias.empty() ? tab_name_ : std::move(alias);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        for (auto &col : cols_) {
            col.tab_name = visible_tab_name;
        }
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
        fast_conds_ = compile_conds(fed_conds_, cols_);
    }

    void beginTuple() override {
        current_tuple_.reset();
        cached_rids_.clear();
        cached_pos_ = 0;
        use_cached_rids_ = false;
        if (context_ != nullptr && context_->is_select_ && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->RegisterPredicateRead(context_->txn_, tab_name_, fed_conds_, cols_);
        }
        use_cached_rids_ = RuntimeFilterCache::Get().lookup(sm_manager_, tab_name_, cols_.front().tab_name,
                                                             fed_conds_, &cached_rids_);
        if (use_cached_rids_ && context_ != nullptr && context_->txn_mgr_ != nullptr &&
            context_->txn_mgr_->IsMvccTxn(context_->txn_)) {
            context_->txn_mgr_->CollectVisibleVersionRids(tab_name_, {}, {}, false, {}, false, fed_conds_, cols_,
                                                          context_->txn_, &cached_rids_);
            std::sort(cached_rids_.begin(), cached_rids_.end(), [](const Rid &lhs, const Rid &rhs) {
                if (lhs.page_no != rhs.page_no) {
                    return lhs.page_no < rhs.page_no;
                }
                return lhs.slot_no < rhs.slot_no;
            });
            cached_rids_.erase(std::unique(cached_rids_.begin(), cached_rids_.end(),
                                           [](const Rid &lhs, const Rid &rhs) {
                                               return lhs.page_no == rhs.page_no && lhs.slot_no == rhs.slot_no;
                                           }),
                               cached_rids_.end());
        }
        if (!use_cached_rids_) {
            scan_ = std::make_unique<RmScan>(fh_);
        } else {
            scan_.reset();
        }
        find_next_valid();
    }

    void nextTuple() override {
        if (use_cached_rids_) {
            find_next_valid();
            return;
        }
        if (scan_ == nullptr || scan_->is_end()) {
            return;
        }
        scan_->next();
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

    std::string getType() override { return "SeqScanExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return rid_; }

   private:
    void find_next_valid() {
        current_tuple_.reset();
        while (use_cached_rids_ && cached_pos_ < cached_rids_.size()) {
            rid_ = cached_rids_[cached_pos_++];
            rows_++;
            std::unique_ptr<RmRecord> rec;
            try {
                if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                    rec = context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, rid_, context_->txn_);
                } else {
                    rec = fh_->get_record(rid_, context_);
                }
            } catch (RecordNotFoundError &) {
                continue;
            }
            if (rec == nullptr || !eval_compiled_conds(fast_conds_, rec->data)) {
                continue;
            }
            if (context_ != nullptr && context_->is_select_ && context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->RegisterRecordRead(context_->txn_, tab_name_, rid_);
            }
            current_tuple_ = std::move(rec);
            return;
        }
        while (scan_ != nullptr && !scan_->is_end()) {
            rid_ = scan_->rid();
            rows_++;
            std::unique_ptr<RmRecord> rec;
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                rec = context_->txn_mgr_->GetVisibleRecord(tab_name_, fh_, rid_, context_->txn_);
            } else {
                rec = fh_->get_record(rid_, context_);
            }
            if (rec == nullptr) {
                scan_->next();
                continue;
            }
            if (eval_compiled_conds(fast_conds_, rec->data)) {
                if (context_ != nullptr && context_->is_select_ && context_->txn_mgr_ != nullptr) {
                    context_->txn_mgr_->RegisterRecordRead(context_->txn_, tab_name_, rid_);
                }
                current_tuple_ = std::move(rec);
                return;
            }
            scan_->next();
        }
    }
};
