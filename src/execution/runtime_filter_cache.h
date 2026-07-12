#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/context.h"
#include "errors.h"
#include "execution_defs.h"
#include "record/rm_scan.h"
#include "system/sm.h"

class RuntimeFilterCache {
   public:
    static RuntimeFilterCache &Get() {
        static RuntimeFilterCache cache;
        return cache;
    }

    static bool choose_columns(TabMeta &tab, const std::string &visible_tab,
                               const std::vector<Condition> &conds, std::vector<ColMeta> *cols) {
        if (cols == nullptr) {
            return false;
        }
        cols->clear();
        std::vector<std::string> seen;
        for (const auto &cond : conds) {
            if (!cond.is_rhs_val || cond.op != OP_EQ || cond.rhs_val.raw == nullptr) {
                continue;
            }
            if (!cond.lhs_col.tab_name.empty() && cond.lhs_col.tab_name != visible_tab &&
                cond.lhs_col.tab_name != tab.name) {
                continue;
            }
            auto col = tab.get_col(cond.lhs_col.col_name);
            if (std::find(seen.begin(), seen.end(), col->name) != seen.end()) {
                continue;
            }
            seen.push_back(col->name);
            cols->push_back(*col);
        }
        std::sort(cols->begin(), cols->end(), [](const ColMeta &lhs, const ColMeta &rhs) {
            return lhs.offset < rhs.offset;
        });
        return cols->size() >= 2;
    }

    bool lookup(SmManager *sm_manager, const std::string &tab_name, const std::string &visible_tab,
                const std::vector<Condition> &conds, std::vector<Rid> *rids) {
        if (sm_manager == nullptr || rids == nullptr) {
            return false;
        }
        TabMeta &tab = sm_manager->db_.get_table(tab_name);
        std::vector<ColMeta> cols;
        if (!choose_columns(tab, visible_tab, conds, &cols)) {
            return false;
        }

        std::string key;
        if (!make_key_from_conditions(cols, visible_tab, conds, &key)) {
            return false;
        }

        auto entry = get_or_create_entry(tab_name, cols);
        ensure_built(entry, sm_manager, tab_name);

        std::shared_lock<std::shared_mutex> lock(entry->latch);
        auto it = entry->rows.find(key);
        if (it == entry->rows.end()) {
            rids->clear();
            return true;
        }
        *rids = it->second;
        return true;
    }

    bool has_table_entries(const std::string &tab_name) {
        std::shared_lock<std::shared_mutex> lock(latch_);
        auto it = table_entries_.find(tab_name);
        return it != table_entries_.end() && !it->second.empty();
    }

    void insert_record(const std::string &tab_name, const Rid &rid, const RmRecord &record, const TabMeta &tab) {
        for_each_table_entry(tab_name, [&](const std::shared_ptr<Entry> &entry) {
            std::unique_lock<std::shared_mutex> lock(entry->latch);
            add_locked(entry.get(), make_key_from_record(entry->cols, record), rid);
        });
        (void)tab;
    }

    void update_record(const std::string &tab_name, const Rid &rid, const RmRecord &old_record,
                       const RmRecord &new_record, const TabMeta &tab, bool keep_old_key) {
        for_each_table_entry(tab_name, [&](const std::shared_ptr<Entry> &entry) {
            std::string old_key = make_key_from_record(entry->cols, old_record);
            std::string new_key = make_key_from_record(entry->cols, new_record);
            std::unique_lock<std::shared_mutex> lock(entry->latch);
            if (!keep_old_key && old_key != new_key) {
                remove_locked(entry.get(), old_key, rid);
            }
            add_locked(entry.get(), new_key, rid);
        });
        (void)tab;
    }

    void delete_record(const std::string &tab_name, const Rid &rid, const RmRecord &record, const TabMeta &tab,
                       bool keep_key) {
        if (keep_key) {
            return;
        }
        for_each_table_entry(tab_name, [&](const std::shared_ptr<Entry> &entry) {
            std::unique_lock<std::shared_mutex> lock(entry->latch);
            remove_locked(entry.get(), make_key_from_record(entry->cols, record), rid);
        });
        (void)tab;
    }

   private:
    struct Entry {
        std::string tab_name;
        std::vector<ColMeta> cols;
        std::unordered_map<std::string, std::vector<Rid>> rows;
        std::shared_mutex latch;
        bool ready = false;
    };

    RuntimeFilterCache() = default;

    static std::string signature(const std::string &tab_name, const std::vector<ColMeta> &cols) {
        std::string sig = tab_name;
        sig.reserve(tab_name.size() + 1 + cols.size() * 12);
        sig.push_back('\0');
        for (const auto &col : cols) {
            sig.append(col.name);
            sig.push_back('\0');
        }
        return sig;
    }

    static std::string make_key_from_record(const std::vector<ColMeta> &cols, const RmRecord &record) {
        std::string key;
        size_t len = 0;
        for (const auto &col : cols) {
            len += static_cast<size_t>(col.len);
        }
        key.reserve(len);
        for (const auto &col : cols) {
            key.append(record.data + col.offset, col.len);
        }
        return key;
    }

    static bool make_key_from_conditions(const std::vector<ColMeta> &cols, const std::string &visible_tab,
                                         const std::vector<Condition> &conds, std::string *key) {
        key->clear();
        size_t len = 0;
        for (const auto &col : cols) {
            len += static_cast<size_t>(col.len);
        }
        key->reserve(len);
        for (const auto &col : cols) {
            const Condition *match = nullptr;
            for (const auto &cond : conds) {
                if (!cond.is_rhs_val || cond.op != OP_EQ || cond.rhs_val.raw == nullptr) {
                    continue;
                }
                if (!cond.lhs_col.tab_name.empty() && cond.lhs_col.tab_name != visible_tab) {
                    continue;
                }
                if (cond.lhs_col.col_name == col.name) {
                    match = &cond;
                    break;
                }
            }
            if (match == nullptr) {
                return false;
            }
            key->append(match->rhs_val.raw->data, col.len);
        }
        return true;
    }

    std::shared_ptr<Entry> get_or_create_entry(const std::string &tab_name, const std::vector<ColMeta> &cols) {
        std::string sig = signature(tab_name, cols);
        {
            std::shared_lock<std::shared_mutex> read_lock(latch_);
            auto it = entries_.find(sig);
            if (it != entries_.end()) {
                return it->second;
            }
        }
        std::unique_lock<std::shared_mutex> lock(latch_);
        auto it = entries_.find(sig);
        if (it != entries_.end()) {
            return it->second;
        }
        auto entry = std::make_shared<Entry>();
        entry->tab_name = tab_name;
        entry->cols = cols;
        entries_[sig] = entry;
        table_entries_[tab_name].push_back(entry);
        return entry;
    }

    void ensure_built(const std::shared_ptr<Entry> &entry, SmManager *sm_manager, const std::string &tab_name) {
        {
            std::shared_lock<std::shared_mutex> read_lock(entry->latch);
            if (entry->ready) {
                return;
            }
        }

        std::unique_lock<std::shared_mutex> lock(entry->latch);
        if (entry->ready) {
            return;
        }
        entry->rows.clear();
        RmFileHandle *fh = sm_manager->fhs_.at(tab_name).get();
        RmScan scan(fh);
        for (; !scan.is_end(); scan.next()) {
            Rid rid = scan.rid();
            try {
                auto rec = fh->get_record(rid, nullptr);
                add_locked(entry.get(), make_key_from_record(entry->cols, *rec), rid);
            } catch (RMDBError &) {
            }
        }
        entry->ready = true;
    }

    void for_each_table_entry(const std::string &tab_name,
                              const std::function<void(const std::shared_ptr<Entry> &)> &fn) {
        std::vector<std::shared_ptr<Entry>> entries;
        {
            std::shared_lock<std::shared_mutex> lock(latch_);
            auto it = table_entries_.find(tab_name);
            if (it == table_entries_.end()) {
                return;
            }
            entries = it->second;
        }
        for (const auto &entry : entries) {
            fn(entry);
        }
    }

    static void add_locked(Entry *entry, const std::string &key, const Rid &rid) {
        auto &list = entry->rows[key];
        if (std::find(list.begin(), list.end(), rid) == list.end()) {
            list.push_back(rid);
        }
    }

    static void remove_locked(Entry *entry, const std::string &key, const Rid &rid) {
        auto it = entry->rows.find(key);
        if (it == entry->rows.end()) {
            return;
        }
        auto &list = it->second;
        list.erase(std::remove(list.begin(), list.end(), rid), list.end());
        if (list.empty()) {
            entry->rows.erase(it);
        }
    }

    std::shared_mutex latch_;
    std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Entry>>> table_entries_;
};
