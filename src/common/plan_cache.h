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

#include <cctype>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "optimizer/plan.h"

/**
 * @description: Caches parsed/analyzed/planned SQL statements to avoid
 *               re-planning identical queries. Currently uses exact SQL match
 *               after whitespace normalization, which is safe because the plan
 *               already embeds the literal values from the original SQL.
 */
class PlanCache {
   public:
    PlanCache() = default;
    ~PlanCache() = default;

    // Disable copy
    PlanCache(const PlanCache &) = delete;
    PlanCache &operator=(const PlanCache &) = delete;

    /**
     * @description: Try to get a cached plan for the given SQL.
     * @return nullptr if not found.
     */
    std::shared_ptr<Plan> get(const std::string &sql) {
        std::string key = normalize(sql);
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return nullptr;
        }
        it->second.hit_count++;
        return it->second.plan;
    }

    /**
     * @description: Store a plan in the cache.
     */
    void put(const std::string &sql, std::shared_ptr<Plan> plan) {
        if (plan == nullptr) {
            return;
        }
        std::string key = normalize(sql);
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto &entry = cache_[key];
        entry.plan = std::move(plan);
        entry.hit_count = 0;
    }

    /**
     * @description: Clear all cached plans. Must be called on any DDL.
     */
    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        cache_.clear();
    }

    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return cache_.size();
    }

   private:
    struct Entry {
        std::shared_ptr<Plan> plan;
        uint64_t hit_count = 0;
    };

    // Normalize SQL for cache key: uppercase, collapse whitespace.
    // This keeps literals intact so the cached plan remains semantically correct.
    static std::string normalize(const std::string &sql) {
        std::string result;
        result.reserve(sql.size());
        bool last_was_space = true;  // skip leading spaces
        for (char c : sql) {
            if (std::isspace(static_cast<unsigned char>(c))) {
                if (!last_was_space) {
                    result.push_back(' ');
                    last_was_space = true;
                }
            } else {
                result.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
                last_was_space = false;
            }
        }
        // trim trailing space
        if (!result.empty() && result.back() == ' ') {
            result.pop_back();
        }
        return result;
    }

    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, Entry> cache_;
};
