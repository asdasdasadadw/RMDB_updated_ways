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

#include <unordered_map>
#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

class RecoveryManager {
public:
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
    }

    void analyze();
    void redo();
    void undo();
    txn_id_t max_txn_id() const { return max_txn_id_; }
    bool has_recovery_work() const { return has_recovery_work_; }
private:
    enum class TxnTerminalState {
        NONE,
        COMMITTED,
        ABORTED,
    };

    struct TxnRecoveryState {
        bool redo{false};
        bool undo{false};
        lsn_t last_lsn{INVALID_LSN};
        TxnTerminalState terminal{TxnTerminalState::NONE};
    };

    std::unordered_map<txn_id_t, TxnRecoveryState> txn_states_;
    txn_id_t max_txn_id_{INVALID_TXN_ID};
    lsn_t scan_start_{0};
    lsn_t valid_end_{0};
    bool has_recovery_work_{false};
    DiskManager* disk_manager_;                                     // 用来读写文件
    BufferPoolManager* buffer_pool_manager_;                        // 对页面进行读写
    SmManager* sm_manager_;                                         // 访问数据库元数据
};
