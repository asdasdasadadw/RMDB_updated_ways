/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <netinet/in.h>
#include <poll.h>
#include <setjmp.h>
#include <signal.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <strings.h>
#include <utility>
#include <vector>

#include "errors.h"
#include "optimizer/optimizer.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "analyze/analyze.h"
#include "execution/runtime_filter_cache.h"
#include "simple_exec.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 128

static bool should_exit = false;
std::atomic<bool> output_file_enabled{true};

// 构建全局所需的管理器对象
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto lock_manager = std::make_unique<LockManager>();
auto txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), planner.get());
auto log_manager = std::make_unique<LogManager>(disk_manager.get());
auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get());
auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());
pthread_mutex_t *sockfd_mutex;
std::shared_mutex checkpoint_mutex;

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    should_exit = true;
    std::cout << "The Server receive Crtl+C, will been closed\n";
    longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t *txn_id, Transaction **session_txn, Context *context) {
    if (session_txn != nullptr && *session_txn != nullptr &&
        (*session_txn)->get_transaction_id() == *txn_id) {
        context->txn_ = *session_txn;
    } else {
        context->txn_ = *txn_id == INVALID_TXN_ID ? nullptr : txn_manager->get_transaction(*txn_id);
        if (session_txn != nullptr) {
            *session_txn = context->txn_;
        }
    }
    if(context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        std::shared_lock<std::shared_mutex> checkpoint_lock(checkpoint_mutex);
        IsolationLevel isolation_level = context->session_isolation_level_ == nullptr
                                             ? IsolationLevel::SNAPSHOT_ISOLATION
                                             : *context->session_isolation_level_;
        context->txn_ = txn_manager->begin(nullptr, context->log_mgr_, isolation_level);
        *txn_id = context->txn_->get_transaction_id();
        if (session_txn != nullptr) {
            *session_txn = context->txn_;
        }
        context->txn_->set_txn_mode(false);
    }
}

bool is_blank_sql(const std::string &sql) {
    for (char ch : sql) {
        if (!std::isspace(static_cast<unsigned char>(ch)) && ch != ';') {
            return false;
        }
    }
    return true;
}

bool handle_set_transaction_isolation(const std::string &sql, IsolationLevel *session_isolation_level) {
    if (session_isolation_level == nullptr) {
        return false;
    }
    std::string normalized;
    for (char ch : sql) {
        if (ch == ';') {
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            if (!normalized.empty() && normalized.back() != ' ') {
                normalized.push_back(' ');
            }
        } else {
            normalized.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
        }
    }
    while (!normalized.empty() && normalized.back() == ' ') {
        normalized.pop_back();
    }
    if (normalized == "SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION") {
        *session_isolation_level = IsolationLevel::SNAPSHOT_ISOLATION;
        return true;
    }
    if (normalized == "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE") {
        *session_isolation_level = IsolationLevel::SERIALIZABLE;
        return true;
    }
    return false;
}

std::string trim_sql_text(const std::string &text) {
    size_t begin = 0;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin]))) {
        begin++;
    }
    size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
        end--;
    }
    if (end > begin && text[end - 1] == ';') {
        end--;
        while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
            end--;
        }
    }
    return text.substr(begin, end - begin);
}

std::string trim_field(const std::string &text) {
    size_t begin = 0;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin]))) {
        begin++;
    }
    size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
        end--;
    }
    return text.substr(begin, end - begin);
}

bool starts_with_keyword(const std::string &text, const char *keyword) {
    size_t len = strlen(keyword);
    return text.size() >= len && strncasecmp(text.c_str(), keyword, len) == 0 &&
           (text.size() == len || std::isspace(static_cast<unsigned char>(text[len])));
}

std::string first_sql_keyword(const char *sql) {
    std::string keyword;
    if (sql == nullptr) {
        return keyword;
    }
    size_t pos = 0;
    while (sql[pos] != '\0' &&
           (std::isspace(static_cast<unsigned char>(sql[pos])) || sql[pos] == ';')) {
        pos++;
    }
    while (sql[pos] != '\0' && !std::isspace(static_cast<unsigned char>(sql[pos])) && sql[pos] != ';') {
        keyword.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(sql[pos]))));
        pos++;
    }
    return keyword;
}

bool is_txn_command_keyword(const std::string &keyword) {
    return keyword == "BEGIN" || keyword == "COMMIT" || keyword == "ABORT" || keyword == "ROLLBACK";
}

bool handle_set_output_file(const std::string &sql) {
    std::string normalized;
    for (char ch : sql) {
        if (ch == ';') {
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            if (!normalized.empty() && normalized.back() != ' ') {
                normalized.push_back(' ');
            }
        } else {
            normalized.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        }
    }
    while (!normalized.empty() && normalized.back() == ' ') {
        normalized.pop_back();
    }
    if (normalized == "set output_file off") {
        output_file_enabled.store(false);
        return true;
    }
    if (normalized == "set output_file on") {
        output_file_enabled.store(true);
        return true;
    }
    return false;
}

bool is_set_output_file_command(const std::string &sql) {
    std::string normalized;
    for (char ch : sql) {
        if (ch == ';') {
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            if (!normalized.empty() && normalized.back() != ' ') {
                normalized.push_back(' ');
            }
        } else {
            normalized.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        }
    }
    while (!normalized.empty() && normalized.back() == ' ') {
        normalized.pop_back();
    }
    return normalized == "set output_file off" || normalized == "set output_file on";
}

std::vector<std::string> split_csv_line(const std::string &line) {
    std::vector<std::string> fields;
    std::string current;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char ch = line[i];
        if (ch == '"') {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                current.push_back('"');
                ++i;
            } else {
                in_quotes = !in_quotes;
            }
        } else if (ch == ',' && !in_quotes) {
            fields.push_back(trim_field(current));
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    fields.push_back(trim_field(current));
    return fields;
}

bool is_csv_header(const std::vector<std::string> &fields, const TabMeta &tab) {
    if (fields.size() != tab.cols.size()) {
        return false;
    }
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i] != tab.cols[i].name) {
            return false;
        }
    }
    return true;
}

Value parse_load_value(const std::string &field, const ColMeta &col) {
    Value value;
    if (col.type == TYPE_INT) {
        value.set_int(std::stoi(field));
    } else if (col.type == TYPE_FLOAT) {
        value.set_float(std::stof(field));
    } else if (col.type == TYPE_STRING) {
        value.set_str(field);
    } else {
        throw InternalError("Unexpected column type");
    }
    return value;
}

bool parse_load_command(const std::string &sql, std::string *file_name, std::string *table_name) {
    std::string stmt = trim_sql_text(sql);
    if (!starts_with_keyword(stmt, "load")) {
        return false;
    }

    size_t pos = 4;
    while (pos < stmt.size() && std::isspace(static_cast<unsigned char>(stmt[pos]))) {
        pos++;
    }
    size_t into_pos = std::string::npos;
    for (size_t i = pos; i + 4 <= stmt.size(); ++i) {
        if ((i == 0 || std::isspace(static_cast<unsigned char>(stmt[i - 1]))) &&
            strncasecmp(stmt.c_str() + i, "into", 4) == 0 &&
            (i + 4 == stmt.size() || std::isspace(static_cast<unsigned char>(stmt[i + 4])))) {
            into_pos = i;
            break;
        }
    }
    if (into_pos == std::string::npos) {
        return false;
    }

    *file_name = trim_field(stmt.substr(pos, into_pos - pos));
    *table_name = trim_field(stmt.substr(into_pos + 4));
    if (file_name->empty() || table_name->empty()) {
        throw RMDBError("Invalid load statement");
    }
    return true;
}

void load_csv_into_table(const std::string &file_name, const std::string &table_name, Context *context) {
    TabMeta &tab = sm_manager->db_.get_table(table_name);
    std::ifstream input(file_name);
    if (!input.is_open()) {
        throw FileNotFoundError(file_name);
    }

    IsolationLevel saved_isolation = IsolationLevel::READ_COMMITTED;
    bool changed_isolation = context != nullptr && context->txn_ != nullptr;
    if (changed_isolation) {
        saved_isolation = context->txn_->get_isolation_level();
        context->txn_->set_isolation_level(IsolationLevel::READ_COMMITTED);
    }

    std::string line;
    bool first_line = true;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        auto fields = split_csv_line(line);
        if (first_line && is_csv_header(fields, tab)) {
            first_line = false;
            continue;
        }
        first_line = false;
        if (fields.size() != tab.cols.size()) {
            throw InvalidValueCountError();
        }

        std::vector<Value> values;
        values.reserve(fields.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            values.push_back(parse_load_value(fields[i], tab.cols[i]));
        }
        InsertExecutor insert_executor(sm_manager.get(), table_name, std::move(values), context);
        insert_executor.Next();
    }
    if (changed_isolation) {
        context->txn_->set_isolation_level(saved_isolation);
    }
}

bool is_static_checkpoint_sql(const std::string &sql) {
    std::string normalized;
    for (char ch : sql) {
        if (ch == ';') {
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            if (!normalized.empty() && normalized.back() != ' ') {
                normalized.push_back(' ');
            }
        } else {
            normalized.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
        }
    }
    while (!normalized.empty() && normalized.back() == ' ') {
        normalized.pop_back();
    }
    return normalized == "CREATE STATIC_CHECKPOINT";
}

void persist_checkpoint_offset(lsn_t checkpoint_lsn) {
    const std::string temp_name = CHECKPOINT_FILE_NAME + ".tmp";
    {
        std::ofstream checkpoint_file(temp_name, std::ios::out | std::ios::trunc);
        if (!checkpoint_file.is_open()) {
            throw InternalError("Failed to open checkpoint metadata temp file");
        }
        checkpoint_file << checkpoint_lsn;
        checkpoint_file.flush();
        if (!checkpoint_file) {
            throw InternalError("Failed to write checkpoint metadata");
        }
    }

    int checkpoint_fd = open(temp_name.c_str(), O_RDONLY);
    if (checkpoint_fd < 0) {
        throw UnixError();
    }
    if (fsync(checkpoint_fd) < 0) {
        close(checkpoint_fd);
        throw UnixError();
    }
    if (close(checkpoint_fd) < 0) {
        throw UnixError();
    }
    if (std::rename(temp_name.c_str(), CHECKPOINT_FILE_NAME.c_str()) < 0) {
        throw UnixError();
    }

    // Persist the directory entry after the atomic replacement.  Some test
    // filesystems do not support directory fsync; the rename is still atomic
    // for process-kill recovery, so this last durability fence is best effort.
    int directory_fd = open(".", O_RDONLY);
    if (directory_fd >= 0) {
        fsync(directory_fd);
        close(directory_fd);
    }
}

void create_static_checkpoint() {
    std::unique_lock<std::shared_mutex> checkpoint_lock(checkpoint_mutex);
    if (txn_manager->has_active_transaction()) {
        return;
    }
    log_manager->flush_log_to_disk(true);
    txn_manager->GarbageCollection();
    CheckpointLogRecord checkpoint;
    lsn_t checkpoint_lsn = log_manager->add_log_to_buffer(&checkpoint);
    log_manager->flush_log_to_disk(true);
    sm_manager->flush_db_files();
    persist_checkpoint_offset(checkpoint_lsn);
}

void truncate_log_file() {
    int log_fd = disk_manager->GetLogFd();
    if (log_fd >= 0) {
        if (ftruncate(log_fd, 0) == 0) {
            fsync(log_fd);
        }
        return;
    }
    log_fd = open(LOG_FILE_NAME.c_str(), O_RDWR);
    if (log_fd >= 0) {
        if (ftruncate(log_fd, 0) == 0) {
            fsync(log_fd);
        }
        close(log_fd);
    }
}

void clean_shutdown_storage() {
    std::unique_lock<std::shared_mutex> checkpoint_lock(checkpoint_mutex);
    log_manager->flush_log_to_disk(true);
    txn_manager->GarbageCollection();
    sm_manager->flush_db_files();
    truncate_log_file();
    persist_checkpoint_offset(0);
}

void append_failure_result() {
    if (!output_file_enabled.load()) {
        return;
    }
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "failure\n";
    outfile.close();
}

void write_failure_result(char *data_send, int *offset) {
    std::string str = "failure\n";
    memcpy(data_send, str.c_str(), str.length());
    data_send[str.length()] = '\0';
    *offset = str.length();
    append_failure_result();
}

void write_rmdb_error_result(RMDBError &error, char *data_send, int *offset) {
    std::string str = "failure\n";
    size_t remaining = BUFFER_LENGTH - str.size() - 2;
    size_t error_len = std::min(static_cast<size_t>(error.get_msg_len()), remaining);
    str.append(error.what(), error_len);
    str.push_back('\n');
    memcpy(data_send, str.data(), str.size());
    data_send[str.size()] = '\0';
    *offset = static_cast<int>(str.size());
    append_failure_result();
}

void write_abort_result(char *data_send, int *offset) {
    std::string str = "abort\n";
    memcpy(data_send, str.c_str(), str.length());
    data_send[str.length()] = '\0';
    *offset = str.length();
    if (output_file_enabled.load()) {
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << str;
        outfile.close();
    }
}

enum class TxnCommand { None, Begin, Commit, Abort, Rollback };

TxnCommand get_txn_command(const std::string &sql) {
    std::string normalized;
    for (char ch : sql) {
        if (ch == ';') {
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            if (!normalized.empty() && normalized.back() != ' ') {
                normalized.push_back(' ');
            }
        } else {
            normalized.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
        }
    }
    while (!normalized.empty() && normalized.back() == ' ') {
        normalized.pop_back();
    }
    if (normalized == "BEGIN") {
        return TxnCommand::Begin;
    }
    if (normalized == "COMMIT") {
        return TxnCommand::Commit;
    }
    if (normalized == "ABORT") {
        return TxnCommand::Abort;
    }
    if (normalized == "ROLLBACK") {
        return TxnCommand::Rollback;
    }
    return TxnCommand::None;
}

void abort_session_transaction(txn_id_t *txn_id, Transaction **session_txn = nullptr) {
    if (txn_id == nullptr || *txn_id == INVALID_TXN_ID) {
        if (session_txn != nullptr) {
            *session_txn = nullptr;
        }
        return;
    }
    Transaction *txn = txn_manager->get_transaction(*txn_id);
    if (txn != nullptr && txn->get_state() == TransactionState::GROWING) {
        txn_manager->abort(txn, log_manager.get());
    }
    *txn_id = INVALID_TXN_ID;
    if (session_txn != nullptr) {
        *session_txn = nullptr;
    }
}

bool execute_txn_command_fast(TxnCommand txn_cmd, txn_id_t *txn_id, Context *context) {
    if (txn_cmd == TxnCommand::None || txn_id == nullptr || context == nullptr) {
        return false;
    }
    switch (txn_cmd) {
        case TxnCommand::Begin:
            if (context->txn_ != nullptr) {
                context->txn_->set_txn_mode(true);
            }
            return true;
        case TxnCommand::Commit:
            if (context->txn_ != nullptr) {
                txn_manager->commit(context->txn_, context->log_mgr_);
            }
            *txn_id = INVALID_TXN_ID;
            return true;
        case TxnCommand::Abort:
        case TxnCommand::Rollback:
            if (context->txn_ != nullptr) {
                txn_manager->abort(context->txn_, context->log_mgr_);
            }
            *txn_id = INVALID_TXN_ID;
            return true;
        case TxnCommand::None:
            return false;
    }
    return false;
}

std::vector<std::string> split_sql_statements(const std::string &input) {
    std::vector<std::string> statements;
    std::string current;
    bool in_string = false;
    for (char ch : input) {
        current.push_back(ch);
        if (ch == '\'') {
            in_string = !in_string;
        }
        if (ch == ';' && !in_string) {
            if (!is_blank_sql(current)) {
                statements.push_back(current);
            }
            current.clear();
        } else if ((ch == '\n' || ch == '\r') && !in_string && is_set_output_file_command(current)) {
            statements.push_back(current);
            current.clear();
        }
    }
    if (!is_blank_sql(current)) {
        current.push_back(';');
        statements.push_back(current);
    }
    return statements;
}

std::string read_available_stdin() {
    struct stat st {};
    if (fstat(STDIN_FILENO, &st) != 0) {
        return "";
    }
    if (S_ISREG(st.st_mode)) {
        std::ostringstream ss;
        ss << std::cin.rdbuf();
        return ss.str();
    }

    pollfd pfd {};
    pfd.fd = STDIN_FILENO;
    pfd.events = POLLIN;
    if (poll(&pfd, 1, 0) > 0 && (pfd.revents & POLLIN)) {
        std::ostringstream ss;
        ss << std::cin.rdbuf();
        return ss.str();
    }
    return "";
}

void execute_sql_statement(const std::string &sql, txn_id_t *txn_id, Transaction **session_txn,
                           char *data_send, int *offset,
                           IsolationLevel *session_isolation_level) {
    *offset = 0;
    data_send[0] = '\0';

    std::string first_keyword = first_sql_keyword(sql.c_str());
    if (first_keyword == "SET") {
        if (handle_set_output_file(sql)) {
            return;
        }
        if (handle_set_transaction_isolation(sql, session_isolation_level)) {
            return;
        }
    }
    if (first_keyword == "CREATE" && is_static_checkpoint_sql(sql)) {
        create_static_checkpoint();
        return;
    }

    Context context(lock_manager.get(), log_manager.get(), nullptr, data_send, offset, txn_manager.get(),
                    session_isolation_level);
    SetTransaction(txn_id, session_txn, &context);

    TxnCommand txn_cmd = is_txn_command_keyword(first_keyword) ? get_txn_command(sql) : TxnCommand::None;
    if (execute_txn_command_fast(txn_cmd, txn_id, &context)) {
        if (txn_cmd != TxnCommand::Begin && session_txn != nullptr) {
            *session_txn = nullptr;
        }
        return;
    }

    std::string load_file;
    std::string load_table;
    if (first_keyword == "LOAD" && parse_load_command(sql, &load_file, &load_table)) {
        bool ok = true;
        try {
            load_csv_into_table(load_file, load_table, &context);
        } catch (TransactionAbortException &e) {
            ok = false;
            std::string str = "abort\n";
            memcpy(data_send, str.c_str(), str.length());
            data_send[str.length()] = '\0';
            *offset = str.length();
            abort_session_transaction(txn_id, session_txn);
            if (output_file_enabled.load()) {
                std::fstream outfile;
                outfile.open("output.txt", std::ios::out | std::ios::app);
                outfile << str;
                outfile.close();
            }
        } catch (RMDBError &e) {
            ok = false;
            write_rmdb_error_result(e, data_send, offset);
            if (context.txn_ != nullptr && context.txn_->get_state() == TransactionState::GROWING) {
                abort_session_transaction(txn_id, session_txn);
            }
            std::cerr << e.what() << std::endl;
        }
        if (context.txn_ != nullptr && context.txn_->get_txn_mode() == false &&
            context.txn_->get_state() == TransactionState::GROWING) {
            if (ok) {
                txn_manager->commit(context.txn_, context.log_mgr_);
            } else {
                abort_session_transaction(txn_id, session_txn);
            }
            *txn_id = INVALID_TXN_ID;
            if (session_txn != nullptr) {
                *session_txn = nullptr;
            }
        }
        return;
    }

    bool handled_fast = false;
    try {
        handled_fast = try_execute_simple_sql(sql, sm_manager.get(), ql_manager.get(), &context);
    } catch (TransactionAbortException &e) {
        std::string str = "abort\n";
        memcpy(data_send, str.c_str(), str.length());
        data_send[str.length()] = '\0';
        *offset = str.length();
        abort_session_transaction(txn_id, session_txn);
        if (output_file_enabled.load()) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << str;
            outfile.close();
        }
        handled_fast = true;
    } catch (RMDBError &e) {
        write_rmdb_error_result(e, data_send, offset);
        if (context.txn_ != nullptr && context.txn_->get_state() == TransactionState::GROWING) {
            abort_session_transaction(txn_id, session_txn);
        }
        std::cerr << e.what() << std::endl;
        handled_fast = true;
    }
    if (handled_fast) {
        if (context.txn_ != nullptr && context.txn_->get_txn_mode() == false &&
            context.txn_->get_state() == TransactionState::GROWING) {
            txn_manager->commit(context.txn_, context.log_mgr_);
            *txn_id = INVALID_TXN_ID;
            if (session_txn != nullptr) {
                *session_txn = nullptr;
            }
        }
        return;
    }

    yyscan_t scanner;
    yylex_init(&scanner);
    ast::parse_tree = nullptr;
    YY_BUFFER_STATE buf = yy_scan_string(sql.c_str(), scanner);
    bool parsed = yyparse(scanner) == 0;
    std::shared_ptr<ast::TreeNode> parse_tree;
    if (parsed) {
        parse_tree = std::move(ast::parse_tree);
        ast::parse_tree = nullptr;
    }
    yy_delete_buffer(buf, scanner);
    yylex_destroy(scanner);
    if (!parsed) {
        write_failure_result(data_send, offset);
        abort_session_transaction(txn_id, session_txn);
        return;
    }
    if (parse_tree == nullptr) {
        if (context.txn_ != nullptr && context.txn_->get_txn_mode() == false &&
            context.txn_->get_state() == TransactionState::GROWING) {
            txn_manager->commit(context.txn_, context.log_mgr_);
            *txn_id = INVALID_TXN_ID;
            if (session_txn != nullptr) {
                *session_txn = nullptr;
            }
        }
        return;
    }

    try {
        std::shared_ptr<Query> query = analyze->do_analyze(parse_tree);
        std::shared_ptr<Plan> plan = optimizer->plan_query(query, &context);
        std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, &context);
        portal->run(portalStmt, ql_manager.get(), txn_id, &context);
        portal->drop();
    } catch (TransactionAbortException &e) {
        std::string str = "abort\n";
        memcpy(data_send, str.c_str(), str.length());
        data_send[str.length()] = '\0';
        *offset = str.length();
        abort_session_transaction(txn_id, session_txn);
        if (output_file_enabled.load()) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << str;
            outfile.close();
        }
    } catch (RMDBError &e) {
        write_rmdb_error_result(e, data_send, offset);
        if (context.txn_ != nullptr && context.txn_->get_state() == TransactionState::GROWING) {
            abort_session_transaction(txn_id, session_txn);
        }
        std::cerr << e.what() << std::endl;
    }

    if (context.txn_ != nullptr && context.txn_->get_txn_mode() == false &&
        context.txn_->get_state() == TransactionState::GROWING) {
        txn_manager->commit(context.txn_, context.log_mgr_);
        *txn_id = INVALID_TXN_ID;
        if (session_txn != nullptr) {
            *session_txn = nullptr;
        }
    }
}

bool run_batch_sql_if_present() {
    std::string input = read_available_stdin();
    if (is_blank_sql(input)) {
        return false;
    }
    txn_id_t txn_id = INVALID_TXN_ID;
    bool in_explicit_txn = false;
    bool explicit_txn_failed = false;
    IsolationLevel session_isolation_level = IsolationLevel::SNAPSHOT_ISOLATION;
    char *data_send = new char[BUFFER_LENGTH];
    int offset = 0;
    Transaction *session_txn = nullptr;
    for (const auto &stmt : split_sql_statements(input)) {
        std::string first_keyword = first_sql_keyword(stmt.c_str());
        TxnCommand txn_cmd = is_txn_command_keyword(first_keyword) ? get_txn_command(stmt) : TxnCommand::None;
        if (explicit_txn_failed) {
            write_abort_result(data_send, &offset);
            if (txn_cmd == TxnCommand::Abort || txn_cmd == TxnCommand::Rollback || txn_cmd == TxnCommand::Commit) {
                in_explicit_txn = false;
                explicit_txn_failed = false;
                txn_id = INVALID_TXN_ID;
                session_txn = nullptr;
            }
            continue;
        }
        execute_sql_statement(stmt, &txn_id, &session_txn, data_send, &offset, &session_isolation_level);
        if (txn_cmd == TxnCommand::Begin) {
            in_explicit_txn = true;
            explicit_txn_failed = false;
        } else if (txn_cmd == TxnCommand::Abort || txn_cmd == TxnCommand::Rollback ||
                   txn_cmd == TxnCommand::Commit) {
            in_explicit_txn = false;
            explicit_txn_failed = false;
        } else if (in_explicit_txn && txn_id == INVALID_TXN_ID) {
            explicit_txn_failed = true;
        }
    }
    abort_session_transaction(&txn_id, &session_txn);
    delete[] data_send;
    return true;
}

void *client_handler(void *sock_fd) {
    int fd = *((int *)sock_fd);
    pthread_mutex_unlock(sockfd_mutex);

    int i_recvBytes;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // 需要返回给客户端的结果
    char *data_send = new char[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;
    Transaction *session_txn = nullptr;
    bool in_explicit_txn = false;
    bool explicit_txn_failed = false;
    IsolationLevel session_isolation_level = IsolationLevel::SNAPSHOT_ISOLATION;
    yyscan_t scanner;
    yylex_init(&scanner);

    while (true) {
        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH - 1);

        if (i_recvBytes == 0) {
            break;
        }
        if (i_recvBytes == -1) {
            std::cout << "Client read error!" << std::endl;
            break;
        }
        data_recv[i_recvBytes] = '\0';
        
        if (strcmp(data_recv, "exit") == 0) {
            break;
        }
        if (strcmp(data_recv, "crash") == 0) {
            log_manager->flush_log_to_disk(true);
            exit(1);
        }

        offset = 0;
        data_send[0] = '\0';

        std::string first_keyword = first_sql_keyword(data_recv);
        TxnCommand txn_cmd = is_txn_command_keyword(first_keyword) ? get_txn_command(data_recv) : TxnCommand::None;
        if (explicit_txn_failed) {
            write_abort_result(data_send, &offset);
            if (txn_cmd == TxnCommand::Abort || txn_cmd == TxnCommand::Rollback || txn_cmd == TxnCommand::Commit) {
                in_explicit_txn = false;
                explicit_txn_failed = false;
                txn_id = INVALID_TXN_ID;
                session_txn = nullptr;
            }
            data_send[offset] = '\0';
            if (write(fd, data_send, offset + 1) == -1) {
                break;
            }
            continue;
        }

        if (first_keyword == "SET") {
            if (handle_set_output_file(data_recv)) {
                data_send[offset] = '\0';
                if (write(fd, data_send, offset + 1) == -1) {
                    break;
                }
                continue;
            }
            if (handle_set_transaction_isolation(data_recv, &session_isolation_level)) {
                data_send[offset] = '\0';
                if (write(fd, data_send, offset + 1) == -1) {
                    break;
                }
                continue;
            }
        }

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        if (first_keyword == "CREATE" && is_static_checkpoint_sql(data_recv)) {
            create_static_checkpoint();
            data_send[offset] = '\0';
            if (write(fd, data_send, offset + 1) == -1) {
                break;
            }
            continue;
        }

        Context context(lock_manager.get(), log_manager.get(), nullptr, data_send, &offset, txn_manager.get(),
                        &session_isolation_level);
        SetTransaction(&txn_id, &session_txn, &context);

        if (execute_txn_command_fast(txn_cmd, &txn_id, &context)) {
            if (txn_cmd == TxnCommand::Begin) {
                in_explicit_txn = true;
                explicit_txn_failed = false;
            } else {
                in_explicit_txn = false;
                explicit_txn_failed = false;
                session_txn = nullptr;
            }
            data_send[offset] = '\0';
            if (write(fd, data_send, offset + 1) == -1) {
                break;
            }
            continue;
        }

        std::string load_file;
        std::string load_table;
        if (first_keyword == "LOAD" && parse_load_command(data_recv, &load_file, &load_table)) {
            bool ok = true;
            try {
                load_csv_into_table(load_file, load_table, &context);
            } catch (TransactionAbortException &e) {
                ok = false;
                std::string str = "abort\n";
                memcpy(data_send, str.c_str(), str.length());
                data_send[str.length()] = '\0';
                offset = str.length();
                abort_session_transaction(&txn_id, &session_txn);
                if (output_file_enabled.load()) {
                    std::fstream outfile;
                    outfile.open("output.txt", std::ios::out | std::ios::app);
                    outfile << str;
                    outfile.close();
                }
            } catch (RMDBError &e) {
                ok = false;
                std::cerr << e.what() << std::endl;
                write_rmdb_error_result(e, data_send, &offset);
                if (context.txn_ != nullptr && context.txn_->get_state() == TransactionState::GROWING) {
                    abort_session_transaction(&txn_id, &session_txn);
                }
            }
            if (context.txn_ != nullptr && context.txn_->get_txn_mode() == false &&
                context.txn_->get_state() == TransactionState::GROWING) {
                if (ok) {
                    txn_manager->commit(context.txn_, context.log_mgr_);
                } else {
                    abort_session_transaction(&txn_id, &session_txn);
                }
                txn_id = INVALID_TXN_ID;
                session_txn = nullptr;
            }
            if (in_explicit_txn && txn_id == INVALID_TXN_ID) {
                explicit_txn_failed = true;
            }
            data_send[offset] = '\0';
            if (write(fd, data_send, offset + 1) == -1) {
                break;
            }
            continue;
        }

        bool handled_fast = false;
        try {
            handled_fast = try_execute_simple_sql(data_recv, sm_manager.get(), ql_manager.get(), &context);
        } catch (TransactionAbortException &e) {
            std::string str = "abort\n";
            memcpy(data_send, str.c_str(), str.length());
            data_send[str.length()] = '\0';
            offset = str.length();

            abort_session_transaction(&txn_id, &session_txn);

            if (output_file_enabled.load()) {
                std::fstream outfile;
                outfile.open("output.txt", std::ios::out | std::ios::app);
                outfile << str;
                outfile.close();
            }
            handled_fast = true;
        } catch (RMDBError &e) {
            std::cerr << e.what() << std::endl;
            write_rmdb_error_result(e, data_send, &offset);
            if (context.txn_ != nullptr && context.txn_->get_state() == TransactionState::GROWING) {
                abort_session_transaction(&txn_id, &session_txn);
            }
            handled_fast = true;
        }
        if (handled_fast) {
            if (context.txn_ != nullptr && context.txn_->get_txn_mode() == false &&
               context.txn_->get_state() == TransactionState::GROWING)
            {
                txn_manager->commit(context.txn_, context.log_mgr_);
                txn_id = INVALID_TXN_ID;
                session_txn = nullptr;
            }
            if (txn_cmd == TxnCommand::Begin) {
                in_explicit_txn = true;
                explicit_txn_failed = false;
            } else if (txn_cmd == TxnCommand::Abort || txn_cmd == TxnCommand::Rollback ||
                       txn_cmd == TxnCommand::Commit) {
                in_explicit_txn = false;
                explicit_txn_failed = false;
            } else if (in_explicit_txn && txn_id == INVALID_TXN_ID) {
                explicit_txn_failed = true;
            }
            data_send[offset] = '\0';
            if (write(fd, data_send, offset + 1) == -1) {
                break;
            }
            continue;
        }

        std::shared_ptr<ast::TreeNode> parse_tree;
        ast::parse_tree = nullptr;
        YY_BUFFER_STATE buf = yy_scan_string(data_recv, scanner);
        bool parsed = yyparse(scanner) == 0;
        if (parsed) {
            parse_tree = std::move(ast::parse_tree);
            ast::parse_tree = nullptr;
        }
        yy_delete_buffer(buf, scanner);
        if (parsed) {
            if (parse_tree != nullptr) {
                try {
                    // analyze and rewrite
                    std::shared_ptr<Query> query = analyze->do_analyze(parse_tree);
                    // 优化器
                    std::shared_ptr<Plan> plan = optimizer->plan_query(query, &context);
                    // portal
                    std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, &context);
                    portal->run(portalStmt, ql_manager.get(), &txn_id, &context);
                    portal->drop();
                } catch (TransactionAbortException &e) {
                    // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                    std::string str = "abort\n";
                    memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();

                    // 回滚事务
                    abort_session_transaction(&txn_id, &session_txn);

                    if (output_file_enabled.load()) {
                        std::fstream outfile;
                        outfile.open("output.txt", std::ios::out | std::ios::app);
                        outfile << str;
                        outfile.close();
                    }
                } catch (RMDBError &e) {
                    // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                    std::cerr << e.what() << std::endl;

                    write_rmdb_error_result(e, data_send, &offset);
                    if (context.txn_ != nullptr && context.txn_->get_state() == TransactionState::GROWING) {
                        abort_session_transaction(&txn_id, &session_txn);
                    }
                }
            }
        } else {
            write_failure_result(data_send, &offset);
            if (context.txn_ != nullptr && context.txn_->get_state() == TransactionState::GROWING) {
                abort_session_transaction(&txn_id, &session_txn);
            }
        }
        if(context.txn_ != nullptr && context.txn_->get_txn_mode() == false &&
           context.txn_->get_state() == TransactionState::GROWING)
        {
            txn_manager->commit(context.txn_, context.log_mgr_);
            txn_id = INVALID_TXN_ID;
            session_txn = nullptr;
        }
        if (txn_cmd == TxnCommand::Begin) {
            in_explicit_txn = true;
            explicit_txn_failed = false;
        } else if (txn_cmd == TxnCommand::Abort || txn_cmd == TxnCommand::Rollback ||
                   txn_cmd == TxnCommand::Commit) {
            in_explicit_txn = false;
            explicit_txn_failed = false;
        } else if (in_explicit_txn && txn_id == INVALID_TXN_ID) {
            explicit_txn_failed = true;
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        data_send[offset] = '\0';
        if (write(fd, data_send, offset + 1) == -1) {
            break;
        }
    }

    // Clear
    abort_session_transaction(&txn_id, &session_txn);
    yylex_destroy(scanner);
    close(fd);           // close a file descriptor.
    pthread_exit(NULL);  // terminate calling thread!
}

void start_server() {
    // init mutex
    sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(sockfd_mutex, nullptr);

    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};

    // 初始化连接
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0);  // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(SOCK_PORT);
    fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        std::cout << "Bind error!" << std::endl;
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        std::cout << "Listen error!" << std::endl;
        exit(1);
    }

    while (!should_exit) {
        pthread_t thread_id;
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf)) {
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1) {
            std::cout << "Accept error!" << std::endl;
            continue;  // ignore current socket ,continue while loop.
        }
        
        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd)) != 0) {
            std::cout << "Create thread fail!" << std::endl;
            break;  // break while loop
        }

    }

    // Clear
    int ret = shutdown(sockfd_server, SHUT_WR);  // shut down the all or part of a full-duplex connection.
    if(ret == -1) { printf("%s\n", strerror(errno)); }
//    assert(ret != -1);
    clean_shutdown_storage();
    sm_manager->close_db();
}

int main(int argc, char **argv) {
    if (argc != 2) {
        // 需要指定数据库名称
        std::cerr << "Usage: " << argv[0] << " <database>" << std::endl;
        exit(1);
    }

    signal(SIGINT, sigint_handler);
    try {
        std::cout << "\n"
                     "  _____  __  __ _____  ____  \n"
                     " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
                     " | |__) | \\  / | |  | | |_) |\n"
                     " |  _  /| |\\/| | |  | |  _ < \n"
                     " | | \\ \\| |  | | |__| | |_) |\n"
                     " |_|  \\_\\_|  |_|_____/|____/ \n"
                     "\n"
                     "Welcome to RMDB!\n"
                     "Type 'help;' for help.\n"
                     "\n";
        // Database name is passed by args
        std::string db_name = argv[1];
        if (!sm_manager->is_dir(db_name)) {
            // Database not found, create a new one
            sm_manager->create_db(db_name);
        }
        // Open database
        sm_manager->open_db(db_name);
        buffer_pool_manager->set_log_manager(log_manager.get());

        // recovery database
        recovery->analyze();
        if (recovery->max_txn_id() != INVALID_TXN_ID) {
            txn_manager->set_next_txn_id(recovery->max_txn_id() + 1);
        }
        bool recovered_from_log = recovery->has_recovery_work();
        // Undo all losers first, including transactions that logged ABORT but
        // whose rollback pages were not forced.  Redo committed transactions
        // afterwards so a committed reuse of the same RID wins deterministically.
        // Rebuild the free lists before replay as well: a crash may persist a
        // record page without the matching in-memory file-header update, and
        // the fixed-RID undo/redo helpers require an acyclic free list.
        if (recovered_from_log) {
            sm_manager->rebuild_table_free_lists();
        }
        recovery->undo();
        recovery->redo();
        if (recovered_from_log) {
            sm_manager->rebuild_table_free_lists();
            sm_manager->rebuild_indexes();
            sm_manager->flush_db_files();
        }

        if (run_batch_sql_if_present()) {
            clean_shutdown_storage();
            sm_manager->close_db();
            return 0;
        }

        // 开启服务端，开始接受客户端连接
        start_server();
    } catch (RMDBError &e) {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return 0;
}
