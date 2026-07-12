/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <cerrno>
#include <limits>
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"

static_assert(sizeof(off_t) >= sizeof(wal_offset_t), "RMDB requires 64-bit file offsets");

DiskManager::DiskManager() {
    for (int i = 0; i < MAX_FD; ++i) {
        fd2pageno_[i] = 0;
    }
}

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    off_t pos = static_cast<off_t>(page_no) * PAGE_SIZE;
    ssize_t bytes_written = pwrite(fd, offset, num_bytes, pos);
    if (bytes_written != num_bytes) {
        throw InternalError("DiskManager::write_page Error");
    }
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    off_t pos = static_cast<off_t>(page_no) * PAGE_SIZE;
    ssize_t bytes_read = pread(fd, offset, num_bytes, pos);
    if (bytes_read != num_bytes) {
        throw InternalError("DiskManager::read_page Error");
    }
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    if (mkdir(path.c_str(), 0755) < 0 && errno != EEXIST) {
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true 
 * @param {string} &path 指定路径文件
 */
bool DiskManager::is_file(const std::string &path) {
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    if (is_file(path)) {
        throw FileExistsError(path);
    }
    int fd = open(path.c_str(), O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        throw UnixError();
    }
    if (close(fd) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string &path) {
    // Todo:
    // 调用unlink()函数
    // 注意不能删除未关闭的文件
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    if (path2fd_.count(path) != 0) {
        throw FileNotClosedError(path);
    }
    if (unlink(path.c_str()) < 0) {
        throw UnixError();
    }
    if (path == LOG_FILE_NAME) {
        log_fd_ = -1;
        log_write_offset_.store(-1, std::memory_order_relaxed);
    }
}


/**
 * @description: 打开指定路径文件 
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    if (path2fd_.count(path) != 0) {
        // 文件已被打开，不能重复打开
        throw FileNotClosedError(path);
    }
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        throw UnixError();
    }
    path2fd_[path] = fd;
    fd2path_[fd] = path;
    return fd;
}

/**
 * @description:用于关闭指定路径文件 
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    if (close(fd) < 0) {
        throw UnixError();
    }
    if (fd == log_fd_) {
        log_fd_ = -1;
        log_write_offset_.store(-1, std::memory_order_relaxed);
    }
    auto path = fd2path_[fd];
    fd2path_.erase(fd);
    path2fd_.erase(path);
}


/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
wal_offset_t DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? static_cast<wal_offset_t>(stat_buf.st_size) : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}


/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char *log_data, int size, wal_offset_t offset) {
    if (log_data == nullptr || size < 0 || offset < 0) {
        return -1;
    }
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    wal_offset_t file_size = get_file_size(LOG_FILE_NAME);
    if (file_size < 0 || offset > file_size) {
        return -1;
    }

    wal_offset_t available = file_size - offset;
    size = static_cast<int>(std::min<wal_offset_t>(size, available));
    if(size == 0) return 0;
    ssize_t bytes_read = pread(log_fd_, log_data, size, static_cast<off_t>(offset));
    assert(bytes_read == size);
    return bytes_read;
}


/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    wal_offset_t offset = log_write_offset_.load(std::memory_order_relaxed);
    if (offset < 0) {
        offset = get_file_size(LOG_FILE_NAME);
        if (offset < 0) {
            throw InternalError("Failed to determine WAL size");
        }
        log_write_offset_.store(offset, std::memory_order_relaxed);
    }
    if (size < 0 || offset > std::numeric_limits<wal_offset_t>::max() - size) {
        throw InternalError("WAL offset overflow");
    }
    offset = log_write_offset_.fetch_add(size, std::memory_order_relaxed);
    ssize_t bytes_write = pwrite(log_fd_, log_data, size, static_cast<off_t>(offset));
    if (bytes_write != size) {
        throw UnixError();
    }
}

void DiskManager::sync_log() {
    if (log_fd_ == -1) {
        if (!is_file(LOG_FILE_NAME)) {
            return;
        }
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    if (fdatasync(log_fd_) < 0) {
        throw UnixError();
    }
}

void DiskManager::truncate_log(wal_offset_t size) {
    if (size < 0 || size > static_cast<wal_offset_t>(std::numeric_limits<off_t>::max())) {
        throw InternalError("Invalid WAL truncate offset");
    }
    if (log_fd_ == -1) {
        log_fd_ = get_file_fd(LOG_FILE_NAME);
    }
    if (ftruncate(log_fd_, static_cast<off_t>(size)) < 0) {
        throw UnixError();
    }
    if (fdatasync(log_fd_) < 0) {
        throw UnixError();
    }
    log_write_offset_.store(size, std::memory_order_release);
}

void DiskManager::sync_file(int fd) {
    if (fd < 0) {
        throw FileNotOpenError(fd);
    }
    if (fdatasync(fd) < 0) {
        throw UnixError();
    }
}
