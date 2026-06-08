/* Copyright 2026 - 2026 wzycc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "core/collector_type.h"
#include "icollector.h"
#include <spdlog/spdlog.h>
#include "common/ebpf_common.hpp"
#include "common/spin_lock.hpp"
// #include "ebpf/job_fd_basic.h"
#include "ebpf/job_fd_rw_stat.h"
#include <queue>
#include <thread>

struct FileIOInfo{
    std::string path;
    std::string mount_point;
    std::string fs_type;
    unsigned long long pos;
    u64 read_bytes;
    double read_speed;
    u64 write_bytes;
    double write_speed;
    u64 read_mean;
    u64 write_mean;
    s64 read_variance;
    s64 write_variance;
    u64 write_count;
    u64 read_count;
};


struct IOUsageInfo{
    pid_t pid;
    // proc/io信息
    unsigned long long read_bytes;  // 进程从存储层读取的物理字节数
    unsigned long long write_bytes; // 进程已写入到存储层的字节数
    double read_speed;
    double write_speed;

    unsigned long long rchar;   //请求读字节数
    unsigned long long wchar;   //请求写字节数
    double rchar_speed;   //请求读字节速度
    double wchar_speed;   //请求写字节速度

    unsigned long long syscr;   //请求读次数
    unsigned long long syscw;   //请求写次数
    
    // 文件相关信息
    std::unordered_map<int, FileIOInfo> file_info;

    // 块设备相关信息（感觉意义不是太大，先不启用
    // unsigned long long dread_bytes; 
    // unsigned long long dwrite_bytes; 
    // unsigned long long dread_wait_time_ns; 
    // unsigned long long dwrite_wait_time_ns; 
    // unsigned long long dread_count; 
    // unsigned long long dwrite_count; 
};

struct user_pidfd_key{
    uint32_t pid;
    uint32_t fd;
    bool operator==(const user_pidfd_key& o) const noexcept {
        return pid == o.pid && fd == o.fd;
    }
};

namespace std {
    template <>
    struct hash<user_pidfd_key> {
        std::size_t operator()(const user_pidfd_key& k) const noexcept {
            // 简单混合，够用即可
            return (std::size_t(k.pid)  << 16) ^
                   (std::size_t(k.fd)   << 8);
        }
    };
}

class IOUsageCollector : public ICollector{
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type);
private:
    double collect_period;
    struct pid_state{
        std::chrono::steady_clock::time_point last_time{};
        unsigned long long last_read_bytes{};
        unsigned long long last_write_bytes{};
        unsigned long long last_rchar{};
        unsigned long long last_wchar{};
    };
    std::unordered_map<int, pid_state> pid_state_dict;
    std::unordered_map<uint64_t, pid_state> job_summary_state_dict;
    bool summary;
    // bpf 相关
    struct user_rw_stat{
        std::chrono::steady_clock::time_point last_read_time;
        u64 read_time{0};
        std::chrono::steady_clock::time_point last_write_time;
        u64 write_time{0};
        u64 read_bytes{0};
        u64 last_read_bytes{0};
        double read_speed{0};
        u64 write_bytes{0};
        u64 last_write_bytes{0};
        double write_speed{0};
        u64 read_mean{0};
        u64 write_mean{0};
        u64 read_variance{0};
        u64 write_variance{0};
        u64 write_count{0};
        u64 read_count{0};
    };



    
    bool use_ebpf{false};
    bool init_ebpf();
    void deinit_ebpf();
    bool polling_running;
    std::unique_ptr<std::thread> poll_thread;
    std::optional<IOUsageCollector::user_rw_stat> get_fd_stat(pid_t pid, uint32_t fd);
    SpinLock event_swap_mtx_;
    std::unique_ptr<std::queue<event>> event_front_;
    std::unique_ptr<std::queue<event>> event_back_;
    // std::string bpf_o_path = "bpf_obj/job_fd_basic.bpf.o";
    std::string bpf_o_path = "lib/joblens/bpf_obj/job_fd_rw_stat.bpf.o";
    std::string rb_name = "event_rb";
    std::string pid2jobid_map_name = "pid2job";
    std::string pid2fdstat_map_name = "job_fd_stat";
    std::vector<bpf_link*> bpf_links_;
    // (pid,fd) -> user_rw_stat
    std::unordered_map<user_pidfd_key, user_rw_stat> pidfd_stat_map;
    bpf_object* bpf_obj_;
    ring_buffer* bpf_rb_;
};

