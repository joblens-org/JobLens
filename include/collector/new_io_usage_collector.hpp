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
#include <bpf/libbpf.h>
#include "ebpf/job_io_new.h"
#include <unordered_map>
#include <chrono>

// 统一 I/O 计量结构：VFS syscall 层语义（弃用物理块 read_bytes/write_bytes）
struct IoCounters {
    u64 rchar{0}, wchar{0};
    u64 syscr{0}, syscw{0};
    u64 read_mean{0}, write_mean{0};
    s64 read_variance{0}, write_variance{0};
    double rchar_speed{0}, wchar_speed{0};  // 用户态差分
};

// 文件内单个进程的 IO
struct ProcFileIO {
    pid_t pid{0};
    bool alive{false};
    IoCounters io;
};

// 文件级：total + 按进程细分
struct FileIOStat {
    std::string path;
    std::string mount_point;
    std::string fs_type;
    unsigned long long pos{0};
    IoCounters total;
    std::unordered_map<pid_t, ProcFileIO> processes;
};

// 进程级聚合（含短命进程）
struct ProcIOStat {
    pid_t pid{0};
    std::string source;  // "alive" | "ephemeral"
    bool alive{false};
    IoCounters io;
};

// 时延直方图
struct LatencyHist {
    u64 read_hist[LATENCY_BUCKETS]{};
    u64 write_hist[LATENCY_BUCKETS]{};
};

// 顶层：一次 collect 返回一个 Job 的完整快照
struct JobIOStat {
    uint64_t job_id{0};
    double collect_period{0};
    IoCounters job_total;
    LatencyHist job_latency;
    std::unordered_map<std::string, FileIOStat> files;
    std::unordered_map<pid_t, ProcIOStat> processes;
};

class NewIOUsageCollector : public ICollector {
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;
private:
    bool init_ebpf();
    void deinit_ebpf();
    // 周期缓存：DUMP_TTL 内共享一次全表遍历
    void refresh_dump_cache_if_needed();
    // 清理已死且已输出过的短命进程的 eBPF 条目
    void cleanup_dead_pids();
    // 短命进程生命周期状态
    struct EphemeralState { uint64_t job_id{0}; uint64_t output_count{0}; bool alive{false}; };

    std::string bpf_o_path = JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/job_io_new.bpf.o";
    std::string pid2jobid_map_name = "pid2job";
    std::string cgroup2jobid_map_name = "cgroup2job";
    std::string jobstat_map_name = "job_stat";
    std::string jobfdstat_map_name = "job_fd_stat";
    std::string latency_map_name = "latency_hist";

    bpf_object* bpf_obj_{nullptr};
    std::vector<bpf_link*> bpf_links_;

    // 周期遍历缓存
    static constexpr int DUMP_TTL_MS = 200;
    std::chrono::steady_clock::time_point last_dump_time_{};
    std::vector<job_pid_fd_key> dump_keys_;
    std::vector<rw_stat> dump_vals_;

    // 短命进程状态 + 上周期基线
    std::unordered_map<pid_t, EphemeralState> known_pids_;
    std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> last_job_time_;
    std::unordered_map<pid_t, IoCounters> last_proc_io_;
    std::unordered_map<std::string, IoCounters> last_file_io_;
    std::unordered_map<uint64_t, IoCounters> last_job_io_;
    std::unordered_map<uint64_t, LatencyHist> last_job_latency_;
};
