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
#include <chrono>
#include <spdlog/spdlog.h>

struct CPUMemInfo{
    pid_t         pid{};
    pid_t        ppid{};
    std::string name;
    double      cpuPercent{};
    unsigned long long utime{};
    unsigned long long stime{};
    // 自进程启动以来已被回收（waitpid）的子进程 CPU 时间，单位 jiffies。
    // 子进程退出后其 /proc/<pid> 消失，只有这两个累计量能把它已经消耗的 CPU 留下来。
    unsigned long long cutime{};
    unsigned long long cstime{};
    unsigned long long starttime{};
    long hz{};

    // 内存相关
    long mem_vm_kb{};     // 虚拟内存 (VmSize)
    long mem_rss_kb{};    // 物理内存 (VmRSS)
    long mem_swap_kb{};   // 交换区 (VmSwap)
    // 进程级记录：该进程自身的 VmHWM 峰值；
    // 汇总记录 (pid=0)：本次采样时刻进程聚合 RSS 的时序最大值（不是各进程峰值的和）。
    // 两者含义不同，汇总口径见 CPUMemCollector::update_job_peak()。
    long mem_peak_rss_kb{};
    // PSS（Proportional Set Size）。-1 表示不可用（未开启/内核不支持/本次读取失败）。
    // 进程级记录为单进程 PSS，汇总记录为本次采样各进程 PSS 之和。
    long mem_pss_kb{-1};
    // 仅汇总记录有效：聚合 PSS 的时序最大值；-1 表示本次作业从未取到 PSS。
    long mem_peak_pss_kb{-1};
    // 仅汇总记录有效：参与峰值统计的采样次数，用于判断峰值/均值是否只基于 1 次采样。
    unsigned long long sample_count{};
    double      memoryPercent{};
    int         numThreads{};
};

class CPUMemCollector : public ICollector {
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type);
    CollectDataParseFuncV2 get_writer_parser_v2(const std::string& writer_type) override;

private:
    struct CpuSample {
        long hz;
        long numCores;
        unsigned long long total;
    };
    // PSS 读取策略：Rollup 读 /proc/<pid>/smaps_rollup（内核 4.14+，单次聚合读）；
    // Smaps 为旧内核降级路径（逐 VMA 累加 Pss 行，代价明显更高）；Disabled 表示不采集 PSS。
    enum class PssMode { Disabled, Rollup, Smaps };
    bool CPUOf(int pid, CPUMemInfo& info, const CpuSample& sample,
               const std::chrono::steady_clock::time_point& now);
    bool MemOf(int pid, CPUMemInfo& info);
    bool PssOf(int pid, CPUMemInfo& info);
    bool ReadRollupPssKb(int pid, long& pss_kb);
    bool ReadSmapsPssKb(int pid, long& pss_kb);
    void update_job_peak(const Job& job, CPUMemInfo& summary_info,
                         const std::chrono::steady_clock::time_point& now);
    void prune_stale_pid_states(const std::chrono::steady_clock::time_point& now);
    friend struct CPUMemCollectorTestAccess;
    bool inited = false;
    bool summary = false;
    bool pss_enabled = true;
    bool pss_fallback_smaps = true;
    PssMode pss_mode = PssMode::Disabled;

    // 作业级峰值状态：按 JobID 保存“采样聚合值”的时序最大值。
    // 采集器实例跨作业共享，因此必须按作业隔离；作业结束后靠 TTL 回收。
    struct JobPeakState {
        long peak_rss_kb{0};
        long peak_pss_kb{-1};
        unsigned long long samples{0};
        std::chrono::steady_clock::time_point last_seen{};
    };
    std::unordered_map<uint64_t, JobPeakState> job_peak_map;

    long getTotalPhysMemKB()
    {
        FILE* fp = std::fopen("/proc/meminfo", "r");
        if (!fp) return -1;
        long kb = 0;
        if (std::fscanf(fp, "MemTotal: %ld kB", &kb) != 1) kb = -1;
        std::fclose(fp);
        return kb;
    }

    long PhysMemKB = getTotalPhysMemKB();

    struct pid_state{
        unsigned long long lastProc{};
        unsigned long long lastStarttime{};
        // 该 PID 上次采样时的系统总 jiffies。按 PID 缓存，保证 Δproc 与 Δtotal
        // 覆盖同一个采样区间——采样器实例在所有作业间共享，不能用实例级系统基线。
        unsigned long long lastTotal{};
        // 该 PID 最近一次被采样的时刻，用于筛选需要检查进程身份的陈旧基线
        std::chrono::steady_clock::time_point last_seen{};
    };
    std::unordered_map<int, pid_state> pid_state_dict;
    std::chrono::steady_clock::time_point last_prune_{};

};
