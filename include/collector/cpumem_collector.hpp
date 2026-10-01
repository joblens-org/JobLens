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
    unsigned long long starttime{};
    long hz{};

    // 内存相关
    long mem_vm_kb{};     // 虚拟内存 (VmSize)
    long mem_rss_kb{};    // 物理内存 (VmRSS)
    long mem_swap_kb{};   // 交换区 (VmSwap)
    long mem_peak_rss_kb{};   
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
    bool CPUOf(int pid, CPUMemInfo& info, const CpuSample& sample,
               const std::chrono::steady_clock::time_point& now);
    bool MemOf(int pid, CPUMemInfo& info);
    void prune_stale_pid_states(const std::chrono::steady_clock::time_point& now);
    friend struct CPUMemCollectorTestAccess;
    bool inited = false;
    bool summary = false;

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
