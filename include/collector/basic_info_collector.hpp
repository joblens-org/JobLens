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

#include "core/collector_type.h"
#include "icollector.h"
#include <spdlog/spdlog.h>
#include <linux/taskstats.h>
#include <unordered_map>
#include <chrono>

struct BasicInfo {
    pid_t pid{};
    std::string name;
    
    // CPU统计 (百分比和纳秒)
    double cpuPercent{};
    uint64_t cpuUserNs{};      // 用户态CPU时间(纳秒)
    uint64_t cpuSystemNs{};    // 内核态CPU时间(纳秒)
    uint64_t cpuTotalNs{};     // 总CPU时间(纳秒)
    
    // 内存统计
    double memoryPercent{};
    uint64_t memRssBytes{};    // RSS (字节)
    uint64_t memVmBytes{};     // 虚拟内存 (字节)
    
    // IO统计
    double readSpeed{};        // 读取速度 (字节/秒)
    double writeSpeed{};       // 写入速度 (字节/秒)
    uint64_t readBytes{};      // 总读取字节数
    uint64_t writeBytes{};     // 总写入字节数
    uint64_t readOps{};        // 读取操作次数
    uint64_t writeOps{};       // 写入操作次数
    
    // 其他
    int numThreads{};
    uint64_t voluntaryCtxSw{}; // 自愿上下文切换
    uint64_t nonvoluntaryCtxSw{}; // 非自愿上下文切换
};


class BasicInfoCollector : public ICollector {
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;

private:
    // Netlink连接管理
    bool connect_to_taskstats();
    void disconnect_from_taskstats();
    
    // 核心采集函数
    bool get_taskstats_for_tgid(int tgid, struct taskstats* out_stats);
    
    // 计算函数
    void calculate_cpu_percent(BasicInfo& info, const struct taskstats& stats, 
                               const std::chrono::steady_clock::time_point& now);
    void calculate_io_speed(BasicInfo& info, const struct taskstats& stats,
                           const std::chrono::steady_clock::time_point& now);
    void calculate_memory_percent(BasicInfo& info, const struct taskstats& stats);
    
    uint64_t get_total_memory_bytes();

    bool inited = false;
    bool summary = false;
    
    // Netlink相关
    struct nl_sock* nl_sock = nullptr;
    int family_id = -1;
    
    // 物理内存总量
    uint64_t totalMemoryBytes = 0;
    
    // 状态缓存 (用于计算速度和百分比)
    struct pid_state {
        uint64_t lastCpuUserNs = 0;
        uint64_t lastCpuSystemNs = 0;
        uint64_t lastReadBytes = 0;
        uint64_t lastWriteBytes = 0;
        uint64_t lastReadOps = 0;
        uint64_t lastWriteOps = 0;
        std::chrono::steady_clock::time_point lastTime;
    };
    std::unordered_map<int, pid_state> pid_state_dict;
};

