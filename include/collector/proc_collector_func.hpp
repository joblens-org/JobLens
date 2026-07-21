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

#include <dirent.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <fmt/core.h>
#include <memory>
#include "core/collector_type.h"
#include "icollector.h"
#include <any>
// 前置声明，降低头文件耦合
class Job;

namespace proc_collector {

struct proc_info {
    int8_t      type{int8_t(CollectorType::ProcCollector)};
    int         pid{};
    std::string name;
    int         ppid{};
    // CPU 相关信息
    double      cpuPercent{};      
    unsigned long long utime{};
    unsigned long long stime{};
    unsigned long long starttime{};
    long hz{};
    long numCores{};
    
    // 内存相关信息
    std::size_t memoryRss{};       // 字节
    double      memoryPercent{};
    int         numThreads{};
    int         ioReadCount{};
    int         ioWriteCount{};
    int         netConnCount{};
    std::string status{"unknown"};
};

class ProcCollector : public IPeriodicJobCollector {
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;
private:
    std::any impl_collect(const Job& job);
    std::unique_ptr<proc_info> snapshotOf(int pid);

    struct pid_state{
        unsigned long long lastTotal{};
        unsigned long long lastProc{};
    };

    std::unordered_map<int, pid_state> pid_state_dict;
    bool inited;
};


} // namespace proc_collector
