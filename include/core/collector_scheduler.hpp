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

#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>
#include <atomic>
#include <future>
#include <string>
#include <unordered_map>
#include <optional>
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <date/date.h>

#include "collector/proc_collector_func.hpp"
#include "core/collector_type.h"
#include "common/config.hpp"
#include "common/streamer_watcher.hpp"
#include "common/timer_scheduler.hpp"
#include "core/writer_manager.hpp"
#include <nlohmann/json.hpp>

#include "job_lifecycle_event.h"

class CollectorScheduler {
public:
    ~CollectorScheduler();

    // 非拷贝、可移动
    CollectorScheduler(const CollectorScheduler&)            = delete;
    CollectorScheduler& operator=(const CollectorScheduler&) = delete;
    CollectorScheduler(CollectorScheduler&&)                 = default;
    CollectorScheduler& operator=(CollectorScheduler&&)      = default;


    void start();
    void shutdown();
    
    nlohmann::json snapshot();

    static CollectorScheduler& instance();

private:
    CollectorScheduler();
    void addJobCollectFunc(std::string name, std::string config, CollectFunc collector_handle,CollectInitFunc init_handle,CollectDeinitFunc deinit_handle);
    void addSystemCollectFunc(std::string name, std::string config, CollectFunc collector_handle,CollectInitFunc init_handle,CollectDeinitFunc deinit_handle);
    void addCallback(std::string name, OnFinish cb);
    void registerCollectFuncs();
    void registerFinishCallbacks();
    void onJobLifecycle(JobEvent ev, Job& job);
    void addJobCollect(const Job& job);
    void startCollector(std::string collector);
    void addJob2Collector(size_t jobid, std::string collector);
    void rmJobCollect(const Job& job);
    void updateJobCollect(const Job& job);
    Config& global_config = Config::instance();
    TimerScheduler timerScheduler_;

    struct collector_state{
        std::vector<size_t> jobid_list;
        std::mutex              m_;
        size_t task_id;
        std::atomic<bool>        running;
    };

    struct collector_info
    {
        std::string name;
        CollectorScope scope;
        std::string config_name;
        double freq;
        CollectFunc collect_handle;
        CollectInitFunc init_handle;
        CollectDeinitFunc deinit_handle;
        std::vector<OnFinish> finish_cbs;
    };
    

    std::mutex              m_;
    std::unordered_map<std::string, collector_info> collector_info_dict;
    std::unordered_map<std::string, collector_state> collector_state_dict;
    std::unordered_map<std::string, OnFinish>   finishCallbacks_;

    double default_freq;
    std::vector<std::string> default_cbs_name;
    bool                    running_ = false;
};