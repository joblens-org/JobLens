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

#include <mutex>
#include <vector>
#include <string>

#include "core/collector_type.h"
#include "core/collector_runtime.hpp"
#include "core/event_collector_manager.hpp"
#include "core/periodic_collector_manager.hpp"
#include "common/config.hpp"
#include "common/timer_scheduler.hpp"
#include <nlohmann/json.hpp>

#include "job_lifecycle_event.h"

class CollectorScheduler {
public:
    ~CollectorScheduler();

    CollectorScheduler(const CollectorScheduler&)            = delete;
    CollectorScheduler& operator=(const CollectorScheduler&) = delete;


    void start();
    void shutdown();
    
    nlohmann::json snapshot();
    nlohmann::json getCollector(const std::string& name);
    nlohmann::json startCollector(const std::string& name);
    nlohmann::json stopCollector(const std::string& name);

    static CollectorScheduler& instance();

private:
    CollectorScheduler();
    void addCallback(std::string name, OnFinish cb);
    void registerCollectFuncs();
    void registerFinishCallbacks();
    void registerRPCMethods();
    void onJobLifecycle(JobEvent ev, Job& job);
    void onJobAdded(const Job& job);
    void onJobRemoved(const Job& job);
    void onJobUpdated(const Job& job);
    Config& global_config = Config::instance();
    TimerScheduler timerScheduler_;
    
    
    std::mutex              m_;
    CollectorInfoMap collector_info_dict;
    FinishCallbackMap finishCallbacks_;

    double default_freq;
    std::vector<std::string> default_cbs_name;
    PeriodicJobManager periodic_job_manager_;
    PeriodicSystemManager periodic_system_manager_;
    EventJobManager event_job_manager_;
    EventSystemManager event_system_manager_;
    bool                    running_ = false;
};
