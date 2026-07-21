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
#include <atomic>
#include <string>
#include <chrono>
#include <unordered_map>
#include <unordered_set>

#include "core/collector_type.h"
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

    static CollectorScheduler& instance();

private:
    CollectorScheduler();
    void addPeriodicJobCollector(std::string name, std::string config, const CollectorHandle& collector_handle);
    void addPeriodicSystemCollector(std::string name, std::string config, const CollectorHandle& collector_handle);
    void addJobEventCollectFunc(std::string name, std::string config, const CollectorHandle& collector_handle);
    void addSystemEventCollectFunc(std::string name, std::string config, const CollectorHandle& collector_handle);
    void addCallback(std::string name, OnFinish cb);
    void registerCollectFuncs();
    void registerFinishCallbacks();
    void onJobLifecycle(JobEvent ev, Job& job);
    void onJobAdded(const Job& job);
    void onJobRemoved(const Job& job);
    void onJobUpdated(const Job& job);
    void startPeriodicJobCollector(std::string collector);
    void addJob2PeriodicCollector(size_t jobid, std::string collector);
    void removeJobFromPeriodicCollector(uint64_t jobid, std::string collector);
    void addJob2EventCollector(const Job& job, std::string collector);
    void removeJobFromEventCollector(uint64_t jobid, std::string collector);
    void updateJobForEventCollector(const Job& job, std::string collector);
    void flushEventJobCollector(std::string collector);
    void flushEventSystemCollector(std::string collector);
    Config& global_config = Config::instance();
    TimerScheduler timerScheduler_;

    struct PeriodicJobState{
        std::vector<size_t> jobid_list;
        std::mutex              m_;
        size_t task_id;
        std::atomic<bool>        running;
    };

    struct PeriodicSystemState{
        std::mutex              m_;
        size_t task_id{0};
        std::atomic<bool>        running{false};
    };

    struct EventJobState{
        std::mutex              m_;
        size_t flush_task_id{0};
        std::atomic<bool>        running{false};
        std::atomic<bool>        flushing{false};
        size_t flush_threshold{100};
        size_t max_batch_size{1000};
        std::chrono::milliseconds max_wait{1000};
        std::unordered_set<uint64_t> active_job_ids;
    };

    struct EventSystemState{
        std::mutex              m_;
        size_t flush_task_id{0};
        std::atomic<bool>        running{false};
        std::atomic<bool>        flushing{false};
        size_t flush_threshold{100};
        size_t max_batch_size{1000};
        std::chrono::milliseconds max_wait{1000};
    };

    struct collector_info
    {
        std::string name;
        CollectorScope scope;
        CollectorTrigger trigger;
        std::string config_name;
        double freq;
        ICollector* base{nullptr};
        IPeriodicJobCollector* periodic_job{nullptr};
        IPeriodicSystemCollector* periodic_system{nullptr};
        IEventJobCollector* event_job{nullptr};
        IEventSystemCollector* event_system{nullptr};
        CollectInitFunc init_handle;
        CollectDeinitFunc deinit_handle;
        std::vector<OnFinish> finish_cbs;
    };
    

    std::mutex              m_;
    std::unordered_map<std::string, collector_info> collector_info_dict;
    std::unordered_map<std::string, PeriodicJobState> periodic_job_state_dict;
    std::unordered_map<std::string, PeriodicSystemState> periodic_system_state_dict;
    std::unordered_map<std::string, EventJobState> event_job_state_dict;
    std::unordered_map<std::string, EventSystemState> event_system_state_dict;
    std::unordered_map<std::string, OnFinish>   finishCallbacks_;

    double default_freq;
    std::vector<std::string> default_cbs_name;
    bool                    running_ = false;
};
