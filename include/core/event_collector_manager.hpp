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

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/timer_scheduler.hpp"
#include "core/collector_runtime.hpp"

class EventJobManager {
public:
    EventJobManager(TimerScheduler& timer_scheduler,
                    CollectorInfoMap& collector_infos,
                    const FinishCallbackMap& finish_callbacks,
                    const std::vector<std::string>& default_callback_names);

    void registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle);
    bool startCollector(const std::string& name);
    bool stopCollector(const std::string& name);
    void addJob(const Job& job, const std::string& collector);
    void removeJob(uint64_t jobid, const std::string& collector);
    void updateJob(const Job& job, const std::string& collector);
    nlohmann::json getCollectorStatus(const std::string& name) const;
    nlohmann::json listCollectors() const;
    void shutdown();

private:
    struct State {
        mutable std::mutex m_;
        size_t flush_task_id{0};
        std::atomic<bool> running{false};
        std::atomic<bool> flushing{false};
        size_t flush_threshold{100};
        size_t max_batch_size{1000};
        std::chrono::milliseconds max_wait{1000};
        std::unordered_set<uint64_t> active_job_ids;
    };

    void flushCollector(const std::string& collector);
    void configureState(const std::string& name, const std::string& config);

    TimerScheduler& timerScheduler_;
    CollectorInfoMap& collector_info_dict_;
    const FinishCallbackMap& finishCallbacks_;
    const std::vector<std::string>& default_cbs_name_;
    mutable std::mutex m_;
    std::unordered_map<std::string, State> states_;
};

class EventSystemManager {
public:
    EventSystemManager(TimerScheduler& timer_scheduler,
                       CollectorInfoMap& collector_infos,
                       const FinishCallbackMap& finish_callbacks,
                       const std::vector<std::string>& default_callback_names);

    void registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle);
    bool startCollector(const std::string& name);
    bool stopCollector(const std::string& name);
    nlohmann::json getCollectorStatus(const std::string& name) const;
    nlohmann::json listCollectors() const;
    void shutdown();

private:
    struct State {
        mutable std::mutex m_;
        size_t flush_task_id{0};
        std::atomic<bool> running{false};
        std::atomic<bool> flushing{false};
        size_t flush_threshold{100};
        size_t max_batch_size{1000};
        std::chrono::milliseconds max_wait{1000};
    };

    void flushCollector(const std::string& collector);
    void configureState(const std::string& name, const std::string& config);

    TimerScheduler& timerScheduler_;
    CollectorInfoMap& collector_info_dict_;
    const FinishCallbackMap& finishCallbacks_;
    const std::vector<std::string>& default_cbs_name_;
    mutable std::mutex m_;
    std::unordered_map<std::string, State> states_;
};
