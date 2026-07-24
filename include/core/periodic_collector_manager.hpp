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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/timer_scheduler.hpp"
#include "core/collector_runtime.hpp"

class PeriodicJobManager {
public:
    PeriodicJobManager(TimerScheduler& timer_scheduler,
                       CollectorInfoMap& collector_infos,
                       const FinishCallbackMap& finish_callbacks,
                       const std::vector<std::string>& default_callback_names,
                       double default_freq);

    void registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle);
    bool startCollector(const std::string& collector_name);
    bool stopCollector(const std::string& collector_name);
    void addJob(size_t jobid, const std::string& collector);
    void removeJob(uint64_t jobid, const std::string& collector);
    nlohmann::json getCollectorStatus(const std::string& name) const;
    nlohmann::json listCollectors() const;
    void shutdown();

private:
    struct State {
        std::vector<size_t> jobid_list;
        mutable std::mutex m_;
        size_t task_id{0};
        std::atomic<bool> running{false};
    };

    TimerScheduler& timerScheduler_;
    CollectorInfoMap& collector_info_dict_;
    const FinishCallbackMap& finishCallbacks_;
    const std::vector<std::string>& default_cbs_name_;
    double default_freq_;
    mutable std::mutex m_;
    std::unordered_map<std::string, State> states_;
};

class PeriodicSystemManager {
public:
    PeriodicSystemManager(TimerScheduler& timer_scheduler,
                          CollectorInfoMap& collector_infos,
                          const FinishCallbackMap& finish_callbacks,
                          const std::vector<std::string>& default_callback_names,
                          double default_freq);

    void registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle);
    bool startCollector(const std::string& name);
    bool stopCollector(const std::string& name);
    nlohmann::json getCollectorStatus(const std::string& name) const;
    nlohmann::json listCollectors() const;
    void shutdown();

private:
    struct State {
        mutable std::mutex m_;
        size_t task_id{0};
        std::atomic<bool> running{false};
    };

    TimerScheduler& timerScheduler_;
    CollectorInfoMap& collector_info_dict_;
    const FinishCallbackMap& finishCallbacks_;
    const std::vector<std::string>& default_cbs_name_;
    double default_freq_;
    mutable std::mutex m_;
    std::unordered_map<std::string, State> states_;
};
