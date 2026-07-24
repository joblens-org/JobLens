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

#include <string>
#include <unordered_map>
#include <vector>

#include "core/collector_type.h"

inline const char* to_string(CollectorScope scope) {
    switch (scope) {
        case CollectorScope::Job: return "Job";
        case CollectorScope::System: return "System";
        default: return "Undefined";
    }
}

inline const char* to_string(CollectorTrigger trigger) {
    switch (trigger) {
        case CollectorTrigger::Periodic: return "Periodic";
        case CollectorTrigger::EventBatch: return "EventBatch";
        default: return "Undefined";
    }
}

struct CollectorRuntimeInfo {
    std::string name;
    CollectorScope scope{CollectorScope::Undefined};
    CollectorTrigger trigger{CollectorTrigger::Undefined};
    std::string config_name;
    double freq{0.0};
    ICollector* base{nullptr};
    IPeriodicJobCollector* periodic_job{nullptr};
    IPeriodicSystemCollector* periodic_system{nullptr};
    IEventJobCollector* event_job{nullptr};
    IEventSystemCollector* event_system{nullptr};
    CollectInitFunc init_handle;
    CollectDeinitFunc deinit_handle;
    std::vector<OnFinish> finish_cbs;
};

using CollectorInfoMap = std::unordered_map<std::string, CollectorRuntimeInfo>;
using FinishCallbackMap = std::unordered_map<std::string, OnFinish>;

inline Job makeSystemCollectorJob() {
    Job sys_job;
    sys_job.JobID = 0;
    sys_job.jobtype = JobType::Sys;
    sys_job.subtype = JobSubType::Common;
    return sys_job;
}

inline nlohmann::json makeCollectorStatusBase(const CollectorRuntimeInfo& info) {
    nlohmann::json status;
    status["name"] = info.name;
    status["scope"] = to_string(info.scope);
    status["trigger"] = to_string(info.trigger);
    status["config_name"] = info.config_name;
    status["freq"] = info.freq;
    status["writer_count"] = info.finish_cbs.size();
    return status;
}
