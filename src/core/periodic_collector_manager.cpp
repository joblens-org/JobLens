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
#include "core/periodic_collector_manager.hpp"

#include "common/config.hpp"
#include "common/utils.hpp"
#include "collector/icollector.h"
#include "core/collector_manager_utils.hpp"
#include "core/job_registry.hpp"

#include <algorithm>
#include <spdlog/spdlog.h>

PeriodicJobManager::PeriodicJobManager(TimerScheduler& timer_scheduler,
                                       CollectorInfoMap& collector_infos,
                                       const FinishCallbackMap& finish_callbacks,
                                       const std::vector<std::string>& default_callback_names,
                                       double default_freq)
    : timerScheduler_(timer_scheduler),
      collector_info_dict_(collector_infos),
      finishCallbacks_(finish_callbacks),
      default_cbs_name_(default_callback_names),
      default_freq_(default_freq) {}

void PeriodicJobManager::registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle) {
    if (!collector_handle.periodic_job) {
        spdlog::error("CollectorScheduler: {} does not implement IPeriodicJobCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    auto& info = collector_info_dict_[name];
    info.name = name;
    info.config_name = config;
    info.base = collector_handle.base;
    info.periodic_job = collector_handle.periodic_job;
    info.init_handle = collector_handle.init;
    info.deinit_handle = collector_handle.deinit;
    info.scope = CollectorScope::Job;
    info.trigger = CollectorTrigger::Periodic;
    info.freq = resolveCollectorFreq(name, config, default_freq_, true);
    info.finish_cbs = resolveCollectorFinishCallbacks(name, config, finishCallbacks_, default_cbs_name_);
}

bool PeriodicJobManager::startCollector(const std::string& collector_name) {
    auto& state = states_[collector_name];
    if (state.running) return true;
    CollectorRuntimeInfo info;
    try {
        info = collector_info_dict_.at(collector_name);
    }
    catch(const std::exception& e) {
        spdlog::error("CollectorScheduler: start collector error, can not find {}!", collector_name);
        return false;
    }
    try {
        auto config_node = Config::instance().getRawNode(info.config_name);
        auto j_config = Utils::yamlToJson(config_node);
        if (!info.init_handle(j_config)) {
            spdlog::error("CollectorScheduler: collector {} init failed", collector_name);
            return false;
        }
        auto freq = info.freq;
        state.task_id = timerScheduler_.registerRepeatingTimer(
            std::chrono::milliseconds(static_cast<long long>(1000.0 / freq)),
            [this, collector_name](){
                auto it_info = collector_info_dict_.find(collector_name);
                if (it_info == collector_info_dict_.end()) return;
                auto& info_ref = it_info->second;

                auto it_state = states_.find(collector_name);
                if (it_state == states_.end()) return;
                auto& state_ref = it_state->second;
                std::vector<size_t> jobids_snapshot;
                {
                    std::lock_guard lg(state_ref.m_);
                    if(state_ref.jobid_list.empty() && state_ref.running){
                        info_ref.deinit_handle();
                        timerScheduler_.cancelTimer(state_ref.task_id);
                        state_ref.running = false;
                        return;
                    }
                    jobids_snapshot = state_ref.jobid_list;
                }

                for(auto jobid: jobids_snapshot){
                    auto job = JobRegistry::instance().findJob(jobid);
                    std::lock_guard lg(state_ref.m_);
                    if(!state_ref.running) return;
                    if(!job.has_value()) continue;
                    try {
                        auto ret = info_ref.periodic_job->collect(job.value());
                        auto now = std::chrono::system_clock::now();
                        for(const auto& cb:info_ref.finish_cbs){
                            cb(collector_name, job.value(), ret, now);
                        }
                    }
                    catch(const std::exception& e) {
                        spdlog::error("CollectorScheduler: collector {} collect error: {}", collector_name, e.what());
                    }
                }
            }
        );
        state.running = true;
    }
    catch(const std::exception& e) {
        spdlog::error("CollectorScheduler: start collector {} error: {}", collector_name, e.what());
        return false;
    }
    spdlog::info("CollectorScheduler: start collector {}", collector_name);
    return true;
}

bool PeriodicJobManager::stopCollector(const std::string& collector_name) {
    auto it = states_.find(collector_name);
    auto info_it = collector_info_dict_.find(collector_name);
    if (info_it == collector_info_dict_.end()) return false;
    if (it == states_.end()) return true;
    auto& state = it->second;
    std::lock_guard lg(state.m_);
    if (!state.running) {
        state.jobid_list.clear();
        return true;
    }
    state.running = false;
    timerScheduler_.cancelTimer(state.task_id);
    info_it->second.deinit_handle();
    state.jobid_list.clear();
    state.task_id = 0;
    return true;
}

void PeriodicJobManager::addJob(size_t jobid, const std::string& collector) {
    auto& state = states_[collector];
    std::lock_guard lg(state.m_);
    if (std::find(state.jobid_list.begin(), state.jobid_list.end(), jobid) != state.jobid_list.end()) {
        return;
    }
    state.jobid_list.push_back(jobid);
    if(!state.running){
        startCollector(collector);
    }
}

void PeriodicJobManager::removeJob(uint64_t jobid, const std::string& collector) {
    auto it = states_.find(collector);
    if (it == states_.end()) return;
    auto& state = it->second;
    std::lock_guard lg(state.m_);
    state.jobid_list.erase(std::remove_if(state.jobid_list.begin(),state.jobid_list.end(),[jobid](size_t x){return x==jobid;}));
}

nlohmann::json PeriodicJobManager::getCollectorStatus(const std::string& name) const {
    nlohmann::json status;
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return status;
    status = makeCollectorStatusBase(info_it->second);
    status["running"] = false;
    status["task_id"] = 0;
    status["job_count"] = 0;
    status["job_ids"] = nlohmann::json::array();
    auto state_it = states_.find(name);
    if (state_it != states_.end()) {
        auto& state = state_it->second;
        std::lock_guard lg(state.m_);
        status["running"] = state.running.load();
        status["task_id"] = state.task_id;
        status["job_count"] = state.jobid_list.size();
        for (auto jobid : state.jobid_list) {
            status["job_ids"].push_back(jobid);
        }
    }
    return status;
}

nlohmann::json PeriodicJobManager::listCollectors() const {
    nlohmann::json collectors = nlohmann::json::array();
    for (const auto& [name, state] : states_) {
        collectors.push_back(getCollectorStatus(name));
    }
    return collectors;
}

void PeriodicJobManager::shutdown() {
    std::vector<std::string> names;
    for (const auto& [name, state] : states_) {
        names.push_back(name);
    }
    for (const auto& name : names) {
        stopCollector(name);
    }
}

PeriodicSystemManager::PeriodicSystemManager(TimerScheduler& timer_scheduler,
                                             CollectorInfoMap& collector_infos,
                                             const FinishCallbackMap& finish_callbacks,
                                             const std::vector<std::string>& default_callback_names,
                                             double default_freq)
    : timerScheduler_(timer_scheduler),
      collector_info_dict_(collector_infos),
      finishCallbacks_(finish_callbacks),
      default_cbs_name_(default_callback_names),
      default_freq_(default_freq) {}

void PeriodicSystemManager::registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle) {
    if (!collector_handle.periodic_system) {
        spdlog::error("CollectorScheduler: {} does not implement IPeriodicSystemCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    auto& info = collector_info_dict_[name];
    info.name = name;
    info.config_name = config;
    info.base = collector_handle.base;
    info.periodic_system = collector_handle.periodic_system;
    info.init_handle = collector_handle.init;
    info.deinit_handle = collector_handle.deinit;
    info.scope = CollectorScope::System;
    info.trigger = CollectorTrigger::Periodic;
    info.freq = resolveCollectorFreq(name, config, default_freq_, false);
    info.finish_cbs = resolveCollectorFinishCallbacks(name, config, finishCallbacks_, default_cbs_name_);

    if (resolveAutoStart(config)) {
        startCollector(name);
    }
}

bool PeriodicSystemManager::startCollector(const std::string& name) {
    auto& state = states_[name];
    if (state.running) {
        return true;
    }
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return false;
    auto& info = info_it->second;
    auto config_node = Config::instance().getRawNode(info.config_name);
    auto j_config = Utils::yamlToJson(config_node);
    if (!info.init_handle(j_config)) {
        spdlog::error("CollectorScheduler: system collector {} init failed", name);
        return false;
    }
    state.task_id = timerScheduler_.registerRepeatingTimer(
        std::chrono::milliseconds(static_cast<long long>(1000.0 / info.freq)),
        [this, name](){
            auto info_it = collector_info_dict_.find(name);
            if (info_it == collector_info_dict_.end() || !info_it->second.periodic_system) return;
            auto ret = info_it->second.periodic_system->collect();
            auto now = std::chrono::system_clock::now();
            auto sys_job = makeSystemCollectorJob();
            for(const auto& cb:info_it->second.finish_cbs){
                cb(name, sys_job, ret, now);
            }
        }
    );
    state.running = true;
    return true;
}

bool PeriodicSystemManager::stopCollector(const std::string& name) {
    auto state_it = states_.find(name);
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return false;
    if (state_it == states_.end()) return true;
    auto& state = state_it->second;
    std::lock_guard lg(state.m_);
    if (!state.running) return true;
    state.running = false;
    timerScheduler_.cancelTimer(state.task_id);
    info_it->second.deinit_handle();
    state.task_id = 0;
    return true;
}

nlohmann::json PeriodicSystemManager::getCollectorStatus(const std::string& name) const {
    nlohmann::json status;
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return status;
    status = makeCollectorStatusBase(info_it->second);
    status["running"] = false;
    status["task_id"] = 0;
    auto state_it = states_.find(name);
    if (state_it != states_.end()) {
        auto& state = state_it->second;
        std::lock_guard lg(state.m_);
        status["running"] = state.running.load();
        status["task_id"] = state.task_id;
    }
    return status;
}

nlohmann::json PeriodicSystemManager::listCollectors() const {
    nlohmann::json collectors = nlohmann::json::array();
    for (const auto& [name, state] : states_) {
        collectors.push_back(getCollectorStatus(name));
    }
    return collectors;
}

void PeriodicSystemManager::shutdown() {
    std::vector<std::string> names;
    for (const auto& [name, state] : states_) {
        names.push_back(name);
    }
    for (const auto& name : names) {
        stopCollector(name);
    }
}
