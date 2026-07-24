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
#include "core/event_collector_manager.hpp"

#include "common/config.hpp"
#include "common/utils.hpp"
#include "collector/icollector.h"
#include "core/collector_manager_utils.hpp"

#include <spdlog/spdlog.h>

EventJobManager::EventJobManager(TimerScheduler& timer_scheduler,
                                 CollectorInfoMap& collector_infos,
                                 const FinishCallbackMap& finish_callbacks,
                                 const std::vector<std::string>& default_callback_names)
    : timerScheduler_(timer_scheduler),
      collector_info_dict_(collector_infos),
      finishCallbacks_(finish_callbacks),
      default_cbs_name_(default_callback_names) {}

void EventJobManager::registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle) {
    if (!collector_handle.event_job) {
        spdlog::error("CollectorScheduler: {} does not implement IEventJobCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    auto& info = collector_info_dict_[name];
    info.name = name;
    info.config_name = config;
    info.base = collector_handle.base;
    info.event_job = collector_handle.event_job;
    info.init_handle = collector_handle.init;
    info.deinit_handle = collector_handle.deinit;
    info.scope = CollectorScope::Job;
    info.trigger = CollectorTrigger::EventBatch;
    info.finish_cbs = resolveCollectorFinishCallbacks(name, config, finishCallbacks_, default_cbs_name_);
    configureState(name, config);
    startCollector(name);
}

void EventJobManager::configureState(const std::string& name, const std::string& config) {
    auto batch_config = resolveEventBatchConfig(config);
    auto& state = states_[name];
    state.flush_threshold = batch_config.flush_threshold;
    state.max_batch_size = batch_config.max_batch_size;
    state.max_wait = batch_config.max_wait;
}

bool EventJobManager::startCollector(const std::string& name) {
    auto& state = states_[name];
    if (state.running) return true;
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end() || !info_it->second.event_job) return false;
    auto& info = info_it->second;
    auto config_node = Config::instance().getRawNode(info.config_name);
    auto j_config = Utils::yamlToJson(config_node);
    if (!info.init_handle(j_config)) {
        spdlog::error("CollectorScheduler: event job collector {} init failed", name);
        return false;
    }
    info.event_job->set_ready_notify([this, name](){
        auto state_it = states_.find(name);
        auto info_it = collector_info_dict_.find(name);
        if (state_it == states_.end() || info_it == collector_info_dict_.end() || !info_it->second.event_job) return;
        if (!state_it->second.running) return;
        if (info_it->second.event_job->pending_events() >= state_it->second.flush_threshold) {
            timerScheduler_.triggerTimer(state_it->second.flush_task_id);
        }
    });
    state.flush_task_id = timerScheduler_.registerRepeatingTimer(state.max_wait, [this, name](){
        flushCollector(name);
    });
    state.running = true;
    return true;
}

bool EventJobManager::stopCollector(const std::string& name) {
    auto state_it = states_.find(name);
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return false;
    if (state_it == states_.end()) return true;
    auto& state = state_it->second;
    std::lock_guard lg(state.m_);
    if (!state.running) {
        state.active_job_ids.clear();
        return true;
    }
    state.running = false;
    timerScheduler_.cancelTimer(state.flush_task_id);
    info_it->second.deinit_handle();
    state.active_job_ids.clear();
    state.flush_task_id = 0;
    return true;
}

void EventJobManager::addJob(const Job& job, const std::string& collector) {
    auto state_it = states_.find(collector);
    auto info_it = collector_info_dict_.find(collector);
    if (state_it == states_.end() || info_it == collector_info_dict_.end() || !info_it->second.event_job) return;
    {
        std::lock_guard lg(state_it->second.m_);
        state_it->second.active_job_ids.insert(job.JobID);
    }
    info_it->second.event_job->add_job(job);
}

void EventJobManager::removeJob(uint64_t jobid, const std::string& collector) {
    auto state_it = states_.find(collector);
    auto info_it = collector_info_dict_.find(collector);
    if (state_it == states_.end() || info_it == collector_info_dict_.end() || !info_it->second.event_job) return;
    {
        std::lock_guard lg(state_it->second.m_);
        state_it->second.active_job_ids.erase(jobid);
    }
    info_it->second.event_job->remove_job(jobid);
}

void EventJobManager::updateJob(const Job& job, const std::string& collector) {
    auto state_it = states_.find(collector);
    auto info_it = collector_info_dict_.find(collector);
    if (state_it == states_.end() || info_it == collector_info_dict_.end() || !info_it->second.event_job) return;
    {
        std::lock_guard lg(state_it->second.m_);
        if (!state_it->second.active_job_ids.count(job.JobID)) {
            state_it->second.active_job_ids.insert(job.JobID);
        }
    }
    info_it->second.event_job->update_job(job);
}

void EventJobManager::flushCollector(const std::string& collector) {
    auto state_it = states_.find(collector);
    auto info_it = collector_info_dict_.find(collector);
    if (state_it == states_.end() || info_it == collector_info_dict_.end() || !info_it->second.event_job) return;
    auto& state = state_it->second;
    if (!state.running) return;
    bool expected = false;
    if (!state.flushing.compare_exchange_strong(expected, true)) return;
    auto pending = info_it->second.event_job->pending_events();
    if (pending > 0 && state.running) {
        auto ret = info_it->second.event_job->drain_events(state.max_batch_size);
        auto now = std::chrono::system_clock::now();
        auto sys_job = makeSystemCollectorJob();
        for(const auto& cb:info_it->second.finish_cbs){
            cb(collector, sys_job, ret, now);
        }
    }
    state.flushing = false;
}

nlohmann::json EventJobManager::getCollectorStatus(const std::string& name) const {
    nlohmann::json status;
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return status;
    status = makeCollectorStatusBase(info_it->second);
    status["running"] = false;
    status["flushing"] = false;
    status["flush_task_id"] = 0;
    status["active_job_count"] = 0;
    status["active_job_ids"] = nlohmann::json::array();
    status["pending_events"] = 0;
    auto state_it = states_.find(name);
    if (state_it != states_.end()) {
        auto& state = state_it->second;
        std::lock_guard lg(state.m_);
        status["running"] = state.running.load();
        status["flushing"] = state.flushing.load();
        status["flush_task_id"] = state.flush_task_id;
        status["active_job_count"] = state.active_job_ids.size();
        status["flush_threshold"] = state.flush_threshold;
        status["max_batch_size"] = state.max_batch_size;
        status["max_wait_ms"] = state.max_wait.count();
        if (info_it->second.event_job) {
            status["pending_events"] = info_it->second.event_job->pending_events();
        }
        for (auto jobid : state.active_job_ids) {
            status["active_job_ids"].push_back(jobid);
        }
    }
    return status;
}

nlohmann::json EventJobManager::listCollectors() const {
    nlohmann::json collectors = nlohmann::json::array();
    for (const auto& [name, state] : states_) {
        collectors.push_back(getCollectorStatus(name));
    }
    return collectors;
}

void EventJobManager::shutdown() {
    std::vector<std::string> names;
    for (const auto& [name, state] : states_) {
        names.push_back(name);
    }
    for (const auto& name : names) {
        stopCollector(name);
    }
}

EventSystemManager::EventSystemManager(TimerScheduler& timer_scheduler,
                                       CollectorInfoMap& collector_infos,
                                       const FinishCallbackMap& finish_callbacks,
                                       const std::vector<std::string>& default_callback_names)
    : timerScheduler_(timer_scheduler),
      collector_info_dict_(collector_infos),
      finishCallbacks_(finish_callbacks),
      default_cbs_name_(default_callback_names) {}

void EventSystemManager::registerCollector(const std::string& name, const std::string& config, const CollectorHandle& collector_handle) {
    if (!collector_handle.event_system) {
        spdlog::error("CollectorScheduler: {} does not implement IEventSystemCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    auto& info = collector_info_dict_[name];
    info.name = name;
    info.config_name = config;
    info.base = collector_handle.base;
    info.event_system = collector_handle.event_system;
    info.init_handle = collector_handle.init;
    info.deinit_handle = collector_handle.deinit;
    info.scope = CollectorScope::System;
    info.trigger = CollectorTrigger::EventBatch;
    info.finish_cbs = resolveCollectorFinishCallbacks(name, config, finishCallbacks_, default_cbs_name_);
    configureState(name, config);
    startCollector(name);
}

void EventSystemManager::configureState(const std::string& name, const std::string& config) {
    auto batch_config = resolveEventBatchConfig(config);
    auto& state = states_[name];
    state.flush_threshold = batch_config.flush_threshold;
    state.max_batch_size = batch_config.max_batch_size;
    state.max_wait = batch_config.max_wait;
}

bool EventSystemManager::startCollector(const std::string& name) {
    auto& state = states_[name];
    if (state.running) return true;
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end() || !info_it->second.event_system) return false;
    auto& info = info_it->second;
    auto config_node = Config::instance().getRawNode(info.config_name);
    auto j_config = Utils::yamlToJson(config_node);
    if (!info.init_handle(j_config)) {
        spdlog::error("CollectorScheduler: event system collector {} init failed", name);
        return false;
    }
    info.event_system->set_ready_notify([this, name](){
        auto state_it = states_.find(name);
        auto info_it = collector_info_dict_.find(name);
        if (state_it == states_.end() || info_it == collector_info_dict_.end() || !info_it->second.event_system) return;
        if (!state_it->second.running) return;
        if (info_it->second.event_system->pending_events() >= state_it->second.flush_threshold) {
            timerScheduler_.triggerTimer(state_it->second.flush_task_id);
        }
    });
    state.flush_task_id = timerScheduler_.registerRepeatingTimer(state.max_wait, [this, name](){
        flushCollector(name);
    });
    state.running = true;
    return true;
}

bool EventSystemManager::stopCollector(const std::string& name) {
    auto state_it = states_.find(name);
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return false;
    if (state_it == states_.end()) return true;
    auto& state = state_it->second;
    std::lock_guard lg(state.m_);
    if (!state.running) return true;
    state.running = false;
    timerScheduler_.cancelTimer(state.flush_task_id);
    info_it->second.deinit_handle();
    state.flush_task_id = 0;
    return true;
}

void EventSystemManager::flushCollector(const std::string& collector) {
    auto state_it = states_.find(collector);
    auto info_it = collector_info_dict_.find(collector);
    if (state_it == states_.end() || info_it == collector_info_dict_.end() || !info_it->second.event_system) return;
    auto& state = state_it->second;
    if (!state.running) return;
    bool expected = false;
    if (!state.flushing.compare_exchange_strong(expected, true)) return;
    auto pending = info_it->second.event_system->pending_events();
    if (pending > 0 && state.running) {
        auto ret = info_it->second.event_system->drain_events(state.max_batch_size);
        auto now = std::chrono::system_clock::now();
        auto sys_job = makeSystemCollectorJob();
        for(const auto& cb:info_it->second.finish_cbs){
            cb(collector, sys_job, ret, now);
        }
    }
    state.flushing = false;
}

nlohmann::json EventSystemManager::getCollectorStatus(const std::string& name) const {
    nlohmann::json status;
    auto info_it = collector_info_dict_.find(name);
    if (info_it == collector_info_dict_.end()) return status;
    status = makeCollectorStatusBase(info_it->second);
    status["running"] = false;
    status["flushing"] = false;
    status["flush_task_id"] = 0;
    status["pending_events"] = 0;
    auto state_it = states_.find(name);
    if (state_it != states_.end()) {
        auto& state = state_it->second;
        std::lock_guard lg(state.m_);
        status["running"] = state.running.load();
        status["flushing"] = state.flushing.load();
        status["flush_task_id"] = state.flush_task_id;
        status["flush_threshold"] = state.flush_threshold;
        status["max_batch_size"] = state.max_batch_size;
        status["max_wait_ms"] = state.max_wait.count();
        if (info_it->second.event_system) {
            status["pending_events"] = info_it->second.event_system->pending_events();
        }
    }
    return status;
}

nlohmann::json EventSystemManager::listCollectors() const {
    nlohmann::json collectors = nlohmann::json::array();
    for (const auto& [name, state] : states_) {
        collectors.push_back(getCollectorStatus(name));
    }
    return collectors;
}

void EventSystemManager::shutdown() {
    std::vector<std::string> names;
    for (const auto& [name, state] : states_) {
        names.push_back(name);
    }
    for (const auto& name : names) {
        stopCollector(name);
    }
}
