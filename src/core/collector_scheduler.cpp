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
#include "core/collector_scheduler.hpp"
#include "core/collector_registry.hpp"
#include "core/writer_manager.hpp"
#include "core/job_registry.hpp"
#include "common/config.hpp"
#include "common/utils.hpp"

#include <algorithm>
#include <set>

CollectorScheduler::CollectorScheduler()
    : timerScheduler_(Config::instance().getInt("lens_config", "max_collector_threads"))
{
    default_freq = Config::instance().getInt("collectors_config", "default_freq");
    default_cbs_name = Config::instance().getArray<std::string>("collectors_config", "default_use_writers");
    registerFinishCallbacks();

    registerCollectFuncs();

    JobRegistry::instance().addLifecycleCb(
        [this](JobEvent e, const Job& job){
            switch (e) {
                case JobEvent::Added: {
                    onJobAdded(job);
                    spdlog::info("CollectorScheduler: job {} added, {} PIDs", job.JobID, job.JobPIDs.size());
                    break;
                }
                case JobEvent::Removed: {
                    onJobRemoved(job);
                    spdlog::info("CollectorScheduler: job {} removed", job.JobID);
                    break;
                }
                case JobEvent::Updated: {
                    onJobUpdated(job);
                    spdlog::info("CollectorScheduler: job {} updated", job.JobID);
                    break;
                }
                default: {
                    spdlog::warn("CollectorScheduler: error job event");
                }
            }
        }
    );

    spdlog::info("CollectorScheduler: initialized with {} collector(s) and {} writer(s)",
                collector_info_dict.size(), finishCallbacks_.size());
}

CollectorScheduler::~CollectorScheduler() { shutdown(); }




void CollectorScheduler::startPeriodicJobCollector(std::string collector_name){
    std::function<std::any(Job&)> func_handle;
    collector_info info; 
    std::string config_name;
    try {
        info = collector_info_dict.at(collector_name);
    }
    catch(const std::exception& e) {
        spdlog::error("CollectorScheduler: start collector error, can not find {}!",collector_name);
        return;
    }
    try
    {
        auto config_node = Config::instance().getRawNode(info.config_name);
        auto j_config = Utils::yamlToJson(config_node);
        if (!info.init_handle(j_config)) {
            spdlog::error("CollectorScheduler: collector {} init failed", collector_name);
            return;
        }
        auto freq = info.freq;

        auto& collector_job = periodic_job_state_dict[collector_name];

        collector_job.task_id = timerScheduler_.registerRepeatingTimer(
            std::chrono::milliseconds(static_cast<long long>(1000.0 / freq)),
            [this, collector_name](){
                // 使用find代替operator[]，避免与持有m_的路径形成数据竞争
                auto it_info = collector_info_dict.find(collector_name);
                if (it_info == collector_info_dict.end()) return;
                auto& info = it_info->second;

                    auto it_state = periodic_job_state_dict.find(collector_name);
                    if (it_state == periodic_job_state_dict.end()) return;
                auto& collector_job = it_state->second;                
                std::vector<size_t> jobids_snapshot;
                {
                    std::lock_guard lg(collector_job.m_);
                    if(collector_job.jobid_list.empty() && collector_job.running){
                        //由JobRegistry完成任务清理工作
                        //没有任务，取消这个收集器，节省资源
                        info.deinit_handle();
                        timerScheduler_.cancelTimer(collector_job.task_id);
                        collector_job.running = false;
                        return;
                    }
                    jobids_snapshot = collector_job.jobid_list;
                }

                for(auto jobid: jobids_snapshot){
                    //TODO:当压力过高时，这里应该改为非阻塞执行
                    auto job = JobRegistry::instance().findJob(jobid);
                    std::lock_guard lg(collector_job.m_);
                    if(!collector_job.running) return; //采集器已经deinit
                    if(!job.has_value())continue;
                    {
                        std::any ret;
                        try
                        {
                            ret = info.periodic_job->collect(job.value());
                            auto now = std::chrono::system_clock::now();
                            for(const auto& cb:info.finish_cbs){
                                cb(collector_name, job.value(), ret, now);
                            }
                        }
                        catch(const std::exception& e)
                        {
                            spdlog::error("CollectorScheduler: collector {} collect error: {}", collector_name, e.what());
                        }
                    }
                    
                }
                return;
                
            }
        );
        collector_job.running = true;
    }
    catch(const std::exception& e) {
        spdlog::error("CollectorScheduler: start collector {} error: {}", collector_name, e.what());
    }


    spdlog::info("CollectorScheduler: start collector {}", collector_name);

}

void CollectorScheduler::addJob2PeriodicCollector(size_t jobid, std::string collector){
    auto& state = periodic_job_state_dict[collector];
    std::lock_guard lg(state.m_);
    /* 去重: 避免同一JobID被重复push导致collect(Job) dedup命中返回空快照 */
    if (std::find(state.jobid_list.begin(), state.jobid_list.end(), jobid) != state.jobid_list.end()) {
        return;
    }
    state.jobid_list.push_back(jobid);
    if(!state.running){
        startPeriodicJobCollector(collector);
    }
}

void CollectorScheduler::onJobAdded(const Job& job){
    std::lock_guard lg(m_);
    for(auto& collector_name:job.CollectorNames){
        if (collector_info_dict.count(collector_name) == 0)
        {
            spdlog::error("CollectorScheduler: {} name is error, continue..", collector_name);
            continue;
        }
        auto& info = collector_info_dict.at(collector_name);
        if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::EventBatch) {
            addJob2EventCollector(job, collector_name);
            continue;
        }
        if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::Periodic) {
            addJob2PeriodicCollector(job.JobID, collector_name);
        }
    }
}

void CollectorScheduler::onJobRemoved(const Job& job){
    for(auto collector_name:job.CollectorNames){
        auto info_it = collector_info_dict.find(collector_name);
        if (info_it == collector_info_dict.end()) {
            continue;
        }
        auto& info = info_it->second;
        if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::Periodic) {
            removeJobFromPeriodicCollector(job.JobID, collector_name);
        } else if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::EventBatch) {
            removeJobFromEventCollector(job.JobID, collector_name);
        }
    }
}

void CollectorScheduler::onJobUpdated(const Job& job){
    std::lock_guard lg(m_);
    
    const std::set<std::string> needed{job.CollectorNames.begin(),
                                                   job.CollectorNames.end()};

    std::vector<std::string> to_remove;
    to_remove.reserve(periodic_job_state_dict.size());
    for (const auto& [name, state] : periodic_job_state_dict) {
        if (!needed.count(name)) {
            to_remove.push_back(name);
        }
    }

    for(auto& collector_name:job.CollectorNames){
        if (collector_info_dict.count(collector_name) == 0)
        {
            spdlog::error("CollectorScheduler: {} name is error, continue..", collector_name);
            continue;
        }
        auto& info = collector_info_dict.at(collector_name);
        if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::Periodic) {
            auto& state = periodic_job_state_dict[collector_name];
            auto it = std::find(state.jobid_list.begin(), state.jobid_list.end(), job.JobID);
            if (it == state.jobid_list.end()){
                addJob2PeriodicCollector(job.JobID, collector_name);
            }
        } else if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::EventBatch) {
            updateJobForEventCollector(job, collector_name);
        }
    }
    for(auto& collector_name:to_remove){
        removeJobFromPeriodicCollector(job.JobID, collector_name);
    }
}

void CollectorScheduler::removeJobFromPeriodicCollector(uint64_t jobid, std::string collector){
    auto it = periodic_job_state_dict.find(collector);
    if (it == periodic_job_state_dict.end()) return;
    auto& state = it->second;
    std::lock_guard lg(state.m_);
    state.jobid_list.erase(std::remove_if(state.jobid_list.begin(),state.jobid_list.end(),[jobid](size_t x){return x==jobid;}));
}

void CollectorScheduler::addJob2EventCollector(const Job& job, std::string collector){
    auto state_it = event_job_state_dict.find(collector);
    auto info_it = collector_info_dict.find(collector);
    if (state_it == event_job_state_dict.end() || info_it == collector_info_dict.end() || !info_it->second.event_job) return;
    {
        std::lock_guard lg(state_it->second.m_);
        state_it->second.active_job_ids.insert(job.JobID);
    }
    info_it->second.event_job->add_job(job);
}

void CollectorScheduler::removeJobFromEventCollector(uint64_t jobid, std::string collector){
    auto state_it = event_job_state_dict.find(collector);
    auto info_it = collector_info_dict.find(collector);
    if (state_it == event_job_state_dict.end() || info_it == collector_info_dict.end() || !info_it->second.event_job) return;
    {
        std::lock_guard lg(state_it->second.m_);
        state_it->second.active_job_ids.erase(jobid);
    }
    info_it->second.event_job->remove_job(jobid);
}

void CollectorScheduler::updateJobForEventCollector(const Job& job, std::string collector){
    auto state_it = event_job_state_dict.find(collector);
    auto info_it = collector_info_dict.find(collector);
    if (state_it == event_job_state_dict.end() || info_it == collector_info_dict.end() || !info_it->second.event_job) return;
    {
        std::lock_guard lg(state_it->second.m_);
        if (!state_it->second.active_job_ids.count(job.JobID)) {
            state_it->second.active_job_ids.insert(job.JobID);
        }
    }
    info_it->second.event_job->update_job(job);
}

void CollectorScheduler::flushEventJobCollector(std::string collector){
    auto state_it = event_job_state_dict.find(collector);
    auto info_it = collector_info_dict.find(collector);
    if (state_it == event_job_state_dict.end() || info_it == collector_info_dict.end() || !info_it->second.event_job) return;
    auto& state = state_it->second;
    bool expected = false;
    if (!state.flushing.compare_exchange_strong(expected, true)) return;
    auto pending = info_it->second.event_job->pending_events();
    if (pending > 0) {
        auto ret = info_it->second.event_job->drain_events(state.max_batch_size);
        auto now = std::chrono::system_clock::now();
        Job sys_job;
        sys_job.JobID = 0;
        sys_job.jobtype = JobType::Sys;
        sys_job.subtype = JobSubType::Common;
        for(const auto& cb:info_it->second.finish_cbs){
            cb(collector, sys_job, ret, now);
        }
    }
    state.flushing = false;
}

void CollectorScheduler::flushEventSystemCollector(std::string collector){
    auto state_it = event_system_state_dict.find(collector);
    auto info_it = collector_info_dict.find(collector);
    if (state_it == event_system_state_dict.end() || info_it == collector_info_dict.end() || !info_it->second.event_system) return;
    auto& state = state_it->second;
    bool expected = false;
    if (!state.flushing.compare_exchange_strong(expected, true)) return;
    auto pending = info_it->second.event_system->pending_events();
    if (pending > 0) {
        auto ret = info_it->second.event_system->drain_events(state.max_batch_size);
        auto now = std::chrono::system_clock::now();
        Job sys_job;
        sys_job.JobID = 0;
        sys_job.jobtype = JobType::Sys;
        sys_job.subtype = JobSubType::Common;
        for(const auto& cb:info_it->second.finish_cbs){
            cb(collector, sys_job, ret, now);
        }
    }
    state.flushing = false;
}

void CollectorScheduler::addPeriodicJobCollector(std::string name, std::string config, const CollectorHandle& collector_handle) {
    if (!collector_handle.periodic_job) {
        spdlog::error("CollectorScheduler: {} does not implement IPeriodicJobCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    collector_info_dict[name].name = name;
    collector_info_dict[name].config_name = config;
    collector_info_dict[name].base = collector_handle.base;
    collector_info_dict[name].periodic_job = collector_handle.periodic_job;
    collector_info_dict[name].init_handle = collector_handle.init;
    collector_info_dict[name].deinit_handle = collector_handle.deinit;
    collector_info_dict[name].scope = CollectorScope::Job;
    collector_info_dict[name].trigger = CollectorTrigger::Periodic;

    if (Config::instance().has(config, "freq")) {
        auto freq = Config::instance().getDouble(config, "freq");
        collector_info_dict[name].freq = freq;
    } else if (Config::instance().has(config, "period")) {
        auto period = Config::instance().getDouble(config, "period");
        auto freq = 1.0 / period;
        collector_info_dict[name].freq = freq;
    } else {
        spdlog::info("CollectorScheduler: can not get collect {} freq, use default freq", name);
        collector_info_dict[name].freq = default_freq;
    }


    try{
        auto writers = Config::instance().getArray<std::string>(config, "use_writers");
        for (const auto& writer_name: writers){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} writers, use default writers", name);
        for (const auto& writer_name: default_cbs_name){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }
}   

void CollectorScheduler::addPeriodicSystemCollector(std::string name, std::string config, const CollectorHandle& collector_handle){
    if (!collector_handle.periodic_system) {
        spdlog::error("CollectorScheduler: {} does not implement IPeriodicSystemCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    collector_info_dict[name].name = name;
    collector_info_dict[name].config_name = config;
    collector_info_dict[name].base = collector_handle.base;
    collector_info_dict[name].periodic_system = collector_handle.periodic_system;
    collector_info_dict[name].init_handle = collector_handle.init;
    collector_info_dict[name].deinit_handle = collector_handle.deinit;
    collector_info_dict[name].scope = CollectorScope::System;
    collector_info_dict[name].trigger = CollectorTrigger::Periodic;

    try{
        auto freq = Config::instance().getDouble(config, "freq");
        collector_info_dict[name].freq = freq;
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} freq, use default freq", name);
        collector_info_dict[name].freq = default_freq;
    }

    try{
        auto writers = Config::instance().getArray<std::string>(config, "use_writers");
        for (const auto& writer_name: writers){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} writers, use default writers", name);
        for (const auto& writer_name: default_cbs_name){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }

    try{
        auto auto_start = Config::instance().getBool(config, "auto_start");
        if(auto_start){
            auto& state = periodic_system_state_dict[name];
            if (!state.running) {
                auto config_node = Config::instance().getRawNode(config);
                auto j_config = Utils::yamlToJson(config_node);
                if (!collector_info_dict[name].init_handle(j_config)) {
                    spdlog::error("CollectorScheduler: system collector {} init failed", name);
                    return;
                }
                state.task_id = timerScheduler_.registerRepeatingTimer(
                    std::chrono::milliseconds(static_cast<long long>(1000.0 / collector_info_dict[name].freq)),
                    [this, name](){
                        auto info_it = collector_info_dict.find(name);
                        if (info_it == collector_info_dict.end() || !info_it->second.periodic_system) return;
                        auto ret = info_it->second.periodic_system->collect();
                        auto now = std::chrono::system_clock::now();
                        Job sys_job;
                        sys_job.JobID = 0;
                        sys_job.jobtype = JobType::Sys;
                        sys_job.subtype = JobSubType::Common;
                        for(const auto& cb:info_it->second.finish_cbs){
                            cb(name, sys_job, ret, now);
                        }
                    }
                );
                state.running = true;
            }
        }
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} auto_start, do not start it", name);
    }
}

void CollectorScheduler::addJobEventCollectFunc(std::string name, std::string config, const CollectorHandle& collector_handle){
    if (!collector_handle.event_job) {
        spdlog::error("CollectorScheduler: {} does not implement IEventJobCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    collector_info_dict[name].name = name;
    collector_info_dict[name].config_name = config;
    collector_info_dict[name].base = collector_handle.base;
    collector_info_dict[name].event_job = collector_handle.event_job;
    collector_info_dict[name].init_handle = collector_handle.init;
    collector_info_dict[name].deinit_handle = collector_handle.deinit;
    collector_info_dict[name].scope = CollectorScope::Job;
    collector_info_dict[name].trigger = CollectorTrigger::EventBatch;

    try{
        auto writers = Config::instance().getArray<std::string>(config, "use_writers");
        for (const auto& writer_name: writers){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} writers, use default writers", name);
        for (const auto& writer_name: default_cbs_name){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }
    auto config_node = Config::instance().getRawNode(config);
    auto j_config = Utils::yamlToJson(config_node);
    if (!collector_info_dict[name].init_handle(j_config)) {
        spdlog::error("CollectorScheduler: event job collector {} init failed", name);
        return;
    }
    auto& state = event_job_state_dict[name];
    if (Config::instance().has(config, "flush_threshold")) {
        state.flush_threshold = Config::instance().getInt(config, "flush_threshold");
    }
    if (Config::instance().has(config, "max_batch_size")) {
        state.max_batch_size = Config::instance().getInt(config, "max_batch_size");
    }
    if (Config::instance().has(config, "max_wait_ms")) {
        state.max_wait = std::chrono::milliseconds(Config::instance().getInt(config, "max_wait_ms"));
    }
    collector_info_dict[name].event_job->set_ready_notify([this, name](){
        auto state_it = event_job_state_dict.find(name);
        auto info_it = collector_info_dict.find(name);
        if (state_it == event_job_state_dict.end() || info_it == collector_info_dict.end() || !info_it->second.event_job) return;
        if (info_it->second.event_job->pending_events() >= state_it->second.flush_threshold) {
            timerScheduler_.triggerTimer(state_it->second.flush_task_id);
        }
    });
    state.flush_task_id = timerScheduler_.registerRepeatingTimer(state.max_wait, [this, name](){
        flushEventJobCollector(name);
    });
    state.running = true;
}

void CollectorScheduler::addSystemEventCollectFunc(std::string name, std::string config, const CollectorHandle& collector_handle){
    if (!collector_handle.event_system) {
        spdlog::error("CollectorScheduler: {} does not implement IEventSystemCollector", name);
        return;
    }
    std::lock_guard lg(m_);
    collector_info_dict[name].name = name;
    collector_info_dict[name].config_name = config;
    collector_info_dict[name].base = collector_handle.base;
    collector_info_dict[name].event_system = collector_handle.event_system;
    collector_info_dict[name].init_handle = collector_handle.init;
    collector_info_dict[name].deinit_handle = collector_handle.deinit;
    collector_info_dict[name].scope = CollectorScope::System;
    collector_info_dict[name].trigger = CollectorTrigger::EventBatch;

    try{
        auto writers = Config::instance().getArray<std::string>(config, "use_writers");
        for (const auto& writer_name: writers){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} writers, use default writers", name);
        for (const auto& writer_name: default_cbs_name){
            if(!finishCallbacks_.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            auto cb = finishCallbacks_.at(writer_name);
            collector_info_dict[name].finish_cbs.push_back(cb);
        }
    }
    auto config_node = Config::instance().getRawNode(config);
    auto j_config = Utils::yamlToJson(config_node);
    if (!collector_info_dict[name].init_handle(j_config)) {
        spdlog::error("CollectorScheduler: event system collector {} init failed", name);
        return;
    }
    auto& state = event_system_state_dict[name];
    if (Config::instance().has(config, "flush_threshold")) {
        state.flush_threshold = Config::instance().getInt(config, "flush_threshold");
    }
    if (Config::instance().has(config, "max_batch_size")) {
        state.max_batch_size = Config::instance().getInt(config, "max_batch_size");
    }
    if (Config::instance().has(config, "max_wait_ms")) {
        state.max_wait = std::chrono::milliseconds(Config::instance().getInt(config, "max_wait_ms"));
    }
    collector_info_dict[name].event_system->set_ready_notify([this, name](){
        auto state_it = event_system_state_dict.find(name);
        auto info_it = collector_info_dict.find(name);
        if (state_it == event_system_state_dict.end() || info_it == collector_info_dict.end() || !info_it->second.event_system) return;
        if (info_it->second.event_system->pending_events() >= state_it->second.flush_threshold) {
            timerScheduler_.triggerTimer(state_it->second.flush_task_id);
        }
    });
    state.flush_task_id = timerScheduler_.registerRepeatingTimer(state.max_wait, [this, name](){
        flushEventSystemCollector(name);
    });
    state.running = true;
}


void CollectorScheduler::addCallback(std::string name, OnFinish cb) {
    std::lock_guard lg(m_);
    auto ret = finishCallbacks_.count(name);
    if(ret){
        spdlog::error("CollectorScheduler: addCallback error, because this writer has been added");
        return;
    }
    finishCallbacks_[name] = cb;
}

void CollectorScheduler::onJobLifecycle(JobEvent ev, Job& job) {
    switch (ev) {
        case JobEvent::Added: {
            spdlog::info("CollectorScheduler: job {} added, {} PIDs", job.JobID, job.JobPIDs.size());
            onJobAdded(job);
            break;
        }
        case JobEvent::Removed: {
            spdlog::info("CollectorScheduler: job {} removed", job.JobID);
            onJobRemoved(job);
            break;
        }
        case JobEvent::Updated: {
            spdlog::info("CollectorScheduler: job {} updated", job.JobID);
            onJobUpdated(job);
            break;
        }
        default: {
            spdlog::warn("CollectorScheduler: error job event");
        }
    }
}

void CollectorScheduler::start() {
    std::lock_guard lg(m_);
    if (running_) return;
    running_ = true;
}

void CollectorScheduler::shutdown() {
    {
        std::lock_guard lg(m_);
        if (!running_) return;
        running_ = false;
    }
    timerScheduler_.shutdown();
    spdlog::info("CollectorScheduler: CollectorScheduler shutdown complete");
}

nlohmann::json CollectorScheduler::snapshot() {
    nlohmann::json result;
    std::lock_guard lg(m_);
    return result;
}

CollectorScheduler& CollectorScheduler::instance() {
    static CollectorScheduler instance;
    return instance;
}


void CollectorScheduler::registerCollectFuncs() {
    global_config = Config::instance();
    struct Collector {
        std::string name;
        std::string type;
        std::string config;
    };
    
    auto collectors = global_config.getArray<Collector>("collectors_config", "collectors",
        [](const YAML::Node& node) {
            Collector c;
            c.name = node["name"].as<std::string>();
            c.type = node["type"].as<std::string>();
            c.config = node["config"].as<std::string>();
            return c;
        });
    auto& collector_reg = CollectorRegistry::instance();
    
    for (const auto& collector : collectors) {
        
        auto collector_handle = collector_reg.createCollector(collector.type, collector.name);
        if (!collector_handle.init) {
            spdlog::error("CollectorScheduler: {} init error",collector.name);
        }
        auto scope = collector_reg.getScope(collector.type);
        auto trigger = collector_reg.getTrigger(collector.type);
        if(scope == CollectorScope::Undefined){
            spdlog::error("CollectorScheduler: {} scope is undefined, skip it",collector.name);
            continue;
        }
        if(trigger == CollectorTrigger::Undefined){
            spdlog::error("CollectorScheduler: {} trigger is undefined, skip it",collector.name);
            continue;
        }
        if(scope == CollectorScope::Job && trigger == CollectorTrigger::Periodic){
            addPeriodicJobCollector(
                collector.name,
                collector.config,
                collector_handle
            );
        }// 虽然这种实现不好看，但是可以隔离Job和System的逻辑
        else if(scope == CollectorScope::System && trigger == CollectorTrigger::Periodic){
            addPeriodicSystemCollector(
                collector.name,
                collector.config,
                collector_handle
            );
        }
        else if(scope == CollectorScope::Job && trigger == CollectorTrigger::EventBatch){
            addJobEventCollectFunc(
                collector.name,
                collector.config,
                collector_handle
            );
        }
        else if(scope == CollectorScope::System && trigger == CollectorTrigger::EventBatch){
            addSystemEventCollectFunc(
                collector.name,
                collector.config,
                collector_handle
            );
        }
        else{
            spdlog::error("CollectorScheduler: {} scope or trigger is error, skip it",collector.name);
            continue;
        }
        
    }
}

void CollectorScheduler::registerFinishCallbacks() {
    auto callbacks = WriterManager::instance().get_onFinishCallbacks();
    
    for (const auto& cb_func : callbacks) {
        auto name = std::get<0>(cb_func);
        auto func = std::get<1>(cb_func);
        addCallback(name, func);
    }
}
