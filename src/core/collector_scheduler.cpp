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
#include "common/local_rpc.hpp"

#include <set>

CollectorScheduler::CollectorScheduler()
    : timerScheduler_(Config::instance().getInt("lens_config", "max_collector_threads"))
    , default_freq(Config::instance().getInt("collectors_config", "default_freq"))
    , default_cbs_name(Config::instance().getArray<std::string>("collectors_config", "default_use_writers"))
    , periodic_job_manager_(timerScheduler_, collector_info_dict, finishCallbacks_, default_cbs_name, default_freq)
    , periodic_system_manager_(timerScheduler_, collector_info_dict, finishCallbacks_, default_cbs_name, default_freq)
    , event_job_manager_(timerScheduler_, collector_info_dict, finishCallbacks_, default_cbs_name)
    , event_system_manager_(timerScheduler_, collector_info_dict, finishCallbacks_, default_cbs_name)
{
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
    registerRPCMethods();
}

CollectorScheduler::~CollectorScheduler() { shutdown(); }

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
            event_job_manager_.addJob(job, collector_name);
            continue;
        }
        if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::Periodic) {
            periodic_job_manager_.addJob(job.JobID, collector_name);
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
            periodic_job_manager_.removeJob(job.JobID, collector_name);
        } else if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::EventBatch) {
            event_job_manager_.removeJob(job.JobID, collector_name);
        }
    }
}

void CollectorScheduler::onJobUpdated(const Job& job){
    std::lock_guard lg(m_);
    
    const std::set<std::string> needed{job.CollectorNames.begin(),
                                                   job.CollectorNames.end()};

    std::vector<std::string> to_remove;
    to_remove.reserve(collector_info_dict.size());
    for (const auto& [name, info] : collector_info_dict) {
        if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::Periodic && !needed.count(name)) {
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
            periodic_job_manager_.addJob(job.JobID, collector_name);
        } else if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::EventBatch) {
            event_job_manager_.updateJob(job, collector_name);
        }
    }
    for(auto& collector_name:to_remove){
        periodic_job_manager_.removeJob(job.JobID, collector_name);
    }
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
    periodic_job_manager_.shutdown();
    periodic_system_manager_.shutdown();
    event_job_manager_.shutdown();
    event_system_manager_.shutdown();
    timerScheduler_.shutdown();
    spdlog::info("CollectorScheduler: CollectorScheduler shutdown complete");
}

nlohmann::json CollectorScheduler::snapshot() {
    nlohmann::json result;
    std::vector<std::string> names;
    {
        std::lock_guard lg(m_);
        names.reserve(collector_info_dict.size());
        for (const auto& [name, info] : collector_info_dict) {
            names.push_back(name);
        }
    }
    result["status"] = "ok";
    result["collectors"] = nlohmann::json::array();
    for (const auto& name : names) {
        result["collectors"].push_back(getCollector(name));
    }
    result["total"] = result["collectors"].size();
    return result;
}

nlohmann::json CollectorScheduler::getCollector(const std::string& name) {
    nlohmann::json result;
    std::lock_guard lg(m_);
    auto info_it = collector_info_dict.find(name);
    if (info_it == collector_info_dict.end()) {
        result["status"] = "error";
        result["msg"] = "collector not found: " + name;
        return result;
    }
    const auto& info = info_it->second;
    if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::Periodic) {
        result = periodic_job_manager_.getCollectorStatus(name);
    } else if (info.scope == CollectorScope::System && info.trigger == CollectorTrigger::Periodic) {
        result = periodic_system_manager_.getCollectorStatus(name);
    } else if (info.scope == CollectorScope::Job && info.trigger == CollectorTrigger::EventBatch) {
        result = event_job_manager_.getCollectorStatus(name);
    } else if (info.scope == CollectorScope::System && info.trigger == CollectorTrigger::EventBatch) {
        result = event_system_manager_.getCollectorStatus(name);
    } else {
        result["status"] = "error";
        result["msg"] = "collector has unsupported scope or trigger: " + name;
        return result;
    }
    result["status"] = "ok";
    return result;
}

nlohmann::json CollectorScheduler::startCollector(const std::string& name) {
    nlohmann::json result;
    CollectorScope scope;
    CollectorTrigger trigger;
    {
        std::lock_guard lg(m_);
        auto info_it = collector_info_dict.find(name);
        if (info_it == collector_info_dict.end()) {
            result["status"] = "error";
            result["msg"] = "collector not found: " + name;
            return result;
        }
        scope = info_it->second.scope;
        trigger = info_it->second.trigger;
    }
    bool ok = false;
    if (scope == CollectorScope::Job && trigger == CollectorTrigger::Periodic) {
        ok = periodic_job_manager_.startCollector(name);
    } else if (scope == CollectorScope::System && trigger == CollectorTrigger::Periodic) {
        ok = periodic_system_manager_.startCollector(name);
    } else if (scope == CollectorScope::Job && trigger == CollectorTrigger::EventBatch) {
        ok = event_job_manager_.startCollector(name);
    } else if (scope == CollectorScope::System && trigger == CollectorTrigger::EventBatch) {
        ok = event_system_manager_.startCollector(name);
    }
    result = getCollector(name);
    result["status"] = ok ? "ok" : "error";
    if (!ok) result["msg"] = "failed to start collector: " + name;
    return result;
}

nlohmann::json CollectorScheduler::stopCollector(const std::string& name) {
    nlohmann::json result;
    CollectorScope scope;
    CollectorTrigger trigger;
    {
        std::lock_guard lg(m_);
        auto info_it = collector_info_dict.find(name);
        if (info_it == collector_info_dict.end()) {
            result["status"] = "error";
            result["msg"] = "collector not found: " + name;
            return result;
        }
        scope = info_it->second.scope;
        trigger = info_it->second.trigger;
    }
    bool ok = false;
    if (scope == CollectorScope::Job && trigger == CollectorTrigger::Periodic) {
        ok = periodic_job_manager_.stopCollector(name);
    } else if (scope == CollectorScope::System && trigger == CollectorTrigger::Periodic) {
        ok = periodic_system_manager_.stopCollector(name);
    } else if (scope == CollectorScope::Job && trigger == CollectorTrigger::EventBatch) {
        ok = event_job_manager_.stopCollector(name);
    } else if (scope == CollectorScope::System && trigger == CollectorTrigger::EventBatch) {
        ok = event_system_manager_.stopCollector(name);
    }
    result = getCollector(name);
    result["status"] = ok ? "ok" : "error";
    if (!ok) result["msg"] = "failed to stop collector: " + name;
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
            periodic_job_manager_.registerCollector(
                collector.name,
                collector.config,
                collector_handle
            );
        }
        else if(scope == CollectorScope::System && trigger == CollectorTrigger::Periodic){
            periodic_system_manager_.registerCollector(
                collector.name,
                collector.config,
                collector_handle
            );
        }
        else if(scope == CollectorScope::Job && trigger == CollectorTrigger::EventBatch){
            event_job_manager_.registerCollector(
                collector.name,
                collector.config,
                collector_handle
            );
        }
        else if(scope == CollectorScope::System && trigger == CollectorTrigger::EventBatch){
            event_system_manager_.registerCollector(
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

void CollectorScheduler::registerRPCMethods() {
    RPCServer::instance().register_method("CollectorScheduler/list_collectors",
        [this](const nlohmann::json& req) -> nlohmann::json {
            return snapshot();
        }
    );
    RPCServer::instance().register_method("CollectorScheduler/get_collector",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json response;
            if (!req.contains("name") || !req["name"].is_string()) {
                response["status"] = "error";
                response["msg"] = "Missing or invalid 'name' parameter";
                return response;
            }
            return getCollector(req["name"].get<std::string>());
        }
    );
    RPCServer::instance().register_method("CollectorScheduler/start_collector",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json response;
            if (!req.contains("name") || !req["name"].is_string()) {
                response["status"] = "error";
                response["msg"] = "Missing or invalid 'name' parameter";
                return response;
            }
            return startCollector(req["name"].get<std::string>());
        }
    );
    RPCServer::instance().register_method("CollectorScheduler/stop_collector",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json response;
            if (!req.contains("name") || !req["name"].is_string()) {
                response["status"] = "error";
                response["msg"] = "Missing or invalid 'name' parameter";
                return response;
            }
            return stopCollector(req["name"].get<std::string>());
        }
    );
}

void CollectorScheduler::registerFinishCallbacks() {
    auto callbacks = WriterManager::instance().get_onFinishCallbacks();
    
    for (const auto& cb_func : callbacks) {
        auto name = std::get<0>(cb_func);
        auto func = std::get<1>(cb_func);
        addCallback(name, func);
    }
}
