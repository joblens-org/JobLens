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
#include "core/job_registry.hpp"
#include <sstream>
#include "common/config.hpp"
#include "common/utils.hpp"

#include <iostream>
#include <tuple>

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
                    addJobCollect(job);
                    spdlog::info("CollectorScheduler: job {} added, {} PIDs", job.JobID, job.JobPIDs.size());
                    break;
                }
                case JobEvent::Removed: {
                    rmJobCollect(job);
                    spdlog::info("CollectorScheduler: job {} removed", job.JobID);
                    break;
                }
                case JobEvent::Updated: {
                    updateJobCollect(job);
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




void CollectorScheduler::startCollector(std::string collector_name){
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

        auto& collector_job = collector_state_dict[collector_name];

        collector_job.task_id = timerScheduler_.registerRepeatingTimer(
            std::chrono::milliseconds(static_cast<long long>(1000.0 / freq)),
            [this, collector_name](){
                // 使用find代替operator[]，避免与持有m_的路径形成数据竞争
                auto it_info = collector_info_dict.find(collector_name);
                if (it_info == collector_info_dict.end()) return;
                auto& info = it_info->second;

                auto it_state = collector_state_dict.find(collector_name);
                if (it_state == collector_state_dict.end()) return;
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
                            ret = info.collect_handle(job.value());
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

void CollectorScheduler::addJob2Collector(size_t jobid, std::string collector){
    auto& state = collector_state_dict[collector];
    std::lock_guard lg(state.m_);
    state.jobid_list.push_back(jobid); 
    if(!state.running){
        startCollector(collector);
    }
}

void CollectorScheduler::addJobCollect(const Job& job){
    std::lock_guard lg(m_);
    for(auto& collector_name:job.CollectorNames){
        if (collector_info_dict.count(collector_name) == 0)
        {
            spdlog::error("CollectorScheduler: {} name is error, continue..", collector_name);
            continue;
        }
        addJob2Collector(job.JobID, collector_name);
    }
}

void CollectorScheduler::rmJobCollect(const Job& job){
    for(auto collector_name:job.CollectorNames){
        // 使用find代替at，避免定时器回调并发修改容器时抛出out_of_range
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) {
            spdlog::error("CollectorScheduler: collector {} not found for removal", collector_name);
            continue;
        }
        auto& state = it->second;
        std::lock_guard lg(state.m_);
        size_t id = job.JobID;
        state.jobid_list.erase(std::remove_if(state.jobid_list.begin(),state.jobid_list.end(),[id](size_t x){return x==id;}));
    }
}

void CollectorScheduler::updateJobCollect(const Job& job){
    std::lock_guard lg(m_);
    
    const std::set<std::string> needed{job.CollectorNames.begin(),
                                                   job.CollectorNames.end()};

    std::vector<std::string> to_remove;
    to_remove.reserve(collector_state_dict.size());
    for (const auto& [name, state] : collector_state_dict) {
        if (!needed.count(name))
            to_remove.push_back(name);
    }

    // 添加采集器
    for(auto& collector_name:job.CollectorNames){
        if (collector_info_dict.count(collector_name) == 0)
        {
            spdlog::error("CollectorScheduler: {} name is error, continue..", collector_name);
            continue;
        }
        auto& state = collector_state_dict[collector_name];
        auto it = std::find(state.jobid_list.begin(), state.jobid_list.end(), job.JobID);
        if (it == state.jobid_list.end()){
            addJob2Collector(job.JobID, collector_name);
        }
    }
    // 删除采集器
    for(auto& collector_name:to_remove){
        // 使用find代替at，避免定时器回调并发修改容器时抛出out_of_range
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) continue;
        auto& state = it->second;
        std::lock_guard lg(state.m_);
        size_t id = job.JobID;
        state.jobid_list.erase(std::remove_if(state.jobid_list.begin(),state.jobid_list.end(),[id](size_t x){return x==id;}));
    }
}

void CollectorScheduler::addJobCollectFunc(std::string name, std::string config, CollectFunc collector_handle,CollectInitFunc init_handle,CollectDeinitFunc deinit_handle) {
    std::lock_guard lg(m_);
    collector_info_dict[name].name = name;
    collector_info_dict[name].config_name = config;
    collector_info_dict[name].collect_handle = collector_handle;
    collector_info_dict[name].init_handle = init_handle;
    collector_info_dict[name].deinit_handle = deinit_handle;
    collector_info_dict[name].scope = CollectorScope::Job;

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

void CollectorScheduler::addSystemCollectFunc(std::string name, std::string config, CollectFunc collector_handle,CollectInitFunc init_handle,CollectDeinitFunc deinit_handle){
    std::lock_guard lg(m_);
    collector_info_dict[name].name = name;
    collector_info_dict[name].config_name = config;
    collector_info_dict[name].collect_handle = collector_handle;
    collector_info_dict[name].init_handle = init_handle;
    collector_info_dict[name].deinit_handle = deinit_handle;
    collector_info_dict[name].scope = CollectorScope::System;

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
            collector_state_dict[name].jobid_list.push_back(0); //job 0已经默认存在
            startCollector(name);
        }
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} auto_start, do not start it", name);
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
            addJobCollect(job);
            break;
        }
        case JobEvent::Removed: {
            spdlog::info("CollectorScheduler: job {} removed", job.JobID);
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
        if(scope == CollectorScope::Undefined){
            spdlog::error("CollectorScheduler: {} scope is undefined, skip it",collector.name);
            continue;
        }
        if(scope == CollectorScope::Job){
            addJobCollectFunc(
                collector.name,
                collector.config,
                collector_handle.collect,
                collector_handle.init,
                collector_handle.deinit
            );
        }// 虽然这种实现不好看，但是可以隔离Job和System的逻辑
        else if(scope == CollectorScope::System){
            addSystemCollectFunc(
                collector.name,
                collector.config,
                collector_handle.collect,
                collector_handle.init,
                collector_handle.deinit
            );
        }
        else{
            spdlog::error("CollectorScheduler: {} scope is error, skip it",collector.name);
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