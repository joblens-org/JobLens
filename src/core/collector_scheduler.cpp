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
#include "core/output_context.hpp"
#include "core/output_router.hpp"
#include "core/collector_execution_context.hpp"
#include <sstream>
#include "common/config.hpp"
#include "common/utils.hpp"

#include <iostream>
#include <tuple>
#include <fmt/core.h>
#include "common/local_rpc.hpp"

CollectorScheduler::CollectorScheduler()
    : taskExecutor_({
          static_cast<size_t>(Config::instance().getInt("lens_config", "max_collector_threads")),
          static_cast<size_t>(Config::instance().getInt(
              "collector_execution_config", "max_registered_tasks", 1024))}),
      timerScheduler_(taskExecutor_),
      eventPoller_(static_cast<size_t>(Config::instance().getInt(
          "collector_execution_config", "max_event_sources", 1024)))
{
    eventPoller_.start();
    runtime_->schedulerStatusReader(timerScheduler_.statusReader());
    auto runtime = runtime_;
    RPCServer::instance().register_method("CollectorScheduler/status",
        [runtime](const nlohmann::json&) { return runtime->snapshot(); });
    RPCServer::instance().register_method("CollectorScheduler/collector_status",
        [runtime](const nlohmann::json& params) { return runtime->collector(params); });
    default_freq = Config::instance().getInt("collectors_config", "default_freq");
    // Writers 必须在路由绑定与 System 采集器自动启动前就绪。
    WriterManager::instance().initialize();

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
                collector_info_dict.size(), WriterManager::instance().names().size());
}

CollectorScheduler::~CollectorScheduler() { shutdown(); }




void CollectorScheduler::startCollector(std::string collector_name){
    collector_info info;
    try {
        info = collector_info_dict.at(collector_name);
    }
    catch(const std::exception& e) {
        spdlog::error("CollectorScheduler: start collector error, can not find {}!",collector_name);
        return;
    }
    if (!info.route_ok()) {
        // 路由无效的采集器不启动，避免输出静默丢失；状态中保留错误。
        spdlog::error("CollectorScheduler: collector {} has an invalid output route, refusing to start", collector_name);
        return;
    }
    if (info.trigger == CollectorTrigger::Event) {
        spdlog::info("CollectorScheduler: collector {} is event-triggered; dispatch is wired in a later task", collector_name);
        return;
    }
    try
    {
        CollectorRuntime::Operation initializing(runtime_, collector_name, "init");
        auto config_node = Config::instance().getRawNode(info.config_name);
        auto j_config = Utils::yamlToJson(config_node);
        NullSourceRegistrar registrar;
        CollectorInitContext init_context(info.scope, registrar);
        if (!info.init_handle(j_config, init_context)) {
            initializing.fail("Collector initialization returned false");
            spdlog::error("CollectorScheduler: collector {} init failed", collector_name);
            return;
        }
        auto freq = info.freq;

        auto& collector_job = collector_state_dict[collector_name];

        collector_job.task_id = timerScheduler_.registerRepeatingTimer(
            std::chrono::milliseconds(static_cast<long long>(1000.0 / freq)),
            [this, collector_name](){
                CollectorRuntime::Operation callback(runtime_, collector_name, "callback");
                auto it_info = collector_info_dict.find(collector_name);
                if (it_info == collector_info_dict.end()) return;
                auto& info = it_info->second;

                auto it_state = collector_state_dict.find(collector_name);
                if (it_state == collector_state_dict.end()) return;
                auto& collector_job = it_state->second;

                const auto make_sample = [&](const Job& job) {
                    auto& manager = WriterManager::instance();
                    auto sample = std::make_shared<SampleMetadata>();
                    sample->sample_id = fmt::format("{}:{}", manager.process_run_id(),
                                                    manager.next_sample_counter());
                    sample->collector_name = collector_name;
                    sample->collector_type = info.type;
                    sample->job = job;
                    sample->sampled_at = std::chrono::system_clock::now();
                    return sample;
                };

                if (info.scope == CollectorScope::System) {
                    std::unique_lock<std::mutex> lg(collector_job.m_, std::defer_lock);
                    {
                        CollectorRuntime::Operation waiting(runtime_, collector_name, "wait_collector_lock");
                        lg.lock();
                    }
                    if (!collector_job.running) return;

                    Job system_job;
                    system_job.JobID = 0;
                    system_job.jobtype = JobType::Sys;
                    system_job.subtype = JobSubType::Common;
                    CommonJobAttr attr;
                    attr.auto_update_child = false;
                    system_job.sub_attr = attr;

                    OutputContext output(info.router, make_sample(system_job));
                    std::unique_ptr<CollectorRuntime::Operation> write_op;
                    output.set_write_observer(
                        [&](std::string_view /*channel*/) {
                            write_op = std::make_unique<CollectorRuntime::Operation>(
                                runtime_, collector_name, "write", 0);
                        },
                        [&](std::string_view channel, bool failed) {
                            if (failed && write_op) {
                                write_op->fail("output rejected on channel " + std::string(channel));
                            }
                            write_op.reset();
                        });

                    CollectOutcome outcome;
                    std::string error;
                    bool ok = true;
                    try {
                        CollectorRuntime::Operation collecting(runtime_, collector_name, "collect", 0);
                        outcome = std::get<PeriodicSystemBinding>(info.binding).collect(output);
                    } catch (const std::exception& e) {
                        ok = false;
                        error = e.what();
                    } catch (...) {
                        ok = false;
                        error = "collector threw an unknown exception";
                    }
                    if (ok) {
                        if (outcome.state == CollectState::Failed) {
                            ok = false;
                            error = outcome.error.empty() ? "collector reported failure" : outcome.error;
                        } else if (outcome.state == CollectState::Skipped && output.submitted() > 0) {
                            ok = false;
                            error = "collector returned Skipped after emitting output";
                        }
                        if (output.had_error()) {
                            ok = false;
                            error = output.error_summary();
                        }
                    }
                    if (!ok) {
                        callback.fail(error);
                        runtime_->error(collector_name, error);
                        spdlog::error("CollectorScheduler: collector {} system collect failed: {}",
                                      collector_name, error);
                    } else if (outcome.state == CollectState::Success) {
                        runtime_->clear_error(collector_name);
                    }
                    return;
                }

                std::vector<size_t> jobids_snapshot;
                {
                    std::unique_lock<std::mutex> lg(collector_job.m_, std::defer_lock);
                    {
                        CollectorRuntime::Operation waiting(runtime_, collector_name, "wait_collector_lock");
                        lg.lock();
                    }
                    if(collector_job.jobid_list.empty() && collector_job.running){
                        //由JobRegistry完成任务清理工作
                        //没有任务，取消这个收集器，节省资源
                        CollectorRuntime::Operation deinitializing(runtime_, collector_name, "deinit");
                        info.deinit_handle();
                        timerScheduler_.cancelTimer(collector_job.task_id);
                        collector_job.running = false;
                        runtime_->timer(collector_name, false);
                        return;
                    }
                    jobids_snapshot = collector_job.jobid_list;
                }

                for(auto jobid: jobids_snapshot){
                    std::optional<Job> job;
                    {
                        CollectorRuntime::Operation looking_up(runtime_, collector_name, "lookup_job", jobid);
                        job = JobRegistry::instance().findJob(jobid);
                    }
                    std::unique_lock<std::mutex> lg(collector_job.m_, std::defer_lock);
                    {
                        CollectorRuntime::Operation waiting(runtime_, collector_name, "wait_collector_lock", jobid);
                        lg.lock();
                    }
                    if(!collector_job.running) return; //采集器已经deinit
                    if(!job.has_value())continue;

                    OutputContext output(info.router, make_sample(job.value()));
                    std::unique_ptr<CollectorRuntime::Operation> write_op;
                    output.set_write_observer(
                        [&](std::string_view /*channel*/) {
                            write_op = std::make_unique<CollectorRuntime::Operation>(
                                runtime_, collector_name, "write", jobid);
                        },
                        [&](std::string_view channel, bool failed) {
                            if (failed && write_op) {
                                write_op->fail("output rejected on channel " + std::string(channel));
                            }
                            write_op.reset();
                        });

                    CollectOutcome outcome;
                    std::string error;
                    bool ok = true;
                    {
                        try {
                            CollectorRuntime::Operation collecting(runtime_, collector_name, "collect", jobid);
                            outcome = std::get<PeriodicJobBinding>(info.binding).collect(job.value(), output);
                        } catch (const std::exception& e) {
                            ok = false;
                            error = e.what();
                        } catch (...) {
                            ok = false;
                            error = "collector threw an unknown exception";
                        }
                    }
                    if (ok) {
                        if (outcome.state == CollectState::Failed) {
                            ok = false;
                            error = outcome.error.empty() ? "collector reported failure" : outcome.error;
                        } else if (outcome.state == CollectState::Skipped && output.submitted() > 0) {
                            ok = false;
                            error = "collector returned Skipped after emitting output";
                        }
                        if (output.had_error()) {
                            ok = false;
                            error = output.error_summary();
                        }
                    }
                    if (!ok) {
                        callback.fail(error);
                        runtime_->error(collector_name, error);
                        spdlog::error("CollectorScheduler: collector {} job {} native collect failed: {}",
                                      collector_name, jobid, error);
                    } else if (outcome.state == CollectState::Success) {
                        // Skipped 表示没有可用样本，不能掩盖此前的错误。
                        runtime_->clear_error(collector_name);
                    }
                }
            }, collector_name
        );
        collector_job.running = true;
        runtime_->timer(collector_name, true);
    }
    catch(const std::exception& e) {
        runtime_->error(collector_name, e.what());
        spdlog::error("CollectorScheduler: start collector {} error: {}", collector_name, e.what());
    }

    spdlog::info("CollectorScheduler: start collector {}", collector_name);
}

bool CollectorScheduler::addJob2Collector(const Job& job, const std::string& collector){
    auto& state = collector_state_dict[collector];
    std::lock_guard lg(state.m_);
    /* 去重: 避免同一JobID被重复push导致collect(Job) dedup命中返回空快照 */
    if (std::find(state.jobid_list.begin(), state.jobid_list.end(), job.JobID) != state.jobid_list.end()) {
        return false;
    }
    state.jobid_list.push_back(job.JobID);
    runtime_->jobs(collector, state.jobid_list.size());
    if(!state.running){
        startCollector(collector);
    }
    // init 失败时采集器没有进入运行态，不投递 Job 钩子，避免采集器在未初始化状态被回调
    if (state.running) {
        fireJobHook(collector, JobEvent::Added, job);
    }
    return true;
}

void CollectorScheduler::detachJobFromCollector(const std::string& collector_name, collector_state& state, const Job& job){
    const size_t id = job.JobID;
    auto new_end = std::remove_if(state.jobid_list.begin(), state.jobid_list.end(),
                                  [id](size_t x){ return x == id; });
    if (new_end == state.jobid_list.end()) {
        // 本采集器并未绑定该 Job（例如采集器是在 Job 建立之后才注册的），不产生 Removed
        return;
    }
    state.jobid_list.erase(new_end, state.jobid_list.end());
    runtime_->jobs(collector_name, state.jobid_list.size());
    fireJobHook(collector_name, JobEvent::Removed, job);
}

void CollectorScheduler::fireJobHook(const std::string& collector_name, JobEvent event, const Job& job){
    // 调用方已持有该采集器的 collector_state::m_，保证钩子与 collect 互斥
    auto it_info = collector_info_dict.find(collector_name);
    if (it_info == collector_info_dict.end()) return;
    // Job 作用域钩子只投递给 Job 级采集器；System 级由 notifySystemCollectors 统一广播
    if (it_info->second.scope != CollectorScope::Job) return;
    notifyJobHook(it_info->second, event, job);
}

void CollectorScheduler::notifySystemCollectors(const Job& job, JobEvent event){
    // 调用方已持有 m_，保证遍历期间 collector_state_dict 不被并发插入
    if (job.JobID == 0) return;  // 系统占位 Job 没有生命周期语义
    for (auto& [name, state] : collector_state_dict) {
        auto it_info = collector_info_dict.find(name);
        if (it_info == collector_info_dict.end()) continue;
        if (it_info->second.scope != CollectorScope::System) continue;
        std::lock_guard lg(state.m_);
        if (!state.running) continue;  // 未 init / 已 deinit 的采集器不接收 Job 事件
        notifyJobHook(it_info->second, event, job);
    }
}

void CollectorScheduler::notifyJobHook(const collector_info& info, JobEvent event, const Job& job){
    const JobLifecycleFunc* hook = nullptr;
    const char* event_name = "unknown";
    switch (event) {
        case JobEvent::Added:   hook = &info.on_job_added;   event_name = "added";   break;
        case JobEvent::Updated: hook = &info.on_job_updated; event_name = "updated"; break;
        case JobEvent::Removed: hook = &info.on_job_removed; event_name = "removed"; break;
    }
    if (!hook || !(*hook)) return;
    try {
        (*hook)(job);
    }
    catch (const std::exception& e) {
        // 钩子异常不能冒泡到 JobRegistry 的增删流程
        spdlog::error("CollectorScheduler: collector {} on_job_{} hook failed: {}",
                      info.name, event_name, e.what());
    }
    catch (...) {
        spdlog::error("CollectorScheduler: collector {} on_job_{} hook failed: unknown exception",
                      info.name, event_name);
    }
}

void CollectorScheduler::addJobCollect(const Job& job){
    std::lock_guard lg(m_);
    for(const auto& collector_name : job.CollectorNames){
        if (collector_info_dict.count(collector_name) == 0)
        {
            spdlog::error("CollectorScheduler: {} name is error, continue..", collector_name);
            continue;
        }
        addJob2Collector(job, collector_name);
    }
    notifySystemCollectors(job, JobEvent::Added);
}

void CollectorScheduler::rmJobCollect(const Job& job){
    std::lock_guard lg(m_);
    for(const auto& collector_name : job.CollectorNames){
        // 使用find代替at，避免定时器回调并发修改容器时抛出out_of_range
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) {
            spdlog::error("CollectorScheduler: collector {} not found for removal", collector_name);
            continue;
        }
        auto& state = it->second;
        std::lock_guard lg_state(state.m_);
        detachJobFromCollector(collector_name, state, job);
    }
    notifySystemCollectors(job, JobEvent::Removed);
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

    // 删除本 Job 不再需要的采集器绑定（被摘除的采集器收到 on_job_removed）
    for(const auto& collector_name : to_remove){
        // 使用find代替at，避免定时器回调并发修改容器时抛出out_of_range
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) continue;
        auto& state = it->second;
        std::lock_guard lg_state(state.m_);
        detachJobFromCollector(collector_name, state, job);
    }
    // 添加采集器（新的绑定收到 on_job_added），已在列表中的只收到 on_job_updated
    for(const auto& collector_name : job.CollectorNames){
        if (collector_info_dict.count(collector_name) == 0)
        {
            spdlog::error("CollectorScheduler: {} name is error, continue..", collector_name);
            continue;
        }
        if (addJob2Collector(job, collector_name)) continue;
        // 已绑定：通知 Job 属性/PID 变化
        auto it = collector_state_dict.find(collector_name);
        auto it_info = collector_info_dict.find(collector_name);
        if (it == collector_state_dict.end() || it_info == collector_info_dict.end()) continue;
        std::lock_guard lg_state(it->second.m_);
        if (!it->second.running) continue;
        notifyJobHook(it_info->second, JobEvent::Updated, job);
    }
    notifySystemCollectors(job, JobEvent::Updated);
}

void CollectorScheduler::buildOutputRoute(collector_info& info) {
    auto result = OutputRouter::build(info.name, info.config_name, info.channels);
    if (!result.ok()) {
        info.route_error = result.error;
        runtime_->error(info.name, result.error);
        spdlog::error("CollectorScheduler: {}", result.error);
        return;
    }
    info.router = std::move(result.router);
    spdlog::info("CollectorScheduler: collector {} bound {} output channel(s)",
                 info.name, info.channels.size());
}

bool CollectorScheduler::parse_output_policy(const std::string& raw, bool supports_final,
                                             OutputPolicy& policy, std::string& error) {
    if (raw == "stream") {
        policy = OutputPolicy::Stream;
        return true;
    }
    if (raw != "final" && raw != "both") {
        error = "invalid output_policy '" + raw + "'";
        return false;
    }
    policy = raw == "final" ? OutputPolicy::Final : OutputPolicy::Both;
    if (!supports_final) {
        error = "output_policy '" + raw + "' is not supported by this collector";
        return false;
    }
    // Scoped finalization is added by a later task. Reject it explicitly instead
    // of silently dropping the requested report.
    error = "output_policy '" + raw + "' is not implemented in this build";
    return false;
}

void CollectorScheduler::applyOutputPolicy(collector_info& info) {
    const auto raw = Config::instance().getString(info.config_name, "output_policy", "stream");
    std::string error;
    if (!parse_output_policy(raw, info.supports_final, info.output_policy, error)) {
        info.route_error = "collector " + info.name + ": " + error;
    }
}

void CollectorScheduler::commonCollectorSetup(const std::string& name, const std::string& type,
                                              const std::string& config, CollectorHandle& handle,
                                              collector_info& info) {
    info.name = name;
    info.type = type;
    info.config_name = config;
    info.scope = handle.scope;
    info.trigger = handle.trigger;
    info.supports_final = handle.supports_final;
    info.binding = std::move(handle.binding);
    info.channels = std::move(handle.channels);
    info.init_handle = std::move(handle.init);
    info.deinit_handle = std::move(handle.deinit);
    info.on_job_added = std::move(handle.on_job_added);
    info.on_job_updated = std::move(handle.on_job_updated);
    info.on_job_removed = std::move(handle.on_job_removed);
    if (!handle.error.empty()) info.route_error = handle.error;

    if (info.trigger == CollectorTrigger::Periodic) {
        if (Config::instance().has(config, "freq")) {
            info.freq = Config::instance().getDouble(config, "freq");
        } else if (Config::instance().has(config, "period")) {
            const auto period = Config::instance().getDouble(config, "period");
            info.freq = period > 0 ? 1.0 / period : 0;
        } else {
            info.freq = default_freq;
        }
        if (info.freq <= 0)
            info.route_error = "collector " + name + " has an invalid frequency/period";
    }

    applyOutputPolicy(info);
}

void CollectorScheduler::addJobCollectFunc(std::string name, std::string type, std::string config, CollectorHandle handle) {
    std::lock_guard lg(m_);
    auto& info = collector_info_dict[name];
    commonCollectorSetup(name, std::move(type), config, handle, info);
    if (!std::holds_alternative<PeriodicJobBinding>(info.binding) && info.route_error.empty())
        info.route_error = "collector " + name + " is missing its periodic Job binding";

    collector_state_dict.try_emplace(name);
    runtime_->configure(name, config, to_string(info.scope),
                        info.freq > 0 ? 1000.0 / info.freq : 0,
                        to_string(info.trigger), to_string(info.output_policy));
    if (!info.route_error.empty()) runtime_->error(name, info.route_error);
    else buildOutputRoute(info);
}

void CollectorScheduler::addSystemCollectFunc(std::string name, std::string type, std::string config, CollectorHandle handle){
    std::lock_guard lg(m_);
    auto& info = collector_info_dict[name];
    commonCollectorSetup(name, std::move(type), config, handle, info);
    if (!std::holds_alternative<PeriodicSystemBinding>(info.binding) && info.route_error.empty())
        info.route_error = "collector " + name + " is missing its periodic System binding";

    collector_state_dict.try_emplace(name);
    runtime_->configure(name, config, to_string(info.scope),
                        info.freq > 0 ? 1000.0 / info.freq : 0,
                        to_string(info.trigger), to_string(info.output_policy));
    if (!info.route_error.empty()) {
        runtime_->error(name, info.route_error);
        return;
    }
    buildOutputRoute(info);

    try {
        if (Config::instance().getBool(config, "auto_start")) {
            if (!info.route_ok()) {
                spdlog::error("CollectorScheduler: collector {} route is invalid, skip auto start", name);
            } else {
                collector_state_dict[name].jobid_list.push_back(0); //job 0已经默认存在
                runtime_->jobs(name, collector_state_dict[name].jobid_list.size());
                startCollector(name);
            }
        }
    }
    catch(const std::exception& e) {
        spdlog::info("CollectorScheduler: can not get collect {} auto_start, do not start it", name);
    }
}

void CollectorScheduler::addJobEventCollectFunc(std::string name, std::string type,
                                                std::string config, CollectorHandle handle) {
    std::lock_guard lg(m_);
    auto& info = collector_info_dict[name];
    commonCollectorSetup(name, std::move(type), config, handle, info);
    if (!std::holds_alternative<EventJobBinding>(info.binding) && info.route_error.empty())
        info.route_error = "collector " + name + " is missing its Job event binding";
    collector_state_dict.try_emplace(name);
    runtime_->configure(name, config, to_string(info.scope), 0,
                        to_string(info.trigger), to_string(info.output_policy));
    if (!info.route_error.empty()) runtime_->error(name, info.route_error);
    else buildOutputRoute(info);
    spdlog::info("CollectorScheduler: collector {} is Job/Event; production dispatch is pending", name);
}

void CollectorScheduler::addSystemEventCollectFunc(std::string name, std::string type,
                                                   std::string config, CollectorHandle handle) {
    std::lock_guard lg(m_);
    auto& info = collector_info_dict[name];
    commonCollectorSetup(name, std::move(type), config, handle, info);
    if (!std::holds_alternative<EventSystemBinding>(info.binding) && info.route_error.empty())
        info.route_error = "collector " + name + " is missing its System event binding";
    collector_state_dict.try_emplace(name);
    runtime_->configure(name, config, to_string(info.scope), 0,
                        to_string(info.trigger), to_string(info.output_policy));
    if (!info.route_error.empty()) runtime_->error(name, info.route_error);
    else buildOutputRoute(info);
    spdlog::info("CollectorScheduler: collector {} is System/Event; production dispatch is pending", name);
}


void CollectorScheduler::start() {
    std::lock_guard lg(m_);
    if (running_) return;
    running_ = true;
    runtime_->lifecycle("running");
}

void CollectorScheduler::shutdown() {
    std::lock_guard shutdown_lock(shutdown_mutex_);
    runtime_->lifecycle("stopping");
    {
        std::lock_guard lg(m_);
        running_ = false;
    }
    eventPoller_.shutdown();
    timerScheduler_.shutdown();
    taskExecutor_.shutdown();
    runtime_->lifecycle("stopped");
    spdlog::info("CollectorScheduler: CollectorScheduler shutdown complete");
}

nlohmann::json CollectorScheduler::snapshot() {
    return runtime_->snapshot();
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
    for (const auto& collector : collectors)
        runtime_->configure(collector.name, collector.config, "undefined", 0);
    
    for (const auto& collector : collectors) {
        
        auto collector_handle = collector_reg.createCollector(collector.type, collector.name);
        if (!collector_handle.valid()) {
            const std::string error = collector_handle.error.empty()
                ? "Collector has no valid initialization handler"
                : collector_handle.error;
            runtime_->error(collector.name, error);
            spdlog::error("CollectorScheduler: {}: {}", collector.name, error);
            continue;
        }
        auto scope = collector_reg.getScope(collector.type);
        if(scope == CollectorScope::Undefined){
            runtime_->error(collector.name, "Collector scope is undefined");
            spdlog::error("CollectorScheduler: {} scope is undefined, skip it",collector.name);
            continue;
        }
        const auto trigger = collector_reg.getTrigger(collector.type);
        if(scope == CollectorScope::Job){
            if (trigger == CollectorTrigger::Periodic) {
                addJobCollectFunc(collector.name, collector.type, collector.config,
                                  std::move(collector_handle));
            } else {
                addJobEventCollectFunc(collector.name, collector.type, collector.config,
                                       std::move(collector_handle));
            }
        } else if(scope == CollectorScope::System){
            if (trigger == CollectorTrigger::Periodic) {
                addSystemCollectFunc(collector.name, collector.type, collector.config,
                                     std::move(collector_handle));
            } else {
                addSystemEventCollectFunc(collector.name, collector.type, collector.config,
                                          std::move(collector_handle));
            }
        } else {
            spdlog::error("CollectorScheduler: {} scope is error, skip it",collector.name);
            continue;
        }
        
    }
}
