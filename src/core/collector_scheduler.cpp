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




std::shared_ptr<CollectorScheduler::CollectorLane>
CollectorScheduler::createLane(const std::string& name, const collector_info& info,
                               collector_state& state) {
    auto lane = std::make_shared<CollectorLane>();
    lane->name = name;
    lane->info = info;
    lane->state = &state;

    auto handle = taskExecutor_.registerTask(name, [this, lane] {
        return runCollectorLane(lane);
    });
    if (!handle) {
        const std::string error = "collector " + name + " has no available execution lane";
        runtime_->error(name, error);
        spdlog::error("CollectorScheduler: {}", error);
        return nullptr;
    }
    state.lane = *handle;
    state.lane_valid = true;
    return lane;
}

bool CollectorScheduler::initializeCollector(const std::string& name,
                                             const collector_info& info,
                                             SourceRegistrar& registrar) {
    CollectorRuntime::Operation initializing(runtime_, name, "init");
    auto config_node = Config::instance().getRawNode(info.config_name);
    auto j_config = Utils::yamlToJson(config_node);
    CollectorInitContext init_context(info.scope, registrar);
    if (!info.init_handle(j_config, init_context)) {
        initializing.fail("Collector initialization returned false");
        spdlog::error("CollectorScheduler: collector {} init failed", name);
        return false;
    }
    return true;
}

bool CollectorScheduler::startEventCollectorLane(const std::string& name,
                                                 const collector_info& info,
                                                 collector_state& state,
                                                 std::shared_ptr<CollectorLane> lane) {
    auto sink = [this, state_ptr = &state, lane_handle = state.lane](
                    const ReadySources& ready) {
        {
            std::lock_guard lock(state_ptr->ready_m_);
            state_ptr->ready.insert(state_ptr->ready.end(), ready.begin(), ready.end());
        }
        taskExecutor_.request(lane_handle, RequestPolicy::Remember);
    };
    auto registrar = std::make_shared<StagedSourceRegistrar>(eventPoller_, std::move(sink));
    state.registrar = registrar;

    if (!initializeCollector(name, info, *registrar)) {
        if (info.deinit_handle) info.deinit_handle();
        state.initialized = false;
        registrar->revoke();
        taskExecutor_.invalidate(state.lane);
        state.lane_valid = false;
        state.registrar.reset();
        return false;
    }
    if (!registrar->commit()) {
        if (info.deinit_handle) info.deinit_handle();
        state.initialized = false;
        registrar->revoke();
        taskExecutor_.invalidate(state.lane);
        state.lane_valid = false;
        state.registrar.reset();
        runtime_->error(name, "collector source commit failed");
        return false;
    }
    state.initialized = true;

    if (info.scope == CollectorScope::Job)
        startEventJobCollector(name, info, state, std::move(lane));
    else
        startEventSystemCollector(name, info, state, std::move(lane));
    return true;
}

bool CollectorScheduler::startPeriodicCollectorLane(const std::string& name,
                                                    const collector_info& info,
                                                    collector_state& state,
                                                    std::shared_ptr<CollectorLane> lane) {
    NullSourceRegistrar registrar;
    if (!initializeCollector(name, info, registrar)) {
        if (info.deinit_handle) info.deinit_handle();
        state.initialized = false;
        taskExecutor_.invalidate(state.lane);
        state.lane_valid = false;
        return false;
    }
    state.initialized = true;

    if (info.scope == CollectorScope::Job)
        startPeriodicJobCollector(name, info, state, std::move(lane));
    else
        startPeriodicSystemCollector(name, info, state, std::move(lane));
    return true;
}

void CollectorScheduler::startCollector(std::string collector_name) {
    // Caller holds m_.
    auto it = collector_info_dict.find(collector_name);
    if (it == collector_info_dict.end()) {
        spdlog::error("CollectorScheduler: start collector error, can not find {}!", collector_name);
        return;
    }

    const collector_info info = it->second;
    if (!info.route_ok()) {
        spdlog::error("CollectorScheduler: collector {} has an invalid output route, refusing to start",
                      collector_name);
        return;
    }

    auto& state = collector_state_dict[collector_name];
    auto lane = createLane(collector_name, info, state);
    if (!lane) return;

    bool started = false;
    try {
        if (info.trigger == CollectorTrigger::Event)
            started = startEventCollectorLane(collector_name, info, state, lane);
        else
            started = startPeriodicCollectorLane(collector_name, info, state, lane);
    } catch (const std::exception& error) {
        runtime_->error(collector_name, error.what());
        spdlog::error("CollectorScheduler: start collector {} error: {}",
                      collector_name, error.what());
        stopCollectorInstance(*lane);
        return;
    }

    if (started)
        spdlog::info("CollectorScheduler: start collector {}", collector_name);
}

void CollectorScheduler::startPeriodicJobCollector(const std::string& name,
                                                   const collector_info& info,
                                                   collector_state& state,
                                                   std::shared_ptr<CollectorLane> lane) {
    (void)lane;
    state.task_id = timerScheduler_.registerRepeatingWorkTimer(
        std::chrono::milliseconds(static_cast<long long>(1000.0 / info.freq)),
        state.lane, RequestPolicy::SkipIfBusy, name);
    state.running = true;
    runtime_->timer(name, true);
}

void CollectorScheduler::startPeriodicSystemCollector(const std::string& name,
                                                      const collector_info& info,
                                                      collector_state& state,
                                                      std::shared_ptr<CollectorLane> lane) {
    (void)lane;
    state.task_id = timerScheduler_.registerRepeatingWorkTimer(
        std::chrono::milliseconds(static_cast<long long>(1000.0 / info.freq)),
        state.lane, RequestPolicy::SkipIfBusy, name);
    state.running = true;
    runtime_->timer(name, true);
}

void CollectorScheduler::startEventJobCollector(const std::string& name,
                                                const collector_info& info,
                                                collector_state& state,
                                                std::shared_ptr<CollectorLane> lane) {
    (void)info;
    (void)lane;
    state.running = true;
    runtime_->timer(name, false);
}

void CollectorScheduler::startEventSystemCollector(const std::string& name,
                                                   const collector_info& info,
                                                   collector_state& state,
                                                   std::shared_ptr<CollectorLane> lane) {
    (void)info;
    (void)lane;
    state.running = true;
    runtime_->timer(name, false);
}

WorkResult CollectorScheduler::runCollectorLane(std::shared_ptr<CollectorLane> lane) {
    if (lane->info.trigger == CollectorTrigger::Event) return runEventDrain(*lane);
    if (lane->info.scope == CollectorScope::System) return runPeriodicSystemTick(*lane);
    return runPeriodicJobSweep(*lane);
}

void CollectorScheduler::stopCollectorInstance(CollectorLane& lane) {
    auto& state = *lane.state;
    if (!state.running && !state.initialized && !state.registrar && !state.lane_valid) return;

    state.running = false;
    if (state.task_id != 0) {
        timerScheduler_.cancelTimer(state.task_id);
        state.task_id = 0;
    }
    if (state.lane_valid) {
        taskExecutor_.invalidate(state.lane);
        state.lane_valid = false;
    }
    // Deinit first: collectors own their source registrations and may destroy
    // them here. The registrar must still be alive for those destructors.
    if (state.initialized && lane.info.deinit_handle) {
        lane.info.deinit_handle();
        state.initialized = false;
    }
    if (state.registrar) {
        state.registrar->revoke();
        state.registrar.reset();
    }
    runtime_->timer(lane.name, false);
    state.sweep_active = false;
    state.sweep_jobs.clear();
    state.sweep_cursor = 0;
    finalizePendingRemovals(lane);
}

void CollectorScheduler::stopAllCollectors() {
    std::vector<CollectorLane> lanes;
    {
        std::lock_guard lock(m_);
        for (auto& [name, state] : collector_state_dict) {
            auto it = collector_info_dict.find(name);
            if (it == collector_info_dict.end()) continue;
            CollectorLane lane;
            lane.name = name;
            lane.info = it->second;
            lane.state = &state;
            lanes.push_back(std::move(lane));
        }
    }
    for (auto& lane : lanes) stopCollectorInstance(lane);
}

void CollectorScheduler::finalizePendingRemovals(CollectorLane& lane) {
    auto& state = *lane.state;
    std::vector<uint64_t> closes;
    {
        std::lock_guard lock(state.m_);
        closes.swap(state.pending_close);
    }
    for (auto job_id : closes) {
        Job job;
        bool found = false;
        {
            std::lock_guard lock(state.snapshot_m_);
            auto it = state.job_snapshots.find(job_id);
            if (it != state.job_snapshots.end()) {
                job = it->second;
                state.job_snapshots.erase(it);
                state.job_generations.erase(job_id);
                state.job_generation_counter.erase(job_id);
                found = true;
            }
        }
        if (found) notifyJobHook(lane.info, JobEvent::Removed, job);
        recordRemovalAck(job_id);
    }
}

void CollectorScheduler::recordRemovalAck(uint64_t jobID) {
    bool last = false;
    {
        std::lock_guard lock(removal_m_);
        auto it = removal_pending_.find(jobID);
        if (it == removal_pending_.end()) return;
        if (--it->second == 0) {
            removal_pending_.erase(it);
            last = true;
        }
    }
    if (last) JobRegistry::instance().acknowledgeJobRemoval(jobID);
}

bool CollectorScheduler::addJob2Collector(const Job& job, const std::string& collector){
    auto& state = collector_state_dict[collector];
    std::lock_guard lg(state.m_);
    /* 去重: 避免同一JobID被重复push导致collect(Job) dedup命中返回空快照 */
    if (std::find(state.jobid_list.begin(), state.jobid_list.end(), job.JobID) != state.jobid_list.end()) {
        return false;
    }
    state.jobid_list.push_back(job.JobID);
    {
        std::lock_guard snapshot_lock(state.snapshot_m_);
        state.job_generations[job.JobID] = ++state.job_generation_counter[job.JobID];
        state.job_snapshots[job.JobID] = job;
    }
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

bool CollectorScheduler::detachJobFromCollector(const std::string& collector_name,
                                                collector_state& state, const Job& job,
                                                bool job_ending){
    const size_t id = job.JobID;
    auto new_end = std::remove_if(state.jobid_list.begin(), state.jobid_list.end(),
                                  [id](size_t x){ return x == id; });
    if (new_end == state.jobid_list.end()) {
        // 本采集器并未绑定该 Job（例如采集器是在 Job 建立之后才注册的），不产生 Removed
        return false;
    }
    state.jobid_list.erase(new_end, state.jobid_list.end());
    runtime_->jobs(collector_name, state.jobid_list.size());

    auto info_it = collector_info_dict.find(collector_name);
    const bool event_scope = info_it != collector_info_dict.end() &&
                             info_it->second.scope == CollectorScope::Job &&
                             info_it->second.trigger == CollectorTrigger::Event;
    if (job_ending && event_scope) {
        // Keep the snapshot and let the event lane consume accepted tail input
        // before the registry clears kernel attribution.
        state.pending_close.push_back(id);
        if (state.lane_valid)
            taskExecutor_.request(state.lane, RequestPolicy::Remember);
        return true;
    }

    fireJobHook(collector_name, JobEvent::Removed, job);
    {
        std::lock_guard snapshot_lock(state.snapshot_m_);
        state.job_snapshots.erase(id);
        state.job_generations.erase(id);
        state.job_generation_counter.erase(id);
    }
    return false;
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

    std::vector<std::string> bound_job_collectors;
    for(const auto& collector_name : job.CollectorNames){
        auto state_it = collector_state_dict.find(collector_name);
        auto info_it = collector_info_dict.find(collector_name);
        if (state_it == collector_state_dict.end() || info_it == collector_info_dict.end()) {
            spdlog::error("CollectorScheduler: collector {} not found for removal", collector_name);
            continue;
        }
        if (info_it->second.scope != CollectorScope::Job) continue;
        const auto& jobs = state_it->second.jobid_list;
        if (std::find(jobs.begin(), jobs.end(), job.JobID) != jobs.end())
            bound_job_collectors.push_back(collector_name);
    }

    if (!bound_job_collectors.empty()) {
        std::lock_guard removal_lock(removal_m_);
        removal_pending_[job.JobID] = bound_job_collectors.size();
    }

    for (const auto& collector_name : bound_job_collectors) {
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) continue;
        auto& state = it->second;
        std::lock_guard lg_state(state.m_);
        const bool deferred = detachJobFromCollector(collector_name, state, job, true);
        if (!deferred) recordRemovalAck(job.JobID);
    }

    if (bound_job_collectors.empty())
        JobRegistry::instance().acknowledgeJobRemoval(job.JobID);

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
        detachJobFromCollector(collector_name, state, job, false);
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
        {
            std::lock_guard snapshot_lock(it->second.snapshot_m_);
            it->second.job_snapshots[job.JobID] = job;
        }
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
    try {
        if (info.route_ok() && Config::instance().getBool(config, "auto_start"))
            startCollector(name);
    } catch (const std::exception& error) {
        spdlog::info("CollectorScheduler: collector {} auto_start failed: {}", name, error.what());
    }
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
    try {
        if (info.route_ok() && Config::instance().getBool(config, "auto_start"))
            startCollector(name);
    } catch (const std::exception& error) {
        spdlog::info("CollectorScheduler: collector {} auto_start failed: {}", name, error.what());
    }
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
    stopAllCollectors();
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
