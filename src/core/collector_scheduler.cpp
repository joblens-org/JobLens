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

    spdlog::info("CollectorScheduler: initialized with {} collector(s) and {} writer(s)",
                collector_info_dict.size(), WriterManager::instance().names().size());
}

CollectorScheduler::~CollectorScheduler() { shutdown(); }




std::shared_ptr<CollectorScheduler::CollectorLaneContext>
CollectorScheduler::createLane(const std::string& name, const collector_info& info,
                               collector_state& state) {
    auto lane = std::make_shared<CollectorLaneContext>();
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
                                                 std::shared_ptr<CollectorLaneContext> lane) {
    auto sink = [this, state_ptr = &state, lane_handle = state.lane](
                    const ReadySources& ready) {
        {
            std::lock_guard lock(state_ptr->ready_m_);
            for (const auto& source : ready) {
                auto it = std::find_if(state_ptr->ready.begin(), state_ptr->ready.end(),
                    [&](const ReadySource& item) { return item.source == source.source; });
                if (it == state_ptr->ready.end()) state_ptr->ready.push_back(source);
                else it->readiness |= source.readiness;
            }
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
                                                    std::shared_ptr<CollectorLaneContext> lane) {
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

bool CollectorScheduler::startCollector(std::string collector_name) {
    // Caller holds m_.
    auto it = collector_info_dict.find(collector_name);
    if (it == collector_info_dict.end()) {
        spdlog::error("CollectorScheduler: start collector error, can not find {}!", collector_name);
        return false;
    }

    const collector_info info = it->second;
    if (!info.route_ok()) {
        spdlog::error("CollectorScheduler: collector {} has an invalid output route, refusing to start",
                      collector_name);
        return false;
    }

    auto& state = collector_state_dict[collector_name];
    if (state.drain_failed.load()) {
        spdlog::error("CollectorScheduler: refusing to restart failed event collector {}", collector_name);
        return false;
    }
    auto lane = createLane(collector_name, info, state);
    if (!lane) return false;

    // A collector that reports running without a successful init would make
    // actual() claim holders that never existed. Only the started flag counts.
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
        // A start that failed must not run the stop/accounting protocol: the
        // caller (addJob2Collector) already holds this lane's state mutex while
        // startCollector runs, and stopCollectorInstance would re-lock it in
        // finalizePendingRemovals on the same thread. A lane that never entered
        // the running state owns no accepted tail, so only the partial init is
        // released here; the caller rolls membership/snapshot/token back.
        teardownCollectorLane(*lane, /*release_partial_init=*/true);
        return false;
    }

    if (started)
        spdlog::info("CollectorScheduler: start collector {}", collector_name);
    return started;
}

void CollectorScheduler::startPeriodicJobCollector(const std::string& name,
                                                   const collector_info& info,
                                                   collector_state& state,
                                                   std::shared_ptr<CollectorLaneContext> lane) {
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
                                                      std::shared_ptr<CollectorLaneContext> lane) {
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
                                                std::shared_ptr<CollectorLaneContext> lane) {
    (void)info;
    (void)lane;
    state.running = true;
    // A fresh attachment starts with no unfinished drain from a previous run.
    state.drain_pending = false;
    runtime_->timer(name, false);
}

void CollectorScheduler::startEventSystemCollector(const std::string& name,
                                                   const collector_info& info,
                                                   collector_state& state,
                                                   std::shared_ptr<CollectorLaneContext> lane) {
    (void)info;
    (void)lane;
    state.running = true;
    // A fresh attachment starts with no unfinished drain from a previous run.
    state.drain_pending = false;
    runtime_->timer(name, false);
}

WorkResult CollectorScheduler::runCollectorLane(std::shared_ptr<CollectorLaneContext> lane) {
    if (lane->info.trigger == CollectorTrigger::Event) return runEventDrain(*lane);
    if (lane->info.scope == CollectorScope::System) return runPeriodicSystemTick(*lane);
    return runPeriodicJobSweep(*lane);
}

void CollectorScheduler::stopCollectorInstance(CollectorLaneContext& lane) {
    auto& state = *lane.state;
    if (!state.running && !state.initialized && !state.registrar && !state.lane_valid) return;

    teardownCollectorLane(lane);
    // Teardown is not proof of an unfinished tail. The owned snapshots and
    // close bookkeeping live with the scheduler and are freed with it; no
    // completion is sent, so the registry retains its durable Closing row.
    if (!state.drain_pending && !state.drain_failed.load())
        notifyDrained(finalizePendingRemovals(lane));
}

void CollectorScheduler::teardownCollectorLane(CollectorLaneContext& lane,
                                               bool release_partial_init) {
    // Lock-free by contract: the failed-start path runs this under the caller's
    // state mutex, so it must not take any lane lock of its own.
    auto& state = *lane.state;
    state.running = false;
    if (state.task_id != 0) {
        timerScheduler_.cancelTimer(state.task_id);
        state.task_id = 0;
    }
    if (state.lane_valid) {
        taskExecutor_.invalidate(state.lane);
        state.lane_valid = false;
    }
    {
        std::lock_guard lock(state.ready_m_);
        if (state.ready_timer) timerScheduler_.cancelTimer(*state.ready_timer);
        state.ready_timer.reset();
        state.ready_after = {};
        state.ready.clear();
    }
    // Deinit first: collectors own their source registrations and may destroy
    // them here. The registrar must still be alive for those destructors.
    if (lane.info.deinit_handle && (state.initialized || release_partial_init)) {
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
}

void CollectorScheduler::stopAllCollectors() {
    std::vector<CollectorLaneContext> lanes;
    {
        std::lock_guard lock(m_);
        for (auto& [name, state] : collector_state_dict) {
            auto it = collector_info_dict.find(name);
            if (it == collector_info_dict.end()) continue;
            CollectorLaneContext lane;
            lane.name = name;
            lane.info = it->second;
            lane.state = &state;
            lanes.push_back(std::move(lane));
        }
    }
    for (auto& lane : lanes) stopCollectorInstance(lane);
}

std::vector<uint64_t> CollectorScheduler::finalizePendingRemovals(CollectorLaneContext& lane) {
    auto& state = *lane.state;
    std::vector<collector_state::PendingClose> closes;
    std::vector<uint64_t> completed;
    {
        std::lock_guard lock(state.m_);
        closes.swap(state.pending_close);
    }
    for (const auto& close : closes) {
        Job job;
        bool have_snapshot = false;
        {
            std::lock_guard lock(state.snapshot_m_);
            const auto snapshot_it = state.job_snapshots.find(close.job_id);
            const auto generation_it = state.job_generations.find(close.job_id);
            // Validate the attachment epoch BEFORE any hook or snapshot
            // mutation. A snapshot written by a newer attachment must never be
            // consumed or erased by this older close; it survives for its own
            // epoch and the stale acknowledgement below is rejected by token.
            if (snapshot_it != state.job_snapshots.end() &&
                generation_it != state.job_generations.end() &&
                generation_it->second == close.generation) {
                job = snapshot_it->second;
                state.job_snapshots.erase(snapshot_it);
                state.job_generations.erase(generation_it);
                state.job_generation_counter.erase(close.job_id);
                have_snapshot = true;
            }
        }
        if (have_snapshot) notifyJobHook(lane.info, JobEvent::Removed, job);
        // Every accepted tail has been produced now. Report the completion
        // outside every lane lock so the registry can archive. A completion
        // for a superseded attachment is rejected by its token.
        if (recordRemovalAck(close.job_id, close.token)) completed.push_back(close.job_id);
    }
    return completed;
}

bool CollectorScheduler::recordRemovalAck(uint64_t jobID, uint64_t token) {
    std::lock_guard lock(removal_m_);
    auto it = removal_pending_.find(jobID);
    if (it == removal_pending_.end()) return false;
    const auto token_it = drain_tokens_.find(jobID);
    if (token_it == drain_tokens_.end() || token_it->second != token) {
        // Stale completion: the attachment it belonged to is gone (or has been
        // replaced). It must never decrement a newer drain's counter.
        return false;
    }
    if (--it->second != 0) return false;
    removal_pending_.erase(it);
    drain_tokens_.erase(jobID);
    draining_.erase(jobID);
    return true;
}

bool CollectorScheduler::addJob2Collector(const Job& job, const std::string& collector,
                                          std::string* error){
    auto& state = collector_state_dict[collector];
    std::lock_guard lg(state.m_);
    /* 去重: 避免同一JobID被重复push导致collect(Job) dedup命中返回空快照 */
    if (std::find(state.jobid_list.begin(), state.jobid_list.end(), job.JobID) != state.jobid_list.end()) {
        return false;
    }
    // The previous attachment's accepted tail is still in flight. That close
    // owns the snapshot stored under this JobID; attaching now would replace it
    // and the old close would consume the new attachment's snapshot. Defer the
    // reattach to a later reconcile pass, after the lane finalized the epoch.
    for (const auto& close : state.pending_close) {
        if (close.job_id != job.JobID) continue;
        if (error != nullptr) *error = "previous attachment is still draining";
        return false;
    }
    // A new attachment generation starts here: any drain of the previous one is
    // obsolete and must not be completed by its late acknowledgements.
    {
        std::lock_guard removal_lock(removal_m_);
        removal_pending_.erase(job.JobID);
        drain_tokens_.erase(job.JobID);
        draining_.erase(job.JobID);
    }
    nextAttachToken(job.JobID);
    state.jobid_list.push_back(job.JobID);
    {
        std::lock_guard snapshot_lock(state.snapshot_m_);
        state.job_generations[job.JobID] = ++state.job_generation_counter[job.JobID];
        state.job_snapshots[job.JobID] = job;
    }
    runtime_->jobs(collector, state.jobid_list.size());
    if (!state.running && !startCollector(collector)) {
        // The collector never entered the running state. Roll the attachment
        // back: membership, snapshot and token must not outlive a failed init,
        // and the dedup marker must not block the next reconcile retry.
        {
            std::lock_guard snapshot_lock(state.snapshot_m_);
            state.job_snapshots.erase(job.JobID);
            state.job_generations.erase(job.JobID);
            state.job_generation_counter.erase(job.JobID);
        }
        auto& list = state.jobid_list;
        list.erase(std::remove(list.begin(), list.end(), job.JobID), list.end());
        runtime_->jobs(collector, list.size());
        // Release the token only when no other collector still holds the job.
        bool held = false;
        for (auto& [other_name, other] : collector_state_dict) {
            if (other_name == collector) continue;
            std::lock_guard other_lock(other.m_);
            if (std::find(other.jobid_list.begin(), other.jobid_list.end(), job.JobID) !=
                other.jobid_list.end()) {
                held = true;
                break;
            }
        }
        if (!held) {
            attach_tokens_.erase(job.JobID);
            attach_revisions_.erase(job.JobID);
        }
        attach_errors_[job.JobID] = "collector '" + collector + "' failed to start";
        if (error != nullptr) *error = attach_errors_[job.JobID];
        spdlog::error("CollectorScheduler: {} could not attach JobID {}; retrying on the "
                      "next reconcile", collector, job.JobID);
        return false;
    }
    // init 失败时采集器没有进入运行态，不投递 Job 钩子，避免采集器在未初始化状态被回调
    if (state.running) {
        fireJobHook(collector, JobEvent::Added, job);
    } else {
        return false;
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
    if (event_scope && (job_ending || state.drain_failed.load())) {
        // Keep the snapshot and let the event lane consume accepted tail input
        // before the registry clears kernel attribution. The token identifies
        // this attachment so a late completion cannot finish a newer one; the
        // generation pins the exact snapshot epoch this close may release.
        uint64_t token = 0;
        const auto token_it = attach_tokens_.find(id);
        if (token_it != attach_tokens_.end()) token = token_it->second;
        uint64_t generation = 0;
        {
            std::lock_guard snapshot_lock(state.snapshot_m_);
            const auto generation_it = state.job_generations.find(id);
            if (generation_it != state.job_generations.end())
                generation = generation_it->second;
        }
        state.pending_close.push_back(collector_state::PendingClose{id, token, generation});
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
    std::vector<uint64_t> completed;
    {
        std::lock_guard lg(m_);
        completed = rmJobCollectLocked(job);
    }
    notifyDrained(completed);
}

std::vector<uint64_t> CollectorScheduler::rmJobCollectLocked(const Job& job){
    // Caller holds m_.
    std::vector<uint64_t> completed;

    // An accepted drain keeps ownership of the stopping boundary: it may not
    // be restarted just because the collector membership is already detached.
    // The deferred tail (event lane) would otherwise be reported as clean.
    {
        std::lock_guard removal_lock(removal_m_);
        if (draining_.count(job.JobID) != 0) return completed;
    }

    // Membership alone is not the whole boundary: a lane that was stopped while
    // the record was still Active already holds a deferred close (its tail is
    // still owed) and has no membership any more. Hold every Job-scope lane
    // while the boundary is computed, so the completion count and the adopted
    // tokens are fixed before any acknowledgement can be applied.
    std::vector<std::unique_lock<std::mutex>> lane_locks;
    lane_locks.reserve(collector_state_dict.size());
    std::vector<collector_state*> job_lanes;
    std::vector<std::string> bound_job_collectors;
    size_t adopted_closes = 0;
    for (auto& [name, state] : collector_state_dict) {
        auto info_it = collector_info_dict.find(name);
        if (info_it == collector_info_dict.end()) continue;
        if (info_it->second.scope != CollectorScope::Job) continue;
        lane_locks.emplace_back(state.m_);
        job_lanes.push_back(&state);
        if (std::find(state.jobid_list.begin(), state.jobid_list.end(), job.JobID) !=
            state.jobid_list.end())
            bound_job_collectors.push_back(name);
        for (const auto& close : state.pending_close)
            if (close.job_id == job.JobID) ++adopted_closes;
    }

    uint64_t drain_token = 0;
    bool immediate = false;
    {
        std::lock_guard removal_lock(removal_m_);
        if (draining_.count(job.JobID) != 0) return completed;
        if (bound_job_collectors.empty() && adopted_closes == 0) {
            // Nothing holds the instance: the stopping boundary is already
            // proven, so the completion is immediate.
            removal_pending_.erase(job.JobID);
            drain_tokens_.erase(job.JobID);
            draining_.erase(job.JobID);
            completed.push_back(job.JobID);
            immediate = true;
        } else {
            // A fresh token identifies THIS boundary: a tail that finalized
            // before it carries an older token and must not count here.
            drain_token = nextAttachToken(job.JobID);
            // Attached holders and prior-stop deferred closes are one fixed
            // completion count.
            removal_pending_[job.JobID] = bound_job_collectors.size() + adopted_closes;
            drain_tokens_[job.JobID] = drain_token;
            draining_.insert(job.JobID);
            // Adoption rewrites the completion obligation only. The captured
            // generation still names the tail lifetime this close releases.
            for (auto* state : job_lanes) {
                for (auto& close : state->pending_close)
                    if (close.job_id == job.JobID) close.token = drain_token;
            }
        }
    }
    lane_locks.clear();
    if (immediate) return completed;

    for (const auto& collector_name : bound_job_collectors) {
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) continue;
        auto& state = it->second;
        std::lock_guard lg_state(state.m_);
        const bool deferred = detachJobFromCollector(collector_name, state, job, true);
        if (!deferred && recordRemovalAck(job.JobID, drain_token)) completed.push_back(job.JobID);
    }

    // The System-scope view is a whole-registry projection diffed once per
    // reconcile pass (see reconcileOnce). Broadcasting Removed here as well
    // would deliver two Removed events for one removal.
    return completed;
}

bool CollectorScheduler::applyJobBinding(const Job& job, std::vector<std::string>& applied,
                                         std::string& error){
    std::lock_guard lg(m_);
    return applyJobBindingLocked(job, applied, error);
}

bool CollectorScheduler::applyJobBindingLocked(const Job& job, std::vector<std::string>& applied,
                                               std::string& error){
    const std::set<std::string> needed{job.CollectorNames.begin(), job.CollectorNames.end()};

    // An unknown collector name must fail the application instead of being
    // silently skipped; otherwise the caller is told a collector is collecting
    // when nothing was ever attached.
    for (const auto& name : needed) {
        if (collector_info_dict.count(name) == 0) {
            error = "collector '" + name + "' is not configured on this core";
            return false;
        }
    }

    std::vector<std::string> to_remove;
    to_remove.reserve(collector_state_dict.size());
    for (const auto& [name, state] : collector_state_dict) {
        if (!needed.count(name)) to_remove.push_back(name);
    }

    // One System-scope notification per real change, not per application
    // attempt: the same binding is applied again right after an attach and by
    // the intent-repair path, and a no-op must not look like new facts.
    bool changed = false;

    // Detach collectors the new selection no longer includes (on_job_removed).
    for (const auto& collector_name : to_remove) {
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) continue;
        auto& state = it->second;
        std::lock_guard lg_state(state.m_);
        if (detachJobFromCollector(collector_name, state, job, false)) changed = true;
    }

    // Attach new collectors (on_job_added); refresh snapshots of existing ones so
    // bound consumers see updated facts/PIDs (on_job_updated).
    for (const auto& collector_name : job.CollectorNames) {
        if (addJob2Collector(job, collector_name)) {
            changed = true;
            continue;
        }
        auto it = collector_state_dict.find(collector_name);
        auto it_info = collector_info_dict.find(collector_name);
        if (it == collector_state_dict.end() || it_info == collector_info_dict.end()) continue;
        {
            std::lock_guard lg_state(it->second.m_);
            if (!it->second.running) continue;
            // Only a collector that really holds the job may refresh its
            // snapshot. An attachment still draining its previous epoch is not
            // a holder; writing here would create a newer snapshot under the
            // JobID that the deferred close must never see.
            if (std::find(it->second.jobid_list.begin(), it->second.jobid_list.end(), job.JobID) ==
                it->second.jobid_list.end())
                continue;
            // A retained collector is refreshed whenever the facts really
            // changed, and only then: re-applying an unchanged snapshot (for
            // example the binding application that runs right after attach)
            // must not deliver a second notification for the same state.
            bool facts_changed = true;
            {
                std::lock_guard snapshot_lock(it->second.snapshot_m_);
                const auto previous = it->second.job_snapshots.find(job.JobID);
                if (previous != it->second.job_snapshots.end())
                    facts_changed = dump_job(previous->second).dump() != dump_job(job).dump();
                it->second.job_snapshots[job.JobID] = job;
            }
            if (facts_changed) {
                notifyJobHook(it_info->second, JobEvent::Updated, job);
                changed = true;
            }
        }
    }

    // Report exactly the collectors that hold the job and are running.
    for (const auto& collector_name : job.CollectorNames) {
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) continue;
        std::lock_guard lg_state(it->second.m_);
        if (!it->second.running) {
            if (error.empty())
                error = "collector '" + collector_name + "' is not running";
            continue;
        }
        if (std::find(it->second.jobid_list.begin(), it->second.jobid_list.end(),
                      job.JobID) == it->second.jobid_list.end()) {
            if (error.empty())
                error = "collector '" + collector_name + "' did not attach the job";
            continue;
        }
        applied.push_back(collector_name);
    }

    // Stamp the actual holders onto the per-collector snapshots: the generic
    // collection status is scheduler-reported runtime state, partial success
    // included, never inferred from the desired list.
    for (const auto& collector_name : applied) {
        auto it = collector_state_dict.find(collector_name);
        if (it == collector_state_dict.end()) continue;
        std::lock_guard snapshot_lock(it->second.snapshot_m_);
        auto snap = it->second.job_snapshots.find(job.JobID);
        if (snap == it->second.job_snapshots.end()) continue;
        snap->second.collection.in_pipeline = true;
        snap->second.collection.collectors = applied;
    }

    return error.empty();
}

void CollectorScheduler::updateJobCollect(const Job& job){
    std::vector<std::string> applied;
    std::string error;
    applyJobBinding(job, applied, error);
    if (!error.empty())
        spdlog::warn("CollectorScheduler: applying binding for JobID {} reported: {}",
                     job.JobID, error);
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
    // Scoped finalization is not implemented: reject it explicitly instead of
    // silently dropping the requested report.
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


void CollectorScheduler::attach(IJobRecordSource* source, IDrainCompletionSink* sink) {
    // Composition order: the desired-state source and the drain-completion sink
    // are fixed before the worker starts. They are never replaced afterwards.
    std::lock_guard lock(reconcile_m_);
    record_source_ = source;
    drain_sink_ = sink;
}

void CollectorScheduler::wake() {
    {
        std::lock_guard lock(reconcile_m_);
        reconcile_requested_ = true;
    }
    reconcile_cv_.notify_one();
}

void CollectorScheduler::notifyDrained(const std::vector<uint64_t>& job_ids) {
    if (job_ids.empty()) return;
    IDrainCompletionSink* sink = nullptr;
    {
        std::lock_guard lock(reconcile_m_);
        sink = drain_sink_;
    }
    if (sink == nullptr) return;
    for (const auto id : job_ids) sink->onDrained(id);
}

uint64_t CollectorScheduler::nextAttachToken(uint64_t job_id) {
    // Caller holds m_. A reattach of the same instance gets a strictly greater
    // token; tokens are never reused, so a late completion for an older
    // attachment can be rejected instead of merged.
    const uint64_t token = ++attach_token_seq_;
    attach_tokens_[job_id] = token;
    return token;
}

bool CollectorScheduler::beginDrain(uint64_t job_id) {
    IJobRecordSource* source = nullptr;
    {
        std::lock_guard lock(reconcile_m_);
        source = record_source_;
    }
    if (source == nullptr) {
        // No desired-state source: this runtime cannot prove any boundary.
        return false;
    }
    std::optional<JobRecord> record;
    if (!source->runtimeRecord(job_id, record)) {
        // The store cannot be read: an unknown desired state is not proof that
        // nothing is attached, so the caller must keep the Closing record.
        return false;
    }
    if (!record) {
        // Not tracked by the registry: nothing this runtime could hold.
        return true;
    }
    std::vector<uint64_t> completed;
    {
        std::lock_guard lg(m_);
        if (record->state != RecordState::Closing) {
            // The durable state is authoritative: a drain for a record that is
            // not Closing is refused rather than guessed.
            return false;
        }
        completed = rmJobCollectLocked(record->job);
    }
    notifyDrained(completed);
    return true;
}

ActualCollectionState CollectorScheduler::actual(uint64_t job_id) {
    ActualCollectionState state;
    {
        std::lock_guard lg(m_);
        const auto token_it = attach_tokens_.find(job_id);
        if (token_it != attach_tokens_.end()) {
            state.tracked = true;
            state.attach_token = token_it->second;
        }
        const auto revision_it = attach_revisions_.find(job_id);
        if (revision_it != attach_revisions_.end())
            state.last_revision = revision_it->second;
        const auto error_it = attach_errors_.find(job_id);
        if (error_it != attach_errors_.end()) state.error = error_it->second;
        for (const auto& [name, collector] : collector_state_dict) {
            auto info_it = collector_info_dict.find(name);
            if (info_it == collector_info_dict.end()) continue;
            if (info_it->second.scope != CollectorScope::Job) continue;
            // Only a collector that really entered the running state holds the
            // job. A membership left behind by a failed init is not an
            // attachment and must never be reported as one.
            if (!collector.running.load()) continue;
            std::lock_guard lg_state(collector.m_);
            if (std::find(collector.jobid_list.begin(), collector.jobid_list.end(),
                          job_id) != collector.jobid_list.end()) {
                state.collectors.push_back(name);
                state.tracked = true;
            }
        }
    }
    {
        std::lock_guard lock(removal_m_);
        state.draining = draining_.count(job_id) != 0;
    }
    return state;
}

void CollectorScheduler::reconcileLoop() {
    for (;;) {
        {
            std::unique_lock lock(reconcile_m_);
            reconcile_cv_.wait_for(lock, reconcile_interval_, [this] {
                return reconcile_requested_ || reconcile_stopping_;
            });
            if (reconcile_stopping_) return;
            reconcile_requested_ = false;
        }
        try {
            reconcileOnce();
        } catch (const std::exception& error) {
            spdlog::error("CollectorScheduler: reconcile failed: {}", error.what());
        } catch (...) {
            spdlog::error("CollectorScheduler: reconcile failed: unknown exception");
        }
    }
}

void CollectorScheduler::reconcileOnce() {
    // This worker is the only place desired state becomes actual state. It
    // reads records from the registry (never a mirror) and reconciles:
    //   1. detach every collector binding a job the record no longer selects,
    //   2. attach/refresh what the record selects,
    //   3. drain Closing records.
    IJobRecordSource* source = nullptr;
    {
        std::lock_guard lock(reconcile_m_);
        source = record_source_;
    }
    if (source == nullptr) return;
    std::vector<JobRecord> records;
    if (!source->runtimeRecords(records)) {
        // The desired state cannot be read right now. That is NOT an empty
        // registry: keep every current attachment instead of detaching all of
        // them, which would turn a store outage into a collection outage.
        return;
    }

    std::vector<uint64_t> completed;
    {
        std::lock_guard lg(m_);

        std::map<uint64_t, Job> open_jobs;
        std::vector<Job> closing_jobs;
        std::set<uint64_t> closing_ids;
        for (const auto& record : records) {
            if (record.job.JobID == 0) continue;
            if (record.state == RecordState::Closing) {
                closing_ids.insert(record.job.JobID);
                closing_jobs.push_back(record.job);
                continue;
            }
            Job desired = record.job;
            desired.CollectorNames = record.selection.effectiveCollectors();
            open_jobs.emplace(record.job.JobID, std::move(desired));
        }

        // Closing records are removed through the single accounting path
        // FIRST: it reserves the drain token and the pending count before any
        // membership disappears, so the deferred event tail owns a real
        // acknowledgement instead of being completed from an unaccounted
        // detach and archived early.
        for (const auto& job : closing_jobs) {
            auto local = rmJobCollectLocked(job);
            completed.insert(completed.end(), local.begin(), local.end());
        }

        // Detach collectors that still hold a job the record no longer wants
        // (or that is gone). A job ending keeps its accepted tail alive in the
        // event lane before the snapshot is released. Closing records are
        // excluded: the drain above owns their membership and completion.
        for (auto& [name, state] : collector_state_dict) {
            auto info_it = collector_info_dict.find(name);
            if (info_it == collector_info_dict.end()) continue;
            if (info_it->second.scope != CollectorScope::Job) continue;

            std::vector<uint64_t> bound;
            {
                std::lock_guard lg_state(state.m_);
                bound = state.jobid_list;
            }
            for (const auto job_id : bound) {
                if (job_id == 0) continue;   // the system sentinel
                if (closing_ids.count(job_id) != 0) continue;
                bool wanted = false;
                const auto open_it = open_jobs.find(job_id);
                if (open_it != open_jobs.end()) {
                    const auto& names = open_it->second.CollectorNames;
                    wanted = std::find(names.begin(), names.end(), name) != names.end();
                }
                if (wanted) continue;
                Job snapshot;
                bool have_snapshot = false;
                {
                    std::lock_guard lg_snap(state.snapshot_m_);
                    const auto it = state.job_snapshots.find(job_id);
                    if (it != state.job_snapshots.end()) {
                        snapshot = it->second;
                        have_snapshot = true;
                    }
                }
                if (!have_snapshot) {
                    // No snapshot means no accepted tail to honour: drop the
                    // membership; the release loop below drops its token.
                    std::lock_guard lg_state(state.m_);
                    auto& list = state.jobid_list;
                    list.erase(std::remove(list.begin(), list.end(), job_id), list.end());
                    runtime_->jobs(name, list.size());
                    continue;
                }
                std::lock_guard lg_state(state.m_);
                detachJobFromCollector(name, state, snapshot, /*job_ending=*/true);
            }
        }

        // System-scope collectors see the whole registry, including records
        // that select no job collector. This diff is notification bookkeeping
        // only: the records remain the sole authority.
        {
            std::map<uint64_t, std::pair<Job, std::string>> next;
            for (const auto& record : records) {
                if (record.job.JobID == 0) continue;
                if (record.state == RecordState::Closing) continue;
                Job projection = record.job;
                projection.CollectorNames = record.selection.effectiveCollectors();
                const auto key = dump_job(projection).dump();
                const auto previous = system_projection_.find(record.job.JobID);
                if (previous == system_projection_.end())
                    notifySystemCollectors(projection, JobEvent::Added);
                else if (previous->second.second != key)
                    notifySystemCollectors(projection, JobEvent::Updated);
                next.emplace(record.job.JobID, std::make_pair(std::move(projection), key));
            }
            for (const auto& [id, previous] : system_projection_) {
                if (next.count(id) == 0)
                    notifySystemCollectors(previous.first, JobEvent::Removed);
            }
            system_projection_ = std::move(next);
        }

        // Attach / refresh what the records select. A refused application is
        // reported by the runtime status (actual()); it is never invented as
        // applied.
        for (const auto& [job_id, desired] : open_jobs) {
            if (desired.CollectorNames.empty()) continue;
            std::vector<std::string> applied;
            std::string error;
            applyJobBindingLocked(desired, applied, error);
            if (error.empty())
                attach_errors_.erase(job_id);
            else
                attach_errors_[job_id] = error;
        }

        // The runtime reports the revision it really converged to.
        for (const auto& record : records) {
            if (record.state == RecordState::Closing) continue;
            if (attach_errors_.count(record.job.JobID) != 0) continue;
            if (attach_tokens_.count(record.job.JobID) == 0) continue;
            attach_revisions_[record.job.JobID] = record.revision;
        }

        // Release the token of an instance the runtime no longer holds. Facts
        // refreshes and automatic bookkeeping do not detach anything, so they
        // keep the token; a real detach releases it and a later reattach gets
        // a strictly greater one.
        std::set<uint64_t> held;
        for (const auto& [name, state] : collector_state_dict) {
            auto info_it = collector_info_dict.find(name);
            if (info_it == collector_info_dict.end()) continue;
            if (info_it->second.scope != CollectorScope::Job) continue;
            std::lock_guard lg_state(state.m_);
            for (const auto job_id : state.jobid_list)
                if (job_id != 0) held.insert(job_id);
        }
        for (auto it = attach_tokens_.begin(); it != attach_tokens_.end();) {
            if (held.count(it->first) == 0) {
                attach_revisions_.erase(it->first);
                attach_errors_.erase(it->first);
                it = attach_tokens_.erase(it);
            } else {
                ++it;
            }
        }
    }

    std::sort(completed.begin(), completed.end());
    completed.erase(std::unique(completed.begin(), completed.end()), completed.end());
    notifyDrained(completed);
}

void CollectorScheduler::start() {
    {
        std::lock_guard lg(m_);
        if (running_) return;
        running_ = true;
    }
    {
        std::lock_guard lock(reconcile_m_);
        reconcile_stopping_ = false;
        reconcile_requested_ = true;
    }
    if (record_source_ != nullptr && !reconcile_thread_.joinable())
        reconcile_thread_ = std::thread([this] { reconcileLoop(); });
    reconcile_cv_.notify_one();
    runtime_->lifecycle("running");
}

void CollectorScheduler::shutdown() {
    std::lock_guard shutdown_lock(shutdown_mutex_);
    {
        // Stop reconciliation first; in-flight proven drains still report to
        // the attached sink until worker and collector shutdown completes.
        std::lock_guard lock(reconcile_m_);
        reconcile_stopping_ = true;
    }
    reconcile_cv_.notify_all();
    if (reconcile_thread_.joinable()) reconcile_thread_.join();
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
