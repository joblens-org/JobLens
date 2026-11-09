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
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>
#include <atomic>
#include <future>
#include <string>
#include <unordered_map>
#include <optional>
#include <set>
#include <map>
#include <condition_variable>
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <date/date.h>

#include "core/collector_type.h"
#include "common/config.hpp"
#include "common/event_poller.hpp"
#include "common/task_executor.hpp"
#include "common/timer_scheduler.hpp"
#include "core/writer_manager.hpp"
#include <nlohmann/json.hpp>

#include "job_lifecycle_event.h"
#include "core/collector_runtime.hpp"
#include "core/collection_runtime.hpp"

// The collection runtime: the SOLE owner of the actual attach state. It pulls
// desired state from the registry through IJobRecordSource and reports a
// completed stop through IDrainCompletionSink; the registry never mirrors what
// is attached and never calls into a collector lane.
class CollectorScheduler : public ICollectionRuntime {
public:
    ~CollectorScheduler();

    // 非拷贝、可移动
    CollectorScheduler(const CollectorScheduler&)            = delete;
    CollectorScheduler& operator=(const CollectorScheduler&) = delete;
    CollectorScheduler(CollectorScheduler&&)                 = default;
    CollectorScheduler& operator=(CollectorScheduler&&)      = default;


    // Composition: the desired-state source and the drain-completion sink are
    // injected once, before start(). No singleton reverse read is used after
    // this call.
    void attach(IJobRecordSource* source, IDrainCompletionSink* sink);
    void start();
    void shutdown();

    nlohmann::json snapshot();

    // ICollectionRuntime -----------------------------------------------------
    // Coalesced reconcile request. Never blocks and never runs a collector.
    void wake() override;
    // Release every collector for one instance and prove the accepted tail is
    // produced. True = the runtime owns the drain and will call onDrained().
    bool beginDrain(uint64_t internal_id) override;
    ActualCollectionState actual(uint64_t internal_id) override;

    static CollectorScheduler& instance();

    // Shared validation used by registration and the policy regression test.
    static bool parse_output_policy(const std::string& raw, bool supports_final,
                                    OutputPolicy& policy, std::string& error);

private:
    friend struct CollectorSchedulerTestAccess;
    CollectorScheduler();

    struct collector_state{
        std::vector<size_t> jobid_list;
        mutable std::mutex      m_;
        size_t task_id = 0;
        std::atomic<bool>        running{false};
        bool initialized = false;

        // One serial lane per collector instance. Periodic timers and event
        // readiness both become requests on this handle.
        WorkHandle lane{};
        bool lane_valid = false;
        std::shared_ptr<StagedSourceRegistrar> registrar;

        // Poller-thread readiness is only staged here; the lane consumes it.
        std::mutex ready_m_;
        std::vector<ReadySource> ready;

        // A drain that outlived its round budget (on_ready returned more=true)
        // is still producing its accepted tail. The executor requeues the lane
        // on Yield; this flag makes the continuation rounds call on_ready even
        // when no new readiness was staged, and keeps pending_close untouched
        // until a round reports the drain complete. Guarded by m_, like every
        // other lane-owned field. A continuation round may pass an empty
        // ReadySources list: on_ready drains the collector's own backlog, the
        // list only names sources that signalled readiness.
        bool drain_pending = false;

        // Immutable Job snapshots visible to event contexts. Closing snapshots
        // stay until their lane tail has been finalized.
        std::mutex snapshot_m_;
        std::unordered_map<uint64_t, Job> job_snapshots;
        std::unordered_map<uint64_t, uint64_t> job_generations;
        std::unordered_map<uint64_t, uint64_t> job_generation_counter;

        // One deferred removal. It names the attachment whose accepted tail is
        // still being produced: the attach token rejects a completion from an
        // older attachment, and the snapshot generation proves the snapshot
        // being released is still the one this close captured. A newer
        // attachment never inherits the old close. Guarded by the collector
        // state mutex, like every other membership field.
        struct PendingClose {
            uint64_t job_id = 0;
            uint64_t token = 0;
            uint64_t generation = 0;
        };
        std::vector<PendingClose> pending_close;
        std::vector<size_t> sweep_jobs;
        size_t sweep_cursor = 0;
        bool sweep_active = false;
    };

    struct collector_info
    {
        std::string name;
        std::string type;
        CollectorScope scope = CollectorScope::Undefined;
        CollectorTrigger trigger = CollectorTrigger::Periodic;
        OutputPolicy output_policy = OutputPolicy::Stream;
        bool supports_final = false;
        std::string config_name;
        double freq = 0;
        CollectInitFunc init_handle;
        CollectDeinitFunc deinit_handle;
        JobLifecycleFunc on_job_added;
        JobLifecycleFunc on_job_updated;
        JobLifecycleFunc on_job_removed;
        // Exactly one binding selected by scope + trigger.
        CollectorBinding binding = PeriodicJobBinding{};
        std::vector<OutputChannel> channels;
        std::shared_ptr<const OutputRouter> router;
        std::string route_error;   // 非空表示路由绑定失败，采集器不得启动
        bool route_ok() const { return route_error.empty(); }
    };

    struct CollectorLaneContext {
        std::string name;
        collector_info info;
        collector_state* state = nullptr;
    };

    struct CollectInvocation {
        CollectOutcome outcome;
        std::string error;
        bool ok = true;
    };

    void addJobCollectFunc(std::string name, std::string type, std::string config, CollectorHandle handle);
    void addSystemCollectFunc(std::string name, std::string type, std::string config, CollectorHandle handle);
    void addJobEventCollectFunc(std::string name, std::string type, std::string config, CollectorHandle handle);
    void addSystemEventCollectFunc(std::string name, std::string type, std::string config, CollectorHandle handle);
    void commonCollectorSetup(const std::string& name, const std::string& type,
                              const std::string& config, CollectorHandle& handle,
                              collector_info& info);
    void applyOutputPolicy(collector_info& info);
    /// 为原生采集器编译路由；失败时写入 route_error。
    void buildOutputRoute(collector_info& info);
    void registerCollectFuncs();
    void addJobCollect(const Job& job);
    void rmJobCollect(const Job& job);
    // Caller holds m_. Returns the instances whose drain completed and whose
    // completion must be delivered after m_ is released.
    std::vector<uint64_t> rmJobCollectLocked(const Job& job);
    void updateJobCollect(const Job& job);
    // Reconciles the scheduler's job -> collector membership with the registry's
    // intent and reports which collectors really hold the job. Installed as the
    // registry's collection applier, so intent changes reach the lanes without
    // waiting for a restart. Returns false (with `error`) when a requested
    // collector is unknown or failed to start.
    bool applyJobBinding(const Job& job, std::vector<std::string>& applied,
                         std::string& error);
    // Caller holds m_.
    bool applyJobBindingLocked(const Job& job, std::vector<std::string>& applied,
                               std::string& error);
    // Caller holds m_. Returns false when the collector could not be
    // initialized/started, so the caller must not claim an attachment.
    bool startCollector(std::string collector);
    std::shared_ptr<CollectorLaneContext> createLane(const std::string& name,
                                              const collector_info& info,
                                              collector_state& state);
    bool initializeCollector(const std::string& name, const collector_info& info,
                             SourceRegistrar& registrar);
    void startPeriodicJobCollector(const std::string& name, const collector_info& info,
                                   collector_state& state,
                                   std::shared_ptr<CollectorLaneContext> lane);
    void startPeriodicSystemCollector(const std::string& name, const collector_info& info,
                                      collector_state& state,
                                      std::shared_ptr<CollectorLaneContext> lane);
    void startEventJobCollector(const std::string& name, const collector_info& info,
                                collector_state& state,
                                std::shared_ptr<CollectorLaneContext> lane);
    void startEventSystemCollector(const std::string& name, const collector_info& info,
                                   collector_state& state,
                                   std::shared_ptr<CollectorLaneContext> lane);
    bool startEventCollectorLane(const std::string& name, const collector_info& info,
                                 collector_state& state,
                                 std::shared_ptr<CollectorLaneContext> lane);
    bool startPeriodicCollectorLane(const std::string& name, const collector_info& info,
                                    collector_state& state,
                                    std::shared_ptr<CollectorLaneContext> lane);
    WorkResult runCollectorLane(std::shared_ptr<CollectorLaneContext> lane);
    WorkResult runPeriodicJobSweep(CollectorLaneContext& lane);
    WorkResult runPeriodicSystemTick(CollectorLaneContext& lane);
    WorkResult runEventDrain(CollectorLaneContext& lane);
    void collectJobSample(CollectorLaneContext& lane, size_t jobid,
                          CollectorRuntime::Operation& callback);
    void collectSystemSample(CollectorLaneContext& lane,
                             CollectorRuntime::Operation& callback);
    std::shared_ptr<SampleMetadata> makeSample(const std::string& name,
                                               const std::string& type,
                                               const Job& job) const;
    void attachWriteObserver(OutputContext& output, const std::string& name,
                             std::optional<size_t> jobid,
                             std::unique_ptr<CollectorRuntime::Operation>& write_op);
    CollectInvocation invokePeriodicJob(const collector_info& info, const Job& job,
                                        OutputContext& output, size_t jobid);
    CollectInvocation invokePeriodicSystem(const collector_info& info, OutputContext& output);
    void applyCollectResult(const std::string& name, std::optional<size_t> jobid,
                            const CollectInvocation& result, const OutputContext& output,
                            CollectorRuntime::Operation& callback);
    void stopCollectorInstance(CollectorLaneContext& lane);
    // Releases one lane's timer, executor slot, collector instance and staged
    // registrar. Takes no lock: a failed start runs it while its caller still
    // holds the lane state mutex, so it must never enter the stop/accounting
    // protocol (finalizePendingRemovals locks that same mutex).
    // release_partial_init: a start that threw may have allocated in its init
    // before failing, so deinit must run even though `initialized` is false —
    // the same contract the initializeCollector()==false paths follow.
    void teardownCollectorLane(CollectorLaneContext& lane,
                               bool release_partial_init = false);
    std::vector<uint64_t> finalizePendingRemovals(CollectorLaneContext& lane);
    // Returns true when this acknowledgement completed the drain for that
    // attachment. A token that no longer names the pending drain is stale and
    // changes nothing.
    bool recordRemovalAck(uint64_t jobID, uint64_t token);
    // Reconcile worker: converge to the records the registry publishes.
    void reconcileLoop();
    void reconcileOnce();
    void notifyDrained(const std::vector<uint64_t>& internal_ids);
    // Attach bookkeeping: a reattach of the same instance gets a strictly
    // greater token. Caller holds m_.
    uint64_t nextAttachToken(uint64_t internal_id);
    void stopAllCollectors();
    /// 挂载 Job 到采集器；返回 true 表示这是一次新的、已成功运行的挂载。
    /// A failed start rolls the membership back so the next reconcile retries
    /// init instead of deduplicating against a dead attachment.
    bool addJob2Collector(const Job& job, const std::string& collector,
                          std::string* error = nullptr);
    /// 从采集器摘除 Job；返回 true 表示 Removed 钩子被延后到 event lane。
    /// 调用方需持有 state.m_ 以及 m_。
    bool detachJobFromCollector(const std::string& collector_name, collector_state& state,
                                const Job& job, bool job_ending);
    /// 分发 Job 作用域钩子；调用方需持有该采集器的 state.m_
    void fireJobHook(const std::string& collector_name, JobEvent event, const Job& job);
    /// 向所有运行中的 System 作用域采集器广播 Job 事件；调用方需持有 m_
    void notifySystemCollectors(const Job& job, JobEvent event);
    /// 实际调用钩子并吞掉异常（钩子抛异常不得污染 Job 生命周期流程）
    void notifyJobHook(const collector_info& info, JobEvent event, const Job& job);

    Config& global_config = Config::instance();
    std::shared_ptr<CollectorRuntime> runtime_ = std::make_shared<CollectorRuntime>();
    // One bounded pool executes every timer, event and lifecycle callback.
    // TimerScheduler only owns deadlines and its wait thread.
    TaskExecutor taskExecutor_;
    TimerScheduler timerScheduler_;
    EventPoller eventPoller_;

    std::mutex              m_;
    std::mutex shutdown_mutex_;
    std::mutex removal_m_;
    // An accepted drain: how many bound collectors still owe an acknowledgement.
    std::unordered_map<uint64_t, size_t> removal_pending_;
    // The attach token the pending drain belongs to. A completion carrying any
    // other token is stale and must not complete a newer attachment.
    std::unordered_map<uint64_t, uint64_t> drain_tokens_;
    // Instances whose drain was accepted and whose completion is pending.
    std::set<uint64_t> draining_;
    // Monotonic attach tokens (never reused) and the live token per instance.
    uint64_t attach_token_seq_ = 0;
    std::unordered_map<uint64_t, uint64_t> attach_tokens_;
    // Last record revision each attachment converged to (actual-state report).
    std::unordered_map<uint64_t, uint64_t> attach_revisions_;
    // The last application failure per instance, cleared on convergence.
    std::unordered_map<uint64_t, std::string> attach_errors_;
    // Last System-scope notification projection per record (job + its
    // serialized form). Notification bookkeeping only: never an authoritative
    // state, never an attachment.
    std::map<uint64_t, std::pair<Job, std::string>> system_projection_;
    std::unordered_map<std::string, collector_info> collector_info_dict;
    std::unordered_map<std::string, collector_state> collector_state_dict;
    // Injected composition seam. Set once before start(); read afterwards.
    IJobRecordSource* record_source_ = nullptr;
    IDrainCompletionSink* drain_sink_ = nullptr;
    std::thread reconcile_thread_;
    std::mutex reconcile_m_;
    std::condition_variable reconcile_cv_;
    bool reconcile_requested_ = false;
    bool reconcile_stopping_ = false;
    std::chrono::milliseconds reconcile_interval_{1000};
    double default_freq;
    bool                    running_ = false;
};
