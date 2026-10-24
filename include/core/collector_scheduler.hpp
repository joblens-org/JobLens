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
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <date/date.h>

#include "core/collector_type.h"
#include "common/config.hpp"
#include "common/streamer_watcher.hpp"
#include "common/event_poller.hpp"
#include "common/task_executor.hpp"
#include "common/timer_scheduler.hpp"
#include "core/writer_manager.hpp"
#include <nlohmann/json.hpp>

#include "job_lifecycle_event.h"
#include "core/collector_runtime.hpp"

class CollectorScheduler {
public:
    ~CollectorScheduler();

    // 非拷贝、可移动
    CollectorScheduler(const CollectorScheduler&)            = delete;
    CollectorScheduler& operator=(const CollectorScheduler&) = delete;
    CollectorScheduler(CollectorScheduler&&)                 = default;
    CollectorScheduler& operator=(CollectorScheduler&&)      = default;


    void start();
    void shutdown();
    
    nlohmann::json snapshot();

    static CollectorScheduler& instance();

    // Shared validation used by registration and the policy regression test.
    static bool parse_output_policy(const std::string& raw, bool supports_final,
                                    OutputPolicy& policy, std::string& error);

private:
    friend struct CollectorSchedulerTestAccess;
    CollectorScheduler();

    struct collector_state{
        std::vector<size_t> jobid_list;
        std::mutex              m_;
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

        // Immutable Job snapshots visible to event contexts. Closing snapshots
        // stay until their lane tail has been finalized.
        std::mutex snapshot_m_;
        std::unordered_map<uint64_t, Job> job_snapshots;
        std::unordered_map<uint64_t, uint64_t> job_generations;
        std::unordered_map<uint64_t, uint64_t> job_generation_counter;

        // Protected by m_.
        std::vector<uint64_t> pending_close;
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

    struct CollectorLane {
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
    void updateJobCollect(const Job& job);
    // Caller holds m_.
    void startCollector(std::string collector);
    std::shared_ptr<CollectorLane> createLane(const std::string& name,
                                              const collector_info& info,
                                              collector_state& state);
    bool initializeCollector(const std::string& name, const collector_info& info,
                             SourceRegistrar& registrar);
    void startPeriodicJobCollector(const std::string& name, const collector_info& info,
                                   collector_state& state,
                                   std::shared_ptr<CollectorLane> lane);
    void startPeriodicSystemCollector(const std::string& name, const collector_info& info,
                                      collector_state& state,
                                      std::shared_ptr<CollectorLane> lane);
    void startEventJobCollector(const std::string& name, const collector_info& info,
                                collector_state& state,
                                std::shared_ptr<CollectorLane> lane);
    void startEventSystemCollector(const std::string& name, const collector_info& info,
                                   collector_state& state,
                                   std::shared_ptr<CollectorLane> lane);
    bool startEventCollectorLane(const std::string& name, const collector_info& info,
                                 collector_state& state,
                                 std::shared_ptr<CollectorLane> lane);
    bool startPeriodicCollectorLane(const std::string& name, const collector_info& info,
                                    collector_state& state,
                                    std::shared_ptr<CollectorLane> lane);
    WorkResult runCollectorLane(std::shared_ptr<CollectorLane> lane);
    WorkResult runPeriodicJobSweep(CollectorLane& lane);
    WorkResult runPeriodicSystemTick(CollectorLane& lane);
    WorkResult runEventDrain(CollectorLane& lane);
    void collectJobSample(CollectorLane& lane, size_t jobid,
                          CollectorRuntime::Operation& callback);
    void collectSystemSample(CollectorLane& lane,
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
    void stopCollectorInstance(CollectorLane& lane);
    void finalizePendingRemovals(CollectorLane& lane);
    void recordRemovalAck(uint64_t jobID);
    void stopAllCollectors();
    /// 挂载 Job 到采集器；返回 true 表示这是一次新的挂载（已触发 on_job_added）
    bool addJob2Collector(const Job& job, const std::string& collector);
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
    std::unordered_map<uint64_t, size_t> removal_pending_;
    std::unordered_map<std::string, collector_info> collector_info_dict;
    std::unordered_map<std::string, collector_state> collector_state_dict;
    double default_freq;
    bool                    running_ = false;
};
