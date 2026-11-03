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
// job_registry.h — the registry facade
//
// JobRegistry owns the JobStore: the single durable JobRecord representation
// and the only writer of facts, desired selection and Open/Closing state. It
// holds NO runtime mirror. Which collectors are actually attached belongs to
// the collection runtime, reached through the explicit ICollectionRuntime seam;
// the runtime converges by reading records through IJobRecordSource and reports
// a completed stop through IDrainCompletionSink.
#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <set>

#include "common/streamer_watcher.hpp"
#include "core/collector_type.h"
#include "core/job_identity.hpp"
#include "job_lifecycle_event.h"
#include "ebpf/trace_condor_starter.h"
#include "common/ebpf_common.hpp"
#include "core/job_store.hpp"
#include "core/collection_runtime.hpp"
#include "core/kernel_attribution.hpp"
#include "core/job_auto_policy.hpp"
#include "core/liveness.hpp"
#include "core/scheduler_adapter.hpp"

#include "job_watcher/condor_adapter.hpp"
#include "job_watcher/slurm_adapter.hpp"

// ---------------------------------------------------------------------------
// Collection selection control (registry-owned)
// ---------------------------------------------------------------------------
// These are the control-plane commands the registry executes itself: target
// validation, manual-stop precedence, revision CAS and the durable transaction
// live behind one entry point instead of a separate manager object calling
// back into the registry.
enum class CollectionError {
    None = 0,
    InvalidRequest,
    UnknownCollector,
    NotFound,
    StaleRevision,
    StoreUnavailable,
    // The caller tried to mutate intent from inside a gated mutation (a
    // lifecycle hook or collection applier). Nothing was committed.
    ReentrantMutation,
};

inline const char* to_string(CollectionError error)
{
    switch (error) {
        case CollectionError::None:              return "ok";
        case CollectionError::InvalidRequest:    return "invalid_request";
        case CollectionError::UnknownCollector:  return "unknown_collector";
        case CollectionError::NotFound:          return "not_found";
        case CollectionError::StaleRevision:     return "stale_revision";
        case CollectionError::ReentrantMutation: return "reentrant_mutation";
        case CollectionError::StoreUnavailable:  return "store_unavailable";
    }
    return "unknown";
}

struct CollectionRequest {
    std::vector<uint64_t> targets;
    // set_collectors: required, explicit [] means persistent stop.
    // reset_auto: must be absent.
    std::optional<std::vector<std::string>> collectors;
    // Optional binding revision guard, single-target requests only. It is
    // compared inside the store transaction, not in a caller-side lock.
    std::optional<uint64_t> expected_revision;
};

struct CollectionTargetOutcome {
    uint64_t internal_id = 0;
    std::string instance_id;
    // The accepted desired state after the command.
    CollectionSelection selection;
    // The record revision the command committed. Acceptance only: the runtime
    // converges asynchronously, so this is not proof the collectors attached.
    uint64_t revision = 0;
    // What the runtime actually holds right now, sampled after the commit. It
    // may still show the previous attachment; that is the honest answer.
    ActualCollectionState actual;
    bool changed = false;
    // The automatic decision was refused before the transaction (unknown
    // instance or invalid collector). Such a target is never "committed", even
    // when a previous valid revision exists.
    bool skipped = false;
};

struct CollectionOutcome {
    bool ok = false;
    CollectionError error = CollectionError::None;
    std::string message;
    std::vector<CollectionTargetOutcome> targets;  // exact targets on success
    size_t unchanged = 0;
    size_t skipped_manual_override = 0;
    size_t skipped_invalid = 0;
};

// Automatic policy result for one instance.
struct AutoSelection {
    uint64_t internal_id = 0;
    std::vector<std::string> collectors;
    std::string policy_revision;
};

// Construction options. Production uses the singleton (all defaults); tests
// and inventory-only service startup construct an isolated store without
// watchers, eBPF or RPC registration.
struct JobRegistryOptions {
    std::string db_path;                 // empty: job_registry_config.job_db_path
    // Start the inventory heartbeat (discovery adapters + maintenance) in the
    // constructor. Tests and store-only instances disable it.
    bool enable_inventory_maintenance = true;
    bool enable_pid_tracker = true;
    bool register_rpc = true;
    // --- heartbeat test seams (used instead of the config-driven adapters) --
    // Pre-built discovery sources to drive, with the adapter name each
    // observation belongs to. When non-empty, the configuration is not
    // consulted and automatic selection stays disabled.
    std::vector<std::pair<std::string, IDiscoverySource*>> test_sources;
    // With this flag the heartbeat runs without reading Config (test wiring;
    // combine with test_sources, or leave it empty for legacy-only upkeep).
    bool test_heartbeat = false;
    // Liveness probe overrides (empty = production /proc/cgroup probes).
    ProcessLivenessProbe test_process_probe;
    CgroupMembershipProbe test_cgroup_probe;
    // Observation -> typed facts override (empty = the registered adapters).
    std::function<Job(const Observation&)> test_facts_encoder;
    // Automatic-selection policy for test wiring (production builds it from
    // job_registry_config.inventory + <type>_job_watcher). The adapter name
    // below is the key it is armed under.
    std::function<AutoCollectorPolicy()> test_auto_policy;
    std::string test_auto_policy_adapter{"condor"};
};

class JobRegistry : private KernelAttribution::Host,
                    public IDrainCompletionSink,
                    public IJobRecordSource {
public:
    static JobRegistry& instance();          // 仍保留单例，方便迁移；也可由 main() 构造
    explicit JobRegistry(const JobRegistryOptions& options);
    ~JobRegistry();

    // 禁止拷贝
    JobRegistry(const JobRegistry&)            = delete;
    JobRegistry& operator=(const JobRegistry&) = delete;

    // Job 增删
    bool addJob(const Job& job);
    void delJob(uint64_t jobID);
    // Inventory-grade retirement: archive atomically (active -> history,
    // identity index and binding removed) and report durable success.
    bool retireInstance(uint64_t jobID);
    // 通过 sub_attr 匹配删除作业，返回删除的作业数量
    size_t delJob(const SubAttrMatcher& matcher);
    size_t closingJobCount() const;
    bool cleanupClosingJobs();
    // Read-only snapshot: no refresh, no PID pruning, no retirement.
    std::optional<Job> findJob(uint64_t jobID);
    // 通过 sub_attr 匹配查找作业，返回匹配的作业列表
    std::vector<Job> findJob(const SubAttrMatcher& matcher) const;
    std::vector<Job> snapshot() const;

    // 获取下一个全局唯一递增的JobID（durable 预留；失败返回 0）
    uint64_t getNextJobID();

    // 统一删除接口，通过 JSON 请求解析删除条件
    // 支持通过 JobID 或 sub_attr 匹配删除
    // 返回删除的作业数量
    size_t delJobByRequest(const nlohmann::json& req);

    // --- Inventory store facade --------------------------------------------
    // Identity-resolving idempotent upsert. Incomplete identities and
    // conflicting live generations are rejected explicitly and never indexed.
    // An upsert never fires lifecycle callbacks or starts collection: runtime
    // application is binding-driven.
    UpsertResult upsertObservedJob(const Job& job);
    std::optional<JobRecord> inventoryRecord(uint64_t jobID) const;
    std::optional<JobRecord> inventoryRecordByIdentity(
        const CanonicalIdentity& identity) const;
    std::vector<JobRecord> inventoryRecords() const;
    CollectionSelection selectionRecord(uint64_t jobID) const;
    // Optional collector-name gate. Unset accepts every non-empty name.
    void setCollectorValidator(std::function<bool(const std::string&)> validator);
    // operation = set: manual selection replacing any previous intent; []
    // stops collection. `collectors` must be present.
    CollectionOutcome setCollectors(const CollectionRequest& request);
    // operation = reset_auto: clear the manual override and let the recorded
    // automatic selection apply. `collectors` must be absent.
    CollectionOutcome resetAuto(const CollectionRequest& request);
    // Automatic policy decision for a batch of instances. Manual overrides,
    // including empty ones, are never overwritten.
    CollectionOutcome applyAutoPolicy(const std::vector<AutoSelection>& selections);
    // Test seam: replaces the kernel-cleanup step so the retirement ordering
    // (closing -> cleanup -> archive) can be exercised without BPF. Production
    // leaves it unset and uses KernelAttribution; it grants no way to write job
    // state.
    void setKernelCleanupForTest(
        std::function<bool(uint64_t, const Job&, std::string&)> cleanup);
    std::string instanceId(uint64_t jobID) const;
    StoreHealth storeHealth() const;

    // --- Collection runtime seam ------------------------------------------
    // The component that actually runs collectors (CollectorScheduler) is
    // injected at composition time. The registry never reaches for its
    // singleton, and it never calls into it on a source or RPC stack: a command
    // commits desired state, then only wakes the runtime.
    void setCollectionRuntime(ICollectionRuntime* runtime);
    ICollectionRuntime* collectionRuntime() const;
    // Coalesced wake of the installed runtime. Entered under the mutation gate:
    // a runtime that mutates intent from its wake callback is refused instead
    // of recursing back into the command path.
    void wake_runtime();
    // Actual state as the runtime reports it (untracked when no runtime).
    ActualCollectionState actualCollection(uint64_t jobID);
    // IDrainCompletionSink: the runtime proved the instance released every
    // collector. Runs checked kernel cleanup then the archive; a failure keeps
    // the Closing record retryable.
    void onDrained(uint64_t internal_id) override;
    // IJobRecordSource: the runtime's read surface for desired state.
    bool runtimeRecords(std::vector<JobRecord>& out) const override;
    bool runtimeRecord(uint64_t internal_id,
                       std::optional<JobRecord>& out) const override;
    // Re-read process/attribute facts for one stored job from the host and
    // persist them. Used by the heartbeat for records no discovery source
    // covers (legacy/manual registrations); reads never do this for
    // themselves.
    bool refreshJobFacts(uint64_t jobID);
    // Lifecycle state is a heartbeat decision, not a read side effect.
    void setInstanceState(uint64_t jobID, const std::string& state);
    // Liveness evidence from the last scan. Not a lifecycle state: an
    // unverified instance stays Open but must not be admitted to the kernel.
    void setExecutionVerified(uint64_t jobID, bool verified);

    // --- Lifecycle -----------------------------------------------------------
    // Stop discovery admission and polling. Must run before the collector
    // scheduler drains. Idempotent.
    void stopInventoryDiscovery();
    // Finalize retirements, stop the heartbeat, archive closing records and
    // close the store. Returns false when a closing record could not be
    // archived (the incomplete state stays persisted). Idempotent; the
    // destructor calls it as a fallback.
    bool deinit();

    // --- Inventory heartbeat (v2 §2.3) ---------------------------------------
    // One on-demand authoritative pass (the `ensure_registered` refresh
    // provider): scan -> upsert -> hint merge -> retirement -> reconciliation,
    // exactly what the periodic worker runs. Returns false when the heartbeat
    // is not running.
    bool reconcileInventoryNow();
    // Discovery event entries (the sink the adapters get, exposed for tests
    // and out-of-band producers). Cheap and bounded: they only enqueue.
    void onDiscoveryObservation(const Observation& observation);
    // Kernel exit hint entry (cheap; schedules a coalesced refresh).
    void notifyExit(pid_t pid);

    // Automatic selection entry point: record the decision as the instance's
    // automatic intent (manual overrides are never touched). Returns false
    // when the transaction was rejected.
    bool applyAutoSelection(uint64_t internal_id, const std::vector<std::string>& collectors,
                            const std::string& policy_revision);

    // Kernel attribution stats for the RPC surface.
    bool attributionRunning() const;
    uint64_t attributionPushedEvents() const;
    uint64_t attributionDroppedEvents() const;

    // Test seam only (see StoreFaultMode).
    void injectStoreFaultForTest(StoreFaultMode mode, size_t skip = 0, size_t count = 1);
    void clearStoreFaultsForTest();

private:
    JobRegistry();
    // Which wiring the heartbeat was assembled from: injected test seams or
    // the production configuration/adapter stack.
    enum class InventoryWiring { Disabled, Test, Production };
    // Decides WHICH discovery sources, probes and facts encoder the heartbeat
    // drives, once, before any source starts. Test wiring copies the
    // constructor-provided seams verbatim; production resolves the
    // configuration and builds one adapter per configured type.
    InventoryWiring resolve_inventory_components(const JobRegistryOptions& options);
    void init_inventory(const JobRegistryOptions& options);
    // Admit restored active records to kernel ownership through the single
    // admission path (a restarted process starts with an empty namespace).
    void admitRestoredOwners();
    void startDiscoverySources(InventoryWiring wiring);
    // Automatic selection for one newly created instance, evaluated outside
    // every registry lock (plan 6.2: Lua never runs under a lock).
    // Returns nullopt when no automatic decision was needed, true when the
    // decision (including an explicit reject) is durable, false otherwise.
    std::optional<bool> apply_auto_policy(const Job& job, const std::string& adapter);
    // Shared implementation of set/reset_auto. Validation happens before the
    // store call; the durable read-transform-write and its revision guard are
    // one store transaction.
    CollectionOutcome mutateSelection(const CollectionRequest& request, bool manual);
    bool validate_collectors(const std::vector<std::string>& collectors,
                             std::string& bad_name) const;
    // Record-driven repair of a missing initial automatic decision (plan §5.5):
    // the predicate is derived from durable state so a restart repairs too.
    bool missing_initial_decision(const JobRecord& record) const;
    std::optional<bool> repair_initial_decision(const JobRecord& record,
                                                UpsertResult& result);
    // The policy decision for one newly created instance, or nullopt when the
    // adapter has no enabled policy.
    struct AutoDecision {
        std::vector<std::string> collectors;
        std::string policy_revision;
    };
    std::optional<AutoDecision> autoSelectionFor(const Job& job, const std::string& adapter);
    // Evaluates the automatic policy for a set of live instances. Synchronous
    // by design: a slow rule set costs latency and is warned about, it is never
    // deferred to another thread (plan §1.1.3).
    void evaluateAutoPolicies(const std::vector<uint64_t>& internal_ids,
                              std::vector<AutoSelection>& out);
    void regRPCHandle();
    // Guards the sentinel job and the active-reader set.
    mutable std::shared_mutex mtx_;
    Job sentinel_job_;   // system JobID 0 (in-memory only, never stored)

    // The single durable representation. Everything below reads state through
    // it; the registry never duplicates a JobRecord field.
    std::unique_ptr<JobStore> store_;

    // Injected at composition time. Never held across a call into it.
    mutable std::mutex                     runtime_mtx_;
    ICollectionRuntime*                    collection_runtime_ = nullptr;
    // Guards the installed exit hint observer.
    mutable std::mutex                     exit_observer_mtx_;
    std::function<void(pid_t)>             exit_observer_;

    // Owned discovery/maintenance stack (construction order: store -> manager
    // -> heartbeat -> attribution -> RPC; destruction runs deinit()).
    // Adapters are built from the resolved configuration through the
    // SchedulerAdapterRegistry; the registry never names a scheduler.
    std::vector<std::unique_ptr<ISchedulerAdapter>> adapters_;

    // --- inventory heartbeat state (guarded by hb_mtx_ unless noted) --------
    struct HeartbeatHint {
        enum class Kind { Observation = 0, Exit, Refresh };
        Kind kind = Kind::Observation;
        Observation observation;
        pid_t pid = 0;
    };
    struct AbsenceEntry {
        ExecutionGeneration generation;
        int complete_absences = 0;
        std::int64_t last_seen_ms = 0;
    };
    mutable std::mutex hb_mtx_;
    std::condition_variable hb_cv_;
    std::deque<HeartbeatHint> hints_;
    std::map<uint64_t, AbsenceEntry> absences_;
    std::set<uint64_t> suspect_ids_;
    std::map<std::string, ExecutionGeneration> ended_generations_;
    std::vector<std::pair<uint64_t, ExecutionGeneration>> closing_generations_;
    bool hb_admitted_ = false;     // false after stopInventoryDiscovery()
    bool hb_running_ = false;      // worker thread alive
    bool hb_stopping_ = false;
    bool hint_wakeup_ = false;
    // Set by requestRescan() to cut the maintenance wait short (cooldown still
    // limits how often a forced scan actually runs).
    bool hb_wake_ = false;
    bool rescan_flag_ = false;
    bool degraded_ = false;
    bool incomplete_drain_ = false;
    bool status_published_ = false;
    bool last_scan_complete_ = false;
    std::int64_t last_scan_start_ms_ = 0;
    std::int64_t last_scan_success_ms_ = 0;
    std::int64_t last_forced_scan_ms_ = 0;
    std::string hb_last_error_;
    std::thread::id hb_worker_id_;
    std::uint32_t reconcile_interval_sec_ = 30;
    std::uint64_t hb_events_received_ = 0;
    std::uint64_t hb_events_dropped_ = 0;
    std::uint64_t hb_rescan_requests_ = 0;
    std::uint64_t hb_obsolete_hints_ = 0;
    std::uint64_t hb_incomplete_observations_ = 0;
    std::uint64_t hb_upsert_conflicts_ = 0;
    std::uint64_t hb_retirements_ = 0;
    std::uint64_t hb_passes_ = 0;
    std::uint64_t hb_stop_errors_ = 0;
    std::vector<std::pair<std::string, bool>> source_started_;  // name, started
    std::vector<std::string> source_start_notes_;
    // Serializes passes (worker vs on-demand reconciles). Never held while
    // taking hb_mtx_.
    std::mutex pass_mtx_;
    std::unique_ptr<std::thread> heartbeat_thread_;
    // Liveness probes (production or test-injected) and the observation ->
    // typed-facts encoder. Set once during init, read during passes.
    ProcessLivenessProbe process_probe_;
    CgroupMembershipProbe cgroup_probe_;
    std::function<Job(const Observation&)> facts_encoder_;
    // Every discovery source the heartbeat drives (adapters or test sources),
    // populated by resolve_inventory_components().
    std::vector<std::pair<std::string, IDiscoverySource*>> hb_sources_;
    DiscoverySink heartbeat_sink_;
    // Control-plane validation. Guarded because tests install it after
    // construction; the command path copies the callable under the lock and
    // never calls it while holding a store lock.
    mutable std::mutex selection_validation_mtx_;
    std::function<bool(const std::string&)> collector_validator_;
    // Automatic collector selection keyed by adapter name, armed from the
    // resolved inventory configuration. A disabled entry selects nothing.
    std::map<std::string, AutoCollectorPolicy> auto_policies_;
    bool deinit_done_{false};
    bool deinit_ok_{true};

    // Heartbeat internals.
    void startHeartbeat();
    void heartbeatLoop();
    bool runPass(bool do_scan, bool wait);
    void processObservation(const Observation& observation, std::set<uint64_t>& observed);
    Job toJob(const Observation& observation) const;
    void enqueueHint(HeartbeatHint hint);
    void requestRescan(const std::string& reason);
    bool rescanRequested() const;
    void refreshLegacyRecords();
    // Re-evaluates the automatic decision for live instances that never got a
    // durable one (plan §5.5). Bounded per pass.
    void repairMissingInitialDecisions();
    void evaluateRetirements(const std::set<uint64_t>& observed_ids,
                             const std::map<std::string, bool>& source_complete);
    bool retireIfConfirmed(const JobRecord& record, const std::string& reason);
    void rememberEnded(const JobRecord& record);
    bool isObsolete(const CanonicalIdentity& identity, const ExecutionGeneration& gen);
    void publishStatusProvider();
    void retractStatusProvider();
    void noteHeartbeatError(const std::string& error);
    nlohmann::json heartbeatStatusJson() const;
    void installHeartbeatSink();
    std::int64_t clockNow() const;
    void drainHints(std::deque<HeartbeatHint>& out);

    // Re-arms a drain for a restored Closing record and retries a cleanup that
    // failed after the runtime reported completion.
    void reconcileClosingRemovals();
    // Destructive post-drain step, in this order only: the record is already
    // closing (durable), then kernel ownership is removed with owner checks,
    // then the archive releases the identity index.
    bool cleanupThenArchive(uint64_t jobID);
    // Archival entry point for a record that is already closing. `allow_recovery`
    // is false on the shutdown pass: a single-pass shutdown never retries a
    // write whose outcome is unknown.
    bool finalizeClosingRemoval(uint64_t jobID, bool allow_recovery);
    // Verify every row whose last write lost certainty; false when the store
    // itself cannot recover.
    bool recoverStoreIfNeeded(std::string& error);
    // /proc + scheduler metadata refresh of one job (no persistence).
    void update_job(Job& job);

    // Kernel-side attribution capability (pid2job/cgroup2job). Owned here,
    // driven by lifecycle calls and its own reconcile thread.
    void init_pid_tracker();
    void stop_pid_tracker();
    std::unique_ptr<KernelAttribution> attribution_;
    // 内核登记入口：KernelAttribution 的生命周期调用薄封装（Added/Removed）。
    // Serializes activation against retirement so a stale snapshot cannot
    // revive a closing owner.
    bool sync_job_to_kernel(const Job& job);
    // Observation/startup repair: (re)installs the kernel projection of one
    // instance that is already active. The kernel component owns no queryable
    // "already installed" state, so this is an idempotent re-seed.
    void ensure_kernel_owner(const Job& job);
    std::mutex kernel_owner_mtx_;
    // Removes kernel ownership. The injected test seam may report a failure;
    // the production KernelAttribution API cannot, so it always reports clean.
    void remove_job_from_kernel(const Job& job, bool& ok, std::string& error);
    std::mutex kernel_cleanup_mtx_;
    std::function<bool(uint64_t, const Job&, std::string&)> kernel_cleanup_for_test_;
    // Serializes cleanup+archive so two retry paths cannot clean the same
    // closing record concurrently; the archive is idempotent under it.
    std::mutex closing_mtx_;
    // Instances whose runtime drain completed while cleanup/archive has not yet
    // succeeded. Retried by the maintenance pass; never re-armed.
    mutable std::mutex drain_mtx_;
    std::set<uint64_t> drain_complete_;
    // KernelAttribution::Host — the registry-side projection updates.
    bool attributionJobExists(uint64_t internal_id) override;
    void attributionJobAddPid(uint64_t internal_id, pid_t pid) override;
    void attributionJobRemovePid(uint64_t internal_id, pid_t pid) override;
    void attributionEnumerateJobs(const std::function<void(const Job&)>& fn) override;
    void attributionExitHint(pid_t pid) override;
    std::string attributionCgroupPath(uint64_t internal_id) override;
};
