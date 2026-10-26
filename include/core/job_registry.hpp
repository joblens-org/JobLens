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
// job_registry.h
#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <functional>
#include <optional>
#include <string>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <thread>
#include <atomic>


#include "common/streamer_watcher.hpp"
#include "core/collector_type.h"
#include "core/job_identity.hpp"
#include "job_lifecycle_event.h"
#include "ebpf/trace_condor_starter.h"
#include "common/ebpf_common.hpp"
#include "core/job_pid_tracker.hpp"
#include "core/job_auto_policy.hpp"

#include "job_watcher/condor_job_watcher.hpp"
#include "job_watcher/slurm_job_watcher.hpp"

class JobCollectionManager;
class JobInventoryMaintenance;

// ---------------------------------------------------------------------------
// Inventory-grade storage (doc/job_inventory_collection_decoupling_plan.md)
//
// LevelDB namespaces:
//   j:  active inventory facts (legacy namespace, additive schema-2 fields)
//   h:  archived history (legacy namespace, final binding summary)
//   c:  durable allocator high-water mark (legacy namespace)
//   m:  schema version, Agent DB UUID, migration state
//   i:  canonical identity key -> internal ID
//   b:  per-instance collection binding intent
// ---------------------------------------------------------------------------

// Construction options. Production uses the singleton (all defaults); tests
// and inventory-only service startup construct an isolated store without
// watchers, eBPF or RPC registration (plan section 5.1).
struct JobRegistryOptions {
    std::string db_path;                 // empty: job_registry_config.job_db_path
    // Start the inventory stack (watchers + maintenance controller) in the
    // constructor. Tests and store-only instances disable it.
    bool enable_inventory_maintenance = true;
    bool enable_pid_tracker = true;
    bool register_rpc = true;
    // Test seam: stop a legacy migration after this many durable batches and
    // leave it resumable. 0 = unlimited.
    size_t migration_batch_limit = 0;
};

enum class UpsertDisposition {
    Created,
    Updated,
    Unchanged,
    Rejected,
    Conflict,
};

// One scheduler observation. `facts` carries the typed scheduler attributes;
// its JobID and CollectorNames are deliberately ignored: inventory upserts
// never allocate a caller-chosen ID and never activate collection.
//
// `members_complete` mirrors the discovery flag: true means the process list
// is authoritative, an empty list included ("read succeeded, no members");
// false means the member set is only partially known, so an empty list must
// never erase stored members.
struct ObservedExecution {
    NativeSelector selector;
    ExecutionGeneration generation;
    std::vector<ProcessIdentity> processes;
    bool members_complete = true;
    Job facts;
    int64_t observed_at_us = 0;          // 0 = now
};

struct InventoryRecord {
    uint64_t internal_id = 0;
    std::string instance_id;             // opaque handle derived from DB UUID
    NativeSelector selector;             // original wire identity (verbatim)
    CanonicalIdentity identity;          // valid only when identity_complete
    bool identity_complete = false;
    ExecutionGeneration generation;
    std::vector<ProcessIdentity> processes;
    uint64_t facts_revision = 0;
    bool quarantined = false;
    std::string state = "active";        // active | suspect | closing
    int64_t first_seen_us = 0;
    int64_t last_seen_us = 0;
};

struct UpsertResult {
    bool ok = false;
    UpsertDisposition disposition = UpsertDisposition::Rejected;
    uint64_t internal_id = 0;
    std::string instance_id;
    uint64_t facts_revision = 0;
    std::string error;                   // stable code
    std::string message;
};

enum class CollectionMode {
    Auto,
    Manual,
};

inline const char* to_string(CollectionMode mode)
{
    return mode == CollectionMode::Manual ? "manual" : "auto";
}

std::optional<CollectionMode> collection_mode_from_string(const std::string& raw);

// Per-instance collection intent. Revisions are monotonically increasing
// per instance; revision 0 means "no intent recorded yet" (auto, empty).
struct BindingRecord {
    uint64_t internal_id = 0;
    uint64_t revision = 0;
    CollectionMode mode = CollectionMode::Auto;
    std::vector<std::string> auto_collectors;
    std::vector<std::string> manual_collectors;
    std::string policy_revision;

    // effective_collectors = manual if mode == manual, else auto. An explicit
    // empty manual list is a persistent stop and must win over auto.
    std::vector<std::string> effectiveCollectors() const;
};

// Runtime state of a committed binding. Intent is owned by the store; applied
// state is owned by whatever consumes it (CollectorScheduler). An Agent with no
// collection runtime reports `applied=false` and an explicit error instead of
// echoing the desired list back as if it were in effect.
struct AppliedCollection {
    bool applied = false;
    uint64_t revision = 0;
    std::vector<std::string> collectors;
    std::string error;
};

struct StoreHealth {
    bool db_running = false;
    bool migration_complete = false;
    std::string migration_state;         // empty | in_progress:<stage> | complete
    uint32_t schema_version = 0;
    std::string db_uuid;
    bool degraded = false;
    uint64_t write_failures = 0;
    std::string last_write_error;
    uint64_t incomplete_identities = 0;
    uint64_t quarantined_identities = 0;
    uint64_t corrupt_records = 0;
    uint64_t active_instances = 0;
};

// Test-only fault injection. The next durable control-plane writes at
// `skip` offset (0 = the very next one) fail. FailBeforeWrite models a LevelDB
// write that never landed; FailAfterWrite models the ambiguous case where the
// batch landed but the status reported failure. Either way the mutation is
// reported as failed and store health records the error.
enum class StoreFaultMode {
    None,
    FailBeforeWrite,
    FailAfterWrite,
};

class JobRegistry {
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
    // Finish the destructive cleanup after scoped collector work has drained.
    // Returns false when at least one closing record could not be archived.
    bool acknowledgeJobRemoval(uint64_t jobID);
    size_t closingJobCount() const;
    bool cleanupClosingJobs();
    // Read-only snapshot: no refresh, no PID pruning, no retirement.
    std::optional<Job> findJob(uint64_t jobID);
    // 通过 sub_attr 匹配查找作业，返回匹配的作业列表
    std::vector<Job> findJob(const SubAttrMatcher& matcher) const;
    std::vector<Job> snapshot() const;

    // 生命周期回调注册
    void addLifecycleCb(JobLifecycleCb cb);

    // 获取下一个全局唯一递增的JobID（durable 预留；失败返回 0）
    uint64_t getNextJobID();

    // 统一删除接口，通过 JSON 请求解析删除条件
    // 支持通过 JobID 或 sub_attr 匹配删除
    // 返回删除的作业数量
    size_t delJobByRequest(const nlohmann::json& req);

    // --- Inventory store ---------------------------------------------------
    // Identity-resolving idempotent upsert. Incomplete identities and
    // conflicting live generations are rejected explicitly and never indexed.
    // An upsert never fires lifecycle callbacks or starts collection: runtime
    // application is binding-driven (task 4).
    UpsertResult upsertObservedJob(const ObservedExecution& observation);
    std::optional<InventoryRecord> inventoryRecord(uint64_t jobID) const;
    std::optional<InventoryRecord> inventoryRecordByIdentity(
        const CanonicalIdentity& identity) const;
    std::vector<InventoryRecord> inventoryRecords() const;
    BindingRecord bindingRecord(uint64_t jobID) const;
    // One durable transaction for every submitted binding. All targets must
    // exist; on any write failure nothing is committed and `error` is set.
    bool commitBindings(const std::vector<BindingRecord>& bindings, std::string& error);
    bool hasInstance(uint64_t jobID) const;
    std::string instanceId(uint64_t jobID) const;
    StoreHealth storeHealth() const;
    bool migrationComplete() const { return migration_complete_; }

    // --- Collection runtime seam ------------------------------------------
    // The component that actually runs collectors (CollectorScheduler) installs
    // this sink. It is called outside every registry lock, after the durable
    // intent transaction, and reports which collectors really took effect.
    using CollectionApplier = std::function<bool(
        const Job& job, std::vector<std::string>& applied_collectors, std::string& error)>;
    void setCollectionApplier(CollectionApplier applier);
    // Last runtime application result for one instance.
    AppliedCollection appliedCollection(uint64_t jobID) const;
    // Converges one instance's runtime to its current durable binding. A call
    // is a no-op when the applied state already matches (same revision, same
    // collector set, applied); otherwise the binding is applied again — this is
    // the retry path for a failed application and the reconciliation path for
    // state that drifted (a crash between intent and runtime, a runtime that
    // started late). Cheap enough to run every maintenance pass.
    void reconcileCollectionIntent(uint64_t jobID);
    // Re-read process/attribute facts for one stored job from the host and
    // persist them. Used by maintenance for records no discovery source covers
    // (legacy/manual registrations); reads never do this for themselves.
    bool refreshJobFacts(uint64_t jobID);
    // Lifecycle state is a maintenance decision, not a read side effect.
    void setInstanceState(uint64_t jobID, const std::string& state);
    // Cheap hint sink for kernel exit events. The maintenance controller uses it
    // to schedule a coalesced refresh; the callback must not do I/O.
    void setExitObserver(std::function<void(pid_t)> observer);

    // --- Lifecycle -----------------------------------------------------------
    // Plan 5.3: stop discovery admission and polling. Must run before the
    // collector scheduler drains. Idempotent.
    void stopInventoryDiscovery();
    // Finalize retirements, stop the pid tracker, archive closing records and
    // close the store. Returns false when a closing record could not be
    // archived (the incomplete state stays persisted). Idempotent; the
    // destructor calls it as a fallback.
    bool deinit();

    // Automatic selection entry point: record the decision as the instance's
    // automatic intent (manual overrides are never touched). Returns false
    // when the transaction was rejected.
    bool applyAutoSelection(uint64_t internal_id, const std::vector<std::string>& collectors,
                            const std::string& policy_revision);

    // Test seam only (see StoreFaultMode).
    void injectStoreFaultForTest(StoreFaultMode mode, size_t skip = 0, size_t count = 1);
    void clearStoreFaultsForTest();

private:
    JobRegistry();
    bool run_migration();
    bool migrate_allocator();
    bool migrate_index(std::string& cursor, size_t& batches, size_t batch_limit);
    bool migrate_finalize();
    bool resume_migration_state(std::string& stage, std::string& cursor);
    bool load_or_create_db_uuid();
    void restore_records_from_db();
    // Construction step 3: resolve the inventory configuration, build exactly
    // one discovery watcher per configured type, start the maintenance
    // controller (initial scan included) and arm the automatic-selection
    // inputs. Invalid configuration degrades to "no inventory" with an error
    // log instead of aborting the service.
    void init_inventory();
    // Automatic selection for one newly created instance, evaluated outside
    // every registry lock (plan 6.2: Lua never runs under a lock).
    void apply_auto_policy(const Job& job, SchedulerType type);
    void regRPCHandle();
    inline void update_job(Job& job);
    mutable std::shared_mutex              mtx_;
    std::unordered_map<uint64_t, Job>           jobs_;   // key = JobID
    // Removed but not yet acknowledged by the collector lanes. The snapshot
    // keeps attribution alive for accepted tail input.
    std::unordered_map<uint64_t, Job>       closing_jobs_;
    std::unordered_map<uint64_t, InventoryRecord> records_;
    std::unordered_map<std::string, uint64_t>     identity_ids_;
    std::vector<JobLifecycleCb>            cbs_;
    // Runtime application results, guarded by mtx_.
    std::unordered_map<uint64_t, AppliedCollection> applied_collections_;
    // Serializes inventory admission (identity lookup -> allocate -> commit).
    std::mutex                             upsert_mtx_;
    // Guards the installed collection applier. Never held while it runs.
    mutable std::mutex                     applier_mtx_;
    CollectionApplier                      collection_applier_;
    // Serializes collection-runtime applications. Ordering rule: every applier
    // call reads the current durable binding inside this section, so the last
    // application always reflects the latest committed intent — an older
    // caller can never overwrite a newer binding's runtime state.
    std::mutex                             apply_mtx_;
    // Guards the installed exit hint observer.
    mutable std::mutex                     exit_observer_mtx_;
    std::function<void(pid_t)>             exit_observer_;

    // Owned discovery/maintenance stack (construction order: db -> manager ->
    // inventory -> pid tracker -> RPC; destruction runs deinit()).
    std::unique_ptr<CondorJobWatcher> condor_source_;
    std::unique_ptr<SlurmJobWatcher> slurm_source_;
    std::unique_ptr<JobInventoryMaintenance> inventory_;
    std::shared_ptr<JobCollectionManager> collection_manager_;
    // Automatic collector selection per scheduler type, armed from the
    // resolved inventory configuration. A disabled entry selects nothing.
    std::map<SchedulerType, AutoCollectorPolicy> auto_policies_;
    bool deinit_done_{false};
    bool deinit_ok_{true};

    // 采集JobInfo持久化
    bool addJob(const Job& job, bool from_db);
    bool init_job_db(const JobRegistryOptions& options);
    void deinit_job_db();
    bool persist_new_job(const Job& job);
    bool update_job_info(const Job& job);
    bool end_job_in_db(uint64_t jobID);
    bool finishJobRemoval(const Job& job);
    bool db_running{false};
    leveldb::DB* job_db = nullptr;    // LevelDB 实例，通过 delete 释放
    mutable std::mutex db_mtx_;         // 序列化job_db的并发事务操作

    // Durable control-plane write. Caller must hold db_mtx_. Always uses
    // `sync=true`; returns the effective status (including injected faults).
    leveldb::Status write_batch_locked(leveldb::WriteBatch& batch, const char* operation);
    void record_store_failure(const std::string& message);
    void record_store_success();
    bool store_ready() const;
    uint64_t allocate_internal_id(std::string& error);
    void note_record_in_memory(const InventoryRecord& meta, const Job& job);
    // Lifecycle fan-out copied under the lock and invoked without it, so a
    // callback can never re-enter the registry while it is locked.
    std::vector<JobLifecycleCb> lifecycle_callbacks() const;
    void broadcast_lifecycle(JobEvent event, const Job& job);
    // Calls the installed collection applier for one job and records the
    // runtime result. The binding is re-read and the desired set derived from
    // it *inside* a serialized section (apply_mtx_), so two concurrent
    // applications always converge on the latest durable intent instead of
    // letting a stale fact-update overwrite a newer binding. Never called with
    // a registry lock held.
    void applyLatestCollectionIntent(uint64_t internal_id);
    // Durable lifecycle-state write for one record (the closing transition).
    // In-memory state changes alone cannot survive a crash during the drain.
    bool persist_record_state(uint64_t jobID, const std::string& state);

    // JobID计数器相关
    void init_job_id_counter();      // 初始化计数器（从数据库加载最大值）
    uint64_t next_job_id_{1};        // 下一个可用的JobID，从1开始
    bool job_id_exhausted_{false};
    std::mutex job_id_mtx_;          // JobID计数器的互斥锁

    // Store metadata
    std::string db_uuid_;
    uint32_t schema_version_{0};
    bool migration_complete_{false};
    std::string migration_state_;
    size_t migration_batch_limit_{0};
    std::atomic<bool> store_degraded_{false};
    std::atomic<uint64_t> store_write_failures_{0};
    uint64_t corrupt_records_{0};
    std::string last_store_error_;   // guarded by db_mtx_
    StoreFaultMode fault_mode_{StoreFaultMode::None};
    size_t fault_skip_{0};
    size_t fault_count_{0};

    // 共享 pid2job / cgroup2job 维护: JobPidTracker 加载生命周期追踪 bpf,
    // JobRegistry 通过它向内核登记 Job 归属并消费 fork/exit 事件。
    void init_pid_tracker();
    void stop_pid_tracker();
    void sync_job_to_kernel(Job& job);            // Added: 写 cgroup2job + 种子 pid2job
    void remove_job_from_kernel(const Job& job);  // Removed: 删 cgroup2job + 清 pid2job 残留
    void on_kernel_fork(uint32_t pid, uint64_t job_id);  // ringbuf FORK: 给 Job 加 pid
    void on_kernel_exit(uint32_t pid, uint64_t job_id);  // ringbuf EXIT: 从 Job 删 pid
    void reconcile_loop();                         // 低频对账, 防 ringbuf 丢事件导致漂移
    std::unique_ptr<JobPidTracker> pid_tracker_;
    std::atomic<bool> reconcile_running_{false};
    std::unique_ptr<std::thread> reconcile_thread_;
    // 上一次对账时的 ringbuf 丢弃计数, 用于只上报增量而非每次重复告警。
    uint64_t last_dropped_events_{0};
};
