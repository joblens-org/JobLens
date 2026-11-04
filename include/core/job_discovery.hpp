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
// Discovery-adapter observation model for the job-inventory/collection
// decoupling (doc/job_inventory_collection_decoupling_plan.md sections 3.1,
// 4.1 and 5.2).
//
// This header is the contract between the scheduler discovery adapters
// (CondorJobWatcher, SlurmJobWatcher) and the inventory maintenance
// controller:
//
// Discovery contract (renamed from job_observation when registration became a
// Job-based synchronous path; plan §5.1). The header owns source and scan —
// the root-explicit proc/cgroup readers live in common/procfs.hpp — and it
// must never depend on LevelDB; the registration result type lives in its own
// contract header.
//
//   * scan()      - one synchronous source pass, returns ScanResult
//   * health()    - SourceHealth of that source (initializing/healthy/degraded)
//   * jobs        - discovered Jobs (typed facts + execution evidence group)
//                   the controller admits through JobRegistry upserts; never a
//                   JobRegistry dependency of its own
//
// Two rules are structural rather than conventional:
//
//   1. A discovered Job either is ready (resolvable identity plus a complete
//      execution generation) or is explicitly incomplete with a reason on the
//      execution group. There is no "best effort" middle state, and an
//      incomplete discovery is not eligible for automatic collection or for
//      retirement decisions.
//   2. A failed or incomplete scan is never interpreted as "everything else
//      ended". ScanResult::complete says whether the pass covered the
//      configured sources; a consumer must check it before treating absence as
//      termination evidence.
#pragma once

#include "core/collector_type.h"
#include "core/job_identity.hpp"
#include "core/registration_contract.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

// ---------------------------------------------------------------------------
// Clock injection
// ---------------------------------------------------------------------------

// Wall-clock epoch milliseconds. Discovery adapters take a ScanClock so tests
// and the maintenance controller can drive a deterministic clock.
std::int64_t now_epoch_ms();
using ScanClock = std::function<std::int64_t()>;

inline std::int64_t scan_clock_now(const ScanClock& clock)
{
    return clock ? clock() : now_epoch_ms();
}

// ---------------------------------------------------------------------------
// Discovery inputs
// ---------------------------------------------------------------------------

// Where an adapter reads its discovery inputs. The defaults are the real host
// paths; every root is overridable so unprivileged fixtures can present a fake
// procfs/cgroup tree instead of requiring root or live schedulers.
struct DiscoveryEnvironment {
    // procfs root, e.g. "<fixture>/proc". Contains <pid>/{stat,cmdline,environ,cgroup}
    // and sys/kernel/random/boot_id.
    std::string proc_root{"/proc"};
    // cgroup v2 mount root used to turn a procfs "0::/path" line into an
    // absolute cgroup directory ("<cgroup_root>/path/cgroup.procs").
    std::string cgroup_root{"/sys/fs/cgroup"};
    // Optional slurmd spool root ("/var/spool/slurmd"), holding "job<id>"
    // directories the daemon creates per running job on the node. Empty
    // disables the spool source; it can only corroborate presence, never
    // provide process evidence.
    std::string slurm_spool_dir;
    // Resolved scheduler cluster namespaces. Empty means "derive from the
    // source itself" (Condor: COLLECTOR_HOST, Slurm: SLURM_CLUSTER_NAME).
    // Neither value is ever guessed from a native job number.
    std::string cluster_name;
};

// ---------------------------------------------------------------------------
// Scan result
// ---------------------------------------------------------------------------

// Counts are all "inputs of this source pass", not "jobs in the registry".
struct ScanCounts {
    // Manager-level discovery inputs examined (one Condor starter, one
    // slurmstepd, one spool directory).
    std::size_t candidates = 0;
    std::size_t complete = 0;
    std::size_t incomplete = 0;
    // Process or spool entries that could not be read or attributed at all
    // (permission denied, exited mid-scan, not a job<id> directory). They never
    // become observations and never stand in for the observed absence of a
    // known execution.
    std::size_t skipped = 0;

    std::size_t observed() const { return complete + incomplete; }
};

struct ScanResult {
    // The discovered Jobs: typed facts plus the execution evidence group,
    // built where the source parsed its inputs. JobID, JobPIDs and
    // CollectorNames stay empty: they are store/intent owned.
    std::vector<Job> jobs;
    // True when the pass covered every configured source without an I/O or
    // configuration error. Incomplete *discoveries* (missing ad, unresolved
    // namespace) do not make a pass incomplete: they were still observed.
    // Consumers must never read absent jobs out of a pass with complete=false.
    bool complete = false;
    std::string error;
    ScanCounts counts;
    // Source-level notes: skipped sources, unavailable eBPF event source, ...
    std::vector<std::string> notes;
    std::int64_t started_at_ms = 0;
    std::int64_t finished_at_ms = 0;

    bool ok() const { return error.empty(); }
};

// ---------------------------------------------------------------------------
// Source health
// ---------------------------------------------------------------------------

// Initializing: no source pass has finished yet (startup barrier; section 5.1
// keeps inventory health here until the first pass completes or degrades).
// Healthy: the last pass covered its sources. Degraded: the last pass failed
// or only covered part of its sources; consumers keep retrying and must not
// treat absence as termination.
enum class SourceState {
    Initializing = 0,
    Healthy,
    Degraded,
};

const char* to_string(SourceState state);

struct SourceHealth {
    SourceState state = SourceState::Initializing;
    // True while the adapter owns a running poll/scan thread.
    bool polling = false;
    // Epoch ms; 0 means "never".
    std::int64_t last_scan_start_ms = 0;
    std::int64_t last_scan_success_ms = 0;
    // Most recent failure text. Kept after recovery for diagnosis; `state` is
    // authoritative.
    std::string last_error;
    // Most recent source-level note (e.g. "eBPF event source unavailable").
    std::string last_note;
    // A consumer asked for an out-of-band pass; cleared when one starts.
    bool rescan_requested = false;
    std::size_t consecutive_failures = 0;
};

// ---------------------------------------------------------------------------
// Adapter runtime options and source interface
// ---------------------------------------------------------------------------

// A discovery adapter never reads Config, never loads eBPF and never starts a
// thread in its constructor: the assembly layer (JobRegistry::init_inventory)
// builds these options explicitly and start() begins operation.
struct DiscoveryOptions {
    // Load the eBPF ring buffer used for incremental discovery hints. It is
    // always started by start(), never by the constructor.
    bool enable_event_source = true;
    // Periodic scan tick of the adapter's poll thread; 0 disables it (the
    // maintenance controller owns pacing and runs the authoritative passes).
    std::uint32_t scan_interval_ms = 0;
    // Condor tier-1 starter enumeration: path of the slot cgroup subtree
    // RELATIVE to discovery.cgroup_root (verified layout: the startd keeps
    // one directory per slot under <cgroup_root>/system.slice/htcondor, each
    // listing that slot's processes in cgroup.procs). Empty, unreadable or
    // starter-less subtrees fall back to the full process-table scan. Slurm
    // does not use this field.
    std::string starter_cgroup_subtree;

    DiscoveryEnvironment discovery;
    ScanClock clock;
};

// Synchronous single-job registration entry (plan §3/§5.1): registers one
// discovered Job and returns the result. It runs on the caller's
// thread (the source's poll thread), never on a worker. Both discovery
// paths — exec events and the source's own periodic scan — use it.
using JobSink = std::function<UpsertResult(const Job&)>;

// Receives discovery output from a source.
struct DiscoverySink {
    // Register one discovered Job immediately on the poll thread. The store
    // writes it without a fence, so the callback never waits for persistence.
    // Empty when this core has no inventory runtime.
    JobSink on_job;
};

// One discovery source for one scheduler type. Implemented by the
// watchers; the registry drives sources through this interface and owns no
// adapter details. The adapter NAME (see name()) is the scheduler identity —
// there is no per-scheduler enum to implement.
class IDiscoverySource {
public:
    virtual ~IDiscoverySource() = default;

    virtual const std::string& name() const = 0;

    // Installs the sink and then starts polling (the sink exists before any
    // callback can run). Returns false when the adapter has nothing to poll
    // for (no event source and no scan interval); the sink stays installed and
    // scan()/observeForHint() keep working.
    virtual bool start(const DiscoverySink& sink) = 0;
    // Idempotent: stops and joins the poll thread, releases the event source.
    virtual void stop() = 0;
    // One synchronous authoritative source pass.
    virtual ScanResult scan() = 0;
    virtual SourceHealth health() const = 0;
    // Ask for an out-of-band authoritative pass; consumed by the owner.
    virtual void requestRescan() = 0;
    // Targeted observation for one exec-event pid. The BPF filter guarantees
    // the parent is the execution manager, so the manager is the ppid; only
    // this execution's files are read (O(job), not O(node)). `hint_ppid` is
    // the exec event's own ppid: when positive (event objects that carry the
    // field) the /proc ppid read is skipped; when zero (old BPF objects) the
    // hint's stat is read instead. Returns nullopt when the hint cannot be
    // resolved (process gone, parent is not the expected manager); the caller
    // then falls back to a full scan.
    virtual std::optional<Job> observeForHint(pid_t hint_pid, pid_t hint_ppid = 0) = 0;
};

// Process-monotonic read order, assigned before a builder starts reading. The
// store uses it so an older scan can never overwrite a newer event (plan §4.1).
std::uint64_t next_discovery_read_order();

// Process identity from the fake or real procfs (see common/procfs.hpp).

