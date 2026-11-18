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
// Discovery-adapter contract for the job registry.
//
// This header is the boundary between the scheduler discovery adapters
// (CondorJobWatcher, SlurmJobWatcher) and the registry heartbeat:
//
//   * enumerateManagers() - the authoritative pass: the live per-job managers
//                   of one source, its coverage and its health update
//   * observe(manager)    - O(job) targeted pass for one known manager
//   * health()    - SourceHealth of that source (initializing/healthy/degraded)
//   * observeForHint(pid) - O(job) targeted pass for one exec-event pid
//
// The header owns the sources and the manager-pass contract. The root-explicit
// proc/cgroup readers live in common/procfs.hpp, and this contract must never
// depend on LevelDB; the registration result type lives in its own contract
// header.
//
// Two rules are structural rather than conventional:
//
//   1. A discovered Job either is ready (resolvable identity plus a complete
//      execution manager) or is explicitly incomplete with a reason on the
//      execution group. There is no "best effort" middle state, and an
//      incomplete discovery is not eligible for automatic collection or for
//      retirement decisions.
//   2. A failed or incomplete pass is never interpreted as "everything else
//      ended". ManagerSet::complete says whether the pass covered the
//      configured sources; a consumer must check it before treating absence as
//      termination evidence.
#pragma once

#include "core/collector_type.h"
#include "core/job_identity.hpp"
#include "core/registration_contract.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

// ---------------------------------------------------------------------------
// Clock injection
// ---------------------------------------------------------------------------

// Wall-clock epoch milliseconds. Discovery adapters take a ScanClock so tests
// and the registry heartbeat can drive a deterministic clock.
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
    std::string clusterName;
};

// ---------------------------------------------------------------------------
// Pass notes
// ---------------------------------------------------------------------------

// Human-readable join of one pass' source notes, stored in
// SourceHealth::last_note. Notes are short diagnostics ("eBPF event source
// unavailable", "fell back to process-table enumeration"), so one field keeps
// them all visible instead of silently dropping every note but the first.
inline std::string join_source_notes(const std::vector<std::string>& notes)
{
    std::string joined;
    for (const auto& note : notes) {
        if (!joined.empty()) joined += "; ";
        joined += note;
    }
    return joined;
}

// ---------------------------------------------------------------------------
// Manager enumeration
// ---------------------------------------------------------------------------

// One discovery pass' manager inputs: the processes that supervise exactly one
// execution of this scheduler (Condor: condor_starter; Slurm: slurmstepd).
// Deliberately carries no Job: reading ads, environments and pid identity sets is the
// per-manager observe() step, so a pass can decide what is new before paying
// for any of it.
struct ManagerSet {
    std::vector<ProcessIdentity> managers;
    // True when the pass covered its inputs without an I/O or configuration
    // error. An individually unreadable manager (start ticks or boot id
    // missing) is a partial entry, not an incomplete pass. Consumers must never
    // read absence out of a ManagerSet with complete=false.
    bool complete = false;
    std::string error;
};

// ---------------------------------------------------------------------------
// Source health
// ---------------------------------------------------------------------------

// Initializing: no source pass has finished yet (startup barrier; section 5.1
// keeps source health here until the first pass completes or degrades).
// Healthy: the last pass covered its sources. Degraded: the last pass failed
// or only covered part of its sources; consumers keep retrying and must not
// treat absence as termination.
enum class SourceState {
    Initializing = 0,
    Healthy,
    Degraded,
};

struct SourceHealth {
    SourceState state = SourceState::Initializing;
    // True while the adapter owns a running poll thread.
    bool polling = false;
    // Epoch ms of the last authoritative pass; 0 means "never".
    std::int64_t last_scan_start_ms = 0;
    std::int64_t last_scan_success_ms = 0;
    // Most recent failure text; cleared by the next successful pass. `state` is
    // authoritative.
    std::string last_error;
    // Most recent pass' source-level notes, joined with "; " (e.g. "eBPF event
    // source unavailable"). Empty when the last pass had no notes.
    std::string last_note;
    // A consumer asked for an out-of-band pass; cleared when one starts.
    bool rescan_requested = false;
    std::size_t consecutive_failures = 0;
};

// ---------------------------------------------------------------------------
// Adapter runtime options and source interface
// ---------------------------------------------------------------------------

// A discovery adapter never reads Config, never loads eBPF and never starts a
// thread in its constructor: the assembly layer (JobRegistry::armDiscovery)
// builds these options explicitly and start() begins operation.
struct DiscoveryOptions {
    // Load the eBPF ring buffer used for incremental discovery hints. It is
    // always started by start(), never by the constructor.
    bool enable_event_source = true;
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

// Synchronous single-job registration entry: registers one
// discovered Job and returns the result. It runs on the caller's
// thread (the source's poll thread), never on a worker. The exec-event path
// registers through it; the registry's manager pass upserts its observe()
// results directly.
using JobSink = std::function<UpsertResult(const Job&)>;

// Receives discovery output from a source.
struct DiscoverySink {
    // Register one discovered Job immediately on the poll thread. The store
    // writes it without a fence, so the callback never waits for persistence.
    // Empty when this core has no discovery runtime.
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
    // for (no event source); the sink stays installed and
    // enumerateManagers()/observe()/observeForHint() keep working — discovery
    // just runs without exec-hint acceleration.
    virtual bool start(const DiscoverySink& sink) = 0;
    // Idempotent: stops and joins the poll thread, releases the event source.
    virtual void stop() = 0;

    // The authoritative manager enumeration of one pass: one entry per live
    // per-job manager, with no job ad and no pid identity reads. Called once per
    // registry pass. It owns the source health of that pass: it stamps
    // last_scan_start_ms, consumes a pending rescan request, and sets
    // Healthy/Degraded from its coverage. The caller then pays observe() only
    // for managers it does not track.
    virtual ManagerSet enumerateManagers() = 0;

    // The complete Job for one known manager: identity plus initial facts and
    // execution evidence, built without touching any other execution. Called
    // only for managers the caller does not already track. It never updates
    // SourceHealth: a manager that cannot be observed is a retry, not a pass
    // failure. nullopt means "cannot be observed" (process gone, PID reused,
    // discovery incomplete), never a guessed Job.
    virtual std::optional<Job> observe(const ProcessIdentity& manager) = 0;

    virtual SourceHealth health() const = 0;
    // Ask for an out-of-band authoritative pass; consumed by the owner.
    virtual void requestRescan() = 0;
    // Targeted observation for one exec-event pid. The BPF filter guarantees
    // the parent is the execution manager, so the manager is the ppid; only
    // this execution's files are read (O(job), not O(node)). `hint_ppid` is
    // the exec event's own ppid: when positive (event objects that carry the
    // field) the /proc ppid read is skipped; when zero (old BPF objects) the
    // hint's stat is read instead. Returns nullopt when the hint cannot be
    // resolved (process gone, parent is not the expected manager); discovery
    // then waits for the next authoritative manager pass.
    virtual std::optional<Job> observeForHint(pid_t hint_pid, pid_t hint_ppid = 0) = 0;
};

// Process-monotonic read order, assigned before a builder starts reading. The
// store uses it so an older observation can never overwrite a newer event.
std::uint64_t next_discovery_read_order();

// Process identity from the fake or real procfs (see common/procfs.hpp).

