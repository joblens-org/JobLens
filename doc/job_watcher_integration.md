# Job Watcher Integration Guide

**Status:** Implemented on `feature/collector-output-refactor` (2026-10-29,
commits 76dc9ea..): scheduler identity is the adapter name end to end —
`SchedulerType`/`InventoryType` enums deleted, native-id rules and config
sections self-register/by-convention, facts ride the `SubAttr` JSON
alternative for post-built-in schedulers. See the "File touch list" — it now
contains only the watcher's own files. Chinese version:
[job_watcher_integration.zh-CN.md](job_watcher_integration.zh-CN.md)

This guide describes how to add a **new scheduler type** to the job inventory —
for example an `ssh` session source — by writing one *job watcher* (a discovery
source) and one *scheduler adapter*. Everything downstream (the `JobStore`
merge semantics, the heartbeat's retirement rules, the collection binding and
reconciliation, the RPC surface, kernel attribution) works for the new type
without modification.

Reference implementation: `condor` / `slurm`
([include/job_watcher/](../include/job_watcher/)). The imagined example below
is `ssh`, the same case the architecture doc uses to demonstrate the
extension point.

---

## 1. What you are adding

Exactly one scheduler type costs you **two classes and one registration line**,
plus a handful of mechanical shared-code edits that the current config layer
still hardcodes (§4 Step 1):

```
include/job_watcher/ssh_job_watcher.hpp   # IDiscoverySource impl (the watcher)
include/job_watcher/ssh_adapter.hpp       # ISchedulerAdapter impl + REGISTER line
src/ebpf/trace_ssh_session.bpf.c          # optional: exec-hint event source
```

What you get for free:

- durable storage (`JobRecord`), identity indexing, generation conflicts;
- the heartbeat: authoritative scans, absence tracking, retirement evidence
  rules, legacy-row refresh;
- desired-vs-applied collection reconciliation and `CollectionStatus`;
- `JobRegistry/*` and `JobCollection/*` RPC methods and the trigger proxy;
- kernel attribution (`pid2job` / `cgroup2job`) **iff** your adapter declares
  the `kernel_attribution` trait;
- an unprivileged test path (fixture file trees, injected probes/sources).

## 2. How the line works (recap)

1. **Registration (before `main()`):** `REGISTER_SCHEDULER_ADAPTER("ssh",
   SshSchedulerAdapter)` at TU scope inserts a factory into
   `SchedulerAdapterRegistry` — same convention as `AUTO_REGISTER_COLLECTOR`.
2. **Assembly (`JobRegistry::resolve_inventory_components()`, invoked from
   `init_inventory()`):** the resolved config's
   `inventory.types` entries are built through the registry, one adapter per
   type. The adapter is the *only* component that knows the scheduler's
   attribute schema, identity format and capabilities.
3. **Runtime:** the heartbeat drives the adapter's `IDiscoverySource`:
   periodic authoritative `scan()` passes, plus O(job) `observeForHint(pid)`
   targeted passes fed by exec-event hints from the eBPF ring buffer. Every
   discovered **Job** carries its typed facts and one execution evidence
   group, built by the watcher where it parses its sources; the registry
   derives selector/identity on admission. There is no second envelope.

Two structural rules govern the whole line:

- A discovered **Job** either is ready (resolvable canonical identity plus a
  complete execution generation — `discovery_job_ready`) or is *explicitly
  incomplete* with reasons on `job.execution->incomplete_reason`. There is no
  best-effort middle state.
- A **failed or partial scan is never "everything else ended"**:
  `ScanResult::complete` says whether the pass covered its sources; consumers
  must check it before treating absence as termination evidence.

### The exec fast path (job start → collection)

Latency-critical discovery never waits for a heartbeat pass:

1. The watcher's poll thread wakes the moment the kernel delivers the exec
   event (`ring_buffer__poll` blocks inside libbpf's epoll; no polling slice
   in between).
2. The poll thread resolves the pid itself (`observeForHint`) and registers
   the discovered Job **synchronously on the same thread** through
   `sink.on_job(job)`: store upsert → lifecycle/attach. No queue, no worker;
   the heartbeat keeps scans, retirement and reconciliation only.
3. Scheduler state may not be written yet at exec time (job ad, spool dir):
   an **incomplete discovery never creates a record**; the watcher requests
   one coalesced authoritative rescan and the next pass converges it in tens
   of milliseconds.
4. `JobRegistry/inventory_status` keeps `exec_ingest_depth`,
   `queue_depth` and `events_dropped` as deprecated constant-zero keys:
   the ingest worker and the observation queue no longer exist.

Deliberately **not** solved here (layer 2, needs its own design): events
between the process's first instruction and the user-space `pid2job` seeding
are still invisible to the syscall-tracepoint collectors. Seeding a
provisional `pid2job` entry from the exec tracer itself would close that
window — but it decouples capture start from collector attach and therefore
changes the collector lifecycle contract.

## 3. Contracts your watcher must honor

### 3.1 `IDiscoverySource` lifecycle

| Method | Contract |
|---|---|
| ctor | Takes `DiscoveryOptions` only. **No Config access, no eBPF load, no thread.** An adapter that is never started costs nothing. |
| `start(sink)` | Installs the sink **before** anything can call back, then loads the event source on demand and starts polling. Returns `false` when there is nothing to poll (no event source and no scan interval) — `scan()` / `observeForHint()` keep working. |
| `stop()` | Idempotent; joins the poll thread and releases the event source. Bounded latency (slice-based wait, never an unbounded join). |
| `scan()` | One synchronous authoritative pass. Serialized internally (`scan_mutex_`). |
| `health()` | `Initializing` until the first pass finishes; `Healthy` when the last pass covered its sources; `Degraded` on failure/partial coverage. Consumers must not treat absence as termination while degraded. |
| `requestRescan()` | Sets a flag the owner consumes; never scans inline. |
| `observeForHint(pid)` | Targeted pass for one exec-event pid — O(job), only this execution's files. Return `nullopt` when unresolvable (process gone, parent mismatch); the caller falls back to a full scan. |

### 3.2 Threading and the sink

- The poll thread drains the ring buffer, resolves each pid into a Job
  (`observeForHint`) and registers it through `sink.on_job`. Registration
  writes without a fence, so the callback never waits for persistence — but
  it does the discovery I/O and the upsert inline, on purpose.
- Sink callbacks are caller-owned code running on *your* thread: wrap them in
  try/catch so a throwing consumer cannot take the watcher down.
- Discovery I/O (`/proc`, ad/spool reads) happens where the Job is built:
  `scan()` on the heartbeat worker, `observeForHint()` on the poll thread.

### 3.3 Building a discovered `Job`

The Job is the registration unit: typed facts plus one `execution` evidence
group, filled where the source parses its inputs. `JobID`, `JobPIDs` and
`CollectorNames` stay empty — they are store/intent owned.

- Identity fields (`subtype_name`, `cluster_name`, `clusterTag`,
  `NativeJobID`, `execution->step_id`): whatever the source really provided,
  verbatim. Never synthesize values.
- Cluster namespace (e.g. `COLLECTOR_HOST`, `SLURM_CLUSTER_NAME`): explicit
  config override wins; otherwise derive from the source environment. **Never
  guessed from a native job number.** Unresolved namespace ⇒ incomplete
  discovery with a reason.
- `execution->generation`: manager pid + manager start ticks + boot id. All
  three or the generation is incomplete.
- `execution->members`: prefer the execution cgroup's `cgroup.procs` — a
  *successful read including an empty list is authoritative*
  (`members_authoritative = true`). Fall back to manager children only when
  the cgroup is unreadable, and mark the list partial
  (`members_authoritative = false`). A member whose start ticks cannot be read
  stays in the list with `start_ticks == 0` so the missing evidence remains
  visible.
- Finish with `finalize_discovery_job(job, reasons)`: it computes readiness
  (`discovery_job_ready`) from the identity/generation checks and records
  every missing piece in `execution->incomplete_reason` (empty exactly when
  ready). Caveats go into `execution->notes`.

### 3.4 Adapter duties (`ISchedulerAdapter`)

- `discoveryOptions(settings)`: map `InventoryTypeSettings` onto your
  `DiscoveryOptions` (event source on/off, scan interval, cluster namespace).
- (Discovery facts are filled by the watcher itself; there is no
  `encodeFacts` detour through neutral fields.)
- `decodeFacts(row)` / `legacySelector(job)`: best-effort reconstruction for
  schema<2 rows. Incomplete is fine; a guess is not.
- `traits()`: be honest.
  `cgroup_scoped_membership = true` only if `cgroup.procs` is the
  authoritative membership for your executions;
  `kernel_attribution = true` only if your manager semantics support pid/cgroup
  attribution. A missing trait is a normal state, never an error: jobs of an
  adapter without the trait simply never get cgroup-based retirement evidence
  or kernel maps.

## 4. Step by step

### Step 1 — no shared-code edits

Since the G4 completion, adding a scheduler type touches **no shared file**.
The adapter name is the scheduler identity everywhere: config `types` are
registry names validated at startup, `<name>_job_watcher` sections resolve by
convention, native-id rules self-register (`REGISTER_NATIVE_ID_RULE`), facts
for non-built-in schedulers ride the `SubAttr` JSON alternative with
`subtype_name`, and identity/cgroup derivation dispatches through
`ISchedulerAdapter` methods. The only optional edit is a
`DiscoveryEnvironment` field if your source needs a special root (like
Slurm's spool dir); generic roots (`proc_root`, `cgroup_root`) already exist.

### Step 2 — write the watcher

Header-only, modeled on `condor_job_watcher.hpp`:

```cpp
// include/job_watcher/ssh_job_watcher.hpp
class SshJobWatcher final : public IDiscoverySource {
public:
    explicit SshJobWatcher(DiscoveryOptions options);   // options only
    ~SshJobWatcher() override { stop(); }

    const std::string& name() const override { return name_; }

    bool start(const DiscoverySink& sink) override;     // sink first, then poll
    void stop() override;                               // idempotent, bounded
    ScanResult scan() override;                         // authoritative pass
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Job> observeForHint(pid_t hint_pid) override;

private:
    void poll_loop();                                   // event-blocked (epoll)
    Job observe_session(...);                           // your observe_starter
    bool init_event_source();                           // optional eBPF
    ...
};
```

Implementation checklist (all of these are load-bearing in the Condor
watcher — copy the patterns, not just the shape):

- `poll_loop()`: **event-blocked**, not slice-polled — a single
  `ring_buffer__poll(rb, timeout)` call blocks inside libbpf's own epoll, so
  exec events reach the sink at kernel-wakeup speed; the timeout only bounds
  how long a `stop()` can go unnoticed (100 ms cadence) and fires the
  self-scan deadline when `scan_interval_ms > 0`. The old slice design's
  latency came from sleeping on a condition variable *before* a non-blocking
  drain — don't reintroduce that split. Self-scan only if
  `options.scan_interval_ms > 0` — in production the registry heartbeat owns
  pacing and the interval is 0.
- `scan()`: check `proc_root` readability first (failure ⇒ `complete=false`);
  read `boot_id`; enumerate candidate managers from the process table;
  `skipped` entries are counted and noted, never fabricated into Jobs; build
  one Job per manager (the store is the identity idempotency point — no merge
  model); finish through the same health-update path (`finish_scan` pattern).
- `observeForHint(pid)`: read the parent, **verify the parent's cmdline is
  the expected manager** (the eBPF filter guarantees it for real events, but a
  recycled pid must not fabricate a discovery), then build one Job with the
  hinted pid preferred during fact resolution.

### Step 3 — write the adapter

```cpp
// include/job_watcher/ssh_adapter.hpp
class SshSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit SshSchedulerAdapter(const InventoryTypeSettings& settings)
        : watcher_(discoveryOptions(settings)) {}

    const std::string& name() const override { return name_; }
    std::string configSection() const override { return "ssh_job_watcher"; }
    IDiscoverySource& source() override { return watcher_; }
    DiscoveryOptions discoveryOptions(const InventoryTypeSettings&) const override;
    Job decodeFacts(const nlohmann::json& row) const override;
    NativeSelector legacySelector(const Job&) const override;
    AdapterTraits traits() const override;   // ssh: both traits false

private:
    std::string name_{"ssh"};
    SshJobWatcher watcher_;
};

REGISTER_SCHEDULER_ADAPTER("ssh", SshSchedulerAdapter)
```

### Step 4 — link the registration into the binary

The registry is populated by static initialization, so the TU must be linked
into `JobLens`. Two equivalent options; the codebase currently uses (a):

- **(a)** `#include "job_watcher/ssh_adapter.hpp"` next to the condor/slurm
  includes in `include/core/job_registry.hpp`;
- **(b)** put the `REGISTER_SCHEDULER_ADAPTER` line in a small
  `src/job_watcher/ssh_adapter.cpp` — `GLOB_RECURSE src/*.cpp` picks it up
  automatically.

### Step 5 — optional eBPF event source

If exec-event hints make sense for your scheduler, add
`src/ebpf/trace_<manager>.bpf.c` on the `sys_exit_execve` tracepoint: read
`real_parent->comm`, compare with your manager name, reserve + submit
`{pid, ppid, comm}` into a `BPF_MAP_TYPE_RINGBUF` named `exec_events`
(256 KB is the house size). Follow the ebpf conventions in
[src/ebpf/AGENTS.md](../src/ebpf/): the shared header include comes **last**,
the shared struct lives in `include/ebpf/<name>.h` behind
`#ifndef __VMLINUX_H__`, and the build compiles `src/ebpf/*.bpf.c` into
`build/bpf_obj/*.bpf.o` automatically. Userspace loads it with
`EbpfCommon::resolve_bpf_obj("trace_<manager>.bpf.o")` — treat every failure
as a note plus scan-only degradation, never a startup error.

Without an event source your adapter simply runs scan-only: hints are never
produced, authoritative passes still maintain the inventory.

### Step 6 — configuration

Add a section to `config/config.example.yaml` (production keeps its real
`config/config.yaml` gitignored):

```yaml
ssh_job_watcher:
  auto_collect: true          # or use_rules + rules_dir for the Lua engine
  use_rules: false
  auto_add_collectors: [cpumem_collector]
  cluster_name: ""            # empty: derive from the source environment
```

and list the type under `job_registry_config.inventory.types: [condor, slurm, ssh]`.
Leaving it out keeps the type dormant; the heartbeat still maintains any
existing records of that type.

### Step 7 — tests

Follow the two existing tiers; neither needs root, live schedulers or eBPF:

1. **Watcher tier** — fixture file tree + real watcher, like
   `tests/integration/run_watcher_scan_test.sh`: build a fake
   procfs/cgroup/job-ad tree, point `DiscoveryEnvironment` at it, assert
   `scan()` observations, health transitions, `start/stop` lifecycle, and
   that an unavailable event source is an explicit note. The script reuses
   the Ninja `JobLens` link command so production objects are exercised as
   shipped.
2. **Registry tier** — fake source + probes + encoder through the
   constructor seams, like `tests/integration/job_inventory_maintenance_test.cpp`:

   ```cpp
   JobRegistryOptions options;
   options.db_path            = (tmp / "db").string();
   options.register_rpc       = false;
   options.test_sources       = {{"ssh", &fake_source}};
   options.test_process_probe = probes.process_probe();
   options.test_cgroup_probe  = probes.cgroup_probe();
   options.test_facts_encoder = encode_test_facts;
   options.test_heartbeat     = true;
   JobRegistry registry(options);
   registry.runInventoryPass(/*do_scan=*/true);   // drive passes directly
   ```

   This is where the invariants (I1–I8) get pinned: merge semantics,
   generation conflicts, absence/suspect transitions, retirement evidence,
   closing semantics.

For eBPF-dependent tests, guard on namespace availability the way
`tests/integration/run_net_usage_test.sh` does and skip cleanly.

## 5. File touch list (summary)

| New | Purpose |
|---|---|
| `include/job_watcher/<t>_job_watcher.hpp` | `IDiscoverySource` implementation |
| `include/job_watcher/<t>_adapter.hpp` | `ISchedulerAdapter` + `REGISTER_SCHEDULER_ADAPTER` |
| `src/ebpf/trace_<manager>.bpf.c` (+ `include/ebpf/<manager>.h`) | optional exec-hint event source |
| watcher/registry tier tests | fixture trees, fake-source passes |

| Edited | Purpose |
|---|---|
| `include/core/job_registry.hpp` | include the adapter header (option (a)); nothing else |
| `include/core/job_observation.hpp` | optional: a special `DiscoveryEnvironment` root (e.g. a spool dir) |
| `config/config.example.yaml` | `<t>_job_watcher` section + `inventory.types` entry |

No changes are needed in `collector_type.h`, `job_identity`, `inventory_config`,
`JobStore`, the heartbeat/retirement logic, `KernelAttribution`, the RPC methods
or the trigger: identity, config sections, native-id rules, facts (JSON
alternative), matching and cgroup derivation are all adapter-driven.

## 6. Final checklist

- [ ] Constructor: options only — no Config, no eBPF, no threads.
- [ ] `start()` installs the sink before any possible callback.
- [ ] Poll thread registers exec hints synchronously through `on_job`;
      periodic self-scans (if `scan_interval_ms > 0`) use the same entry.
- [ ] `stop()` is idempotent and bounded; safe from the destructor.
- [ ] `scan()` reports `complete=false` only for pass-level failures;
      skipped sources are counted and noted, never treated as absence.
- [ ] Discovered Jobs are ready or explicitly incomplete
      (`execution->incomplete_reason`); the cluster namespace is never guessed
      from a native id.
- [ ] Cgroup membership: successful read (including empty) is authoritative;
      fallback paths mark `members_authoritative=false`.
- [ ] The builder leaves `JobID`/`JobPIDs`/`CollectorNames` empty (store and
      intent own them).
- [ ] `traits()` is truthful; missing capabilities are normal states.
- [ ] eBPF failures degrade to scan-only with an explicit note.
- [ ] Both test tiers pass unprivileged; new invariants covered in the
      registry tier.
