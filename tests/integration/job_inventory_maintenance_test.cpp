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
/* Deterministic maintenance acceptance (task 3 of
 * doc/job_inventory_collection_decoupling_plan.md).
 *
 *   A08 start-before-scan, events injected during a scan are replayed,
 *       duplicate/reordered hints collapse, ended generations never resurrect.
 *   A09 one missed scan recovers; two complete absences retire; failed or
 *       incomplete scans never count as absence.
 *   A10 empty PID list with a live manager/cgroup does not retire; positive
 *       manager end plus an empty cgroup retires with no read involvement.
 *   A11 EPERM/unreadable/unresolved input is unknown, not death; incomplete
 *       identities stay visible and retried; no wrong merge.
 *   A12 bounded hint queue overflow counts drops, degrades and requests a
 *       rescan without blocking the producer; the authoritative scan repairs.
 *   A13 tracker-unavailable mode (source started without an event source)
 *       keeps maintenance and retirement working and reports the gap.
 *   A14 a late acknowledgement for an old generation never removes the new
 *       instance's attribution or identity index.
 *   A23 repeated reads cause no maintenance side effects and snapshots stay
 *       coherent while sources and stores mutate; the sentinel is excluded.
 *   A25 stop during a blocked reconciliation, repeated start/stop, source
 *       initialization failure and an incomplete drain are all reported.
 *
 * Every scenario uses fake sources, an injectable clock and fake liveness
 * probes. No scheduler, no eBPF and no real procfs/cgroup tree is involved.
 */
#include "core/inventory_status_provider.hpp"
#include "core/job_inventory_maintenance.hpp"
#include "core/job_observation.hpp"
#include "core/job_registry.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

void pass_case(const char* id, const char* description)
{
    std::cout << "PASS " << id << " " << description << '\n' << std::flush;
}

bool wait_until(const std::function<bool()>& predicate, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

std::string fresh_dir(const std::string& base, const std::string& name)
{
    const auto path = std::filesystem::path(base) / name;
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path.string();
}

JobRegistryOptions isolated(const std::string& path)
{
    JobRegistryOptions options;
    options.db_path = path;
    options.enable_inventory_maintenance = false;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    return options;
}

// ---------------------------------------------------------------------------
// Deterministic clock
// ---------------------------------------------------------------------------

class FakeClock {
public:
    ScanClock closure()
    {
        return [this]() {
            const std::int64_t value = now_ms.load();
            now_ms.fetch_add(1);
            return value;
        };
    }

    void set(std::int64_t value) { now_ms.store(value); }

    std::atomic<std::int64_t> now_ms{1'700'000'000'000};
};

// ---------------------------------------------------------------------------
// Fake discovery source
// ---------------------------------------------------------------------------

ScanResult empty_scan()
{
    ScanResult result;
    result.complete = true;
    result.started_at_ms = 1;
    result.finished_at_ms = 2;
    return result;
}

class FakeSource final : public IDiscoverySource {
public:
    FakeSource(std::string name, SchedulerType type)
        : name_(std::move(name)), type_(type)
    {
    }

    // ---- IDiscoverySource ---------------------------------------------------
    const std::string& name() const override { return name_; }
    SchedulerType schedulerType() const override { return type_; }

    bool start(const DiscoverySink& sink) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++start_calls;
        if (start_throws_) throw std::runtime_error("event source init failed");
        sink_ = sink;
        started = true;
        health_state_.polling = true;
        health_state_.state = SourceState::Healthy;
        return start_result;
    }

    void stop() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stop_calls;
        started = false;
        health_state_.polling = false;
    }

    ScanResult scan() override
    {
        std::function<ScanResult()> runner;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++scan_calls;
            if (!started) scan_without_start = true;
            runner = scan_fn;
        }
        ScanResult result = runner ? runner() : empty_scan();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            last_result = result;
            health_state_.last_scan_start_ms = result.started_at_ms;
            if (result.complete) {
                health_state_.state = SourceState::Healthy;
                health_state_.last_scan_success_ms = result.finished_at_ms;
                health_state_.consecutive_failures = 0;
            } else {
                health_state_.state = SourceState::Degraded;
                health_state_.last_error = result.error;
                ++health_state_.consecutive_failures;
            }
        }
        return result;
    }

    SourceHealth health() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return health_state_;
    }

    void requestRescan() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++rescan_calls;
        health_state_.rescan_requested = true;
    }

    std::optional<Observation> observeForHint(pid_t hint_pid) override
    {
        std::function<std::optional<Observation>(pid_t)> observe;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++observe_calls;
            observe = observe_fn;
        }
        return observe ? observe(hint_pid) : std::nullopt;
    }

    // ---- Test drivers ---------------------------------------------------------
    // Invokes the installed observation sink exactly like an adapter's poll
    // thread would (outside the source lock).
    void inject(const Observation& observation)
    {
        ObservationSink sink;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            sink = sink_.on_observation;
        }
        check(static_cast<bool>(sink), "source " + name_ + ": sink is not installed");
        sink(observation);
    }

    void set_scan(std::function<ScanResult()> fn)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        scan_fn = std::move(fn);
    }

    // Targeted-observation behaviour for exec-hint cases; defaults to
    // "this pid is not mine" like a source whose filter does not match.
    void set_observe(std::function<std::optional<Observation>(pid_t)> fn)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        observe_fn = std::move(fn);
    }

    int scans() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return scan_calls;
    }

    int starts() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return start_calls;
    }

    int stops() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return stop_calls;
    }

    int rescans() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return rescan_calls;
    }

    int observations_for_hint() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return observe_calls;
    }

    bool scanned_without_start() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return scan_without_start;
    }

    bool scan_only() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return !start_result;
    }

    void set_start_result(bool value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        start_result = value;
    }

    void set_health_error(const std::string& error)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        health_state_.state = SourceState::Degraded;
        health_state_.last_error = error;
    }

    // A25: model a source whose start() throws.
    void set_start_throw(bool value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        start_throws_ = value;
    }

private:
    std::string name_;
    SchedulerType type_;
    mutable std::mutex mutex_;
    DiscoverySink sink_;
    std::function<ScanResult()> scan_fn;
    std::function<std::optional<Observation>(pid_t)> observe_fn;
    SourceHealth health_state_;
    ScanResult last_result;
    bool started = false;
    bool start_result = true;
    bool start_throws_ = false;
    bool scan_without_start = false;
    int start_calls = 0;
    int stop_calls = 0;
    int scan_calls = 0;
    int rescan_calls = 0;
    int observe_calls = 0;
};

// ---------------------------------------------------------------------------
// Fake liveness / cgroup probes
// ---------------------------------------------------------------------------

class FakeProbes {
public:
    void set_process(pid_t pid, LivenessState state, std::uint64_t start_ticks = 0)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        processes_[pid] = Entry{state, start_ticks};
    }

    void set_cgroup(const std::string& path, std::vector<pid_t> members, bool readable = true)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cgroups_[path] = CgroupMembership{readable, std::move(members), ""};
    }

    void unset_cgroup(const std::string& path)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cgroups_.erase(path);
    }

    ProcessLivenessProbe process_probe()
    {
        return [this](const ProcessIdentity& identity) {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = processes_.find(identity.pid);
            if (it == processes_.end()) {
                return LivenessResult{LivenessState::Dead, "not tracked"};
            }
            if (it->second.state != LivenessState::Alive) return LivenessResult{it->second.state, "fake"};
            if (identity.start_ticks != 0 && it->second.start_ticks != 0 &&
                identity.start_ticks != it->second.start_ticks) {
                return LivenessResult{LivenessState::Dead, "pid reused"};
            }
            return LivenessResult{LivenessState::Alive, "fake alive"};
        };
    }

    CgroupMembershipProbe cgroup_probe()
    {
        return [this](const std::string& path) {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = cgroups_.find(path);
            if (it == cgroups_.end()) {
                CgroupMembership membership;
                membership.error = "not readable";
                return membership;
            }
            return it->second;
        };
    }

private:
    struct Entry {
        LivenessState state = LivenessState::Unknown;
        std::uint64_t start_ticks = 0;
    };
    std::mutex mutex_;
    std::map<pid_t, Entry> processes_;
    std::map<std::string, CgroupMembership> cgroups_;
};

// ---------------------------------------------------------------------------
// Observation helpers
// ---------------------------------------------------------------------------

Observation make_condor_observation(const std::string& native,
                                    pid_t manager_pid,
                                    std::uint64_t manager_ticks,
                                    const std::string& cluster = "htcondor01",
                                    const std::string& tag = "schedd-a",
                                    const std::string& cgroup = "",
                                    const std::vector<pid_t>& members = {})
{
    Observation observation;
    observation.scheduler_type = SchedulerType::Condor;
    observation.selector.scheduler_type = SchedulerType::Condor;
    observation.selector.cluster_name = cluster;
    observation.selector.cluster_tag = tag;
    observation.selector.native_job_id = native;
    observation.generation.boot_id = "boot-aaa";
    observation.generation.manager_pid = manager_pid;
    observation.generation.manager_start_ticks = manager_ticks;
    observation.generation.cgroup_identity = cgroup;
    observation.cgroup_path = cgroup;
    observation.owner = "alice";
    observation.scheduler_host = "htcondor01";
    for (const pid_t member : members) {
        observation.members.push_back(ProcessIdentity{member, manager_ticks, "boot-aaa"});
    }
    finalize_observation(observation);
    return observation;
}

Observation make_unresolved_observation(const std::string& native, pid_t manager_pid)
{
    Observation observation = make_condor_observation(native, manager_pid, 900000);
    observation.selector.cluster_name.clear();
    observation.incomplete_reason.clear();
    finalize_observation(observation);
    return observation;
}

ScanResult scan_with(std::vector<Observation> observations, bool complete = true,
                     const std::string& error = "")
{
    ScanResult result;
    result.complete = complete;
    result.error = error;
    result.started_at_ms = 10;
    result.finished_at_ms = 11;
    result.observations = std::move(observations);
    for (const auto& observation : result.observations) {
        if (observation.identity_complete) ++result.counts.complete;
        else ++result.counts.incomplete;
    }
    result.counts.candidates = result.observations.size();
    return result;
}

InventoryMaintenanceConfig condor_config(std::uint32_t interval = 30)
{
    InventoryMaintenanceConfig config;
    config.types = {InventoryType::Condor};
    config.reconcile_interval_sec = interval;
    InventoryTypeSettings settings;
    settings.inventory = true;
    config.settings.emplace(InventoryType::Condor, settings);
    config.settings.emplace(InventoryType::Slurm, InventoryTypeSettings{});
    return config;
}

std::size_t count_records(JobRegistry& registry)
{
    const auto records = registry.inventoryRecords();
    std::size_t count = 0;
    for (const auto& record : records) {
        if (record.internal_id != 0) ++count;
    }
    return count;
}

std::optional<InventoryRecord> find_native(JobRegistry& registry, const std::string& native,
                                           const std::string& cluster = "htcondor01",
                                           const std::string& tag = "schedd-a")
{
    NativeSelector selector;
    selector.scheduler_type = SchedulerType::Condor;
    selector.cluster_name = cluster;
    selector.cluster_tag = tag;
    selector.native_job_id = native;
    const auto identity = canonicalize(selector);
    if (!identity) return std::nullopt;
    return registry.inventoryRecordByIdentity(*identity);
}

// ---------------------------------------------------------------------------
// A08 - start before scan, buffered events, duplicate/obsolete hints
// ---------------------------------------------------------------------------

void case_a08(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "a08")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;

    const Observation observed_by_scan =
        make_condor_observation("100.0", 5000, 111, "htcondor01", "schedd-a", "/cg/slot-a");
    const Observation injected_during_scan =
        make_condor_observation("101.0", 5001, 222, "htcondor01", "schedd-a", "/cg/slot-b");

    source.set_scan([&]() {
        // An event arriving while the pass is running must be buffered and
        // merged through the same upsert path, not lost.
        source.inject(injected_during_scan);
        return scan_with({observed_by_scan});
    });

    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());

    check(maintenance.start(), "A08: maintenance did not start");
    check(source.scans() == 1, "A08: the initial full scan did not run");
    check(!source.scanned_without_start(), "A08: scan ran before the source was started");
    check(source.starts() == 1, "A08: the source was not started exactly once");

    check(count_records(registry) == 2, "A08: buffered event during the scan was not merged");
    check(find_native(registry, "100.0").has_value(), "A08: scan observation missing");
    check(find_native(registry, "101.0").has_value(), "A08: injected observation missing");

    // Duplicate and reordered hints collapse to the existing entry.
    const auto before = find_native(registry, "101.0");
    check(before.has_value(), "A08: injected record vanished");
    maintenance.onObservation(injected_during_scan);
    maintenance.onObservation(injected_during_scan);
    maintenance.onObservation(observed_by_scan);
    maintenance.reconcileNow();
    check(count_records(registry) == 2, "A08: duplicate hints duplicated records");
    check(find_native(registry, "101.0")->internal_id == before->internal_id,
          "A08: duplicate hint changed the internal id");

    // End the execution and prove an obsolete hint cannot resurrect it. The
    // source stops injecting 101.0 so the job is genuinely gone from discovery.
    source.set_scan([&]() { return scan_with({observed_by_scan}); });
    probes.set_process(5001, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-b", {}, true);
    maintenance.reconcileNow();
    check(!find_native(registry, "101.0").has_value(), "A08: terminated execution not retired");

    maintenance.onObservation(injected_during_scan);
    maintenance.reconcileNow();
    check(!find_native(registry, "101.0").has_value(),
          "A08: obsolete hint resurrected an ended generation");

    // A new execution generation for the same logical job is a new entry.
    const Observation replacement =
        make_condor_observation("101.0", 5002, 333, "htcondor01", "schedd-a", "/cg/slot-b");
    maintenance.onObservation(replacement);
    maintenance.reconcileNow();
    const auto revived = find_native(registry, "101.0");
    check(revived.has_value(), "A08: a new generation was rejected after retirement");
    check(revived->generation.manager_start_ticks == 333,
          "A08: the new generation was not stored");
    check(count_records(registry) == 2, "A08: duplicate entry for one generation");

    maintenance.stop();
    pass_case("A08", "start-before-scan, in-scan event replay, duplicate hints, no resurrection");
}

// ---------------------------------------------------------------------------
// A09 - absence accounting
// ---------------------------------------------------------------------------

void case_a09(const std::string& base)
{
    // Scenario 1: one missed complete scan, then the job reappears.
    {
        JobRegistry registry(isolated(fresh_dir(base, "a09-recover")));
        FakeClock clock;
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        const Observation job =
            make_condor_observation("200.0", 5100, 444, "htcondor01", "schedd-a", "/cg/slot-1");

        int pass = 0;
        source.set_scan([&]() {
            ++pass;
            if (pass == 2) return scan_with({});
            return scan_with({job});
        });

        std::vector<IDiscoverySource*> sources;
        sources.push_back(&source);
        JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
        maintenance.setClock(clock.closure());
        maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
        check(maintenance.start(), "A09: maintenance did not start");

        probes.set_process(5100, LivenessState::Dead);
        probes.unset_cgroup("/cg/slot-1");
        maintenance.reconcileNow();  // miss #1
        check(find_native(registry, "200.0").has_value(),
              "A09: one missed scan retired the instance");
        maintenance.reconcileNow();  // observed again
        check(find_native(registry, "200.0").has_value(),
              "A09: recovered instance was retired");
        check(maintenance.statusJson()["retirements"] == 0,
              "A09: an unrecoverable retirement was counted");
        maintenance.stop();
    }

    // Scenario 2: two consecutive complete absences retire.
    {
        JobRegistry registry(isolated(fresh_dir(base, "a09-retire")));
        FakeClock clock;
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        const Observation job =
            make_condor_observation("210.0", 5200, 555, "htcondor01", "schedd-a", "/cg/slot-2");
        bool first = true;
        source.set_scan([&]() {
            if (first) {
                first = false;
                return scan_with({job});
            }
            return scan_with({});
        });

        std::vector<IDiscoverySource*> sources;
        sources.push_back(&source);
        JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
        maintenance.setClock(clock.closure());
        maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
        check(maintenance.start(), "A09: maintenance did not start");

        probes.set_process(5200, LivenessState::Dead);
        probes.unset_cgroup("/cg/slot-2");  // cgroup evidence unknown
        maintenance.reconcileNow();  // absence #1
        check(find_native(registry, "210.0").has_value(),
              "A09: retirement happened after a single absence");
        check(maintenance.statusJson()["suspect_count"] == 1,
              "A09: an absent instance was not reported suspect");
        maintenance.reconcileNow();  // absence #2
        check(!find_native(registry, "210.0").has_value(),
              "A09: two complete absences did not retire");
        check(maintenance.statusJson()["retirements"] == 1,
              "A09: retirement counter did not advance");
        maintenance.stop();
    }

    // Scenario 3: failed/incomplete scans never count as absence.
    {
        JobRegistry registry(isolated(fresh_dir(base, "a09-failed")));
        FakeClock clock;
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        const Observation job =
            make_condor_observation("220.0", 5300, 666, "htcondor01", "schedd-a", "/cg/slot-3");
        int pass = 0;
        source.set_scan([&]() {
            ++pass;
            if (pass == 1) return scan_with({job});
            if (pass <= 3) return scan_with({}, false, "procfs unreadable");
            return scan_with({});
        });

        std::vector<IDiscoverySource*> sources;
        sources.push_back(&source);
        JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
        maintenance.setClock(clock.closure());
        maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
        check(maintenance.start(), "A09: maintenance did not start");

        probes.set_process(5300, LivenessState::Dead);
        probes.unset_cgroup("/cg/slot-3");
        maintenance.reconcileNow();  // pass 2: failed
        maintenance.reconcileNow();  // pass 3: failed
        check(find_native(registry, "220.0").has_value(),
              "A09: failed scans were treated as absence");
        check(maintenance.statusJson()["suspect_count"] == 0,
              "A09: failed scans marked the instance suspect");
        maintenance.reconcileNow();  // pass 4: complete miss #1
        check(find_native(registry, "220.0").has_value(),
              "A09: retirement after one complete absence");
        maintenance.reconcileNow();  // pass 5: complete miss #2
        check(!find_native(registry, "220.0").has_value(),
              "A09: two complete absences after failures did not retire");
        maintenance.stop();
    }

    pass_case("A09", "recovery after one miss, retirement after two complete absences only");
}

// ---------------------------------------------------------------------------
// A10 - empty PID window vs positive termination evidence
// ---------------------------------------------------------------------------

void case_a10(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "a10")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    const Observation job =
        make_condor_observation("300.0", 5400, 777, "htcondor01", "schedd-a", "/cg/slot-4");

    source.set_scan([&]() { return scan_with({job}); });

    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "A10: maintenance did not start");

    // The execution is really running: manager alive, cgroup has a member. A
    // later scan sees no job, and the record's PID list is empty (the window
    // between manager exit and member reaping).
    probes.set_process(5400, LivenessState::Alive, 777);
    probes.set_process(6001, LivenessState::Alive, 777);
    probes.set_cgroup("/cg/slot-4", {6001}, true);
    const auto record = find_native(registry, "300.0");
    check(record.has_value(), "A10: record missing");
    check(record->processes.empty(), "A10: fixture should have an empty PID list");

    source.set_scan([&]() { return scan_with({}); });
    maintenance.reconcileNow();
    maintenance.reconcileNow();
    maintenance.reconcileNow();
    check(find_native(registry, "300.0").has_value(),
          "A10: an empty PID list retired a live execution");

    // Positive evidence: manager ended and the execution cgroup is empty. An
    // exit hint is enough: the targeted pass retires it without any source
    // read and without any collector involvement.
    probes.set_process(5400, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-4", {}, true);
    const int scans_before = source.scans();
    maintenance.notifyExit(5400);
    check(wait_until([&]() { return !find_native(registry, "300.0").has_value(); }, 3000),
          "A10: positive termination evidence did not retire");
    check(source.scans() == scans_before,
          "A10: retirement needed a source scan instead of targeted evidence");

    maintenance.stop();
    pass_case("A10", "no premature retirement on an empty PID window; positive evidence retires");
}

// ---------------------------------------------------------------------------
// A11 - unknown is not death, incomplete identities are retried
// ---------------------------------------------------------------------------

void case_a11(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "a11")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    const Observation job =
        make_condor_observation("400.0", 5500, 888, "htcondor01", "schedd-a", "/cg/slot-5");

    bool first = true;
    source.set_scan([&]() {
        if (first) {
            first = false;
            return scan_with({job});
        }
        return scan_with({});
    });

    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "A11: maintenance did not start");

    // EPERM on the manager probe: unknown, not death, even after two complete
    // absences.
    probes.set_process(5500, LivenessState::Unknown);
    probes.set_cgroup("/cg/slot-5", {}, false);  // unreadable cgroup
    maintenance.reconcileNow();
    maintenance.reconcileNow();
    maintenance.reconcileNow();
    check(find_native(registry, "400.0").has_value(),
          "A11: an unreadable liveness check was treated as death");

    // An unresolved namespace cannot be indexed, is counted as incomplete and
    // is retried once the namespace resolves.
    const Observation unresolved = make_unresolved_observation("401.0", 5501);
    check(!unresolved.identity_complete, "A11: fixture should be incomplete");
    maintenance.onObservation(unresolved);
    maintenance.reconcileNow();
    check(!find_native(registry, "401.0").has_value(),
          "A11: an unresolved identity was indexed");
    check(maintenance.statusJson()["incomplete_observations"].get<std::uint64_t>() >= 1,
          "A11: incomplete observation was not reported");
    check(count_records(registry) == 1, "A11: an incomplete identity created a record");

    const Observation resolved =
        make_condor_observation("401.0", 5501, 999, "htcondor01", "schedd-a", "/cg/slot-6");
    // The manager is live; the empty scans must not retire it (absence is not
    // the subject of this case).
    probes.set_process(5501, LivenessState::Alive, 999);
    maintenance.onObservation(resolved);
    maintenance.reconcileNow();
    check(find_native(registry, "401.0").has_value(),
          "A11: a retried complete identity was not indexed");
    check(count_records(registry) == 2, "A11: retry duplicated or merged records");

    // A namespace-less observation must never merge into another namespace.
    maintenance.onObservation(make_unresolved_observation("400.0", 5502));
    maintenance.reconcileNow();
    check(count_records(registry) == 2, "A11: an unresolved identity merged into a record");

    // A second cluster namespace with the same native id stays distinct.
    const Observation other_cluster =
        make_condor_observation("400.0", 5600, 1111, "htcondor02", "schedd-b", "/cg/slot-7");
    probes.set_process(5600, LivenessState::Alive, 1111);
    maintenance.onObservation(other_cluster);
    maintenance.reconcileNow();
    check(count_records(registry) == 3, "A11: namespaces merged");
    check(find_native(registry, "400.0", "htcondor02", "schedd-b").has_value(),
          "A11: second namespace record missing");
    check(find_native(registry, "400.0")->generation.manager_pid == 5500,
          "A11: wrong record was updated");

    maintenance.stop();
    pass_case("A11", "EPERM/unknown is not death; incomplete identities visible and retried");
}

// ---------------------------------------------------------------------------
// A12 - bounded queue overflow, degradation, rescan, repair
// ---------------------------------------------------------------------------

void case_a12(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "a12")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;

    const Observation alpha =
        make_condor_observation("500.0", 5700, 1212, "htcondor01", "schedd-a", "/cg/slot-8");
    const Observation beta =
        make_condor_observation("501.0", 5701, 1313, "htcondor01", "schedd-a", "/cg/slot-9");

    source.set_scan([&]() { return scan_with({}); });

    JobInventoryMaintenance::Options options;
    options.queue_capacity = 4;
    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources), options);
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "A12: maintenance did not start");

    // Block a reconciliation inside the source scan, then overflow the queue
    // from the producer side.
    std::promise<void> scan_entered;
    std::promise<void> release_scan;
    auto gate = release_scan.get_future().share();
    source.set_scan([&]() {
        scan_entered.set_value();
        gate.wait();
        return scan_with({});
    });

    std::thread reconciler([&]() { maintenance.reconcileNow(); });
    scan_entered.get_future().wait();

    const int rescans_before = source.rescans();
    for (int index = 0; index < 12; ++index) maintenance.onObservation(alpha);

    const auto degraded = maintenance.statusJson();
    check(degraded["events_dropped"].get<std::uint64_t>() >= 1,
          "A12: queue overflow did not count drops");
    check(degraded["queue_depth"].get<std::size_t>() <= options.queue_capacity,
          "A12: the hint queue was not bounded");
    check(degraded["state"] == "degraded", "A12: overflow did not degrade the source state");
    check(degraded["rescan_requests"].get<std::uint64_t>() >= 1,
          "A12: overflow did not request a rescan");
    check(source.rescans() > rescans_before, "A12: the source was not asked to rescan");

    release_scan.set_value();
    reconciler.join();

    // The authoritative complete scan repairs live state.
    source.set_scan([&]() { return scan_with({alpha, beta}); });
    maintenance.reconcileNow();
    const auto repaired = maintenance.statusJson();
    check(repaired["state"] == "healthy", "A12: a complete scan did not clear degradation");
    check(find_native(registry, "500.0").has_value(), "A12: repair lost the observed job");
    check(find_native(registry, "501.0").has_value(), "A12: repair did not add the second job");
    check(repaired["events_dropped"].get<std::uint64_t>() >=
              degraded["events_dropped"].get<std::uint64_t>(),
          "A12: drop counter regressed");

    maintenance.stop();
    pass_case("A12", "bounded queue drops, degradation, rescan request and scan repair");
}

// ---------------------------------------------------------------------------
// A13 - tracker-unavailable mode
// ---------------------------------------------------------------------------

void case_a13(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "a13")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    source.set_start_result(false);
    source.set_health_error("eBPF event source unavailable");

    const Observation job =
        make_condor_observation("600.0", 5800, 1414, "htcondor01", "schedd-a", "/cg/slot-10");
    source.set_scan([&]() { return scan_with({job}); });

    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "A13: maintenance did not start without an event source");

    const auto status = maintenance.statusJson();
    std::cout << "STATUS " << status.dump() << '\n';
    check(status["tracker_unavailable"] == true, "A13: tracker unavailability not reported");
    check(status["sources"][0]["started"] == false, "A13: source start state misreported");
    check(status["sources"][0]["start_note"].get<std::string>().find("unavailable") !=
              std::string::npos ||
              status["sources"][0]["start_note"].get<std::string>().find("scan-only") !=
                  std::string::npos,
          "A13: no note about the missing event source");
    check(find_native(registry, "600.0").has_value(),
          "A13: scanning-only maintenance did not register the job");

    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(5800, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-10", {}, true);
    maintenance.reconcileNow();
    check(!find_native(registry, "600.0").has_value(),
          "A13: retirement did not work in tracker-unavailable mode");

    maintenance.stop();
    pass_case("A13", "tracker unavailable: maintenance and retirement continue, health reports it");
}

// ---------------------------------------------------------------------------
// A14 - old closing record vs a new instance reusing pid/cgroup
// ---------------------------------------------------------------------------

void case_a14(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "a14")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;

    // A registered lifecycle observer makes retirements go through the
    // closing state instead of archiving immediately.
    std::mutex observer_mutex;
    std::vector<uint64_t> removed;
    registry.addLifecycleCb([&](JobEvent event, const Job& job) {
        if (event != JobEvent::Removed) return;
        std::lock_guard<std::mutex> lock(observer_mutex);
        removed.push_back(job.JobID);
    });

    const Observation old_job =
        make_condor_observation("700.0", 5900, 1515, "htcondor01", "schedd-a", "/cg/slot-11");
    source.set_scan([&]() { return scan_with({old_job}); });

    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "A14: maintenance did not start");

    const auto old_record = find_native(registry, "700.0");
    check(old_record.has_value(), "A14: old record missing");

    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(5900, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-11", {}, true);
    maintenance.reconcileNow();
    check(registry.closingJobCount() == 1, "A14: retirement did not enter the closing state");
    {
        std::lock_guard<std::mutex> lock(observer_mutex);
        check(removed.size() == 1 && removed.front() == old_record->internal_id,
              "A14: the closing retirement was not published to observers");
    }
    check(registry.inventoryRecord(old_record->internal_id).has_value(),
          "A14: closing instance disappeared before acknowledgement");

    // A new job reuses the same PID and cgroup path while the old record is
    // still closing.
    const Observation new_job =
        make_condor_observation("700.1", 5900, 1616, "htcondor01", "schedd-a", "/cg/slot-11");
    source.set_scan([&]() { return scan_with({new_job}); });
    maintenance.reconcileNow();

    const auto new_record = find_native(registry, "700.1");
    check(new_record.has_value(), "A14: new instance was not registered");
    check(new_record->internal_id != old_record->internal_id,
          "A14: new instance reused the retired internal id");

    // Late cleanup of the old generation must not touch the new ownership.
    check(maintenance.finalize(), "A14: finalize with a pending closing record failed");
    check(registry.closingJobCount() == 0, "A14: old closing record was not finalized");
    check(registry.inventoryRecord(new_record->internal_id).has_value(),
          "A14: old acknowledgement deleted the new instance");
    const auto still_new = find_native(registry, "700.1");
    check(still_new.has_value() && still_new->internal_id == new_record->internal_id,
          "A14: old acknowledgement removed the new identity index");
    check(still_new->generation.manager_start_ticks == 1616,
          "A14: new generation was replaced by stale state");

    // A stale hint for the retired generation still cannot resurrect it.
    maintenance.onObservation(old_job);
    maintenance.reconcileNow();
    check(!find_native(registry, "700.0").has_value(),
          "A14: a stale hint resurrected the retired generation");
    check(find_native(registry, "700.1").has_value(),
          "A14: stale hint disturbed the new instance");

    maintenance.stop();
    pass_case("A14", "late cleanup of an old generation cannot remove new ownership");
}

// ---------------------------------------------------------------------------
// A23 - reads are side-effect free and snapshots are coherent
// ---------------------------------------------------------------------------

void case_a23(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "a23")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;

    const Observation first =
        make_condor_observation("800.0", 6000, 1717, "htcondor01", "schedd-a", "/cg/slot-12");
    const Observation second =
        make_condor_observation("801.0", 6001, 1818, "htcondor01", "schedd-a", "/cg/slot-13");
    std::atomic<bool> mutate{false};
    source.set_scan([&]() {
        if (mutate.load() % 2 == 0) return scan_with({first, second});
        return scan_with({first});
    });

    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    probes.set_process(6000, LivenessState::Alive, 1717);
    probes.set_process(6001, LivenessState::Alive, 1818);
    check(maintenance.start(), "A23: maintenance did not start");

    // Quiet phase: reads alone must not advance any maintenance counter.
    const auto before = maintenance.statusJson();
    for (int index = 0; index < 100; ++index) {
        const auto records = registry.inventoryRecords();
        for (const auto& record : records) {
            (void)registry.inventoryRecord(record.internal_id);
            if (record.identity_complete) {
                const auto by_identity = registry.inventoryRecordByIdentity(record.identity);
                check(by_identity.has_value() && by_identity->internal_id == record.internal_id,
                      "A23: identity lookup disagrees with the snapshot");
            }
        }
        (void)registry.findJob(0);
        (void)registry.snapshot();
        (void)maintenance.statusJson();
    }
    const auto after = maintenance.statusJson();
    check(before["pass_count"] == after["pass_count"], "A23: reads triggered a maintenance pass");
    check(before["retirements"] == after["retirements"], "A23: reads triggered retirements");
    check(before["events_received"] == after["events_received"], "A23: reads emitted events");
    check(before["last_scan_start_ms"] == after["last_scan_start_ms"],
          "A23: reads changed scan timestamps");
    check(before["queue_depth"] == after["queue_depth"], "A23: reads changed the queue");

    // Concurrent phase: maintenance mutates while reads keep observing.
    std::atomic<bool> stop{false};
    std::thread reader([&]() {
        while (!stop.load()) {
            const auto records = registry.inventoryRecords();
            if (records.size() > 5) throw std::runtime_error("A23: duplicate records appeared");
            for (const auto& record : records) {
                check(record.internal_id != 0, "A23: the system sentinel leaked into inventory");
                if (!record.identity_complete) continue;
                const auto by_identity = registry.inventoryRecordByIdentity(record.identity);
                check(by_identity.has_value(), "A23: snapshot references a missing identity");
                check(by_identity->internal_id == record.internal_id,
                      "A23: snapshot and identity index disagree");
            }
            (void)maintenance.statusJson();
        }
    });
    for (int index = 0; index < 40; ++index) {
        mutate.store(index);
        maintenance.reconcileNow();
    }
    stop.store(true);
    reader.join();

    const auto final_records = registry.inventoryRecords();
    for (const auto& record : final_records) {
        check(record.internal_id != 0, "A23: sentinel present in the final snapshot");
    }
    check(count_records(registry) == 2, "A23: mutation loop lost an instance");
    maintenance.stop();
    pass_case("A23", "reads are side-effect free; snapshots coherent; sentinel excluded");
}

// ---------------------------------------------------------------------------
// A25 - shutdown safety and incomplete-drain reporting
// ---------------------------------------------------------------------------

void case_a25(const std::string& base)
{
    // Repeat start/stop and check the provider lifecycle.
    {
        JobRegistry registry(isolated(fresh_dir(base, "a25-repeat")));
        FakeClock clock;
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        std::vector<IDiscoverySource*> sources;
        sources.push_back(&source);
        JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
        maintenance.setClock(clock.closure());
        maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());

        for (int round = 0; round < 3; ++round) {
            check(maintenance.start(), "A25: repeated start failed");
            check(maintenance.running(), "A25: controller is not running after start");
            check(static_cast<bool>(get_inventory_status_provider()),
                  "A25: status provider was not published");
            const auto status = get_inventory_status_provider()();
            check(status["state"] != "stopped", "A25: status reports stopped while running");
            maintenance.stop();
            check(!maintenance.running(), "A25: controller still running after stop");
            check(!get_inventory_status_provider(), "A25: status provider was not cleared");
            check(!maintenance.incompleteDrain(), "A25: a clean stop reported an incomplete drain");
        }
        check(source.starts() == 3, "A25: source start count mismatch");
        check(source.stops() == 3, "A25: source stop count mismatch");
    }

    // Stop while a reconciliation is blocked inside a source scan.
    {
        auto registry = std::make_unique<JobRegistry>(isolated(fresh_dir(base, "a25-blocked")));
        FakeClock clock;
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        std::vector<IDiscoverySource*> sources;
        sources.push_back(&source);
        auto maintenance = std::make_unique<JobInventoryMaintenance>(
            *registry, condor_config(), std::move(sources));
        maintenance->setClock(clock.closure());
        maintenance->setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
        check(maintenance->start(), "A25: maintenance did not start");

        std::promise<void> scan_entered;
        std::promise<void> release_scan;
        auto gate = release_scan.get_future().share();
        source.set_scan([&]() {
            scan_entered.set_value();
            gate.wait();
            return scan_with({});
        });
        std::thread reconciler([&]() { maintenance->reconcileNow(); });
        scan_entered.get_future().wait();

        std::thread stopper([&]() { maintenance->stop(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        release_scan.set_value();
        reconciler.join();
        stopper.join();
        check(!maintenance->running(), "A25: stop during a blocked scan left it running");
        check(!get_inventory_status_provider(), "A25: provider survived a blocked stop");

        // Nothing may touch the store after it is closed: destroy the controller
        // first, then the registry, and verify no crash and no dangling provider.
        maintenance.reset();
        registry.reset();
        check(!get_inventory_status_provider(), "A25: provider survived registry teardown");
    }

    // Source initialization failure is not fatal.
    {
        JobRegistry registry(isolated(fresh_dir(base, "a25-startfail")));
        FakeClock clock;
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        source.set_start_throw(true);
        std::vector<IDiscoverySource*> sources;
        sources.push_back(&source);
        JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
        maintenance.setClock(clock.closure());
        maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
        check(maintenance.start(), "A25: a throwing source start aborted maintenance");
        check(maintenance.statusJson()["tracker_unavailable"] == true,
              "A25: a failed source start was not reported");
        maintenance.stop();
        check(!maintenance.incompleteDrain(), "A25: clean stop after a failed source start");
    }

    // Incomplete drain: the archive fails, so shutdown must report it.
    {
        JobRegistry registry(isolated(fresh_dir(base, "a25-drain")));
        FakeClock clock;
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        std::mutex observer_mutex;
        std::vector<uint64_t> removed;
        registry.addLifecycleCb([&](JobEvent event, const Job& job) {
            if (event != JobEvent::Removed) return;
            std::lock_guard<std::mutex> lock(observer_mutex);
            removed.push_back(job.JobID);
        });

        const Observation job =
            make_condor_observation("900.0", 6100, 1919, "htcondor01", "schedd-a", "/cg/slot-14");
        source.set_scan([&]() { return scan_with({job}); });
        std::vector<IDiscoverySource*> sources;
        sources.push_back(&source);
        JobInventoryMaintenance maintenance(registry, condor_config(), std::move(sources));
        maintenance.setClock(clock.closure());
        maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
        check(maintenance.start(), "A25: maintenance did not start");

        source.set_scan([&]() { return scan_with({}); });
        probes.set_process(6100, LivenessState::Dead);
        probes.set_cgroup("/cg/slot-14", {}, true);
        maintenance.reconcileNow();
        check(registry.closingJobCount() == 1, "A25: closing state missing");

        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
        maintenance.stop();
        check(maintenance.incompleteDrain(), "A25: an incomplete drain was not reported");
        check(maintenance.statusJson()["incomplete_drain"] == true,
              "A25: status does not expose the incomplete drain");

        registry.clearStoreFaultsForTest();
        check(maintenance.finalize(), "A25: retry after the fault did not finalize");
        check(registry.closingJobCount() == 0, "A25: closing record survived the retry");
    }

    pass_case("A25", "repeat start/stop, blocked-scan stop, failed source start, incomplete drain");
}

// ---------------------------------------------------------------------------
// Legacy/manual registrations: no generation, no source, still maintained
// ---------------------------------------------------------------------------

void case_legacy_retirement(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "legacy")));
    FakeProbes probes;
    probes.set_process(4321, LivenessState::Alive);

    Job job;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    job.JobPIDs = {4321};
    job.CollectorNames = {"cpumem_collector"};
    CommonJobAttr attr;
    attr.auto_update_child = false;
    job.sub_attr = attr;
    check(registry.addJob(job), "legacy: manual registration must succeed");

    const auto records = registry.inventoryRecords();
    check(records.size() == 1, "legacy: the registration created no inventory record");
    const uint64_t id = records.front().internal_id;
    check(!records.front().generation.complete(),
          "legacy: a manual registration must not invent an execution generation");

    // Inventory discovery is disabled (no types, no sources) and the record is
    // still refreshed and retired: reads no longer do it, so maintenance must.
    InventoryMaintenanceConfig config;
    std::vector<IDiscoverySource*> sources;
    JobInventoryMaintenance maintenance(registry, config, std::move(sources));
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "legacy: maintenance must run without discovery sources");
    check(maintenance.statusJson()["configured_types"].empty(),
          "legacy: no discovery type should be configured");

    maintenance.reconcileNow();
    check(registry.findJob(id).has_value(),
          "legacy: a live manual registration was retired");
    check(registry.inventoryRecord(id).has_value() &&
              registry.inventoryRecord(id)->state == "active",
          "legacy: a live registration must stay active");

    probes.set_process(4321, LivenessState::Dead);
    maintenance.reconcileNow();
    check(!registry.findJob(id).has_value(),
          "legacy: a dead manual registration was not retired");
    check(!registry.inventoryRecord(id).has_value(),
          "legacy: the retired record was not archived");
    maintenance.stop();

    pass_case("LEGACY", "manual registrations retire on process evidence without a source");
}

// ---------------------------------------------------------------------------
// Event-driven and on-demand passes (plan 5.2 / 4.3 ensure_registered)
// ---------------------------------------------------------------------------

void case_rescan(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "rescan")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;

    const Observation job =
        make_condor_observation("950.0", 9100, 191919, "htcondor01", "schedd-a", "/cg/slot-95");
    source.set_scan([&]() { return scan_with({job}); });

    std::vector<IDiscoverySource*> sources;
    sources.push_back(&source);
    // A long interval proves the extra passes come from the event request, not
    // from the periodic timer.
    JobInventoryMaintenance maintenance(registry, condor_config(30), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "rescan: maintenance did not start");
    check(find_native(registry, "950.0").has_value(), "rescan: initial scan missing");

    const int scans_after_start = source.scans();
    source.requestRescan();
    check(wait_until([&]() { return source.scans() > scans_after_start; }, 5000),
          "rescan: an event request did not trigger an authoritative pass");

    // On-demand pass used by `ensure_registered`.
    const auto refresh = get_inventory_refresh_provider();
    check(static_cast<bool>(refresh), "rescan: no on-demand refresh provider registered");
    const int scans_before_refresh = source.scans();
    check(refresh(), "rescan: on-demand refresh did not report a completed pass");
    check(source.scans() > scans_before_refresh,
          "rescan: on-demand refresh did not run a scan");

    // Positive termination evidence still retires through the same pass.
    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(9100, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-95", {}, true);
    maintenance.reconcileNow();
    check(!find_native(registry, "950.0").has_value(),
          "rescan: positive termination evidence did not retire the instance");
    maintenance.stop();

    pass_case("RESCAN", "event-driven and on-demand passes run the same reconcile path");
}

// ---------------------------------------------------------------------------
// Exec hints: targeted observation without a full scan (plan 5.2)
// ---------------------------------------------------------------------------

void case_exec_hint(const std::string& base)
{
    JobRegistry registry(isolated(fresh_dir(base, "exec-hint")));
    FakeClock clock;
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;

    // The authoritative pass would find nothing: only the targeted path can
    // create the instance here.
    source.set_scan([&]() { return scan_with({}); });

    const Observation job =
        make_condor_observation("960.0", 9700, 292929, "htcondor01", "schedd-a", "/cg/slot-96");
    source.set_observe([&](pid_t pid) -> std::optional<Observation> {
        if (pid != 4242) return std::nullopt;
        return job;
    });

    std::vector<IDiscoverySource*> sources{&source};
    JobInventoryMaintenance maintenance(registry, condor_config(30), std::move(sources));
    maintenance.setClock(clock.closure());
    maintenance.setLivenessProbes(probes.process_probe(), probes.cgroup_probe());
    check(maintenance.start(), "exec hint: maintenance did not start");
    const int scans_after_start = source.scans();

    maintenance.onExecHint(4242);
    check(wait_until([&]() { return find_native(registry, "960.0").has_value(); }, 5000),
          "exec hint: the targeted observation did not create the instance");
    check(source.observations_for_hint() >= 1,
          "exec hint: the source was never asked for a targeted observation");
    check(source.scans() == scans_after_start,
          "exec hint: resolving one pid must not trigger a full scan");

    // An unresolvable hint is counted, never turned into a record.
    const auto unresolved_before =
        maintenance.statusJson()["unresolved_hints"].get<std::uint64_t>();
    maintenance.onExecHint(9999);
    check(wait_until([&]() {
              return maintenance.statusJson()["unresolved_hints"].get<std::uint64_t>() >
                     unresolved_before;
          }, 5000),
          "exec hint: an unresolvable pid was not counted");
    check(registry.inventoryRecords().size() == 1,
          "exec hint: an unresolvable pid must not create a record");

    maintenance.stop();
    pass_case("EXEC", "exec hints resolve to targeted observations without a full scan");
}

// ---------------------------------------------------------------------------
// Configuration contract (plan section 8)
// ---------------------------------------------------------------------------

void case_config()
{
    // Absent inventory block: opt-in default, interval 30, auto_collect false.
    {
        const auto config = resolve_inventory_maintenance_config(nlohmann::json::object());
        check(config.types.empty(), "config: absent types must default to []");
        check(config.reconcile_interval_sec == 30, "config: interval default is not 30");
        check(config.settings.at(InventoryType::Condor).auto_collect == false,
              "config: auto_collect default is not false");
        check(!config.settings.at(InventoryType::Condor).inventory,
              "config: condor must not be inventoried by default");
    }

    // Field-specific precedence: explicit per-type beats legacy, and declaring
    // the type in inventory.types never erases it.
    {
        nlohmann::json root;
        root["job_registry_config"]["inventory"]["types"] = {"condor", "slurm"};
        root["job_registry_config"]["inventory"]["reconcile_interval_sec"] = "2";
        root["job_registry_config"]["auto_add_condorjob"] = "true";
        root["job_registry_config"]["auto_add_slurmjob"] = "true";
        root["condor_job_watcher"]["auto_collect"] = "false";
        const auto config = resolve_inventory_maintenance_config(root);
        check(config.types.size() == 2, "config: inventory types were not resolved");
        check(config.reconcile_interval_sec == 2, "config: interval was not read");
        check(config.settings.at(InventoryType::Condor).inventory, "config: condor not inventoried");
        check(config.settings.at(InventoryType::Condor).auto_collect_explicit,
              "config: explicit auto_collect not recognised");
        check(!config.settings.at(InventoryType::Condor).auto_collect,
              "config: explicit auto_collect did not win over the legacy flag");
        check(config.settings.at(InventoryType::Slurm).auto_collect,
              "config: legacy auto_add_slurmjob did not resolve");
    }

    // Legacy auto-add folding (user-defined semantics): auto_add_<type>job=true
    // inventories the type and turns automatic collection on with the
    // configured default collectors; use_rules only decides how the list is
    // chosen when adding.
    {
        nlohmann::json root;
        root["job_registry_config"]["auto_add_condorjob"] = true;
        root["condor_job_watcher"]["auto_add_collectors"] = {"cpumem_collector", "io_collector"};
        root["condor_job_watcher"]["use_rules"] = false;
        root["condor_job_watcher"]["cluster_name"] = "htcondor01";
        const auto config = resolve_inventory_maintenance_config(root);
        check(config.types.size() == 1 && config.types.front() == InventoryType::Condor,
              "config: legacy auto_add_condorjob did not fold into inventory");
        const auto& condor = config.settings.at(InventoryType::Condor);
        check(condor.inventory && condor.auto_collect,
              "config: folded type must be inventoried with automatic collection");
        check(condor.legacy_auto_collect, "config: the legacy origin must stay visible");
        check(!condor.use_rules, "config: use_rules defaults to false");
        check(condor.default_collectors.size() == 2 &&
                  condor.default_collectors[0] == "cpumem_collector",
              "config: default collectors were not resolved");
        check(condor.cluster_name == "htcondor01",
              "config: the explicit cluster namespace was not resolved");
    }

    // auto_add=false (or absent) adds nothing and inventories nothing.
    {
        nlohmann::json root;
        root["job_registry_config"]["auto_add_condorjob"] = false;
        const auto config = resolve_inventory_maintenance_config(root);
        check(config.types.empty(),
              "config: auto_add=false must not enable inventory or collection");
        check(!config.settings.at(InventoryType::Condor).auto_collect,
              "config: auto_add=false must not select collectors");
    }

    // An explicit auto_collect beats the folded legacy flag even when the flag
    // is true and the type is not listed explicitly.
    {
        nlohmann::json root;
        root["job_registry_config"]["auto_add_condorjob"] = true;
        root["condor_job_watcher"]["auto_collect"] = false;
        const auto config = resolve_inventory_maintenance_config(root);
        check(config.types.empty() || !config.settings.at(InventoryType::Condor).auto_collect,
              "config: explicit auto_collect=false must win over the fold");
    }

    // Unknown type, bad interval values.
    bool threw = false;
    try {
        nlohmann::json root;
        root["job_registry_config"]["inventory"]["types"] = {"pbs"};
        (void)resolve_inventory_maintenance_config(root);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "config: an unknown inventory type was accepted");

    threw = false;
    try {
        nlohmann::json root;
        root["job_registry_config"]["inventory"]["reconcile_interval_sec"] = "0";
        (void)resolve_inventory_maintenance_config(root);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "config: an interval below 1 second was accepted");

    threw = false;
    try {
        nlohmann::json root;
        root["job_registry_config"]["inventory"]["reconcile_interval_sec"] = "soon";
        (void)resolve_inventory_maintenance_config(root);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "config: a non-numeric interval was accepted");

    threw = false;
    try {
        InventoryMaintenanceConfig config = condor_config();
        config.reconcile_interval_sec = 0;
        std::vector<IDiscoverySource*> sources;
        JobRegistry registry(isolated("/tmp/opencode/joblens-maint-config-check"));
        JobInventoryMaintenance maintenance(registry, config, std::move(sources));
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "controller: an interval below 1 second was accepted at construction");

    pass_case("CONFIG", "section 8 precedence, defaults and startup rejection");
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string base =
        argc > 1 ? std::string(argv[1]) : std::string("/tmp/opencode/joblens-maint-tests");
    std::filesystem::create_directories(base);

    try {
        std::cout << "maintenance test data dir: " << base << '\n';
        case_config();
        case_a08(base);
        case_a09(base);
        case_a10(base);
        case_a11(base);
        case_a12(base);
        case_a13(base);
        case_a14(base);
        case_a23(base);
        case_a25(base);
        case_legacy_retirement(base);
        case_rescan(base);
        case_exec_hint(base);
        std::cout << "ALL MAINTENANCE ASSERTIONS PASSED\n";
    } catch (const std::exception& error) {
        std::cerr << "FAILED: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
