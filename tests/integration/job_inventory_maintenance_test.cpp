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
/* Deterministic inventory-heartbeat acceptance (v2 architecture §2.3).
 *
 * The maintenance controller is gone: every case drives the JobRegistry
 * heartbeat (test sources + injected probes + test facts encoder) and asserts
 * the same behaviors the v1 controller suites pinned:
 *
 *   CONFIG section 8 precedence, defaults and startup rejection
 *   A08  start-before-scan, events injected during a scan are replayed,
 *        duplicate hints do not duplicate records, obsolete hints do not
 *        resurrect an ended generation
 *   A09  one missed scan recovers; two complete absences retire; failed or
 *        incomplete scans never count as absence
 *   A10  empty PID list with a live manager/cgroup does not retire; positive
 *        evidence (manager ended + confirmed empty cgroup) retires
 *   A11  EPERM/unreadable/unresolved input is unknown, not death; incomplete
 *        identities stay visible and retried; no wrong merge
 *   A12  bounded hint queue overflow counts drops, degrades and requests a
 *        rescan; the rescan repairs the dropped hints
 *   A13  tracker-unavailable mode (source started without an event source)
 *   A14  a late acknowledgement for an old generation never removes the new
 *        instance's ownership (real drain-ack path)
 *   A23  reads are side-effect free; snapshots coherent; sentinel excluded
 *   A25  heartbeat stop is idempotent and ordered; a failed source start is
 *        not fatal; an interrupted closing survives a restart and archives
 *   A28  unknown cgroup members are not death evidence; confirmed death retires
 *   LEGACY  manual registrations retire on process evidence without a source
 *   RESCAN  event-driven and on-demand passes run the same reconcile path
 *   EXEC    exec hints resolve to targeted observations without a full scan
 *   EXEC2   an incomplete exec observation (job ad not yet written) retries
 *           on a short ladder instead of waiting for the next reconcile
 */
#include "core/inventory_config.hpp"
#include "core/inventory_status_provider.hpp"
#include "core/job_registry.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
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

std::string fresh_dir(const std::string& base, const std::string& name)
{
    const auto path = std::filesystem::path(base) / name;
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path.string();
}

// ---------------------------------------------------------------------------
// Fake discovery source (an IDiscoverySource with controllable scans/hints)
// ---------------------------------------------------------------------------

class FakeSource final : public IDiscoverySource {
public:
    FakeSource(std::string name, SchedulerType type)
        : name_(std::move(name)), type_(type) {}

    const std::string& name() const override { return name_; }
    SchedulerType schedulerType() const override { return type_; }

    bool start(const DiscoverySink& sink) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++starts_;
            if (start_throw_) throw std::runtime_error("source start failed");
            sink_ = sink;
            started_ = true;
        }
        return start_result_;
    }
    void stop() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stops_;
        started_ = false;
    }

    ScanResult scan() override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!started_) scanned_without_start_ = true;
            ++scans_;
        }
        std::function<ScanResult()> fn;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            fn = scan_fn_;
        }
        return fn ? fn() : ScanResult{};
    }
    SourceHealth health() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return health_;
    }
    void requestRescan() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        health_.rescan_requested = true;
    }
    std::optional<Observation> observeForHint(pid_t hint) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++observations_for_hint_;
        return observe_fn_ ? observe_fn_(hint) : std::nullopt;
    }

    // --- test controls -----------------------------------------------------
    void set_scan(std::function<ScanResult()> fn)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        scan_fn_ = std::move(fn);
    }
    void set_observe(std::function<std::optional<Observation>(pid_t)> fn)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        observe_fn_ = std::move(fn);
    }
    void set_start_result(bool value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        start_result_ = value;
    }
    void set_start_throw(bool value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        start_throw_ = value;
    }
    void set_health_error(const std::string& error)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        health_.state = SourceState::Degraded;
        health_.last_error = error;
    }
    // Delivers one observation through the installed sink (the producing
    // thread; the callback only enqueues).
    void inject(const Observation& observation)
    {
        ObservationSink sink;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            sink = sink_.on_observation;
        }
        if (sink) sink(observation);
    }

    int starts() const { std::lock_guard<std::mutex> lock(mutex_); return starts_; }
    int stops() const { std::lock_guard<std::mutex> lock(mutex_); return stops_; }
    int scans() const { std::lock_guard<std::mutex> lock(mutex_); return scans_; }
    int observations_for_hint() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return observations_for_hint_;
    }
    bool scanned_without_start() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return scanned_without_start_;
    }

private:
    std::string name_;
    SchedulerType type_;
    mutable std::mutex mutex_;
    DiscoverySink sink_;
    std::function<ScanResult()> scan_fn_;
    std::function<std::optional<Observation>(pid_t)> observe_fn_;
    SourceHealth health_;
    bool start_result_ = true;
    bool start_throw_ = false;
    bool started_ = false;
    bool scanned_without_start_ = false;
    int starts_ = 0;
    int stops_ = 0;
    int scans_ = 0;
    int observations_for_hint_ = 0;
};

// ---------------------------------------------------------------------------
// Fake liveness probes
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
            if (it->second.state != LivenessState::Alive)
                return LivenessResult{it->second.state, "fake"};
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

// The observation -> facts encoding the adapters normally provide. The store
// normalizes identity fields, so the shape matches the v1 fallback encoder.
Job encode_test_facts(const Observation& observation)
{
    Job facts;
    facts.jobtype = JobType::Job;
    if (observation.scheduler_type == SchedulerType::Condor) {
        facts.subtype = JobSubType::Condor;
        CondorJobAttr attributes{};
        attributes.auto_update_child = false;
        attributes.starter_pid = observation.generation.manager_pid;
        attributes.scheduler_host = observation.scheduler_host;
        attributes.scheduler_name = observation.selector.cluster_tag;
        attributes.slots_cgroup_path = observation.cgroup_path;
        attributes.owner = observation.owner;
        attributes.collector_host = observation.selector.cluster_name;
        facts.sub_attr = attributes;
    } else if (observation.scheduler_type == SchedulerType::Slurm) {
        facts.subtype = JobSubType::Slurm;
        SlurmJobAttr attributes{};
        attributes.auto_update_child = false;
        attributes.stepd_pid = observation.generation.manager_pid;
        attributes.cluster_name = observation.selector.cluster_name;
        attributes.cgroup_path = observation.cgroup_path;
        attributes.user = observation.owner;
        facts.sub_attr = attributes;
    }
    return facts;
}

JobRegistryOptions heartbeat_options(const std::string& path, FakeSource& source,
                                     FakeProbes& probes)
{
    JobRegistryOptions options;
    options.db_path = path;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    options.enable_inventory_maintenance = true;
    options.test_sources = {{"condor", &source}};
    options.test_process_probe = probes.process_probe();
    options.test_cgroup_probe = probes.cgroup_probe();
    options.test_facts_encoder = encode_test_facts;
    options.test_heartbeat = true;
    return options;
}

// Heartbeat without any discovery source (legacy/manual maintenance only).
JobRegistryOptions legacy_heartbeat_options(const std::string& path, FakeProbes& probes)
{
    JobRegistryOptions options;
    options.db_path = path;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    options.enable_inventory_maintenance = true;
    options.test_process_probe = probes.process_probe();
    options.test_cgroup_probe = probes.cgroup_probe();
    options.test_facts_encoder = encode_test_facts;
    options.test_heartbeat = true;
    return options;
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

std::optional<JobRecord> find_native(JobRegistry& registry, const std::string& native,
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

nlohmann::json heartbeat_status()
{
    const auto provider = get_inventory_status_provider();
    check(static_cast<bool>(provider), "the heartbeat status provider is not published");
    return provider();
}

template <typename Predicate>
bool wait_until(Predicate predicate, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

// Waits until the heartbeat drained its queued hints, so an explicit pass
// evaluates retirements against an empty observation set.
void wait_hb_quiet()
{
    wait_until([] {
        const auto provider = get_inventory_status_provider();
        return !provider || provider()["queue_depth"].get<std::size_t>() == 0;
    }, 3000);
}


// ---------------------------------------------------------------------------
// CONFIG - section 8 precedence (unchanged contract)
// ---------------------------------------------------------------------------

void case_config()
{
    {
        const auto config = resolve_inventory_maintenance_config(nlohmann::json::object());
        check(config.types.empty(), "config: absent types must default to []");
        check(config.reconcile_interval_sec == 30, "config: default interval is 30");
        check(!config.settings.at(InventoryType::Condor).auto_collect,
              "config: auto_collect defaults to false");
    }
    {
        const auto config = resolve_inventory_maintenance_config(nlohmann::json{
            {"job_registry_config", nlohmann::json{
                {"inventory", nlohmann::json{
                    {"types", nlohmann::json::array({"condor", "slurm"})},
                    {"reconcile_interval_sec", 5},
                }},
            }},
            {"condor_job_watcher", nlohmann::json{
                {"auto_collect", true},
                {"use_rules", false},
                {"auto_add_collectors", nlohmann::json::array({"cpumem_collector"})},
                {"cluster_name", "htcondor01"},
            }},
            {"slurm_job_watcher", nlohmann::json{
                {"use_rules", true},
                {"rules_dir", "/tmp/rules"},
                {"rules_prefix", "slurm_job_"},
            }},
        });
        check(config.types.size() == 2, "config: both types resolved");
        check(config.reconcile_interval_sec == 5, "config: explicit interval wins");
        const auto& condor = config.settings.at(InventoryType::Condor);
        check(condor.auto_collect && !condor.use_rules &&
                  condor.default_collectors.size() == 1,
              "config: explicit condor settings win");
        const auto& slurm = config.settings.at(InventoryType::Slurm);
        check(!slurm.auto_collect && slurm.use_rules && slurm.rules_dir == "/tmp/rules",
              "config: slurm rules settings resolved");
    }
    {
        // Legacy folding: auto_add_condorjob=true implies inventory + auto.
        const auto config = resolve_inventory_maintenance_config(nlohmann::json{
            {"job_registry_config", nlohmann::json{{"auto_add_condorjob", true}}},
        });
        check(config.types.size() == 1 && config.types.front() == InventoryType::Condor,
              "config: legacy auto_add folds into inventory discovery");
        check(config.settings.at(InventoryType::Condor).auto_collect,
              "config: legacy folding turns auto collection on");
    }
    {
        bool threw = false;
        try {
            (void)resolve_inventory_maintenance_config(nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"inventory", nlohmann::json{
                        {"types", nlohmann::json::array({"bogus"})}},
                }},
            }});
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        check(threw, "config: an unknown type must reject startup");
    }
    {
        bool threw = false;
        try {
            (void)resolve_inventory_maintenance_config(nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"inventory", nlohmann::json{{"reconcile_interval_sec", 0}}},
                }},
            });
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        check(threw, "config: an interval below one second must reject startup");
    }
    pass_case("CONFIG", "section 8 precedence, defaults and startup rejection");
}

// ---------------------------------------------------------------------------
// A08 - start before scan, in-scan event replay, duplicate/obsolete hints
// ---------------------------------------------------------------------------

void run_a08(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    const Observation scanned =
        make_condor_observation("100.0", 5100, 554, "htcondor01", "schedd-a", "/cg/slot-2");
    probes.set_process(5100, LivenessState::Alive, 554);
    source.set_scan([&]() { return scan_with({scanned}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a08"), source, probes));

    check(source.scans() == 1, "A08: the initial full scan did not run");
    check(!source.scanned_without_start(), "A08: scan ran before the source was started");
    check(source.starts() == 1, "A08: the source was not started exactly once");
    check(find_native(registry, "100.0").has_value(), "A08: scan observation missing");

    // An event buffered during the initial pass is committed by the pass that
    // drains it, through the same upsert path.
    const Observation injected =
        make_condor_observation("101.0", 5101, 555, "htcondor01", "schedd-a", "/cg/slot-2");
    source.inject(injected);
    check(wait_until([&]() { return find_native(registry, "101.0").has_value(); }, 5000),
          "A08: buffered event during the scan was not merged");
    check(count_records(registry) == 2, "A08: duplicate hints duplicated records");

    const auto before = find_native(registry, "101.0");
    check(before.has_value(), "A08: injected record vanished");
    source.inject(injected);   // duplicate hint for the same generation
    check(wait_until([&]() {
              auto record = find_native(registry, "101.0");
              return record.has_value() &&
                     record->internal_id == before->internal_id;
          }, 3000),
          "A08: duplicate hint changed the internal id");
    check(count_records(registry) == 2, "A08: duplicate hints duplicated records");

    // Termination + obsolete hint: a hint for an ended generation must not
    // resurrect it.
    wait_hb_quiet();
    probes.set_process(5101, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-2", {}, true);
    source.set_scan([&]() { return scan_with({}); });
    check(registry.runInventoryPass(true), "A08: the retirement pass did not run");
    check(!find_native(registry, "101.0").has_value(),
          "A08: terminated execution not retired");
    source.inject(injected);
    wait_hb_quiet();
    registry.runInventoryPass(true);
    check(!find_native(registry, "101.0").has_value(),
          "A08: obsolete hint resurrected an ended generation");

    // A new generation for the same identity registers as a new instance.
    const Observation revived =
        make_condor_observation("101.0", 5102, 556, "htcondor01", "schedd-a", "/cg/slot-2");
    probes.set_process(5102, LivenessState::Alive, 556);
    source.set_scan([&]() { return scan_with({revived}); });
    check(registry.runInventoryPass(true), "A08: the revival pass did not run");
    const auto revived_record = find_native(registry, "101.0");
    check(revived_record.has_value() && revived_record->generation.manager_pid == 5102,
          "A08: a new generation was rejected after retirement");
    check(count_records(registry) == 2, "A08: duplicate entry for one generation");

    registry.deinit();
    pass_case("A08", "start-before-scan, in-scan event replay, duplicate hints, no resurrection");
}

// ---------------------------------------------------------------------------
// A09 - absence accounting
// ---------------------------------------------------------------------------

void run_a09(const std::string& base)
{
    // Scenario 1: one missed scan recovers; only a second retires.
    {
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        const Observation job =
            make_condor_observation("200.0", 5200, 666, "htcondor01", "schedd-a", "/cg/slot-1");
        int pass = 0;
        source.set_scan([&]() {
            ++pass;
            if (pass == 1) return scan_with({job});
            if (pass == 2) return scan_with({});
            return scan_with({job});
        });
        probes.set_process(5200, LivenessState::Dead);
        probes.unset_cgroup("/cg/slot-1");   // unreadable: absence-only retirement
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a09-recover"), source, probes));
        check(registry.runInventoryPass(true), "A09: pass 2 did not run");
        check(find_native(registry, "200.0").has_value(),
              "A09: one missed scan retired the instance");
        check(registry.runInventoryPass(true), "A09: pass 3 did not run");
        check(find_native(registry, "200.0").has_value(),
              "A09: recovered instance was retired");
        registry.deinit();
    }

    // Scenario 2: two complete absences with positive evidence retire.
    {
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        const Observation job =
            make_condor_observation("210.0", 5301, 777, "htcondor01", "schedd-a", "/cg/slot-2");
        int pass = 0;
        source.set_scan([&]() {
            ++pass;
            return pass == 1 ? scan_with({job}) : scan_with({});
        });
        probes.set_process(5301, LivenessState::Dead);
        probes.unset_cgroup("/cg/slot-2");   // unreadable: absence-only retirement
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a09-retire"), source, probes));
        registry.runInventoryPass(true);   // complete miss #1
        check(find_native(registry, "210.0").has_value(),
              "A09: retirement happened after a single absence");
        check(heartbeat_status()["suspect_count"] == 1,
              "A09: an absent instance was not reported suspect");
        registry.runInventoryPass(true);
        check(!find_native(registry, "210.0").has_value(),
              "A09: two complete absences did not retire");
        registry.deinit();
    }

    // Scenario 3: failed/incomplete scans never count as absence.
    {
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
        probes.set_process(5300, LivenessState::Dead);
        probes.unset_cgroup("/cg/slot-3");
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a09-failed"), source, probes));
        registry.runInventoryPass(true);   // pass 2: failed
        registry.runInventoryPass(true);   // pass 3: failed
        check(find_native(registry, "220.0").has_value(),
              "A09: failed scans were treated as absence");
        check(heartbeat_status()["suspect_count"] == 0,
              "A09: failed scans marked the instance suspect");
        registry.runInventoryPass(true);   // pass 4: complete miss #1
        check(find_native(registry, "220.0").has_value(),
              "A09: retirement after one complete absence");
        check(registry.runInventoryPass(true), "A09: pass 5 did not run");   // complete miss #2
        check(!find_native(registry, "220.0").has_value(),
              "A09: two complete absences after failures did not retire");
        registry.deinit();
    }

    pass_case("A09", "recovery after one miss, retirement after two complete absences only");
}

// ---------------------------------------------------------------------------
// A10 - empty PID window vs positive termination evidence
// ---------------------------------------------------------------------------

void run_a10(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    const Observation job =
        make_condor_observation("300.0", 5400, 777, "htcondor01", "schedd-a", "/cg/slot-4");
    source.set_scan([&]() { return scan_with({job}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a10"), source, probes));

    // The execution is really running: manager alive, cgroup has a member. A
    // later scan sees no job, and the record's PID list is empty (the window
    // between manager exit and member reaping).
    probes.set_process(5400, LivenessState::Alive, 777);
    probes.set_process(6001, LivenessState::Alive, 777);
    probes.set_cgroup("/cg/slot-4", {6001}, true);
    const auto record = find_native(registry, "300.0");
    check(record.has_value(), "A10: record missing");
    check(record->members.empty(), "A10: fixture should have an empty PID list");

    source.set_scan([&]() { return scan_with({}); });
    registry.runInventoryPass(true);
    registry.runInventoryPass(true);
    registry.runInventoryPass(true);
    check(find_native(registry, "300.0").has_value(),
          "A10: an empty PID list retired a live execution");

    // Positive evidence: manager ended and the execution cgroup is empty. An
    // exit hint is enough: the targeted pass retires it without any source
    // read and without any collector involvement.
    probes.set_process(5400, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-4", {}, true);
    const int scans_before = source.scans();
    registry.notifyExit(5400);
    check(wait_until([&]() { return !find_native(registry, "300.0").has_value(); }, 3000),
          "A10: positive termination evidence did not retire");
    check(source.scans() == scans_before,
          "A10: retirement needed a source scan instead of targeted evidence");

    registry.deinit();
    pass_case("A10", "no premature retirement on an empty PID window; positive evidence retires");
}

// ---------------------------------------------------------------------------
// A11 - unknown is not death, incomplete identities are retried
// ---------------------------------------------------------------------------

void run_a11(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a11"), source, probes));
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

    // EPERM on the manager probe: unknown, not death, even after two complete
    // absences.
    probes.set_process(5500, LivenessState::Unknown);
    probes.set_cgroup("/cg/slot-5", {}, false);   // unreadable cgroup
    registry.runInventoryPass(true);
    registry.runInventoryPass(true);
    registry.runInventoryPass(true);
    check(find_native(registry, "400.0").has_value(),
          "A11: an unreadable liveness check was treated as death");

    // An unresolved namespace cannot be indexed, is counted as incomplete and
    // is retried once the namespace resolves.
    const Observation unresolved = make_unresolved_observation("401.0", 5501);
    check(!unresolved.identity_complete, "A11: fixture should be incomplete");
    registry.onDiscoveryObservation(unresolved);
    registry.runInventoryPass(true);
    check(!find_native(registry, "401.0").has_value(),
          "A11: an unresolved identity was indexed");
    check(heartbeat_status()["incomplete_observations"].get<std::uint64_t>() >= 1,
          "A11: incomplete observation was not reported");
    check(count_records(registry) == 1, "A11: an incomplete identity created a record");

    const Observation resolved =
        make_condor_observation("401.0", 5501, 999, "htcondor01", "schedd-a", "/cg/slot-6");
    // The manager is live; the empty scans must not retire it (absence is not
    // the subject of this case).
    probes.set_process(5501, LivenessState::Alive, 999);
    registry.onDiscoveryObservation(resolved);
    registry.runInventoryPass(true);
    check(find_native(registry, "401.0").has_value(),
          "A11: a retried complete identity was not indexed");
    check(count_records(registry) == 2, "A11: retry duplicated or merged records");

    // A namespace-less observation must never merge into another namespace.
    registry.onDiscoveryObservation(make_unresolved_observation("400.0", 5502));
    registry.runInventoryPass(true);
    check(count_records(registry) == 2, "A11: an unresolved identity merged into a record");

    // A second cluster namespace with the same native id stays distinct.
    const Observation other_cluster =
        make_condor_observation("400.0", 5600, 1111, "htcondor02", "schedd-b", "/cg/slot-7");
    probes.set_process(5600, LivenessState::Alive, 1111);
    registry.onDiscoveryObservation(other_cluster);
    registry.runInventoryPass(true);
    check(count_records(registry) == 3, "A11: namespaces merged");
    check(find_native(registry, "400.0", "htcondor02", "schedd-b").has_value(),
          "A11: second namespace record missing");
    check(find_native(registry, "400.0")->generation.manager_pid == 5500,
          "A11: wrong record was updated");

    registry.deinit();
    pass_case("A11", "EPERM/unknown is not death; incomplete identities visible and retried");
}

// ---------------------------------------------------------------------------
// A12 - bounded queue overflow, degradation, rescan, repair
// ---------------------------------------------------------------------------

void run_a12(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a12"), source, probes));

    const Observation alpha =
        make_condor_observation("500.0", 5700, 1212, "htcondor01", "schedd-a", "/cg/slot-8");
    const Observation beta =
        make_condor_observation("501.0", 5701, 1313, "htcondor01", "schedd-a", "/cg/slot-9");

    // Block a pass inside the source scan, then overflow the queue from the
    // producer side.
    std::promise<void> scan_entered;
    std::promise<void> release_scan;
    auto gate = release_scan.get_future().share();
    source.set_scan([&]() {
        scan_entered.set_value();
        gate.wait();
        return scan_with({});
    });

    std::thread reconciler([&]() { registry.reconcileInventoryNow(); });
    scan_entered.get_future().wait();

    const int rescans_before = heartbeat_status()["rescan_requests"].get<std::uint64_t>();
    for (int index = 0; index < 600; ++index) registry.onDiscoveryObservation(alpha);

    const auto degraded = heartbeat_status();
    check(degraded["events_dropped"].get<std::uint64_t>() >= 1,
          "A12: queue overflow did not count drops");
    check(degraded["queue_depth"].get<std::size_t>() <= 512,
          "A12: the hint queue was not bounded");
    check(degraded["state"] == "degraded", "A12: overflow did not degrade the source state");
    check(degraded["rescan_requests"].get<std::uint64_t>() >= static_cast<std::uint64_t>(rescans_before) + 1,
          "A12: overflow did not request a rescan");

    release_scan.set_value();
    reconciler.join();

    // The authoritative rescan repairs whatever the dropped hints carried.
    source.set_scan([&]() { return scan_with({alpha, beta}); });
    check(wait_until([&]() { return count_records(registry) == 2; }, 5000),
          "A12: the rescan did not repair the dropped hints");

    registry.deinit();
    pass_case("A12", "bounded queue drops, degradation, rescan request and scan repair");
}

// ---------------------------------------------------------------------------
// A13 - source started without an event source (scan-only)
// ---------------------------------------------------------------------------

void run_a13(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    auto options = heartbeat_options(fresh_dir(base, "a13"), source, probes);
    source.set_start_result(false);   // scan-only source
    JobRegistry registry(options);

    const auto status = heartbeat_status();
    check(status["tracker_unavailable"] == true,
          "A13: a scan-only source was not reported");
    check(status["state"] != "stopped", "A13: the heartbeat did not run");

    const Observation job =
        make_condor_observation("600.0", 5800, 1414, "htcondor01", "schedd-a", "/cg/slot-10");
    bool first = true;
    source.set_scan([&]() {
        if (first) {
            first = false;
            return scan_with({job});
        }
        return scan_with({});
    });

    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(5800, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-10", {}, true);
    registry.runInventoryPass(true);
    check(!find_native(registry, "600.0").has_value(),
          "A13: retirement did not work in tracker-unavailable mode");

    registry.deinit();
    pass_case("A13", "tracker unavailable: maintenance and retirement continue, health reports it");
}

// ---------------------------------------------------------------------------
// A14 - old closing record vs a new instance reusing pid/cgroup
// ---------------------------------------------------------------------------

void run_a14(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    const Observation old_job =
        make_condor_observation("700.0", 5900, 1515, "htcondor01", "schedd-a", "/cg/slot-11");
    source.set_scan([&]() { return scan_with({old_job}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a14"), source, probes));

    // A registered lifecycle observer makes retirements go through the
    // closing state instead of archiving immediately.
    std::mutex observer_mutex;
    std::vector<uint64_t> removed;
    registry.addLifecycleCb([&](JobEvent event, const Job& job) {
        if (event != JobEvent::Removed) return;
        std::lock_guard<std::mutex> lock(observer_mutex);
        removed.push_back(job.JobID);
    });

    const auto old_record = find_native(registry, "700.0");
    check(old_record.has_value(), "A14: old record missing");

    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(5900, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-11", {}, true);
    registry.runInventoryPass(true);
    check(registry.closingJobCount() == 1,
          "A14: retirement did not enter the closing state");
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
    registry.runInventoryPass(true);

    const auto new_record = find_native(registry, "700.1");
    check(new_record.has_value(), "A14: new instance was not registered");
    check(new_record->internal_id != old_record->internal_id,
          "A14: new instance reused the retired internal id");

    // The collector drain acknowledges the removal: this is the real
    // production ack path, and it must never touch the new ownership.
    check(registry.acknowledgeJobRemoval(old_record->internal_id),
          "A14: the drain acknowledgement failed");
    check(registry.closingJobCount() == 0, "A14: old closing record was not finalized");
    check(registry.inventoryRecord(new_record->internal_id).has_value(),
          "A14: old acknowledgement deleted the new instance");
    const auto still_new = find_native(registry, "700.1");
    check(still_new.has_value() && still_new->internal_id == new_record->internal_id,
          "A14: old acknowledgement removed the new identity index");
    check(still_new->generation.manager_start_ticks == 1616,
          "A14: new generation was replaced by stale state");

    // A stale hint for the retired generation still cannot resurrect it.
    registry.onDiscoveryObservation(old_job);
    registry.runInventoryPass(true);
    check(!find_native(registry, "700.0").has_value(),
          "A14: a stale hint resurrected the retired generation");
    check(find_native(registry, "700.1").has_value(),
          "A14: stale hint disturbed the new instance");

    registry.deinit();
    pass_case("A14", "late cleanup of an old generation cannot remove new ownership");
}

// ---------------------------------------------------------------------------
// A23 - reads are side-effect free and snapshots are coherent
// ---------------------------------------------------------------------------

void run_a23(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a23"), source, probes));

    const Observation first =
        make_condor_observation("800.0", 6000, 1717, "htcondor01", "schedd-a", "/cg/slot-12");
    const Observation second =
        make_condor_observation("801.0", 6001, 1818, "htcondor01", "schedd-a", "/cg/slot-13");
    std::atomic<int> mutate{0};
    source.set_scan([&]() {
        if (mutate.load() % 2 == 0) return scan_with({first, second});
        return scan_with({first});
    });

    probes.set_process(6000, LivenessState::Alive, 1717);
    probes.set_process(6001, LivenessState::Alive, 1818);

    // Quiet phase: reads alone must not advance any heartbeat counter.
    const auto before = heartbeat_status();
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
        (void)heartbeat_status();
    }
    const auto after = heartbeat_status();
    check(before["pass_count"] == after["pass_count"],
          "A23: reads triggered a maintenance pass");
    check(before["retirements"] == after["retirements"],
          "A23: reads triggered retirements");
    check(before["events_received"] == after["events_received"],
          "A23: reads emitted events");
    check(before["last_scan_start_ms"] == after["last_scan_start_ms"],
          "A23: reads changed scan timestamps");
    check(before["queue_depth"] == after["queue_depth"], "A23: reads changed the queue");

    // Concurrent phase: the heartbeat mutates while reads keep observing.
    std::atomic<bool> stop{false};
    std::thread reader([&]() {
        while (!stop.load()) {
            const auto records = registry.inventoryRecords();
            if (records.size() > 5u) throw std::runtime_error("A23: duplicate records appeared");
            for (const auto& record : records) {
                check(record.internal_id != 0, "A23: the system sentinel leaked into inventory");
                if (!record.identity_complete) continue;
                const auto by_identity = registry.inventoryRecordByIdentity(record.identity);
                check(by_identity.has_value(), "A23: snapshot references a missing identity");
                check(by_identity->internal_id == record.internal_id,
                      "A23: snapshot and identity index disagree");
            }
            (void)heartbeat_status();
        }
    });
    for (int index = 0; index < 40; ++index) {
        mutate.store(index);
        registry.runInventoryPass(true);
    }
    stop.store(true);
    reader.join();

    const auto final_records = registry.inventoryRecords();
    for (const auto& record : final_records) {
        check(record.internal_id != 0, "A23: sentinel present in the final snapshot");
    }
    check(count_records(registry) == 2, "A23: mutation loop lost an instance");
    registry.deinit();
    pass_case("A23", "reads are side-effect free; snapshots coherent; sentinel excluded");
}

// ---------------------------------------------------------------------------
// A25 - stop safety, failed source start, interrupted closing across restart
// ---------------------------------------------------------------------------

void run_a25(const std::string& base)
{
    // stopInventoryDiscovery is idempotent and keeps the status visible while
    // records drain; deinit retracts the providers.
    {
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a25-repeat"), source, probes));

        check(static_cast<bool>(get_inventory_status_provider()),
              "A25: status provider was not published");
        check(heartbeat_status()["state"] != "stopped",
              "A25: status reports stopped while running");
        check(source.starts() == 1, "A25: source start count mismatch");

        registry.stopInventoryDiscovery();
        registry.stopInventoryDiscovery();
        check(static_cast<bool>(get_inventory_status_provider()),
              "A25: status provider was not kept while draining");
        check(source.stops() == 1, "A25: source stop count mismatch");

        check(registry.deinit(), "A25: deinit must succeed");
        check(!get_inventory_status_provider(), "A25: status provider was not cleared");
        check(registry.deinit(), "A25: a second deinit must keep the result");
        check(source.starts() == 1 && source.stops() == 1,
              "A25: sources must not restart after discovery stopped");
    }

    // Stop while a pass is blocked inside a source scan stays bounded.
    {
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        auto registry = std::make_unique<JobRegistry>(
            heartbeat_options(fresh_dir(base, "a25-blocked"), source, probes));

        std::promise<void> scan_entered;
        std::promise<void> release_scan;
        auto gate = release_scan.get_future().share();
        source.set_scan([&]() {
            scan_entered.set_value();
            gate.wait();
            return scan_with({});
        });
        std::thread reconciler([&]() { registry->reconcileInventoryNow(); });
        scan_entered.get_future().wait();

        std::thread stopper([&]() { registry->stopInventoryDiscovery(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        release_scan.set_value();
        reconciler.join();
        stopper.join();
        check(!get_inventory_status_provider() ||
                  heartbeat_status()["state"] == "stopped",
              "A25: stop during a blocked scan left the heartbeat running");

        // Nothing may touch the store after it is closed: destroy the
        // registry and verify no crash and no dangling provider.
        registry.reset();
        check(!get_inventory_status_provider(), "A25: provider survived registry teardown");
    }

    // Source initialization failure is not fatal.
    {
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        source.set_start_throw(true);
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a25-startfail"),
                                               source, probes));
        check(heartbeat_status()["tracker_unavailable"] == true,
              "A25: a failed source start was not reported");
        registry.deinit();
        check(!get_inventory_status_provider(), "A25: provider survived deinit");
    }

    // Interrupted closing: the archive fails, shutdown reports it, and a
    // restart on the same store archives the restored closing record.
    {
        const auto path = fresh_dir(base, "a25-drain");
        FakeSource source("condor", SchedulerType::Condor);
        FakeProbes probes;
        std::mutex observer_mutex;
        std::vector<uint64_t> removed;
        uint64_t closing_id = 0;
        {
            const Observation job = make_condor_observation(
                "900.0", 6100, 1919, "htcondor01", "schedd-a", "/cg/slot-14");
            source.set_scan([&]() { return scan_with({job}); });
            JobRegistry registry(heartbeat_options(path, source, probes));
            registry.addLifecycleCb([&](JobEvent event, const Job& job) {
                if (event != JobEvent::Removed) return;
                std::lock_guard<std::mutex> lock(observer_mutex);
                removed.push_back(job.JobID);
            });

            const auto created = find_native(registry, "900.0");
            check(created.has_value(), "A25: the observation was not registered");
            closing_id = created->internal_id;

            source.set_scan([&]() { return scan_with({}); });
            probes.set_process(6100, LivenessState::Dead);
            probes.set_cgroup("/cg/slot-14", {}, true);
            registry.runInventoryPass(true);
            check(registry.closingJobCount() == 1, "A25: closing state missing");

            // Both archive attempts (the closing cleanup and the drain ack)
            // must fail so the interrupted closing survives the shutdown.
            registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, 0, 2);
            check(!registry.deinit(),
                  "A25: an incomplete drain was not reported");
        }

        {
            // Restart: the interrupted retirement is restored as closing and
            // the cleanup archives it.
            FakeSource source2("condor", SchedulerType::Condor);
            JobRegistry registry(heartbeat_options(path, source2, probes));
            const auto record = registry.inventoryRecord(closing_id);
            check(record.has_value() && record->state == "closing",
                  "A25: the interrupted closing was not restored");
            check(registry.deinit(), "A25: retry after the fault did not finalize");
            check(registry.closingJobCount() == 0,
                  "A25: closing record survived the retry");
        }
    }

    pass_case("A25", "heartbeat stop is ordered and idempotent; a failed source start is "
                     "not fatal; an interrupted closing survives a restart");
}

// ---------------------------------------------------------------------------
// A28 - a readable but unprobeable cgroup is not an empty cgroup
// ---------------------------------------------------------------------------

void run_a28(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a28"), source, probes));
    const Observation job =
        make_condor_observation("600.0", 5800, 1414, "htcondor01", "schedd-a", "/cg/slot-10");

    bool first = true;
    source.set_scan([&]() {
        if (first) {
            first = false;
            return scan_with({job});
        }
        return scan_with({});
    });

    // The manager exited, the cgroup still lists a member, but the member's
    // stat is unreadable (Unknown). "Readable and non-empty" is never "empty",
    // so the first pass must not retire — the old rule treated Unknown members
    // as dead and retired a possibly-running execution.
    probes.set_process(5800, LivenessState::Dead);
    probes.set_process(6101, LivenessState::Unknown);
    probes.set_cgroup("/cg/slot-10", {6101}, true);
    registry.runInventoryPass(true);
    registry.runInventoryPass(true);
    registry.runInventoryPass(true);
    check(find_native(registry, "600.0").has_value(),
          "A28: unknown cgroup members were read as death evidence");

    // Once the member reads as dead the execution is conclusively gone: the
    // cgroup member is dead and no other member evidence exists.
    probes.set_process(6101, LivenessState::Dead);
    registry.runInventoryPass(true);
    check(!find_native(registry, "600.0").has_value(),
          "A28: a conclusively dead execution was not retired");

    registry.deinit();
    pass_case("A28", "unknown cgroup members are not death evidence; confirmed death retires");
}

// ---------------------------------------------------------------------------
// Legacy/manual registrations: no generation, no source, still maintained
// ---------------------------------------------------------------------------

void run_legacy(const std::string& base)
{
    FakeProbes probes;
    probes.set_process(4321, LivenessState::Alive);

    JobRegistry registry(legacy_heartbeat_options(fresh_dir(base, "legacy"), probes));

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

    check(heartbeat_status()["configured_types"].empty(),
          "legacy: no discovery type should be configured");

    check(registry.runInventoryPass(true), "legacy: the pass did not run");
    check(registry.findJob(id).has_value(),
          "legacy: a live manual registration was retired");
    check(registry.inventoryRecord(id).has_value() &&
              registry.inventoryRecord(id)->state == "active",
          "legacy: a live registration must stay active");

    probes.set_process(4321, LivenessState::Dead);
    check(registry.runInventoryPass(true), "legacy: the retirement pass did not run");
    check(!registry.findJob(id).has_value(),
          "legacy: a dead manual registration was not retired");
    check(!registry.inventoryRecord(id).has_value(),
          "legacy: the retired record was not archived");
    registry.deinit();

    pass_case("LEGACY", "manual registrations retire on process evidence without a source");
}

// ---------------------------------------------------------------------------
// RESCAN - event-driven and on-demand passes
// ---------------------------------------------------------------------------

void run_rescan(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    const Observation job =
        make_condor_observation("950.0", 9100, 191919, "htcondor01", "schedd-a", "/cg/slot-95");
    source.set_scan([&]() { return scan_with({job}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "rescan"), source, probes));
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
    registry.runInventoryPass(true);
    check(!find_native(registry, "950.0").has_value(),
          "rescan: positive termination evidence did not retire the instance");
    registry.deinit();

    pass_case("RESCAN", "event-driven and on-demand passes run the same reconcile path");
}

// ---------------------------------------------------------------------------
// EXEC - exec hints: targeted observation without a full scan
// ---------------------------------------------------------------------------

void run_exec_hint(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "exec-hint"), source, probes));

    // The authoritative pass would find nothing: only the targeted path can
    // create the instance here.
    source.set_scan([&]() { return scan_with({}); });

    const Observation job =
        make_condor_observation("960.0", 9700, 292929, "htcondor01", "schedd-a", "/cg/slot-96");
    source.set_observe([&](pid_t pid) -> std::optional<Observation> {
        if (pid != 4242) return std::nullopt;
        return job;
    });

    const int scans_after_start = source.scans();

    registry.onDiscoveryExecHint(4242);
    check(wait_until([&]() { return find_native(registry, "960.0").has_value(); }, 5000),
          "exec hint: the targeted observation did not create the instance");
    check(source.observations_for_hint() >= 1,
          "exec hint: the source was never asked for a targeted observation");
    check(source.scans() == scans_after_start,
          "exec hint: resolving one pid must not trigger a full scan");

    // An unresolvable hint is counted, never turned into a record.
    const auto unresolved_before =
        heartbeat_status()["unresolved_hints"].get<std::uint64_t>();
    registry.onDiscoveryExecHint(9999);
    check(wait_until([&]() {
              return heartbeat_status()["unresolved_hints"].get<std::uint64_t>() >
                     unresolved_before;
          }, 5000),
          "exec hint: an unresolvable pid was not counted");
    check(registry.inventoryRecords().size() == 1,
          "exec hint: an unresolvable pid must not create a record");

    registry.deinit();
    pass_case("EXEC", "exec hints resolve to targeted observations without a full scan");
}

// EXEC2 - the exec-hint fast path retries incomplete observations (scheduler
// state not yet written at exec time) on a short ladder before any rescan.
void run_exec_hint_retry(const std::string& base)
{
    FakeSource source("condor", SchedulerType::Condor);
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "exec-retry"), source, probes));

    // The authoritative pass would find nothing: only the retry ladder can
    // create the instance here.
    source.set_scan([&]() { return scan_with({}); });
    const int scans_after_start = source.scans();

    int observe_attempts = 0;
    source.set_observe([&](pid_t pid) -> std::optional<Observation> {
        if (pid != 4242) return std::nullopt;
        ++observe_attempts;
        Observation observation = make_condor_observation("961.0", 9701, 292930,
                                                          "htcondor01", "schedd-a", "/cg/slot-97");
        if (observe_attempts == 1) {
            // The job ad is not written yet at exec time: the native id is
            // unknown, so the observation is explicitly incomplete and must
            // not create a record.
            observation.identity_complete = false;
            observation.canonical_identity.reset();
            observation.selector.native_job_id.clear();
            observation.incomplete_reason = "condor job ad unavailable";
        }
        return observation;
    });

    registry.onDiscoveryExecHint(4242);
    check(wait_until([&]() { return find_native(registry, "961.0").has_value(); }, 5000),
          "exec retry: the incomplete observation was never retried to completion");
    check(observe_attempts >= 2,
          "exec retry: the source was not re-observed after the incomplete pass");
    check(source.observations_for_hint() >= 2,
          "exec retry: the re-observation did not go through the hint path");
    check(source.scans() == scans_after_start,
          "exec retry: the ladder must resolve without a full scan");

    registry.deinit();
    pass_case("EXEC2", "incomplete exec observations retry before any rescan");
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string base =
        argc > 1 ? std::string(argv[1]) : std::string("/tmp/joblens-heartbeat-test");
    std::filesystem::create_directories(base);
    try {
        case_config();
        run_a08(base);
        run_a09(base);
        run_a10(base);
        run_a11(base);
        run_a12(base);
        run_a13(base);
        run_a14(base);
        run_a23(base);
        run_a25(base);
        run_a28(base);
        run_legacy(base);
        run_rescan(base);
        run_exec_hint(base);
        run_exec_hint_retry(base);
        std::cout << "ALL PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
