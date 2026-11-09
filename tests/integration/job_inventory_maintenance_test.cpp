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
 *   RETIRE-EXPLICIT an explicit retirement marks the ended generation; a
 *        late same-generation observation is refused, a new one registers
 *   ADMIT  the inventory capture_all admission gate: S1/S3 combinations,
 *        drop/reject counters, NotAdmitted on the event path
 *   STOP-PIN a manual empty selection survives scan rotation and fact
 *        changes; automatic policy never re-pins collectors over a stop
 *   LEGACY  manual registrations retire on process evidence without a source
 *   RESCAN  event-driven and on-demand passes run the same reconcile path
 *   EXEC    exec hints resolve to targeted observations without a full scan
 *   EXEC2   an incomplete exec observation (job ad not yet written) retries
 *           on a short ladder instead of waiting for the next reconcile
 */
#include "core/inventory_config.hpp"
#include "core/inventory_status_provider.hpp"
#include "core/job_registry.hpp"
#include "collection_runtime_fixture.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
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
    explicit FakeSource(std::string name)
        : name_(std::move(name)) {}

    const std::string& name() const override { return name_; }

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
    std::optional<Job> observeForHint(pid_t hint, pid_t /*hint_ppid*/) override
    {
        // No remaining test drives the targeted hint path; the registry's
        // hint entry was removed with the watchers' self-registration loop.
        (void)hint;
        return std::nullopt;
    }

    // --- test controls -----------------------------------------------------
    void set_scan(std::function<ScanResult()> fn)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        scan_fn_ = std::move(fn);
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
    // Registers one discovered Job through the installed sink, exactly like
    // an exec event on the source's poll thread (synchronous, no queue).
    UpsertResult inject(const Job& job)
    {
        JobSink sink;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            sink = sink_.on_job;
        }
        return sink ? sink(job) : UpsertResult{};
    }

    int starts() const { std::lock_guard<std::mutex> lock(mutex_); return starts_; }
    int stops() const { std::lock_guard<std::mutex> lock(mutex_); return stops_; }
    int scans() const { std::lock_guard<std::mutex> lock(mutex_); return scans_; }
    bool scanned_without_start() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return scanned_without_start_;
    }

private:
    std::string name_;
    mutable std::mutex mutex_;
    DiscoverySink sink_;
    std::function<ScanResult()> scan_fn_;
    SourceHealth health_;
    bool start_result_ = true;
    bool start_throw_ = false;
    bool started_ = false;
    bool scanned_without_start_ = false;
    int starts_ = 0;
    int stops_ = 0;
    int scans_ = 0;
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
// Discovery fixtures: discovered Jobs carry typed facts plus the execution
// evidence group, exactly like the watchers build them.
Job make_condor_job(const std::string& native,
                    pid_t manager_pid,
                    std::uint64_t manager_ticks,
                    const std::string& cluster = "htcondor01",
                    const std::string& tag = "schedd-a",
                    const std::string& cgroup = "",
                    const std::vector<pid_t>& members = {})
{
    Job job;
    job.subtype_name = "condor";
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Condor;
    job.cluster_name = cluster;
    job.clusterTag = tag;
    job.NativeJobID = native;
    CondorJobAttr attributes{};
    attributes.auto_update_child = false;
    attributes.starter_pid = manager_pid;
    attributes.scheduler_host = "htcondor01";
    attributes.scheduler_name = tag;
    attributes.slots_cgroup_path = cgroup;
    attributes.owner = "alice";
    attributes.collector_host = cluster;
    job.sub_attr = attributes;
    JobExecution& execution = job.execution.emplace();
    execution.generation.boot_id = "boot-aaa";
    execution.generation.manager_pid = manager_pid;
    execution.generation.manager_start_ticks = manager_ticks;
    execution.generation.cgroup_identity = cgroup;
    for (const pid_t member : members) {
        execution.members.push_back(ProcessIdentity{member, manager_ticks, "boot-aaa"});
    }
    CondorJobWatcher::finalize_discovery(job);
    return job;
}

Job make_unresolved_job(const std::string& native, pid_t manager_pid)
{
    Job job = make_condor_job(native, manager_pid, 900000);
    job.cluster_name.clear();
    CondorJobWatcher::finalize_discovery(job);
    return job;
}

ScanResult scan_with(std::vector<Job> jobs, bool complete = true,
                     const std::string& error = "")
{
    ScanResult result;
    result.complete = complete;
    result.error = error;
    result.started_at_ms = 10;
    result.finished_at_ms = 11;
    result.jobs = std::move(jobs);
    for (const auto& job : result.jobs) {
        if (CondorJobWatcher::discovery_ready(job)) ++result.counts.complete;
        else ++result.counts.incomplete;
    }
    result.counts.candidates = result.jobs.size();
    return result;
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
    selector.adapter = "condor";
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
        check(!config.settings.at("condor").auto_collect,
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
        const auto& condor = config.settings.at("condor");
        check(condor.auto_collect && !condor.use_rules &&
                  condor.default_collectors.size() == 1,
              "config: explicit condor settings win");
        const auto& slurm = config.settings.at("slurm");
        check(!slurm.auto_collect && slurm.use_rules && slurm.rules_dir == "/tmp/rules",
              "config: slurm rules settings resolved");
    }
    {
        // Legacy folding: auto_add_condorjob=true implies inventory + auto.
        const auto config = resolve_inventory_maintenance_config(nlohmann::json{
            {"job_registry_config", nlohmann::json{{"auto_add_condorjob", true}}},
        });
        check(config.types.size() == 1 && config.types.front() == "condor",
              "config: legacy auto_add folds into inventory discovery");
        check(config.settings.at("condor").auto_collect,
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
    {
        // Tri-state auto_add: the explicit key wins, and each legacy pair
        // combination folds onto the same resolved state.
        const auto resolve = [](const nlohmann::json& document) {
            return resolve_inventory_maintenance_config(document);
        };
        const auto condor_add = [](const char* auto_add) {
            nlohmann::json watcher{{"auto_add", auto_add}};
            return nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"inventory", nlohmann::json{{"types", nlohmann::json::array({"condor"})}}},
                }},
                {"condor_job_watcher", std::move(watcher)},
            };
        };
        check(resolve(condor_add("off")).settings.at("condor").auto_add == AutoAdd::Off,
              "config: explicit auto_add=off resolves to Off");
        check(resolve(condor_add("default")).settings.at("condor").auto_add ==
                  AutoAdd::Default,
              "config: explicit auto_add=default resolves to Default");
        check(resolve(condor_add("rule")).settings.at("condor").auto_add == AutoAdd::Rule,
              "config: explicit auto_add=rule resolves to Rule");

        const auto with_pair = [](bool auto_collect, std::optional<bool> use_rules) {
            nlohmann::json watcher{{"auto_collect", auto_collect}};
            if (use_rules) watcher["use_rules"] = *use_rules;
            return nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"inventory", nlohmann::json{{"types", nlohmann::json::array({"condor"})}}},
                }},
                {"condor_job_watcher", std::move(watcher)},
            };
        };
        check(resolve(with_pair(false, std::nullopt)).settings.at("condor").auto_add ==
                  AutoAdd::Off,
              "config: auto_collect=false maps to Off");
        check(resolve(with_pair(false, true)).settings.at("condor").auto_add == AutoAdd::Off,
              "config: auto_collect=false maps to Off regardless of use_rules");
        check(resolve(with_pair(true, false)).settings.at("condor").auto_add ==
                  AutoAdd::Default,
              "config: auto_collect=true + use_rules=false maps to Default");
        check(resolve(with_pair(true, true)).settings.at("condor").auto_add ==
                  AutoAdd::Rule,
              "config: auto_collect=true + use_rules=true maps to Rule");

        // The explicit tri-state beats a contradicting legacy pair.
        {
            const auto config = resolve(nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"inventory", nlohmann::json{{"types", nlohmann::json::array({"condor"})}}},
                }},
                {"condor_job_watcher", nlohmann::json{
                    {"auto_add", "off"},
                    {"auto_collect", true},
                    {"use_rules", true},
                }},
            });
            check(config.settings.at("condor").auto_add == AutoAdd::Off,
                  "config: an explicit auto_add wins over the legacy pair");
        }
        // The legacy flag folds into the same mapping (Default here).
        {
            const auto config = resolve(nlohmann::json{
                {"job_registry_config", nlohmann::json{{"auto_add_condorjob", true}}},
            });
            check(config.settings.at("condor").auto_add == AutoAdd::Default,
                  "config: the legacy auto_add flag folds onto Default");
        }
        // An invalid tri-state value rejects startup.
        bool threw = false;
        try {
            (void)resolve(condor_add("sometimes"));
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        check(threw, "config: an invalid auto_add value must reject startup");
    }
    pass_case("CONFIG", "section 8 precedence, defaults and startup rejection");
}

// ---------------------------------------------------------------------------
// A08 - start before scan, in-scan event replay, duplicate/obsolete hints
// ---------------------------------------------------------------------------

void run_a08(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const Job scanned =
        make_condor_job("100.0", 5100, 554, "htcondor01", "schedd-a", "/cg/slot-2");
    probes.set_process(5100, LivenessState::Alive, 554);
    source.set_scan([&]() { return scan_with({scanned}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a08"), source, probes));

    check(source.scans() == 1, "A08: the initial full scan did not run");
    check(!source.scanned_without_start(), "A08: scan ran before the source was started");
    check(source.starts() == 1, "A08: the source was not started exactly once");
    check(find_native(registry, "100.0").has_value(), "A08: scan observation missing");

    // An event buffered during the initial pass is committed by the pass that
    // drains it, through the same upsert path.
    const Job injected =
        make_condor_job("101.0", 5101, 555, "htcondor01", "schedd-a", "/cg/slot-2");
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
    check(registry.reconcileInventoryNow(), "A08: the retirement pass did not run");
    check(!find_native(registry, "101.0").has_value(),
          "A08: terminated execution not retired");
    source.inject(injected);
    wait_hb_quiet();
    registry.reconcileInventoryNow();
    check(!find_native(registry, "101.0").has_value(),
          "A08: obsolete hint resurrected an ended generation");

    // A new generation for the same identity registers as a new instance.
    const Job revived =
        make_condor_job("101.0", 5102, 556, "htcondor01", "schedd-a", "/cg/slot-2");
    probes.set_process(5102, LivenessState::Alive, 556);
    source.set_scan([&]() { return scan_with({revived}); });
    check(registry.reconcileInventoryNow(), "A08: the revival pass did not run");
    const auto revived_record = find_native(registry, "101.0");
    check(revived_record.has_value() &&
              revived_record->job.execution->generation.manager_pid == 5102,
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
        FakeSource source("condor");
        FakeProbes probes;
        const Job job =
            make_condor_job("200.0", 5200, 666, "htcondor01", "schedd-a", "/cg/slot-1");
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
        check(registry.reconcileInventoryNow(), "A09: pass 2 did not run");
        check(find_native(registry, "200.0").has_value(),
              "A09: one missed scan retired the instance");
        check(registry.reconcileInventoryNow(), "A09: pass 3 did not run");
        check(find_native(registry, "200.0").has_value(),
              "A09: recovered instance was retired");
        registry.deinit();
    }

    // Scenario 2: two complete absences with positive evidence retire.
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job job =
            make_condor_job("210.0", 5301, 777, "htcondor01", "schedd-a", "/cg/slot-2");
        int pass = 0;
        source.set_scan([&]() {
            ++pass;
            return pass == 1 ? scan_with({job}) : scan_with({});
        });
        probes.set_process(5301, LivenessState::Dead);
        probes.unset_cgroup("/cg/slot-2");   // unreadable: absence-only retirement
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a09-retire"), source, probes));
        registry.reconcileInventoryNow();   // complete miss #1
        check(find_native(registry, "210.0").has_value(),
              "A09: retirement happened after a single absence");
        check(heartbeat_status()["suspect_count"] == 1,
              "A09: an absent instance was not reported suspect");
        registry.reconcileInventoryNow();
        check(!find_native(registry, "210.0").has_value(),
              "A09: two complete absences did not retire");
        registry.deinit();
    }

    // Scenario 3: failed/incomplete scans never count as absence.
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job job =
            make_condor_job("220.0", 5300, 666, "htcondor01", "schedd-a", "/cg/slot-3");
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
        registry.reconcileInventoryNow();   // pass 2: failed
        registry.reconcileInventoryNow();   // pass 3: failed
        check(find_native(registry, "220.0").has_value(),
              "A09: failed scans were treated as absence");
        check(heartbeat_status()["suspect_count"] == 0,
              "A09: failed scans marked the instance suspect");
        registry.reconcileInventoryNow();   // pass 4: complete miss #1
        check(find_native(registry, "220.0").has_value(),
              "A09: retirement after one complete absence");
        check(registry.reconcileInventoryNow(), "A09: pass 5 did not run");   // complete miss #2
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
    FakeSource source("condor");
    FakeProbes probes;
    const Job job =
        make_condor_job("300.0", 5400, 777, "htcondor01", "schedd-a", "/cg/slot-4");
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
    check(record->job.execution->members.empty(),
          "A10: fixture should have an empty PID list");

    source.set_scan([&]() { return scan_with({}); });
    registry.reconcileInventoryNow();
    registry.reconcileInventoryNow();
    registry.reconcileInventoryNow();
    check(find_native(registry, "300.0").has_value(),
          "A10: an empty PID list retired a live execution");

    // Positive evidence: manager ended and the execution cgroup is empty. An
    // exit hint is enough: the targeted pass retires it without any source
    // read and without any collector involvement.
    probes.set_process(5400, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-4", {}, true);
    // Consume any rescan request left over from the earlier phases: a pending
    // request legitimately forces a scan, which would make this assertion about
    // the targeted retirement path meaningless.
    registry.reconcileInventoryNow();
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
    FakeSource source("condor");
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a11"), source, probes));
    const Job job =
        make_condor_job("400.0", 5500, 888, "htcondor01", "schedd-a", "/cg/slot-5");

    // Scan-driven injection: `scan_jobs` is what the next pass observes. The
    // out-of-band producer entry is gone with the hint queue; the scan path
    // is the honest equivalent (same admission, same bookkeeping).
    std::vector<Job> scan_jobs;
    bool first = true;
    source.set_scan([&]() {
        if (first) {
            first = false;
            return scan_with({job});
        }
        return scan_with(scan_jobs);
    });

    // EPERM on the manager probe: unknown, not death, even after two complete
    // absences.
    probes.set_process(5500, LivenessState::Unknown);
    probes.set_cgroup("/cg/slot-5", {}, false);   // unreadable cgroup
    registry.reconcileInventoryNow();
    registry.reconcileInventoryNow();
    registry.reconcileInventoryNow();
    check(find_native(registry, "400.0").has_value(),
          "A11: an unreadable liveness check was treated as death");

    // An unresolved namespace cannot be indexed, is counted as incomplete and
    // is retried once the namespace resolves.
    const Job unresolved = make_unresolved_job("401.0", 5501);
    check(!CondorJobWatcher::discovery_ready(unresolved), "A11: fixture should be incomplete");
    scan_jobs = {unresolved};
    registry.reconcileInventoryNow();
    check(!find_native(registry, "401.0").has_value(),
          "A11: an unresolved identity was indexed");
    check(heartbeat_status()["incomplete_observations"].get<std::uint64_t>() >= 1,
          "A11: incomplete observation was not reported");
    check(count_records(registry) == 1, "A11: an incomplete identity created a record");

    const Job resolved =
        make_condor_job("401.0", 5501, 999, "htcondor01", "schedd-a", "/cg/slot-6");
    // The manager is live; the empty scans must not retire it (absence is not
    // the subject of this case).
    probes.set_process(5501, LivenessState::Alive, 999);
    scan_jobs = {resolved};
    registry.reconcileInventoryNow();
    check(find_native(registry, "401.0").has_value(),
          "A11: a retried complete identity was not indexed");
    check(count_records(registry) == 2, "A11: retry duplicated or merged records");

    // A namespace-less discovery must never merge into another namespace.
    scan_jobs = {make_unresolved_job("400.0", 5502)};
    registry.reconcileInventoryNow();
    check(count_records(registry) == 2, "A11: an unresolved identity merged into a record");

    // A second cluster namespace with the same native id stays distinct.
    const Job other_cluster =
        make_condor_job("400.0", 5600, 1111, "htcondor02", "schedd-b", "/cg/slot-7");
    probes.set_process(5600, LivenessState::Alive, 1111);
    scan_jobs = {other_cluster};
    registry.reconcileInventoryNow();
    check(count_records(registry) == 3, "A11: namespaces merged");
    check(find_native(registry, "400.0", "htcondor02", "schedd-b").has_value(),
          "A11: second namespace record missing");
    check(find_native(registry, "400.0")->job.execution->generation.manager_pid == 5500,
          "A11: wrong record was updated");

    registry.deinit();
    pass_case("A11", "EPERM/unknown is not death; incomplete identities visible and retried");
}

// ---------------------------------------------------------------------------
// A12 - producer registration proceeds while a pass is in flight; nothing
//       is queued, so nothing can be dropped
// ---------------------------------------------------------------------------

void run_a12(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a12"), source, probes));

    const Job alpha =
        make_condor_job("500.0", 5700, 1212, "htcondor01", "schedd-a", "/cg/slot-8");
    const Job beta =
        make_condor_job("501.0", 5701, 1313, "htcondor01", "schedd-a", "/cg/slot-9");

    // Block a pass inside the source scan, then register from the producer
    // side (the sink the poll thread would call). The old bounded queue with
    // its drop accounting is gone; registration is synchronous and must
    // neither block on the in-flight pass nor lose the discoveries.
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

    const UpsertResult alpha_result = source.inject(alpha);
    const UpsertResult beta_result = source.inject(beta);
    check(alpha_result.ok && beta_result.ok,
          "A12: producer-side registration failed while a pass was in flight");
    check(count_records(registry) == 2,
          "A12: synchronous registration did not land while the pass was blocked");

    const auto status = heartbeat_status();
    check(status["events_dropped"].get<std::uint64_t>() == 0,
          "A12: an event was dropped even though no queue exists");
    check(status["queue_depth"].get<std::size_t>() == 0,
          "A12: the deprecated queue-depth key must report zero");

    release_scan.set_value();
    reconciler.join();

    // The next authoritative pass re-observes both: idempotent, no repair
    // needed, nothing duplicated.
    source.set_scan([&]() { return scan_with({alpha, beta}); });
    registry.reconcileInventoryNow();
    check(count_records(registry) == 2, "A12: the rescan duplicated live instances");

    registry.deinit();
    pass_case("A12", "synchronous producer registration under an in-flight pass loses nothing");
}

// ---------------------------------------------------------------------------
// A13 - source started without an event source (scan-only)
// ---------------------------------------------------------------------------

void run_a13(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    auto options = heartbeat_options(fresh_dir(base, "a13"), source, probes);
    source.set_start_result(false);   // scan-only source
    JobRegistry registry(options);

    const auto status = heartbeat_status();
    check(status["tracker_unavailable"] == true,
          "A13: a scan-only source was not reported");
    check(status["state"] != "stopped", "A13: the heartbeat did not run");

    const Job job =
        make_condor_job("600.0", 5800, 1414, "htcondor01", "schedd-a", "/cg/slot-10");
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
    registry.reconcileInventoryNow();
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
    FakeSource source("condor");
    FakeProbes probes;
    const Job old_job =
        make_condor_job("700.0", 5900, 1515, "htcondor01", "schedd-a", "/cg/slot-11");
    source.set_scan([&]() { return scan_with({old_job}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a14"), source, probes));

    // A registered lifecycle observer makes retirements go through the
    // closing state instead of archiving immediately.
    std::mutex observer_mutex;
    std::vector<uint64_t> removed;
    install_deferred_drain(registry, [&](JobEvent event, const Job& job) {
        if (event != JobEvent::Removed) return;
        std::lock_guard<std::mutex> lock(observer_mutex);
        removed.push_back(job.JobID);
    });

    const auto old_record = find_native(registry, "700.0");
    check(old_record.has_value(), "A14: old record missing");

    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(5900, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-11", {}, true);
    registry.reconcileInventoryNow();
    check(registry.closingJobCount() == 1,
          "A14: retirement did not enter the closing state");
    // The runtime's own pass is what dispatches the drain/Removed; the
    // heartbeat only committed the Closing intent.
    test_runtime(registry).reconcile(old_record->internal_id);
    {
        std::lock_guard<std::mutex> lock(observer_mutex);
        check(removed.size() == 1 && removed.front() == old_record->internal_id,
              "A14: the closing retirement was not published to observers");
    }
    check(registry.inventoryRecord(old_record->internal_id).has_value(),
          "A14: closing instance disappeared before acknowledgement");

    // A new job reuses the same PID and cgroup path while the old record is
    // still closing.
    const Job new_job =
        make_condor_job("700.1", 5900, 1616, "htcondor01", "schedd-a", "/cg/slot-11");
    source.set_scan([&]() { return scan_with({new_job}); });
    registry.reconcileInventoryNow();

    const auto new_record = find_native(registry, "700.1");
    check(new_record.has_value(), "A14: new instance was not registered");
    check(new_record->internal_id != old_record->internal_id,
          "A14: new instance reused the retired internal id");

    // The collector drain acknowledges the removal: this is the real
    // production ack path, and it must never touch the new ownership.
    check(test_runtime(registry).completeDrain(old_record->internal_id),
          "A14: the drain acknowledgement failed");
    check(registry.closingJobCount() == 0, "A14: old closing record was not finalized");
    check(registry.inventoryRecord(new_record->internal_id).has_value(),
          "A14: old acknowledgement deleted the new instance");
    const auto still_new = find_native(registry, "700.1");
    check(still_new.has_value() && still_new->internal_id == new_record->internal_id,
          "A14: old acknowledgement removed the new identity index");
    check(still_new->job.execution->generation.manager_start_ticks == 1616,
          "A14: new generation was replaced by stale state");

    // A stale discovery for the retired generation still cannot resurrect it.
    source.set_scan([&]() { return scan_with({old_job}); });
    registry.reconcileInventoryNow();
    check(!find_native(registry, "700.0").has_value(),
          "A14: a stale discovery resurrected the retired generation");
    check(find_native(registry, "700.1").has_value(),
          "A14: a stale discovery disturbed the new instance");

    registry.deinit();
    pass_case("A14", "late cleanup of an old generation cannot remove new ownership");
}

// ---------------------------------------------------------------------------
// A23 - reads are side-effect free and snapshots are coherent
// ---------------------------------------------------------------------------

void run_a23(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a23"), source, probes));

    const Job first =
        make_condor_job("800.0", 6000, 1717, "htcondor01", "schedd-a", "/cg/slot-12");
    const Job second =
        make_condor_job("801.0", 6001, 1818, "htcondor01", "schedd-a", "/cg/slot-13");
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
        registry.reconcileInventoryNow();
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
        FakeSource source("condor");
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
        FakeSource source("condor");
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
        FakeSource source("condor");
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
        FakeSource source("condor");
        FakeProbes probes;
        std::mutex observer_mutex;
        std::vector<uint64_t> removed;
        uint64_t closing_id = 0;
        {
            const Job job = make_condor_job(
                "900.0", 6100, 1919, "htcondor01", "schedd-a", "/cg/slot-14");
            source.set_scan([&]() { return scan_with({job}); });
            JobRegistry registry(heartbeat_options(path, source, probes));
            install_deferred_drain(registry, [&](JobEvent event, const Job& job) {
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
            registry.reconcileInventoryNow();
            check(registry.closingJobCount() == 1, "A25: closing state missing");

            // Both archive attempts (the closing cleanup and the drain ack)
            // must fail so the interrupted closing survives the shutdown.
            registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, 0, 2);
            check(!registry.deinit(),
                  "A25: an incomplete drain was not reported");
        }

        {
            // Restart: the interrupted retirement is restored as closing and
            // the closing cleanup archives it. Maintenance is disabled on this
            // instance so the restored row can be observed before the same
            // cleanup the heartbeat would run archives it (otherwise the
            // background pass races the assertion).
            FakeSource source2("condor");
            auto restart_options = heartbeat_options(path, source2, probes);
            restart_options.enable_inventory_maintenance = false;
            JobRegistry registry(restart_options);
            const auto record = registry.inventoryRecord(closing_id);
            check(record.has_value() && record->state == "closing",
                  "A25: the interrupted closing was not restored");
            check(registry.cleanupClosingJobs(),
                  "A25: the restart closing cleanup did not archive the record");
            check(registry.closingJobCount() == 0,
                  "A25: closing record survived the retry");
            check(registry.deinit(), "A25: retry after the fault did not finalize");
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
    FakeSource source("condor");
    FakeProbes probes;
    JobRegistry registry(heartbeat_options(fresh_dir(base, "a28"), source, probes));
    const Job job =
        make_condor_job("600.0", 5800, 1414, "htcondor01", "schedd-a", "/cg/slot-10");

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
    registry.reconcileInventoryNow();
    registry.reconcileInventoryNow();
    registry.reconcileInventoryNow();
    check(find_native(registry, "600.0").has_value(),
          "A28: unknown cgroup members were read as death evidence");

    // Once the member reads as dead the execution is conclusively gone: the
    // cgroup member is dead and no other member evidence exists.
    probes.set_process(6101, LivenessState::Dead);
    registry.reconcileInventoryNow();
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
    check(!records.front().job.execution->generation.complete(),
          "legacy: a manual registration must not invent an execution generation");

    check(heartbeat_status()["configured_types"].empty(),
          "legacy: no discovery type should be configured");

    check(registry.reconcileInventoryNow(), "legacy: the pass did not run");
    check(registry.findJob(id).has_value(),
          "legacy: a live manual registration was retired");
    check(registry.inventoryRecord(id).has_value() &&
              registry.inventoryRecord(id)->state == "active",
          "legacy: a live registration must stay active");

    probes.set_process(4321, LivenessState::Dead);
    check(registry.reconcileInventoryNow(), "legacy: the retirement pass did not run");
    check(!registry.findJob(id).has_value(),
          "legacy: a dead manual registration was not retired");
    check(!registry.inventoryRecord(id).has_value(),
          "legacy: the retired record was not archived");
    registry.deinit();

    pass_case("LEGACY", "manual registrations retire on process evidence without a source");
}

// ---------------------------------------------------------------------------
// RESTORE-UNKNOWN (round 3): an unverifiable restored execution keeps its saved
// evidence.  The suspect verdict blocks admission; deleting the members is not a
// verdict and would let the later absence rule retire an instance whose child is
// still alive.
// ---------------------------------------------------------------------------

void run_restore_unknown_keeps_evidence(const std::string& base)
{
    const auto path = fresh_dir(base, "restore-unknown");
    const std::string boot = read_boot_id();
    check(!boot.empty(), "RESTORE-UNKNOWN: cannot read the current boot id");
    const pid_t manager_pid = 8100;
    const pid_t child_pid = 8200;
    const std::string cgroup = "/cg/slot-81";

    Job job = make_condor_job("810.0", manager_pid, 181818,
                              "htcondor01", "schedd-a", cgroup);
    // The restore path validates against the CURRENT boot, so the fixture
    // must carry the real one; the member models a live child under an alive
    // manager with its own start time.
    job.execution->generation.boot_id = boot;
    job.execution->members.clear();
    job.execution->members.push_back(ProcessIdentity{child_pid, 191919, boot});
    CondorJobWatcher::finalize_discovery(job);

    {
        FakeSource source("condor");
        FakeProbes probes;
        probes.set_process(manager_pid, LivenessState::Alive, 181818);
        probes.set_process(child_pid, LivenessState::Alive, 191919);
        probes.set_cgroup(cgroup, {child_pid}, true);
        source.set_scan([&]() { return scan_with({job}); });
        JobRegistry registry(heartbeat_options(path, source, probes));
        check(find_native(registry, "810.0").has_value(),
              "RESTORE-UNKNOWN: the baseline observation was not registered");
        check(registry.deinit(), "RESTORE-UNKNOWN: the baseline shutdown failed");
    }

    // Restart where the manager cannot be read: the execution is UNKNOWN.  No
    // maintenance worker here, so the restore verdict is observed directly
    // (a heartbeat pass may legitimately promote a suspect row with no absence).
    {
        FakeProbes probes;
        probes.set_process(manager_pid, LivenessState::Unknown, 181818);
        probes.set_process(child_pid, LivenessState::Alive, 191919);
        probes.set_cgroup(cgroup, {child_pid}, true);
        JobRegistryOptions options;
        options.db_path = path;
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        options.enable_inventory_maintenance = false;
        options.test_process_probe = probes.process_probe();
        options.test_cgroup_probe = probes.cgroup_probe();
        options.test_heartbeat = true;
        JobRegistry registry(options);

        const auto restored = find_native(registry, "810.0");
        check(restored.has_value(), "RESTORE-UNKNOWN: the record was not restored");
        check(restored->state == kStateActive && !restored->execution_verified,
              "RESTORE-UNKNOWN: an unverifiable execution must stay Open but "
              "carry the unverified (suspect) evidence, not a lifecycle state");
        check(std::find(restored->job.JobPIDs.begin(), restored->job.JobPIDs.end(),
                        child_pid) != restored->job.JobPIDs.end(),
              "RESTORE-UNKNOWN: restore deleted the saved child PID");
        check(std::any_of(restored->job.execution->members.begin(),
                          restored->job.execution->members.end(),
                          [&](const ProcessIdentity& member) {
                              return member.pid == child_pid && member.complete();
                          }),
              "RESTORE-UNKNOWN: restore deleted the saved child identity");
        check(registry.deinit(), "RESTORE-UNKNOWN: the restore-only shutdown failed");
    }

    // Heartbeat restart: later the manager is positively dead and the cgroup is
    // unreadable.  Two complete source passes without an observation must still
    // not retire the instance while the saved child is alive.
    FakeSource source("condor");
    FakeProbes probes;
    probes.set_process(manager_pid, LivenessState::Unknown, 181818);
    probes.set_process(child_pid, LivenessState::Alive, 191919);
    probes.set_cgroup(cgroup, {child_pid}, true);
    source.set_scan([&]() { return scan_with({}); });
    JobRegistry registry(heartbeat_options(path, source, probes));
    const auto restored = find_native(registry, "810.0");
    check(restored.has_value(), "RESTORE-UNKNOWN: the heartbeat restore lost the record");
    check(std::find(restored->job.JobPIDs.begin(), restored->job.JobPIDs.end(),
                    child_pid) != restored->job.JobPIDs.end(),
          "RESTORE-UNKNOWN: the heartbeat restore dropped the child evidence");

    probes.set_process(manager_pid, LivenessState::Dead, 181818);
    probes.unset_cgroup(cgroup);
    check(registry.reconcileInventoryNow(), "RESTORE-UNKNOWN: first pass did not run");
    check(registry.reconcileInventoryNow(), "RESTORE-UNKNOWN: second pass did not run");

    const auto after = find_native(registry, "810.0");
    check(after.has_value(),
          "RESTORE-UNKNOWN: the instance was retired although its saved child is alive");
    check(std::find(after->job.JobPIDs.begin(), after->job.JobPIDs.end(), child_pid) !=
              after->job.JobPIDs.end(),
          "RESTORE-UNKNOWN: the child evidence was dropped before the verdict");
    check(registry.findJob(after->internal_id).has_value(),
          "RESTORE-UNKNOWN: the live instance disappeared from the active set");
    check(registry.deinit(), "RESTORE-UNKNOWN: shutdown failed");

    pass_case("RESTORE-UNKNOWN",
              "unverifiable restore is suspect, keeps member evidence and is not retired");
}

// ---------------------------------------------------------------------------
// RETIRE-EXPLICIT - an explicit retirement marks the ended generation
// ---------------------------------------------------------------------------

void run_retire_explicit(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const Job job =
        make_condor_job("310.0", 5400, 888, "htcondor01", "schedd-a", "/cg/slot-31");
    probes.set_process(5400, LivenessState::Alive, 888);
    source.set_scan([&]() { return scan_with({job}); });
    JobRegistry registry(
        heartbeat_options(fresh_dir(base, "retire-explicit"), source, probes));

    const auto active = find_native(registry, "310.0");
    check(active.has_value(), "RETIRE-EXPLICIT: the live instance was not inventoried");
    const std::uint64_t retired_id = active->internal_id;

    // Explicit retirement while every process is still alive: no evidence
    // path runs, so the command itself must mark the ended generation.
    check(registry.retireInstance(retired_id),
          "RETIRE-EXPLICIT: the explicit retirement was refused");
    check(!find_native(registry, "310.0").has_value(),
          "RETIRE-EXPLICIT: the retired instance stayed active");

    // A scan observation of the SAME execution generation must not
    // re-register the retired instance: isObsolete() refuses it.
    const std::uint64_t obsolete_before =
        heartbeat_status()["obsolete_hints"].get<std::uint64_t>();
    check(registry.reconcileInventoryNow(),
          "RETIRE-EXPLICIT: the post-retirement pass did not run");
    check(!find_native(registry, "310.0").has_value(),
          "RETIRE-EXPLICIT: a same-generation observation resurrected the "
          "explicitly retired instance");
    check(heartbeat_status()["obsolete_hints"].get<std::uint64_t>() > obsolete_before,
          "RETIRE-EXPLICIT: the refused observation was not counted as obsolete");

    // A NEW generation (different manager, different start ticks) registers
    // as a new instance: the mark is per generation, never per identity.
    const Job revived =
        make_condor_job("310.0", 5401, 889, "htcondor01", "schedd-a", "/cg/slot-31");
    probes.set_process(5401, LivenessState::Alive, 889);
    source.set_scan([&]() { return scan_with({revived}); });
    check(registry.reconcileInventoryNow(),
          "RETIRE-EXPLICIT: the revival pass did not run");
    const auto revived_record = find_native(registry, "310.0");
    check(revived_record.has_value() &&
              revived_record->job.execution->generation.manager_pid == 5401,
          "RETIRE-EXPLICIT: a new generation was refused after the explicit "
          "retirement");
    check(count_records(registry) == 1, "RETIRE-EXPLICIT: unexpected record count");

    registry.deinit();
    pass_case("RETIRE-EXPLICIT",
              "explicit retirement marks the ended generation; a same-generation "
              "observation is refused, a new generation registers");
}

// ---------------------------------------------------------------------------
// ADMIT - inventory capture_all admission gate (S1 x S3 matrix)
// ---------------------------------------------------------------------------

void run_admit(const std::string& base)
{
    const auto rules_root = std::filesystem::path(base) / "admit-rules";
    std::filesystem::create_directories(rules_root / "accept");
    std::filesystem::create_directories(rules_root / "reject");
    {
        std::ofstream accept(rules_root / "accept" / "condor_job_admit.lua");
        accept << "local rule = {}\n"
                  "rule.name = \"admit_accept\"\n"
                  "rule.priority = 10\n"
                  "function rule.condition(job)\n"
                  "    return true, \"CPUMemCollector\"\n"
                  "end\n"
                  "return rule\n";
        std::ofstream reject(rules_root / "reject" / "condor_job_admit.lua");
        reject << "local rule = {}\n"
                  "rule.name = \"admit_reject\"\n"
                  "rule.priority = 10\n"
                  "function rule.condition(job)\n"
                  "    return false\n"
                  "end\n"
                  "return rule\n";
    }

    // S1 off x S3 off: the scan keeps running, nothing registers, drops are
    // counted, and the event path reports NotAdmitted without a retry ask.
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job job =
            make_condor_job("320.0", 5500, 111, "htcondor01", "schedd-a", "/cg/slot-32");
        probes.set_process(5500, LivenessState::Alive, 111);
        source.set_scan([&]() { return scan_with({job}); });
        auto options = heartbeat_options(fresh_dir(base, "admit-off"), source, probes);
        options.test_capture_all = false;
        JobRegistry registry(options);

        check(source.scans() >= 1, "ADMIT: the watcher must keep scanning with the gate closed");
        check(count_records(registry) == 0,
              "ADMIT: S1off x S3off must register nothing from the scan");
        const auto refused = source.inject(job);
        check(!refused.ok && refused.code == RegistrationError::NotAdmitted,
              "ADMIT: the event path must report NotAdmitted");
        check(!refused.retry_registration(),
              "ADMIT: a refused admission must not ask the watcher to rescan");
        check(registry.reconcileInventoryNow(), "ADMIT: the pass must run");
        check(count_records(registry) == 0,
              "ADMIT: S1off x S3off must register nothing on a pass");
        check(!find_native(registry, "320.0").has_value(),
              "ADMIT: the observed job must stay unregistered");
        const auto status = heartbeat_status();
        check(status["capture_all"].get<bool>() == false,
              "ADMIT: the status must report the gate state");
        check(status["dropped_observations"].get<std::uint64_t>() >= 2,
              "ADMIT: drops from the scan and event paths must be counted");
        registry.deinit();
    }

    // S1 off x S3 default: registration plus the configured default list as
    // the initial automatic selection.
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job job =
            make_condor_job("330.0", 5600, 222, "htcondor01", "schedd-a", "/cg/slot-33");
        probes.set_process(5600, LivenessState::Alive, 222);
        source.set_scan([&]() { return scan_with({job}); });
        auto options = heartbeat_options(fresh_dir(base, "admit-default"), source, probes);
        options.test_capture_all = false;
        options.test_auto_add_states = {{"condor", AutoAdd::Default}};
        options.test_auto_policy = [] {
            return AutoCollectorPolicy::defaults({"CPUMemCollector"});
        };
        JobRegistry registry(options);

        const auto record = find_native(registry, "330.0");
        check(record.has_value(), "ADMIT: S1off x S3default must register the job");
        const auto binding = registry.selectionRecord(record->internal_id);
        check(binding.mode() == CollectionMode::Auto &&
                  binding.automatic.has_value() &&
                  binding.automatic->size() == 1 &&
                  binding.automatic->front() == "CPUMemCollector",
              "ADMIT: the default list must become the initial automatic selection");
        check(binding.automatic_source == "defaults",
              "ADMIT: the selection must identify the defaults source");
        const auto status = heartbeat_status();
        check(status["dropped_observations"].get<std::uint64_t>() == 0 &&
                  status["rejected_observations"].get<std::uint64_t>() == 0,
              "ADMIT: an admitted job must not count as dropped or rejected");
        registry.deinit();
    }

    // S1 off x S3 rule: a rejecting rule keeps the job out of the inventory
    // (unlike blanket mode, where a reject registers an empty selection); an
    // accepting rule registers with the rule's list.
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job rejected_job =
            make_condor_job("340.0", 5700, 333, "htcondor01", "schedd-a", "/cg/slot-34");
        probes.set_process(5700, LivenessState::Alive, 333);
        source.set_scan([&]() { return scan_with({rejected_job}); });
        auto options = heartbeat_options(fresh_dir(base, "admit-reject"), source, probes);
        options.test_capture_all = false;
        options.test_auto_add_states = {{"condor", AutoAdd::Rule}};
        options.test_auto_policy = [&] {
            return AutoCollectorPolicy::rules((rules_root / "reject").string(),
                                              "condor_job_");
        };
        JobRegistry registry(options);

        check(count_records(registry) == 0,
              "ADMIT: a rejecting rule must not register the job");
        check(registry.reconcileInventoryNow(), "ADMIT: the reject pass must run");
        check(count_records(registry) == 0,
              "ADMIT: a rejecting rule must keep the job out across passes");
        check(heartbeat_status()["rejected_observations"].get<std::uint64_t>() >= 1,
              "ADMIT: rule rejections must be counted");
        registry.deinit();
    }
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job accepted_job =
            make_condor_job("350.0", 5800, 444, "htcondor01", "schedd-a", "/cg/slot-35");
        probes.set_process(5800, LivenessState::Alive, 444);
        source.set_scan([&]() { return scan_with({accepted_job}); });
        auto options = heartbeat_options(fresh_dir(base, "admit-accept"), source, probes);
        options.test_capture_all = false;
        options.test_auto_add_states = {{"condor", AutoAdd::Rule}};
        options.test_auto_policy = [&] {
            return AutoCollectorPolicy::rules((rules_root / "accept").string(),
                                              "condor_job_");
        };
        JobRegistry registry(options);

        const auto record = find_native(registry, "350.0");
        check(record.has_value(), "ADMIT: an accepting rule must register the job");
        const auto binding = registry.selectionRecord(record->internal_id);
        check(binding.automatic.has_value() && binding.automatic->size() == 1 &&
                  binding.automatic->front() == "CPUMemCollector",
              "ADMIT: the rule's list must become the initial automatic selection");
        check(binding.automatic_source.rfind("rules:", 0) == 0,
              "ADMIT: the selection must identify the rules source");
        check(heartbeat_status()["dropped_observations"].get<std::uint64_t>() == 0,
              "ADMIT: an accepted job must not count as dropped");
        registry.deinit();
    }

    // S1 on is guarded by the whole existing suite (default options keep the
    // gate open); this case pins the default itself.
    {
        const auto config = resolve_inventory_maintenance_config(nlohmann::json::object());
        check(config.capture_all, "ADMIT: capture_all must default to true");
        const auto off = resolve_inventory_maintenance_config(nlohmann::json{
            {"job_registry_config", nlohmann::json{
                {"inventory", nlohmann::json{{"capture_all", false}}},
            }},
        });
        check(!off.capture_all, "ADMIT: capture_all=false must parse");
    }

    pass_case("ADMIT",
              "capture_all gate: off+off registers nothing, default/rule register with "
              "their lists, a rule reject stays out and is counted");
}

// ---------------------------------------------------------------------------
// STOP-PIN - a manual stop survives scan rotation and fact changes
// ---------------------------------------------------------------------------

void run_stop_pin(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    Job job =
        make_condor_job("360.0", 5900, 555, "htcondor01", "schedd-a", "/cg/slot-36");
    probes.set_process(5900, LivenessState::Alive, 555);
    source.set_scan([&]() { return scan_with({job}); });
    auto options = heartbeat_options(fresh_dir(base, "stop-pin"), source, probes);
    options.test_auto_policy = [] {
        return AutoCollectorPolicy::defaults({"CPUMemCollector"});
    };
    JobRegistry registry(options);

    const auto record = find_native(registry, "360.0");
    check(record.has_value(), "STOP-PIN: the job must register");
    const auto initial = registry.selectionRecord(record->internal_id);
    check(initial.automatic.has_value() && initial.automatic->size() == 1,
          "STOP-PIN: the automatic selection must start armed");

    // The stop: an explicit manual empty selection.
    CollectionRequest stop;
    stop.targets = {record->internal_id};
    stop.collectors = std::vector<std::string>{};
    check(registry.setCollectors(stop).ok, "STOP-PIN: the stop must commit");

    // Scan rotation with a FACT CHANGE (a new member process): the
    // observation merges, but the automatic policy must never re-pin
    // collectors over an explicit manual stop.
    job = make_condor_job("360.0", 5900, 555, "htcondor01", "schedd-a", "/cg/slot-36",
                          {5901, 5902});
    probes.set_process(5901, LivenessState::Alive, 555);
    probes.set_process(5902, LivenessState::Alive, 555);
    for (int pass = 0; pass < 3; ++pass) {
        check(registry.reconcileInventoryNow(), "STOP-PIN: the pass must run");
        const auto binding = registry.selectionRecord(record->internal_id);
        check(binding.mode() == CollectionMode::Manual && binding.manual.has_value() &&
                  binding.manual->empty(),
              "STOP-PIN: the manual stop must survive the observation merge");
        check(binding.effectiveCollectors().empty(),
              "STOP-PIN: applied/desired collectors must stay empty after scans");
        check(registry.actualCollection(record->internal_id).collectors.empty(),
              "STOP-PIN: nothing may actually attach after a stop");
    }

    registry.deinit();
    pass_case("STOP-PIN",
              "a manual stop survives scan rotation and fact changes; the empty "
              "manual selection always pins down the automatic policy");
}

// ---------------------------------------------------------------------------
// RESCAN - event-driven and on-demand passes
// ---------------------------------------------------------------------------

void run_rescan(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const Job job =
        make_condor_job("950.0", 9100, 191919, "htcondor01", "schedd-a", "/cg/slot-95");
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
    registry.reconcileInventoryNow();
    check(!find_native(registry, "950.0").has_value(),
          "rescan: positive termination evidence did not retire the instance");
    registry.deinit();

    pass_case("RESCAN", "event-driven and on-demand passes run the same reconcile path");
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
        run_restore_unknown_keeps_evidence(base);
        run_retire_explicit(base);
        run_admit(base);
        run_stop_pin(base);
        run_rescan(base);
        std::cout << "ALL PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
