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
/* Deterministic registry-heartbeat acceptance.
 *
 * Every case drives the JobRegistry heartbeat (test sources + injected probes
 * + test facts encoder) and asserts:
 *
 *   * CONFIG precedence, defaults and startup rejection
 *   * start-before-scan: events injected during a scan are replayed,
 *     duplicate hints do not duplicate records, obsolete hints do not
 *     resurrect an ended generation
 *   * one missed scan recovers; two complete absences retire; failed or
 *     incomplete scans never count as absence
 *   * empty PID list with a live manager/cgroup does not retire; positive
 *     evidence (manager ended + confirmed empty cgroup) retires
 *   * EPERM/unreadable/unresolved input is unknown, not death; incomplete
 *     identities stay visible and retried; no wrong merge
 *   * tracker-unavailable mode (source started without an event source)
 *   * a late acknowledgement for an old generation never removes the new
 *     instance's ownership (real drain-ack path)
 *   * reads are side-effect free; snapshots coherent; sentinel excluded
 *   * heartbeat stop is idempotent and ordered; a failed source start is
 *     not fatal; an interrupted closing survives a restart and archives
 *   * unknown cgroup members are not death evidence; confirmed death retires
 *   * RETIRE-EXPLICIT: an explicit retirement marks the ended generation; a
 *     late same-generation observation is refused, a new one registers
 *   * ADMIT: the capture_all admission gate (capture_all/auto_add
 *     combinations), drop/reject counters, NotAdmitted on the event path
 *   * STOP-PIN: a manual empty selection survives scan rotation and fact
 *     changes; automatic policy never re-pins collectors over a stop
 *   * LEGACY: manual registrations retire on process evidence without a source
 *   * RESCAN: event-driven and on-demand passes run the same reconcile path
 *   * EXEC: exec hints resolve to targeted observations without a full scan
 *   * EXEC2: an incomplete exec observation converges through a coalesced
 *     rescan instead of waiting for the next reconcile
 *   * DISCOVERY-CREATE-ONLY: a pass observes only unknown managers; a changed
 *     advertisement under a known manager never reaches the record
 *   * MANUAL-REFRESH: a manual record keeps manager/members through the
 *     adapter refresh and a no-change refresh moves no revision
 *   * MANUAL-ABSENCE: a manual record retires on two complete manager-miss
 *     passes when positive evidence is blocked
 *   * EVENT-ADMISSION: admission is a creation gate; a closed gate still
 *     updates a tracked identity (evidence and provenance, never the creation
 *     metadata) and still refuses an unknown one
 */
#include "core/maintenance_config.hpp"
#include "core/status_provider.hpp"
#include "core/job_registry.hpp"
#include "job_watcher/condor_job_watcher.hpp"
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
// Fake discovery source (an IDiscoverySource with controllable passes/hints)
// ---------------------------------------------------------------------------

// What one fake pass publishes: the discovered Jobs, its coverage and its
// error. Local to this test — the production contract exposes managers through
// ManagerSet and one Job at a time through observe().
struct PassPayload {
    std::vector<Job> jobs;
    bool complete = true;
    std::string error;
};

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

    // The heartbeat pass enumerates managers once and then pays observe() only
    // for managers it does not track. The fixture implements the same split:
    // one source read per pass, observe() answers from that read.
    ManagerSet enumerateManagers() override
    {
        const PassPayload pass = run_pass();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            last_pass_ = pass;
        }
        ManagerSet set;
        set.complete = pass.complete;
        set.error = pass.error;
        for (const auto& job : pass.jobs) {
            const ProcessIdentity& manager = job.execution.manager;
            if (!manager.complete()) continue;
            if (std::find(set.managers.begin(), set.managers.end(), manager) ==
                set.managers.end()) {
                set.managers.push_back(manager);
            }
        }
        return set;
    }

    std::optional<Job> observe(const ProcessIdentity& manager) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++observes_;
        for (const auto& job : last_pass_.jobs) {
            if (job.execution.manager == manager) return job;
        }
        return std::nullopt;
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
    void set_scan(std::function<PassPayload()> fn)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pass_fn_ = std::move(fn);
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
    int scans() const { std::lock_guard<std::mutex> lock(mutex_); return passes_; }
    int observes() const { std::lock_guard<std::mutex> lock(mutex_); return observes_; }
    bool scanned_without_start() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return passed_without_start_;
    }

private:
    PassPayload run_pass()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!started_) passed_without_start_ = true;
            ++passes_;
        }
        std::function<PassPayload()> fn;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            fn = pass_fn_;
        }
        return fn ? fn() : PassPayload{};
    }

    std::string name_;
    mutable std::mutex mutex_;
    DiscoverySink sink_;
    std::function<PassPayload()> pass_fn_;
    PassPayload last_pass_;
    SourceHealth health_;
    bool start_result_ = true;
    bool start_throw_ = false;
    bool started_ = false;
    bool passed_without_start_ = false;
    int starts_ = 0;
    int stops_ = 0;
    int passes_ = 0;
    int observes_ = 0;
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
    job.execution.manager.boot_id = "boot-aaa";
    job.execution.manager.pid = manager_pid;
    job.execution.manager.start_ticks = manager_ticks;
    for (const pid_t member : members) {
        job.execution.members.push_back(ProcessIdentity{member, manager_ticks, "boot-aaa"});
    }
    auto finalized = CondorJobWatcher::finalize_discovery(std::move(job));
    return finalized ? *finalized : Job{};
}

Job make_unresolved_job(const std::string& native, pid_t manager_pid)
{
    // The namespace fact is what names the job: clear both its typed copy and
    // the rendered projection, and do not run the watcher's finalize (which
    // would drop the observation instead of publishing it).
    Job job = make_condor_job(native, manager_pid, 900000);
    job.cluster_name.clear();
    std::get<CondorJobAttr>(job.sub_attr).collector_host.clear();
    return job;
}

// A manual registration fixture: the same typed facts a discovery produces,
// plus the collector intent addJob requires. Execution evidence (start ticks
// and boot id) is re-bound by the registry on the manual path, so the values
// here are placeholders the fake probes own.
Job make_manual_condor_job(const std::string& native, pid_t manager_pid,
                           std::uint64_t manager_ticks, const std::string& cgroup,
                           const std::vector<pid_t>& members)
{
    Job job = make_condor_job(native, manager_pid, manager_ticks, "htcondor01",
                              "schedd-a", cgroup, members);
    job.CollectorNames = {"cpumem_collector"};
    return job;
}

PassPayload scan_with(std::vector<Job> jobs, bool complete = true,
                      const std::string& error = "")
{
    PassPayload pass;
    pass.complete = complete;
    pass.error = error;
    pass.jobs = std::move(jobs);
    return pass;
}

JobRegistryOptions heartbeat_options(const std::string& path, FakeSource& source,
                                     FakeProbes& probes)
{
    JobRegistryOptions options;
    options.db_path = path;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
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
    options.test_process_probe = probes.process_probe();
    options.test_cgroup_probe = probes.cgroup_probe();
    options.test_heartbeat = true;
    return options;
}

std::size_t count_records(JobRegistry& registry)
{
    const auto records = registry.jobRecords();
    std::size_t count = 0;
    for (const auto& record : records) {
        if (record.job.JobID != 0) ++count;
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
    return registry.jobRecordByKey(*identity);
}

nlohmann::json heartbeat_status()
{
    const auto provider = get_status_provider();
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
        const auto provider = get_status_provider();
        return !provider || provider()["queue_depth"].get<std::size_t>() == 0;
    }, 3000);
}


// ---------------------------------------------------------------------------
// CONFIG - section 8 precedence (unchanged contract)
// ---------------------------------------------------------------------------

void case_config()
{
    {
        const auto config = resolve_maintenance_config(nlohmann::json::object());
        check(config.types.empty(), "config: absent types must default to []");
        check(config.reconcile_interval_sec == 30, "config: default interval is 30");
        check(!config.settings.at("condor").auto_collect,
              "config: auto_collect defaults to false");
    }
    {
        const auto config = resolve_maintenance_config(nlohmann::json{
            {"job_registry_config", nlohmann::json{
                {"maintenance", nlohmann::json{
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
        check(slurm.rules_prefix == "slurm",
              "config: the rule prefix is the adapter name, not the configured value");
    }
    {
        // Legacy folding: auto_add_condorjob=true implies discovery + auto.
        const auto config = resolve_maintenance_config(nlohmann::json{
            {"job_registry_config", nlohmann::json{{"auto_add_condorjob", true}}},
        });
        check(config.types.size() == 1 && config.types.front() == "condor",
              "config: legacy auto_add folds into discovery");
        check(config.settings.at("condor").auto_collect,
              "config: legacy folding turns auto collection on");
    }
    {
        bool threw = false;
        try {
            (void)resolve_maintenance_config(nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"maintenance", nlohmann::json{
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
            (void)resolve_maintenance_config(nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"maintenance", nlohmann::json{{"reconcile_interval_sec", 0}}},
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
            return resolve_maintenance_config(document);
        };
        const auto condor_add = [](const char* auto_add) {
            nlohmann::json watcher{{"auto_add", auto_add}};
            return nlohmann::json{
                {"job_registry_config", nlohmann::json{
                    {"maintenance", nlohmann::json{{"types", nlohmann::json::array({"condor"})}}},
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
                    {"maintenance", nlohmann::json{{"types", nlohmann::json::array({"condor"})}}},
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
                    {"maintenance", nlohmann::json{{"types", nlohmann::json::array({"condor"})}}},
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
// start before scan, in-scan event replay, duplicate/obsolete hints
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

    check(source.scans() == 1, "the initial full scan did not run");
    check(!source.scanned_without_start(), "scan ran before the source was started");
    check(source.starts() == 1, "the source was not started exactly once");
    check(find_native(registry, "100.0").has_value(), "scan observation missing");

    // An event buffered during the initial pass is committed by the pass that
    // drains it, through the same upsert path.
    const Job injected =
        make_condor_job("101.0", 5101, 555, "htcondor01", "schedd-a", "/cg/slot-2");
    source.inject(injected);
    check(wait_until([&]() { return find_native(registry, "101.0").has_value(); }, 5000),
          "buffered event during the scan was not merged");
    check(count_records(registry) == 2, "duplicate hints duplicated records");

    const auto before = find_native(registry, "101.0");
    check(before.has_value(), "injected record vanished");
    source.inject(injected);   // duplicate hint for the same generation
    check(wait_until([&]() {
              auto record = find_native(registry, "101.0");
              return record.has_value() &&
                     record->job.JobID == before->job.JobID;
          }, 3000),
          "duplicate hint changed the internal id");
    check(count_records(registry) == 2, "duplicate hints duplicated records");

    // Termination + obsolete hint: a hint for an ended generation must not
    // resurrect it.
    wait_hb_quiet();
    probes.set_process(5101, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-2", {}, true);
    source.set_scan([&]() { return scan_with({}); });
    check(registry.reconcileNow(), "the retirement pass did not run");
    check(!find_native(registry, "101.0").has_value(),
          "terminated execution not retired");
    source.inject(injected);
    wait_hb_quiet();
    registry.reconcileNow();
    check(!find_native(registry, "101.0").has_value(),
          "obsolete hint resurrected an ended generation");

    // A new generation for the same identity registers as a new instance.
    const Job revived =
        make_condor_job("101.0", 5102, 556, "htcondor01", "schedd-a", "/cg/slot-2");
    probes.set_process(5102, LivenessState::Alive, 556);
    source.set_scan([&]() { return scan_with({revived}); });
    check(registry.reconcileNow(), "the revival pass did not run");
    const auto revived_record = find_native(registry, "101.0");
    check(revived_record.has_value() &&
              revived_record->job.execution.manager.pid == 5102,
          "a new generation was rejected after retirement");
    check(count_records(registry) == 2, "duplicate entry for one generation");

    registry.deinit();
    pass_case("", "start-before-scan, in-scan event replay, duplicate hints, no resurrection");
}

// ---------------------------------------------------------------------------
// absence accounting
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
        check(registry.reconcileNow(), "pass 2 did not run");
        check(find_native(registry, "200.0").has_value(),
              "one missed scan retired the instance");
        check(registry.reconcileNow(), "pass 3 did not run");
        check(find_native(registry, "200.0").has_value(),
              "recovered instance was retired");
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
        registry.reconcileNow();   // complete miss #1
        check(find_native(registry, "210.0").has_value(),
              "retirement happened after a single absence");
        check(heartbeat_status()["suspect_count"] == 1,
              "an absent instance was not reported suspect");
        registry.reconcileNow();
        check(!find_native(registry, "210.0").has_value(),
              "two complete absences did not retire");
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
        registry.reconcileNow();   // pass 2: failed
        registry.reconcileNow();   // pass 3: failed
        check(find_native(registry, "220.0").has_value(),
              "failed scans were treated as absence");
        check(heartbeat_status()["suspect_count"] == 0,
              "failed scans marked the instance suspect");
        registry.reconcileNow();   // pass 4: complete miss #1
        check(find_native(registry, "220.0").has_value(),
              "retirement after one complete absence");
        check(registry.reconcileNow(), "pass 5 did not run");   // complete miss #2
        check(!find_native(registry, "220.0").has_value(),
              "two complete absences after failures did not retire");
        registry.deinit();
    }

    pass_case("", "recovery after one miss, retirement after two complete absences only");
}

// ---------------------------------------------------------------------------
// SEEN-RECOVERY: a manager enumerated again restores the liveness verdict
// ---------------------------------------------------------------------------

void run_seen_recovery(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const Job job =
        make_condor_job("205.0", 5350, 909, "htcondor01", "schedd-a", "/cg/slot-205");
    probes.set_process(5350, LivenessState::Alive, 909);
    bool present = true;
    source.set_scan([&]() { return present ? scan_with({job}) : scan_with({}); });
    JobRegistry registry(
        heartbeat_options(fresh_dir(base, "seen-recovery"), source, probes));

    const auto created = find_native(registry, "205.0");
    check(created.has_value(), "SEEN-RECOVERY: the fixture job did not register");
    const uint64_t id = created->job.JobID;
    check(registry.jobRecord(id)->verified,
          "SEEN-RECOVERY: a freshly observed record must be verified");

    // One complete pass without the manager: absence evidence, not death.
    present = false;
    check(registry.reconcileNow(), "SEEN-RECOVERY: the absence pass did not run");
    const auto absent = registry.jobRecord(id);
    check(absent.has_value(), "SEEN-RECOVERY: one absence retired a live record");
    check(!absent->verified,
          "SEEN-RECOVERY: an absent record must carry the unverified verdict");
    check(heartbeat_status()["suspect_count"] == 1,
          "SEEN-RECOVERY: an absent record was not reported suspect");

    // The manager is enumerated again on the next complete pass: presence
    // restores the verdict even though no fact changed.
    present = true;
    check(registry.reconcileNow(), "SEEN-RECOVERY: the recovery pass did not run");
    const auto recovered = registry.jobRecord(id);
    check(recovered.has_value(), "SEEN-RECOVERY: the recovered record disappeared");
    check(recovered->verified,
          "SEEN-RECOVERY: a seen record stayed unverified");
    check(heartbeat_status()["suspect_count"] == 0,
          "SEEN-RECOVERY: a seen record stayed in the suspect set");

    registry.deinit();
    pass_case("SEEN-RECOVERY",
              "a manager enumerated again restores verified/suspect state");
}

// ---------------------------------------------------------------------------
// empty PID window vs positive termination evidence
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
    check(record.has_value(), "record missing");
    check(record->job.execution.members.empty(),
          "fixture should have an empty PID list");

    source.set_scan([&]() { return scan_with({}); });
    registry.reconcileNow();
    registry.reconcileNow();
    registry.reconcileNow();
    check(find_native(registry, "300.0").has_value(),
          "an empty PID list retired a live execution");

    // Positive evidence: manager ended and the execution cgroup is empty. An
    // exit hint is enough: the targeted pass retires it without any source
    // read and without any collector involvement.
    probes.set_process(5400, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-4", {}, true);
    // Consume any rescan request left over from the earlier phases: a pending
    // request legitimately forces a scan, which would make this assertion about
    // the targeted retirement path meaningless.
    registry.reconcileNow();
    const int scans_before = source.scans();
    registry.notifyExit(5400);
    check(wait_until([&]() { return !find_native(registry, "300.0").has_value(); }, 3000),
          "positive termination evidence did not retire");
    check(source.scans() == scans_before,
          "retirement needed a source scan instead of targeted evidence");

    registry.deinit();
    pass_case("", "no premature retirement on an empty PID window; positive evidence retires");
}

// ---------------------------------------------------------------------------
// unknown is not death, incomplete identities are retried
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
    registry.reconcileNow();
    registry.reconcileNow();
    registry.reconcileNow();
    check(find_native(registry, "400.0").has_value(),
          "an unreadable liveness check was treated as death");

    // An unresolved namespace cannot be indexed, is counted as incomplete and
    // is retried once the namespace resolves.
    const Job unresolved = make_unresolved_job("401.0", 5501);
    check(!canonicalize(selector_of(unresolved)).has_value(), "fixture should be incomplete");
    scan_jobs = {unresolved};
    registry.reconcileNow();
    check(!find_native(registry, "401.0").has_value(),
          "an unresolved identity was indexed");
    check(heartbeat_status()["incomplete_observations"].get<std::uint64_t>() >= 1,
          "incomplete observation was not reported");
    check(count_records(registry) == 1, "an incomplete identity created a record");

    const Job resolved =
        make_condor_job("401.0", 5501, 999, "htcondor01", "schedd-a", "/cg/slot-6");
    // The manager is live; the empty scans must not retire it (absence is not
    // the subject of this case).
    probes.set_process(5501, LivenessState::Alive, 999);
    scan_jobs = {resolved};
    registry.reconcileNow();
    check(find_native(registry, "401.0").has_value(),
          "a retried complete identity was not indexed");
    check(count_records(registry) == 2, "retry duplicated or merged records");

    // A namespace-less discovery must never merge into another namespace.
    scan_jobs = {make_unresolved_job("400.0", 5502)};
    registry.reconcileNow();
    check(count_records(registry) == 2, "an unresolved identity merged into a record");

    // A second cluster namespace with the same native id stays distinct.
    const Job other_cluster =
        make_condor_job("400.0", 5600, 1111, "htcondor02", "schedd-b", "/cg/slot-7");
    probes.set_process(5600, LivenessState::Alive, 1111);
    scan_jobs = {other_cluster};
    registry.reconcileNow();
    check(count_records(registry) == 3, "namespaces merged");
    check(find_native(registry, "400.0", "htcondor02", "schedd-b").has_value(),
          "second namespace record missing");
    check(find_native(registry, "400.0")->job.execution.manager.pid == 5500,
          "wrong record was updated");

    registry.deinit();
    pass_case("", "EPERM/unknown is not death; incomplete identities visible and retried");
}

// ---------------------------------------------------------------------------
// producer registration proceeds while a pass is in flight; nothing
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

    std::thread reconciler([&]() { registry.reconcileNow(); });
    scan_entered.get_future().wait();

    const UpsertResult alpha_result = source.inject(alpha);
    const UpsertResult beta_result = source.inject(beta);
    check(alpha_result.ok && beta_result.ok,
          "producer-side registration failed while a pass was in flight");
    check(count_records(registry) == 2,
          "synchronous registration did not land while the pass was blocked");

    const auto status = heartbeat_status();
    check(status["events_dropped"].get<std::uint64_t>() == 0,
          "an event was dropped even though no queue exists");
    check(status["queue_depth"].get<std::size_t>() == 0,
          "the deprecated queue-depth key must report zero");

    release_scan.set_value();
    reconciler.join();

    // The next authoritative pass re-observes both: idempotent, no repair
    // needed, nothing duplicated.
    source.set_scan([&]() { return scan_with({alpha, beta}); });
    registry.reconcileNow();
    check(count_records(registry) == 2, "the rescan duplicated live instances");

    registry.deinit();
    pass_case("", "synchronous producer registration under an in-flight pass loses nothing");
}

// ---------------------------------------------------------------------------
// source started without an event source (manager pass only)
// ---------------------------------------------------------------------------

void run_a13(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    auto options = heartbeat_options(fresh_dir(base, "a13"), source, probes);
    source.set_start_result(false);   // no event source: manager pass only
    JobRegistry registry(options);

    const auto status = heartbeat_status();
    check(status["tracker_unavailable"] == true,
          "a pass-only source was not reported");
    check(status["state"] != "stopped", "the heartbeat did not run");

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
    registry.reconcileNow();
    check(!find_native(registry, "600.0").has_value(),
          "retirement did not work in tracker-unavailable mode");

    registry.deinit();
    pass_case("", "tracker unavailable: maintenance and retirement continue, health reports it");
}

// ---------------------------------------------------------------------------
// old closing record vs a new instance reusing pid/cgroup
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
    check(old_record.has_value(), "old record missing");

    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(5900, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-11", {}, true);
    registry.reconcileNow();
    check(registry.closingJobCount() == 1,
          "retirement did not enter the closing state");
    // The runtime's own pass is what dispatches the drain/Removed; the
    // heartbeat only committed the Closing intent.
    test_runtime(registry).reconcile(old_record->job.JobID);
    {
        std::lock_guard<std::mutex> lock(observer_mutex);
        check(removed.size() == 1 && removed.front() == old_record->job.JobID,
              "the closing retirement was not published to observers");
    }
    check(registry.jobRecord(old_record->job.JobID).has_value(),
          "closing instance disappeared before acknowledgement");

    // A new job reuses the same PID and cgroup path while the old record is
    // still closing.
    const Job new_job =
        make_condor_job("700.1", 5900, 1616, "htcondor01", "schedd-a", "/cg/slot-11");
    source.set_scan([&]() { return scan_with({new_job}); });
    registry.reconcileNow();

    const auto new_record = find_native(registry, "700.1");
    check(new_record.has_value(), "new instance was not registered");
    check(new_record->job.JobID != old_record->job.JobID,
          "new instance reused the retired internal id");

    // The collector drain acknowledges the removal: this is the real
    // production ack path, and it must never touch the new ownership.
    check(test_runtime(registry).completeDrain(old_record->job.JobID),
          "the drain acknowledgement failed");
    check(registry.closingJobCount() == 0, "old closing record was not finalized");
    check(registry.jobRecord(new_record->job.JobID).has_value(),
          "old acknowledgement deleted the new instance");
    const auto still_new = find_native(registry, "700.1");
    check(still_new.has_value() && still_new->job.JobID == new_record->job.JobID,
          "old acknowledgement removed the new identity index");
    check(still_new->job.execution.manager.start_ticks == 1616,
          "new generation was replaced by stale state");

    // A stale discovery for the retired generation still cannot resurrect it.
    source.set_scan([&]() { return scan_with({old_job}); });
    registry.reconcileNow();
    check(!find_native(registry, "700.0").has_value(),
          "a stale discovery resurrected the retired generation");
    check(find_native(registry, "700.1").has_value(),
          "a stale discovery disturbed the new instance");

    registry.deinit();
    pass_case("", "late cleanup of an old generation cannot remove new ownership");
}

// ---------------------------------------------------------------------------
// reads are side-effect free and snapshots are coherent
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
        const auto records = registry.jobRecords();
        for (const auto& record : records) {
            (void)registry.jobRecord(record.job.JobID);
            if (const auto key = registry.keyOf(record.job)) {
                const auto by_key = registry.jobRecordByKey(*key);
                check(by_key.has_value() && by_key->job.JobID == record.job.JobID,
                      "identity lookup disagrees with the snapshot");
            }
        }
        (void)registry.findJob(0);
        (void)registry.snapshot();
        (void)heartbeat_status();
    }
    const auto after = heartbeat_status();
    check(before["pass_count"] == after["pass_count"],
          "reads triggered a maintenance pass");
    check(before["retirements"] == after["retirements"],
          "reads triggered retirements");
    check(before["events_received"] == after["events_received"],
          "reads emitted events");
    check(before["last_scan_start_ms"] == after["last_scan_start_ms"],
          "reads changed scan timestamps");
    check(before["queue_depth"] == after["queue_depth"], "reads changed the queue");

    // Concurrent phase: the heartbeat mutates while reads keep observing.
    std::atomic<bool> stop{false};
    std::thread reader([&]() {
        while (!stop.load()) {
            const auto records = registry.jobRecords();
            if (records.size() > 5u) throw std::runtime_error("duplicate records appeared");
            for (const auto& record : records) {
                check(record.job.JobID != 0, "the system sentinel leaked into the registry");
                const auto key = registry.keyOf(record.job);
                if (!key) continue;
                const auto by_key = registry.jobRecordByKey(*key);
                check(by_key.has_value(), "snapshot references a missing identity");
                check(by_key->job.JobID == record.job.JobID,
                      "snapshot and identity index disagree");
            }
            (void)heartbeat_status();
        }
    });
    for (int index = 0; index < 40; ++index) {
        mutate.store(index);
        registry.reconcileNow();
    }
    stop.store(true);
    reader.join();

    const auto final_records = registry.jobRecords();
    for (const auto& record : final_records) {
        check(record.job.JobID != 0, "sentinel present in the final snapshot");
    }
    check(count_records(registry) == 2, "mutation loop lost an instance");
    registry.deinit();
    pass_case("", "reads are side-effect free; snapshots coherent; sentinel excluded");
}

// ---------------------------------------------------------------------------
// stop safety, failed source start, interrupted closing across restart
// ---------------------------------------------------------------------------

void run_a25(const std::string& base)
{
    // stopDiscovery is idempotent and keeps the status visible while
    // records drain; deinit retracts the providers.
    {
        FakeSource source("condor");
        FakeProbes probes;
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a25-repeat"), source, probes));

        check(static_cast<bool>(get_status_provider()),
              "status provider was not published");
        check(heartbeat_status()["state"] != "stopped",
              "status reports stopped while running");
        check(source.starts() == 1, "source start count mismatch");

        registry.stopDiscovery();
        registry.stopDiscovery();
        check(static_cast<bool>(get_status_provider()),
              "status provider was not kept while draining");
        check(source.stops() == 1, "source stop count mismatch");

        check(registry.deinit(), "deinit must succeed");
        check(!get_status_provider(), "status provider was not cleared");
        check(registry.deinit(), "a second deinit must keep the result");
        check(source.starts() == 1 && source.stops() == 1,
              "sources must not restart after discovery stopped");
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
        std::thread reconciler([&]() { registry->reconcileNow(); });
        scan_entered.get_future().wait();

        std::thread stopper([&]() { registry->stopDiscovery(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        release_scan.set_value();
        reconciler.join();
        stopper.join();
        check(!get_status_provider() ||
                  heartbeat_status()["state"] == "stopped",
              "stop during a blocked scan left the heartbeat running");

        // Nothing may touch the store after it is closed: destroy the
        // registry and verify no crash and no dangling provider.
        registry.reset();
        check(!get_status_provider(), "provider survived registry teardown");
    }

    // Source initialization failure is not fatal.
    {
        FakeSource source("condor");
        FakeProbes probes;
        source.set_start_throw(true);
        JobRegistry registry(heartbeat_options(fresh_dir(base, "a25-startfail"),
                                               source, probes));
        check(heartbeat_status()["tracker_unavailable"] == true,
              "a failed source start was not reported");
        registry.deinit();
        check(!get_status_provider(), "provider survived deinit");
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
            check(created.has_value(), "the observation was not registered");
            closing_id = created->job.JobID;

            source.set_scan([&]() { return scan_with({}); });
            probes.set_process(6100, LivenessState::Dead);
            probes.set_cgroup("/cg/slot-14", {}, true);
            registry.reconcileNow();
            check(registry.closingJobCount() == 1, "closing state missing");

            // Both archive attempts (the closing cleanup and the drain ack)
            // must fail so the interrupted closing survives the shutdown.
            registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, 0, 2);
            check(!registry.deinit(),
                  "an incomplete drain was not reported");
        }

        {
            // Restart: the interrupted retirement is restored as closing and
            // the closing cleanup archives it. Maintenance is disabled on this
            // instance so the restored row can be observed before the same
            // cleanup the heartbeat would run archives it (otherwise the
            // background pass races the assertion).
            FakeSource source2("condor");
            auto restart_options = heartbeat_options(path, source2, probes);
            restart_options.test_static_registry = true;
            JobRegistry registry(restart_options);
            const auto record = registry.jobRecord(closing_id);
            check(record.has_value() && record->state == RecordState::Closing,
                  "the interrupted closing was not restored");
            check(registry.cleanupClosingJobs(),
                  "the restart closing cleanup did not archive the record");
            check(registry.closingJobCount() == 0,
                  "closing record survived the retry");
            check(registry.deinit(), "retry after the fault did not finalize");
        }
    }

    pass_case("", "heartbeat stop is ordered and idempotent; a failed source start is "
                     "not fatal; an interrupted closing survives a restart");
}

// ---------------------------------------------------------------------------
// a readable but unprobeable cgroup is not an empty cgroup
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
    registry.reconcileNow();
    registry.reconcileNow();
    registry.reconcileNow();
    check(find_native(registry, "600.0").has_value(),
          "unknown cgroup members were read as death evidence");

    // Once the member reads as dead the execution is conclusively gone: the
    // cgroup member is dead and no other member evidence exists.
    probes.set_process(6101, LivenessState::Dead);
    registry.reconcileNow();
    check(!find_native(registry, "600.0").has_value(),
          "a conclusively dead execution was not retired");

    registry.deinit();
    pass_case("", "unknown cgroup members are not death evidence; confirmed death retires");
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
    set_pids(job, {4321});
    job.CollectorNames = {"cpumem_collector"};
    CommonJobAttr attr;
    attr.auto_update_child = false;
    job.sub_attr = attr;
    check(registry.addJob(job), "legacy: manual registration must succeed");

    const auto records = registry.jobRecords();
    check(records.size() == 1, "legacy: the registration created no job record");
    const uint64_t id = records.front().job.JobID;
    check(!records.front().job.execution.manager.complete(),
          "legacy: a manual registration must not invent an execution generation");

    check(heartbeat_status()["configured_types"].empty(),
          "legacy: no discovery type should be configured");

    check(registry.reconcileNow(), "legacy: the pass did not run");
    check(registry.findJob(id).has_value(),
          "legacy: a live manual registration was retired");
    check(registry.jobRecord(id).has_value() &&
              registry.jobRecord(id)->state == RecordState::Active,
          "legacy: a live registration must stay active");

    probes.set_process(4321, LivenessState::Dead);
    check(registry.reconcileNow(), "legacy: the retirement pass did not run");
    check(!registry.findJob(id).has_value(),
          "legacy: a dead manual registration was not retired");
    check(!registry.jobRecord(id).has_value(),
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
    job.execution.manager.boot_id = boot;
    job.execution.members.clear();
    job.execution.members.push_back(ProcessIdentity{child_pid, 191919, boot});
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
        options.test_static_registry = true;
        options.test_process_probe = probes.process_probe();
        options.test_cgroup_probe = probes.cgroup_probe();
        options.test_heartbeat = true;
        JobRegistry registry(options);

        const auto restored = find_native(registry, "810.0");
        check(restored.has_value(), "RESTORE-UNKNOWN: the record was not restored");
        check(restored->state == RecordState::Active && !restored->verified,
              "RESTORE-UNKNOWN: an unverifiable execution must stay Open but "
              "carry the unverified (suspect) evidence, not a lifecycle state");
        check(std::any_of(restored->job.execution.members.begin(),
                          restored->job.execution.members.end(),
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
    check(std::any_of(restored->job.execution.members.begin(),
                      restored->job.execution.members.end(),
                      [&](const ProcessIdentity& member) { return member.pid == child_pid; }),
          "RESTORE-UNKNOWN: the heartbeat restore dropped the child evidence");

    probes.set_process(manager_pid, LivenessState::Dead, 181818);
    probes.unset_cgroup(cgroup);
    check(registry.reconcileNow(), "RESTORE-UNKNOWN: first pass did not run");
    check(registry.reconcileNow(), "RESTORE-UNKNOWN: second pass did not run");

    const auto after = find_native(registry, "810.0");
    check(after.has_value(),
          "RESTORE-UNKNOWN: the instance was retired although its saved child is alive");
    check(std::any_of(after->job.execution.members.begin(),
                      after->job.execution.members.end(),
                      [&](const ProcessIdentity& member) { return member.pid == child_pid; }),
          "RESTORE-UNKNOWN: the child evidence was dropped before the verdict");
    check(registry.findJob(after->job.JobID).has_value(),
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
    const std::uint64_t retired_id = active->job.JobID;

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
    check(registry.reconcileNow(),
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
    check(registry.reconcileNow(),
          "RETIRE-EXPLICIT: the revival pass did not run");
    const auto revived_record = find_native(registry, "310.0");
    check(revived_record.has_value() &&
              revived_record->job.execution.manager.pid == 5401,
          "RETIRE-EXPLICIT: a new generation was refused after the explicit "
          "retirement");
    check(count_records(registry) == 1, "RETIRE-EXPLICIT: unexpected record count");

    registry.deinit();
    pass_case("RETIRE-EXPLICIT",
              "explicit retirement marks the ended generation; a same-generation "
              "observation is refused, a new generation registers");
}

// ---------------------------------------------------------------------------
// ADMIT - maintenance capture_all admission gate (capture_all x auto_add matrix)
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
        std::filesystem::create_directories(rules_root / "accept_bare");
        std::ofstream accept_bare(rules_root / "accept_bare" / "condor_job_admit.lua");
        accept_bare << "local rule = {}\n"
                       "rule.name = \"admit_accept_bare\"\n"
                       "rule.priority = 10\n"
                       "function rule.condition(job)\n"
                       "    return true\n"
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

    // capture_all off x auto_add off: the scan keeps running, nothing registers, drops are
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
        check(registry.reconcileNow(), "ADMIT: the pass must run");
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

    // capture_all off x auto_add default: registration plus the configured default list as
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
        const auto binding = registry.selectionRecord(record->job.JobID);
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

    // capture_all off x auto_add rule: a rejecting rule keeps the job out of the registry
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
        check(registry.reconcileNow(), "ADMIT: the reject pass must run");
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
        const auto binding = registry.selectionRecord(record->job.JobID);
        check(binding.automatic.has_value() && binding.automatic->size() == 1 &&
                  binding.automatic->front() == "CPUMemCollector",
              "ADMIT: the rule's list must become the initial automatic selection");
        check(binding.automatic_source.rfind("rules:", 0) == 0,
              "ADMIT: the selection must identify the rules source");
        check(heartbeat_status()["dropped_observations"].get<std::uint64_t>() == 0,
              "ADMIT: an accepted job must not count as dropped");
        registry.deinit();
    }

    // capture_all off x auto_add rule, bare accept: the rule accepts without naming
    // collectors and the configured auto_add_collectors defaults become the
    // selection.
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job job =
            make_condor_job("355.0", 5850, 445, "htcondor01", "schedd-a", "/cg/slot-35b");
        probes.set_process(5850, LivenessState::Alive, 445);
        source.set_scan([&]() { return scan_with({job}); });
        auto options = heartbeat_options(fresh_dir(base, "admit-bare-defaults"), source, probes);
        options.test_capture_all = false;
        options.test_auto_add_states = {{"condor", AutoAdd::Rule}};
        options.test_auto_policy = [&] {
            return AutoCollectorPolicy::rules((rules_root / "accept_bare").string(),
                                              "condor_job_", {"CPUMemCollector"});
        };
        JobRegistry registry(options);

        const auto record = find_native(registry, "355.0");
        check(record.has_value(),
              "ADMIT: a bare accept must register the job");
        const auto binding = registry.selectionRecord(record->job.JobID);
        check(binding.automatic.has_value() && binding.automatic->size() == 1 &&
                  binding.automatic->front() == "CPUMemCollector",
              "ADMIT: a bare accept must fall back to the configured defaults");
        check(binding.automatic_source.rfind("rules:", 0) == 0,
              "ADMIT: the fallback selection must still identify the rules source");
        const auto status = heartbeat_status();
        check(status["dropped_observations"].get<std::uint64_t>() == 0 &&
                  status["rejected_observations"].get<std::uint64_t>() == 0 &&
                  status["admission_errors"].get<std::uint64_t>() == 0,
              "ADMIT: a bare accept with defaults must not count as an error");
        registry.deinit();
    }

    // capture_all off x auto_add rule, bare accept without configured defaults: the decision
    // is refused — nothing registers and the error is counted (it is an
    // admission error, not a rule reject).
    {
        FakeSource source("condor");
        FakeProbes probes;
        const Job job =
            make_condor_job("356.0", 5860, 446, "htcondor01", "schedd-a", "/cg/slot-35c");
        probes.set_process(5860, LivenessState::Alive, 446);
        source.set_scan([&]() { return scan_with({job}); });
        auto options = heartbeat_options(fresh_dir(base, "admit-bare-nodefaults"), source, probes);
        options.test_capture_all = false;
        options.test_auto_add_states = {{"condor", AutoAdd::Rule}};
        options.test_auto_policy = [&] {
            return AutoCollectorPolicy::rules((rules_root / "accept_bare").string(),
                                              "condor_job_");
        };
        JobRegistry registry(options);

        check(count_records(registry) == 0,
              "ADMIT: a bare accept without defaults must not register the job");
        check(registry.reconcileNow(), "ADMIT: the refused pass must run");
        check(count_records(registry) == 0,
              "ADMIT: a bare accept without defaults must stay out across passes");
        const auto status = heartbeat_status();
        check(status["admission_errors"].get<std::uint64_t>() >= 1,
              "ADMIT: the refused decision must count as an admission error");
        check(status["rejected_observations"].get<std::uint64_t>() == 0,
              "ADMIT: the refused decision must not count as a rule reject");
        check(status["dropped_observations"].get<std::uint64_t>() == 0,
              "ADMIT: the refused decision must not count as dropped");
        registry.deinit();
    }

    // capture_all on is guarded by the whole existing suite (default options keep the
    // gate open); this case pins the default itself.
    {
        const auto config = resolve_maintenance_config(nlohmann::json::object());
        check(config.capture_all, "ADMIT: capture_all must default to true");
        const auto off = resolve_maintenance_config(nlohmann::json{
            {"job_registry_config", nlohmann::json{
                {"maintenance", nlohmann::json{{"capture_all", false}}},
            }},
        });
        check(!off.capture_all, "ADMIT: capture_all=false must parse");
    }

    pass_case("ADMIT",
              "capture_all gate: off+off registers nothing, default/rule register with "
              "their lists, a rule reject stays out and is counted, a bare accept falls "
              "back to the configured defaults or is refused without them");
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
    const auto initial = registry.selectionRecord(record->job.JobID);
    check(initial.automatic.has_value() && initial.automatic->size() == 1,
          "STOP-PIN: the automatic selection must start armed");

    // The stop: an explicit manual empty selection.
    CollectionRequest stop;
    stop.targets = {record->job.JobID};
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
        check(registry.reconcileNow(), "STOP-PIN: the pass must run");
        const auto binding = registry.selectionRecord(record->job.JobID);
        check(binding.mode() == CollectionMode::Manual && binding.manual.has_value() &&
                  binding.manual->empty(),
              "STOP-PIN: the manual stop must survive the observation merge");
        check(binding.effectiveCollectors().empty(),
              "STOP-PIN: applied/desired collectors must stay empty after scans");
        check(registry.actualCollection(record->job.JobID).collectors.empty(),
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
    const auto refresh = get_refresh_provider();
    check(static_cast<bool>(refresh), "rescan: no on-demand refresh provider registered");
    const int scans_before_refresh = source.scans();
    check(refresh(), "rescan: on-demand refresh did not report a completed pass");
    check(source.scans() > scans_before_refresh,
          "rescan: on-demand refresh did not run a scan");

    // Positive termination evidence still retires through the same pass.
    source.set_scan([&]() { return scan_with({}); });
    probes.set_process(9100, LivenessState::Dead);
    probes.set_cgroup("/cg/slot-95", {}, true);
    registry.reconcileNow();
    check(!find_native(registry, "950.0").has_value(),
          "rescan: positive termination evidence did not retire the instance");
    registry.deinit();

    pass_case("RESCAN", "event-driven and on-demand passes run the same reconcile path");
}

// ---------------------------------------------------------------------------
// DISCOVERY-CREATE-ONLY: a pass discovers managers; it never re-reads or
// merges facts into a record it already owns
// ---------------------------------------------------------------------------

void run_discovery_create_only(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const pid_t manager_pid = 4150001;
    const pid_t member_pid = 4150011;
    probes.set_process(manager_pid, LivenessState::Alive, 424242);
    probes.set_process(member_pid, LivenessState::Alive, 424242);
    Job advertised = make_condor_job("1000.0", manager_pid, 424242, "htcondor01",
                                     "schedd-a", "/cg/slot-1000", {member_pid});
    source.set_scan([&]() { return scan_with({advertised}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "discovery-create-only"),
                                           source, probes));

    const auto created = find_native(registry, "1000.0");
    check(created.has_value(), "DISCOVERY-CREATE-ONLY: the unknown manager was not observed");
    const auto* created_attr = std::get_if<CondorJobAttr>(&created->job.sub_attr);
    check(created_attr != nullptr && created_attr->owner == "alice",
          "DISCOVERY-CREATE-ONLY: the created record does not carry the advertised facts");
    check(source.observes() == 1,
          "DISCOVERY-CREATE-ONLY: the first pass must observe an unknown manager once");
    const std::uint64_t created_revision = created->revision;
    const int scans_after_create = source.scans();

    // The same manager now advertises a different job: another native id and a
    // different owner. The manager is already tracked, so the pass must neither
    // observe it nor merge the advertisement into the record.
    advertised = make_condor_job("1001.0", manager_pid, 424242, "htcondor01",
                                 "schedd-a", "/cg/slot-1000", {member_pid});
    std::get<CondorJobAttr>(advertised.sub_attr).owner = "bob";
    check(registry.reconcileNow(),
          "DISCOVERY-CREATE-ONLY: the second pass did not run");

    check(source.scans() > scans_after_create,
          "DISCOVERY-CREATE-ONLY: the second pass did not enumerate managers");
    check(source.observes() == 1,
          "DISCOVERY-CREATE-ONLY: a known manager was observed again");

    const auto after = find_native(registry, "1000.0");
    check(after.has_value(),
          "DISCOVERY-CREATE-ONLY: a known-manager advertisement renamed the record");
    check(after->job.NativeJobID == "1000.0",
          "DISCOVERY-CREATE-ONLY: a known-manager advertisement replaced the identity");
    const auto* after_attr = std::get_if<CondorJobAttr>(&after->job.sub_attr);
    check(after_attr != nullptr && after_attr->owner == "alice",
          "DISCOVERY-CREATE-ONLY: a known-manager advertisement merged into the record");
    check(after->revision == created_revision,
          "DISCOVERY-CREATE-ONLY: an unapplied advertisement advanced the revision");
    check(after->job.execution.read_order != created->job.execution.read_order,
          "DISCOVERY-CREATE-ONLY: the eager refresh did not run for the record");
    check(!find_native(registry, "1001.0").has_value(),
          "DISCOVERY-CREATE-ONLY: the advertisement created a second record");
    check(count_records(registry) == 1,
          "DISCOVERY-CREATE-ONLY: the pass changed the record count");

    registry.deinit();
    pass_case("DISCOVERY-CREATE-ONLY",
              "only unknown managers are observed: a changed advertisement under a known "
              "manager keeps its created facts, identity and revision");
}

// ---------------------------------------------------------------------------
// MANUAL-REFRESH: a manual registration is refreshed through the same adapter
// path as a discovered one, and a no-change refresh moves no revision
// ---------------------------------------------------------------------------

void run_manual_refresh_parity(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const pid_t manager_pid = 4150101;
    const pid_t first_member = 4150111;
    const pid_t second_member = 4150112;
    probes.set_process(manager_pid, LivenessState::Alive, 515151);
    probes.set_process(first_member, LivenessState::Alive);
    probes.set_process(second_member, LivenessState::Alive);

    source.set_scan([]() { return scan_with({}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "manual-refresh"), source, probes));

    const Job manual = make_manual_condor_job("1100.0", manager_pid, 515151,
                                              "/cg/slot-1100",
                                              {first_member, second_member});
    check(registry.addJob(manual), "MANUAL-REFRESH: the manual registration must succeed");
    const auto created = find_native(registry, "1100.0");
    check(created.has_value(), "MANUAL-REFRESH: the manual record was not indexed");
    check(created->source == FactSource::Manual,
          "MANUAL-REFRESH: a manual registration must start as Manual");
    const std::uint64_t id = created->job.JobID;
    const std::vector<pid_t> created_members = pids_of(created->job);
    check(created_members.size() == 2,
          "MANUAL-REFRESH: the manual registration dropped its bound members");
    check(created->job.execution.manager.pid == manager_pid,
          "MANUAL-REFRESH: the manual registration dropped its manager");
    check(created->job.execution.read_order == 0,
          "MANUAL-REFRESH: a manual fixture must start without a discovery read order");

    // The source enumerates the record's own manager on every pass, so the
    // record is "seen" (absence is not the subject of this case). The
    // advertised job carries another identity: a pass that observed it would
    // create a second record, which the record count below catches.
    const Job advertised = make_condor_job("1100.9", manager_pid, 515151, "htcondor01",
                                           "schedd-a", "/cg/slot-1100", {first_member});
    source.set_scan([&]() { return scan_with({advertised}); });
    check(registry.reconcileNow(), "MANUAL-REFRESH: the first pass did not run");

    const auto after_first = registry.jobRecord(id);
    check(after_first.has_value(), "MANUAL-REFRESH: the refreshed record disappeared");
    check(after_first->job.execution.manager == created->job.execution.manager,
          "MANUAL-REFRESH: the refresh rewrote the execution manager");
    check(pids_of(after_first->job) == created_members,
          "MANUAL-REFRESH: the refresh dropped or replaced the member evidence");
    check(after_first->verified, "MANUAL-REFRESH: a seen record must stay verified");
    check(after_first->source == FactSource::Manual,
          "MANUAL-REFRESH: a refresh must not change the record's provenance");
    check(after_first->job.execution.read_order != 0,
          "MANUAL-REFRESH: the eager refresh did not run for the manual record");
    const std::uint64_t revision_after_first = after_first->revision;
    const std::uint64_t read_order_after_first = after_first->job.execution.read_order;

    // A second, identical pass is a no-change refresh: same evidence, same
    // revision, no durable write.
    check(registry.reconcileNow(), "MANUAL-REFRESH: the second pass did not run");
    const auto after_second = registry.jobRecord(id);
    check(after_second.has_value(),
          "MANUAL-REFRESH: the record disappeared after the second pass");
    check(after_second->revision == revision_after_first,
          "MANUAL-REFRESH: a no-change refresh advanced the revision");
    check(after_second->job.execution.read_order != read_order_after_first,
          "MANUAL-REFRESH: the second pass did not perform its own read");
    check(pids_of(after_second->job) == created_members &&
              after_second->job.execution.manager == created->job.execution.manager,
          "MANUAL-REFRESH: the second refresh disturbed the execution evidence");
    check(count_records(registry) == 1, "MANUAL-REFRESH: the pass duplicated the record");

    registry.deinit();
    pass_case("MANUAL-REFRESH",
              "a manual Condor record keeps its manager and members through the same "
              "adapter refresh, stays verified, and a no-change refresh moves no revision");
}

// ---------------------------------------------------------------------------
// MANUAL-ABSENCE: a manager that disappears from two complete manager passes
// retires a manual record too (positive evidence is blocked in this case)
// ---------------------------------------------------------------------------

void run_manual_absence_retirement(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const pid_t manager_pid = 4150201;
    const pid_t member_pid = 4150211;
    probes.set_process(manager_pid, LivenessState::Dead);
    probes.set_process(member_pid, LivenessState::Dead);
    // Readable but non-empty: NOT positive termination evidence, so only the
    // absence rule can retire the record.
    probes.set_cgroup("/cg/slot-1200", {member_pid}, true);
    source.set_scan([]() { return scan_with({}); });
    JobRegistry registry(heartbeat_options(fresh_dir(base, "manual-absence"), source, probes));

    const Job manual = make_manual_condor_job("1200.0", manager_pid, 616161,
                                              "/cg/slot-1200", {member_pid});
    check(registry.addJob(manual), "MANUAL-ABSENCE: the manual registration must succeed");
    const auto created = find_native(registry, "1200.0");
    check(created.has_value(), "MANUAL-ABSENCE: the manual record was not indexed");
    const std::uint64_t id = created->job.JobID;
    const std::uint64_t retirements_before =
        heartbeat_status()["retirements"].get<std::uint64_t>();

    check(registry.reconcileNow(), "MANUAL-ABSENCE: the first pass did not run");
    check(registry.jobRecord(id).has_value(),
          "MANUAL-ABSENCE: one absence retired the manual record");
    check(heartbeat_status()["retirements"].get<std::uint64_t>() == retirements_before,
          "MANUAL-ABSENCE: one absence counted as a retirement");
    check(heartbeat_status()["suspect_count"] == 1,
          "MANUAL-ABSENCE: the absent record was not reported suspect");

    check(registry.reconcileNow(), "MANUAL-ABSENCE: the second pass did not run");
    check(!registry.jobRecord(id).has_value(),
          "MANUAL-ABSENCE: two complete absences did not retire the manual record");
    check(heartbeat_status()["retirements"].get<std::uint64_t>() == retirements_before + 1,
          "MANUAL-ABSENCE: the retirement was not counted");
    check(!find_native(registry, "1200.0").has_value(),
          "MANUAL-ABSENCE: the retired record kept its identity index entry");

    registry.deinit();
    pass_case("MANUAL-ABSENCE",
              "a manual record whose manager is missing from two complete manager passes "
              "retires on absence, never after one pass and never on a non-empty cgroup");
}

// ---------------------------------------------------------------------------
// EVENT-ADMISSION: the admission gate is a creation gate; an event for an
// already-tracked identity updates it under a closed gate
// ---------------------------------------------------------------------------

void run_event_admission_create_only(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const pid_t manager_pid = 4150301;
    const pid_t first_member = 4150311;
    const pid_t second_member = 4150312;
    probes.set_process(manager_pid, LivenessState::Alive, 717171);
    probes.set_process(first_member, LivenessState::Alive);
    probes.set_process(second_member, LivenessState::Alive);
    source.set_scan([]() { return scan_with({}); });

    // The gate is CLOSED for new identities (capture_all off, condor auto_add
    // unset => Off). A creation attempt therefore refuses and counts a drop,
    // making "the gate did not run" observable instead of assumed.
    auto options = heartbeat_options(fresh_dir(base, "event-admission"), source, probes);
    options.test_capture_all = false;
    JobRegistry registry(options);

    const Job manual = make_manual_condor_job("1300.0", manager_pid, 717171,
                                              "/cg/slot-1300",
                                              {first_member, second_member});
    check(registry.addJob(manual), "EVENT-ADMISSION: the manual registration must succeed");
    const auto created = find_native(registry, "1300.0");
    check(created.has_value(), "EVENT-ADMISSION: the manual record was not indexed");
    const std::uint64_t id = created->job.JobID;
    const std::uint64_t revision_before = created->revision;
    const std::uint64_t dropped_before =
        heartbeat_status()["dropped_observations"].get<std::uint64_t>();

    // An exec event for the identity the registry already owns: it must update
    // the record without re-running the closed admission gate.
    Job update = make_condor_job("1300.0", manager_pid, 717171, "htcondor01",
                                 "schedd-a", "/cg/slot-1300",
                                 {first_member, second_member});
    std::get<CondorJobAttr>(update.sub_attr).owner = "bob";
    const UpsertResult accepted = source.inject(update);
    check(accepted.ok,
          "EVENT-ADMISSION: an event for a tracked identity was refused");
    const auto after = registry.jobRecord(id);
    check(after.has_value(), "EVENT-ADMISSION: the updated record disappeared");
    // The record is a creation snapshot: an observation moves the execution
    // evidence and the provenance, never the scheduler metadata the record was
    // created with.
    const auto* after_attr = std::get_if<CondorJobAttr>(&after->job.sub_attr);
    check(after_attr != nullptr && after_attr->owner == "alice",
          "EVENT-ADMISSION: the event rewrote the record's creation metadata");
    check(after->source == FactSource::Discovery,
          "EVENT-ADMISSION: an accepted observation did not move the provenance");
    check(after->revision > revision_before,
          "EVENT-ADMISSION: the accepted observation did not advance the revision");
    check(heartbeat_status()["dropped_observations"].get<std::uint64_t>() == dropped_before,
          "EVENT-ADMISSION: a tracked identity was sent through the admission gate");

    // Same sink, unknown identity: the gate is armed and refuses the creation.
    probes.set_process(4150302, LivenessState::Alive, 818181);
    probes.set_process(4150313, LivenessState::Alive);
    const Job fresh = make_condor_job("1301.0", 4150302, 818181, "htcondor01",
                                      "schedd-a", "/cg/slot-1301", {4150313});
    const UpsertResult refused = source.inject(fresh);
    check(!refused.ok && refused.code == RegistrationError::NotAdmitted,
          "EVENT-ADMISSION: an unknown identity bypassed the closed admission gate");
    check(heartbeat_status()["dropped_observations"].get<std::uint64_t>() == dropped_before + 1,
          "EVENT-ADMISSION: the refused creation was not counted as dropped");
    check(count_records(registry) == 1,
          "EVENT-ADMISSION: a refused observation created a record");

    registry.deinit();
    pass_case("EVENT-ADMISSION",
              "admission runs only for unknown identities: a closed gate still updates a "
              "tracked record and still refuses a new one");
}

// ---------------------------------------------------------------------------
// GENERATION-CONFLICT: a refused observation must not mark its manager tracked
// ---------------------------------------------------------------------------

void run_generation_conflict_tracking(const std::string& base)
{
    FakeSource source("condor");
    // A second source with the same adapter name enumerates the same manager in
    // the same pass. That makes a false "tracked" mapping observable: a mirror
    // that takes the refused manager as known skips its own observe and marks
    // the old record seen, resetting its absence.
    FakeSource mirror("condor");
    FakeProbes probes;
    const pid_t first_manager = 4150401;
    const pid_t second_manager = 4150402;
    const std::string cgroup = "/cg/slot-1400";
    probes.set_process(first_manager, LivenessState::Dead);
    probes.set_process(second_manager, LivenessState::Alive, 919191);
    // Unreadable cgroup: only the absence rule can retire the first record.
    probes.unset_cgroup(cgroup);

    const Job first = make_condor_job("1400.0", first_manager, 818181, "htcondor01",
                                      "schedd-a", cgroup);
    const Job second = make_condor_job("1400.0", second_manager, 919191, "htcondor01",
                                       "schedd-a", cgroup);
    bool second_only = false;
    const auto scan_now = [&]() {
        return scan_with(second_only ? std::vector<Job>{second} : std::vector<Job>{first});
    };
    source.set_scan(scan_now);
    mirror.set_scan(scan_now);
    auto options = heartbeat_options(fresh_dir(base, "generation-conflict"), source, probes);
    options.test_sources = {{"condor", &source}, {"condor", &mirror}};
    JobRegistry registry(options);

    const auto created = find_native(registry, "1400.0");
    check(created.has_value(),
          "GENERATION-CONFLICT: the first execution was not registered");
    const uint64_t first_id = created->job.JobID;
    check(created->job.execution.manager.pid == first_manager,
          "GENERATION-CONFLICT: the first record does not own its manager");
    const int observes_after_create = source.observes();
    check(observes_after_create == 1,
          "GENERATION-CONFLICT: the creation pass observed the manager more than once");
    check(mirror.observes() == 0,
          "GENERATION-CONFLICT: the second source re-observed a tracked manager");

    // The second execution reuses the identity while the first record is live:
    // its observation is refused as a generation conflict and must stay
    // unknown, not be mapped onto the old record.
    second_only = true;
    check(registry.reconcileNow(),
          "GENERATION-CONFLICT: the conflict pass did not run");
    check(source.observes() == observes_after_create + 1,
          "GENERATION-CONFLICT: the refused manager was not observed");
    check(mirror.observes() == 1,
          "GENERATION-CONFLICT: the refused manager was taken as tracked by the second source");
    const auto still_first = registry.jobRecord(first_id);
    check(still_first.has_value(),
          "GENERATION-CONFLICT: a refused observation replaced the live generation");
    check(still_first->job.execution.manager.pid == first_manager,
          "GENERATION-CONFLICT: a refused observation rewrote the manager");
    check(!still_first->verified,
          "GENERATION-CONFLICT: a refused manager still cleared the old record's absence");
    check(heartbeat_status()["suspect_count"] == 1,
          "GENERATION-CONFLICT: the old record is not reported absent");

    // The next complete pass must observe the manager again (it is not
    // tracked), and the old record's second absence retires it.
    check(registry.reconcileNow(),
          "GENERATION-CONFLICT: the retirement pass did not run");
    check(source.observes() == observes_after_create + 2,
          "GENERATION-CONFLICT: the refused manager became tracked");
    check(mirror.observes() == 2,
          "GENERATION-CONFLICT: the refused manager became tracked by the second source");
    check(!registry.jobRecord(first_id).has_value(),
          "GENERATION-CONFLICT: the old generation was kept alive by a refused observation");
    check(!find_native(registry, "1400.0").has_value(),
          "GENERATION-CONFLICT: the retired identity stayed indexed");

    // With the identity free, the same manager creates the new execution.
    check(registry.reconcileNow(),
          "GENERATION-CONFLICT: the revival pass did not run");
    const auto revived = find_native(registry, "1400.0");
    check(revived.has_value(),
          "GENERATION-CONFLICT: the new execution was not registered");
    check(revived->job.JobID != first_id,
          "GENERATION-CONFLICT: the new execution reused the retired internal id");
    check(revived->job.execution.manager.pid == second_manager,
          "GENERATION-CONFLICT: the new execution does not own its manager");
    check(count_records(registry) == 1,
          "GENERATION-CONFLICT: the revival duplicated the record");

    registry.deinit();
    pass_case("GENERATION-CONFLICT",
              "a refused observation leaves its manager unknown so it is retried, the old "
              "record still retires on absence, and the new execution creates afterwards");
}

// ---------------------------------------------------------------------------
// GATE-KNOWN: an unknown manager for a tracked identity bypasses admission
// ---------------------------------------------------------------------------

void run_pass_admission_create_only(const std::string& base)
{
    FakeSource source("condor");
    FakeProbes probes;
    const pid_t manager_pid = 4150501;
    probes.set_process(manager_pid, LivenessState::Alive, 616161);
    source.set_scan([]() { return scan_with({}); });

    // Closed gate: capture_all off and condor auto_add unset (Off). A creation
    // attempt is refused and counted as dropped, making "the gate did not run"
    // observable.
    auto options = heartbeat_options(fresh_dir(base, "pass-admission"), source, probes);
    options.test_capture_all = false;
    JobRegistry registry(options);

    Job manual = make_manual_condor_job("1500.0", manager_pid, 616161,
                                        "/cg/slot-1500", {});
    // The registration carries the identity but no execution generation yet.
    manual.execution.manager = ProcessIdentity{};
    check(registry.addJob(manual), "GATE-KNOWN: the manual registration must succeed");
    const auto created = find_native(registry, "1500.0");
    check(created.has_value(), "GATE-KNOWN: the manual record was not indexed");
    check(!created->job.execution.manager.complete(),
          "GATE-KNOWN: the fixture must start without an execution manager");
    const uint64_t id = created->job.JobID;
    const std::uint64_t dropped_before =
        heartbeat_status()["dropped_observations"].get<std::uint64_t>();

    // The source enumerates a complete manager for the identity the registry
    // already owns. The creation gate must not run: a closed gate would drop
    // the only observation that can complete the record.
    const Job discovered = make_condor_job("1500.0", manager_pid, 616161, "htcondor01",
                                           "schedd-a", "/cg/slot-1500");
    source.set_scan([&]() { return scan_with({discovered}); });
    check(registry.reconcileNow(),
          "GATE-KNOWN: the discovery pass did not run");

    const auto after = registry.jobRecord(id);
    check(after.has_value(), "GATE-KNOWN: the record disappeared");
    check(after->job.execution.manager.pid == manager_pid,
          "GATE-KNOWN: the tracked identity was dropped by the creation gate");
    check(after->job.execution.manager.complete(),
          "GATE-KNOWN: the observed manager is incomplete");
    check(heartbeat_status()["dropped_observations"].get<std::uint64_t>() == dropped_before,
          "GATE-KNOWN: a tracked identity was sent through the admission gate");
    check(count_records(registry) == 1,
          "GATE-KNOWN: the observation created a second record");

    registry.deinit();
    pass_case("GATE-KNOWN",
              "an unknown manager for an already tracked identity updates the record "
              "without running the creation admission gate");
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
        run_seen_recovery(base);
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
        run_discovery_create_only(base);
        run_manual_refresh_parity(base);
        run_manual_absence_retirement(base);
        run_event_admission_create_only(base);
        run_generation_conflict_tracking(base);
        run_pass_admission_create_only(base);
        std::cout << "ALL PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
