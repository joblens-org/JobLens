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
/* Discovery-adapter acceptance:
 *
 *   * one authoritative pass (enumerateManagers) names the live managers and
 *     owns source health; observe(manager) publishes only finalized
 *     observations with the correct native identity fields, manager
 *     generation, cgroup and members.
 *   * Missing/unreadable ads yield *no* published observation: never a crash,
 *     never a guessed identity.
 *   * start() installs the sink before any thread exists, stop() is idempotent
 *     and joined, stop()-start()-stop() is safe.
 *   * SourceHealth goes initializing -> healthy on a successful pass and
 *     -> degraded on an injected failure, then recovers.
 *
 * Everything runs against a fixture tree under a temporary root: the discovery
 * roots are injectable, so this needs no root, no eBPF and no live scheduler.
 * The eBPF event source is unavailable on an unprivileged host, which the
 * adapters report as an explicit source note; the process-table manager pass is
 * the part under test.
 */
#include "core/job_identity.hpp"
#include "core/job_discovery.hpp"
#include "core/liveness.hpp"
#include "core/scheduler_adapter.hpp"
#include "common/procfs.hpp"
#include "job_watcher/condor_adapter.hpp"
#include "job_watcher/condor_job_watcher.hpp"
#include "job_watcher/slurm_adapter.hpp"
#include "job_watcher/slurm_job_watcher.hpp"
#include "common/config.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;

namespace {

constexpr std::int64_t kFakeNowMs = 1700000000123LL;

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

void write_file(const fs::path& path, const std::string& content)
{
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot write " + path.string());
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
    if (!out) throw std::runtime_error("write failed " + path.string());
}

// /proc/<pid>/stat with `start_ticks` in field 22 (starttime).
std::string stat_line(pid_t pid, const std::string& comm, pid_t ppid, std::uint64_t start_ticks)
{
    std::string line = std::to_string(pid) + " (" + comm + ") S " + std::to_string(ppid);
    for (int field = 5; field <= 21; ++field) line += " 0";
    line += " " + std::to_string(start_ticks) + " 4096 200 0 0 0 0 0 0 0 0 0 0 0\n";
    return line;
}

std::string env_block(const std::vector<std::pair<std::string, std::string>>& variables)
{
    std::string block;
    for (const auto& variable : variables) {
        block += variable.first;
        block += '=';
        block += variable.second;
        block += '\0';
    }
    return block;
}

void make_process(const fs::path& proc_root, pid_t pid, const std::string& comm, pid_t ppid,
                  std::uint64_t start_ticks,
                  const std::vector<std::pair<std::string, std::string>>& environment,
                  const std::string& cgroup_relative)
{
    const auto dir = proc_root / std::to_string(pid);
    fs::create_directories(dir);
    write_file(dir / "stat", stat_line(pid, comm, ppid, start_ticks));

    std::string cmdline = comm + " -f";
    cmdline.push_back('\0');
    write_file(dir / "cmdline", cmdline);
    write_file(dir / "environ", env_block(environment));
    write_file(dir / "cgroup", "0::" + cgroup_relative + "\n");
}

void make_cgroup(const fs::path& cgroup_root, const std::string& relative,
                 const std::vector<pid_t>& pids)
{
    std::string content;
    for (pid_t pid : pids) content += std::to_string(pid) + "\n";
    write_file(cgroup_root / fs::path(relative).relative_path() / "cgroup.procs", content);
}

// The kernel's per-task children file: without a job-scoped cgroup this is the
// only membership source the adapters may read (never a process-table pass).
void make_children(const fs::path& proc_root, pid_t pid, const std::vector<pid_t>& children)
{
    std::string content;
    for (pid_t child : children) content += std::to_string(child) + " ";
    write_file(proc_root / std::to_string(pid) / "task" / std::to_string(pid) / "children",
               content);
}

// Fixture layout mirrors real discovery inputs:
//   <root>/proc/<pid>/{stat,cmdline,environ,cgroup}  process table + env
//   <root>/cgroup/.../cgroup.procs                   execution cgroup membership
//   <root>/condor/ads/*.ad                           _CONDOR_JOB_AD files
//   <root>/slurmd/job<id>/                           slurmd spool per-job dirs
struct Fixture {
    fs::path root;
    fs::path proc;
    fs::path cgroup;
    fs::path ads;
    fs::path slurmd;
    fs::path good_ad;
    fs::path ad_without_global_job_id;
    fs::path missing_ad;

    ~Fixture()
    {
        if (root.empty()) return;
        std::error_code ec;
        fs::remove_all(root, ec);
    }
};

// Fills `fixture` in place: the object owns the temp root and removes it.
void make_fixture(Fixture& fixture, const std::string& base)
{
    std::string pattern = base + ".XXXXXX";
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    char* created = ::mkdtemp(buffer.data());
    if (!created) throw std::runtime_error("mkdtemp failed for " + base);
    fixture.root = created;
    fixture.proc = fixture.root / "proc";
    fixture.cgroup = fixture.root / "cgroup";
    fixture.ads = fixture.root / "condor" / "ads";
    fixture.slurmd = fixture.root / "slurmd";
    fixture.good_ad = fixture.ads / "76226337.0.ad";
    fixture.ad_without_global_job_id = fixture.ads / "no-global-job-id.ad";
    fixture.missing_ad = fixture.ads / "missing.ad";

    write_file(fixture.proc / "sys" / "kernel" / "random" / "boot_id", "boot-fixture-0001\n");
    fs::create_directories(fixture.cgroup);
    fs::create_directories(fixture.ads);

    write_file(fixture.good_ad,
               "GlobalJobId = \"scheduler@schedd07.fixture#76226337.0#1775723895\"\n"
               "Owner = \"alice\"\n"
               "Cmd = \"/bin/sleep\"\n");
    // An ad that exists but cannot name a job: incomplete, never guessed.
    write_file(fixture.ad_without_global_job_id, "Owner = \"bob\"\n");

    // Condor: one healthy execution with two member processes in its slot
    // cgroup, one starter whose ad file is missing, one whose ad has no
    // GlobalJobId.
    make_process(fixture.proc, 4101, "condor_starter", 1, 7001,
                 {{"_CONDOR_JOB_AD", fixture.good_ad.string()}}, "/htcondor/slot1");
    make_process(fixture.proc, 4110, "sleep", 4101, 7010, {{"CONDOR_SLOT", "1"}},
                 "/htcondor/slot1");
    make_process(fixture.proc, 4111, "sleep", 4101, 7011, {{"CONDOR_SLOT", "1"}},
                 "/htcondor/slot1");
    make_cgroup(fixture.cgroup, "/htcondor/slot1", {4101, 4110, 4111});
    // Every slot directory exists with its cgroup.procs, mirroring the real
    // startd layout the cgroup-subtree enumeration tier lists.
    make_cgroup(fixture.cgroup, "/htcondor/slot2", {4201});
    make_cgroup(fixture.cgroup, "/htcondor/slot3", {4301});

    make_process(fixture.proc, 4201, "condor_starter", 1, 7020,
                 {{"_CONDOR_JOB_AD", fixture.missing_ad.string()}}, "/htcondor/slot2");
    make_process(fixture.proc, 4301, "condor_starter", 1, 7030,
                 {{"_CONDOR_JOB_AD", fixture.ad_without_global_job_id.string()}},
                 "/htcondor/slot3");

    // Slurm: one stepd with two member processes, one stepd without any
    // attributable member, and a spool job directory with no live stepd.
    make_process(fixture.proc, 5101, "slurmstepd", 1, 8001, {{"SLURM_VERSION", "24.05"}},
                 "/slurm/job_9002/step_3");
    make_process(fixture.proc, 5110, "sleep", 5101, 8010,
                 {{"SLURM_JOB_ID", "9002"},
                  {"SLURM_STEP_ID", "3"},
                  {"SLURM_CLUSTER_NAME", "slurm-fixture"},
                  {"SLURM_JOB_USER", "alice"},
                  {"SLURM_JOB_NAME", "demo"},
                  {"SLURM_JOB_PARTITION", "debug"}},
                 "/slurm/job_9002/step_3");
    make_process(fixture.proc, 5111, "sleep", 5101, 8011,
                 {{"SLURM_JOB_ID", "9002"},
                  {"SLURM_STEP_ID", "3"},
                  {"SLURM_CLUSTER_NAME", "slurm-fixture"}},
                 "/slurm/job_9002/step_3");
    make_cgroup(fixture.cgroup, "/slurm/job_9002/step_3", {5101, 5110, 5111});

    make_process(fixture.proc, 5201, "slurmstepd", 1, 8100, {{"SLURM_VERSION", "24.05"}},
                 "/slurm/job_9003/step_1");

    fs::create_directories(fixture.slurmd / "job9001");
    fs::create_directories(fixture.slurmd / "not-a-job-directory");
}

// The explicit per-adapter namespace override. Condor fixtures pass the
// cluster namespace here; slurm fixtures pass "" so their observations
// derive the namespace from the fixture environ (or stay incomplete, as the
// spool-only entries must).
DiscoveryOptions fixture_options(const Fixture& fixture,
                                 const std::string& cluster_name = "htcondor-fixture")
{
    DiscoveryOptions options;
    options.enable_event_source = false;  // unprivileged fixture: no eBPF
    options.discovery.proc_root = fixture.proc.string();
    options.discovery.cgroup_root = fixture.cgroup.string();
    options.discovery.cluster_name = cluster_name;
    // The fixture's slot cgroups live at <cgroup>/htcondor/slotN: the
    // cgroup-subtree enumeration tier must engage for the condor cases
    // (slurm ignores the field).
    options.starter_cgroup_subtree = "htcondor";
    options.clock = [] { return kFakeNowMs; };
    return options;
}

// The result of one authoritative pass: the enumerated managers plus the Job
// of every manager this test could observe. The registry observes only the
// managers it does not track yet; observing all of them here is the same read
// path.
struct PassSummary {
    ManagerSet set;
    std::vector<Job> jobs;
    std::size_t unobservable = 0;
};

PassSummary run_pass(IDiscoverySource& source)
{
    PassSummary summary;
    summary.set = source.enumerateManagers();
    summary.jobs.reserve(summary.set.managers.size());
    for (const auto& manager : summary.set.managers) {
        auto job = source.observe(manager);
        if (job) summary.jobs.push_back(std::move(*job));
        else ++summary.unobservable;
    }
    return summary;
}

// A published observation is finalized, so a native id is enough to find it.
const Job* find_complete(const std::vector<Job>& jobs, const std::string& native_job_id)
{
    for (const auto& job : jobs) {
        if (job.NativeJobID == native_job_id) return &job;
    }
    return nullptr;
}

const ProcessIdentity* manager_for(const ManagerSet& set, pid_t pid)
{
    for (const auto& manager : set.managers) {
        if (manager.pid == pid) return &manager;
    }
    return nullptr;
}

std::vector<pid_t> member_pids(const Job& job)
{
    std::vector<pid_t> pids;
    for (const auto& member : job.execution.members) pids.push_back(member.pid);
    return pids;
}

// Managers of the published jobs, in observation order.
std::vector<ProcessIdentity> observed_managers(const std::vector<Job>& jobs)
{
    std::vector<ProcessIdentity> managers;
    managers.reserve(jobs.size());
    for (const auto& job : jobs) managers.push_back(job.execution.manager);
    return managers;
}

// Deterministic comparison order: the enumeration's order is not part of the
// contract, the (pid, start ticks, boot id) set is.
void sort_managers(std::vector<ProcessIdentity>& managers)
{
    std::sort(managers.begin(), managers.end(),
              [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                  if (lhs.pid != rhs.pid) return lhs.pid < rhs.pid;
                  if (lhs.start_ticks != rhs.start_ticks)
                      return lhs.start_ticks < rhs.start_ticks;
                  return lhs.boot_id < rhs.boot_id;
              });
}

bool contains(const std::string& haystack, const std::string& needle)
{
    return haystack.find(needle) != std::string::npos;
}

// A DiscoverySink carrying only the registration callback (the fixture has
// no eBPF, so no exec hints can arrive).
DiscoverySink discovery_sink(JobSink on_job)
{
    DiscoverySink sink;
    sink.on_job = std::move(on_job);
    return sink;
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void condor_pass_finds_fixture_job(const Fixture& fixture)
{
    CondorJobWatcher watcher(fixture_options(fixture));
    check(watcher.health().state == SourceState::Initializing,
          "a fresh adapter starts in SourceState::Initializing");
    check(!watcher.polling(), "constructing a discovery adapter must not start polling");

    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "condor pass must succeed on the fixture: " + pass.set.error);
    check(pass.set.complete, "condor pass must report complete source coverage");
    check(pass.set.managers.size() == 3,
          "three slot cgroups must name three managers, got " +
              std::to_string(pass.set.managers.size()));
    check(pass.jobs.size() == 1 && pass.unobservable == 2,
          "one complete and two unobservable condor managers, got jobs=" +
              std::to_string(pass.jobs.size()) + " unobservable=" +
              std::to_string(pass.unobservable));
    const std::string note = watcher.health().last_note;
    check(contains(note, "starters enumerated from cgroup subtree"),
          "the cgroup-subtree enumeration tier must engage and say so in the health notes");
    check(contains(note, "eBPF"),
          "an unavailable/disabled eBPF event source must be reported as an explicit note");
    // Every observed Job carries a process-monotonic read order, assigned
    // before the builder reads, so the store can reject an older pass

    for (const auto& discovered : pass.jobs)
        check(discovered.execution.read_order > 0,
              "a discovered Job must carry a read order");

    const auto* job = find_complete(pass.jobs, "76226337.0");
    check(job != nullptr, "the fixture Condor job must be a complete discovery");
    check(job->subtype_name == "condor", "scheduler type must be condor");
    const auto identity = canonicalize(selector_of(*job));
    check(identity.has_value(), "a ready discovery resolves a canonical identity");
    check(identity->key() == "condor|htcondor-fixture@scheduler@schedd07.fixture|76226337.0",
          "canonical key must keep cluster namespace, schedd tag and native id, got " +
              identity->key());
    check(job->clusterTag == "scheduler@schedd07.fixture",
          "the schedd namespace must come from the job ad");
    check(job->execution.manager.pid == 4101 &&
              job->execution.manager.start_ticks == 7001,
          "manager pid/start ticks must identify the executing condor_starter");
    check(job->execution.manager.boot_id == "boot-fixture-0001",
          "generation must carry the boot id");
    check(job->execution.manager.complete(), "generation evidence must be complete");
    const auto& condor_attr = std::get<CondorJobAttr>(job->sub_attr);
    check(condor_attr.slots_cgroup_path == (fixture.cgroup / "htcondor" / "slot1").string(),
          "the slot cgroup path must be discovered, got " + condor_attr.slots_cgroup_path);
    check(job->execution.members_authoritative, "member process identities must be complete");
    const auto members = member_pids(*job);
    check(members.size() == 2 && members[0] == 4110 && members[1] == 4111,
          "members are the slot cgroup processes without the starter");
    check(condor_attr.owner == "alice", "owner must come from the job ad");
    check(condor_attr.scheduler_host == "schedd07.fixture",
          "schedd host must come from the job ad");

    const auto health = watcher.health();
    check(health.state == SourceState::Healthy, "a successful pass makes the source healthy");
    check(health.last_scan_success_ms == kFakeNowMs,
          "health must record the successful pass timestamp");
    check(health.consecutive_failures == 0, "a successful pass resets the failure count");
    check(!health.polling, "the manager pass alone must not start a poll thread");
}

void condor_incomplete_observations_are_not_published(const Fixture& fixture)
{
    CondorJobWatcher watcher(fixture_options(fixture));
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "condor pass must succeed: " + pass.set.error);

    // Incomplete discoveries are never published, so the registry cannot see a
    // half-read job. The two starters with an unusable ad are enumerated (they
    // are live managers) but observe() answers nullopt for them.
    check(pass.unobservable == 2,
          "both starters with an unusable ad must stay unpublished, got " +
              std::to_string(pass.unobservable));
    const auto* missing_ad = manager_for(pass.set, 4201);
    check(missing_ad != nullptr, "the starter with the missing ad must be enumerated");
    check(!watcher.observe(*missing_ad).has_value(),
          "a missing ad file must not publish a Job");
    const auto* unnamed_ad = manager_for(pass.set, 4301);
    check(unnamed_ad != nullptr, "the starter with the nameless ad must be enumerated");
    check(!watcher.observe(*unnamed_ad).has_value(),
          "an ad without GlobalJobId must not publish a Job");
    for (const auto& discovered : pass.jobs) {
        check(!discovered.NativeJobID.empty(),
              "no native job id may be synthesized for an unreadable ad");
        check(execution_complete(discovered.execution),
              "a published discovery must carry complete execution evidence");
    }
}

void slurm_pass_finds_fixture_job(const Fixture& fixture)
{
    auto options = fixture_options(fixture, "");
    options.discovery.slurm_spool_dir = fixture.slurmd.string();
    SlurmJobWatcher watcher(options);

    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "slurm pass must succeed on the fixture: " + pass.set.error);
    check(pass.set.complete, "slurm pass must report complete source coverage");
    check(pass.set.managers.size() == 2,
          "two slurmstepd processes must name two managers, got " +
              std::to_string(pass.set.managers.size()));
    check(pass.jobs.size() == 1 && pass.unobservable == 1,
          "one complete and one unobservable slurm manager, got jobs=" +
              std::to_string(pass.jobs.size()) + " unobservable=" +
              std::to_string(pass.unobservable));

    const auto* job = find_complete(pass.jobs, "9002");
    check(job != nullptr, "the fixture Slurm job must be a complete discovery");
    check(job->subtype_name == "slurm", "scheduler type must be slurm");
    const auto& slurm_step_attr = std::get<SlurmJobAttr>(job->sub_attr);
    check(slurm_step_attr.step_id == 3,
          "step id must be the normalized member step");
    auto slurm_selector = selector_of(*job);
    if (const auto step_name = slurm_step_name(slurm_step_attr.step_id))
        slurm_selector.step_id = *step_name;
    else
        slurm_selector.step_id = std::to_string(slurm_step_attr.step_id);
    const auto slurm_identity = canonicalize(slurm_selector);
    check(slurm_identity && slurm_identity->key() == "slurm|slurm-fixture|9002|3",
          "canonical key must keep cluster, job id and step");
    check(job->execution.manager.pid == 5101 &&
              job->execution.manager.start_ticks == 8001,
          "manager pid/start ticks must identify the executing slurmstepd");
    const auto& slurm_attr = std::get<SlurmJobAttr>(job->sub_attr);
    check(slurm_attr.cgroup_path ==
              (fixture.cgroup / "slurm" / "job_9002" / "step_3").string(),
          "the step cgroup path must be discovered, got " + slurm_attr.cgroup_path);
    const auto members = member_pids(*job);
    check(members.size() == 2 && members[0] == 5110 && members[1] == 5111,
          "members are the step processes without slurmstepd");
    check(job->execution.members_authoritative,
          "a readable step cgroup must make the member set authoritative");
    check(slurm_attr.user == "alice" && slurm_attr.job_name == "demo" &&
              slurm_attr.partition == "debug",
          "scheduler metadata must come from the members' environment");

    // A slurmstepd without attributable members is enumerated (it is live) but
    // never published, so the registry cannot see a half-read job.
    const auto* unattributable = manager_for(pass.set, 5201);
    check(unattributable != nullptr, "the unattributable stepd must be enumerated");
    check(!watcher.observe(*unattributable).has_value(),
          "a slurmstepd without attributable members must not publish a Job");
    for (const auto& discovered : pass.jobs) {
        check(execution_complete(discovered.execution),
              "a published slurm discovery must carry complete execution evidence");
    }
}

// The spool source no longer produces observations; it only participates in
// the pass coverage. An unreadable configured spool root must degrade the
// source, and spool entries must never become managers.
void slurm_spool_root_is_pass_coverage(const Fixture& fixture)
{
    auto options = fixture_options(fixture, "");
    options.discovery.slurm_spool_dir = (fixture.root / "missing-spool").string();
    SlurmJobWatcher watcher(options);

    const auto failed = watcher.enumerateManagers();
    check(!failed.complete,
          "an unreadable configured spool root must make the pass incomplete");
    check(contains(failed.error, "spool"),
          "the pass error must name the spool root: " + failed.error);
    check(failed.managers.size() == 2,
          "the live stepds are still enumerated, got " +
              std::to_string(failed.managers.size()));
    auto health = watcher.health();
    check(health.state == SourceState::Degraded,
          "an incomplete pass degrades the source");
    check(health.consecutive_failures == 1, "the failure count must advance");

    options.discovery.slurm_spool_dir = fixture.slurmd.string();
    watcher.set_discovery_environment(options.discovery);
    const auto recovered = watcher.enumerateManagers();
    check(recovered.error.empty() && recovered.complete,
          "the pass recovers once the spool root is readable: " + recovered.error);
    check(recovered.managers.size() == 2,
          "spool entries must not become managers, got " +
              std::to_string(recovered.managers.size()));
    health = watcher.health();
    check(health.state == SourceState::Healthy,
          "a readable spool root restores healthy state");
}


void health_degrades_and_recovers(const Fixture& fixture)
{
    auto options = fixture_options(fixture);
    options.discovery.proc_root = (fixture.root / "missing-proc").string();
    CondorJobWatcher watcher(options);
    check(watcher.health().state == SourceState::Initializing, "starts initializing");

    const auto failed = watcher.enumerateManagers();
    check(!failed.error.empty(), "a pass against a missing proc root must fail");
    check(!failed.complete, "a failed pass is never complete");
    check(failed.managers.empty(), "a failed pass must not invent managers");
    check(contains(failed.error, "missing-proc"), "the pass error must name the missing root");

    auto health = watcher.health();
    check(health.state == SourceState::Degraded, "a failed pass degrades the source");
    check(contains(health.last_error, "missing-proc"), "health must keep the failure text");
    check(health.consecutive_failures == 1, "the failure count must advance");
    check(health.last_scan_success_ms == 0, "a failure must not look like a success");

    watcher.set_discovery_environment(fixture_options(fixture).discovery);
    const auto recovered = watcher.enumerateManagers();
    check(recovered.error.empty() && recovered.complete,
          "the pass recovers once the root is readable: " + recovered.error);
    health = watcher.health();
    check(health.state == SourceState::Healthy, "a successful pass restores healthy state");
    check(health.consecutive_failures == 0, "recovery resets the failure count");
    check(health.last_scan_success_ms == kFakeNowMs, "recovery records the success timestamp");
    check(health.last_error.empty(), "a successful pass clears the last error");
}

void rescan_request_is_consumed(const Fixture& fixture)
{
    CondorJobWatcher watcher(fixture_options(fixture));
    watcher.request_rescan();
    check(watcher.health().rescan_requested, "a rescan request must be visible in health");
    watcher.enumerateManagers();
    check(!watcher.health().rescan_requested, "a pass satisfies the pending rescan request");
}

void construction_does_nothing_until_start(const Fixture& fixture)
{
    // The only construction path: no Config read, no eBPF load, no thread.
    CondorJobWatcher watcher(fixture_options(fixture));
    check(!watcher.polling(), "a fresh adapter must not poll");
    check(!watcher.thread_joinable(), "a fresh adapter must not own a thread");
    check(!watcher.health().polling, "health must not report polling before start()");
    check(watcher.health().state == SourceState::Initializing,
          "a fresh adapter is initializing until a pass ran");

    // A pass before start() works: the registry heartbeat owns the timing.
    const auto early = watcher.enumerateManagers();
    check(early.error.empty() && early.complete,
          "the manager pass must work without start(): " + early.error);
    check(watcher.health().state == SourceState::Healthy,
          "a pass records its health without start()");
}

void observe_hint_targets_one_execution(const Fixture& fixture)
{
    CondorJobWatcher condor(fixture_options(fixture));
    // 4110 is a member process whose parent is the starter 4101.
    const auto targeted = condor.observeForHint(4110);
    check(targeted.has_value(), "a hint under a starter must resolve");
    check(targeted->NativeJobID == "76226337.0",
          "the targeted discovery must name the fixture job");
    check(targeted->execution.manager.pid == 4101,
          "the manager is the hint's parent (the starter)");
    check(member_pids(*targeted).size() == 2,
          "the targeted discovery keeps the cgroup member list");

    // Unresolvable hints yield nullopt; discovery then waits for the next pass.
    check(!condor.observeForHint(9999).has_value(), "a dead pid must not resolve");
    check(!condor.observeForHint(4101).has_value(),
          "a pid whose parent is not a starter must not resolve");

    auto slurm_options = fixture_options(fixture, "");
    slurm_options.discovery.slurm_spool_dir = (fixture.slurmd).string();
    SlurmJobWatcher slurm(slurm_options);
    // 5110's parent is the stepd 5101 of job 9002 step 3.
    const auto step = slurm.observeForHint(5110);
    check(step.has_value(), "a hint under a stepd must resolve");
    check(step->NativeJobID == "9002" &&
              std::get<SlurmJobAttr>(step->sub_attr).step_id == 3,
          "the targeted slurm discovery must name job and step");
    check(step->execution.manager.pid == 5101,
          "the manager is the hint's parent (the stepd)");
}

void polymorphic_source_interface(const Fixture& fixture)
{
    CondorJobWatcher condor(fixture_options(fixture));
    SlurmJobWatcher slurm(fixture_options(fixture, ""));

    IDiscoverySource* sources[] = {&condor, &slurm};
    check(sources[0]->name() == "condor", "the condor source must name itself");
    check(sources[0]->name() == "condor", "condor type");
    check(sources[1]->name() == "slurm", "the slurm source must name itself");
    check(sources[1]->name() == "slurm", "slurm type");

    for (IDiscoverySource* source : sources) {
        source->requestRescan();
        check(source->health().rescan_requested,
              "the interface must expose the rescan request");
        const ManagerSet pass = source->enumerateManagers();
        check(source->health().rescan_requested == false,
              "an interface pass satisfies the rescan request");
        check(pass.error.empty() && pass.complete,
              "the interface pass must complete against the fixture: " + pass.error);
        check(source->health().state == SourceState::Healthy,
              "the interface health must record the pass");
    }

    // Without an event source there is nothing to poll; the sink stays installed.
    check(!sources[0]->start(DiscoverySink{}),
          "start() without an event source must report nothing to poll");
}

void lifecycle_is_safe(const Fixture& fixture)
{
    CondorJobWatcher watcher(fixture_options(fixture));

    std::atomic<int> delivered{0};
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    check(delivered.load() == 0,
          "no callback may run before start(): the sink is a start() argument");

    // The fixture has no event source, so there is nothing to poll: start()
    // refuses the thread and discovery stays with the registry pass.
    check(!watcher.start(discovery_sink(
              [&delivered](const Job&) { delivered.fetch_add(1); return UpsertResult{}; })),
          "start() without an event source must report nothing to poll");
    check(!watcher.polling(), "a refused start() must not report polling");
    check(!watcher.thread_joinable(), "a refused start() must not own a thread");
    check(!watcher.health().polling, "health must not report polling after a refused start");

    watcher.stop();
    check(!watcher.polling() && !watcher.thread_joinable(),
          "stop() after a refused start() is safe");
    watcher.stop();  // idempotent
    check(!watcher.thread_joinable(), "a second stop() must stay a no-op");

    // The adapter stays usable for the authoritative pass after start()/stop().
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty() && pass.set.complete,
          "the adapter must serve a pass after stop(): " + pass.set.error);
    check(pass.jobs.size() == 1 && pass.unobservable == 2,
          "the pass must still publish the fixture job, got jobs=" +
              std::to_string(pass.jobs.size()));
    check(delivered.load() == 0,
          "the exec-event sink must not receive manager-pass discoveries");

    // Same lifecycle for the Slurm adapter.
    SlurmJobWatcher slurm(fixture_options(fixture, ""));
    check(!slurm.start(discovery_sink([](const Job&) { return UpsertResult{}; })),
          "the Slurm adapter must refuse a thread without an event source");
    check(!slurm.polling() && !slurm.thread_joinable(),
          "a refused Slurm start() must own no thread");
    slurm.stop();
    slurm.stop();
    check(!slurm.polling() && !slurm.thread_joinable(), "Slurm stop() is idempotent and joined");
}

// ---------------------------------------------------------------------------
// Adapter fact refresh: the per-record reader the unified update chain uses.
// managerOfPid names the framework supervisor of one job pid; refreshFacts
// refreshes one record in place (facts + execution evidence) and reports what
// the CALLER's probes saw, so the registry keeps deciding retirement.
// ---------------------------------------------------------------------------

// One Condor record as the registry holds it: typed facts, a member identity
// and no manager yet — refreshFacts must recover the manager from the member.
Job condor_refresh_record(const Fixture& fixture)
{
    Job job;
    job.subtype = JobSubType::Condor;
    job.subtype_name = "condor";
    job.NativeJobID = "76226337.0";
    job.cluster_name = "htcondor-fixture";
    CondorJobAttr attr;
    attr.slots_cgroup_path = (fixture.cgroup / "htcondor" / "slot1").string();
    job.sub_attr = attr;
    job.execution.members = {ProcessIdentity{4110, 7010, "boot-fixture-0001"}};
    return job;
}

// A process probe that records what it was asked and reports the fixture
// manager as gone: the adapter must consume the answer, not re-probe.
ProcessLivenessProbe recording_probe(std::vector<pid_t>& asked, pid_t dead_pid)
{
    return [&asked, dead_pid](const ProcessIdentity& identity) -> LivenessResult {
        asked.push_back(identity.pid);
        if (identity.pid == dead_pid)
            return LivenessResult{LivenessState::Dead, "fixture: manager ended"};
        return LivenessResult{LivenessState::Alive, "fixture: process live"};
    };
}

void condor_adapter_refresh_evidence(const Fixture& fixture)
{
    CondorSchedulerAdapter adapter(DiscoveryTypeSettings{});
    auto* watcher = dynamic_cast<CondorJobWatcher*>(&adapter.source());
    check(watcher != nullptr,
          "the condor adapter must expose its watcher through source()");
    watcher->set_discovery_environment(fixture_options(fixture).discovery);

    // managerOfPid: the starter that launched the job process.
    const auto manager = adapter.managerOfPid(4110);
    check(manager.has_value(), "a condor job process must resolve its starter");
    check(manager->pid == 4101 && manager->start_ticks == 7001,
          "the manager identity must be the parent's pid and start ticks");
    check(manager->boot_id == "boot-fixture-0001",
          "the manager identity must carry the fixture boot id");
    check(manager->complete(), "a resolved manager must be a complete identity");
    check(!adapter.managerOfPid(4101).has_value(),
          "a pid whose parent is not a condor_starter must not resolve");
    check(!adapter.managerOfPid(9999).has_value(), "an absent pid must not resolve");

    Job job = condor_refresh_record(fixture);
    std::vector<pid_t> asked;
    const ProcessLivenessProbe process_probe = recording_probe(asked, 4101);
    const RefreshResult result =
        adapter.refreshFacts(job, process_probe, make_cgroup_membership_probe());

    check(job.execution.read_order > 0, "refreshFacts must assign a read order");
    check(job.execution.manager.pid == 4101 && job.execution.manager.start_ticks == 7001 &&
              job.execution.manager.boot_id == "boot-fixture-0001",
          "refreshFacts must resolve the manager from a member pid");
    check(job.execution.manager.complete(),
          "a refresh whose manager is resolvable must leave a complete generation");
    check(job.NativeJobID == "76226337.0",
          "a targeted refresh must keep the record's native id, got " + job.NativeJobID);
    check(job.clusterTag.empty() && job.cluster_name == "htcondor-fixture",
          "a refresh must not rewrite the record's creation namespace, got '" +
              job.clusterTag + "'");
    const auto members = member_pids(job);
    check(members.size() == 2 && members[0] == 4110 && members[1] == 4111,
          "refreshFacts must refresh the member set from the slot cgroup");
    check(job.execution.members[0].start_ticks == 7010 &&
              job.execution.members[0].boot_id == "boot-fixture-0001",
          "member evidence must carry the full process triple");
    check(job.execution.members_authoritative,
          "a readable slot cgroup makes the member list authoritative");

    check(result.manager == LivenessState::Dead,
          "the injected process probe must decide manager liveness");
    check(!asked.empty() && asked.front() == 4101,
          "the injected probe must be asked about the manager first");
    check(result.any_member_live, "a live member process must be reported");
    check(!result.member_inconclusive,
          "the fixture probe answers every member, so none is inconclusive");
    check(!result.cgroup_empty_confirmed,
          "a non-empty execution cgroup is not confirmed empty");

    // The cgroup probe is the caller's too: a readable but empty cgroup is the
    // positive termination evidence the retirement rule needs.
    const CgroupMembershipProbe empty_cgroup = [](const std::string&) {
        CgroupMembership membership;
        membership.readable = true;
        return membership;
    };
    const RefreshResult empty = adapter.refreshFacts(job, process_probe, empty_cgroup);
    check(empty.cgroup_empty_confirmed,
          "a readable empty execution cgroup must be reported as confirmed empty");

    // A record with no resolvable manager and no cgroup path reports an
    // Unknown manager: unresolved is never death evidence.
    Job orphan;
    orphan.subtype = JobSubType::Condor;
    orphan.subtype_name = "condor";
    orphan.sub_attr = CondorJobAttr{};
    orphan.execution.members = {ProcessIdentity{9999, 0, {}}};
    const RefreshResult unresolved =
        adapter.refreshFacts(orphan, process_probe, make_cgroup_membership_probe());
    check(unresolved.manager == LivenessState::Unknown,
          "a record without a manager identity must report Unknown, not Dead");
    check(!unresolved.cgroup_empty_confirmed,
          "a record without a cgroup path cannot confirm an empty cgroup");
}

void slurm_adapter_refresh_evidence(const Fixture& fixture)
{
    SlurmSchedulerAdapter adapter(DiscoveryTypeSettings{});
    auto* watcher = dynamic_cast<SlurmJobWatcher*>(&adapter.source());
    check(watcher != nullptr, "the slurm adapter must expose its watcher through source()");
    watcher->set_discovery_environment(fixture_options(fixture, "").discovery);

    // The per-job manager is slurmstepd, never slurmd.
    const auto manager = adapter.managerOfPid(5110);
    check(manager.has_value(), "a slurm job process must resolve its step daemon");
    check(manager->pid == 5101 && manager->start_ticks == 8001,
          "the manager identity must be the stepd's pid and start ticks");
    check(manager->boot_id == "boot-fixture-0001",
          "the manager identity must carry the fixture boot id");
    check(!adapter.managerOfPid(5101).has_value(),
          "a pid whose parent is not a slurmstepd must not resolve");

    Job job;
    job.subtype = JobSubType::Slurm;
    job.subtype_name = "slurm";
    job.NativeJobID = "9002";
    SlurmJobAttr attr;
    attr.job_id = 9002;
    attr.step_id = 3;
    attr.cluster_name = "slurm-fixture";
    attr.cgroup_path = (fixture.cgroup / "slurm" / "job_9002" / "step_3").string();
    job.sub_attr = attr;
    job.execution.members = {ProcessIdentity{5110, 8010, "boot-fixture-0001"}};

    std::vector<pid_t> asked;
    const ProcessLivenessProbe process_probe = recording_probe(asked, 5101);
    const RefreshResult result =
        adapter.refreshFacts(job, process_probe, make_cgroup_membership_probe());

    check(job.execution.read_order > 0,
          "the slurm refresh must assign a read order");
    check(job.execution.manager.pid == 5101 && job.execution.manager.complete(),
          "the slurm refresh must resolve the stepd from a member pid");
    check(job.NativeJobID == "9002" && std::get<SlurmJobAttr>(job.sub_attr).step_id == 3,
          "the targeted refresh must keep job and step identity");
    const auto members = member_pids(job);
    check(members.size() == 2 && members[0] == 5110 && members[1] == 5111,
          "the step cgroup members must be the refreshed member set");
    check(result.manager == LivenessState::Dead,
          "the injected probe must decide the stepd's liveness");
    check(result.any_member_live, "a live step member must be reported");
}

// Without a readable cgroup the refresh reads the manager's own children file
// (/proc/<pid>/task/<tid>/children) instead of walking the process table: one
// execution with an unreadable ad still keeps a correct member set.
// Registered last: it extends the shared fixture with its own processes.
void condor_refresh_without_cgroup_reads_children(const Fixture& fixture)
{
    make_process(fixture.proc, 4601, "condor_starter", 1, 7301, {}, "/");
    make_process(fixture.proc, 4610, "sleep", 4601, 7310, {{}}, "/");
    // The kernel's per-task children file is the only membership source here.
    write_file(fixture.proc / "4601" / "task" / "4601" / "children", "4610\n");

    CondorSchedulerAdapter adapter(DiscoveryTypeSettings{});
    auto* watcher = dynamic_cast<CondorJobWatcher*>(&adapter.source());
    check(watcher != nullptr, "the condor adapter must expose its watcher");
    watcher->set_discovery_environment(fixture_options(fixture).discovery);

    Job job;
    job.subtype = JobSubType::Condor;
    job.subtype_name = "condor";
    // A recorded cgroup that cannot be read: the refresh must fall back to the
    // manager's children rather than the process table.
    CondorJobAttr attr;
    attr.slots_cgroup_path = (fixture.root / "no-such-cgroup").string();
    job.sub_attr = attr;
    job.execution.members = {ProcessIdentity{4610, 7310, "boot-fixture-0001"}};

    std::vector<pid_t> asked;
    const RefreshResult result = adapter.refreshFacts(
        job, recording_probe(asked, 4601), make_cgroup_membership_probe());

    check(job.execution.manager.pid == 4601 && job.execution.manager.start_ticks == 7301,
          "the manager must be resolved from the member's parent");
    const auto members = member_pids(job);
    check(members.size() == 1 && members[0] == 4610,
          "the manager's children must be the refreshed member set");
    check(!job.execution.members_authoritative,
          "a children-only read cannot claim an authoritative member list");
    check(result.manager == LivenessState::Dead,
          "the caller's probe must decide the manager's liveness");
    check(!result.cgroup_empty_confirmed,
          "an unreadable cgroup is never reported as confirmed empty");
}


// ---------------------------------------------------------------------------
// Regression: the classic HTCondor layout where _CONDOR_JOB_AD lives on the
// job child process, not on the starter (the old exec-event watcher read it
// there). The manager pass and the exec-hint path must both still find it.
// ---------------------------------------------------------------------------

void condor_ad_on_member_process_is_found(const Fixture& fixture)
{
    // Reuse the fixture root for the extra ad file; the extra processes get
    // their own slot cgroup.
    const auto ad = fixture.ads / "76226340.0.ad";
    write_file(ad,
               "GlobalJobId = \"scheduler@schedd07.fixture#76226340.0#1775723895\"\n"
               "Owner = \"carol\"\n"
               "Cmd = \"/bin/sleep\"\n");

    make_process(fixture.proc, 4401, "condor_starter", 1, 7101, {}, "/htcondor/slot4");
    make_process(fixture.proc, 4410, "sleep", 4401, 7110,
                 {{"_CONDOR_JOB_AD", ad.string()}}, "/htcondor/slot4");
    make_cgroup(fixture.cgroup, "/htcondor/slot4", {4401, 4410});

    CondorJobWatcher watcher(fixture_options(fixture));
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "the pass must succeed: " + pass.set.error);
    const auto* job = find_complete(pass.jobs, "76226340.0");
    check(job != nullptr,
          "an ad that lives on the job process must be discovered by the pass");
    check(std::get<CondorJobAttr>(job->sub_attr).owner == "carol",
          "the ad metadata must come from the member's ad");

    // The exec-hint path must prefer the hinted pid's own environment.
    CondorJobWatcher hints(fixture_options(fixture));
    const auto targeted = hints.observeForHint(4410);
    check(targeted.has_value(), "a hint for the job process must resolve");
    check(targeted->NativeJobID == "76226340.0",
          "the targeted discovery must resolve the ad from the hinted pid");
    check(execution_complete(targeted->execution),
          "the targeted discovery must carry the manager identity");
}

void condor_without_cgroup_still_resolves_members(const Fixture& fixture)
{
    // Starter with the ad in its own environment but no usable cgroup
    // ("0::/" resolves to no execution cgroup). Members must still be
    // discovered through the starter's own children file
    // (/proc/<pid>/task/<tid>/children), flagged as partial — a per-manager
    // read, never a process-table pass. The cgroup-subtree tier cannot see this
    // starter (no slot directory lists it), so the case pins the subtree-off
    // path.
    const auto ad = fixture.ads / "76226341.0.ad";
    write_file(ad,
               "GlobalJobId = \"scheduler@schedd07.fixture#76226341.0#1775723895\"\n"
               "Owner = \"dave\"\n"
               "Cmd = \"/bin/sleep\"\n");

    make_process(fixture.proc, 4501, "condor_starter", 1, 7201,
                 {{"_CONDOR_JOB_AD", ad.string()}}, "/");
    make_process(fixture.proc, 4510, "sleep", 4501, 7210, {{}}, "/");
    make_process(fixture.proc, 4511, "sleep", 4501, 7211, {{}}, "/");
    // No cgroup: the children file is the only membership evidence available.
    make_children(fixture.proc, 4501, {4510, 4511});

    auto options = fixture_options(fixture);
    options.starter_cgroup_subtree.clear();   // this layout predates slot cgroups
    CondorJobWatcher watcher(options);
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "the pass must succeed: " + pass.set.error);
    const auto* job = find_complete(pass.jobs, "76226341.0");
    check(job != nullptr, "an execution without a cgroup must still be identified");
    const auto members = member_pids(*job);
    check(members.size() == 2,
          "the starter's children must be discovered as members without a cgroup, got " +
              std::to_string(members.size()));
    check(!job->execution.members_authoritative,
          "members resolved without a cgroup must be reported as partial evidence");
}

// ---------------------------------------------------------------------------
// Cgroup-subtree tier fallbacks: a subtree that cannot be read and a readable
// subtree that lists no starter must both fall back to the process table.
// They run last: by then the shared fixture holds five starters (the three
// fixture slots, slot4 from the ad-on-member regression and the cgroup-less
// starter of the previous case).
// ---------------------------------------------------------------------------

void condor_cgroup_subtree_missing_falls_back(const Fixture& fixture)
{
    auto options = fixture_options(fixture);
    options.starter_cgroup_subtree = "no-such-subtree";
    CondorJobWatcher watcher(options);
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(),
          "the pass must succeed despite the missing subtree: " + pass.set.error);
    check(pass.set.complete, "a missing subtree must fall back to a complete pass");
    check(contains(watcher.health().last_note, "fell back to process-table scan"),
          "the fallback must be visible in the health notes");
    check(pass.set.managers.size() == 5,
          "the fallback pass must still find every starter, got " +
              std::to_string(pass.set.managers.size()));
    check(find_complete(pass.jobs, "76226337.0") != nullptr,
          "the fixture job must still be discovered through the fallback");
}

void condor_cgroup_subtree_without_starters_falls_back(const Fixture& fixture)
{
    // A readable subtree whose slot lists no starter process must not be read
    // as "no starters on this node".
    const auto empty_subtree = fixture.cgroup / "htcondor-empty";
    fs::create_directories(empty_subtree / "slot1");
    write_file(empty_subtree / "slot1" / "cgroup.procs", "");

    auto options = fixture_options(fixture);
    options.starter_cgroup_subtree = "htcondor-empty";
    CondorJobWatcher watcher(options);
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty() && pass.set.complete,
          "a starter-less subtree must fall back to a complete pass: " + pass.set.error);
    check(contains(watcher.health().last_note, "fell back to process-table scan"),
          "the starter-less subtree must fall back visibly");
    check(pass.set.managers.size() == 5,
          "the fallback pass must still find every starter, got " +
              std::to_string(pass.set.managers.size()));
    check(find_complete(pass.jobs, "76226337.0") != nullptr,
          "the fixture job must still be discovered through the fallback");
}


// ---------------------------------------------------------------------------
// Unified discovery: enumerateManagers() / observe()
//
// The pass enumerates the managers cheaply and observes only the ones the
// registry does not track yet. observe() must reproduce a manager's Job
// without reading any other execution.
// ---------------------------------------------------------------------------

void condor_pass_owns_health(const Fixture& fixture)
{
    std::int64_t now_ms = kFakeNowMs;
    auto options = fixture_options(fixture);
    options.clock = [&now_ms] { return now_ms; };
    CondorJobWatcher watcher(options);

    const auto first = watcher.enumerateManagers();
    check(first.error.empty() && first.complete,
          "the pass must succeed: " + first.error);
    // The configured slot subtree names four managers: slot1..slot3 plus the
    // slot4 starter added by the ad-on-member regression. The cgroup-less
    // starter has no slot directory and is invisible to this tier.
    check(first.managers.size() == 4,
          "the cgroup-subtree tier must name four managers, got " +
              std::to_string(first.managers.size()));
    auto health = watcher.health();
    check(health.state == SourceState::Healthy, "a complete pass makes the source healthy");
    check(health.last_scan_start_ms == kFakeNowMs, "the pass records its start time");
    check(health.last_scan_success_ms == kFakeNowMs, "a successful pass records its end time");
    check(health.consecutive_failures == 0, "a successful pass resets the failure count");

    // The pass consumes a pending rescan request and stamps the new times.
    // Without this the heartbeat would see rescanRequested() forever and force
    // a pass on every tick.
    now_ms += 1000;
    watcher.requestRescan();
    check(watcher.health().rescan_requested,
          "a rescan request must be visible before the pass");
    const auto second = watcher.enumerateManagers();
    check(second.error.empty() && second.complete,
          "the second pass must succeed: " + second.error);
    health = watcher.health();
    check(!health.rescan_requested, "the pass consumes the pending rescan request");
    check(health.last_scan_start_ms == now_ms, "a pass records its own start time");
    check(health.last_scan_success_ms == now_ms, "a successful pass records its end time");
    check(health.state == SourceState::Healthy,
          "a successful pass keeps the source healthy");
}

void slurm_pass_owns_health(const Fixture& fixture)
{
    std::int64_t now_ms = kFakeNowMs;
    auto options = fixture_options(fixture, "");
    options.discovery.slurm_spool_dir = fixture.slurmd.string();
    options.clock = [&now_ms] { return now_ms; };
    SlurmJobWatcher watcher(options);

    const auto first = watcher.enumerateManagers();
    check(first.error.empty() && first.complete,
          "the pass must succeed: " + first.error);
    check(first.managers.size() == 2,
          "the two fixture stepds must name two managers, got " +
              std::to_string(first.managers.size()));
    auto health = watcher.health();
    check(health.state == SourceState::Healthy, "a complete pass makes the source healthy");
    check(health.last_scan_start_ms == kFakeNowMs, "the pass records its start time");
    check(health.last_scan_success_ms == kFakeNowMs, "a successful pass records its end time");

    now_ms += 1000;
    watcher.requestRescan();
    check(watcher.health().rescan_requested,
          "a rescan request must be visible before the pass");
    const auto second = watcher.enumerateManagers();
    check(second.error.empty() && second.complete,
          "the second pass must succeed: " + second.error);
    health = watcher.health();
    check(!health.rescan_requested, "the pass consumes the pending rescan request");
    check(health.last_scan_start_ms == now_ms, "a pass records its own start time");
    check(health.last_scan_success_ms == now_ms, "a successful pass records its end time");
    check(health.state == SourceState::Healthy,
          "a successful pass keeps the source healthy");
}

void condor_observe_matches_enumerated_managers(const Fixture& /*shared*/)
{
    // A closed-set comparison needs every manager to publish a Job. The shared
    // fixture deliberately keeps two of its three starters incomplete (missing
    // ad, ad without GlobalJobId), so this case uses its own tree: same layout,
    // with a readable ad for each starter.
    Fixture fixture;
    make_fixture(fixture, "/tmp/job_watcher_scan_managers");
    write_file(fixture.missing_ad,
               "GlobalJobId = \"scheduler@schedd07.fixture#76226338.0#1775723895\"\n"
               "Owner = \"bob\"\n");
    write_file(fixture.ad_without_global_job_id,
               "GlobalJobId = \"scheduler@schedd07.fixture#76226339.0#1775723895\"\n"
               "Owner = \"carol\"\n");

    CondorJobWatcher watcher(fixture_options(fixture));
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "the pass must succeed: " + pass.set.error);
    check(pass.jobs.size() == 3,
          "all three starters must publish now, got " + std::to_string(pass.jobs.size()));

    auto observed_set = observed_managers(pass.jobs);
    auto enumerated_set = pass.set.managers;
    sort_managers(observed_set);
    sort_managers(enumerated_set);
    check(enumerated_set.size() == observed_set.size(),
          "one manager per published job, got " + std::to_string(enumerated_set.size()) +
              " for " + std::to_string(observed_set.size()) + " jobs");
    for (std::size_t index = 0; index < enumerated_set.size(); ++index) {
        check(enumerated_set[index] == observed_set[index],
              "the enumerated manager set must equal the observed manager set");
        check(enumerated_set[index].complete(),
              "an enumerated manager must carry the full identity triple");
    }

    // observe() must reproduce that manager's Job: same native identity, same
    // manager, same member set.
    const auto* published_job = find_complete(pass.jobs, "76226337.0");
    check(published_job != nullptr, "the fixture Condor job must be a complete discovery");
    const auto observed = watcher.observe(published_job->execution.manager);
    check(observed.has_value(), "observing a live manager must return its Job");
    check(observed->NativeJobID == published_job->NativeJobID,
          "observe() must name the same native job, got " + observed->NativeJobID);
    check(observed->execution.manager == published_job->execution.manager,
          "observe() must keep the manager identity triple");
    check(member_pids(*observed) == member_pids(*published_job),
          "observe() must reproduce the manager's member set");

    // A manager the pass never reported is not observable: a stale start-tick
    // value and an unknown pid must both yield nullopt, never a Job.
    ProcessIdentity stale = published_job->execution.manager;
    stale.start_ticks += 1;
    check(!watcher.observe(stale).has_value(),
          "a manager with mismatching start ticks must not be observed");
    ProcessIdentity unknown;
    unknown.pid = 999999;
    unknown.start_ticks = 1;
    unknown.boot_id = published_job->execution.manager.boot_id;
    check(!watcher.observe(unknown).has_value(),
          "an unknown manager must not be observed");
}

void slurm_observe_matches_enumerated_managers(const Fixture& /*shared*/)
{
    // Same closed-set requirement as the Condor case: give the fixture's second
    // stepd an attributable member so both managers publish.
    Fixture fixture;
    make_fixture(fixture, "/tmp/job_watcher_scan_slurm_managers");
    make_process(fixture.proc, 5210, "sleep", 5201, 8110,
                 {{"SLURM_JOB_ID", "9003"},
                  {"SLURM_STEP_ID", "1"},
                  {"SLURM_CLUSTER_NAME", "slurm-fixture"}},
                 "/slurm/job_9003/step_1");
    make_cgroup(fixture.cgroup, "/slurm/job_9003/step_1", {5201, 5210});

    auto options = fixture_options(fixture, "");
    options.discovery.slurm_spool_dir = fixture.slurmd.string();
    SlurmJobWatcher watcher(options);
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "the pass must succeed: " + pass.set.error);
    check(pass.jobs.size() == 2,
          "both stepds must publish now, got " + std::to_string(pass.jobs.size()));

    auto observed_set = observed_managers(pass.jobs);
    auto enumerated_set = pass.set.managers;
    sort_managers(observed_set);
    sort_managers(enumerated_set);
    check(enumerated_set.size() == observed_set.size(),
          "one manager per published job, got " + std::to_string(enumerated_set.size()));
    for (std::size_t index = 0; index < enumerated_set.size(); ++index) {
        check(enumerated_set[index] == observed_set[index],
              "the enumerated manager set must equal the observed manager set");
        check(enumerated_set[index].complete(),
              "an enumerated manager must carry the full identity triple");
    }

    // One observe() per manager, each reproducing its own Job.
    for (const auto& manager : pass.set.managers) {
        const auto observed = watcher.observe(manager);
        check(observed.has_value(), "observing a live stepd must return its Job");
        bool matched = false;
        for (const auto& job : pass.jobs) {
            if (job.execution.manager != manager) continue;
            matched = true;
            check(observed->NativeJobID == job.NativeJobID,
                  "observe() must name the same native job");
            check(member_pids(*observed) == member_pids(job),
                  "observe() must reproduce the stepd's member set");
        }
        check(matched, "every enumerated manager must own a published job");
    }

    // A stale or unknown manager must yield nullopt, never a Job.
    ProcessIdentity stale = pass.set.managers.front();
    stale.start_ticks += 1;
    check(!watcher.observe(stale).has_value(),
          "a manager with mismatching start ticks must not be observed");
    ProcessIdentity unknown;
    unknown.pid = 999999;
    unknown.start_ticks = 1;
    unknown.boot_id = pass.set.managers.front().boot_id;
    check(!watcher.observe(unknown).has_value(),
          "an unknown manager must not be observed");
}

void condor_daemon_shared_cgroup_falls_back(const Fixture& /*shared*/)
{
    // A node without per-job cgroups puts a starter in the service cgroup
    // together with every daemon (and with the starter's own parent). That
    // cgroup must never be attributed to the job: the typed cgroup path is
    // cleared and membership comes from the starter's children.
    Fixture fixture;
    make_fixture(fixture, "/tmp/job_watcher_scan_shared_cgroup");
    const auto ad = fixture.ads / "76226342.0.ad";
    write_file(ad,
               "GlobalJobId = \"scheduler@schedd07.fixture#76226342.0#1775723895\"\n"
               "Owner = \"erin\"\n");
    make_process(fixture.proc, 4700, "condor_master", 1, 7400, {}, "/htcondor/shared");
    make_process(fixture.proc, 4701, "condor_starter", 4700, 7401,
                 {{"_CONDOR_JOB_AD", ad.string()}}, "/htcondor/shared");
    make_process(fixture.proc, 4710, "sleep", 4701, 7410, {{}}, "/htcondor/shared");
    make_cgroup(fixture.cgroup, "/htcondor/shared", {4700, 4701, 4710});
    make_children(fixture.proc, 4701, {4710});

    auto options = fixture_options(fixture);
    options.starter_cgroup_subtree.clear();  // the service cgroup is not a slot dir
    CondorJobWatcher watcher(options);
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "the pass must succeed: " + pass.set.error);
    const auto* job = find_complete(pass.jobs, "76226342.0");
    check(job != nullptr, "the starter in the daemon cgroup must still publish");
    check(std::get<CondorJobAttr>(job->sub_attr).slots_cgroup_path.empty(),
          "a daemon-shared cgroup must not be recorded as the job's cgroup");
    const auto members = member_pids(*job);
    check(members.size() == 1 && members[0] == 4710,
          "members must come from the starter's children, not the daemon tree");
    check(!job->execution.members_authoritative,
          "a children-only read is partial evidence");
}

void slurm_empty_cgroup_stays_authoritative(const Fixture& /*shared*/)
{
    // A readable step cgroup answers for the execution scope even when it lists
    // no process: an empty member set is positive evidence ("nothing runs in
    // this scope"), unlike an unreadable cgroup, which stays partial. This is
    // what lets the unified update chain clear members on an authoritative
    // empty list.
    Fixture fixture;
    make_fixture(fixture, "/tmp/job_watcher_scan_empty_cgroup");
    make_process(fixture.proc, 5301, "slurmstepd", 1, 8201, {{"SLURM_VERSION", "24.05"}},
                 "/slurm/job_9004/step_2");
    make_process(fixture.proc, 5310, "sleep", 5301, 8210,
                 {{"SLURM_JOB_ID", "9004"},
                  {"SLURM_STEP_ID", "2"},
                  {"SLURM_CLUSTER_NAME", "slurm-fixture"}},
                 "/slurm/job_9004/step_2");
    make_children(fixture.proc, 5301, {5310});
    // Readable and empty: no process in the step scope.
    make_cgroup(fixture.cgroup, "/slurm/job_9004/step_2", {});

    auto options = fixture_options(fixture, "");
    SlurmJobWatcher watcher(options);
    const auto pass = run_pass(watcher);
    check(pass.set.error.empty(), "the pass must succeed: " + pass.set.error);
    const auto* job = find_complete(pass.jobs, "9004");
    check(job != nullptr, "the stepd must still publish from its children");
    check(job->execution.members_authoritative,
          "a readable empty step cgroup must stay authoritative (an empty set is evidence)");
    const auto members = member_pids(*job);
    check(members.size() == 1 && members[0] == 5310,
          "the children list still supplies the member pid");

    // The unreadable counterpart must stay partial: the same execution with the
    // cgroup file removed cannot claim a confirmed member set.
    fs::remove(fixture.cgroup / "slurm" / "job_9004" / "step_2" / "cgroup.procs");
    const auto unreadable = run_pass(watcher);
    check(unreadable.set.error.empty(),
          "the pass must survive an unreadable cgroup: " + unreadable.set.error);
    const auto* partial = find_complete(unreadable.jobs, "9004");
    check(partial != nullptr, "the stepd must still publish from its children");
    check(!partial->execution.members_authoritative,
          "an unreadable step cgroup must not claim an authoritative member set");
}

} // namespace

int main(int argc, char** argv)
{
    const std::string base =
        argc > 1 ? std::string(argv[1]) : std::string("/tmp/job_watcher_scan_fixture");

    int failures = 0;
    try {
        Fixture fixture;
        make_fixture(fixture, base);

        struct Test {
            const char* name;
            void (*run)(const Fixture&);
        };
        const Test tests[] = {
            {"condor_pass_finds_fixture_job", condor_pass_finds_fixture_job},
            {"condor_incomplete_observations_are_not_published",
             condor_incomplete_observations_are_not_published},
            {"slurm_pass_finds_fixture_job", slurm_pass_finds_fixture_job},
            {"slurm_spool_root_is_pass_coverage", slurm_spool_root_is_pass_coverage},
            {"health_degrades_and_recovers", health_degrades_and_recovers},
            {"rescan_request_is_consumed", rescan_request_is_consumed},
            {"construction_does_nothing_until_start", construction_does_nothing_until_start},
            {"observe_hint_targets_one_execution", observe_hint_targets_one_execution},
            {"polymorphic_source_interface", polymorphic_source_interface},
            {"lifecycle_is_safe", lifecycle_is_safe},
            {"condor_adapter_refresh_evidence", condor_adapter_refresh_evidence},
            {"slurm_adapter_refresh_evidence", slurm_adapter_refresh_evidence},
            // Regressions appended last: they extend the shared fixture with
            // extra processes and would change the counts the cases above
            // assert.
            {"condor_ad_on_member_process_is_found", condor_ad_on_member_process_is_found},
            {"condor_without_cgroup_still_resolves_members",
             condor_without_cgroup_still_resolves_members},
            {"condor_cgroup_subtree_missing_falls_back",
             condor_cgroup_subtree_missing_falls_back},
            {"condor_cgroup_subtree_without_starters_falls_back",
             condor_cgroup_subtree_without_starters_falls_back},
            // Unified discovery (enumerateManagers/observe), appended last for
            // the same fixture-mutation reason.
            {"condor_pass_owns_health", condor_pass_owns_health},
            {"slurm_pass_owns_health", slurm_pass_owns_health},
            {"condor_observe_matches_enumerated_managers",
             condor_observe_matches_enumerated_managers},
            {"slurm_observe_matches_enumerated_managers",
             slurm_observe_matches_enumerated_managers},
            {"condor_daemon_shared_cgroup_falls_back",
             condor_daemon_shared_cgroup_falls_back},
            {"slurm_empty_cgroup_stays_authoritative", slurm_empty_cgroup_stays_authoritative},
            // Fact refresh without a readable cgroup, appended last: it adds
            // its own starter/child pair to the shared fixture.
            {"condor_refresh_without_cgroup_reads_children",
             condor_refresh_without_cgroup_reads_children},
        };

        for (const auto& test : tests) {
            try {
                test.run(fixture);
                std::cout << "PASS " << test.name << "\n";
            } catch (const std::exception& error) {
                std::cout << "FAIL " << test.name << ": " << error.what() << "\n";
                ++failures;
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "FAIL fixture setup: " << error.what() << "\n";
        return 1;
    }

    if (failures != 0) {
        std::cout << failures << " test(s) failed\n";
        return 1;
    }
    std::cout << "ALL PASS\n";
    return 0;
}
