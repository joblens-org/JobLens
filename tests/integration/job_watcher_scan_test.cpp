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
/* Discovery-adapter acceptance for Task 3 (doc/job_inventory_collection_
 * decoupling_plan.md sections 3.1 and 5.2):
 *
 *   * scan() finds fixture jobs and returns complete observations with the
 *     correct native identity fields, manager generation, cgroup and members.
 *   * Missing/unreadable ads and unbacked spool entries yield *incomplete*
 *     observations with reasons: never a crash, never a guessed identity.
 *   * start() installs the sink before any thread exists, stop() is idempotent
 *     and joined, stop()-start()-stop() is safe.
 *   * SourceHealth goes initializing -> healthy on a successful scan and
 *     -> degraded on an injected failure, then recovers.
 *
 * Everything runs against a fixture tree under a temporary root: the discovery
 * roots are injectable, so this needs no root, no eBPF and no live scheduler.
 * The eBPF event source is unavailable on an unprivileged host, which the
 * adapters report as an explicit source note; the process-table scan is the
 * part under test.
 */
#include "core/job_identity.hpp"
#include "core/job_observation.hpp"
#include "job_watcher/condor_job_watcher.hpp"
#include "job_watcher/slurm_job_watcher.hpp"
#include "common/config.hpp"

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

DiscoveryOptions fixture_options(const Fixture& fixture)
{
    DiscoveryOptions options;
    options.enable_event_source = false;  // unprivileged fixture: no eBPF
    options.discovery.proc_root = fixture.proc.string();
    options.discovery.cgroup_root = fixture.cgroup.string();
    options.discovery.condor_cluster_name = "htcondor-fixture";
    options.clock = [] { return kFakeNowMs; };
    return options;
}

const Observation* find_complete(const ScanResult& result, const std::string& native_job_id)
{
    for (const auto& observation : result.observations) {
        if (observation.identity_complete && observation.selector.native_job_id == native_job_id)
            return &observation;
    }
    return nullptr;
}

std::vector<pid_t> member_pids(const Observation& observation)
{
    std::vector<pid_t> pids;
    for (const auto& member : observation.members) pids.push_back(member.pid);
    return pids;
}

bool contains(const std::string& haystack, const std::string& needle)
{
    return haystack.find(needle) != std::string::npos;
}

bool has_source_note(const ScanResult& result)
{
    for (const auto& note : result.notes) {
        if (contains(note, "eBPF")) return true;
    }
    return false;
}

// A DiscoverySink carrying only an observation callback (the fixture has no
// eBPF, so no exec hints can arrive).
DiscoverySink scan_sink(ObservationSink on_observation)
{
    DiscoverySink sink;
    sink.on_observation = std::move(on_observation);
    return sink;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void condor_scan_finds_fixture_job(const Fixture& fixture)
{
    CondorJobWatcher watcher(fixture_options(fixture));
    check(watcher.health().state == SourceState::Initializing,
          "a fresh adapter starts in SourceState::Initializing");
    check(!watcher.polling(), "constructing a discovery adapter must not start polling");

    const auto result = watcher.scan();
    check(result.ok(), "condor scan must succeed on the fixture: " + result.error);
    check(result.complete, "condor scan must report complete source coverage");
    check(result.counts.candidates == 3, "three condor_starter processes were discovered, got " +
                                             std::to_string(result.counts.candidates));
    check(result.counts.complete == 1 && result.counts.incomplete == 2,
          "one complete and two incomplete condor observations, got complete=" +
              std::to_string(result.counts.complete) + " incomplete=" +
              std::to_string(result.counts.incomplete));
    check(has_source_note(result),
          "an unavailable/disabled eBPF event source must be reported as an explicit note");

    const auto* job = find_complete(result, "76226337.0");
    check(job != nullptr, "the fixture Condor job must be a complete observation");
    check(job->scheduler_type == SchedulerType::Condor, "scheduler type must be condor");
    check(job->canonical_identity.has_value(), "complete observation carries a canonical identity");
    check(job->canonical_identity->key() ==
              "condor|htcondor-fixture@scheduler@schedd07.fixture|76226337.0",
          "canonical key must keep cluster namespace, schedd tag and native id, got " +
              job->canonical_identity->key());
    check(job->selector.cluster_tag == "scheduler@schedd07.fixture",
          "the schedd namespace must come from the job ad");
    check(job->incomplete_reason.empty(), "a complete observation has no incomplete reason");
    check(job->generation.manager_pid == 4101 && job->generation.manager_start_ticks == 7001,
          "manager pid/start ticks must identify the executing condor_starter");
    check(job->generation.boot_id == "boot-fixture-0001", "generation must carry the boot id");
    check(job->generation.complete(), "generation evidence must be complete");
    check(job->cgroup_path == (fixture.cgroup / "htcondor" / "slot1").string(),
          "the slot cgroup path must be discovered, got " + job->cgroup_path);
    check(job->members_complete, "member process identities must be complete");
    const auto members = member_pids(*job);
    check(members.size() == 2 && members[0] == 4110 && members[1] == 4111,
          "members are the slot cgroup processes without the starter");
    check(job->owner == "alice", "owner must come from the job ad");
    check(job->scheduler_host == "schedd07.fixture", "schedd host must come from the job ad");
    check(job->source_timestamp_ms == kFakeNowMs,
          "the injected clock must stamp the observation");

    const auto health = watcher.health();
    check(health.state == SourceState::Healthy, "a successful scan makes the source healthy");
    check(health.last_scan_success_ms == kFakeNowMs,
          "health must record the successful scan timestamp");
    check(health.consecutive_failures == 0, "a successful scan resets the failure count");
    check(!health.polling, "scan() alone must not start a poll thread");
}

void condor_incomplete_observations_are_explicit(const Fixture& fixture)
{
    CondorJobWatcher watcher(fixture_options(fixture));
    const auto result = watcher.scan();

    bool saw_unreadable_ad = false;
    bool saw_missing_global_job_id = false;
    for (const auto& observation : result.observations) {
        if (observation.identity_complete) continue;
        check(!observation.incomplete_reason.empty(),
              "an incomplete observation must always explain why");
        check(observation.selector.native_job_id.empty(),
              "no native job id may be synthesized for an unreadable ad");
        check(!observation.canonical_identity ||
                  !observation.canonical_identity->native_job_id.empty(),
              "an incomplete observation must never carry a guessed identity");
        check(!observation.identity_complete || observation.generation.complete(),
              "identity completeness implies a complete generation");
        if (contains(observation.incomplete_reason, "unreadable") &&
            contains(observation.incomplete_reason, "missing.ad")) {
            saw_unreadable_ad = true;
        }
        if (contains(observation.incomplete_reason, "no GlobalJobId")) {
            saw_missing_global_job_id = true;
        }
    }
    check(saw_unreadable_ad, "a missing ad file must produce an 'unreadable ad' reason");
    check(saw_missing_global_job_id, "an ad without GlobalJobId must produce its own reason");
}

void slurm_scan_finds_fixture_job(const Fixture& fixture)
{
    auto options = fixture_options(fixture);
    options.discovery.slurm_spool_dir = fixture.slurmd.string();
    SlurmJobWatcher watcher(options);

    const auto result = watcher.scan();
    check(result.ok(), "slurm scan must succeed on the fixture: " + result.error);
    check(result.complete, "slurm scan must report complete source coverage");
    check(result.counts.candidates == 4,
          "two slurmstepd processes plus two spool entries, got " +
              std::to_string(result.counts.candidates));
    check(result.counts.complete == 1 && result.counts.incomplete == 2,
          "one complete and two incomplete slurm observations, got complete=" +
              std::to_string(result.counts.complete) + " incomplete=" +
              std::to_string(result.counts.incomplete));
    check(result.counts.skipped >= 1, "a non job<id> spool entry must be counted as skipped");

    const auto* job = find_complete(result, "9002");
    check(job != nullptr, "the fixture Slurm job must be a complete observation");
    check(job->scheduler_type == SchedulerType::Slurm, "scheduler type must be slurm");
    check(job->selector.step_id == "3", "step id must be the normalized member step");
    check(job->canonical_identity &&
              job->canonical_identity->key() == "slurm|slurm-fixture|9002|3",
          "canonical key must keep cluster, job id and step");
    check(job->generation.manager_pid == 5101 && job->generation.manager_start_ticks == 8001,
          "manager pid/start ticks must identify the executing slurmstepd");
    check(job->cgroup_path == (fixture.cgroup / "slurm" / "job_9002" / "step_3").string(),
          "the step cgroup path must be discovered, got " + job->cgroup_path);
    const auto members = member_pids(*job);
    check(members.size() == 2 && members[0] == 5110 && members[1] == 5111,
          "members are the step processes without slurmstepd");
    check(job->owner == "alice" && job->job_name == "demo" && job->partition == "debug",
          "scheduler metadata must come from the members' environment");

    bool saw_unattributable_stepd = false;
    bool saw_spool_entry = false;
    for (const auto& observation : result.observations) {
        if (observation.identity_complete) continue;
        check(!observation.incomplete_reason.empty(),
              "an incomplete slurm observation must explain itself");
        if (contains(observation.incomplete_reason, "no attributable job members"))
            saw_unattributable_stepd = true;
        if (observation.selector.native_job_id == "9001") {
            check(contains(observation.incomplete_reason, "no live slurmstepd"),
                  "a spool entry without a live stepd must say so, got " +
                      observation.incomplete_reason);
            check(!observation.canonical_identity,
                  "a spool entry must not claim a canonical identity");
            saw_spool_entry = true;
        }
    }
    check(saw_unattributable_stepd,
          "a slurmstepd without attributable members must be an incomplete observation");
    check(saw_spool_entry, "a spool directory without a live stepd must be observed as incomplete");
}

void health_degrades_and_recovers(const Fixture& fixture)
{
    auto options = fixture_options(fixture);
    options.discovery.proc_root = (fixture.root / "missing-proc").string();
    CondorJobWatcher watcher(options);
    check(watcher.health().state == SourceState::Initializing, "starts initializing");

    const auto failed = watcher.scan();
    check(!failed.ok(), "scan against a missing proc root must fail");
    check(!failed.complete, "a failed scan is never complete");
    check(failed.observations.empty(), "a failed scan must not invent observations");
    check(contains(failed.error, "missing-proc"), "the scan error must name the missing root");

    auto health = watcher.health();
    check(health.state == SourceState::Degraded, "a failed scan degrades the source");
    check(contains(health.last_error, "missing-proc"), "health must keep the failure text");
    check(health.consecutive_failures == 1, "the failure count must advance");
    check(health.last_scan_success_ms == 0, "a failure must not look like a success");

    watcher.set_discovery_environment(fixture_options(fixture).discovery);
    const auto recovered = watcher.scan();
    check(recovered.ok() && recovered.complete, "the scan recovers once the root is readable");
    health = watcher.health();
    check(health.state == SourceState::Healthy, "a successful scan restores healthy state");
    check(health.consecutive_failures == 0, "recovery resets the failure count");
    check(health.last_scan_success_ms == kFakeNowMs, "recovery records the success timestamp");
    check(!health.last_error.empty(), "the last error stays readable after recovery");
}

void rescan_request_is_consumed(const Fixture& fixture)
{
    CondorJobWatcher watcher(fixture_options(fixture));
    watcher.request_rescan();
    check(watcher.health().rescan_requested, "a rescan request must be visible in health");
    watcher.scan();
    check(!watcher.health().rescan_requested, "a scan satisfies the pending rescan request");
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

    // A scan before start() works and stays usable for a scan-only owner.
    const auto early = watcher.scan();
    check(early.ok(), "scan() must work without start() (scan-only owner)");
}

void observe_hint_targets_one_execution(const Fixture& fixture)
{
    CondorJobWatcher condor(fixture_options(fixture));
    // 4110 is a member process whose parent is the starter 4101.
    const auto observation = condor.observeForHint(4110);
    check(observation.has_value(), "a hint under a starter must resolve");
    check(observation->identity_complete, "the targeted observation must be complete");
    check(observation->selector.native_job_id == "76226337.0",
          "the targeted observation must name the fixture job");
    check(observation->generation.manager_pid == 4101,
          "the manager is the hint's parent (the starter)");
    check(member_pids(*observation).size() == 2,
          "the targeted observation keeps the cgroup member list");

    // Unresolvable hints fall back to the caller (full scan).
    check(!condor.observeForHint(9999).has_value(), "a dead pid must not resolve");
    check(!condor.observeForHint(4101).has_value(),
          "a pid whose parent is not a starter must not resolve");

    auto slurm_options = fixture_options(fixture);
    slurm_options.discovery.slurm_spool_dir = (fixture.slurmd).string();
    SlurmJobWatcher slurm(slurm_options);
    // 5110's parent is the stepd 5101 of job 9002 step 3.
    const auto step = slurm.observeForHint(5110);
    check(step.has_value(), "a hint under a stepd must resolve");
    check(step->identity_complete, "the targeted slurm observation must be complete");
    check(step->selector.native_job_id == "9002" && step->selector.step_id == "3",
          "the targeted slurm observation must name job and step");
    check(step->generation.manager_pid == 5101, "the manager is the hint's parent (the stepd)");
}

void polymorphic_source_interface(const Fixture& fixture)
{
    CondorJobWatcher condor(fixture_options(fixture));
    SlurmJobWatcher slurm(fixture_options(fixture));

    IDiscoverySource* sources[] = {&condor, &slurm};
    check(sources[0]->name() == "condor", "the condor source must name itself");
    check(sources[0]->schedulerType() == SchedulerType::Condor, "condor type");
    check(sources[1]->name() == "slurm", "the slurm source must name itself");
    check(sources[1]->schedulerType() == SchedulerType::Slurm, "slurm type");

    for (IDiscoverySource* source : sources) {
        source->requestRescan();
        check(source->health().rescan_requested,
              "the interface must expose the rescan request");
        const ScanResult result = source->scan();
        check(source->health().rescan_requested == false,
              "an interface scan satisfies the rescan request");
        check(result.complete, "the interface scan must complete against the fixture");
        check(source->health().state == SourceState::Healthy,
              "the interface health must recover");
    }

    // A started scan-only source has nothing to poll; the sink stays installed.
    check(!sources[0]->start(DiscoverySink{}),
          "start() without an event source and interval must report nothing to poll");
}

void lifecycle_is_safe(const Fixture& fixture)
{
    auto options = fixture_options(fixture);
    options.scan_interval_ms = 20;
    CondorJobWatcher watcher(options);

    std::atomic<int> delivered{0};
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    check(delivered.load() == 0,
          "no callback may run before start(): the sink is a start() argument");

    check(watcher.start(scan_sink([&delivered](const Observation&) { delivered.fetch_add(1); })),
          "start() must run with a scan interval even without eBPF");
    check(watcher.polling(), "start() must report polling");
    check(watcher.thread_joinable(), "start() must own a joinable poll thread");
    check(watcher.health().polling, "health must report the running poll thread");

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (delivered.load() == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    check(delivered.load() > 0, "the poll thread must deliver scanned observations to the sink");

    watcher.stop();
    check(!watcher.polling(), "stop() must clear polling");
    check(!watcher.thread_joinable(), "stop() must join and release the thread");

    watcher.stop();  // idempotent
    check(!watcher.thread_joinable(), "a second stop() must stay a no-op");

    check(watcher.start(scan_sink([&delivered](const Observation&) { delivered.fetch_add(1); })),
          "start() after stop() must work");
    check(watcher.polling(), "restart must poll again");
    watcher.stop();
    check(!watcher.polling() && !watcher.thread_joinable(), "stop()-start()-stop() is safe");

    // The adapter stays usable for scans after its thread is gone.
    const auto after_stop = watcher.scan();
    check(after_stop.ok(), "scan() must still work after stop()");

    // Nothing to poll: no event source and no interval.
    CondorJobWatcher idle(fixture_options(fixture));
    check(!idle.start(scan_sink([](const Observation&) {})),
          "start() must refuse to spin a thread with no event source and no interval");
    check(!idle.polling(), "a refused start() must not report polling");
    idle.stop();
    check(!idle.thread_joinable(), "stop() after a refused start() is safe");

    // Same lifecycle for the Slurm adapter.
    auto slurm_options = fixture_options(fixture);
    slurm_options.scan_interval_ms = 20;
    SlurmJobWatcher slurm(slurm_options);
    std::atomic<int> slurm_delivered{0};
    check(slurm.start(scan_sink([&slurm_delivered](const Observation&) { slurm_delivered.fetch_add(1); })),
          "the Slurm adapter must start its scan thread");
    check(slurm.polling() && slurm.thread_joinable(), "the Slurm adapter owns a joined thread");
    const auto slurm_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (slurm_delivered.load() == 0 && std::chrono::steady_clock::now() < slurm_deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    check(slurm_delivered.load() > 0, "the Slurm poll thread must deliver observations");
    slurm.stop();
    slurm.stop();
    check(!slurm.polling() && !slurm.thread_joinable(), "Slurm stop() is idempotent and joined");
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
            {"condor_scan_finds_fixture_job", condor_scan_finds_fixture_job},
            {"condor_incomplete_observations_are_explicit",
             condor_incomplete_observations_are_explicit},
            {"slurm_scan_finds_fixture_job", slurm_scan_finds_fixture_job},
            {"health_degrades_and_recovers", health_degrades_and_recovers},
            {"rescan_request_is_consumed", rescan_request_is_consumed},
            {"construction_does_nothing_until_start", construction_does_nothing_until_start},
            {"observe_hint_targets_one_execution", observe_hint_targets_one_execution},
            {"polymorphic_source_interface", polymorphic_source_interface},
            {"lifecycle_is_safe", lifecycle_is_safe},
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
