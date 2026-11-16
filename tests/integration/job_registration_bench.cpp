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
/* Measurement harness for synchronous registration inside the discovery
 * callback.
 *
 * The registration chain this harness measures is exactly:
 *
 *   observeForHint(pid)              -> Job (procfs/ad/cgroup reads + typed facts)
 *   registry.upsertObservedJob(job)  -> durable facts + optional automatic decision
 *   (collector attach is owned by the scheduler's asynchronous reconcile pass,
 *    not by this registration path)
 *
 * This harness runs that chain on real host inputs and reports the per-stage
 * latency distribution, so the cost of doing it on the libbpf poll thread can
 * be judged instead of assumed.
 *
 * Inputs are real: it spawns one fake `condor_starter` parent per job, with a
 * real job child process carrying a real _CONDOR_JOB_AD file, moved into a
 * per-job cgroup v2 directory (the layout the Condor startd produces). The
 * discovered Job itself is produced by the production CondorJobWatcher against
 * /proc, therefore the read path is the shipped one; only the cluster (no
 * working collector/schedd on the test host) is simulated.
 *
 * Usage: see tests/integration/run_registration_bench.sh
 */
#include "common/config.hpp"
#include "core/job_auto_policy.hpp"
#include "core/job_registry.hpp"
#include "core/scheduler_adapter.hpp"
#include "job_watcher/condor_adapter.hpp"

#include <spdlog/spdlog.h>

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

using Clock = std::chrono::steady_clock;

double elapsed_us(const Clock::time_point& a, const Clock::time_point& b)
{
    return std::chrono::duration<double, std::micro>(b - a).count();
}


struct Args {
    std::string db;
    std::string out;
    std::string fixture_root{"/var/tmp/joblens-registration-bench"};
    std::string cluster{"htcondor-bench"};
    std::string pid_file;
    std::string collectors_csv{"new_io_usage_collector,fs_metadata_collector"};
    int jobs = 100;
    int repeat_rounds = 1;
    int burst = 0;
    int batch_probe = 0;
    int lua_rules = 0;      // >0: measure the automatic policy (Lua) per job
    int lua_iterations = 0;
    int register_rules = 0; // >0: arm the real Lua policy inside registration
    bool no_apply = false;
    bool keep_fixture = false;
};

Args parse_args(int argc, char** argv)
{
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string key = argv[i];
        auto next = [&](const char* name) -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << name << "\n";
                std::exit(2);
            }
            return argv[++i];
        };
        if (key == "--db") args.db = next("--db");
        else if (key == "--out") args.out = next("--out");
        else if (key == "--fixture-root") args.fixture_root = next("--fixture-root");
        else if (key == "--cluster") args.cluster = next("--cluster");
        else if (key == "--pid-file") args.pid_file = next("--pid-file");
        else if (key == "--collectors") args.collectors_csv = next("--collectors");
        else if (key == "--jobs") args.jobs = std::stoi(next("--jobs"));
        else if (key == "--repeat") args.repeat_rounds = std::stoi(next("--repeat"));
        else if (key == "--burst") args.burst = std::stoi(next("--burst"));
        else if (key == "--batch-probe") args.batch_probe = std::stoi(next("--batch-probe"));
        else if (key == "--no-apply") args.no_apply = true;
        else if (key == "--lua-rules") args.lua_rules = std::stoi(next("--lua-rules"));
        else if (key == "--lua-iterations")
            args.lua_iterations = std::stoi(next("--lua-iterations"));
        else if (key == "--register-rules")
            args.register_rules = std::stoi(next("--register-rules"));
        else if (key == "--keep-fixture") args.keep_fixture = true;
        else if (key == "--help" || key == "-h") {
            std::cout <<
                "job_registration_bench --db <leveldb-dir> [--out <dir>] [--jobs N]\n"
                "  [--burst N] [--repeat K] [--batch-probe N] [--no-apply]\n"
                "  [--lua-rules K] [--lua-iterations N]\n"
                "  [--register-rules K]\n"
                "  [--collectors a,b] [--cluster NAME]\n"
                "  [--fixture-root DIR] [--pid-file FILE] [--keep-fixture]\n";
            std::exit(0);
        } else {
            std::cerr << "unknown argument: " << key << "\n";
            std::exit(2);
        }
    }
    if (args.db.empty()) {
        std::cerr << "--db is required\n";
        std::exit(2);
    }
    return args;
}

std::vector<std::string> split_csv(const std::string& raw)
{
    std::vector<std::string> out;
    std::stringstream stream(raw);
    std::string item;
    while (std::getline(stream, item, ',')) {
        if (!item.empty()) out.push_back(item);
    }
    return out;
}

void write_file(const fs::path& path, const std::string& content)
{
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << content;
}

// A real ad is a few kilobytes of `Key = Value` lines; parse_ad_identity reads
// it end to end, so the size is part of the measured cost.
std::string make_ad(int index, const std::string& schedd_host)
{
    const int cluster = 880000 + index;
    std::ostringstream ad;
    ad << "GlobalJobId = \"schedd1@" << schedd_host << "#" << cluster << ".0#1775723895\"\n"
       << "ClusterId = " << cluster << "\n"
       << "ProcId = 0\n"
       << "Owner = \"wzycc\"\n"
       << "User = \"wzycc@ihep.ac.cn\"\n"
       << "Cmd = \"/bin/sleep\"\n"
       << "Args = \"3600\"\n"
       << "Iwd = \"/var/lib/condor/execute/dir_" << cluster << "\"\n"
       << "JobUniverse = 5\n"
       << "JobStatus = 2\n"
       << "JobPrio = 0\n"
       << "NiceUser = false\n"
       << "Requirements = (TARGET.Arch == \"X86_64\") && (TARGET.OpSys == \"LINUX\")\n"
       << "ShouldTransferFiles = \"YES\"\n"
       << "WhenToTransferOutput = \"ON_EXIT\"\n"
       << "ImageSize = 12000\n"
       << "DiskUsage = 12000\n"
       << "MemoryUsage = 64\n"
       << "RemoteSysCpu = 0\n"
       << "RemoteUserCpu = 0\n"
       << "NumCkpts = 0\n"
       << "NumJobStarts = 1\n"
       << "JobCurrentStartDate = 1775723895\n"
       << "JobStartDate = 1775723895\n"
       << "EnteredCurrentStatus = 1775723895\n"
       << "StreamOut = false\n"
       << "StreamErr = false\n"
       << "BufferSize = 524288\n"
       << "JobLeaseDuration = 1200\n"
       << "Environment = \"\"\n"
       << "TransferInput = \"\"\n"
       << "PublicClaimId = \"<1775723000.12@schedd1." << schedd_host << ">\"\n"
       << "ClaimId = \"<1775723000.12@schedd1." << schedd_host << ">\"\n"
       << "StartdIpAddr = \"<192.168.82.26:9618?addrs=192.168.82.26-9618>\"\n"
       << "ScheddIpAddr = \"<192.168.82.25:9618?addrs=192.168.82.25-9618>\"\n";
    return ad.str();
}

struct FixtureJob {
    pid_t starter = 0;
    pid_t child = 0;
    fs::path ad;
};

// Moves `pid` into `cgroup_dir` (cgroup v2). Best effort: without a writable
// cgroup root the watcher falls back to the starter's children.
bool move_to_cgroup(pid_t pid, const fs::path& dir)
{
    std::ofstream procs(dir / "cgroup.procs", std::ios::app);
    if (!procs) return false;
    procs << pid << "\n";
    return static_cast<bool>(procs);
}

class Fixture {
public:
    Fixture(std::string root, bool keep) : root_(std::move(root)), keep_(keep)
    {
        fs::remove_all(root_);
        fs::create_directories(root_ / "ads");
        fs::create_directories(root_ / "cgroup");
        cgroup_base_ = fs::path("/sys/fs/cgroup") / "joblens-bench";
        std::error_code ec;
        fs::create_directories(cgroup_base_, ec);
        cgroup_usable_ = !ec && fs::exists(cgroup_base_) &&
                         access((cgroup_base_ / "cgroup.procs").c_str(), W_OK) == 0;
        if (!cgroup_usable_)
            std::cout << "note: cgroup v2 root not writable; members fall back to "
                         "starter children (partial membership)\n";
    }

    ~Fixture() { cleanup(); }

    // One condor_starter parent + one job child with a real job ad.
    std::optional<FixtureJob> spawn(int index)
    {
        FixtureJob job;
        job.ad = fs::path(root_) / "ads" / (std::to_string(index) + ".ad");
        write_file(job.ad, make_ad(index, "htcondor02.ihep.ac.cn"));

        fs::path cgroup_dir;
        if (cgroup_usable_) {
            cgroup_dir = cgroup_base_ / ("job" + std::to_string(index));
            std::error_code ec;
            fs::create_directories(cgroup_dir, ec);
            if (ec) cgroup_dir.clear();
        }

        int pipefd[2];
        if (::pipe(pipefd) != 0) return std::nullopt;

        const pid_t starter = ::fork();
        if (starter < 0) {
            ::close(pipefd[0]);
            ::close(pipefd[1]);
            return std::nullopt;
        }
        if (starter == 0) {
            // Child (the future condor_starter). Own process group so the
            // harness can clean up the whole subtree, and die with the harness
            // so a killed benchmark never leaves fixture processes behind.
            ::close(pipefd[0]);
            ::prctl(PR_SET_PDEATHSIG, SIGKILL);
            ::setpgid(0, 0);
            if (!cgroup_dir.empty()) move_to_cgroup(::getpid(), cgroup_dir);
            const pid_t job_child = ::fork();
            if (job_child == 0) {
                ::prctl(PR_SET_PDEATHSIG, SIGKILL);
                ::setenv("_CONDOR_JOB_AD", job.ad.c_str(), 1);
                ::setenv("_CONDOR_SLOT", ("slot" + std::to_string(index)).c_str(), 1);
                if (!cgroup_dir.empty()) move_to_cgroup(::getpid(), cgroup_dir);
                ::execl("/bin/sleep", "job_process", "3600", static_cast<char*>(nullptr));
                ::_exit(127);
            }
            if (job_child < 0) ::_exit(126);
            if (::write(pipefd[1], &job_child, sizeof(job_child)) !=
                static_cast<ssize_t>(sizeof(job_child)))
                ::_exit(125);
            ::close(pipefd[1]);
            // argv[0] must look like condor_starter: the watcher validates it.
            ::execl("/bin/sleep", "condor_starter", "3600", static_cast<char*>(nullptr));
            ::_exit(127);
        }

        // Parent: learn the job child pid, keep the starter in its process group.
        ::close(pipefd[1]);
        pid_t job_child = 0;
        const ssize_t got = ::read(pipefd[0], &job_child, sizeof(job_child));
        ::close(pipefd[0]);
        ::setpgid(starter, starter);
        if (got != static_cast<ssize_t>(sizeof(job_child)) || job_child <= 0) {
            ::kill(starter, SIGKILL);
            return std::nullopt;
        }
        job.starter = starter;
        job.child = job_child;
        jobs_.push_back(job);
        return job;
    }

    void cleanup()
    {
        for (const auto& job : jobs_) {
            if (job.child > 0) ::kill(job.child, SIGKILL);
            if (job.starter > 0) {
                ::kill(-job.starter, SIGKILL);
                ::kill(job.starter, SIGKILL);
            }
        }
        for (const auto& job : jobs_) {
            if (job.starter > 0) ::waitpid(job.starter, nullptr, 0);
        }
        ::usleep(50 * 1000);
        jobs_.clear();
        std::error_code ec;
        // cgroupfs needs one rmdir per directory; remove_all() is not enough.
        if (cgroup_usable_) {
            for (const auto& entry : fs::directory_iterator(cgroup_base_, ec))
                ::rmdir(entry.path().c_str());
            ::rmdir(cgroup_base_.c_str());
        }
        if (!keep_) fs::remove_all(root_, ec);
    }

    bool cgroup_usable() const { return cgroup_usable_; }

private:
    fs::path root_;
    fs::path cgroup_base_;
    bool cgroup_usable_{false};
    bool keep_{false};
    std::vector<FixtureJob> jobs_;
};

struct Samples {
    std::vector<double> observe;
    std::vector<double> upsert;
    std::vector<double> apply;
    std::vector<double> total;
    std::vector<double> dup;
    std::vector<double> burst;
};

struct Stats {
    double p50 = 0, p95 = 0, p99 = 0, max = 0, mean = 0, sum = 0;
    std::size_t n = 0;
};

Stats stats_of(std::vector<double> values)
{
    Stats stats;
    if (values.empty()) return stats;
    stats.n = values.size();
    std::sort(values.begin(), values.end());
    auto at = [&](double p) {
        const double index = (values.size() - 1) * p;
        const auto lower = static_cast<std::size_t>(std::floor(index));
        const auto upper = static_cast<std::size_t>(std::ceil(index));
        if (lower == upper) return values[lower];
        return values[lower] + (values[upper] - values[lower]) * (index - lower);
    };
    stats.p50 = at(0.50);
    stats.p95 = at(0.95);
    stats.p99 = at(0.99);
    stats.max = values.back();
    double total = 0;
    for (double value : values) total += value;
    stats.sum = total;
    stats.mean = total / static_cast<double>(values.size());
    return stats;
}

void print_stats(const char* name, const Stats& stats)
{
    std::printf("%-24s n=%-6zu p50=%8.1fus p95=%8.1fus p99=%8.1fus max=%9.1fus mean=%8.1fus\n",
                name, stats.n, stats.p50, stats.p95, stats.p99, stats.max, stats.mean);
}

std::string write_bench_config(const fs::path& out_dir, const std::vector<std::string>& collectors)
{
    // Collector names must validate for the automatic intent, exactly like the
    // service's configuration does. Frequencies are irrelevant here: the
    // harness never starts the scheduler's timers.
    const fs::path config_path = out_dir / "bench-config.yaml";
    std::ostringstream config;
    config << "lens_config:\n  log_level: warn\n";
    config << "collectors_config:\n  collectors:\n";
    for (const auto& name : collectors) {
        std::string type = "CPUMemCollector";
        if (name.find("io") != std::string::npos) type = "NewIOUsageCollector";
        if (name.find("metadata") != std::string::npos) type = "FSMetadataCollector";
        config << "    - name: " << name << "\n"
               << "      type: " << type << "\n"
               << "      config: " << name << "_config\n";
    }
    for (const auto& name : collectors) {
        config << name << "_config:\n  freq: 3600\n  use_writers: []\n";
    }
    write_file(config_path, config.str());
    return config_path.string();
}

// The durable part is what makes a callback-synchronous registration slow. A
// single WriteBatch can carry many jobs, so the same sync=true guarantee costs
// one fsync per batch instead of one per record. This probe measures that
// difference on the target filesystem: it writes the same row shape with
// batch sizes 1..128.
void run_batch_probe(const fs::path& dir, int records)
{
    struct Row { size_t batch; double per_record_us; double total_ms; };
    std::vector<Row> rows;
    const std::string value(800, 'x');
    for (const size_t batch_size : {1u, 8u, 32u, 128u}) {
        const auto db_dir = dir / ("batch-" + std::to_string(batch_size));
        fs::remove_all(db_dir);
        fs::create_directories(db_dir);
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::DB* db = nullptr;
        const auto status = leveldb::DB::Open(options, db_dir.string(), &db);
        if (!status.ok() || db == nullptr) {
            std::printf("batch probe: open failed: %s\n", status.ToString().c_str());
            return;
        }
        leveldb::WriteOptions write_options;
        write_options.sync = true;   // the store's setting, unchanged
        leveldb::WriteBatch batch;
        size_t pending = 0;
        const auto start = Clock::now();
        for (int i = 0; i < records; ++i) {
            batch.Put("j:" + std::to_string(i), value);
            if (++pending < batch_size) continue;
            db->Write(write_options, &batch);
            batch.Clear();
            pending = 0;
        }
        if (pending > 0) db->Write(write_options, &batch);
        const auto end = Clock::now();
        const double total_us = elapsed_us(start, end);
        rows.push_back(Row{batch_size, total_us / records, total_us / 1000.0});
        delete db;
        fs::remove_all(db_dir);
    }
    std::printf("\n== sync WriteBatch probe (%d rows each, sync=true) ==\n", records);
    for (const auto& row : rows) {
        std::printf("batch=%-4zu per-record=%8.1fus  total=%8.1fms\n", row.batch,
                    row.per_record_us, row.total_ms);
    }
}

// Realistic deployment rules: each one reads a few nested fields of the job
// table and returns a decision, like the shipped examples in the API guide.
// With first-accept-wins the walk stops at the first accepting rule, so every
// rule declines except the last: the benchmark still pays K per-job
// evaluations per decision instead of short-circuiting after one.
void write_probe_rules(const fs::path& dir, int count)
{
    fs::remove_all(dir);
    fs::create_directories(dir);
    for (int i = 0; i < count; ++i) {
        const bool last = (i == count - 1);
        std::ostringstream lua;
        lua << "local rule = {}\n"
               "rule.name = \"probe_" << i << "\"\n"
               "rule.description = \"benchmark rule: inspect owner/cluster/pids\"\n"
               "rule.priority = " << (100 - i) << "\n"
               "function rule.condition(job)\n"
               "    local owner = \"\"\n"
               "    local cluster = 0\n"
               "    local cgroup = \"\"\n"
               "    if job.sub_attr then\n"
               "        owner = job.sub_attr.owner or \"\"\n"
               "        cluster = job.sub_attr.cluster_id or 0\n"
               "        cgroup = job.sub_attr.slots_cgroup_path or \"\"\n"
               "    end\n"
               "    local live = 0\n"
               "    for _, pid in ipairs(job.JobPIDs or {}) do\n"
               "        if pid > 0 then live = live + 1 end\n"
               "    end\n"
               "    if owner ~= \"\" and live > 0 and cluster >= " << i << " then\n";
        if (last) {
            lua << "        return true, \"cpumem_collector\"\n";
        } else {
            lua << "        return false\n";
        }
        lua << "    end\n"
               "    return false\n"
               "end\n"
               "return rule\n";
        write_file(dir / ("condor_job_probe_" + std::to_string(i) + ".lua"), lua.str());
    }
}

}  // namespace

int main(int argc, char** argv)
{
    const Args args = parse_args(argc, argv);
    spdlog::set_level(spdlog::level::warn);

    // `--out` is optional in the help text but was required in practice:
    // create_directories("") throws. Default it next to the database so a
    // first run works with the documented minimum arguments.
    const std::string out_dir =
        args.out.empty() ? (fs::path(args.db).parent_path() / "bench-out").string() : args.out;
    fs::create_directories(out_dir.empty() ? fs::path(args.db) / "bench-out" : fs::path(out_dir));
    const auto collectors = split_csv(args.collectors_csv);
    const auto config_path =
        write_bench_config(out_dir.empty() ? (fs::path(args.db) / "bench-out").string() : out_dir,
                           collectors);
    try {
        Config::instance(config_path);
    } catch (const std::exception& error) {
        std::cerr << "config load failed: " << error.what() << "\n";
        return 2;
    }

    // ---- fixtures (real processes, real ads, real cgroups) ------------------
    Fixture fixture(args.fixture_root, args.keep_fixture);
    std::vector<FixtureJob> jobs;
    if (!args.pid_file.empty()) {
        std::ifstream pids(args.pid_file);
        pid_t pid = 0;
        while (pids >> pid) jobs.push_back(FixtureJob{0, pid, {}});
    } else {
        for (int i = 0; i < args.jobs; ++i) {
            auto job = fixture.spawn(i);
            if (!job) {
                std::cerr << "fixture spawn failed at job " << i << "\n";
                return 2;
            }
            jobs.push_back(*job);
        }
    }
    // Let the stacked processes reach their final image before measuring.
    ::usleep(200 * 1000);

    // ---- registry (isolated store, production store/registry code) ---------
    JobRegistryOptions options;
    options.db_path = args.db;
    // Arming a real Lua policy needs the heartbeat wiring (admission + the
    // deferred-policy drain), so only the rule probes arm the inventory
    // stack; the plain registration measurement runs a static registry.
    options.test_static_inventory = args.register_rules == 0;
    options.test_heartbeat = args.register_rules > 0;
    if (args.register_rules > 0) {
        const auto rules_dir = fs::path(out_dir) / "register-rules";
        write_probe_rules(rules_dir, args.register_rules);
        options.test_auto_policy = [rules_dir]() {
            return AutoCollectorPolicy::rules(rules_dir.string(), "condor_job_");
        };
    }
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    JobRegistry registry(options);
    if (!registry.storeHealth().db_running) {
        std::cerr << "store is not writable: " << args.db << "\n";
        return 2;
    }

    // No collection runtime: this benchmark measures the registration and
    // desired-state commit path only. Actual attachment belongs to the
    // scheduler and is not part of this number.

    InventoryTypeSettings settings;
    settings.cluster_name = args.cluster;
    auto adapter = SchedulerAdapterRegistry::instance().build("condor", settings);
    if (!adapter) {
        std::cerr << "condor adapter is not registered\n";
        return 2;
    }

    const auto commits_before = registry.storeHealth().durable_commits;
    std::optional<Job> probe_job;   // one observed Job, reused by the policy probe
    Samples samples;
    std::vector<Job> executions;
    std::size_t incomplete = 0;
    std::size_t rejected = 0;

    for (const auto& job : jobs) {
        const auto t0 = Clock::now();
        std::optional<Job> discovered;
        try {
            // The fixture knows the starter pid: hand it over like a real
            // exec event does, so the observation skips the ppid stat read.
            discovered = adapter->source().observeForHint(job.child, job.starter);
        } catch (const std::exception& error) {
            std::cerr << "observe failed for pid " << job.child << ": " << error.what() << "\n";
            ++incomplete;
            continue;
        }
        const auto t1 = Clock::now();
        if (!discovered) {
            ++incomplete;
            continue;
        }
        Job execution;
        try {
            execution = *discovered;
        } catch (const std::exception& error) {
            std::cerr << "prepare failed: " << error.what() << "\n";
            ++incomplete;
            continue;
        }
        const auto t2 = Clock::now();
        const auto result = registry.upsertObservedJob(execution);
        const auto t3 = Clock::now();
        if (!result.ok) {
            ++rejected;
            continue;
        }
        if (!args.no_apply)
            registry.applyAutoSelection(result.internal_id, collectors, "bench");
        const auto t4 = Clock::now();

        samples.observe.push_back(elapsed_us(t0, t1));
        samples.upsert.push_back(elapsed_us(t2, t3));
        samples.apply.push_back(elapsed_us(t3, t4));
        samples.total.push_back(elapsed_us(t0, t4));
        if (!probe_job) {
            probe_job = execution;
        }
        executions.push_back(std::move(execution));
    }

    // ---- duplicate rounds: same execution seen again ----------------------
    for (int round = 0; round < args.repeat_rounds; ++round) {
        for (const auto& execution : executions) {
            const auto t0 = Clock::now();
            const auto result = registry.upsertObservedJob(execution);
            const auto t1 = Clock::now();
            if (result.ok) samples.dup.push_back(elapsed_us(t0, t1));
        }
    }

    // ---- synthetic burst: fresh identities through the same chain ---------
    // The ad/proc read is the fixture-dependent part; the burst isolates the
    // durable + intent + attach path at hundreds of simultaneous job starts.
    if (args.burst > 0 && !executions.empty()) {
        const Job& templ = executions.front();
        for (int i = 0; i < args.burst; ++i) {
            Job execution = templ;
            execution.NativeJobID = "9" + std::to_string(100000 + i) + ".0";
            const auto t0 = Clock::now();
            const auto result = registry.upsertObservedJob(execution);
            if (result.ok)
                registry.applyAutoSelection(result.internal_id, collectors, "bench");
            const auto t1 = Clock::now();
            if (result.ok) samples.burst.push_back(elapsed_us(t0, t1));
            else ++rejected;
        }
    }

    std::vector<double> lua_calls;
    std::size_t lua_rejected = 0;
    std::size_t lua_errors = 0;
    std::string lua_collectors_sample;
    if (args.lua_rules > 0 && probe_job.has_value()) {
        const auto rules_dir = fs::path(out_dir) / "probe-rules";
        write_probe_rules(rules_dir, args.lua_rules);
        auto policy = AutoCollectorPolicy::rules(rules_dir.string(), "condor_job_");
        const int iterations = args.lua_iterations > 0 ? args.lua_iterations : 200;
        for (int i = 0; i < iterations; ++i) {
            std::string revision;
            std::string error;
            const auto t0 = Clock::now();
            const auto decision = policy.select(*probe_job, revision, error);
            const auto t1 = Clock::now();
            lua_calls.push_back(elapsed_us(t0, t1));
            if (!error.empty()) ++lua_errors;
            if (!decision.accepted) ++lua_rejected;
            else if (decision.collectors.empty()) ++lua_rejected;
            else if (lua_collectors_sample.empty()) {
                for (const auto& name : decision.collectors) lua_collectors_sample += name + " ";
            }
        }
    }

    // ---- report ------------------------------------------------------------
    const auto commits = registry.storeHealth().durable_commits - commits_before;
    const auto observe = stats_of(samples.observe);
    const auto upsert = stats_of(samples.upsert);
    const auto apply = stats_of(samples.apply);
    const auto total = stats_of(samples.total);
    const auto dup = stats_of(samples.dup);
    const auto burst = stats_of(samples.burst);
    const auto lua_stats = stats_of(lua_calls);

    std::printf("\n== registration chain (real procfs/ad/cgroup, sync LevelDB) ==\n");
    std::printf("jobs=%zu incomplete=%zu rejected=%zu cgroup=%s durable_commits=%llu\n",
                jobs.size(), incomplete, rejected,
                fixture.cgroup_usable() ? "yes" : "fallback",
                static_cast<unsigned long long>(commits));
    // An averaged number over new + duplicate + changed facts hid which class a
    // commit count belongs to; keep the repeat count in the report.
    std::printf("rounds=%d\n", args.repeat_rounds);
    if (!samples.total.empty())
        std::printf("commits per registration: %.2f\n",
                    static_cast<double>(commits) / samples.total.size());
    print_stats("observeForHint", observe);
    print_stats("upsertObservedJob", upsert);
    print_stats("auto intent + attach", apply);
    print_stats("TOTAL callback", total);
    print_stats("upsert duplicate", dup);
    print_stats("burst registration", burst);
    if (lua_stats.n > 0) {
        print_stats("lua policy select", lua_stats);
        std::printf("lua rules=%d decisions: rejected=%zu errors=%zu sample=[%s]\n",
                    args.lua_rules, lua_rejected, lua_errors, lua_collectors_sample.c_str());
        std::printf("lua per batch(32) = %.2f ms\n", lua_stats.sum / lua_stats.n * 32 / 1000.0);
    }
    if (args.register_rules > 0)
        std::printf("inline policy: rules=%d (evaluated synchronously, no budget)\n",
                    args.register_rules);
    if (total.n > 0)
        std::printf("sequential registrations/sec (p50 total): %.1f\n",
                    1e6 / std::max(total.p50, 1.0));
    if (burst.n > 0)
        std::printf("burst: %zu registrations in %.1fms (%.1f/sec, mean %.1fus each)\n",
                    burst.n, burst.sum / 1000.0, 1e6 * burst.n / std::max(burst.sum, 1.0),
                    burst.mean);

    std::ofstream csv(out_dir + "/latencies.csv");
    csv << "stage,index,us\n";
    auto dump = [&csv](const char* stage, const std::vector<double>& values) {
        for (std::size_t i = 0; i < values.size(); ++i)
            csv << stage << "," << i << "," << values[i] << "\n";
    };
    dump("observe", samples.observe);
    dump("upsert", samples.upsert);
    dump("apply", samples.apply);
    dump("total", samples.total);
    dump("dup", samples.dup);
    dump("burst", samples.burst);
    dump("lua_policy", lua_calls);

    std::ofstream json(out_dir + "/results.json");
    json << "{\n";
    json << "  \"jobs\": " << jobs.size() << ",\n";
    json << "  \"durable_commits\": " << commits << ",\n";
    json << "  \"incomplete\": " << incomplete << ",\n";
    json << "  \"rejected\": " << rejected << ",\n";
    json << "  \"cgroup\": " << (fixture.cgroup_usable() ? "true" : "false") << ",\n";
    auto put = [&json](const char* name, const Stats& s, bool last) {
        json << "  \"" << name << "\": {\"n\": " << s.n << ", \"p50_us\": " << s.p50
             << ", \"p95_us\": " << s.p95 << ", \"p99_us\": " << s.p99
             << ", \"max_us\": " << s.max << ", \"mean_us\": " << s.mean
             << ", \"sum_us\": " << s.sum << "}" << (last ? "\n" : ",\n");
    };
    put("observe", observe, false);
    put("upsert", upsert, false);
    put("apply", apply, false);
    put("total", total, false);
    put("duplicate", dup, false);
    put("burst", burst, false);
    put("lua_policy", lua_stats, true);
    json << "}\n";

    std::printf("\nwrote %s/latencies.csv and %s/results.json\n", out_dir.c_str(),
                out_dir.c_str());

    if (args.batch_probe > 0) run_batch_probe(fs::path(out_dir) / "batch-probe", args.batch_probe);

    // Shut the store down before the fixture disappears from under it.
    registry.deinit();
    return 0;
}
