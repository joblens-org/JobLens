#include "collector/power_collector.hpp"
#include "common/config.hpp"
#include "common/local_rpc.hpp"
#include "core/job_registry.hpp"

#include <cmath>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
constexpr int map_fd = 10001;
constexpr double initial_total = 0.25;
constexpr uint64_t interval_energy_uj = 36000000;
constexpr double interval_kwh = 36.0 / 3.6e6;
std::string fixture_root;
std::map<std::pair<u64, u32>, u64> runtimes;
unsigned map_reads = 0, consumed_entries = 0;
bool batch_fallback = false;
FILE* ipmi_file = nullptr;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
bool near(double actual, double expected) { return std::abs(actual - expected) < 1e-12; }
std::string redirect(const char* path) {
    if (!fixture_root.empty() && std::strncmp(path, "/sys/", 5) == 0)
        return fixture_root + path;
    return path;
}
void write_rapl(uint64_t value) {
    std::ofstream(fixture_root + "/sys/class/powercap/intel-rapl:0/energy_uj") << value << '\n';
}
void seed(pid_t first, pid_t second) {
    runtimes[{(static_cast<u64>(first) << 32) | static_cast<u32>(first), 0}] = 500000000;
    runtimes[{(static_cast<u64>(second) << 32) | static_cast<u32>(second), 1}] = 250000000;
}
}

// The collector still uses real file streams, libbpf ELF/map metadata, and
// JobRegistry attribution. Only the hardware/kernel/BMC data sources are fixtures.
extern "C" FILE* fopen64(const char* path, const char* mode) {
    using Open = FILE* (*)(const char*, const char*);
    static const auto real_open = reinterpret_cast<Open>(dlsym(RTLD_NEXT, "fopen64"));
    return real_open(redirect(path).c_str(), mode);
}
extern "C" DIR* __real_opendir(const char*);
extern "C" DIR* __wrap_opendir(const char* path) {
    return __real_opendir(redirect(path).c_str());
}
extern "C" int __real_stat(const char*, struct stat*);
extern "C" int __wrap_stat(const char* path, struct stat* value) {
    // Keep the real registry from starting a host BPF tracker in this fixture.
    if (std::strcmp(path, "/sys/fs/bpf") == 0) { errno = ENOENT; return -1; }
    return __real_stat(path, value);
}
extern "C" int __real_bpf_map__fd(const bpf_map*);
extern "C" int __wrap_bpf_map__fd(const bpf_map* map) {
    return std::strcmp(bpf_map__name(map), "task_cpu_time") == 0
        ? map_fd : __real_bpf_map__fd(map);
}
extern "C" int __real_bpf_map_lookup_batch(int, void*, void*, void*, void*, __u32*, const bpf_map_batch_opts*);
extern "C" int __wrap_bpf_map_lookup_batch(int fd, void* in, void* out, void* keys, void* values,
                                            __u32* count, const bpf_map_batch_opts* opts) {
    if (fd != map_fd) return __real_bpf_map_lookup_batch(fd, in, out, keys, values, count, opts);
    ++map_reads;
    if (batch_fallback) { errno = EOPNOTSUPP; return -EOPNOTSUPP; }
    const auto capacity = *count;
    *count = 0;
    for (const auto& [key, value] : runtimes) {
        require(*count < capacity, "fixture map exceeds batch capacity");
        const task_cpu_key packed{key.first, key.second};
        std::memcpy(static_cast<char*>(keys) + *count * sizeof(packed), &packed, sizeof(packed));
        std::memcpy(static_cast<char*>(values) + *count * sizeof(value), &value, sizeof(value));
        ++*count;
    }
    errno = ENOENT;
    return -ENOENT;
}
extern "C" int __real_bpf_map_delete_batch(int, const void*, __u32*, const bpf_map_batch_opts*);
extern "C" int __wrap_bpf_map_delete_batch(int fd, const void* keys, __u32* count,
                                           const bpf_map_batch_opts* opts) {
    if (fd != map_fd) return __real_bpf_map_delete_batch(fd, keys, count, opts);
    for (uint32_t i = 0; i < *count; ++i) {
        task_cpu_key key{};
        std::memcpy(&key, static_cast<const char*>(keys) + i * sizeof(key), sizeof(key));
        consumed_entries += runtimes.erase({static_cast<u64>(key.pid_tgid), static_cast<u32>(key.cpu)});
    }
    return 0;
}
extern "C" int __real_bpf_map_get_next_key(int, const void*, void*);
extern "C" int __wrap_bpf_map_get_next_key(int fd, const void* key, void* next) {
    if (fd != map_fd) return __real_bpf_map_get_next_key(fd, key, next);
    auto found = runtimes.begin();
    if (key) {
        const auto& current = *static_cast<const task_cpu_key*>(key);
        found = runtimes.upper_bound({current.pid_tgid, current.cpu});
    }
    if (found == runtimes.end()) { errno = ENOENT; return -ENOENT; }
    const task_cpu_key packed{found->first.first, found->first.second};
    std::memcpy(next, &packed, sizeof(packed));
    return 0;
}
extern "C" int __real_bpf_map_lookup_elem(int, const void*, void*);
extern "C" int __wrap_bpf_map_lookup_elem(int fd, const void* key, void* value) {
    if (fd != map_fd) return __real_bpf_map_lookup_elem(fd, key, value);
    const auto& packed = *static_cast<const task_cpu_key*>(key);
    const auto found = runtimes.find({packed.pid_tgid, packed.cpu});
    if (found == runtimes.end()) { errno = ENOENT; return -ENOENT; }
    std::memcpy(value, &found->second, sizeof(found->second));
    return 0;
}
extern "C" int __real_bpf_map_delete_elem(int, const void*);
extern "C" int __wrap_bpf_map_delete_elem(int fd, const void* key) {
    if (fd != map_fd) return __real_bpf_map_delete_elem(fd, key);
    const auto& packed = *static_cast<const task_cpu_key*>(key);
    consumed_entries += runtimes.erase({packed.pid_tgid, packed.cpu});
    return 0;
}
extern "C" FILE* __real_popen(const char*, const char*);
extern "C" FILE* __wrap_popen(const char* command, const char* mode) {
    if (std::strncmp(command, "fixture-ipmi", 12) != 0) return __real_popen(command, mode);
    ipmi_file = tmpfile();
    require(ipmi_file != nullptr, "cannot create IPMI fixture stream");
    fputs("Instantaneous power reading: 72 Watts\n", ipmi_file);
    rewind(ipmi_file);
    return ipmi_file;
}
extern "C" int __real_pclose(FILE*);
extern "C" int __wrap_pclose(FILE* file) {
    if (file != ipmi_file) return __real_pclose(file);
    ipmi_file = nullptr;
    return fclose(file);
}

struct PowerCollectorTestAccess {
    static void setup(PowerCollector& collector, bpf_object* object) {
        collector.inited_ = true;
        collector.bpf_obj_ = object;
        collector.rapl_valid_ = true;
        collector.rapl_base_ = "/sys/class/powercap/intel-rapl";
        collector.core_count_ = 2;
        collector.cache_ttl_s_ = 60.0;
        collector.last_collect_ts_ = std::chrono::steady_clock::now() - std::chrono::seconds(1);
        collector.cumulative_kwh_total_ = initial_total;
        collector.cumulative_kwh_by_job_ = {{7, 0.1}, {9, 0.2}};
    }
    static void expire(PowerCollector& collector) { collector.cache_ts_ = {}; }
    static void make_recent(PowerCollector& collector) { collector.cache_ts_ = std::chrono::steady_clock::now(); }
    static void enable_ipmi(PowerCollector& collector) {
        collector.ipmi_available_ = true;
        collector.ipmi_cmd_ = "fixture-ipmi";
    }
    static void set_cores(PowerCollector& collector, int count) { collector.core_count_ = count; }
    static double total(const PowerCollector& collector) { return collector.cumulative_kwh_total_; }
    static auto job_totals(const PowerCollector& collector) { return collector.cumulative_kwh_by_job_; }
    static bool processed(const PowerCollector& collector, uint64_t id) {
        return collector.processed_in_cycle_.count(id) != 0;
    }
    static bool has_consumed_sample(const PowerCollector& collector) {
        return collector.cached_tasks_.size() == 2 && collector.last_rapl_uj_ == interval_energy_uj &&
               collector.cached_rapl_end_uj_ - collector.cached_rapl_start_uj_ == interval_energy_uj;
    }
};

static void expect_total(const PowerCollector& collector, const PowerSnapshot& snap, double value) {
    require(near(PowerCollectorTestAccess::total(collector), value), "internal cumulative total lost or duplicated a sample");
    require(near(snap.cumulative_kwh_total, value), "returned cumulative total added the same sample twice");
    require(snap.cumulative_kwh_by_job.empty(), "public snapshot cumulative map shape changed");
}

static void run(bpf_object* object, const Job& first, const Job& second) {
    map_reads = consumed_entries = 0;
    runtimes.clear();
    write_rapl(interval_energy_uj);
    seed(first.JobPIDs[0], second.JobPIDs[0]);
    PowerCollector collector;
    PowerCollectorTestAccess::setup(collector, object);
    // The first Job refreshes the cache and accounts the consumed interval once.
    const auto consumed = std::any_cast<PowerSnapshot>(collector.collect(first));
    require(map_reads == 1 && consumed_entries == 2 && runtimes.empty(),
            "first sample did not consume the hardware interval once");
    expect_total(collector, consumed, initial_total + interval_kwh);
    const auto accounted_jobs = PowerCollectorTestAccess::job_totals(collector);
    require(near(accounted_jobs.at(first.JobID), (first.JobID == 7 ? 0.1 : 0.2) +
                 consumed.jobs[0].energy_j / 3.6e6), "first Job cumulative attribution incorrect");
    // A different Job reuses the same accounted sample without charging it again.
    const auto other = std::any_cast<PowerSnapshot>(collector.collect(second));
    expect_total(collector, other, initial_total + interval_kwh);
    require(PowerCollectorTestAccess::job_totals(collector) == accounted_jobs &&
            other.jobs.size() == 1 && other.jobs[0].job_id == second.JobID,
            "different Job charged the same sample twice");
    // After the cache expires a new interval is consumed and accounted once more.
    PowerCollectorTestAccess::expire(collector);
    write_rapl(2 * interval_energy_uj);
    seed(first.JobPIDs[0], second.JobPIDs[0]);
    const auto next = std::any_cast<PowerSnapshot>(collector.collect(first));
    require(map_reads == 2 && consumed_entries == 4 && runtimes.empty(),
            "accounted sample prevented the next cache refresh");
    expect_total(collector, next, initial_total + 2 * interval_kwh);
    std::cout << "PASS Power " << (batch_fallback ? "single-key" : "batch")
              << " sample accounted once per cycle for internal and returned totals\n";
}

static void run_empty(bpf_object* object, const Job& first, const Job& second, bool no_cores) {
    PowerCollector collector;
    PowerCollectorTestAccess::setup(collector, object);
    runtimes.clear();
    write_rapl(interval_energy_uj);
    if (no_cores) {
        seed(first.JobPIDs[0], second.JobPIDs[0]);
        PowerCollectorTestAccess::set_cores(collector, 0);
    }
    const auto previous = PowerCollectorTestAccess::job_totals(collector);
    const auto empty = std::any_cast<PowerSnapshot>(collector.collect(first));
    expect_total(collector, empty, initial_total);
    require(empty.jobs.empty() && PowerCollectorTestAccess::job_totals(collector) == previous,
            "unattributable sample reset existing Job cumulative totals");
    PowerCollectorTestAccess::set_cores(collector, 2);
    PowerCollectorTestAccess::expire(collector);
    write_rapl(2 * interval_energy_uj);
    seed(first.JobPIDs[0], second.JobPIDs[0]);
    const auto next = std::any_cast<PowerSnapshot>(collector.collect(first));
    expect_total(collector, next, initial_total + interval_kwh);
    std::cout << "PASS Power " << (no_cores ? "zero denominator" : "empty tasks")
              << " preserves prior totals and allows the next sample\n";
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::off);
    try {
        require(argc == 3, "usage: power_cache_test maps.bpf.o temporary-directory");
        fixture_root = argv[2];
        std::filesystem::create_directories(fixture_root + "/sys/class/powercap/intel-rapl:0");
        for (int cpu = 0; cpu < 2; ++cpu) {
            const auto base = fixture_root + "/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/cpufreq";
            std::filesystem::create_directories(base);
            std::ofstream(base + "/scaling_cur_freq") << "1000000\n";
        }
        std::ofstream config(fixture_root + "/config.yaml");
        config << "job_registry_config:\n  job_db_path: " << fixture_root << "/jobs.db\n";
        config.close();
        RPCServer::instance(fixture_root + "/rpc.sock");
        Config::instance(fixture_root + "/config.yaml");
        struct Child {
            pid_t pid = fork();
            Child() { require(pid >= 0, "fork failed"); if (pid == 0) { for (;;) pause(); } }
            ~Child() { if (pid > 0) { kill(pid, SIGTERM); waitpid(pid, nullptr, 0); } }
        } child;
        std::vector<Job> jobs;
        for (const auto [id, pid] : {std::pair<uint64_t, pid_t>{7, getpid()}, {9, child.pid}}) {
            Job job;
            job.JobID = id;
            job.NativeJobID = "fixture-" + std::to_string(id);
            job.JobPIDs = {pid};
            job.jobtype = JobType::Sys;
            job.subtype = JobSubType::Common;
            job.sub_attr = CommonJobAttr{.auto_update_child = false};
            job.CollectorNames = {"fixture-power"};
            require(JobRegistry::instance().addJob(job), "register fixture Job failed");
            jobs.push_back(job);
        }
        std::unique_ptr<bpf_object, decltype(&bpf_object__close)> object(
            bpf_object__open_file(argv[1], nullptr), bpf_object__close);
        require(object && !libbpf_get_error(object.get()), "open real Power map ELF failed");
        std::cout << "Power sources: file/map/BMC fixtures (no kernel BPF or RAPL hardware validation)\n";
        for (const bool fallback : {false, true}) {
            batch_fallback = fallback;
            run(object.get(), jobs[0], jobs[1]);
        }
        batch_fallback = false;
        run_empty(object.get(), jobs[0], jobs[1], false);
        run_empty(object.get(), jobs[0], jobs[1], true);
        for (const auto& job : jobs) JobRegistry::instance().delJob(job.JobID);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
