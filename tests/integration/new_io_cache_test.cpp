#include "collector/new_io_usage_collector.hpp"
#include <bpf/bpf.h>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>
#include "bpf_map_fixture.hpp"

static int observed_map_fd = -1;
static unsigned batch_calls = 0;
static int batches_before_error = -1;
static std::unordered_map<pid_t, unsigned> liveness_calls;
static pid_t revived_pid = -1;

extern "C" int __real_kill(pid_t, int);
extern "C" int __wrap_kill(pid_t pid, int signal) {
    if (signal == 0) {
        ++liveness_calls[pid];
        // Deterministically model numeric PID reuse without relying on the host
        // allocator wrapping around. All other liveness checks remain real.
        if (pid == revived_pid) return 0;
    }
    return __real_kill(pid, signal);
}

extern "C" int __real_bpf_map_lookup_batch(int, void*, void*, void*, void*,
                                          __u32*, const bpf_map_batch_opts*);
extern "C" int __wrap_bpf_map_lookup_batch(int fd, void* in, void* out, void* keys,
                                          void* values, __u32* count,
                                          const bpf_map_batch_opts* opts) {
    if (fd == observed_map_fd) {
        ++batch_calls;
        if (batches_before_error == 0) {
            batches_before_error = -1;
            *count = 0;
            errno = EIO;
            return -EIO;
        }
        if (batches_before_error > 0) --batches_before_error;
    }
    const int result = BpfMapFixture::enabled ? BpfMapFixture::batch(fd, in, out, keys, values, count)
        : __real_bpf_map_lookup_batch(fd, in, out, keys, values, count, opts);
    return result;
}

struct NewIOUsageCollectorTestAccess {
    static void setup(NewIOUsageCollector& collector, bpf_object* object, bool details) {
        collector.bpf_obj_ = object;
        collector.include_process_details = details;
    }
    static void expire(NewIOUsageCollector& collector) { collector.last_dump_time_ = {}; }
    static void make_recent(NewIOUsageCollector& collector) {
        collector.last_dump_time_ = std::chrono::steady_clock::now();
    }
    static bool has_pid_state(const NewIOUsageCollector& collector, uint64_t job) {
        return collector.known_pids_.count(job) != 0;
    }
    static bool knows_pid(const NewIOUsageCollector& collector, uint64_t job, pid_t pid) {
        const auto found = collector.known_pids_.find(job);
        return found != collector.known_pids_.end() && found->second.count(pid);
    }
    static uint64_t output_count(const NewIOUsageCollector& collector, uint64_t job, pid_t pid) {
        return collector.known_pids_.at(job).at(pid).output_count;
    }
    static bool empty_detail_baselines(const NewIOUsageCollector& collector, uint64_t job) {
        return collector.last_proc_io_.at(job).empty() && collector.last_file_io_.at(job).empty() &&
               collector.last_file_proc_io_.at(job).empty();
    }
    static bool consistent_cache(const NewIOUsageCollector& collector) {
        if (collector.dump_keys_.size() != collector.dump_vals_.size()) return false;
        for (const auto i : collector.dump_index_.entries(7))
            if (i >= collector.dump_keys_.size() || collector.dump_keys_[i].job_id != 7) return false;
        return true;
    }
    static auto sample_time(const NewIOUsageCollector& collector) { return collector.last_job_time_.at(7); }
};

static void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

struct TestFile {
    char path[40] = "/tmp/jl-newio-file.XXXXXX";
    int fd = mkstemp(path);
    ~TestFile() { if (fd >= 0) { close(fd); unlink(path); } }
};

static void run(const char* path, bool details) {
    std::unique_ptr<bpf_object, decltype(&bpf_object__close)> object(
        bpf_object__open_file(path, nullptr), bpf_object__close);
    require(object && !libbpf_get_error(object.get()) && !bpf_object__load(object.get()), "加载独立 map 失败");
    const int fd = bpf_object__find_map_fd_by_name(object.get(), "job_fd_stat");
    const int totals = bpf_object__find_map_fd_by_name(object.get(), "job_stat");
    require(fd >= 0 && totals >= 0, "缺少测试 map");
    observed_map_fd = fd;
    TestFile file;
    require(file.fd >= 0, "创建临时文件失败");
    const auto pid = static_cast<uint32_t>(getpid());
    const auto file_fd = static_cast<uint32_t>(file.fd);
    job_pid_fd_key a{7, pid, file_fd}, b{9, pid, file_fd};
    const int duplicate_fd = dup(file.fd);
    require(duplicate_fd >= 0, "duplicate file descriptor failed");
    job_pid_fd_key same_pid{7, pid, static_cast<uint32_t>(duplicate_fd)};
    rw_stat av{}, bv{};
    av.read_bytes = 100; av.read_count = 2;
    bv.read_bytes = 900; bv.read_count = 9;
    rw_stat extra{}; extra.read_bytes = 50; extra.read_count = 1;
    require(!bpf_map_update_elem(fd, &a, &av, BPF_ANY) &&
            !bpf_map_update_elem(fd, &b, &bv, BPF_ANY) &&
            !bpf_map_update_elem(fd, &same_pid, &extra, BPF_ANY), "初始化明细失败");
    uint64_t job_id = 7;
    require(!bpf_map_update_elem(totals, &job_id, &av, BPF_ANY), "初始化总量失败");
    NewIOUsageCollector collector;
    NewIOUsageCollectorTestAccess::setup(collector, object.get(), details);
    Job job; job.JobID = 7; job.JobPIDs = {getpid()};
    liveness_calls.clear();
    const auto first = std::any_cast<JobIOStat>(collector.collect(job));
    require(first.job_total.rchar == 100, "Job 总量改变");
    require(liveness_calls[pid] == 1, "liveness queried once per FD instead of once per PID");
    require(NewIOUsageCollectorTestAccess::output_count(collector, 7, pid) == 1,
            "output count must increment once per collect, not once per FD");
    if (details) {
        require(first.processes.at(getpid()).io.rchar == 150, "明细串作业");
        require(first.files.size() == 1 && first.files.begin()->second.total.rchar == 150, "文件明细不正确");
    } else {
        require(first.processes.empty() && first.files.empty(), "关闭明细时仍输出明细");
        require(NewIOUsageCollectorTestAccess::empty_detail_baselines(collector, 7),
                "disabled details retained detail baselines");
    }

    av.read_bytes = 160;
    require(!bpf_map_update_elem(fd, &a, &av, BPF_ANY) &&
            !bpf_map_update_elem(totals, &job_id, &av, BPF_ANY), "更新计数失败");
    NewIOUsageCollectorTestAccess::expire(collector);
    const auto second = std::any_cast<JobIOStat>(collector.collect(job));
    require(NewIOUsageCollectorTestAccess::output_count(collector, 7, pid) == 2,
            "known PID output count did not advance on the next sample");
    require(second.collect_period > 0 && second.job_total.rchar_speed > 0, "差分基线失效");
    require(second.job_total.rchar_speed * second.collect_period > 59.99 &&
            second.job_total.rchar_speed * second.collect_period < 60.01, "差分计数不正确");

    const pid_t child = fork();
    require(child >= 0, "fork 失败");
    if (child == 0) _exit(0);
    require(waitpid(child, nullptr, 0) == child, "回收测试进程失败");
    job_pid_fd_key dead_a{7, static_cast<uint32_t>(child), 3};
    job_pid_fd_key dead_b{9, static_cast<uint32_t>(child), 3};
    require(!bpf_map_update_elem(fd, &dead_a, &av, BPF_ANY) &&
            !bpf_map_update_elem(fd, &dead_b, &bv, BPF_ANY), "初始化短命进程失败");
    NewIOUsageCollectorTestAccess::expire(collector);
    rw_stat value{};
    const auto ephemeral = std::any_cast<JobIOStat>(collector.collect(job));
    if (details) require(ephemeral.processes.at(child).source == "ephemeral", "短命进程未输出");
    require(!bpf_map_lookup_elem(fd, &dead_a, &value),
            "first completed dead-PID sample must remain until the next collection cleans it");
    const auto cleaned = std::any_cast<JobIOStat>(collector.collect(job));
    require(!cleaned.processes.count(child), "already-output dead PID was returned again after cleanup");
    require(bpf_map_lookup_elem(fd, &dead_a, &value) != 0 && errno == ENOENT, "当前作业死 PID 未清理");
    require(!bpf_map_lookup_elem(fd, &dead_b, &value) && value.read_bytes == 900, "清理误删其他作业");
    require(NewIOUsageCollectorTestAccess::consistent_cache(collector), "清理未同步刷新索引");
    job.JobID = 9;
    const auto other = std::any_cast<JobIOStat>(collector.collect(job));
    if (details) require(other.processes.at(getpid()).io.rchar == 900, "清理后刷新丢失其他作业");
    require(!bpf_map_delete_elem(fd, &a) && !bpf_map_delete_elem(fd, &b) &&
            !bpf_map_delete_elem(fd, &same_pid), "清理存活条目失败");
    close(duplicate_fd);
    NewIOUsageCollectorTestAccess::expire(collector);
    const auto empty = std::any_cast<JobIOStat>(collector.collect(job));
    require(empty.processes.empty() && empty.files.empty(), "空快照复用旧数据");
    const auto after_empty = batch_calls;
    NewIOUsageCollectorTestAccess::make_recent(collector);
    job.JobID = 99;
    collector.collect(job);
    require(batch_calls == after_empty, "successful empty snapshot was not cached within the TTL");
    require(!NewIOUsageCollectorTestAccess::has_pid_state(collector, 99), "空作业不应保留 PID 状态");
    std::cout << "PASS real collect details=" << details
              << " isolation, file output, rates, ephemeral cleanup, invalidation, empty refresh\n";
}

static void run_cleanup_recovery(const char* path, bool reuse, bool exited_again = false) {
    std::unique_ptr<bpf_object, decltype(&bpf_object__close)> object(
        bpf_object__open_file(path, nullptr), bpf_object__close);
    require(object && !libbpf_get_error(object.get()) && !bpf_object__load(object.get()), "load recovery maps failed");
    observed_map_fd = bpf_object__find_map_fd_by_name(object.get(), "job_fd_stat");
    const pid_t child = fork();
    require(child >= 0, "fork recovery PID failed");
    if (!child) _exit(0);
    require(waitpid(child, nullptr, 0) == child, "reap recovery PID failed");
    const uint32_t entries = reuse ? 1 : 2051;
    for (uint32_t i = 0; i < entries; ++i) {
        const job_pid_fd_key key{7, static_cast<uint32_t>(child), 100000 + i};
        rw_stat value{}; value.read_bytes = 1; value.read_count = 1;
        require(!bpf_map_update_elem(observed_map_fd, &key, &value, BPF_ANY), "seed recovery counters failed");
    }
    NewIOUsageCollector collector;
    NewIOUsageCollectorTestAccess::setup(collector, object.get(), true);
    Job job; job.JobID = 7;
    if (!reuse) batches_before_error = 1;
    const auto first = std::any_cast<JobIOStat>(collector.collect(job));
    require(first.processes.at(child).source == "ephemeral", "dead recovery PID missing");
    if (!reuse) require(first.processes.at(child).io.rchar < entries, "partial dump fixture did not truncate");
    if (reuse && !exited_again) revived_pid = child;
    if (exited_again) {
        // A replacement may exit before the next collection; kill(pid, 0)
        // cannot distinguish it from the previously sampled dead incarnation.
        const job_pid_fd_key updated{7, static_cast<uint32_t>(child), 100000};
        const job_pid_fd_key added{7, static_cast<uint32_t>(child), 100001};
        rw_stat value{}; value.read_bytes = 4; value.read_count = 1;
        require(!bpf_map_update_elem(observed_map_fd, &updated, &value, BPF_ANY) &&
                !bpf_map_update_elem(observed_map_fd, &added, &value, BPF_ANY), "seed exited replacement failed");
        // Keep the existing dump within TTL: cleanup must refresh it before
        // deciding whether these records have already been returned.
        NewIOUsageCollectorTestAccess::make_recent(collector);
    } else {
        NewIOUsageCollectorTestAccess::expire(collector);
    }
    liveness_calls.clear();
    const auto recovered = std::any_cast<JobIOStat>(collector.collect(job));
    revived_pid = -1;
    require(recovered.processes.count(child) && recovered.processes.at(child).io.rchar == (exited_again ? 8 : entries),
            exited_again ? "cleanup deleted changed/new FD records from an exited replacement PID"
            : reuse ? "cleanup deleted a live reused PID before sampling it"
                  : "cleanup deleted dead-PID records omitted from the previous partial dump");
    require(liveness_calls[child] == 1, "cleanup repeated the current PID liveness query");
    if (reuse && !exited_again) require(recovered.processes.at(child).alive, "reused PID was still reported as dead");
    if (exited_again) {
        const auto cleaned = std::any_cast<JobIOStat>(collector.collect(job));
        require(!cleaned.processes.count(child), "returned replacement records were never cleaned");
    }
    std::cout << "PASS dead-PID cleanup recovery "
              << (exited_again ? "exited PID reuse within TTL" : reuse ? "PID reuse" : "partial dump") << '\n';
}

static void run_failure_retry(const char* path) {
    std::unique_ptr<bpf_object, decltype(&bpf_object__close)> object(
        bpf_object__open_file(path, nullptr), bpf_object__close);
    require(object && !libbpf_get_error(object.get()) && !bpf_object__load(object.get()), "load retry maps failed");
    observed_map_fd = bpf_object__find_map_fd_by_name(object.get(), "job_fd_stat");
    require(observed_map_fd >= 0, "retry map missing");
    const auto pid = static_cast<uint32_t>(getpid());
    Job job; job.JobID = 7; job.JobPIDs = {getpid()};
    NewIOUsageCollector collector;
    NewIOUsageCollectorTestAccess::setup(collector, object.get(), true);

    batches_before_error = 0;
    const auto failed = std::any_cast<JobIOStat>(collector.collect(job));
    require(failed.processes.empty(), "failed empty dump invented data");
    const auto after_failure = batch_calls;
    NewIOUsageCollectorTestAccess::make_recent(collector);
    collector.collect(job);
    require(batch_calls > after_failure, "failed empty dump was cached as a success");

    for (uint32_t i = 0; i < 2051; ++i) {
        job_pid_fd_key key{7, pid, 100000 + i};
        rw_stat value{}; value.read_bytes = 1; value.read_count = 1;
        require(!bpf_map_update_elem(observed_map_fd, &key, &value, BPF_ANY), "seed partial traversal failed");
    }
    NewIOUsageCollectorTestAccess::expire(collector);
    batches_before_error = 1;
    const auto partial = std::any_cast<JobIOStat>(collector.collect(job));
    require(partial.processes.at(pid).io.rchar > 0 && partial.processes.at(pid).io.rchar < 2051,
            "partial failure fixture did not preserve a successful first batch");
    const auto after_partial = batch_calls;
    NewIOUsageCollectorTestAccess::make_recent(collector);
    const auto recovered = std::any_cast<JobIOStat>(collector.collect(job));
    require(batch_calls > after_partial && recovered.processes.at(pid).io.rchar == 2051,
            "partial failed traversal was cached instead of retried");
    std::cout << "PASS empty and partial BPF dump failures retry inside the TTL\n";
}

int main(int argc, char** argv) {
    if (argc == 3 && std::strcmp(argv[2], "--map-fixture") == 0) BpfMapFixture::enabled = true;
    else if (argc != 2) { std::cerr << "Usage: new_io_cache_test maps.bpf.o [--map-fixture]\n"; return 2; }
    spdlog::set_level(spdlog::level::warn);
    if (BpfMapFixture::enabled) std::cout << "BPF map backend: in-memory fixture (no kernel BPF validation)\n";
    try {
        run(argv[1], true); run(argv[1], false);
        run_cleanup_recovery(argv[1], true); run_cleanup_recovery(argv[1], false);
        run_cleanup_recovery(argv[1], true, true);
        run_failure_retry(argv[1]);
    }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
