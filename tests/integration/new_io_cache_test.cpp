#include "collector/new_io_usage_collector.hpp"
#include <bpf/bpf.h>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>

struct NewIOUsageCollectorTestAccess {
    static void setup(NewIOUsageCollector& collector, bpf_object* object, bool details) {
        collector.bpf_obj_ = object;
        collector.include_process_details = details;
    }
    static void expire(NewIOUsageCollector& collector) { collector.last_dump_time_ = {}; }
    static bool invalidated(const NewIOUsageCollector& collector) {
        return collector.dump_keys_.empty() && collector.dump_vals_.empty() &&
               collector.dump_index_.entries(7).empty() && collector.dump_index_.entries(9).empty();
    }
    static bool has_pid_state(const NewIOUsageCollector& collector, uint64_t job) {
        return collector.known_pids_.count(job) != 0;
    }
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
    TestFile file;
    require(file.fd >= 0, "创建临时文件失败");
    const auto pid = static_cast<uint32_t>(getpid());
    const auto file_fd = static_cast<uint32_t>(file.fd);
    job_pid_fd_key a{7, pid, file_fd}, b{9, pid, file_fd};
    rw_stat av{}, bv{};
    av.read_bytes = 100; av.read_count = 2;
    bv.read_bytes = 900; bv.read_count = 9;
    require(!bpf_map_update_elem(fd, &a, &av, BPF_ANY) &&
            !bpf_map_update_elem(fd, &b, &bv, BPF_ANY), "初始化明细失败");
    uint64_t job_id = 7;
    require(!bpf_map_update_elem(totals, &job_id, &av, BPF_ANY), "初始化总量失败");
    NewIOUsageCollector collector;
    NewIOUsageCollectorTestAccess::setup(collector, object.get(), details);
    Job job; job.JobID = 7; job.JobPIDs = {getpid()};
    const auto first = std::any_cast<JobIOStat>(collector.collect(job));
    require(first.job_total.rchar == 100, "Job 总量改变");
    if (details) {
        require(first.processes.at(getpid()).io.rchar == 100, "明细串作业");
        require(first.files.size() == 1 && first.files.begin()->second.total.rchar == 100, "文件明细不正确");
    } else require(first.processes.empty() && first.files.empty(), "关闭明细时仍输出明细");

    av.read_bytes = 160;
    require(!bpf_map_update_elem(fd, &a, &av, BPF_ANY) &&
            !bpf_map_update_elem(totals, &job_id, &av, BPF_ANY), "更新计数失败");
    NewIOUsageCollectorTestAccess::expire(collector);
    const auto second = std::any_cast<JobIOStat>(collector.collect(job));
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
    const auto ephemeral = std::any_cast<JobIOStat>(collector.collect(job));
    if (details) require(ephemeral.processes.at(child).source == "ephemeral", "短命进程未输出");
    rw_stat value{};
    require(bpf_map_lookup_elem(fd, &dead_a, &value) != 0 && errno == ENOENT, "当前作业死 PID 未清理");
    require(!bpf_map_lookup_elem(fd, &dead_b, &value) && value.read_bytes == 900, "清理误删其他作业");
    require(NewIOUsageCollectorTestAccess::invalidated(collector), "清理未同步失效索引");
    job.JobID = 9;
    const auto other = std::any_cast<JobIOStat>(collector.collect(job));
    if (details) require(other.processes.at(getpid()).io.rchar == 900, "清理后刷新丢失其他作业");
    require(!bpf_map_delete_elem(fd, &a) && !bpf_map_delete_elem(fd, &b), "清理存活条目失败");
    NewIOUsageCollectorTestAccess::expire(collector);
    const auto empty = std::any_cast<JobIOStat>(collector.collect(job));
    require(empty.processes.empty() && empty.files.empty(), "空快照复用旧数据");
    job.JobID = 99;
    collector.collect(job);
    require(!NewIOUsageCollectorTestAccess::has_pid_state(collector, 99), "空作业不应保留 PID 状态");
    std::cout << "PASS real collect details=" << details
              << " isolation, file output, rates, ephemeral cleanup, invalidation, empty refresh\n";
}

int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "用法: new_io_cache_test maps.bpf.o\n"; return 2; }
    spdlog::set_level(spdlog::level::warn);
    try { run(argv[1], true); run(argv[1], false); }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
