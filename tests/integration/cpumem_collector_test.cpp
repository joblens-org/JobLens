#include "collector/cpumem_collector.hpp"
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <map>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

static std::string denied_stat;
static int denied_stat_errno = EACCES;
static std::map<std::string, std::string> fake_files;   // 目标 /proc 路径 -> 伪造文件路径
static std::map<std::string, int> fake_file_fds;
static std::map<std::string, int> fake_opens;           // 目标 /proc 路径被读取的次数
static unsigned clock_queries = 0, core_queries = 0;

static std::string stat_path(pid_t pid) {
    return "/proc/" + std::to_string(pid) + "/stat";
}

static std::string status_path(pid_t pid) {
    return "/proc/" + std::to_string(pid) + "/status";
}

static std::string rollup_path(pid_t pid) {
    return "/proc/" + std::to_string(pid) + "/smaps_rollup";
}

static std::string smaps_path(pid_t pid) {
    return "/proc/" + std::to_string(pid) + "/smaps";
}

// 为某个 /proc 路径登记伪造内容；同一目标后续调用原地重写文件。
static void write_fake_stat(const std::string& target, const std::string& content) {
    auto it = fake_file_fds.find(target);
    int fd;
    if (it == fake_file_fds.end()) {
        char tmpl[] = "/tmp/jl_fake_stat_XXXXXX";
        fd = mkstemp(tmpl);
        if (fd < 0) throw std::runtime_error("mkstemp failed");
        fake_file_fds[target] = fd;
        fake_files[target] = tmpl;
    } else {
        fd = it->second;
    }
    if (ftruncate(fd, 0) != 0 || lseek(fd, 0, SEEK_SET) != 0 ||
        write(fd, content.data(), content.size()) != static_cast<ssize_t>(content.size()))
        throw std::runtime_error("cannot rewrite fake stat");
}

static std::string make_stat_line(pid_t pid, const std::string& comm,
                                  unsigned long long utime, unsigned long long stime,
                                  unsigned long long starttime) {
    std::ostringstream os;
    os << pid << " (" << comm << ") S 1 2 3 4 5 0 0 0 0 0 "
       << utime << " " << stime << " 0 0 20 0 1 0 " << starttime
       << " 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0";
    return os.str() + "\n";
}

// 带子进程累计量的 /proc/<pid>/stat：cutime/cstime 是已被回收子进程的 CPU 时间。
static std::string make_stat_line_with_children(pid_t pid, const std::string& comm,
                                                unsigned long long utime, unsigned long long stime,
                                                unsigned long long cutime, unsigned long long cstime,
                                                unsigned long long starttime) {
    std::ostringstream os;
    os << pid << " (" << comm << ") S 1 2 3 4 5 0 0 0 0 0 "
       << utime << " " << stime << " " << cutime << " " << cstime
       << " 20 0 1 0 " << starttime
       << " 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0";
    return os.str() + "\n";
}

static std::string make_status_line(unsigned long long rss_kb, unsigned long long vm_kb,
                                    unsigned long long hwm_kb, unsigned long long swap_kb,
                                    int threads) {
    std::ostringstream os;
    os << "Name:\tworker\n"
       << "VmPeak:\t" << vm_kb << " kB\n"
       << "VmSize:\t" << vm_kb << " kB\n"
       << "VmHWM:\t" << hwm_kb << " kB\n"
       << "VmRSS:\t" << rss_kb << " kB\n"
       << "RssAnon:\t" << rss_kb << " kB\n"
       << "VmSwap:\t" << swap_kb << " kB\n"
       << "Threads:\t" << threads << "\n";
    return os.str();
}

// smaps_rollup 只有一组聚合行，Pss_* 子项不能被计入顶层 Pss。
static std::string make_rollup_line(unsigned long long pss_kb) {
    std::ostringstream os;
    os << "00400000-7ffc0000 r-xp 00000000 08:01 1 /usr/bin/worker\n"
       << "Rss:\t\t" << pss_kb * 2 << " kB\n"
       << "Pss:\t\t" << pss_kb << " kB\n"
       << "Pss_Anon:\t" << pss_kb / 2 << " kB\n"
       << "Pss_File:\t" << pss_kb / 2 << " kB\n"
       << "Pss_Shmem:\t0 kB\n"
       << "Swap:\t\t0 kB\n";
    return os.str();
}

// 逐 VMA 的 smaps：每个映射各有一条 Pss 行，降级路径需要把它们累加。
static std::string make_smaps_body(unsigned long long pss_a, unsigned long long pss_b) {
    std::ostringstream os;
    os << "00400000-00401000 r-xp 00000000 08:01 1 /usr/bin/worker\n"
       << "Size:\t4 kB\nRss:\t4 kB\nPss:\t" << pss_a << " kB\nPss_Anon:\t0 kB\n\n"
       << "7f000000-7f001000 rw-p 00000000 00:00 0 [heap]\n"
       << "Size:\t4 kB\nRss:\t4 kB\nPss:\t" << pss_b << " kB\nPss_Anon:\t" << pss_b << " kB\n\n";
    return os.str();
}

// 伪造 /proc/stat 第一行：read_system_cpu 求和该行所有数值字段。
static std::string make_proc_stat_line(unsigned long long total) {
    std::ostringstream os;
    os << "cpu  " << total << " 0 0 0 0 0 0 0 0 0\n";
    return os.str();
}

// 拦截 fopen64：命中伪造表的 /proc 路径读伪造文件，denied_stat 命中则模拟 EACCES，
// 其余一律走真实 procfs。
extern "C" FILE* fopen64(const char* path, const char* mode) {
    using Open = FILE* (*)(const char*, const char*);
    static const auto real_open = reinterpret_cast<Open>(dlsym(RTLD_NEXT, "fopen64"));
    if (!denied_stat.empty() && denied_stat == path) {
        errno = denied_stat_errno;
        return nullptr;
    }
    const auto it = fake_files.find(path ? std::string(path) : std::string());
    if (it != fake_files.end()) {
        ++fake_opens[path];
        return real_open(it->second.c_str(), mode);
    }
    return real_open(path, mode);
}

extern "C" long __real_sysconf(int);
extern "C" long __wrap_sysconf(int name) {
    if (name == _SC_CLK_TCK) ++clock_queries;
    if (name == _SC_NPROCESSORS_ONLN) ++core_queries;
    return __real_sysconf(name);
}

// 访问私有基线表，用于验证陈旧 PID 基线的清理行为。
struct CPUMemCollectorTestAccess {
    static void insert_stale(CPUMemCollector& collector, int pid,
                             const std::chrono::steady_clock::time_point& seen) {
        collector.pid_state_dict[pid].last_seen = seen;
    }
    static bool has(const CPUMemCollector& collector, int pid) {
        return collector.pid_state_dict.count(pid) != 0;
    }
    static size_t size(const CPUMemCollector& collector) {
        return collector.pid_state_dict.size();
    }
    static void reset_prune_clock(CPUMemCollector& collector) { collector.last_prune_ = {}; }
    static void prune_now(CPUMemCollector& collector) {
        collector.prune_stale_pid_states(std::chrono::steady_clock::now());
    }
    static size_t job_peaks(const CPUMemCollector& collector) {
        return collector.job_peak_map.size();
    }
    static bool uses_rollup(const CPUMemCollector& collector) {
        return collector.pss_mode == CPUMemCollector::PssMode::Rollup;
    }
    static void force_smaps_mode(CPUMemCollector& collector) {
        collector.pss_mode = CPUMemCollector::PssMode::Smaps;
    }
};

static const CPUMemInfo& summary_of(const std::vector<CPUMemInfo>& infos) {
    for (const auto& info : infos) {
        if (info.pid == 0) return info;
    }
    throw std::runtime_error("summary record missing");
}

static const CPUMemInfo& record_of(const std::vector<CPUMemInfo>& infos, pid_t pid) {
    for (const auto& info : infos) {
        if (info.pid == pid) return info;
    }
    throw std::runtime_error("process record missing");
}

static void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

static void mark(const std::string& value) {
    const auto line = value + '\n';
    require(write(STDERR_FILENO, line.data(), line.size()) == static_cast<ssize_t>(line.size()),
            "cannot write phase marker");
}

struct Child {
    pid_t pid = fork();
    Child() {
        require(pid >= 0, "fork failed");
        if (pid == 0) { for (;;) pause(); }
    }
    ~Child() { if (pid > 0) { kill(pid, SIGTERM); waitpid(pid, nullptr, 0); } }
};

int main() {
    spdlog::set_level(spdlog::level::warn);
    try {
        const double cores = static_cast<double>(__real_sysconf(_SC_NPROCESSORS_ONLN));
        const bool pss_supported = access("/proc/self/smaps_rollup", F_OK) == 0;
        Child child;
        CPUMemCollector collector;
        require(collector.init({}), "collector init failed");
        Job job;
        job.JobID = 7;
        job.JobPIDs = {getpid(), child.pid};
        std::string expected_name;
        for (const auto* phase : {"first", "second"}) {
            const auto before_cores = core_queries;
            mark(std::string("CPUMEM_BEGIN ") + phase);
            const auto infos = std::any_cast<std::vector<CPUMemInfo>>(collector.collect(job));
            mark(std::string("CPUMEM_END ") + phase);
            require(infos.size() == 2, "live processes missing");
            for (const auto& info : infos) {
                require(!info.name.empty() && info.hz > 0 && info.mem_rss_kb > 0 &&
                        std::isfinite(info.cpuPercent), "real proc sample invalid");
                require(info.cpuPercent >= 0.0 && info.cpuPercent <= 100.0 * cores,
                        "cpuPercent outside physical machine range");
                if (pss_supported) {
                    require(info.mem_pss_kb >= 0 && info.mem_pss_kb <= info.mem_rss_kb,
                            "real PSS sample outside [0, RSS]");
                } else {
                    require(info.mem_pss_kb == -1, "PSS unavailable but a value was reported");
                }
                if (info.pid == child.pid) expected_name = info.name;
            }
            require(core_queries - before_cores == 1, "online core count queried more than once per Job");
        }
        require(clock_queries == 1, "CLK_TCK was not cached across samples");
        denied_stat = "/proc/" + std::to_string(child.pid) + "/stat";
        mark("CPUMEM_BEGIN fallback");
        const auto fallback = std::any_cast<std::vector<CPUMemInfo>>(collector.collect(job));
        mark("CPUMEM_END fallback");
        require(fallback.size() == 2, "stat failure discarded a live process");
        for (const auto& info : fallback) {
            if (info.pid == child.pid) {
                require(info.name == expected_name && info.utime == 0 && info.starttime == 0 &&
                        info.mem_rss_kb > 0, "comm fallback or status collection changed after stat failure");
            }
        }
        denied_stat.clear();
        // Invalid UTF-8 is accepted as a JSON string but dump() rejects it.
        // With trace disabled, the ES parser must leave serialization to the writer.
        CPUMemInfo raw_name;
        raw_name.pid = getpid();
        raw_name.name = std::string(1, static_cast<char>(0xff));
        const auto json = std::any_cast<nlohmann::json>(
            collector.get_writer_parser("ESWriter")(std::vector<CPUMemInfo>{raw_name}));
        require(json.at("process_data").at(0).at("name").get<std::string>() == raw_name.name,
                "disabled trace changed the raw process name");
        collector.deinit();

        Child reused_child;
        CPUMemCollector reuse_collector;
        require(reuse_collector.init({}), "reuse collector init failed");
        Job reuse_job;
        reuse_job.JobID = 8;
        reuse_job.JobPIDs = {reused_child.pid};

        write_fake_stat(stat_path(reused_child.pid), make_stat_line(reused_child.pid, "ps", 3, 15, 111));
        mark("CPUMEM_BEGIN reuse_old");
        const auto old_infos = std::any_cast<std::vector<CPUMemInfo>>(reuse_collector.collect(reuse_job));
        mark("CPUMEM_END reuse_old");
        require(old_infos.size() == 1 && std::isfinite(old_infos.front().cpuPercent) &&
                old_infos.front().cpuPercent < 10000.0, "old process sample invalid");

        usleep(50000);
        write_fake_stat(stat_path(reused_child.pid), make_stat_line(reused_child.pid, "appinit", 1, 1, 222));
        mark("CPUMEM_BEGIN reuse_new");
        const auto new_infos = std::any_cast<std::vector<CPUMemInfo>>(reuse_collector.collect(reuse_job));
        mark("CPUMEM_END reuse_new");
        require(new_infos.size() == 1, "reused process sample missing");
        require(new_infos.front().cpuPercent == 0.0, "PID reuse underflow was not neutralized");

        usleep(50000);
        write_fake_stat(stat_path(reused_child.pid), make_stat_line(reused_child.pid, "appinit", 0, 0, 222));
        mark("CPUMEM_BEGIN reuse_regress");
        const auto regressed = std::any_cast<std::vector<CPUMemInfo>>(reuse_collector.collect(reuse_job));
        mark("CPUMEM_END reuse_regress");
        require(regressed.size() == 1 && regressed.front().cpuPercent == 0.0,
                "CPU counter regression underflow was not neutralized");

        usleep(50000);
        write_fake_stat(stat_path(reused_child.pid), make_stat_line(reused_child.pid, "appinit", 10, 5, 222));
        mark("CPUMEM_BEGIN reuse_grow");
        const auto grown = std::any_cast<std::vector<CPUMemInfo>>(reuse_collector.collect(reuse_job));
        mark("CPUMEM_END reuse_grow");
        require(grown.size() == 1 && grown.front().cpuPercent > 0.0 &&
                std::isfinite(grown.front().cpuPercent) && grown.front().cpuPercent < 100000.0,
                "normal CPU growth regressed after baseline reset");
        reuse_collector.deinit();
        fake_files.clear();
        fake_file_fds.clear();

        // 多作业共享同一个采集器实例：调度器在一个 tick 内对 A、B 依次 collect()，
        // 而 /proc/stat 在两次调用之间只会前进 ε（真实系统里就是前一个作业的采集耗时）。
        // 回归点：若系统总 jiffies 基线是实例级共享的，B 的 Δproc（覆盖整个 tick）会被
        // 除以近零的 Δtotal，cpuPercent 被放大数千倍。
        {
            Child workerA, workerB;
            write_fake_stat(stat_path(workerA.pid), make_stat_line(workerA.pid, "workerA", 100, 0, 601));
            write_fake_stat(stat_path(workerB.pid), make_stat_line(workerB.pid, "workerB", 100, 0, 602));
            CPUMemCollector multi;
            require(multi.init({}), "multi-job collector init failed");
            Job jobA;
            jobA.JobID = 11;
            jobA.JobPIDs = {workerA.pid};
            Job jobB;
            jobB.JobID = 12;
            jobB.JobPIDs = {workerB.pid};

            const unsigned long long t0 = 1000000000ULL;
            const unsigned long long tick = 500ULL * static_cast<unsigned long long>(cores);  // 5s @ HZ=100

            // tick1：为两个作业各自建立基线，首采样输出 0
            write_fake_stat("/proc/stat", make_proc_stat_line(t0));
            mark("CPUMEM_BEGIN multi_tick1");
            (void)multi.collect(jobA);
            (void)multi.collect(jobB);
            mark("CPUMEM_END multi_tick1");

            // tick2：jobA 采样前系统前进整个 tick，Δproc=50 jiffies → 期望 10%
            write_fake_stat(stat_path(workerA.pid), make_stat_line(workerA.pid, "workerA", 150, 0, 601));
            write_fake_stat("/proc/stat", make_proc_stat_line(t0 + tick));
            mark("CPUMEM_BEGIN multi_tick2_a");
            const auto a2 = std::any_cast<std::vector<CPUMemInfo>>(multi.collect(jobA));
            mark("CPUMEM_END multi_tick2_a");
            require(a2.size() == 1 && std::abs(a2.front().cpuPercent - 10.0) < 0.5,
                    "jobA cpuPercent does not match its sampling interval");

            // jobB 采样前系统只前进 1 jiffy：正确实现仍应得到 ~10%，共享基线会得到 5000*cores
            write_fake_stat(stat_path(workerB.pid), make_stat_line(workerB.pid, "workerB", 150, 0, 602));
            write_fake_stat("/proc/stat", make_proc_stat_line(t0 + tick + 1));
            mark("CPUMEM_BEGIN multi_tick2_b");
            const auto b2 = std::any_cast<std::vector<CPUMemInfo>>(multi.collect(jobB));
            mark("CPUMEM_END multi_tick2_b");
            require(b2.size() == 1, "jobB sample missing");
            require(std::abs(b2.front().cpuPercent - 10.0) < 0.5,
                    "jobB cpuPercent inflated by cross-job shared system baseline");
            require(b2.front().cpuPercent <= 100.0 * cores, "jobB cpuPercent exceeds machine capacity");
            multi.deinit();
            fake_files.clear();
            fake_file_fds.clear();
        }

        // 超过 TTL 后先确认进程身份：清理已退出/复用的 PID，保留低频采样的存活进程。
        {
            Child worker;
            write_fake_stat(stat_path(worker.pid), make_stat_line(worker.pid, "workerP", 10, 0, 701));
            const unsigned long long t0 = 1000000000ULL;
            const unsigned long long interval = 100ULL * static_cast<unsigned long long>(cores);
            write_fake_stat("/proc/stat", make_proc_stat_line(t0));
            CPUMemCollector pruner;
            require(pruner.init({}), "prune collector init failed");
            Job prune_job;
            prune_job.JobID = 21;
            prune_job.JobPIDs = {worker.pid};
            (void)pruner.collect(prune_job);
            require(CPUMemCollectorTestAccess::has(pruner, worker.pid), "active pid baseline missing");

            constexpr int kGhostPid = 999999;
            CPUMemCollectorTestAccess::insert_stale(
                pruner, kGhostPid, std::chrono::steady_clock::now() - std::chrono::seconds(600));
            CPUMemCollectorTestAccess::insert_stale(
                pruner, worker.pid, std::chrono::steady_clock::now() - std::chrono::seconds(600));
            const size_t before = CPUMemCollectorTestAccess::size(pruner);

            write_fake_stat(stat_path(worker.pid), make_stat_line(worker.pid, "worker (P)", 20, 0, 701));
            write_fake_stat("/proc/stat", make_proc_stat_line(t0 + interval));
            denied_stat = stat_path(kGhostPid);
            denied_stat_errno = ENOENT;
            CPUMemCollectorTestAccess::reset_prune_clock(pruner);
            mark("CPUMEM_BEGIN prune");
            const auto slow_sample = std::any_cast<std::vector<CPUMemInfo>>(pruner.collect(prune_job));
            mark("CPUMEM_END prune");
            denied_stat.clear();
            denied_stat_errno = EACCES;

            require(CPUMemCollectorTestAccess::size(pruner) < before,
                    "stale pid baseline was not pruned");
            require(!CPUMemCollectorTestAccess::has(pruner, kGhostPid), "stale pid baseline retained");
            require(CPUMemCollectorTestAccess::has(pruner, worker.pid), "live pid baseline was pruned");
            require(slow_sample.size() == 1 && std::abs(slow_sample.front().cpuPercent - 10.0) < 0.01,
                    "slow sampling lost the live process CPU baseline");

            CPUMemCollectorTestAccess::insert_stale(
                pruner, worker.pid, std::chrono::steady_clock::now() - std::chrono::seconds(600));
            write_fake_stat(stat_path(worker.pid), make_stat_line(worker.pid, "worker (P)", 30, 0, 701));
            write_fake_stat("/proc/stat", make_proc_stat_line(t0 + 2 * interval));
            CPUMemCollectorTestAccess::reset_prune_clock(pruner);
            const auto next_slow_sample = std::any_cast<std::vector<CPUMemInfo>>(pruner.collect(prune_job));
            require(next_slow_sample.size() == 1 && std::abs(next_slow_sample.front().cpuPercent - 10.0) < 0.01,
                    "repeated slow sampling lost the live process CPU baseline");

            // 无权限或 stat 解析失败不能确认退出，保留基线供下次检查。
            CPUMemCollectorTestAccess::insert_stale(
                pruner, worker.pid, std::chrono::steady_clock::now() - std::chrono::seconds(600));
            denied_stat = stat_path(worker.pid);
            CPUMemCollectorTestAccess::reset_prune_clock(pruner);
            CPUMemCollectorTestAccess::prune_now(pruner);
            denied_stat.clear();
            require(CPUMemCollectorTestAccess::has(pruner, worker.pid),
                    "permission error was treated as process exit");

            for (const auto* malformed : {"invalid stat\n", "42 (worker)\n", "42 (worker) S 1\n", ""}) {
                write_fake_stat(stat_path(worker.pid), malformed);
                CPUMemCollectorTestAccess::reset_prune_clock(pruner);
                CPUMemCollectorTestAccess::prune_now(pruner);
                require(CPUMemCollectorTestAccess::has(pruner, worker.pid),
                        "malformed stat was treated as process exit");
            }

            // PID 仍存在但 starttime 改变，清理旧进程基线，新进程首采样为 0。
            write_fake_stat(stat_path(worker.pid), make_stat_line(worker.pid, "newWorker", 1, 0, 702));
            CPUMemCollectorTestAccess::reset_prune_clock(pruner);
            CPUMemCollectorTestAccess::prune_now(pruner);
            require(!CPUMemCollectorTestAccess::has(pruner, worker.pid),
                    "expired baseline survived PID reuse");
            const auto reused_sample = std::any_cast<std::vector<CPUMemInfo>>(pruner.collect(prune_job));
            require(reused_sample.size() == 1 && reused_sample.front().cpuPercent == 0.0,
                    "reused PID inherited the old process CPU baseline");
            pruner.deinit();
            require(CPUMemCollectorTestAccess::size(pruner) == 0,
                    "deinit did not release pid baselines");
            fake_files.clear();
            fake_file_fds.clear();
        }

        // 作业级计量口径：peak 取「采样聚合值」的时序 max（而不是各进程峰值之和），
        // 同时覆盖 VmSwap、PSS 聚合与 cutime/cstime 补齐短命子进程 CPU。
        {
            Child a, b;
            CPUMemCollector agg;
            require(agg.init({{"summary", "true"}}), "summary collector init failed");
            if (access("/proc/self/smaps_rollup", F_OK) == 0) {
                require(CPUMemCollectorTestAccess::uses_rollup(agg),
                        "smaps_rollup capability probe did not pick the fast path");
            }
            Job agg_job;
            agg_job.JobID = 31;
            agg_job.JobPIDs = {a.pid, b.pid};
            const unsigned long long t0 = 1000000000ULL;
            const unsigned long long tick = 500ULL * static_cast<unsigned long long>(cores);

            // 采样 1：A 10MB/PSS 4MB/swap 1MB，B 20MB/PSS 6MB/swap 2MB；两进程 VmHWM 都是 50MB
            write_fake_stat(stat_path(a.pid), make_stat_line_with_children(a.pid, "a", 0, 0, 0, 0, 811));
            write_fake_stat(stat_path(b.pid), make_stat_line_with_children(b.pid, "b", 0, 0, 0, 0, 812));
            write_fake_stat(status_path(a.pid), make_status_line(10240, 65536, 51200, 1024, 2));
            write_fake_stat(status_path(b.pid), make_status_line(20480, 65536, 51200, 2048, 3));
            write_fake_stat(rollup_path(a.pid), make_rollup_line(4096));
            write_fake_stat(rollup_path(b.pid), make_rollup_line(6144));
            write_fake_stat("/proc/stat", make_proc_stat_line(t0));
            const auto first_sample = std::any_cast<std::vector<CPUMemInfo>>(agg.collect(agg_job));
            require(first_sample.size() == 3, "summary record missing");
            const auto& sum1 = summary_of(first_sample);
            require(sum1.mem_rss_kb == 30720 && sum1.mem_pss_kb == 10240 && sum1.mem_swap_kb == 3072,
                    "summary aggregation of RSS/PSS/VmSwap is wrong");
            require(sum1.mem_peak_rss_kb == 30720 && sum1.mem_peak_pss_kb == 10240 &&
                    sum1.sample_count == 1, "first sample peak is not the aggregated value");
            for (const auto& info : first_sample) {
                if (info.pid != 0) {
                    require(info.mem_peak_rss_kb == 51200, "per-process VmHWM semantics changed");
                }
            }

            // 采样 2：A 40MB/PSS 10MB/swap 4MB + 已回收子进程 90 jiffies；B 30MB/PSS 8MB/swap 3MB
            write_fake_stat(stat_path(a.pid), make_stat_line_with_children(a.pid, "a", 10, 0, 90, 0, 811));
            write_fake_stat(stat_path(b.pid), make_stat_line_with_children(b.pid, "b", 0, 0, 0, 0, 812));
            write_fake_stat(status_path(a.pid), make_status_line(40960, 65536, 51200, 4096, 2));
            write_fake_stat(status_path(b.pid), make_status_line(30720, 65536, 51200, 3072, 3));
            write_fake_stat(rollup_path(a.pid), make_rollup_line(10240));
            write_fake_stat(rollup_path(b.pid), make_rollup_line(8192));
            write_fake_stat("/proc/stat", make_proc_stat_line(t0 + tick));
            const auto second_sample = std::any_cast<std::vector<CPUMemInfo>>(agg.collect(agg_job));
            const auto& sum2 = summary_of(second_sample);
            require(sum2.mem_rss_kb == 71680 && sum2.mem_pss_kb == 18432 && sum2.mem_swap_kb == 7168,
                    "second sample aggregation is wrong");
            // 各进程 VmHWM 之和是 100MB，真实作业峰值是两次采样聚合值的 max = 70MB
            require(sum2.mem_peak_rss_kb == 71680, "job peak still sums per-process VmHWM");
            require(sum2.mem_peak_pss_kb == 18432 && sum2.sample_count == 2,
                    "job PSS peak or sample count is wrong");
            require(sum2.cutime == 90 && sum2.cstime == 0,
                    "reaped children CPU was not carried into the job summary");
            require(std::abs(record_of(second_sample, a.pid).cpuPercent - 20.0) < 0.5,
                    "cpuPercent does not include reaped children CPU");

            const auto agg_doc = std::any_cast<nlohmann::json>(
                agg.get_writer_parser("ESWriter")(second_sample));
            const auto& sum_json = agg_doc.at("summary");
            require(sum_json.at("mem_peak_rss_kb").get<long>() == 71680 &&
                    sum_json.at("mem_pss_kb").get<long>() == 18432 &&
                    sum_json.at("sample_count").get<unsigned long long>() == 2 &&
                    sum_json.contains("cutime") && sum_json.contains("cstime"),
                    "summary JSON lost the job-level accounting fields");
            require(CPUMemCollectorTestAccess::job_peaks(agg) == 1, "job peak state is not per-job");
            agg.deinit();
            require(CPUMemCollectorTestAccess::job_peaks(agg) == 0,
                    "deinit did not release job peak states");
            fake_files.clear();
            fake_file_fds.clear();
        }

        // PSS 开关：关闭后不得再读 smaps_rollup，且输出中省略字段而不是给 0。
        {
            Child c;
            CPUMemCollector no_pss;
            require(no_pss.init({{"summary", "true"}, {"pss", "false"}}), "pss=false init failed");
            Job no_pss_job;
            no_pss_job.JobID = 32;
            no_pss_job.JobPIDs = {c.pid};
            write_fake_stat(stat_path(c.pid), make_stat_line_with_children(c.pid, "c", 0, 0, 0, 0, 821));
            write_fake_stat(status_path(c.pid), make_status_line(8192, 65536, 9000, 512, 1));
            write_fake_stat(rollup_path(c.pid), make_rollup_line(4096));
            fake_opens.clear();
            const auto no_pss_sample = std::any_cast<std::vector<CPUMemInfo>>(no_pss.collect(no_pss_job));
            require(fake_opens[rollup_path(c.pid)] == 0, "pss=false still read smaps_rollup");
            require(summary_of(no_pss_sample).mem_pss_kb == -1 &&
                    summary_of(no_pss_sample).mem_peak_pss_kb == -1,
                    "disabled PSS must stay unavailable");
            const auto no_pss_doc = std::any_cast<nlohmann::json>(
                no_pss.get_writer_parser("ESWriter")(no_pss_sample));
            require(!no_pss_doc.at("summary").contains("mem_pss_kb") &&
                    !no_pss_doc.at("summary").contains("mem_peak_pss_kb"),
                    "unavailable PSS was rendered as a value");
            no_pss.deinit();
            fake_files.clear();
            fake_file_fds.clear();
        }

        // 降级路径：内核无 smaps_rollup 时逐 VMA 累加 smaps 的 Pss 行。
        {
            Child legacy_child;
            CPUMemCollector legacy;
            require(legacy.init({{"summary", "true"}}), "legacy collector init failed");
            CPUMemCollectorTestAccess::force_smaps_mode(legacy);
            Job legacy_job;
            legacy_job.JobID = 33;
            legacy_job.JobPIDs = {legacy_child.pid};
            write_fake_stat(stat_path(legacy_child.pid),
                            make_stat_line_with_children(legacy_child.pid, "legacy", 0, 0, 0, 0, 831));
            write_fake_stat(status_path(legacy_child.pid),
                            make_status_line(8192, 65536, 9000, 0, 1));
            write_fake_stat(smaps_path(legacy_child.pid), make_smaps_body(12, 8));
            const auto legacy_sample = std::any_cast<std::vector<CPUMemInfo>>(legacy.collect(legacy_job));
            require(record_of(legacy_sample, legacy_child.pid).mem_pss_kb == 20,
                    "smaps fallback did not accumulate per-VMA Pss");
            require(summary_of(legacy_sample).mem_pss_kb == 20 &&
                    summary_of(legacy_sample).mem_peak_pss_kb == 20,
                    "smaps fallback PSS was not aggregated into the summary");
            legacy.deinit();
            fake_files.clear();
            fake_file_fds.clear();
        }

        std::cout << "PASS CPUMem real proc samples, shared CPU inputs, comm fallback, PID reuse guard, "
                     "per-PID system baseline across jobs and identity-aware stale baseline pruning, "
                     "job peak from aggregated series, VmSwap, PSS aggregate/switch/degrade, "
                     "reaped children CPU\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
