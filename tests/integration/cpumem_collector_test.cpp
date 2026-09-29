#include "collector/cpumem_collector.hpp"
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

static std::string denied_stat;
static std::map<std::string, std::string> fake_files;   // 目标 /proc 路径 -> 伪造文件路径
static std::map<std::string, int> fake_file_fds;
static unsigned clock_queries = 0, core_queries = 0;

static std::string stat_path(pid_t pid) {
    return "/proc/" + std::to_string(pid) + "/stat";
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
    const auto it = fake_files.find(path ? std::string(path) : std::string());
    if (it != fake_files.end()) {
        return real_open(it->second.c_str(), mode);
    }
    if (!denied_stat.empty() && denied_stat == path) {
        errno = EACCES;
        return nullptr;
    }
    return real_open(path, mode);
}

extern "C" long __real_sysconf(int);
extern "C" long __wrap_sysconf(int name) {
    if (name == _SC_CLK_TCK) ++clock_queries;
    if (name == _SC_NPROCESSORS_ONLN) ++core_queries;
    return __real_sysconf(name);
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

        std::cout << "PASS CPUMem real proc samples, shared CPU inputs, comm fallback, PID reuse guard "
                     "and per-PID system baseline across jobs\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
