#include "collector/cpumem_collector.hpp"
#include <cmath>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>

static std::string denied_stat;
static unsigned clock_queries = 0, core_queries = 0;

// Only the chosen stat open is denied; all successful reads use real procfs.
extern "C" FILE* fopen64(const char* path, const char* mode) {
    using Open = FILE* (*)(const char*, const char*);
    static const auto real_open = reinterpret_cast<Open>(dlsym(RTLD_NEXT, "fopen64"));
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
        std::cout << "PASS CPUMem real proc samples, shared CPU inputs and comm fallback\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
