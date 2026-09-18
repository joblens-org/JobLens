#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "core/collector_type.h"
#include "icollector.h"
#include <spdlog/spdlog.h>
#include <unordered_map>
#include <vector>
#include <string>

// 单个 SSH 会话进程的采集数据
struct SSHProcessInfo {
    pid_t pid{};
    pid_t ppid{};
    uid_t uid{};
    std::string user;
    std::string comm;          // /proc/pid/comm
    std::string cmdline;       // /proc/pid/cmdline (截断至 256 字节)
    char    state{'?'};        // R/S/D/Z/T
    int     num_threads{};
    double  cpu_percent{};
    long    mem_rss_kb{};
    long    mem_vm_kb{};
    double  mem_percent{};
    unsigned long long starttime{};      // 进程启动 jiffies (from /proc/pid/stat)
    unsigned long long io_read_bytes{};
    unsigned long long io_write_bytes{};
};

class SSHSessionCollector : public ICollector {
public:
    SSHSessionCollector() { scope_ = CollectorScope::System; }

    bool init(const nlohmann::json& cfg) override;
    void deinit() noexcept override;
    CollectResult collect() override;                         // 系统级采集
    CollectResult collect(const Job& job) override { return {}; }  // 不使用
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;

private:
    bool readProcStatus(int pid, SSHProcessInfo& info);
    bool readProcStat(int pid, SSHProcessInfo& info);
    bool readProcCmdline(int pid, SSHProcessInfo& info);
    bool readProcIO(int pid, SSHProcessInfo& info);
    bool readProcComm(int pid, SSHProcessInfo& info);
    std::string uidToUser(uid_t uid);

    bool inited_{false};
    bool collect_io_{false};

    // CPU delta 计算状态 (同 CPUMemCollector 模式)
    struct PidCPUState {
        unsigned long long last_total{};
        unsigned long long last_proc{};
    };
    std::unordered_map<int, PidCPUState> pid_cpu_state_;

    // UID → 用户名缓存
    std::unordered_map<uid_t, std::string> uid_cache_;

    // 系统内存总量
    long total_phys_mem_kb_{0};
    long getTotalPhysMemKB();
};