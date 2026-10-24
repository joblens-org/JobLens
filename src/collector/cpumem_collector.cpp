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
#include "collector/cpumem_collector.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <filesystem>
#include <unistd.h>

#include "core/collector_registry.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include "common/utils.hpp"

namespace {

bool IsVanishedProcPath(int err) {
    return err == ENOENT || err == ENOTDIR;
}

// 采集器配置统一经由 yamlToJson 转成字符串，这里同时兼容字符串、bool 与整数写法。
bool ConfigBool(const nlohmann::json& cfg, const char* key, bool default_value) {
    if (!cfg.is_object() || !cfg.contains(key) || cfg[key].is_null()) return default_value;
    const auto& value = cfg[key];
    if (value.is_boolean()) return value.get<bool>();
    if (value.is_number_integer()) return value.get<long long>() != 0;
    if (value.is_string()) {
        std::string text = value.get<std::string>();
        std::transform(text.begin(), text.end(), text.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return text == "true" || text == "1" || text == "yes" || text == "on";
    }
    return default_value;
}

// 整数配置：同样兼容 yamlToJson 的字符串形态与真实数字。
int ConfigInt(const nlohmann::json& cfg, const char* key, int default_value) {
    if (!cfg.is_object() || !cfg.contains(key) || cfg[key].is_null()) return default_value;
    const auto& value = cfg[key];
    if (value.is_number_integer()) return value.get<int>();
    if (value.is_number_unsigned()) return static_cast<int>(value.get<unsigned int>());
    if (value.is_number_float()) return static_cast<int>(value.get<double>());
    if (value.is_string()) {
        try {
            return std::stoi(value.get<std::string>());
        } catch (const std::exception&) {
            return default_value;
        }
    }
    return default_value;
}

// 单进程/汇总记录共用同一套字段映射，避免三份 parser（ES/File/V2）口径漂移。
nlohmann::json ToJson(const CPUMemInfo& info) {
    nlohmann::json j;
    j["pid"] = info.pid;
    j["ppid"] = info.ppid;
    j["name"] = info.name;
    j["cpuPercent"] = info.cpuPercent;
    j["utime"] = info.utime;
    j["stime"] = info.stime;
    // 已回收子进程的累计 CPU：子进程退出后只能从这里取回它的 CPU 时间。
    j["cutime"] = info.cutime;
    j["cstime"] = info.cstime;
    j["starttime"] = info.starttime;
    j["hz"] = info.hz;
    j["mem_vm_kb"] = info.mem_vm_kb;
    j["mem_rss_kb"] = info.mem_rss_kb;
    j["mem_swap_kb"] = info.mem_swap_kb;
    j["mem_peak_rss_kb"] = info.mem_peak_rss_kb;
    // PSS 不可用时不出字段，避免把「没有数据」渲染成 0。
    if (info.mem_pss_kb >= 0) j["mem_pss_kb"] = info.mem_pss_kb;
    if (info.pid == 0) {
        if (info.mem_peak_pss_kb >= 0) j["mem_peak_pss_kb"] = info.mem_peak_pss_kb;
        j["sample_count"] = info.sample_count;
    }
    j["memoryPercent"] = info.memoryPercent;
    j["numThreads"] = info.numThreads;
    return j;
}

bool IsProcessStatusGone(int pid) {
    std::error_code ec;
    return !std::filesystem::exists(fmt::format("/proc/{}/status", pid), ec);
}

enum class ProcessStatRead { Ok, Missing, Unavailable };

ProcessStatRead ReadProcessStat(int pid, CPUMemInfo& info) {
    errno = 0;
    std::ifstream f(fmt::format("/proc/{}/stat", pid));
    if (!f) {
        return IsVanishedProcPath(errno) ? ProcessStatRead::Missing : ProcessStatRead::Unavailable;
    }

    std::string line;
    if (!std::getline(f, line)) return ProcessStatRead::Unavailable;

    auto p1 = line.find('(');
    auto p2 = line.rfind(')');
    if (p1 == std::string::npos || p2 == std::string::npos || p2 <= p1 || p2 + 2 >= line.size())
        return ProcessStatRead::Unavailable;

    std::istringstream iss(line.substr(p2 + 2));
    char state;
    int ppid, pgrp, session, tty_nr, tpgid;
    unsigned flags;
    unsigned long minflt, cminflt, majflt, cmajflt;
    unsigned long utime, stime, cutime, cstime;
    long priority, nice, num_threads, itrealvalue;
    unsigned long long starttime;

    if (!(iss >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid
        >> flags >> minflt >> cminflt >> majflt >> cmajflt
        >> utime >> stime >> cutime >> cstime
        >> priority >> nice >> num_threads >> itrealvalue >> starttime))
        return ProcessStatRead::Unavailable;

    info.name = line.substr(p1 + 1, p2 - p1 - 1);
    info.utime = utime;
    info.stime = stime;
    info.cutime = cutime;
    info.cstime = cstime;
    info.starttime = starttime;
    info.ppid = ppid;
    return ProcessStatRead::Ok;
}

}

AUTO_REGISTER_JOB_COLLECTOR(
    CPUMemCollector,
    "Collect CPU and memory usage statistics from /proc/[pid]/",
    ConfigParams{
       {"freq", "Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds"},
       {"summary", "Whether to summarize data across all processes (true/false), default false"}       
    }
)

bool CPUMemCollector::init(const nlohmann::json& cfg) {
    
    if (inited){
        spdlog::warn("CPUMemCollector init twice with config: {}", cfg.dump());
        return false;
    }
    spdlog::info("CPUMemCollector init with config: {}", cfg.dump());

    summary = ConfigBool(cfg, "summary", false);

    // PSS 采集策略：能力探测 + 配置开关 + 降级。
    //   smaps_rollup 可用   → 单次聚合读，成本最低；
    //   仅 smaps 可用      → 逐 VMA 累加 Pss（旧内核降级路径，可关闭）；
    //   两者都不可用       → 关闭 PSS，输出中省略该字段而不是给出 0。
    pss_enabled = ConfigBool(cfg, "pss", true);
    pss_fallback_smaps = ConfigBool(cfg, "pss_fallback_smaps", true);
    pss_max_processes = ConfigInt(cfg, "pss_max_processes", 64);
    if (pss_max_processes < 0) {
        spdlog::warn("CPUMemCollector: pss_max_processes={} is negative, treating as unlimited",
                     pss_max_processes);
        pss_max_processes = 0;
    }
    std::error_code ec;
    if (!pss_enabled) {
        pss_mode = PssMode::Disabled;
        spdlog::info("CPUMemCollector: PSS collection disabled by config");
    } else if (std::filesystem::exists("/proc/self/smaps_rollup", ec)) {
        pss_mode = PssMode::Rollup;
        spdlog::info("CPUMemCollector: PSS enabled via smaps_rollup");
    } else if (pss_fallback_smaps && std::filesystem::exists("/proc/self/smaps", ec)) {
        pss_mode = PssMode::Smaps;
        spdlog::warn("CPUMemCollector: smaps_rollup unavailable, PSS degrades to smaps (higher per-sample cost)");
    } else {
        pss_mode = PssMode::Disabled;
        spdlog::warn("CPUMemCollector: no smaps_rollup/smaps available, mem_pss_kb will be omitted");
    }

    inited = true;
    return true;
}


void CPUMemCollector::deinit() noexcept {
    if (!inited){
        return;
    }
    inited = false;
    pid_state_dict.clear();
    job_peak_map.clear();
    last_prune_ = {};
    spdlog::info("CPUMemCollector deinit");
}

// TTL 只筛选待检查项，不能直接删除低频采样的存活进程基线。最多每分钟检查一次，
// 仅清理已消失或 starttime 已改变的进程；权限/解析失败时保留，等待下次确认。
void CPUMemCollector::prune_stale_pid_states(const std::chrono::steady_clock::time_point& now) {
    constexpr double kStateTtlSeconds = 300.0;
    constexpr double kPruneIntervalSeconds = 60.0;
    if (last_prune_.time_since_epoch().count() != 0 &&
        std::chrono::duration<double>(now - last_prune_).count() < kPruneIntervalSeconds) return;
    last_prune_ = now;
    size_t removed = 0;
    for (auto it = pid_state_dict.begin(); it != pid_state_dict.end();) {
        bool stale = false;
        if (std::chrono::duration<double>(now - it->second.last_seen).count() > kStateTtlSeconds) {
            CPUMemInfo info;
            const auto result = ReadProcessStat(it->first, info);
            stale = result == ProcessStatRead::Missing ||
                    (result == ProcessStatRead::Ok && info.starttime != it->second.lastStarttime);
        }
        if (stale) {
            it = pid_state_dict.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    if (removed > 0) {
        spdlog::debug("CPUMemCollector: pruned {} stale pid baselines", removed);
    }

    // 作业级峰值同样按 TTL 回收：作业结束后不再有采样，条目会自然过期。
    constexpr double kJobPeakTtlSeconds = 3600.0;
    size_t pruned_jobs = 0;
    for (auto it = job_peak_map.begin(); it != job_peak_map.end();) {
        if (std::chrono::duration<double>(now - it->second.last_seen).count() > kJobPeakTtlSeconds) {
            it = job_peak_map.erase(it);
            ++pruned_jobs;
        } else {
            ++it;
        }
    }
    if (pruned_jobs > 0) {
        spdlog::debug("CPUMemCollector: pruned {} stale job peak states", pruned_jobs);
    }
}

// 静态函数：读 /proc/stat 获取系统总 jiffies
static unsigned long long read_system_cpu()
{
    std::ifstream stat("/proc/stat");
    std::string line;
    std::getline(stat, line);
    std::istringstream iss(line);
    std::string cpu;
    iss >> cpu;
    unsigned long long value, total = 0;
    while (iss >> value) total += value;
    return total;
}

bool CPUMemCollector::CPUOf(int pid, CPUMemInfo& info, const CpuSample& sample,
                            const std::chrono::steady_clock::time_point& now){
    if (ReadProcessStat(pid, info) != ProcessStatRead::Ok) return false;
    spdlog::trace("CPUOf: pid={} utime={} stime={} starttime={}", pid, info.utime, info.stime, info.starttime);

    /* ---- 计算 CPU 百分比 ---- */
    info.hz = sample.hz;
    if (info.hz > 0 && sample.numCores > 0) {
        // 计入 cutime/cstime：短命令（gzip/tar/pre-post 脚本）在两个采样点之间
        // fork 完就退出，其 /proc/<pid> 早已消失，只有父进程的 children 累计量
        // 还留着这段 CPU。存活子进程的自身时间不会被父进程 cutime 重复统计，
        // 因为内核只在子进程被 reap 时才把它累加进父进程。
        const unsigned long long currProc = info.utime + info.stime + info.cutime + info.cstime;
        auto& cu = pid_state_dict[pid];
        const bool pidReused = cu.lastStarttime != 0 && cu.lastStarttime != info.starttime;
        unsigned long long deltaProc = 0;
        unsigned long long deltaTotal = 0;

        if (pidReused) {
            // PID 被回收：同 PID 已是新进程（starttime 不同），丢弃旧基线避免无符号下溢
            spdlog::debug("CPUOf: pid {} reused (starttime {} -> {}), reset baseline",
                          pid, cu.lastStarttime, info.starttime);
        } else if (cu.lastTotal == 0 || sample.total < cu.lastTotal) {
            // 首次采样，或系统总 jiffies 回退（CPU 热插拔等）：本周期不产出速率
            spdlog::trace("CPUOf: pid {} baseline reset (lastTotal={} total={})",
                          pid, cu.lastTotal, sample.total);
        } else {
            deltaTotal = sample.total - cu.lastTotal;
            if (currProc >= cu.lastProc) {
                deltaProc = currProc - cu.lastProc;
            } else {
                // 计数器回退（进程重启等未识别情况），本周期按 0 处理，避免无符号下溢
                spdlog::debug("CPUOf: pid {} cpu counter regressed ({} -> {}), skip delta",
                              pid, cu.lastProc, currProc);
            }
        }

        // Δproc 与 Δtotal 取自该 PID 的同一采样区间，与 tick 内作业数量/顺序无关
        info.cpuPercent = deltaTotal > 0
                              ? 100.0 * double(deltaProc) / double(deltaTotal) * sample.numCores
                              : 0.0;

        /* 更新基线（用于下一次采样） */
        cu.lastProc      = currProc;
        cu.lastStarttime = info.starttime;
        cu.lastTotal     = sample.total;
        cu.last_seen     = now;
        spdlog::trace("update lastProc={} lastTotal={} lastStarttime={}",
                      cu.lastProc, cu.lastTotal, cu.lastStarttime);
    } else {
        info.cpuPercent = 0.0;
    }
    return true;
}

bool CPUMemCollector::MemOf(int pid,CPUMemInfo& info){
    std::string path = "/proc/" + std::to_string(pid) + "/status";
    errno = 0;
    std::ifstream sf(path);
    if (!sf){
        int err = errno;
        if (IsVanishedProcPath(err) || IsProcessStatusGone(pid)) {
            spdlog::debug("MemOf: skip vanished process status {}", path);
        } else {
            spdlog::error("MemOf: cannot open {}", path);
        }
        return false;
    }

    std::string line;
    while (std::getline(sf, line))
    {
        std::istringstream iss(line);
        std::string key;
        long val = 0;
        std::string unit;

        if (line.compare(0, 6, "VmRSS:") == 0)
        {
            iss >> key >> val >> unit;   // unit = "kB"
            info.mem_rss_kb = val;
        }
        else if (line.compare(0, 7, "VmSize:") == 0)
        {
            iss >> key >> val >> unit;
            info.mem_vm_kb = val;
        }
        else if (line.compare(0, 6, "VmHWM:") == 0)
        {
            iss >> key >> val >> unit;
            info.mem_peak_rss_kb = val;
        }
        else if (line.compare(0, 7, "VmSwap:") == 0)
        {
            iss >> key >> val >> unit;
            info.mem_swap_kb = val;
        }
        else if (line.compare(0, 8, "Threads:") == 0){
            iss >> key >> val;
            info.numThreads = val;
        }
    }
    info.memoryPercent = info.mem_rss_kb > 0 && PhysMemKB > 0 ?
                         100.0 * double(info.mem_rss_kb) / double(PhysMemKB) : 0.0;
    if (!info.mem_rss_kb && !info.mem_vm_kb){
        if (IsProcessStatusGone(pid)) {
            spdlog::debug("MemOf: skip vanished process status /proc/{}/status", pid);
        } else {
            spdlog::error("MemOf: parse /proc/{}/status failed", pid);
        }
        return false;
    }
    return true;
}

// PSS：/proc/<pid>/smaps_rollup 的单次聚合读（内核 4.14+）。
// 只取顶层 "Pss:" 行，"Pss_Anon:"/"Pss_File:"/"Pss_Shmem:" 不计入。
bool CPUMemCollector::ReadRollupPssKb(int pid, long& pss_kb) {
    errno = 0;
    std::ifstream f(fmt::format("/proc/{}/smaps_rollup", pid));
    if (!f) {
        spdlog::debug("PSS: cannot open /proc/{}/smaps_rollup (errno={})", pid, errno);
        return false;
    }
    std::string line;
    while (std::getline(f, line)) {
        if (line.compare(0, 4, "Pss:") != 0) continue;
        std::istringstream iss(line);
        std::string key, unit;
        long val = 0;
        if (!(iss >> key >> val)) return false;
        pss_kb = val;
        return true;
    }
    // 僵尸进程的 smaps_rollup 为空，没有 Pss 行；它本来也不占用内存，按不可用处理。
    return false;
}

// PSS 降级路径：旧内核没有 smaps_rollup，逐 VMA 累加 smaps 的 "Pss:" 行。
bool CPUMemCollector::ReadSmapsPssKb(int pid, long& pss_kb) {
    errno = 0;
    std::ifstream f(fmt::format("/proc/{}/smaps", pid));
    if (!f) {
        spdlog::debug("PSS: cannot open /proc/{}/smaps (errno={})", pid, errno);
        return false;
    }
    long total = 0;
    bool found = false;
    std::string line;
    while (std::getline(f, line)) {
        if (line.compare(0, 4, "Pss:") != 0) continue;
        std::istringstream iss(line);
        std::string key;
        long val = 0;
        if (!(iss >> key >> val)) continue;
        total += val;
        found = true;
    }
    if (!found) return false;
    pss_kb = total;
    return true;
}

// 读取失败（进程刚退出 / 权限受限）不视为 0，而是标记为不可用；
// 汇总时只有全部进程都取不到 PSS 才省略该字段，单个进程失败按 0 计入。
bool CPUMemCollector::PssOf(int pid, CPUMemInfo& info) {
    if (pss_mode == PssMode::Disabled) {
        info.mem_pss_kb = -1;
        return false;
    }
    long pss_kb = 0;
    const bool ok = (pss_mode == PssMode::Rollup) ? ReadRollupPssKb(pid, pss_kb)
                                                  : ReadSmapsPssKb(pid, pss_kb);
    if (!ok) {
        info.mem_pss_kb = -1;
        return false;
    }
    info.mem_pss_kb = pss_kb;
    return true;
}

std::string get_process_name(int pid)
{
    std::ostringstream path;
    path << "/proc/" << pid << "/comm";

    std::ifstream comm(path.str());
    if (!comm){
        // 内核线程没有 /proc/[pid]/comm
        if (errno != ENOENT)  // 其他错误才打印日志
            spdlog::error("get_process_name: cannot open {}", path.str());
        return "";                   // 返回空串表示内核线程或异常
    }
    std::string name;
    std::getline(comm, name);      // 末尾可能带 '\n'，getline 会去掉
    return name;                   // 返回空串表示内核线程或异常
}

CollectResult CPUMemCollector::collect(const Job& job) {
    std::vector<CPUMemInfo> infos;
    const auto now = std::chrono::steady_clock::now();
    prune_stale_pid_states(now);
    // CLK_TCK is invariant; online cores and CPU counters can change between Jobs.
    static const long clock_ticks = sysconf(_SC_CLK_TCK);
    CpuSample sample{clock_ticks, sysconf(_SC_NPROCESSORS_ONLN), 0};
    if (sample.hz > 0 && sample.numCores > 0) {
        // /proc/stat 每 Job 读一次；增量基线按 PID 缓存，保证与 Δproc 覆盖同一采样区间
        sample.total = read_system_cpu();
    }
    infos.reserve(job.JobPIDs.size() + (summary ? 1 : 0));
    // PSS 成本按进程数线性增长（实测单进程约 0.5-1.3ms），超过阈值时本样本整体跳过：
    // 只对前 N 个进程求和会静默低估作业内存，全有或全无才能保证口径可解释。
    const bool pss_over_cap = pss_mode != PssMode::Disabled && pss_max_processes > 0 &&
                              job.JobPIDs.size() > static_cast<size_t>(pss_max_processes);
    if (pss_over_cap) {
        spdlog::debug("CPUMemCollector: job {} has {} pids > pss_max_processes={}, skipping PSS",
                      job.JobID, job.JobPIDs.size(), pss_max_processes);
    }
    for (int pid : job.JobPIDs) {
        if (! Utils::is_process_running(pid)){
            continue;
        }
        CPUMemInfo info;
        if (pid <= 0) continue;
        info.pid = pid;
        if (!CPUOf(pid, info, sample, now)) info.name = get_process_name(pid);
        // 进程已消失/无法读取 status 时不再尝试 PSS，省掉一次必然失败的 open。
        if (MemOf(pid, info) && !pss_over_cap) PssOf(pid, info);
        if (!info.pid) continue;
        // job.JobInfo[fmt::format("proc_info_{}", pid)] = info.get();
        infos.push_back(info);
    }
    spdlog::debug("CPUMemCollector: collected {} entries for job {}", infos.size(), job.JobID);
    if (summary){
        CPUMemInfo sum_info;
        sum_info.pid = 0;
        sum_info.name = "JOB-SUMMARY";
        long pss_sum = 0;
        bool pss_seen = false;
        for(const auto& info: infos){
            sum_info.cpuPercent += info.cpuPercent;
            sum_info.memoryPercent += info.memoryPercent;
            sum_info.utime += info.utime;
            sum_info.stime += info.stime;
            // 汇总后的作业 CPU 总量 = 存活进程自身时间 + 已被回收的短命子进程时间
            sum_info.cutime += info.cutime;
            sum_info.cstime += info.cstime;
            sum_info.mem_vm_kb += info.mem_vm_kb;
            sum_info.mem_rss_kb += info.mem_rss_kb;
            sum_info.mem_swap_kb += info.mem_swap_kb;
            // 注意：这里不再累加各进程的 VmHWM（mem_peak_rss_kb），
            // 各进程峰值出现在不同时刻，相加只会得到系统性偏高的上界。
            if (info.mem_pss_kb >= 0) {
                pss_sum += info.mem_pss_kb;
                pss_seen = true;
            }
            sum_info.numThreads += info.numThreads;
            sum_info.starttime = std::min(sum_info.starttime, info.starttime);
        }
        sum_info.mem_pss_kb = pss_seen ? pss_sum : -1;
        // 作业级峰值 = 对「采样聚合值」序列取 max；见 update_job_peak()
        update_job_peak(job, sum_info, now);
        infos.push_back(sum_info);
    }
    return infos;
}

// 作业级峰值必须对「每次采样的聚合值」取 max，而不是把各进程自身的峰值相加：
// 500MB 的父进程和 500MB 的子进程可能先后达到峰值，求和会报出 1GB，
// 而作业任一时刻的真实占用最多 500MB。sample_count 供下游判断峰值/均值
// 是否只基于极少数采样（例如秒级短作业只被采到 1 次）。
void CPUMemCollector::update_job_peak(const Job& job, CPUMemInfo& summary_info,
                                      const std::chrono::steady_clock::time_point& now) {
    auto& peak = job_peak_map[job.JobID];
    peak.last_seen = now;
    ++peak.samples;
    if (summary_info.mem_rss_kb > peak.peak_rss_kb) {
        peak.peak_rss_kb = summary_info.mem_rss_kb;
    }
    if (summary_info.mem_pss_kb >= 0 &&
        summary_info.mem_pss_kb > peak.peak_pss_kb) {
        peak.peak_pss_kb = summary_info.mem_pss_kb;
    }
    summary_info.mem_peak_rss_kb = peak.peak_rss_kb;
    summary_info.mem_peak_pss_kb = peak.peak_pss_kb;
    summary_info.sample_count = peak.samples;
}

CollectDataParseFunc CPUMemCollector::get_writer_parser(const std::string& writer_type){
    CollectDataParseFunc func = nullptr;
    spdlog::trace("CPUMemCollector: get_writer_parser for writer_type: {}", writer_type);
    if(writer_type.compare("ESWriter") == 0){
        func = [this](std::any data)->std::any{
            nlohmann::json ret;
            if(data.has_value() == false) {
                spdlog::warn("CPUMemCollector: error writer parser, empty data");
                ret["error"] = "empty data";
                return ret;
            }
            ret["process_data"] = nlohmann::json::array();
            const auto& parsed = std::any_cast<const std::vector<CPUMemInfo>&>(data);
            spdlog::trace("CPUMemCollector: parsing data for ESWriter, data length={}", parsed.size());
            for (const auto& info : parsed) {
                
                nlohmann::json j = ToJson(info);
                if (spdlog::should_log(spdlog::level::trace)) {
                    spdlog::trace("CPUMemCollector: parsed data: {}", j.dump());
                }

                if(info.pid == 0){
                    if (summary) ret["summary"] = std::move(j);
                }else{
                    ret["process_data"].push_back(std::move(j));
                }
                
            }
            return ret;
        };
    }
    if(writer_type.compare("FileWriter") == 0){
        func = [this](std::any data)->std::any{
            nlohmann::json ret;
            if(data.has_value() == false) {
                spdlog::warn("CPUMemCollector: error FileWriter parser, empty data");
                ret["error"] = "empty data";
                return ret.dump() + "\n";
            }
            ret["process_data"] = nlohmann::json::array();
            const auto& parsed = std::any_cast<const std::vector<CPUMemInfo>&>(data);
            spdlog::trace("CPUMemCollector: parsing data for FileWriter, data length={}", parsed.size());
            for (const auto& info : parsed) {
                nlohmann::json j = ToJson(info);
                if(info.pid == 0){
                    ret["summary"] = std::move(j);
                }else{
                    ret["process_data"].push_back(std::move(j));
                }
            }
            return ret.dump() + "\n";
        };
    }
    if(writer_type.compare("PrometheusExporterWriter") == 0){
        func = [this](std::any data)->std::any{
            PrometheusExporterWriter::prometheus_job_state ret;
            if(data.has_value() == false) {
                spdlog::warn("CPUMemCollector: error writer parser, empty data");
                ret.JobID = 0;
                return ret;
            }
            if (summary){
                spdlog::trace("PrometheusExporterWriter Parser in summary mode");
            }
            const auto& parsed = std::any_cast<const std::vector<CPUMemInfo>&>(data);
            for (const auto& info : parsed) {
                PrometheusExporterWriter::prometheus_process_state state;
                spdlog::trace("CollectDataParseFunc CPUMemCollector parse pid: {}", info.pid);
                state.pid = info.pid;
                state.cpu_usage_percent = info.cpuPercent;
                state.threads_cnt = info.numThreads;
                state.mem_rss_kb = info.mem_rss_kb;
                state.mem_usage_percent = info.memoryPercent;
                state.mem_vm_kb = info.mem_vm_kb;
                state.mem_pss_kb = info.mem_pss_kb;
                state.name = info.name;
                ret.processes_state.push_back(state);
            }
            return ret;
        };
    }
    return func;
}

CollectDataParseFuncV2 CPUMemCollector::get_writer_parser_v2(const std::string& writer_type) {
    // 只有 FileWriter 提供原生 V2 parser — 其他 writer 类型回退到 V1 默认适配器
    if (writer_type == "FileWriter") {
        return [this](const WriterParseContext& ctx, std::any data) -> std::any {
            nlohmann::json ret;
            if (!data.has_value()) {
                spdlog::warn("CPUMemCollector: V2 FileWriter parser, empty data");
                ret["error"] = "empty data";
                return ret.dump() + "\n";
            }
            ret["process_data"] = nlohmann::json::array();
            const auto& parsed = std::any_cast<const std::vector<CPUMemInfo>&>(data);
            spdlog::trace("CPUMemCollector: V2 parsing data for FileWriter, data length={}", parsed.size());
            for (const auto& info : parsed) {
                nlohmann::json j = ToJson(info);
                if (info.pid == 0) {
                    ret["summary"] = std::move(j);
                } else {
                    ret["process_data"].push_back(std::move(j));
                }
            }
            // V2 上下文信息注入 — 作为概念验证，在输出中附加 writer/collector 上下文
            ret["_writer_name"] = ctx.writer_name;
            ret["_writer_config_name"] = ctx.writer_config_name;
            ret["_collector_name"] = ctx.collector_name;
            ret["_job_id"] = ctx.job.JobID;
            ret["_timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                ctx.timestamp.time_since_epoch()).count();
            return ret.dump() + "\n";
        };
    }
    // ESWriter, PrometheusExporterWriter 等: 返回 nullptr，让 ICollector 默认适配器回退到 V1
    return nullptr;
}
