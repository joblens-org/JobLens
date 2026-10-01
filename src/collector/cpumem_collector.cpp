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

    if(cfg.contains("summary") && cfg["summary"].get<std::string>() == "true"){
        summary = true;
    }else{
        summary = false;
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
        const unsigned long long currProc = info.utime + info.stime;
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
    for (int pid : job.JobPIDs) {
        if (! Utils::is_process_running(pid)){
            continue;
        }
        CPUMemInfo info;
        if (pid <= 0) continue;
        info.pid = pid;
        if (!CPUOf(pid, info, sample, now)) info.name = get_process_name(pid);
        MemOf(pid, info);
        if (!info.pid) continue;
        // job.JobInfo[fmt::format("proc_info_{}", pid)] = info.get();
        infos.push_back(info);
    }
    spdlog::debug("CPUMemCollector: collected {} entries for job {}", infos.size(), job.JobID);
    if (summary){
        CPUMemInfo sum_info;
        sum_info.pid = 0;
        sum_info.name = "JOB-SUMMARY";
        auto min_starttime = std::numeric_limits<unsigned long long>::max();
        for(const auto& info: infos){
            sum_info.cpuPercent += info.cpuPercent;
            sum_info.memoryPercent += info.memoryPercent;
            sum_info.utime += info.utime;
            sum_info.stime += info.stime;
            sum_info.mem_vm_kb += info.mem_vm_kb;
            sum_info.mem_rss_kb += info.mem_rss_kb;
            sum_info.mem_swap_kb += info.mem_swap_kb;
            sum_info.mem_peak_rss_kb += info.mem_peak_rss_kb;
            sum_info.numThreads += info.numThreads;
            sum_info.starttime = std::min(sum_info.starttime, info.starttime);
        }
        infos.push_back(sum_info);
    }
    return infos;
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
                
                nlohmann::json j;
                j["pid"] = info.pid;
                j["ppid"] = info.ppid;
                j["name"] = info.name;
                j["cpuPercent"] = info.cpuPercent;
                j["utime"] = info.utime;
                j["stime"] = info.stime;
                j["starttime"] = info.starttime;
                j["hz"] = info.hz;
                j["mem_vm_kb"] = info.mem_vm_kb;
                j["mem_rss_kb"] = info.mem_rss_kb;
                j["mem_swap_kb"] = info.mem_swap_kb;
                j["mem_peak_rss_kb"] = info.mem_peak_rss_kb;
                j["memoryPercent"] = info.memoryPercent;
                j["numThreads"] = info.numThreads;
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
                nlohmann::json j;
                j["pid"] = info.pid;
                j["ppid"] = info.ppid;
                j["name"] = info.name;
                j["cpuPercent"] = info.cpuPercent;
                j["utime"] = info.utime;
                j["stime"] = info.stime;
                j["starttime"] = info.starttime;
                j["hz"] = info.hz;
                j["mem_vm_kb"] = info.mem_vm_kb;
                j["mem_rss_kb"] = info.mem_rss_kb;
                j["mem_swap_kb"] = info.mem_swap_kb;
                j["mem_peak_rss_kb"] = info.mem_peak_rss_kb;
                j["memoryPercent"] = info.memoryPercent;
                j["numThreads"] = info.numThreads;
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
                nlohmann::json j;
                j["pid"] = info.pid;
                j["ppid"] = info.ppid;
                j["name"] = info.name;
                j["cpuPercent"] = info.cpuPercent;
                j["utime"] = info.utime;
                j["stime"] = info.stime;
                j["starttime"] = info.starttime;
                j["hz"] = info.hz;
                j["mem_vm_kb"] = info.mem_vm_kb;
                j["mem_rss_kb"] = info.mem_rss_kb;
                j["mem_swap_kb"] = info.mem_swap_kb;
                j["mem_peak_rss_kb"] = info.mem_peak_rss_kb;
                j["memoryPercent"] = info.memoryPercent;
                j["numThreads"] = info.numThreads;
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
