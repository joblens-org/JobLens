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
#include <string>
#include <unistd.h>

#include "core/collector_registry.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include "common/utils.hpp"

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
    spdlog::info("CPUMemCollector deinit");
}

// 静态函数：读 /proc/stat 获取系统总 jiffies
static bool read_system_cpu(uint64_t& total)
{
    std::ifstream stat("/proc/stat");
    if (!stat) return false;
    std::string line;
    if (!std::getline(stat, line)) return false;
    std::istringstream iss(line);
    std::string cpu;
    iss >> cpu;                 // "cpu"
    uint64_t user, nice, sys, idle, iowait, irq, softirq, steal;
    iss >> user >> nice >> sys >> idle >> iowait >> irq >> softirq >> steal;
    total = user + nice + sys + idle + iowait + irq + softirq + steal;
    return true;
}

// 静态函数：读 /proc/[pid]/stat 获取进程 jiffies
static bool read_process_cpu(int pid, uint64_t& utime, uint64_t& stime)
{
    std::string path = "/proc/" + std::to_string(pid) + "/stat";
    std::ifstream sf(path);
    if (!sf) return false;
    std::string line;
    if (!std::getline(sf, line)) return false;
    // 找第 14、15 字段（utime, stime）
    // 先跳过第一个括号里的 comm 字段（可能含空格）
    size_t rpar = line.rfind(')');
    if (rpar == std::string::npos) return false;
    std::istringstream iss(line.substr(rpar + 1));
    uint64_t skip;
    iss >> skip; // state
    for (int i = 0; i < 11; ++i) iss >> skip; // 跳过 11 个字段
    iss >> utime >> stime;
    return true;
}

bool CPUMemCollector::CPUOf(int pid, CPUMemInfo& info){
    std::ifstream f(fmt::format("/proc/{}/stat", pid));
    if (!f) return false;

    std::string line, head, tail;
    if (!std::getline(f, line)) return false;

    auto p1 = line.find('(');
    auto p2 = line.rfind(')');
    if (p1 == std::string::npos || p2 == std::string::npos || p2 <= p1)
        return false;

    head = line.substr(0, p1);                 // pid 及之前
    info.name = line.substr(p1 + 1, p2 - p1 - 1);
    tail = line.substr(p2 + 2);                // 跳过 ") "

    std::istringstream iss(tail);
    char state;
    int ppid, pgrp, session, tty_nr, tpgid;
    unsigned flags;
    unsigned long minflt, cminflt, majflt, cmajflt;
    unsigned long utime, stime, cutime, cstime;
    long priority, nice, num_threads, itrealvalue;
    unsigned long long starttime;

    // 顺序把前 20 个字段读掉，我们只用其中 3 个
    iss >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid
        >> flags >> minflt >> cminflt >> majflt >> cmajflt
        >> utime >> stime >> cutime >> cstime
        >> priority >> nice >> num_threads >> itrealvalue >> starttime;

    info.utime = utime;
    info.stime = stime;
    info.starttime = starttime;
    info.ppid = ppid;
    spdlog::debug("CPUOf: pid={} utime={} stime={} starttime={}", pid, info.utime, info.stime, info.starttime);

    /* ---- 计算 CPU 百分比 ---- */
    info.hz = sysconf(_SC_CLK_TCK);      // 每秒 jiffies
    auto numCores = sysconf(_SC_NPROCESSORS_ONLN);
    if (info.hz > 0 && numCores > 0) {
        auto cpuStat = []() -> unsigned long long {   // 取系统总 CPU 时间
            std::ifstream f("/proc/stat");
            std::string line;
            std::getline(f, line);
            std::istringstream iss(line);
            std::string _;
            unsigned long long v, sum = 0;
            iss >> _;                          
            while (iss >> v) sum += v;
            return sum;
        };

        unsigned long long currTotal = cpuStat();
        unsigned long long currProc  = info.utime + info.stime;
        auto& cu = pid_state_dict[pid];
        spdlog::debug("use pid:{}",pid);
        unsigned long long deltaTotal = currTotal - cu.lastTotal;
        unsigned long long deltaProc  = currProc  - cu.lastProc;
        spdlog::debug("deltaTotal={}   deltaProc={}",deltaTotal,deltaProc);
        spdlog::debug("currTotal={}   currProc={}",currTotal,currProc);
        spdlog::debug("lastTotal={}   lastProc={}",cu.lastTotal,cu.lastProc);
        if (deltaTotal > 0) {
            info.cpuPercent = 100.0 * double(deltaProc) / double(deltaTotal) * numCores;
        } else {
            info.cpuPercent = 0.0;
        }

        /* 更新静态缓存（用于下一次采样） */
        cu.lastTotal = currTotal;
        cu.lastProc  = currProc;
        spdlog::debug("update lastTotal={}   lastProc={}",cu.lastTotal,cu.lastProc);
    } else {
        info.cpuPercent = 0.0;
    }
    return true;
}

bool CPUMemCollector::MemOf(int pid,CPUMemInfo& info){
    std::string path = "/proc/" + std::to_string(pid) + "/status";
    std::ifstream sf(path);
    if (!sf){
        spdlog::error("MemOf: cannot open {}", path);
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
        spdlog::error("MemOf: parse /proc/{}/status failed", pid);
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

bool CPUMemCollector::BaseInfo(int pid, CPUMemInfo& info) {
    info.pid = pid;
    info.name = get_process_name(pid);
    return true;
}

CollectResult CPUMemCollector::collect(const Job& job) {
    std::vector<CPUMemInfo> infos;
    for (int pid : job.JobPIDs) {
        if (! Utils::is_process_running(pid)){
            continue;
        }
        CPUMemInfo info;
        if (pid <= 0) continue;
        BaseInfo(pid, info);
        CPUOf(pid, info);
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
    spdlog::debug("CPUMemCollector: get_writer_parser for writer_type: {}", writer_type);
    if(writer_type.compare("ESWriter") == 0){
        func = [this](std::any data)->std::any{
            nlohmann::json ret;
            if(data.has_value() == false) {
                spdlog::warn("CPUMemCollector: error writer parser, empty data");
                ret["error"] = "empty data";
                return ret;
            }
            ret["process_data"] = nlohmann::json::array();
            auto parsed = std::any_cast<std::vector<CPUMemInfo>>(data);
            spdlog::debug("CPUMemCollector: parsing data for ESWriter, data length={}", parsed.size());
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
                spdlog::debug("CPUMemCollector: parsed data: {}", j.dump());

                if(info.pid == 0){
                    if (summary) ret["summary"] = j;
                }else{
                    ret["process_data"].push_back(j);
                }
                
            }
            return ret;
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
                spdlog::debug("PrometheusExporterWriter Parser in summary mode");
            }
            auto parsed = std::any_cast<std::vector<CPUMemInfo>>(data);
            for (const auto& info : parsed) {
                PrometheusExporterWriter::prometheus_process_state state;
                spdlog::debug("CollectDataParseFunc CPUMemCollector parse pid: {}", info.pid);
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