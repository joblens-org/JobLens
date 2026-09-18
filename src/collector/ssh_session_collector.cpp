#include "collector/ssh_session_collector.hpp"

#include <dirent.h>
#include <fstream>
#include <sstream>
#include <cstring>
#include <cerrno>
#include <pwd.h>
#include <unistd.h>
#include <filesystem>

#include "core/collector_registry.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include "common/utils.hpp"

AUTO_REGISTER_SYSTEM_COLLECTOR(
    SSHSessionCollector,
    "Collect per-process CPU/memory/IO for each user via /proc scanning. "
    "Ideal for SSH session monitoring: captures every user's processes and their resource usage.",
    ConfigParams{
        {"freq",       "Sampling frequency in Hz, default 0.1 (every 10s)"},
        {"auto_start", "Whether to auto-start on service launch (true/false), default true"},
        {"collect_io", "Whether to collect I/O stats from /proc/pid/io (true/false), default true"}
    }
)

// ==================== 生命周期 ====================

bool SSHSessionCollector::init(const nlohmann::json& cfg) {
    if (inited_) {
        spdlog::warn("SSHSessionCollector: already initialized");
        return false;
    }
    spdlog::info("SSHSessionCollector init with config: {}", cfg.dump());

    collect_io_ = true;
    if (cfg.contains("collect_io") && cfg["collect_io"].get<std::string>() == "false") {
        collect_io_ = false;
    }

    total_phys_mem_kb_ = getTotalPhysMemKB();
    inited_ = true;
    return true;
}

void SSHSessionCollector::deinit() noexcept {
    if (!inited_) return;
    pid_cpu_state_.clear();
    uid_cache_.clear();
    inited_ = false;
    spdlog::info("SSHSessionCollector deinit");
}

// ==================== /proc 读取函数 ====================

long SSHSessionCollector::getTotalPhysMemKB() {
    std::ifstream f("/proc/meminfo");
    if (!f) return 0;
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("MemTotal:", 0) == 0) {
            long kb = 0;
            std::sscanf(line.c_str(), "MemTotal: %ld kB", &kb);
            return kb;
        }
    }
    return 0;
}

std::string SSHSessionCollector::uidToUser(uid_t uid) {
    auto it = uid_cache_.find(uid);
    if (it != uid_cache_.end()) return it->second;

    struct passwd pwbuf;
    struct passwd* pw = nullptr;
    char buf[4096];
    int ret = getpwuid_r(uid, &pwbuf, buf, sizeof(buf), &pw);
    if (ret == 0 && pw) {
        uid_cache_[uid] = pw->pw_name;
        return pw->pw_name;
    }
    return std::to_string(uid);
}

bool SSHSessionCollector::readProcStatus(int pid, SSHProcessInfo& info) {
    std::string path = "/proc/" + std::to_string(pid) + "/status";
    std::ifstream f(path);
    if (!f) return false;

    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("Uid:", 0) == 0) {
            // Uid: 真实uid  有效uid  saved  fs
            uid_t uid;
            std::sscanf(line.c_str(), "Uid:\t%u", &uid);
            info.uid = uid;
            info.user = uidToUser(uid);
        } else if (line.rfind("VmRSS:", 0) == 0) {
            std::sscanf(line.c_str(), "VmRSS:\t%ld", &info.mem_rss_kb);
        } else if (line.rfind("VmSize:", 0) == 0) {
            std::sscanf(line.c_str(), "VmSize:\t%ld", &info.mem_vm_kb);
        } else if (line.rfind("Threads:", 0) == 0) {
            std::sscanf(line.c_str(), "Threads:\t%d", &info.num_threads);
        }
    }
    return true;
}

bool SSHSessionCollector::readProcStat(int pid, SSHProcessInfo& info) {
    std::string path = "/proc/" + std::to_string(pid) + "/stat";
    std::ifstream f(path);
    if (!f) return false;

    std::string line;
    if (!std::getline(f, line)) return false;

    // 跳过 comm 字段（括号可能含空格）
    size_t rpar = line.rfind(')');
    if (rpar == std::string::npos) return false;

    std::istringstream iss(line.substr(rpar + 2));
    char state;
    int ppid;
    unsigned long utime, stime, cutime, cstime;
    long num_threads;
    unsigned long long starttime;

    iss >> state >> ppid;
    // 跳过 pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt
    for (int i = 0; i < 9; ++i) {
        unsigned long dummy;
        iss >> dummy;
    }
    iss >> utime >> stime >> cutime >> cstime;
    // 跳过 priority nice
    long dummy_l;
    iss >> dummy_l >> dummy_l;
    iss >> num_threads;
    // 跳过 itrealvalue
    unsigned long long dummy_ull;
    iss >> dummy_ull;
    iss >> starttime;

    info.ppid = ppid;
    info.state = state;
    info.num_threads = num_threads;
    info.starttime = starttime;

    // --- CPU% delta 计算 ---
    long hz = sysconf(_SC_CLK_TCK);
    long num_cores = sysconf(_SC_NPROCESSORS_ONLN);

    if (hz > 0 && num_cores > 0) {
        // 读系统总 CPU jiffies
        std::ifstream sf("/proc/stat");
        std::string sline;
        std::getline(sf, sline);
        std::istringstream siss(sline);
        std::string cpu_label;
        siss >> cpu_label;
        unsigned long long v, total_cpu = 0;
        while (siss >> v) total_cpu += v;

        unsigned long long proc_cpu = utime + stime + cutime + cstime;
        auto& st = pid_cpu_state_[pid];

        if (st.last_total > 0) {
            unsigned long long delta_total = total_cpu - st.last_total;
            unsigned long long delta_proc  = proc_cpu - st.last_proc;
            if (delta_total > 0) {
                info.cpu_percent = 100.0 * double(delta_proc) / double(delta_total) * num_cores;
            }
        }
        st.last_total = total_cpu;
        st.last_proc  = proc_cpu;
    }

    // 内存占比
    if (total_phys_mem_kb_ > 0 && info.mem_rss_kb > 0) {
        info.mem_percent = 100.0 * double(info.mem_rss_kb) / double(total_phys_mem_kb_);
    }

    return true;
}

bool SSHSessionCollector::readProcComm(int pid, SSHProcessInfo& info) {
    std::string path = "/proc/" + std::to_string(pid) + "/comm";
    std::ifstream f(path);
    if (!f) return false;
    std::getline(f, info.comm);
    // 去掉末尾换行
    if (!info.comm.empty() && info.comm.back() == '\n')
        info.comm.pop_back();
    return true;
}

bool SSHSessionCollector::readProcCmdline(int pid, SSHProcessInfo& info) {
    std::string path = "/proc/" + std::to_string(pid) + "/cmdline";
    std::ifstream f(path, std::ios::binary);
    if (!f) return false;

    std::ostringstream oss;
    char ch;
    bool first = true;
    while (f.get(ch)) {
        if (ch == '\0') {
            if (!first) oss << ' ';
        } else {
            oss << ch;
            first = false;
        }
    }
    info.cmdline = oss.str();
    // 截断到 256 字节
    if (info.cmdline.size() > 256) {
        info.cmdline.resize(256);
        info.cmdline += "...";
    }
    return true;
}

bool SSHSessionCollector::readProcIO(int pid, SSHProcessInfo& info) {
    if (!collect_io_) return true;  // 配置关闭则不读

    std::string path = "/proc/" + std::to_string(pid) + "/io";
    std::ifstream f(path);
    if (!f) return false;  // 无权限或内核线程，静默跳过

    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("read_bytes:", 0) == 0) {
            std::sscanf(line.c_str(), "read_bytes: %llu", &info.io_read_bytes);
        } else if (line.rfind("write_bytes:", 0) == 0) {
            std::sscanf(line.c_str(), "write_bytes: %llu", &info.io_write_bytes);
        }
    }
    return true;
}

// ==================== 采集主逻辑 ====================

CollectResult SSHSessionCollector::collect() {
    std::vector<SSHProcessInfo> results;

    DIR* dir = opendir("/proc");
    if (!dir) {
        spdlog::error("SSHSessionCollector: cannot open /proc");
        return results;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        // 只处理数字目录（PID）
        if (entry->d_type != DT_DIR) continue;
        int pid = 0;
        bool is_digit = true;
        for (int i = 0; entry->d_name[i] != '\0'; ++i) {
            if (!std::isdigit(entry->d_name[i])) { is_digit = false; break; }
            pid = pid * 10 + (entry->d_name[i] - '0');
        }
        if (!is_digit || pid <= 0) continue;

        SSHProcessInfo info;
        info.pid = pid;

        // 读取顺序: comm → status(uid,内存,线程数) → cmdline → stat(ppid,state,CPU) → io
        if (!readProcComm(pid, info)) continue;
        if (!readProcStatus(pid, info)) continue;
        readProcCmdline(pid, info);    // 先读 cmdline，用于过滤内核线程

        // 跳过内核线程：内核线程 cmdline 为空（/proc/pid/cmdline 是 0 字节文件），
        // 普通用户进程（包括 root）一定有 cmdline
        if (info.uid == 0 && info.cmdline.empty()) continue;

        if (!readProcStat(pid, info)) continue;
        readProcIO(pid, info);         // 失败不影响

        results.push_back(std::move(info));
    }
    closedir(dir);

    // 清理已退出进程的 CPU 状态缓存
    for (auto it = pid_cpu_state_.begin(); it != pid_cpu_state_.end(); ) {
        // 如果本次采集中没出现，且上次采集距今超过一定时间，清除
        bool found = false;
        for (const auto& r : results) {
            if (r.pid == it->first) { found = true; break; }
        }
        if (!found)
            it = pid_cpu_state_.erase(it);
        else
            ++it;
    }

    spdlog::debug("SSHSessionCollector: collected {} processes", results.size());
    return results;
}

// ==================== Writer Parser ====================

CollectDataParseFunc SSHSessionCollector::get_writer_parser(const std::string& writer_type) {
    using nlohmann::json;

    if (writer_type == "ESWriter") {
        return [](std::any data) -> std::any {
            std::vector<json> docs;
            if (!data.has_value()) {
                spdlog::warn("SSHSessionCollector: ESWriter parser, empty data");
                return docs;
            }
            auto vec = std::any_cast<std::vector<SSHProcessInfo>>(data);
            for (const auto& p : vec) {
                json j;
                j["pid"]            = p.pid;
                j["ppid"]           = p.ppid;
                j["uid"]            = p.uid;
                j["user"]           = p.user;
                j["comm"]           = p.comm;
                j["cmdline"]        = p.cmdline;
                j["state"]          = std::string(1, p.state);
                j["num_threads"]    = p.num_threads;
                j["cpu_percent"]    = p.cpu_percent;
                j["mem_rss_kb"]     = p.mem_rss_kb;
                j["mem_vm_kb"]      = p.mem_vm_kb;
                j["mem_percent"]    = p.mem_percent;
                j["starttime"]      = p.starttime;
                j["io_read_bytes"]  = p.io_read_bytes;
                j["io_write_bytes"] = p.io_write_bytes;
                docs.push_back(std::move(j));
            }
            return docs;  // std::vector<json> — ESWriter 按数组拆分多文档
        };
    }

    if (writer_type == "FileWriter") {
        return [](std::any data) -> std::any {
            json ret;
            ret["processes"] = json::array();
            if (!data.has_value()) {
                ret["error"] = "empty data";
                return ret.dump() + "\n";
            }
            auto vec = std::any_cast<std::vector<SSHProcessInfo>>(data);
            for (const auto& p : vec) {
                json j;
                j["pid"] = p.pid;
                j["ppid"] = p.ppid;
                j["user"] = p.user;
                j["comm"] = p.comm;
                j["cmdline"] = p.cmdline;
                j["state"] = std::string(1, p.state);
                j["cpu_percent"] = p.cpu_percent;
                j["mem_rss_kb"] = p.mem_rss_kb;
                j["mem_percent"] = p.mem_percent;
                ret["processes"].push_back(j);
            }
            return ret.dump() + "\n";
        };
    }

    if (writer_type == "PrometheusExporterWriter") {
        return [](std::any data) -> std::any {
            PrometheusExporterWriter::prometheus_job_state ret;
            ret.JobID = 0;
            if (!data.has_value()) return ret;

            auto vec = std::any_cast<std::vector<SSHProcessInfo>>(data);
            for (const auto& p : vec) {
                PrometheusExporterWriter::prometheus_process_state state;
                state.pid = p.pid;
                state.cpu_usage_percent = p.cpu_percent;
                state.threads_cnt = p.num_threads;
                state.mem_rss_kb = p.mem_rss_kb;
                state.mem_usage_percent = p.mem_percent;
                state.mem_vm_kb = p.mem_vm_kb;
                state.name = p.comm;
                ret.processes_state.push_back(state);
            }
            return ret;
        };
    }

    return nullptr;
}