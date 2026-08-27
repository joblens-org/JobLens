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
#include "collector/fs_metadata_collector.hpp"
#include "core/collector_registry.hpp"
#include "common/utils.hpp"
#include "common/ebpf_common.hpp"
#include "ebpf/job_pid_track.h"
#include "writer/prometheus_exporter_writer.hpp"
#include <nlohmann/json.hpp>
#include <sstream>
#include <fstream>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>

using json = nlohmann::json;
using namespace std::chrono;

AUTO_REGISTER_JOB_COLLECTOR(
    FSMetadataCollector,
    "Collect filesystem metadata operation statistics via eBPF (job-level + latency buckets)",
    ConfigParams{
        {"freq", "Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds"},
        {"summary", "Whether to summarize data across all processes (true/false), default false"}
    }
)

// 静态辅助函数：将操作ID转换为操作名称
static std::string op_name_from_id(uint32_t op_id) {
    switch (op_id) {
        case FS_META_OPEN:        return "open";
        case FS_META_CLOSE:       return "close";
        case FS_META_GETATTR:     return "getattr";
        case FS_META_READDIR:     return "readdir";
        case FS_META_CREATE:      return "create";
        case FS_META_MKDIR:       return "mkdir";
        case FS_META_MKNOD:       return "mknod";
        case FS_META_UNLINK:      return "unlink";
        case FS_META_RMDIR:       return "rmdir";
        case FS_META_RENAME:      return "rename";
        case FS_META_LINK:        return "link";
        case FS_META_SYMLINK:     return "symlink";
        case FS_META_READLINK:    return "readlink";
        case FS_META_SETATTR:     return "setattr";
        case FS_META_GETXATTR:    return "getxattr";
        case FS_META_SETXATTR:    return "setxattr";
        case FS_META_LISTXATTR:   return "listxattr";
        case FS_META_REMOVEXATTR: return "removexattr";
        case FS_META_STATFS:      return "statfs";
        case FS_META_SYNC:        return "sync";
        case FS_META_FSYNC:       return "fsync";
        default:                  return "unknown";
    }
}

// 静态辅助函数：将fd转换为绝对路径
static std::string fd_to_path(int pid, int fd) {
    char buf[512];
    std::string p = "/proc/" + std::to_string(pid) + "/fd/" + std::to_string(fd);
    ssize_t n = readlink(p.c_str(), buf, sizeof(buf) - 1);
    if (n < 0) return {};
    buf[n] = '\0';
    return std::string(buf);
}

// 静态辅助函数：根据绝对路径查找挂载点和文件系统类型
static bool path_to_mount(const std::string& abs,
                          std::string& mnt,
                          std::string& fst) {
    /* 缓存项 */
    struct MountEntry {
        std::string mntpoint;
        std::string fstype;
        bool operator<(const MountEntry& rhs) const {
            return mntpoint.size() > rhs.mntpoint.size(); // 长的放前面
        }
    };
    /* 静态缓存：解析后的挂载表 + 时间戳 */
    static std::vector<MountEntry> s_cache;
    static time_t s_cache_mtime = 0;

    /* 1. 判断是否需要重新加载：缓存空 或 文件更新 */
    struct stat st{};
    if (stat("/proc/self/mountinfo", &st) != 0) return false;      // 文件都打不开
    bool need_reload = s_cache.empty() || st.st_mtime > s_cache_mtime;

    if (need_reload) {
        s_cache.clear();
        std::ifstream ifs("/proc/self/mountinfo");
        if (!ifs) return false;

        std::string line;
        while (std::getline(ifs, line)) {
            std::istringstream iss(line);
            int d1, d2, d3;
            std::string dev, root, mntpoint;
            if (!(iss >> d1 >> d2 >> d3 >> dev >> root >> mntpoint))
                continue;

            // 简单取 fs-type：倒数第二段
            std::string fstype;
            auto pos = line.rfind(' ');
            if (pos != std::string::npos) {
                auto pos2 = line.rfind(' ', pos - 1);
                if (pos2 != std::string::npos)
                    fstype = line.substr(pos2 + 1, pos - pos2 - 1);
            }
            s_cache.push_back({std::move(mntpoint), std::move(fstype)});
        }
        /* 按挂载点长度降序，保证最长前缀先匹配 */
        std::sort(s_cache.begin(), s_cache.end());
        s_cache_mtime = st.st_mtime;
    }

    /* 2. 在缓存里找最长前缀 */
    for (const auto& e : s_cache) {
        if (abs.find(e.mntpoint) == 0) {
            mnt = e.mntpoint;
            fst = e.fstype;
            return true;
        }
    }

    /* 3. 缓存里找不到 -> 视为"失效"，下次重新读（可选） */
    s_cache.clear();   // 强制下次重载
    return false;
}

// 最佳努力：为存活进程反查挂载点 / 文件系统类型
static void fill_proc_mount(pid_t pid, std::string& mnt, std::string& fst) {
    std::string fd_dir = "/proc/" + std::to_string(pid) + "/fd/";
    DIR* dir = ::opendir(fd_dir.c_str());
    if (!dir) return;
    struct dirent* ent;
    while ((ent = ::readdir(dir))) {
        if (ent->d_name[0] == '.') continue;
        int fd = std::atoi(ent->d_name);
        std::string path = fd_to_path(pid, fd);
        if (path.empty() || path[0] != '/') continue;
        std::string m, f;
        if (path_to_mount(path, m, f)) {
            mnt = m;
            fst = f;
            break;  // 找到第一个成功的就退出
        }
    }
    ::closedir(dir);
}

bool FSMetadataCollector::init(const nlohmann::json& cfg) {
    // 读取 summary 配置
    if (cfg.contains("summary") && cfg["summary"].get<std::string>() == "true") {
        summary = true;
    } else {
        summary = false;
    }

    // 读取 freq 配置（采集周期）
    if (cfg.contains("freq")) {
        try {
            collect_period = cfg["freq"].get<double>();
        } catch (const std::exception& e) {
            spdlog::warn("FSMetadataCollector: failed to parse freq, using default 1.0");
            collect_period = 1.0;
        }
    }

    // 默认启用 eBPF（不再提供 use_ebpf 开关）
    if (!init_ebpf()) {
        spdlog::error("FSMetadataCollector: init ebpf error");
        deinit_ebpf();
        return false;
    }
    return true;
}

bool FSMetadataCollector::init_ebpf() {
    auto path = Utils::JobLensRootDir() + bpf_o_path;
    // 复用共享 pinned pid2job / cgroup2job（对标 new_io）
    bpf_obj_ = EbpfCommon::load_bpf_obj_pinned(path, bpf_links_, JOBLENS_BPF_PIN_ROOT);
    if (!bpf_obj_) {
        spdlog::error("FSMetadataCollector: failed to load eBPF object from {}", path);
        return false;
    }
    spdlog::info("FSMetadataCollector: eBPF loaded, {} links attached", bpf_links_.size());
    return true;
}

void FSMetadataCollector::deinit_ebpf() {
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
    bpf_obj_ = nullptr;
}

void FSMetadataCollector::deinit() noexcept {
    deinit_ebpf();
    known_pids_.clear();
    last_proc_op_calls_.clear();
    last_job_op_calls_.clear();
    last_job_time_.clear();
    spdlog::info("FSMetadataCollector deinit");
}

void FSMetadataCollector::refresh_dump_cache_if_needed() {
    auto now = steady_clock::now();
    auto elapsed = duration_cast<milliseconds>(now - last_dump_time_).count();
    if (elapsed < DUMP_TTL_MS && !dump_keys_.empty()) return;

    EbpfCommon::lookup_hashmap_batch<fs_meta_key, fs_meta_stat>(
        bpf_obj_, fs_meta_map_name, dump_keys_, dump_vals_);
    last_dump_time_ = now;
}

// 清理已死且已输出过的短命进程的 eBPF 明细条目（保证"至少输出一次"后延迟清理）。
// pid2job 为共享 map，其删除由内核 exit hook 统一负责，此处不触碰。
void FSMetadataCollector::cleanup_dead_pids() {
    std::vector<pid_t> to_cleanup;
    for (const auto& [pid, st] : known_pids_) {
        if (!st.alive && st.output_count >= 1) {
            to_cleanup.push_back(pid);
        }
    }
    for (pid_t pid : to_cleanup) {
        for (const auto& k : dump_keys_) {
            if (k.pid == static_cast<u32>(pid)) {
                EbpfCommon::delete_hashmap_elem<fs_meta_key, fs_meta_stat>(
                    bpf_obj_, fs_meta_map_name, k);
            }
        }
        known_pids_.erase(pid);
    }
}

CollectResult FSMetadataCollector::collect(const Job& job) {
    JobFSMetaStat result;
    result.job_id = job.JobID;

    // pid2job / cgroup2job 由 JobRegistry + 内核 fork/exit hook 统一维护, 本处只读。

    // 1. Job 级按 op 聚合 + 时延直方图（按 job_id 直接切片）
    for (uint32_t op = 0; op < FS_META_MAX; ++op) {
        // Job 级 op 计数
        fs_meta_job_key jkey{};
        jkey.job_id = job.JobID;
        jkey.op = op;
        auto jval = EbpfCommon::lookup_hashmap_elem<fs_meta_job_key, fs_meta_stat>(
            bpf_obj_, fs_meta_job_map_name, jkey);
        if (jval.has_value() && jval->calls > 0) {
            const auto& v = jval.value();
            FSMetaOpCounters oc{};
            oc.op_id = op;
            oc.op_name = op_name_from_id(op);
            oc.calls = v.calls;
            oc.success = v.success;
            oc.errors = v.errors;
            oc.last_errno = v.last_errno;
            oc.total_latency_ns = v.total_latency_ns;
            oc.max_latency_ns = v.max_latency_ns;
            result.job_ops[op] = oc;
            result.job_metadata_ops_total += v.calls;
        }

        // Job 级各 op 的时延直方图（逐桶查询）
        FSMetaLatencyHist hist{};
        bool any = false;
        for (uint32_t b = 0; b < FS_META_LATENCY_BUCKETS; ++b) {
            fs_meta_latency_key lk{};
            lk.job_id = job.JobID;
            lk.op = op;
            lk.bucket = b;
            if (auto c = EbpfCommon::lookup_hashmap_elem<fs_meta_latency_key, u64>(
                    bpf_obj_, fs_meta_latency_map_name, lk)) {
                hist.hist[b] = *c;
                any = true;
            }
        }
        if (any) {
            result.job_latency[op] = hist;
        }
    }

    // 2. 刷新周期缓存 + 按 Job 切片聚合进程（含短命进程）
    refresh_dump_cache_if_needed();
    for (size_t i = 0; i < dump_keys_.size(); ++i) {
        if (dump_keys_[i].job_id != job.JobID) continue;
        pid_t pid = static_cast<pid_t>(dump_keys_[i].pid);
        uint32_t op = dump_keys_[i].op;
        const fs_meta_stat& s = dump_vals_[i];
        if (s.calls == 0) continue;

        auto& proc = result.processes[pid];
        proc.pid = pid;
        proc.alive = Utils::is_process_running(pid);
        proc.source = proc.alive ? "alive" : "ephemeral";

        FSMetaOpCounters oc{};
        oc.op_id = op;
        oc.op_name = op_name_from_id(op);
        oc.calls = s.calls;
        oc.success = s.success;
        oc.errors = s.errors;
        oc.last_errno = s.last_errno;
        oc.total_latency_ns = s.total_latency_ns;
        oc.max_latency_ns = s.max_latency_ns;
        proc.ops[op] = oc;
        proc.metadata_ops_total += s.calls;
    }

    // 3. 存活进程反查挂载点 / fs 类型（最佳努力）
    for (auto& [pid, proc] : result.processes) {
        if (proc.alive) {
            fill_proc_mount(pid, proc.mount_point, proc.fs_type);
        }
    }

    // 4. 更新短命进程状态 + 延迟清理
    for (const auto& [pid, proc] : result.processes) {
        auto& st = known_pids_[pid];
        st.job_id = job.JobID;
        st.output_count++;
        st.alive = proc.alive;
    }
    cleanup_dead_pids();

    // 5. 速率差分（Job 级 + 进程级各 op）
    auto now = steady_clock::now();
    auto it_time = last_job_time_.find(job.JobID);
    double period = 0.0;
    if (it_time != last_job_time_.end()) {
        period = duration<double>(now - it_time->second).count();
        result.collect_period = period;
    }

    if (period > 0) {
        // Job 级 op 速率
        for (auto& [op, oc] : result.job_ops) {
            auto it = last_job_op_calls_.find(op);
            if (it != last_job_op_calls_.end() && oc.calls >= it->second) {
                oc.calls_rate = static_cast<double>(oc.calls - it->second) / period;
            }
            result.job_metadata_ops_rate += oc.calls_rate;
        }
        // 进程级 op 速率
        for (auto& [pid, proc] : result.processes) {
            for (auto& [op, oc] : proc.ops) {
                uint64_t key = (static_cast<uint64_t>(pid) << 32) | op;
                auto it = last_proc_op_calls_.find(key);
                if (it != last_proc_op_calls_.end() && oc.calls >= it->second) {
                    oc.calls_rate = static_cast<double>(oc.calls - it->second) / period;
                }
                proc.metadata_ops_rate += oc.calls_rate;
            }
        }
    }

    // 6. 更新速率基线
    last_job_time_[job.JobID] = now;
    for (const auto& [op, oc] : result.job_ops) {
        last_job_op_calls_[op] = oc.calls;
    }
    for (const auto& [pid, proc] : result.processes) {
        for (const auto& [op, oc] : proc.ops) {
            uint64_t key = (static_cast<uint64_t>(pid) << 32) | op;
            last_proc_op_calls_[key] = oc.calls;
        }
    }

    return std::any(result);
}

CollectDataParseFunc FSMetadataCollector::get_writer_parser(const std::string& writer_type) {
    if (writer_type == "ESWriter") {
        return [](std::any data) -> std::any {
            json j;
            if (!data.has_value()) {
                j["error"] = "empty data";
                return j;
            }
            try {
                auto s = std::any_cast<JobFSMetaStat>(data);
                j["job_id"] = s.job_id;
                j["collect_period"] = s.collect_period;
                j["job_metadata_ops_total"] = s.job_metadata_ops_total;
                j["job_metadata_ops_rate"] = s.job_metadata_ops_rate;

                j["job_ops"] = json::array();
                for (const auto& [op, oc] : s.job_ops) {
                    json oj;
                    oj["op_id"] = oc.op_id;
                    oj["op_name"] = oc.op_name;
                    oj["calls"] = oc.calls;
                    oj["calls_rate"] = oc.calls_rate;
                    oj["success"] = oc.success;
                    oj["errors"] = oc.errors;
                    oj["last_errno"] = oc.last_errno;
                    oj["total_latency_ns"] = oc.total_latency_ns;
                    oj["max_latency_ns"] = oc.max_latency_ns;
                    auto it = s.job_latency.find(op);
                    if (it != s.job_latency.end()) {
                        oj["latency_hist"] = std::vector<u64>(
                            std::begin(it->second.hist), std::end(it->second.hist));
                    }
                    j["job_ops"].push_back(oj);
                }

                j["processes"] = json::array();
                for (const auto& [pid, p] : s.processes) {
                    json pj;
                    pj["pid"] = p.pid;
                    pj["source"] = p.source;
                    pj["alive"] = p.alive;
                    pj["mount_point"] = p.mount_point;
                    pj["fs_type"] = p.fs_type;
                    pj["metadata_ops_total"] = p.metadata_ops_total;
                    pj["metadata_ops_rate"] = p.metadata_ops_rate;
                    pj["ops"] = json::array();
                    for (const auto& [op, oc] : p.ops) {
                        pj["ops"].push_back({
                            {"op_id", oc.op_id}, {"op_name", oc.op_name},
                            {"calls", oc.calls}, {"calls_rate", oc.calls_rate},
                            {"success", oc.success}, {"errors", oc.errors},
                            {"last_errno", oc.last_errno},
                            {"total_latency_ns", oc.total_latency_ns},
                            {"max_latency_ns", oc.max_latency_ns}
                        });
                    }
                    j["processes"].push_back(pj);
                }
                return j;
            } catch (const std::bad_any_cast& e) {
                spdlog::error("FSMetadataCollector: ESWriter parser bad_any_cast: {}", e.what());
                j["error"] = "bad cast";
                return j;
            }
        };
    }

    if (writer_type == "FileWriter") {
        return [](std::any data) -> std::any {
            if (!data.has_value()) {
                return std::string("FSMetadataCollector error=empty_data\n");
            }
            try {
                auto s = std::any_cast<JobFSMetaStat>(data);
                std::ostringstream out;
                out << "FSMetadataCollector job_id=" << s.job_id
                    << " collect_period=" << s.collect_period
                    << " metadata_ops_total=" << s.job_metadata_ops_total
                    << " metadata_ops_rate=" << s.job_metadata_ops_rate
                    << '\n';

                // Job 级各 op
                for (const auto& [op, oc] : s.job_ops) {
                    out << "FSMetadataCollector job_op"
                        << " job_id=" << s.job_id
                        << " op=" << oc.op_name
                        << " calls=" << oc.calls
                        << " calls_rate=" << oc.calls_rate
                        << " success=" << oc.success
                        << " errors=" << oc.errors
                        << " last_errno=" << oc.last_errno
                        << " total_latency_ns=" << oc.total_latency_ns
                        << " max_latency_ns=" << oc.max_latency_ns
                        << '\n';
                }

                // Job 级各 op 的时延桶（仅输出非零桶）
                for (const auto& [op, hist] : s.job_latency) {
                    for (size_t b = 0; b < FS_META_LATENCY_BUCKETS; ++b) {
                        if (hist.hist[b] != 0) {
                            out << "FSMetadataCollector latency_bucket"
                                << " job_id=" << s.job_id
                                << " op=" << op_name_from_id(op)
                                << " bucket=" << b
                                << " count=" << hist.hist[b]
                                << '\n';
                        }
                    }
                }

                // 进程级各 op
                for (const auto& [pid, p] : s.processes) {
                    for (const auto& [op, oc] : p.ops) {
                        out << "FSMetadataCollector process_op"
                            << " job_id=" << s.job_id
                            << " pid=" << p.pid
                            << " source=" << p.source
                            << " alive=" << p.alive
                            << " mount_point=" << p.mount_point
                            << " fs_type=" << p.fs_type
                            << " op=" << oc.op_name
                            << " calls=" << oc.calls
                            << " calls_rate=" << oc.calls_rate
                            << " success=" << oc.success
                            << " errors=" << oc.errors
                            << " last_errno=" << oc.last_errno
                            << " total_latency_ns=" << oc.total_latency_ns
                            << " max_latency_ns=" << oc.max_latency_ns
                            << '\n';
                    }
                }
                return out.str();
            } catch (const std::bad_any_cast& e) {
                spdlog::error("FSMetadataCollector: FileWriter parser bad_any_cast: {}", e.what());
                return std::string("FSMetadataCollector error=bad_cast\n");
            }
        };
    }

    if (writer_type == "PrometheusExporterWriter") {
        return [](std::any data) -> std::any {
            PrometheusExporterWriter::prometheus_job_state ret;
            if (!data.has_value()) {
                ret.JobID = 0;
                return ret;
            }
            try {
                auto s = std::any_cast<JobFSMetaStat>(data);
                ret.JobID = static_cast<int>(s.job_id);

                // Job 级汇总（pid=0）
                uint64_t job_errors = 0;
                for (const auto& [op, oc] : s.job_ops) {
                    job_errors += oc.errors;
                }
                PrometheusExporterWriter::prometheus_process_state jobstate;
                jobstate.pid = 0;
                jobstate.fs_metadata_ops_total = s.job_metadata_ops_total;
                jobstate.fs_metadata_ops_per_sec = s.job_metadata_ops_rate;
                jobstate.fs_metadata_errors_total = job_errors;
                ret.processes_state.push_back(std::move(jobstate));

                // 进程级
                for (const auto& [pid, p] : s.processes) {
                    uint64_t total_errors = 0;
                    for (const auto& [op, oc] : p.ops) {
                        total_errors += oc.errors;
                    }
                    PrometheusExporterWriter::prometheus_process_state ps;
                    ps.pid = p.pid;
                    ps.fs_metadata_ops_total = p.metadata_ops_total;
                    ps.fs_metadata_ops_per_sec = p.metadata_ops_rate;
                    ps.fs_metadata_errors_total = total_errors;
                    ret.processes_state.push_back(std::move(ps));
                }
                return ret;
            } catch (const std::bad_any_cast& e) {
                spdlog::error("FSMetadataCollector: PrometheusExporterWriter parser bad_any_cast: {}", e.what());
                ret.JobID = 0;
                return ret;
            }
        };
    }

    return nullptr;
}
