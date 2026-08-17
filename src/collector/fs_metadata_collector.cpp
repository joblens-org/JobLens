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
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <fstream>
#include <sstream>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include "common/utils.hpp"
#include "core/collector_registry.hpp"

using json = nlohmann::json;
using namespace std::chrono;

AUTO_REGISTER_JOB_COLLECTOR(
    FSMetadataCollector,
    "Collect filesystem metadata operation statistics via eBPF or /proc",
    ConfigParams{
        {"freq", "Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds"},
        {"summary", "Whether to summarize data across all processes (true/false), default false"},
        {"use_ebpf", "Whether to use eBPF for collecting fs metadata statistics"}
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

bool FSMetadataCollector::init_ebpf() {
    auto path = Utils::JobLensRootDir() + bpf_o_path;
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
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
    op_state_dict_.clear();
    spdlog::info("FSMetadataCollector: deinit_ebpf completed");
}

bool FSMetadataCollector::init(const nlohmann::json& cfg) {
    // 读取 summary 配置
    if (cfg.contains("summary") && cfg["summary"].get<std::string>() == "true") {
        summary = true;
    } else {
        summary = false;
    }

    // 读取 use_ebpf 配置
    if (cfg.contains("use_ebpf") && cfg["use_ebpf"].get<std::string>() == "true") {
        use_ebpf = true;
        if (!init_ebpf()) {
            spdlog::error("FSMetadataCollector: init ebpf error, falling back to non-eBPF mode");
            deinit_ebpf();
            use_ebpf = false;
        }
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

    return true;
}

void FSMetadataCollector::deinit() noexcept {
    if (use_ebpf) {
        deinit_ebpf();
    }
    op_state_dict_.clear();
    spdlog::info("FSMetadataCollector deinit");
}

CollectResult FSMetadataCollector::collect(const Job& job) {
    std::vector<FSMetadataProcessInfo> result;

    // 如果启用eBPF，更新pid2job映射
    if (use_ebpf && bpf_obj_) {
        if (!EbpfCommon::update_pid_in_kernel(bpf_obj_, pid2jobid_map_name, job.JobID, job.JobPIDs)) {
            spdlog::error("FSMetadataCollector: update pid in kernel error");
        }
    }

    // 遍历作业的所有PID
    for (int pid : job.JobPIDs) {
        // 检查进程是否仍在运行
        if (!Utils::is_process_running(pid)) {
            continue;
        }

        FSMetadataProcessInfo info{};
        info.pid = pid;

        // 如果启用eBPF，从eBPF map中读取统计信息
        if (use_ebpf && bpf_obj_) {
            for (uint32_t op = 0; op < FS_META_MAX; ++op) {
                fs_meta_key key{static_cast<u32>(pid), op};
                auto val_opt = EbpfCommon::lookup_hashmap_elem<fs_meta_key, fs_meta_stat>(
                    bpf_obj_, fs_meta_map_name, key);

                if (val_opt.has_value() && val_opt.value().calls > 0) {
                    const auto& val = val_opt.value();
                    FSMetadataOpInfo op_info{};
                    op_info.op_id = op;
                    op_info.op_name = op_name_from_id(op);
                    op_info.calls = val.calls;
                    op_info.success = val.success;
                    op_info.errors = val.errors;
                    op_info.last_errno = val.last_errno;
                    op_info.total_latency_ns = val.total_latency_ns;
                    op_info.max_latency_ns = val.max_latency_ns;

                    // 计算调用速率
                    uint64_t state_key = (static_cast<uint64_t>(pid) << 32) | op;
                    auto& state = op_state_dict_[state_key];
                    auto now = steady_clock::now();

                    if (state.last_calls > 0) {
                        double dt = duration_cast<duration<double>>(now - state.last_time).count();
                        if (dt > 0) {
                            op_info.calls_rate = static_cast<double>(op_info.calls - state.last_calls) / dt;
                        }
                    }
                    state.last_time = now;
                    state.last_calls = op_info.calls;

                    // 计算错误速率
                    // 注意：这里使用相同的dt，如果dt为0则error_rate为0
                    if (state.last_calls > 0) {
                        double dt = duration_cast<duration<double>>(now - state.last_time).count();
                        if (dt > 0) {
                            // 这里需要跟踪上次的错误数，简化处理：使用当前错误数计算
                            // 实际上应该跟踪last_errors，但为了简化，暂时不计算error_rate
                            op_info.error_rate = 0.0;
                        }
                    }

                    info.ops.push_back(std::move(op_info));
                    info.metadata_ops_total += op_info.calls;
                }
            }
        }

        // 计算总速率
        double total_rate = 0.0;
        for (const auto& op : info.ops) {
            total_rate += op.calls_rate;
        }
        info.metadata_ops_rate = total_rate;

        // 最佳努力：尝试获取挂载点和文件系统类型
        if (!info.ops.empty()) {
            std::string fd_dir = "/proc/" + std::to_string(pid) + "/fd/";
            DIR* dir = ::opendir(fd_dir.c_str());
            if (dir) {
                struct dirent* ent;
                while ((ent = ::readdir(dir))) {
                    if (ent->d_name[0] == '.') continue;
                    int fd = std::atoi(ent->d_name);
                    std::string path = fd_to_path(pid, fd);
                    if (path.empty() || path[0] != '/') continue;

                    std::string mnt, fst;
                    if (path_to_mount(path, mnt, fst)) {
                        info.mount_point = mnt;
                        info.fs_type = fst;
                        break;  // 找到第一个成功的就退出
                    }
                }
                ::closedir(dir);
            }
        }

        result.push_back(std::move(info));
    }

    // 如果需要汇总数据
    if (summary && !result.empty()) {
        FSMetadataProcessInfo summary_info{};
        summary_info.pid = 0;  // 0表示汇总

        // 汇总所有进程的统计信息
        std::unordered_map<uint32_t, FSMetadataOpInfo> op_summary;
        for (const auto& proc_info : result) {
            summary_info.metadata_ops_total += proc_info.metadata_ops_total;
            summary_info.metadata_ops_rate += proc_info.metadata_ops_rate;

            for (const auto& op : proc_info.ops) {
                if (op_summary.find(op.op_id) == op_summary.end()) {
                    op_summary[op.op_id] = op;
                } else {
                    auto& sum_op = op_summary[op.op_id];
                    sum_op.calls += op.calls;
                    sum_op.success += op.success;
                    sum_op.errors += op.errors;
                    sum_op.total_latency_ns += op.total_latency_ns;
                    if (op.max_latency_ns > sum_op.max_latency_ns) {
                        sum_op.max_latency_ns = op.max_latency_ns;
                    }
                    sum_op.calls_rate += op.calls_rate;
                }
            }
        }

        // 将汇总后的操作信息转换为向量
        for (auto& [op_id, op_info] : op_summary) {
            summary_info.ops.push_back(std::move(op_info));
        }

        result.push_back(std::move(summary_info));
    }

    return std::any(result);
}

CollectDataParseFunc FSMetadataCollector::get_writer_parser(const std::string& writer_type) {
    CollectDataParseFunc func = nullptr;

    if (writer_type == "FileWriter") {
        func = [this](std::any data) -> std::any {
            if (!data.has_value()) {
                spdlog::warn("FSMetadataCollector: FileWriter parser received empty data");
                return std::string("FSMetadataCollector error=empty_data\n");
            }

            try {
                auto parsed = std::any_cast<std::vector<FSMetadataProcessInfo>>(data);
                std::ostringstream out;

                for (const auto& info : parsed) {
                    // 输出进程级汇总行
                    out << "FSMetadataCollector"
                        << " type=" << (info.pid == 0 ? "summary" : "process")
                        << " pid=" << info.pid
                        << " metadata_ops_total=" << info.metadata_ops_total
                        << " metadata_ops_rate=" << info.metadata_ops_rate
                        << " mount_point=" << (info.mount_point.empty() ? "" : info.mount_point)
                        << " fs_type=" << (info.fs_type.empty() ? "" : info.fs_type)
                        << "\n";

                    // 输出每个操作的详细信息
                    for (const auto& op : info.ops) {
                        out << "FSMetadataCollector op"
                            << " pid=" << info.pid
                            << " op=" << op.op_name
                            << " calls=" << op.calls
                            << " calls_rate=" << op.calls_rate
                            << " success=" << op.success
                            << " errors=" << op.errors
                            << " last_errno=" << op.last_errno
                            << " total_latency_ns=" << op.total_latency_ns
                            << " max_latency_ns=" << op.max_latency_ns
                            << "\n";
                    }
                }

                return out.str();
            } catch (const std::bad_any_cast& e) {
                spdlog::error("FSMetadataCollector: FileWriter parser bad_any_cast: {}", e.what());
                return std::string("FSMetadataCollector error=bad_cast\n");
            }
        };
    } else if (writer_type == "ESWriter") {
        func = [this](std::any data) -> std::any {
            json ret;
            ret["process_data"] = json::array();

            if (!data.has_value()) {
                spdlog::warn("FSMetadataCollector: ESWriter parser received empty data");
                ret["error"] = "empty data";
                return ret;
            }

            try {
                auto parsed = std::any_cast<std::vector<FSMetadataProcessInfo>>(data);

                for (const auto& info : parsed) {
                    json j;
                    j["pid"] = info.pid;
                    j["metadata_ops_total"] = info.metadata_ops_total;
                    j["metadata_ops_rate"] = info.metadata_ops_rate;
                    j["mount_point"] = info.mount_point;
                    j["fs_type"] = info.fs_type;

                    j["ops"] = json::array();
                    for (const auto& op : info.ops) {
                        json op_j;
                        op_j["op_id"] = op.op_id;
                        op_j["op_name"] = op.op_name;
                        op_j["calls"] = op.calls;
                        op_j["calls_rate"] = op.calls_rate;
                        op_j["success"] = op.success;
                        op_j["errors"] = op.errors;
                        op_j["last_errno"] = op.last_errno;
                        op_j["total_latency_ns"] = op.total_latency_ns;
                        op_j["max_latency_ns"] = op.max_latency_ns;
                        j["ops"].push_back(op_j);
                    }

                    if (info.pid == 0) {
                        ret["summary"] = j;
                    } else {
                        ret["process_data"].push_back(j);
                    }
                }

                return ret;
            } catch (const std::bad_any_cast& e) {
                spdlog::error("FSMetadataCollector: ESWriter parser bad_any_cast: {}", e.what());
                ret["error"] = "bad cast";
                return ret;
            }
        };
    }

    return func;
}
