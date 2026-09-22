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
#include "collector/basic_info_collector.hpp"
#include <memory>
#include <cstdlib>
#include <cerrno>
#include <algorithm>
#include "core/collector_registry.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include <fstream>
#include <sstream>
#include <fmt/format.h>
#include <unistd.h>
#include <poll.h>
#include <linux/genetlink.h>
#include <netlink/netlink.h>
#include <netlink/genl/genl.h>
#include <netlink/genl/ctrl.h>

AUTO_REGISTER_JOB_COLLECTOR(
    BasicInfoCollector,
    "Collect CPU, memory and IO statistics using taskstats interface",
    ConfigParams{
        {"freq", "Sampling frequency in Hz, default 0.2"},
        {"summary", "Whether to summarize data across all processes (true/false), default false"}
    }
)


bool BasicInfoCollector::init(const nlohmann::json& cfg) {
    if (inited) {
        spdlog::warn("BasicInfoCollector init twice with config: {}", cfg.dump());
        return false;
    }
    
    spdlog::info("BasicInfoCollector init with config: {}", cfg.dump());
    
    // 解析配置
    if (cfg.contains("summary") && cfg["summary"].get<std::string>() == "true") {
        summary = true;
    } else {
        summary = false;
    }
    
    // 建立taskstats连接
    if (!connect_to_taskstats()) {
        spdlog::error("Failed to connect to taskstats interface");
        return false;
    }
    nl_socket_disable_auto_ack(nl_sock);
    // 获取物理内存总量
    totalMemoryBytes = get_total_memory_bytes() * 1024; // 转换为字节
    
    inited = true;
    return true;
}


void BasicInfoCollector::deinit() noexcept {
    if (!inited) {
        return;
    }
    
    disconnect_from_taskstats();
    inited = false;
    spdlog::info("BasicInfoCollector deinit");
}


bool BasicInfoCollector::connect_to_taskstats() {
    std::unique_ptr<struct nl_sock, decltype(&nl_socket_free)> socket(nl_socket_alloc(), nl_socket_free);
    if (!socket || genl_connect(socket.get()) < 0) {
        spdlog::error("Failed to connect to generic netlink");
        return false;
    }
    // Family resolution is done during init. Reconnecting after a timeout can
    // reuse the known ID without a second blocking controller exchange.
    const int resolved_family = family_id >= 0 ? family_id : genl_ctrl_resolve(socket.get(), TASKSTATS_GENL_NAME);
    if (resolved_family < 0 || nl_socket_set_nonblocking(socket.get()) < 0) {
        spdlog::error("Failed to configure nonblocking taskstats connection");
        return false;
    }
    nl_socket_disable_auto_ack(socket.get());
    family_id = resolved_family;
    nl_sock = socket.release();
    return true;
}

void BasicInfoCollector::disconnect_from_taskstats() {
    if (nl_sock) {
        nl_socket_free(nl_sock);
        nl_sock = nullptr;
    }
    family_id = -1;
}


uint64_t BasicInfoCollector::get_total_memory_bytes() {
    std::ifstream meminfo("/proc/meminfo");
    if (!meminfo) return 0;
    
    std::string line;
    while (std::getline(meminfo, line)) {
        if (line.find("MemTotal:") == 0) {
            std::istringstream iss(line);
            std::string key;
            uint64_t value;
            std::string unit;
            iss >> key >> value >> unit;
            return value; // KB
        }
    }
    return 0;
}


namespace {
// The taskstats socket is non-blocking, so netlink calls retry after readiness.
void wait_netlink_fd(int fd, short events) {
    pollfd descriptor{fd, events, 0};
    while (::poll(&descriptor, 1, -1) < 0 && errno == EINTR) {}
}
}

bool BasicInfoCollector::get_taskstats_for_tgid(int tgid, struct taskstats* out_stats) {
    if (!nl_sock && !connect_to_taskstats()) return false;
    if (family_id < 0) return false;

    std::unique_ptr<nl_msg, decltype(&nlmsg_free)> msg(nlmsg_alloc(), nlmsg_free);
    if (!msg || !genlmsg_put(msg.get(), NL_AUTO_PORT, NL_AUTO_SEQ, family_id, 0,
                            0, TASKSTATS_CMD_GET, TASKSTATS_GENL_VERSION) ||
        nla_put_s32(msg.get(), TASKSTATS_CMD_ATTR_PID, tgid) < 0) {
        spdlog::error("BasicInfoCollector: Failed to allocate netlink message");
        return false;
    }
    const int fd = nl_socket_get_fd(nl_sock);
    for (;;) {
        const int sent = nl_send_auto(nl_sock, msg.get());
        if (sent >= 0) break;
        if (sent == -NLE_INTR) continue;
        if (sent == -NLE_AGAIN) {
            wait_netlink_fd(fd, POLLOUT);
            continue;
        }
        spdlog::error("BasicInfoCollector: Failed to send netlink message");
        return false;
    }

    for (;;) {
        unsigned char* reply = nullptr;
        sockaddr_nl address{};
        const int received = nl_recv(nl_sock, &address, &reply, nullptr);
        std::unique_ptr<unsigned char, decltype(&std::free)> owned_reply(reply, std::free);
        if (received == -NLE_INTR) continue;
        if (received == -NLE_AGAIN) {
            wait_netlink_fd(fd, POLLIN);
            continue;
        }
        if (received <= 0 || !reply) {
            spdlog::error("BasicInfoCollector: Failed to receive netlink reply for TGID {}, error code {}", tgid, received);
            return false;
        }
        int remaining = received;
        for (auto* h = reinterpret_cast<nlmsghdr*>(reply); NLMSG_OK(h, remaining);
             h = NLMSG_NEXT(h, remaining)) {
            if (h->nlmsg_seq != nlmsg_hdr(msg.get())->nlmsg_seq) continue;
            if (h->nlmsg_type == NLMSG_ERROR) {
                if (h->nlmsg_len < NLMSG_LENGTH(sizeof(nlmsgerr))) return false;
                const auto* error = static_cast<const nlmsgerr*>(NLMSG_DATA(h));
                if (!error->error) continue;
                spdlog::error("BasicInfoCollector: Netlink error {} for TGID {}", error->error, tgid);
                return false;
            }
            nlattr* attrs[TASKSTATS_TYPE_MAX + 1]{};
            if (genlmsg_parse(h, 0, attrs, TASKSTATS_TYPE_MAX, nullptr) < 0 ||
                !attrs[TASKSTATS_TYPE_AGGR_PID]) return false;
            nlattr* task_attrs[TASKSTATS_TYPE_MAX + 1]{};
            if (nla_parse_nested(task_attrs, TASKSTATS_TYPE_MAX,
                                 attrs[TASKSTATS_TYPE_AGGR_PID], nullptr) < 0 ||
                !task_attrs[TASKSTATS_TYPE_STATS]) return false;
            std::memset(out_stats, 0, sizeof(*out_stats));
            std::memcpy(out_stats, nla_data(task_attrs[TASKSTATS_TYPE_STATS]),
                        std::min<size_t>(sizeof(*out_stats), nla_len(task_attrs[TASKSTATS_TYPE_STATS])));
            return true;
        }
    }
}

void BasicInfoCollector::calculate_cpu_percent(BasicInfo& info, const struct taskstats& stats,
                                              const std::chrono::steady_clock::time_point& now) {
    // 从taskstats获取CPU时间(u秒)
    info.cpuUserNs = stats.ac_utime;
    info.cpuSystemNs = stats.ac_stime;
    info.cpuTotalNs = info.cpuUserNs + info.cpuSystemNs;
    
    auto& state = pid_state_dict[info.pid];
    auto timeElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        now - state.lastTime).count();
    
    if (timeElapsed > 0 && state.lastTime.time_since_epoch().count() > 0) {
        uint64_t cpuDiff = info.cpuTotalNs - (state.lastCpuUserNs + state.lastCpuSystemNs);
        info.cpuPercent = 100.0 * static_cast<double>(cpuDiff) / 
                          static_cast<double>(timeElapsed);
    } else {
        info.cpuPercent = 0.0;
    }
    
    // 更新状态
    state.lastCpuUserNs = info.cpuUserNs;
    state.lastCpuSystemNs = info.cpuSystemNs;
    state.lastTime = now;
}



void BasicInfoCollector::calculate_memory_percent(BasicInfo& info, 
                                                 const struct taskstats& stats) {
    info.memRssBytes = stats.coremem * 1024; // KB to bytes
    info.memVmBytes = stats.virtmem * 1024; // KB to bytes
    
    if (totalMemoryBytes > 0) {
        info.memoryPercent = 100.0 * static_cast<double>(info.memRssBytes) / 
                             static_cast<double>(totalMemoryBytes);
    } else {
        info.memoryPercent = 0.0;
    }
}


void BasicInfoCollector::calculate_io_speed(BasicInfo& info, const struct taskstats& stats,
                                           const std::chrono::steady_clock::time_point& now) {
    info.readBytes = stats.read_bytes;
    info.writeBytes = stats.write_bytes;
    info.readOps = stats.read_syscalls;
    info.writeOps = stats.write_syscalls;
    
    auto& state = pid_state_dict[info.pid];
    auto timeElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - state.lastTime).count() / 1000.0; // 转换为秒
    
    if (timeElapsed > 0 && state.lastTime.time_since_epoch().count() > 0) {
        info.readSpeed = static_cast<double>(info.readBytes - state.lastReadBytes) / timeElapsed;
        info.writeSpeed = static_cast<double>(info.writeBytes - state.lastWriteBytes) / timeElapsed;
    } else {
        info.readSpeed = 0.0;
        info.writeSpeed = 0.0;
    }
    
    // 更新状态
    state.lastReadBytes = info.readBytes;
    state.lastWriteBytes = info.writeBytes;
    state.lastReadOps = info.readOps;
    state.lastWriteOps = info.writeOps;
}


CollectResult BasicInfoCollector::collect(const Job& job) {
    if (!inited) {
        spdlog::error("BasicInfoCollector not initialized");
        return {};
    }
    
    std::vector<BasicInfo> infos;
    auto now = std::chrono::steady_clock::now();
    
    // 如果summary为true且只有一个PID，尝试获取TGID汇总
    // if (summary && job.JobPIDs.size() > 1) {
    //     // 使用第一个PID作为TGID尝试获取汇总
    //     int tgid = job.JobPIDs[0];
    //     struct taskstats stats;
        
    //     if (get_taskstats_for_tgid(tgid, stats)) {
    //         BasicInfo info;
    //         info.pid = 0; // 汇总信息的特殊标记
    //         info.name = "JOB-SUMMARY";
            
    //         calculate_cpu_percent(info, stats, now);
    //         calculate_io_speed(info, stats, now);
    //         calculate_memory_percent(info, stats);
            
    //         info.numThreads = stats.nvcsw + stats.nivcsw; // 近似值
    //         info.voluntaryCtxSw = stats.nvcsw;
    //         info.nonvoluntaryCtxSw = stats.nivcsw;
            
    //         infos.push_back(info);
    //         return infos;
    //     }
    // }
    
    // 逐个进程采集
    for (int pid : job.JobPIDs) {
        if (pid <= 0) continue;
        
        struct taskstats stats;
        if (!get_taskstats_for_tgid(pid, &stats)) {
            spdlog::debug("Failed to get taskstats for pid {}", pid);
            continue;
        }
        
        BasicInfo info;
        info.pid = pid;
        
        // 获取基本信息
        info.name = std::string(stats.ac_comm);
        // 计算各项指标
        calculate_cpu_percent(info, stats, now);
        calculate_io_speed(info, stats, now);
        calculate_memory_percent(info, stats);
        
        // 其他统计
        info.voluntaryCtxSw = stats.nvcsw;
        info.nonvoluntaryCtxSw = stats.nivcsw;

        spdlog::trace("BasicInfoCollector: Collected for name {} PID {}: CPU% {:.2f}, MEM% {:.2f}, ReadSpeed {:.2f} B/s, WriteSpeed {:.2f} B/s",
                      stats.ac_comm, pid, info.cpuPercent, info.memoryPercent, info.readSpeed, info.writeSpeed);
        
        infos.push_back(info);
    }
    
    spdlog::debug("BasicInfoCollector: collected {} entries for job {}", 
                  infos.size(), job.JobID);
    
    return infos;
}


CollectDataParseFunc BasicInfoCollector::get_writer_parser(const std::string& writer_type) {
    CollectDataParseFunc func = nullptr;
    spdlog::trace("BasicInfoCollector: get_writer_parser for writer_type: {}", writer_type);
    
    if (writer_type.compare("ESWriter") == 0) {
        func = [this](std::any data) -> std::any {
            nlohmann::json ret;
            if (!data.has_value()) {
                spdlog::warn("BasicInfoCollector: empty data");
                ret["error"] = "empty data";
                return ret;
            }
            
            ret["process_data"] = nlohmann::json::array();
            const auto& parsed = std::any_cast<const std::vector<BasicInfo>&>(data);
            
            spdlog::trace("BasicInfoCollector: parsing {} entries for ESWriter", parsed.size());
            
            for (const auto& info : parsed) {
                nlohmann::json j;
                j["pid"] = info.pid;
                j["name"] = info.name;
                j["timestamp"] = std::chrono::system_clock::now().time_since_epoch().count();
                
                // CPU信息
                j["cpu_percent"] = info.cpuPercent;
                j["cpu_user_ns"] = info.cpuUserNs;
                j["cpu_system_ns"] = info.cpuSystemNs;
                j["cpu_total_ns"] = info.cpuTotalNs;
                
                // 内存信息
                j["mem_rss_bytes"] = info.memRssBytes;
                j["mem_vm_bytes"] = info.memVmBytes;
                j["mem_percent"] = info.memoryPercent;
                
                // IO信息
                j["io_read_speed_bps"] = info.readSpeed;
                j["io_write_speed_bps"] = info.writeSpeed;
                j["io_read_bytes_total"] = info.readBytes;
                j["io_write_bytes_total"] = info.writeBytes;
                j["io_read_ops_total"] = info.readOps;
                j["io_write_ops_total"] = info.writeOps;
                
                // 其他
                j["num_threads"] = info.numThreads;
                j["ctx_sw_voluntary"] = info.voluntaryCtxSw;
                j["ctx_sw_nonvoluntary"] = info.nonvoluntaryCtxSw;
                
                if (info.pid == 0) {
                    ret["summary"] = std::move(j);
                } else {
                    ret["process_data"].push_back(std::move(j));
                }
            }
            
            return ret;
        };
    }
    
    if (writer_type.compare("FileWriter") == 0) {
        func = [this](std::any data) -> std::any {
            if (!data.has_value()) {
                spdlog::warn("BasicInfoCollector: error FileWriter parser, empty data");
                return std::string("BasicInfoCollector error=empty_data\n");
            }
            const auto& parsed = std::any_cast<const std::vector<BasicInfo>&>(data);
            std::ostringstream out;
            for (const auto& info : parsed) {
                out << "BasicInfoCollector"
                    << " type=" << (info.pid == 0 ? "summary" : "process")
                    << " pid=" << info.pid
                    << " name=" << info.name
                    << " cpu_percent=" << info.cpuPercent
                    << " cpu_user_ns=" << info.cpuUserNs
                    << " cpu_system_ns=" << info.cpuSystemNs
                    << " cpu_total_ns=" << info.cpuTotalNs
                    << " mem_rss_bytes=" << info.memRssBytes
                    << " mem_vm_bytes=" << info.memVmBytes
                    << " mem_percent=" << info.memoryPercent
                    << " io_read_speed_bps=" << info.readSpeed
                    << " io_write_speed_bps=" << info.writeSpeed
                    << " io_read_bytes_total=" << info.readBytes
                    << " io_write_bytes_total=" << info.writeBytes
                    << " io_read_ops_total=" << info.readOps
                    << " io_write_ops_total=" << info.writeOps
                    << " num_threads=" << info.numThreads
                    << " ctx_sw_voluntary=" << info.voluntaryCtxSw
                    << " ctx_sw_nonvoluntary=" << info.nonvoluntaryCtxSw
                    << '\n';
            }
            return out.str();
        };
    }
    
    if (writer_type.compare("PrometheusExporterWriter") == 0) {
        func = [this](std::any data) -> std::any {
            PrometheusExporterWriter::prometheus_job_state ret;
            if (!data.has_value()) {
                spdlog::warn("BasicInfoCollector: empty data");
                ret.JobID = 0;
                return ret;
            }
            
            const auto& parsed = std::any_cast<const std::vector<BasicInfo>&>(data);
            
            for (const auto& info : parsed) {
                PrometheusExporterWriter::prometheus_process_state state;
                state.pid = info.pid;
                state.cpu_usage_percent = info.cpuPercent;
                state.threads_cnt = info.numThreads;
                state.mem_rss_kb = info.memRssBytes / 1024; // 转换为KB
                state.mem_usage_percent = info.memoryPercent;
                state.mem_vm_kb = info.memVmBytes / 1024;
                
                // 扩展Prometheus结构以支持IO指标
                // 需要在prometheus_exporter_writer.hpp中定义这些字段
                ret.processes_state.push_back(state);
                
                // 如果需要，可以添加额外的metric
                // 例如: ret.io_read_speed_bps = info.readSpeed;
            }
            
            return ret;
        };
    }
    
    return func;
}
