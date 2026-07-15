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
#include "core/collector_registry.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include <fstream>
#include <sstream>
#include <fmt/format.h>
#include <unistd.h>
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
    // 创建netlink socket
    nl_sock = nl_socket_alloc();
    if (!nl_sock) {
        spdlog::error("Failed to allocate netlink socket");
        return false;
    }
    
    // 连接到GENERIC NETLINK
    if (genl_connect(nl_sock) < 0) {
        spdlog::error("Failed to connect to generic netlink");
        nl_socket_free(nl_sock);
        nl_sock = nullptr;
        return false;
    }
    
    // 获取family ID
    family_id = genl_ctrl_resolve(nl_sock, TASKSTATS_GENL_NAME);
    if (family_id < 0) {
        spdlog::error("Failed to resolve family ID for taskstats");
        disconnect_from_taskstats();
        return false;
    }
    
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


bool BasicInfoCollector::get_taskstats_for_tgid(int tgid, struct taskstats* out_stats) {
    // 类似于get_taskstats_for_pid，但使用TGID
    int ret = 0;

    if (!nl_sock || family_id < 0) return false;
    
    struct nl_msg* msg = nlmsg_alloc();
    if (!msg){
        spdlog::error("BasicInfoCollector: Failed to allocate netlink message");
        return false;
    }
    spdlog::debug("family_id: {}", family_id);

    genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ, family_id, 0, 
                0, TASKSTATS_CMD_GET, TASKSTATS_GENL_VERSION);
    
    nla_put_s32(msg, TASKSTATS_CMD_ATTR_PID, tgid);

    struct nl_cb* cb = nl_cb_alloc(NL_CB_DEFAULT);
    if (!cb) {
        nlmsg_free(msg);
        spdlog::error("BasicInfoCollector: Failed to allocate netlink callback");
        return false;
    }
    
    int err = nl_send_auto(nl_sock, msg);
    if (err < 0) {
        nlmsg_free(msg);
        nl_cb_put(cb);
        spdlog::error("BasicInfoCollector: Failed to send netlink message");
        return false;
    }
    
    unsigned char* reply = nullptr;
    sockaddr_nl addr;
    addr.nl_family = AF_NETLINK;
    addr.nl_pad = 0;
    addr.nl_pid = 0;
    addr.nl_groups = 0;

    ret = nl_recv(nl_sock, &addr, &reply, NULL);
    if (ret < 0 || !reply) {
        nlmsg_free(msg);
        nl_cb_put(cb);
        spdlog::error("BasicInfoCollector: Failed to receive netlink reply for TGID {}, error code {}", tgid, ret);
        return false;
    }

    struct nlmsghdr *h = (struct nlmsghdr *)(reply);
    auto reply_msg = nlmsg_convert(h);
    if (h->nlmsg_type == NLMSG_ERROR) {
        auto data = (struct nlmsgerr *)NLMSG_DATA(h);
        if (data->error == 0) {
            spdlog::debug("BasicInfoCollector: Netlink ACK received for TGID {}", tgid);
        } else if (data->error == -EPERM) {
            spdlog::error("BasicInfoCollector: Permission denied for TGID {}", tgid);
            nlmsg_free(msg);
            nlmsg_free(reply_msg);
            nl_cb_put(cb);
            return false;

        } else {
            spdlog::error("BasicInfoCollector: Netlink error {} for TGID {}", data->error, tgid);
            nlmsg_free(msg);
            nl_cb_put(cb);
            return false;
        }
    }

    struct nlattr* attrs[TASKSTATS_TYPE_MAX + 1];
    struct genlmsghdr* gnlh = (struct genlmsghdr*)nlmsg_data(nlmsg_hdr(reply_msg));

    ret = genlmsg_parse(nlmsg_hdr(reply_msg), sizeof(gnlh), attrs, TASKSTATS_TYPE_MAX, NULL);
    if(ret < 0) {
        nlmsg_free(msg);
        nlmsg_free(reply_msg);
        nl_cb_put(cb);
        spdlog::error("BasicInfoCollector: Invalid generic netlink message for TGID {}, error code {}", tgid, ret);
        return false;
    }

    if (nla_parse(attrs, TASKSTATS_TYPE_MAX, genlmsg_attrdata(gnlh, 0),
                  genlmsg_attrlen(gnlh, 0), NULL) < 0) {
        nlmsg_free(reply_msg);
        nl_cb_put(cb);
        spdlog::error("BasicInfoCollector: Failed to parse netlink attributes");
        return false;
    }
    
    if (attrs[TASKSTATS_TYPE_AGGR_PID]) {
        spdlog::debug("BasicInfoCollector: Parsing taskstats for TGID {}", tgid);
        struct nlattr* task_attrs[TASKSTATS_TYPE_MAX + 1];
        
        if (nla_parse_nested(task_attrs, TASKSTATS_TYPE_MAX, 
                            attrs[TASKSTATS_TYPE_AGGR_PID], NULL) < 0) {
            nlmsg_free(reply_msg);
            nl_cb_put(cb);
            spdlog::error("BasicInfoCollector: Failed to parse nested taskstats attributes");
            return false;
        }
        
        if (task_attrs[TASKSTATS_TYPE_STATS]) {
            memcpy(out_stats, nla_data(task_attrs[TASKSTATS_TYPE_STATS]), sizeof(struct taskstats));
            spdlog::debug("BasicInfoCollector: Retrieved taskstats for TGID {}, version {}", tgid, out_stats->version);
            spdlog::debug("BasicInfoCollector: TGID {} {} stats - CPU time (user: {}, system: {}), Memory (RSS: {} KB, VM: {} KB), IO (read bytes: {}, write bytes: {})",
                          tgid, std::string(out_stats->ac_comm),
                          out_stats->ac_utime, out_stats->ac_stime,
                          out_stats->coremem, out_stats->virtmem,
                          out_stats->read_bytes, out_stats->write_bytes);
            nlmsg_free(reply_msg);
            nl_cb_put(cb);
            spdlog::debug("BasicInfoCollector: Successfully retrieved taskstats for TGID {}", tgid);
            return true;
        }
    }
    
    nlmsg_free(reply_msg);
    nl_cb_put(cb);
    spdlog::error("BasicInfoCollector: No taskstats for TGID {}", tgid);
    return false;
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
    if (!inited || !nl_sock) {
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

        spdlog::debug("BasicInfoCollector: Collected for name {} PID {}: CPU% {:.2f}, MEM% {:.2f}, ReadSpeed {:.2f} B/s, WriteSpeed {:.2f} B/s",
                      stats.ac_comm, pid, info.cpuPercent, info.memoryPercent, info.readSpeed, info.writeSpeed);
        
        infos.push_back(info);
    }
    
    spdlog::debug("BasicInfoCollector: collected {} entries for job {}", 
                  infos.size(), job.JobID);
    
    return infos;
}


CollectDataParseFunc BasicInfoCollector::get_writer_parser(const std::string& writer_type) {
    CollectDataParseFunc func = nullptr;
    spdlog::debug("BasicInfoCollector: get_writer_parser for writer_type: {}", writer_type);
    
    if (writer_type.compare("ESWriter") == 0) {
        func = [this](std::any data) -> std::any {
            nlohmann::json ret;
            if (!data.has_value()) {
                spdlog::warn("BasicInfoCollector: empty data");
                ret["error"] = "empty data";
                return ret;
            }
            
            ret["process_data"] = nlohmann::json::array();
            auto parsed = std::any_cast<std::vector<BasicInfo>>(data);
            
            spdlog::debug("BasicInfoCollector: parsing {} entries for ESWriter", parsed.size());
            
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
                    ret["summary"] = j;
                } else {
                    ret["process_data"].push_back(j);
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
            auto parsed = std::any_cast<std::vector<BasicInfo>>(data);
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
            
            auto parsed = std::any_cast<std::vector<BasicInfo>>(data);
            
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
