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
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "core/collector_type.h"
#include "icollector.h"
#include <spdlog/spdlog.h>
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_set>

enum class IPVer : uint8_t { V4 = 4, V6 = 6 };
enum class L4Proto : uint8_t { TCP = 6, UDP = 17 };
enum class TcpState : uint8_t {
    UNKNOWN = 0,
    ESTABLISHED, SYN_SENT, SYN_RECV, FIN_WAIT1, FIN_WAIT2,
    TIME_WAIT, CLOSE, CLOSE_WAIT, LAST_ACK, LISTEN, CLOSING
};

struct Endpoint {
    IPVer   ver   = IPVer::V4;
    std::string addr;          // 文本形式，V6 带 []
    uint16_t    port = 0;
};

struct Connection {
    bool summary;
    //储存用hash
    size_t hash;
    //内容
    L4Proto     proto = L4Proto::TCP;
    TcpState    state = TcpState::UNKNOWN;
    Endpoint    local;
    Endpoint    peer;
    uint32_t    recv_q = 0;            //缓存大小，单位是字节
    uint32_t    send_q = 0;            //缓存大小，单位是字节
    uint64_t    sent;                  // tcpi_bytes_sent
    uint64_t    recv;                  // tcpi_bytes_received
    double      send_rate = 0;
    double      recv_rate = 0;
    uint64_t    delivery_rate;         // tcpi_delivery_rate (byte/s)
    uint32_t    uid    = static_cast<uint32_t>(-1);
    uint64_t    inode  = 0;            // socket inode
    uint32_t    fd      = 0;           // 对应 /proc/<pid>/fd/<fd>
    /* 以下仅 TCP 有效 */
    uint32_t    retrans    = 0;          // 重传次数
    uint32_t    rto        = 0;          // retrans timeout
    uint32_t    rtt        = 0;          // smoothed RTT (us)
    uint32_t    rtt_var    = 0;          // RTT variance
};

struct NetInfo{
    pid_t pid;
    std::vector<Connection>     connections;
};

class NetUsageCollector : public IPeriodicJobCollector{
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;
    
private:
    void init_netlink();
    int query_single_tcp(Connection& conn); //使用netlink机制查询
    void parse_tcp_info(const struct inet_diag_msg *m,
                                       unsigned nlmsg_len,
                                       Connection& conn);
    int netlink_fd;
    bool netlink_inited = false;

    std::vector<Connection> ParseProcNetFile(pid_t pid, const std::string& proto_file, L4Proto proto, bool v6);

    struct connection_state{
        std::chrono::steady_clock::time_point last_time{};
        uint64_t  sent{};      // tcpi_bytes_sent
        uint64_t  recv{};      // tcpi_bytes_received
        uint64_t  delivery_rate{};  // tcpi_delivery_rate (byte/s)
    };
    std::unordered_map<int, connection_state> connection_state_dict;
    std::unordered_map<pid_t, std::unordered_set<uint64_t>> pid_inode_dict;
    bool summary = false;
};
