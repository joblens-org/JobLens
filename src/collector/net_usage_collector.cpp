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
#include "collector/net_usage_collector.hpp"

#include <algorithm>
#include <any>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <poll.h>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <linux/netlink.h>
#include <linux/sock_diag.h>
#include <linux/inet_diag.h>
#include <linux/rtnetlink.h>
#include <linux/tcp.h>
#include <iostream>
#include "core/collector_registry.hpp"

#include "writer/prometheus_exporter_writer.hpp"
#include "common/utils.hpp"


AUTO_REGISTER_JOB_COLLECTOR(
    NetUsageCollector, 
    "Collect network usage statistics for TCP connections",
    ConfigParams{
        {"freq", "Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds"},
        {"use_netlink", "Whether to use netlink to query TCP_INFO (true/false), default true"},
        {"summary", "Whether to summarize data across all processes (true/false), default false"}
    }
)

namespace {

static bool IsProcRaceErrno(int err) {
    return err == ENOENT || err == ENOTDIR;
}

// Only the first ten /proc columns are used. Views avoid allocating strings
// for every column of every (usually unrelated) socket in the namespace.
static bool SplitProcColumns(std::string_view line, std::array<std::string_view, 10>& columns) {
    size_t pos = 0;
    for (auto& column : columns) {
        while (pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos]))) ++pos;
        const auto start = pos;
        while (pos < line.size() && !std::isspace(static_cast<unsigned char>(line[pos]))) ++pos;
        if (start == pos) return false;
        column = line.substr(start, pos - start);
    }
    return true;
}

// 判断字符串是否都是数字
static bool IsDigits(const std::string& s) {
    if (s.empty()) return false;
    for (unsigned char c : s) {
        if (!std::isdigit(c)) return false;
    }
    return true;
}

template <typename T>
static bool ParseUnsigned(std::string_view s, T& out, int base = 10) {
    if (s.empty()) return false;
    const auto parsed = std::from_chars(s.data(), s.data() + s.size(), out, base);
    return parsed.ec == std::errc{} && parsed.ptr == s.data() + s.size();
}

template <typename T>
static bool ParseHex(std::string_view s, T& out) {
    return ParseUnsigned(s, out, 16);
}

// 将 /proc/net/tcp 的 IPv4 地址（8位十六进制，小端序）转为点分十进制
static std::string IPv4HexToStr(std::string_view hex8) {
    if (hex8.size() != 8) return "";
    uint32_t v = 0;
    if (!ParseHex<uint32_t>(hex8, v)) return "";
    // v 是小端序表示，需要逐字节反序
    unsigned char b[4];
    b[0] = (v) & 0xff;
    b[1] = (v >> 8) & 0xff;
    b[2] = (v >> 16) & 0xff;
    b[3] = (v >> 24) & 0xff;
    char buf[INET_ADDRSTRLEN] = {0};
    if (!inet_ntop(AF_INET, b, buf, sizeof(buf))) return "";
    return std::string(buf);
}

// 将 /proc/net/tcp6 的 IPv6 地址（32位十六进制，按字节顺序）转为文本
static std::string IPv6HexToStr(std::string_view hex32) {
    if (hex32.size() != 32) return "";
    unsigned char b[16];
    for (int i = 0; i < 16; ++i) {
        unsigned int val = 0;
        if (!ParseHex(hex32.substr(i * 2, 2), val)) return "";
        b[i] = static_cast<unsigned char>(val);
    }
    char buf[INET6_ADDRSTRLEN] = {0};
    if (!inet_ntop(AF_INET6, b, buf, sizeof(buf))) return "";
    return std::string(buf);
}

// 解析本地地址:端口，返回 Endpoint
static Endpoint ParseEndpoint(bool v6, std::string_view addr_port_hex) {
    // 格式: <ADDR_HEX>:<PORT_HEX>
    Endpoint ep;
    ep.ver = v6 ? IPVer::V6 : IPVer::V4;
    auto pos = addr_port_hex.find(':');
    if (pos == std::string::npos) return ep;
    const auto ahex = addr_port_hex.substr(0, pos);
    const auto phex = addr_port_hex.substr(pos + 1);

    uint32_t port = 0;
    ParseHex<uint32_t>(phex, port);
    ep.port = static_cast<uint16_t>(port);

    if (v6) {
        ep.addr = IPv6HexToStr(ahex);
        if (!ep.addr.empty()) ep.addr = "[" + ep.addr + "]";
    } else {
        ep.addr = IPv4HexToStr(ahex);
    }
    return ep;
}

static TcpState MapTcpStateFromHex(std::string_view st_hex) {
    uint32_t st = 0;
    if (!ParseHex<uint32_t>(st_hex, st)) return TcpState::UNKNOWN;
    switch (st) {
        case 0x01: return TcpState::ESTABLISHED;
        case 0x02: return TcpState::SYN_SENT;
        case 0x03: return TcpState::SYN_RECV;
        case 0x04: return TcpState::FIN_WAIT1;
        case 0x05: return TcpState::FIN_WAIT2;
        case 0x06: return TcpState::TIME_WAIT;
        case 0x07: return TcpState::CLOSE;
        case 0x08: return TcpState::CLOSE_WAIT;
        case 0x09: return TcpState::LAST_ACK;
        case 0x0A: return TcpState::LISTEN;
        case 0x0B: return TcpState::CLOSING;
        default:   return TcpState::UNKNOWN;
    }
}

// 建立 inode -> fd 的映射：读取 /proc/<pid>/fd/* 的链接目标 socket:[inode]
using InodeToFd = std::unordered_map<uint64_t, uint32_t>;

static std::optional<InodeToFd> BuildInodeToFdMap(pid_t pid) {
    InodeToFd m;
    std::string dir = "/proc/" + std::to_string(pid) + "/fd";
    const auto close_directory = [](DIR* directory) { closedir(directory); };
    std::unique_ptr<DIR, decltype(close_directory)> dp(opendir(dir.c_str()), close_directory);
    if (!dp) {
        int err = errno;
        if (IsProcRaceErrno(err)) {
            spdlog::debug("NetUsageCollector: skip vanished process fd dir {}: {}", dir, strerror(err));
        } else {
            spdlog::error("NetUsageCollector: open {} failed: {}", dir, strerror(err));
        }
        return std::nullopt;
    }
    dirent* de;
    char buf[PATH_MAX + 1];
    while ((de = readdir(dp.get())) != nullptr) {
        if (!IsDigits(de->d_name)) continue;
        std::string fdpath = dir + "/" + de->d_name;
        ssize_t n = readlink(fdpath.c_str(), buf, PATH_MAX);
        if (n <= 0) continue;
        buf[n] = '\0';
        const std::string_view target(buf, static_cast<size_t>(n));
        // socket:[inode]
        static constexpr std::string_view prefix = "socket:[";
        auto pos = target.find(prefix);
        if (pos != 0) continue;
        auto rpos = target.find(']', prefix.size());
        if (rpos == std::string::npos) continue;
        const auto inode_str = target.substr(prefix.size(), rpos - prefix.size());
        uint64_t inode = 0;
        uint32_t fd_num = 0;
        if (!ParseUnsigned(inode_str, inode) || !ParseUnsigned(std::string_view(de->d_name), fd_num)) continue;
        // 选择最小 fd
        auto it = m.find(inode);
        if (it == m.end() || fd_num < it->second) {
            m[inode] = fd_num;
        }
    }
    return m;
}

static int hashConnection(const Connection& c){
    static auto hash_combine = [](std::size_t& seed, std::size_t val){
        seed ^= val + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    };
    std::size_t seed = 0;
    hash_combine(seed, static_cast<std::size_t>(c.local.ver));
    hash_combine(seed, std::hash<std::string>{}(c.local.addr));
    hash_combine(seed, static_cast<std::size_t>(c.local.port));
    hash_combine(seed, static_cast<std::size_t>(c.peer.ver));
    hash_combine(seed, std::hash<std::string>{}(c.peer.addr));
    hash_combine(seed, static_cast<std::size_t>(c.peer.port));
    return seed;
}



} // namespace


// 取 fd 对应的绝对路径
static std::string fd_to_path(int pid, int fd)
{
    char buf[512];
    std::string p = "/proc/" + std::to_string(pid) + "/fd/" + std::to_string(fd);
    ssize_t n = readlink(p.c_str(), buf, sizeof(buf)-1);
    if (n < 0) return {};
    buf[n] = '\0';
    return std::string(buf);
}

// Parse a namespace table once for all sockets owned by this Job's processes.
// An unsuccessful read is not cached: another live process can supply the table.
static bool ReadProcNetFile(pid_t pid, const char* proto_file, L4Proto proto, bool v6,
                            const std::unordered_set<uint64_t>& wanted,
                            std::vector<Connection>& result) {
    std::vector<Connection> conns;
    std::string path = "/proc/" + std::to_string(pid) + "/net/" + proto_file;
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        int err = errno;
        if (IsProcRaceErrno(err)) {
            spdlog::debug("NetUsageCollector: skip vanished process net file {}: {}", path, strerror(err));
        } else {
            spdlog::error("NetUsageCollector: open {} failed: {}", path, strerror(err));
        }
        return false;
    }
    std::string line;
    // 跳过表头
    if (!std::getline(ifs, line)) return false;

    while (std::getline(ifs, line)) {
        Connection c;
        std::array<std::string_view, 10> cols;
        if (!SplitProcColumns(line, cols) || !ParseUnsigned(cols[9], c.inode) ||
            c.inode == 0 || wanted.count(c.inode) == 0) continue;

        // cols[1] local_address, cols[2] rem_address
        // cols[3] st, cols[4] tx_queue:rx_queue, cols[5] tr:tm->when, cols[6] retrnsmt
        // cols[7] uid, cols[8] timeout, cols[9] inode
        Endpoint local = ParseEndpoint(v6, cols[1]);
        Endpoint peer  = ParseEndpoint(v6, cols[2]);

        // 过滤掉伪连接
        if (peer.addr == "0.0.0.0" || peer.addr == "[::]") continue;
        
        
        c.proto = proto;
        c.local = std::move(local);
        c.peer  = std::move(peer);
        // 利用local和peer计算hash
        c.hash = hashConnection(c);
        if (!ParseUnsigned(cols[7], c.uid)) continue;
        
        
        // // 解析队列 tx_queue:rx_queue（十六进制）
        // 默认使用netlink解析
        auto pos = cols[4].find(':');
        if (pos != std::string::npos) {
            const auto txh = cols[4].substr(0, pos);
            const auto rxh = cols[4].substr(pos + 1);
            uint32_t tx = 0, rx = 0;
            ParseHex<uint32_t>(txh, tx);
            ParseHex<uint32_t>(rxh, rx);
            c.send_q = tx;
            c.recv_q = rx;
        }
        

        if (proto == L4Proto::TCP) {
            c.state = MapTcpStateFromHex(cols[3]);
            if (c.state == TcpState::LISTEN   ||   // 0x0A
                c.state == TcpState::CLOSE   ||   // 0x05
                c.state == TcpState::TIME_WAIT)    // 0x06
            {
                continue;          // 直接丢弃，不放入 conns
            }
            // 重传次数（十六进制字段）
            uint32_t rt = 0;
            if (ParseHex<uint32_t>(cols[6], rt)) {
                c.retrans = rt;
            }
        } else {
            // UDP 状态不使用 TCP 状态机，保持 UNKNOWN
            c.state = TcpState::UNKNOWN;
        }
        
        conns.emplace_back(std::move(c));
    }
    if (ifs.bad()) return false;
    result.insert(result.end(), std::make_move_iterator(conns.begin()), std::make_move_iterator(conns.end()));
    return true;
}

static std::string NetworkNamespaceKey(pid_t pid) {
    const auto path = "/proc/" + std::to_string(pid) + "/ns/net";
    struct stat info{};
    if (::stat(path.c_str(), &info) == 0) {
        return std::to_string(info.st_dev) + ":" + std::to_string(info.st_ino);
    }
    // Without a verified namespace identity, never share another PID's table.
    return "pid:" + std::to_string(pid);
}

struct ProcessSockets {
    pid_t pid;
    InodeToFd inode_to_fd;
    size_t namespace_index = 0;
};

struct NamespaceSockets {
    std::string key;
    std::vector<pid_t> readers;
    std::unordered_set<uint64_t> inodes;
    std::vector<Connection> connections;
    std::unordered_map<uint64_t, std::vector<size_t>> inode_to_connections;
};

void print_hex(const void* p, std::size_t len, bool upper_case = false)
{
    const unsigned char* buf = static_cast<const unsigned char*>(p);
    auto old_flags = std::cout.flags();          // 保存原来的格式标志
    std::cout << std::hex << std::setfill('0');

    for (std::size_t i = 0; i < len; ++i)
    {
        if (upper_case)
            std::cout << std::uppercase;
        else
            std::cout << std::nouppercase;

        std::cout << std::setw(2) << static_cast<unsigned>(buf[i]) << ' ';
    }
    std::cout << std::dec << std::endl;          // 切换回十进制输出
    std::cout.flags(old_flags);                  // 恢复原来的格式
}

namespace {
// The diagnostic socket is non-blocking, so send/recv retry after the socket
// becomes ready again.
void wait_netlink_fd(int fd, short events) {
    pollfd descriptor{fd, events, 0};
    while (::poll(&descriptor, 1, -1) < 0 && errno == EINTR) {}
}
}

int NetUsageCollector::query_single_tcp(Connection& conn) {
    const auto& src = conn.local;
    const auto& dst = conn.peer;
    auto src_ip = src.addr.c_str();
    auto dst_ip = dst.addr.c_str();
    auto src_port = src.port;
    auto dst_port = dst.port;
    spdlog::trace("NetUsageCollector: netlink query tcp {}:{} -> {}:{}", src.addr, src.port, dst.addr, dst.port);
    struct {
        struct nlmsghdr         nlh;
        struct inet_diag_req_v2 req;
    } q = {};

    q.nlh.nlmsg_len = sizeof(q);
    q.nlh.nlmsg_type = SOCK_DIAG_BY_FAMILY;
    q.nlh.nlmsg_flags = NLM_F_REQUEST;   /* 关键：没有 DUMP */
    q.nlh.nlmsg_seq = ++query_sequence;
    q.nlh.nlmsg_pid = getpid();

    if(src.ver == IPVer::V4){
        q.req.sdiag_family   = AF_INET;
        /* ----- 填充 4 元组 ----- */
        inet_pton(AF_INET, src_ip, &q.req.id.idiag_src[0]);
        inet_pton(AF_INET, dst_ip, &q.req.id.idiag_dst[0]);
    }else if (src.ver == IPVer::V6){
        q.req.sdiag_family   = AF_INET6; 
        /* ----- 填充 4 元组 ----- */
        inet_pton(AF_INET6, src_ip, &q.req.id.idiag_src[0]);
        inet_pton(AF_INET6, dst_ip, &q.req.id.idiag_dst[0]);
    }

    q.req.sdiag_protocol = IPPROTO_TCP;
    // q.req.idiag_ext      = (1 << (INET_DIAG_INFO - 1)) |
    //                        (1 << (INET_DIAG_MEMINFO - 1));
    
    q.req.idiag_ext      = (1 << (INET_DIAG_INFO - 1));
    q.req.pad = 0;
    
    q.req.id.idiag_sport = htons(src_port);
    q.req.id.idiag_dport = htons(dst_port);
    q.req.id.idiag_if    = 0;            /* 任意接口 */
    q.req.id.idiag_cookie[0] = -1;
    q.req.id.idiag_cookie[1] = -1;

    for (;;) {
        const auto sent = send(netlink_fd, &q, sizeof(q), MSG_DONTWAIT | MSG_NOSIGNAL);
        if (sent == sizeof(q)) break;
        if (sent < 0 && errno == EINTR) continue;
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            wait_netlink_fd(netlink_fd, POLLOUT);
            continue;
        }
        spdlog::error("NetUsageCollector: netlink req sent msg error");
        return -1;
    }
    spdlog::debug("NetUsageCollector: netlink req sent msg");

    // Match requests even after a previous non-timeout error left a reply queued.
    char buf[1024];
    const nlmsghdr* h = nullptr;
    while (!h) {
        const ssize_t n = recv(netlink_fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            wait_netlink_fd(netlink_fd, POLLIN);
            continue;
        }
        if (n <= 0) { spdlog::error("NetUsageCollector: netlink recv msg error"); return -1; }
        int remaining = static_cast<int>(n);
        for (auto* candidate = reinterpret_cast<const nlmsghdr*>(buf);
             NLMSG_OK(candidate, remaining); candidate = NLMSG_NEXT(candidate, remaining)) {
            if (candidate->nlmsg_seq == q.nlh.nlmsg_seq) { h = candidate; break; }
        }
    }
    if (h->nlmsg_type == NLMSG_ERROR) {
        if (h->nlmsg_len < NLMSG_LENGTH(sizeof(nlmsgerr))) return -1;
        const nlmsgerr* e = (const nlmsgerr*)(NLMSG_DATA(h));
        spdlog::error("NetUsageCollector: netlink query kernel error: {}", e->error);
        return -1;
    }
    if (h->nlmsg_len < NLMSG_LENGTH(sizeof(inet_diag_msg))) return -1;

    const inet_diag_msg* m = (const inet_diag_msg*)(NLMSG_DATA(h));

    const struct rtattr* rta;
    int rtlen = h->nlmsg_len - NLMSG_LENGTH(sizeof(*m));
    for (rta = (struct rtattr*)((char*)m + sizeof(*m)); RTA_OK(rta, rtlen); rta = RTA_NEXT(rta, rtlen)) {
        if (rta->rta_type == INET_DIAG_INFO) {
            struct tcp_info sample{};
            std::memcpy(&sample, RTA_DATA(rta), std::min<size_t>(sizeof(sample), RTA_PAYLOAD(rta)));
            const struct tcp_info* info = &sample;
            // print_hex(info, rta->rta_len, true);
            conn.sent = info->tcpi_bytes_sent;
            conn.recv = info->tcpi_bytes_received;
            conn.delivery_rate = info->tcpi_delivery_rate;
            conn.rto = info->tcpi_rto;
            conn.rtt = info->tcpi_rtt;
            conn.rtt_var = info->tcpi_rttvar;

            // 简单差分计算速度
            auto& state = connection_state_dict[conn.hash];
            auto last_sent = state.sent;
            auto last_recv = state.recv;
            auto now = std::chrono::steady_clock::now();
            auto duration = now - state.last_time;
            double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();

            conn.send_rate = seconds > 0.0 ? (conn.sent - last_sent) / seconds : 0.0;
            conn.recv_rate= seconds > 0.0 ? (conn.recv - last_recv) / seconds : 0.0;
            state.last_time = now;
            state.sent = conn.sent;
            state.recv = conn.recv;
        }
    }
    return 0;
}


void NetUsageCollector::init_netlink(){
    netlink_fd = socket(AF_NETLINK, SOCK_RAW | SOCK_NONBLOCK | SOCK_CLOEXEC, NETLINK_SOCK_DIAG);
    if(netlink_fd < 0){
        spdlog::error("NetUsageCollector: netlink init error");
        return;
    }
    netlink_inited = true;
}


bool NetUsageCollector::init(const nlohmann::json& cfg) {
    spdlog::debug("NetUsageCollector: init with config: {}", cfg.dump());
    if(cfg.contains("summary") && cfg["summary"].get<std::string>() == "true"){
        summary = true;
    }else{
        summary = false;
    }
    netlink_requested = cfg["use_netlink"].get<std::string>() == "true";
    if(netlink_requested){
        init_netlink();
        spdlog::info("NetUsageCollector: use netlink to query tcp info");
    }
    spdlog::info("NetUsageCollector initialized");
    return true;
}


CollectResult NetUsageCollector::collect(const Job& job) {
    if (netlink_requested && !netlink_inited) init_netlink();
    std::vector<NetInfo> all;
    Connection summary_conn;
    summary_conn.summary = true;
    all.reserve(job.JobPIDs.size());
    // These snapshots deliberately live for just this collect(Job) call. They
    // cannot leak stale PID/FD ownership or samples into a later Job or cycle.
    std::vector<ProcessSockets> processes;
    processes.reserve(job.JobPIDs.size());
    std::vector<NamespaceSockets> namespaces;
    std::unordered_map<std::string, size_t> namespace_indices;
    for (pid_t pid : job.JobPIDs) {
        if (!Utils::is_process_running(pid)) continue;
        auto inode_to_fd = BuildInodeToFdMap(pid);
        if (!inode_to_fd) continue;
        processes.push_back({pid, std::move(*inode_to_fd), 0});
        auto& process = processes.back();
        if (process.inode_to_fd.empty()) continue;

        const auto key = NetworkNamespaceKey(pid);
        const auto entry = namespace_indices.emplace(key, namespaces.size());
        if (entry.second) {
            namespaces.emplace_back();
            namespaces.back().key = key;
        }
        process.namespace_index = entry.first->second;
        auto& snapshot = namespaces[process.namespace_index];
        snapshot.readers.push_back(pid);
        for (const auto& [inode, fd] : process.inode_to_fd) {
            snapshot.inodes.insert(inode);
        }
    }

    struct Table { const char* name; L4Proto proto; bool v6; };
    static constexpr std::array<Table, 4> tables{{
        {"tcp", L4Proto::TCP, false}, {"tcp6", L4Proto::TCP, true},
        {"udp", L4Proto::UDP, false}, {"udp6", L4Proto::UDP, true}
    }};
    for (auto& snapshot : namespaces) {
        const bool verify_namespace = snapshot.key.compare(0, 4, "pid:") != 0;
        for (const auto& table : tables) {
            for (pid_t pid : snapshot.readers) {
                const auto begin = snapshot.connections.size();
                if (!ReadProcNetFile(pid, table.name, table.proto, table.v6,
                                     snapshot.inodes, snapshot.connections)) continue;
                if (!verify_namespace || NetworkNamespaceKey(pid) == snapshot.key) break;
                // A reader can move namespaces after grouping. Discard that
                // table and let a stable reader supply the original namespace.
                snapshot.connections.resize(begin);
            }
        }
        for (size_t i = 0; i < snapshot.connections.size(); ++i) {
            auto& c = snapshot.connections[i];
            if (c.proto == L4Proto::TCP && netlink_inited) query_single_tcp(c);
            snapshot.inode_to_connections[c.inode].push_back(i);
        }
    }

    for (const auto& process : processes) {
        NetInfo info{process.pid, {}};
        if (!process.inode_to_fd.empty()) {
            const auto& snapshot = namespaces[process.namespace_index];
            std::vector<size_t> entries;
            for (const auto& [inode, fd] : process.inode_to_fd) {
                const auto found = snapshot.inode_to_connections.find(inode);
                if (found != snapshot.inode_to_connections.end())
                    entries.insert(entries.end(), found->second.begin(), found->second.end());
            }
            // Keep the existing tcp/tcp6/udp/udp6 and table-row ordering.
            std::sort(entries.begin(), entries.end());
            info.connections.reserve(entries.size());
            for (const auto index : entries) {
                auto c = snapshot.connections[index];
                c.fd = process.inode_to_fd.at(c.inode);
                info.connections.push_back(std::move(c));
            }
        }
        for (const auto& c : info.connections) {
            // Preserve the existing sum of process views, including shared
            // sockets. Changing Job accounting is separate from this refactor.
            if (summary) {
                summary_conn.delivery_rate += c.delivery_rate;
                summary_conn.recv_rate += c.recv_rate;
                summary_conn.send_rate += c.send_rate;
                summary_conn.sent += c.sent;
                summary_conn.recv += c.recv;
                summary_conn.retrans += c.retrans;
            }
        }

        all.emplace_back(std::move(info));
    }
    if (summary) {
        NetInfo summary_info;
        summary_info.pid = 0;  // 表示汇总
        summary_info.connections.push_back(std::move(summary_conn));
        all.push_back(std::move(summary_info));
    }
    return CollectResult{std::move(all)};
}


void NetUsageCollector::deinit() noexcept {
    connection_state_dict.clear();
    if (netlink_fd >= 0) close(netlink_fd);
    netlink_fd = -1;
    netlink_inited = false;
    netlink_requested = false;
    spdlog::info("NetUsageCollector {} deinitialized");
}


// 若 writer 未定义标准接口，这里返回一个空的默认解析器（按需在你的框架中实现）
CollectDataParseFunc NetUsageCollector::get_writer_parser(const std::string& writer_type) {
    CollectDataParseFunc func = nullptr;
    spdlog::trace("NetUsageCollector: get_writer_parser for writer_type: {}", writer_type);
    if(writer_type.compare("ESWriter") == 0){
        func = [this](std::any data)->std::any{
            nlohmann::json ret;
            if(data.has_value() == false) {
                spdlog::warn("NetUsageCollector: error writer parser, empty data");
                ret["error"] = "empty data";
                return ret;
            }
            ret["process_data"] = nlohmann::json::array();
            const auto& parsed = std::any_cast<const std::vector<NetInfo>&>(data);
            spdlog::trace("NetUsageCollector: writer parser get {} NetInfo entries", parsed.size());
            for (const auto& info : parsed) {
                nlohmann::json j;
                j["pid"] = info.pid;
                j["connections"] = nlohmann::json::array();
                
                for (const auto& c : info.connections) {
                    nlohmann::json cj;
                    cj["proto"] = (c.proto == L4Proto::TCP) ? "TCP" : "UDP";
                    cj["state"] = static_cast<int>(c.state);
                    cj["local_addr"] = c.local.addr;
                    cj["local_port"] = c.local.port;
                    cj["peer_addr"]  = c.peer.addr;
                    cj["peer_port"]  = c.peer.port;
                    cj["recv_q"]    = c.recv_q;
                    cj["send_q"]    = c.send_q;
                    cj["sent"]      = c.sent;
                    cj["recv"]      = c.recv;
                    cj["send_rate"] = c.send_rate;
                    cj["recv_rate"] = c.recv_rate;
                    cj["delivery_rate"] = c.delivery_rate;
                    cj["uid"]       = c.uid;
                    cj["inode"]     = c.inode;
                    cj["fd"]        = c.fd;
                    cj["retrans"]   = c.retrans;
                    cj["rto"]       = c.rto;
                    cj["rtt"]       = c.rtt;
                    cj["rtt_var"]   = c.rtt_var;
                    cj["fd_path"]   = fd_to_path(info.pid, c.fd);
                    j["connections"].push_back(std::move(cj));
                }
                if (info.pid == 0){
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
            if(data.has_value() == false) {
                spdlog::warn("NetUsageCollector: error FileWriter parser, empty data");
                return std::string("NetUsageCollector error=empty_data\n");
            }
            const auto& parsed = std::any_cast<const std::vector<NetInfo>&>(data);
            std::ostringstream out;
            for (const auto& info : parsed) {
                out << "NetUsageCollector"
                    << " type=" << (info.pid == 0 ? "summary" : "process")
                    << " pid=" << info.pid
                    << " connections=" << info.connections.size()
                    << '\n';
                for (const auto& c : info.connections) {
                    out << "NetUsageCollector connection"
                        << " pid=" << info.pid
                        << " proto=" << ((c.proto == L4Proto::TCP) ? "TCP" : "UDP")
                        << " state=" << static_cast<int>(c.state)
                        << " local_addr=" << c.local.addr
                        << " local_port=" << c.local.port
                        << " peer_addr=" << c.peer.addr
                        << " peer_port=" << c.peer.port
                        << " recv_q=" << c.recv_q
                        << " send_q=" << c.send_q
                        << " sent=" << c.sent
                        << " recv=" << c.recv
                        << " send_rate=" << c.send_rate
                        << " recv_rate=" << c.recv_rate
                        << " delivery_rate=" << c.delivery_rate
                        << " uid=" << c.uid
                        << " inode=" << c.inode
                        << " fd=" << c.fd
                        << " retrans=" << c.retrans
                        << " rto=" << c.rto
                        << " rtt=" << c.rtt
                        << " rtt_var=" << c.rtt_var
                        << " fd_path=" << fd_to_path(info.pid, c.fd)
                        << '\n';
                }
            }
            return out.str();
        };
    }

    if(writer_type.compare("PrometheusExporterWriter") == 0){
        func = [this](std::any data)->std::any{
            PrometheusExporterWriter::prometheus_job_state ret;
            if(data.has_value() == false) {
                spdlog::warn("IOUsageCollector: error writer parser, empty data");
                ret.JobID = 0;
                return ret;
            }
            const auto& parsed = std::any_cast<const std::vector<NetInfo>&>(data);
            
            for (const auto& info : parsed) {
                PrometheusExporterWriter::prometheus_process_state state;
                state.pid = info.pid;
                for (const auto& c : info.connections){
                    state.net_recv_bytes_per_sec += c.recv_rate;
                    state.net_recv_bytes_total += c.recv;
                    state.net_send_bytes_per_sec += c.send_rate;
                    state.net_sent_bytes_total += c.sent;
                    state.tcp_retrans_total += c.retrans;
                    state.tcp_rtt_us += c.rtt;
                }
                if (!info.connections.empty()) {
                    state.tcp_rtt_us /= info.connections.size();
                }

                ret.processes_state.push_back(state);
            }
            return ret;
        };
    }
    
    return func;
}
