#include "collector/net_usage_collector.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include <arpa/inet.h>
#include <dirent.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>

static void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

// A narrow syscall fault injection exercises the safe per-PID fallback when
// procfs denies namespace identity access. All FD/table reads remain real.
static bool deny_namespace_stat = false;
static std::string switch_namespace_path;
static int switch_command_fd = -1, switch_ack_fd = -1;
extern "C" int __real_stat(const char*, struct stat*);
extern "C" int __wrap_stat(const char* path, struct stat* info) {
    if (deny_namespace_stat && std::strstr(path, "/ns/net")) {
        errno = EACCES;
        return -1;
    }
    const int result = __real_stat(path, info);
    if (result == 0 && switch_namespace_path == path) {
        switch_namespace_path.clear();
        char command = 'n';
        require(write(switch_command_fd, &command, 1) == 1 &&
                read(switch_ack_fd, &command, 1) == 1 && command == 'n',
                "child network namespace switch failed");
    }
    return result;
}

static size_t openFdCount() {
    DIR* directory = opendir("/proc/self/fd");
    require(directory != nullptr, "cannot enumerate test FDs");
    size_t count = 0;
    while (const auto* entry = readdir(directory))
        if (entry->d_name[0] != '.') ++count;
    closedir(directory);
    return count;
}

struct Sockets {
    std::vector<int> fds;
    ~Sockets() { clear(); }
    int add(int fd) {
        require(fd >= 0, "socket operation failed");
        fds.push_back(fd);
        return fd;
    }
    void clear() {
        for (int fd : fds) close(fd);
        fds.clear();
    }
};

static sockaddr_in bindLoopback(int fd) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    require(bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "bind failed");
    socklen_t length = sizeof(address);
    require(getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) == 0, "getsockname failed");
    return address;
}

static void connectTo(int fd, const sockaddr_in& address) {
    require(connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0, "connect failed");
}

static uint64_t inodeOf(int fd) {
    struct stat info{};
    require(fstat(fd, &info) == 0, "fstat failed");
    return info.st_ino;
}

static void mark(const std::string& text) {
    const auto line = text + "\n";
    require(write(STDERR_FILENO, line.data(), line.size()) == static_cast<ssize_t>(line.size()), "marker write failed");
}

static const Connection& findConnection(const NetInfo& info, uint64_t inode) {
    const auto it = std::find_if(info.connections.begin(), info.connections.end(),
        [inode](const Connection& c) { return c.inode == inode; });
    require(it != info.connections.end(), "missing owned connection");
    return *it;
}

// Literal fixtures protect the three existing writer contracts, including IPv6
// formatting, empty fd_path and the existing process-sum summary semantics.
static void checkWriters(NetUsageCollector& collector, bool summary) {
    Connection tcp;
    tcp.proto = L4Proto::TCP;
    tcp.state = TcpState::ESTABLISHED;
    tcp.local = {IPVer::V4, "127.0.0.1", 1234};
    tcp.peer = {IPVer::V4, "127.0.0.2", 443};
    tcp.send_q = 7; tcp.recv_q = 8;
    tcp.sent = 100; tcp.recv = 200;
    tcp.send_rate = 1.25; tcp.recv_rate = 2.5;
    tcp.delivery_rate = 50; tcp.uid = 1000; tcp.inode = 42;
    tcp.fd = std::numeric_limits<uint32_t>::max();
    tcp.retrans = 3; tcp.rto = 200000; tcp.rtt = 300; tcp.rtt_var = 40;
    Connection udp;
    udp.proto = L4Proto::UDP;
    udp.local = {IPVer::V6, "[::1]", 5678};
    udp.peer = {IPVer::V6, "[::2]", 53};
    udp.uid = 1000; udp.inode = 43; udp.fd = tcp.fd;
    Connection total;
    total.summary = true;
    total.sent = 100; total.recv = 200;
    total.send_rate = 1.25; total.recv_rate = 2.5;
    total.delivery_rate = 50; total.retrans = 3;
    std::vector<NetInfo> data{{getpid(), {tcp, udp}}};
    if (summary) data.push_back({0, {total}});

    auto expected = nlohmann::json::parse(R"({"process_data":[{"pid":0,"connections":[
      {"proto":"TCP","state":1,"local_addr":"127.0.0.1","local_port":1234,
       "peer_addr":"127.0.0.2","peer_port":443,"recv_q":8,"send_q":7,
       "sent":100,"recv":200,"send_rate":1.25,"recv_rate":2.5,"delivery_rate":50,
       "uid":1000,"inode":42,"fd":4294967295,"retrans":3,"rto":200000,"rtt":300,"rtt_var":40,"fd_path":""},
      {"proto":"UDP","state":0,"local_addr":"[::1]","local_port":5678,
       "peer_addr":"[::2]","peer_port":53,"recv_q":0,"send_q":0,
       "sent":0,"recv":0,"send_rate":0.0,"recv_rate":0.0,"delivery_rate":0,
       "uid":1000,"inode":43,"fd":4294967295,"retrans":0,"rto":0,"rtt":0,"rtt_var":0,"fd_path":""}
    ]}]})");
    expected["process_data"][0]["pid"] = getpid();
    if (summary) expected["summary"] = nlohmann::json::parse(R"({"pid":0,"connections":[
      {"proto":"TCP","state":0,"local_addr":"","local_port":0,"peer_addr":"","peer_port":0,
       "recv_q":0,"send_q":0,"sent":100,"recv":200,"send_rate":1.25,"recv_rate":2.5,
       "delivery_rate":50,"uid":4294967295,"inode":0,"fd":0,"retrans":3,"rto":0,"rtt":0,"rtt_var":0,"fd_path":""}
    ]})");
    const auto es = collector.get_writer_parser("ESWriter");
    require(std::any_cast<nlohmann::json>(es(data)) == expected, "ES schema or field value changed");
    WriterParseContext context{};
    require(std::any_cast<nlohmann::json>(collector.get_writer_parser_v2("ESWriter")(context, data)) == expected,
            "V2 writer adapter changed output");

    const auto pid = std::to_string(getpid());
    std::string expected_file = "NetUsageCollector type=process pid=" + pid + " connections=2\n";
    expected_file += "NetUsageCollector connection pid=" + pid +
        " proto=TCP state=1 local_addr=127.0.0.1 local_port=1234 peer_addr=127.0.0.2 peer_port=443 recv_q=8 send_q=7 sent=100 recv=200 send_rate=1.25 recv_rate=2.5 delivery_rate=50 uid=1000 inode=42 fd=4294967295 retrans=3 rto=200000 rtt=300 rtt_var=40 fd_path=\n";
    expected_file += "NetUsageCollector connection pid=" + pid +
        " proto=UDP state=0 local_addr=[::1] local_port=5678 peer_addr=[::2] peer_port=53 recv_q=0 send_q=0 sent=0 recv=0 send_rate=0 recv_rate=0 delivery_rate=0 uid=1000 inode=43 fd=4294967295 retrans=0 rto=0 rtt=0 rtt_var=0 fd_path=\n";
    if (summary) expected_file += "NetUsageCollector type=summary pid=0 connections=1\n"
        "NetUsageCollector connection pid=0 proto=TCP state=0 local_addr= local_port=0 peer_addr= peer_port=0 recv_q=0 send_q=0 sent=100 recv=200 send_rate=1.25 recv_rate=2.5 delivery_rate=50 uid=4294967295 inode=0 fd=0 retrans=3 rto=0 rtt=0 rtt_var=0 fd_path=\n";
    require(std::any_cast<std::string>(collector.get_writer_parser("FileWriter")(data)) == expected_file,
            "FileWriter format or field value changed");
    const auto prom = std::any_cast<PrometheusExporterWriter::prometheus_job_state>(
        collector.get_writer_parser("PrometheusExporterWriter")(data));
    require(prom.processes_state.size() == (summary ? 2 : 1), "Prometheus process layout changed");
    const auto& p = prom.processes_state[0];
    require(p.pid == getpid() && p.net_sent_bytes_total == 100 && p.net_recv_bytes_total == 200 &&
        p.net_send_bytes_per_sec == 1.25 && p.net_recv_bytes_per_sec == 2.5 &&
        p.tcp_retrans_total == 3 && p.tcp_rtt_us == 150, "Prometheus field values changed");
    if (summary) require(prom.processes_state[1].pid == 0 && prom.processes_state[1].net_sent_bytes_total == 100,
                         "Prometheus summary changed");
}

static void run(bool namespace_race) {
    Sockets sockets;
    const int listener = sockets.add(socket(AF_INET, SOCK_STREAM, 0));
    const auto server_addr = bindLoopback(listener);
    require(listen(listener, 1) == 0, "listen failed");
    const int client = sockets.add(socket(AF_INET, SOCK_STREAM, 0));
    connectTo(client, server_addr);
    const int server = sockets.add(accept(listener, nullptr, nullptr));
    sockets.add(dup(client)); // Keep the legacy smallest-FD selection.
    const int udp_a = sockets.add(socket(AF_INET, SOCK_DGRAM, 0));
    const int udp_b = sockets.add(socket(AF_INET, SOCK_DGRAM, 0));
    const auto addr_a = bindLoopback(udp_a), addr_b = bindLoopback(udp_b);
    connectTo(udp_a, addr_b); connectTo(udp_b, addr_a);
    const auto client_inode = inodeOf(client);
    const std::array<uint64_t, 4> inodes{client_inode, inodeOf(server), inodeOf(udp_a), inodeOf(udp_b)};
    int commands[2], acknowledgements[2];
    require(pipe(commands) == 0 && pipe(acknowledgements) == 0, "pipe failed");
    const pid_t child = fork();
    require(child >= 0, "fork failed");
    if (child == 0) {
        close(commands[1]); close(acknowledgements[0]);
        char command;
        while (read(commands[0], &command, 1) == 1) {
            if (command == 'c') sockets.clear();
            if (command == 'n' && unshare(CLONE_NEWNET) != 0) command = 'e';
            if (write(acknowledgements[1], &command, 1) != 1) break;
        }
        _exit(0);
    }
    close(commands[0]); close(acknowledgements[1]);
    // Always reap the child, including on an assertion failure.
    struct Reap {
        pid_t pid; int command_fd, ack_fd;
        ~Reap() { close(command_fd); close(ack_fd); waitpid(pid, nullptr, 0); }
    } reap{child, commands[1], acknowledgements[0]};

    const auto initial_fds = openFdCount();
    NetUsageCollector collector;
    if (namespace_race) {
        collector.init({{"summary", "false"}, {"use_netlink", "false"}});
        Job job; job.JobPIDs = {getpid()};
        const auto before = std::any_cast<std::vector<NetInfo>>(collector.collect(job));
        require(before.size() == 1 && before[0].connections.size() == 4,
                "namespace race fixture is missing connections");
        // Move the first reader after its grouping stat but before table reads.
        // The stable parent's sockets must still be found via another reader.
        switch_namespace_path = "/proc/" + std::to_string(child) + "/ns/net";
        switch_command_fd = commands[1]; switch_ack_fd = acknowledgements[0];
        job.JobPIDs = {child, getpid()};
        const auto after = std::any_cast<std::vector<NetInfo>>(collector.collect(job));
        require(switch_namespace_path.empty(), "namespace switch hook did not run");
        require(after.size() == 2 && after[1].pid == getpid() && after[1].connections.size() == 4,
                "namespace change in first reader lost stable process connections");
        for (auto inode : inodes) findConnection(after[1], inode);
        collector.deinit();
        require(openFdCount() == initial_fds, "namespace race leaked FDs");
        return;
    }
    collector.init({{"summary", "true"}, {"use_netlink", "true"}});
    checkWriters(collector, true);
    Job job; job.JobID = 17; job.JobPIDs = {getpid(), child};
    mark("NET_BEGIN shared");
    auto first = std::any_cast<std::vector<NetInfo>>(collector.collect(job));
    mark("NET_END shared");
    require(first.size() == 3 && first[0].pid == getpid() && first[1].pid == child && first[2].pid == 0,
            "process order or summary entry changed");
    for (size_t i = 0; i < 2; ++i) {
        require(first[i].connections.size() == 4, "missing TCP/UDP connection or listener leaked");
        for (auto inode : inodes) findConnection(first[i], inode);
        require(findConnection(first[i], client_inode).fd == static_cast<uint32_t>(client), "smallest FD changed");
    }
    require(send(client, "hello!", 6, 0) == 6, "send test payload failed");
    char buffer[6];
    require(recv(server, buffer, sizeof(buffer), MSG_WAITALL) == 6, "receive test payload failed");
    mark("NET_BEGIN updated");
    const auto second = std::any_cast<std::vector<NetInfo>>(collector.collect(job));
    mark("NET_END updated");
    const auto& a = findConnection(second[0], client_inode);
    const auto& b = findConnection(second[1], client_inode);
    require(a.sent >= 6 && a.send_rate > 0, "TCP counters or rate did not advance");
    require(a.sent == b.sent && a.send_rate == b.send_rate, "shared socket was sampled more than once");
    require(second[2].connections[0].sent == second[0].connections[0].sent + second[0].connections[1].sent +
        second[1].connections[0].sent + second[1].connections[1].sent, "legacy process-sum summary changed");

    deny_namespace_stat = true;
    mark("NET_BEGIN unknown_namespace");
    const auto isolated = std::any_cast<std::vector<NetInfo>>(collector.collect(job));
    mark("NET_END unknown_namespace");
    deny_namespace_stat = false;
    require(isolated.size() == 3 && isolated[0].connections.size() == 4 && isolated[1].connections.size() == 4,
            "namespace lookup failure lost process output");

    require(write(commands[1], "c", 1) == 1 && read(acknowledgements[0], buffer, 1) == 1, "child close failed");
    job.JobPIDs = {child};
    mark("NET_BEGIN empty");
    const auto empty = std::any_cast<std::vector<NetInfo>>(collector.collect(job));
    mark("NET_END empty");
    require(empty.size() == 2 && empty[0].connections.empty(), "closed socket ownership was reused");
    checkWriters(collector, true);
    collector.deinit();
    require(openFdCount() == initial_fds, "netlink FD leaked after deinit");
    collector.init({{"summary", "false"}, {"use_netlink", "false"}});
    checkWriters(collector, false);
    job.JobPIDs = {getpid()};
    mark("NET_BEGIN without_netlink");
    const auto without_netlink = std::any_cast<std::vector<NetInfo>>(collector.collect(job));
    mark("NET_END without_netlink");
    require(without_netlink.size() == 1 && without_netlink[0].connections.size() == 4,
            "proc-only mode changed connection structure");
    require(findConnection(without_netlink[0], client_inode).sent == 0, "disabled netlink retained old counters");
    collector.deinit();
    require(openFdCount() == initial_fds, "collector lifecycle leaked FDs");
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::off);
    const bool namespace_race = argc == 2 && std::string(argv[1]) == "--namespace-race";
    try {
        run(namespace_race);
        std::cout << (namespace_race ? "Network namespace reader fallback passed\n" :
                                      "NetUsage collector and writer compatibility passed\n");
    }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
