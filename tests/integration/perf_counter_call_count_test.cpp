#include "common/local_rpc.hpp"
#include "core/collector_registry.hpp"
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <iostream>

struct CountingCollector : ICollector {
    bool init(const nlohmann::json&) override { return true; }
    void deinit() noexcept override {}
    CollectDataParseFunc get_writer_parser(const std::string&) override { return nullptr; }
    CollectResult collect(const Job& job) override {
        if (job.JobID == 0) throw std::runtime_error("模拟采集失败");
        return 1;
    }
};

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    auto& config = Config::instance(argv[1]);
    config.getRawNode("collectors_config")["enable_collector_perf"] = true;
    config.getRawNode("collectors_config")["perf_window_size"] = 10;
    const auto path = "/tmp/joblens-perf-test-" + std::to_string(getpid()) + ".sock";
    auto& rpc = RPCServer::instance(path);
    auto& registry = CollectorRegistry::instance();
    registry.registerCollector<CountingCollector>("CountingCollector", CollectorScope::Job, "计数验证");
    const auto handle = registry.createCollector("CountingCollector", "count_test");
    Job job{};
    job.JobID = 1;
    handle.collect(job);
    handle.collect(job);
    job.JobID = 0;
    try { handle.collect(job); } catch (const std::runtime_error&) {}
    rpc.start();
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::copy(path.begin(), path.end(), address.sun_path);
    timeval timeout{5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) return 3;
    const std::string request = R"({"method":"CollectorRegistry/CollectorsPerfCount","params":{}})";
    if (send(fd, request.data(), request.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(request.size())) return 4;
    std::string response;
    char buffer[4096];
    ssize_t count;
    while ((count = recv(fd, buffer, sizeof(buffer), 0)) > 0) response.append(buffer, count);
    close(fd);
    rpc.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    const auto result = nlohmann::json::parse(response);
    const auto& perf = result["collectors_perf"][0];
    if (perf["call_cnt"] != 2 || perf["err_cnt"] != 1) {
        std::cerr << "FAIL: collector counters " << perf.dump() << '\n';
        return 1;
    }
    PerfCounter samples(10);
    samples.append(10);
    samples.append(20);
    if (samples.snapshot().call_cnt != 2 || samples.snapshot().mean_us != 15) return 1;
    std::cout << "PASS: collector RPC call_cnt=2, err_cnt=1; append owns sample count and mean\n";
}
