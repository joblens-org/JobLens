#include "core/collector_scheduler.hpp"
#include "common/local_rpc.hpp"
#include "core/job_registry.hpp"
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <condition_variable>

using namespace std::chrono_literals;
struct CollectorSchedulerTestAccess {
    static void add(CollectorScheduler& scheduler, const std::string& name,
                    CollectFunc collect, CollectInitFunc init) {
        scheduler.addJobCollectFunc(name, "test", collect, init, [] {});
    }
    static void attach(CollectorScheduler& scheduler, const std::string& name) {
        scheduler.addJob2Collector(123, name);
    }
    static void remove(CollectorScheduler& scheduler, const std::string& name) {
        Job job;
        job.JobID = 123;
        job.CollectorNames = {name};
        scheduler.rmJobCollect(job);
    }
};
void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
nlohmann::json rpc(const std::string& path, const std::string& method,
                   nlohmann::json params = nlohmann::json::object()) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    check(fd >= 0, "socket");
    timeval timeout{2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::copy(path.begin(), path.end(), address.sun_path);
    check(connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "connect");
    auto request = nlohmann::json{{"method", method}, {"params", params}}.dump();
    check(send(fd, request.data(), request.size(), MSG_NOSIGNAL) == ssize_t(request.size()), "send");
    std::string response;
    char buffer[4096];
    ssize_t n;
    while ((n = recv(fd, buffer, sizeof(buffer), 0)) > 0) response.append(buffer, n);
    close(fd);
    check(n == 0, "RPC timeout");
    return nlohmann::json::parse(response);
}
int main() {
    const std::string base = "/tmp/opencode/scheduler-status-" + std::to_string(getpid());
    const auto path = base + ".sock";
    std::ofstream config(base + ".yaml");
    config << "lens_config:\n  max_collector_threads: 3\n"
              "collectors_config:\n  default_freq: 20\n  default_use_writers: []\n  collectors: []\n"
              "writers_config:\n  enable_writer_perf: false\n  writers: []\n"
              "job_registry_config:\n  job_db_path: " << base << ".db\n"
              "test:\n  freq: 20\n  use_writers: []\n";
    config.close();
    auto& server = RPCServer::instance(path);
    Config::instance(base + ".yaml");
    auto& scheduler = CollectorScheduler::instance();
    JobRegistry::instance().delJob(0);
    Job live;
    live.JobID = 123;
    live.JobPIDs = {getpid()};
    live.jobtype = JobType::Sys;
    live.subtype = JobSubType::Common;
    live.sub_attr = CommonJobAttr{.auto_update_child = false};
    live.CollectorNames = {"not-yet-registered"};
    check(JobRegistry::instance().addJob(live), "register live test process");
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false, released = false;
    std::atomic<int> attempts{0};
    CollectorSchedulerTestAccess::add(scheduler, "blocked", [&](const Job&) -> std::any {
        std::unique_lock lock(mutex);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return released; });
        return 1;
    }, [](const nlohmann::json&) { return true; });
    CollectorSchedulerTestAccess::add(scheduler, "failed", [](const Job&) -> std::any { return 1; },
        [](const nlohmann::json&) { return false; });
    CollectorSchedulerTestAccess::add(scheduler, "idle", [](const Job&) -> std::any { return 1; },
        [](const nlohmann::json&) { return true; });
    CollectorSchedulerTestAccess::add(scheduler, "recovery", [&](const Job&) -> std::any {
        if (++attempts == 1) throw std::runtime_error("controlled collect failure");
        return 1;
    }, [](const nlohmann::json&) { return true; });
    server.start();
    auto status = [&](const std::string& name) {
        return rpc(path, "CollectorScheduler/collector_status", {{"name", name}}).at("collector");
    };
    check(rpc(path, "CollectorScheduler/status")["lifecycle"] == "starting", "starting");
    scheduler.start();
    check(status("idle")["state"] == "idle", "idle is observable");
    check(rpc(path, "CollectorScheduler/collector_status", {{"name", "missing"}})["code"] == "unknown_collector", "unknown");
    check(rpc(path, "CollectorScheduler/collector_status", {{"name", 1}})["code"] == "invalid_params", "invalid");
    CollectorSchedulerTestAccess::attach(scheduler, "failed");
    check(status("failed")["state"] == "error", "init failure");
    check(status("failed")["operations"]["init"]["error_count"] == 1, "init error count");
    CollectorSchedulerTestAccess::attach(scheduler, "blocked");
    {
        std::unique_lock lock(mutex);
        check(cv.wait_for(lock, 2s, [&] { return entered; }), "collector entered");
    }
    auto before = std::chrono::steady_clock::now();
    auto blocked = status("blocked");
    auto latency = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - before).count();
    check(latency < 500, "status must not wait for collect lock");
    check(blocked["operations"]["collect"]["active_count"] == 1, "collect progress");
    check(blocked["job_count"] == 1, "add count");
    std::cout << "BLOCKED_RPC_LATENCY_MS=" << latency << "\nSAMPLE=" << blocked.dump() << '\n';
    CollectorSchedulerTestAccess::attach(scheduler, "recovery");
    std::this_thread::sleep_for(180ms);
    {
        std::lock_guard lock(mutex);
        released = true;
    }
    cv.notify_all();
    for (int i = 0; i < 100 && status("recovery")["operations"]["write"]["success_count"] == 0; ++i)
        std::this_thread::sleep_for(20ms);
    auto recovered = status("recovery");
    check(recovered["operations"]["collect"]["error_count"] == 1, "historical collect error");
    check(recovered["has_error"] == false, "recovery");
    CollectorSchedulerTestAccess::remove(scheduler, "blocked");
    CollectorSchedulerTestAccess::remove(scheduler, "blocked");
    check(status("blocked")["job_count"] == 0, "remove and repeated removal");
    scheduler.shutdown();
    check(rpc(path, "CollectorScheduler/status")["lifecycle"] == "stopped", "stopped");
    check(status("blocked")["timer_active"] == false, "timer stopped");
    server.stop();
    std::this_thread::sleep_for(150ms);
    unlink((base + ".yaml").c_str());
    std::cout << "PASS: actual scheduler + actual Unix-socket RPC boundary tests\n";
}
