#pragma once
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <nlohmann/json.hpp>

// 独立观测锁：持锁期间禁止调用采集器、writer 或调度器。
class CollectorRuntime {
public:
    using Clock = std::chrono::steady_clock;
    struct ActiveOperation {
        Clock::time_point started;
        std::optional<uint64_t> job_id;
    };
    struct Metric {
        size_t success = 0, errors = 0, next = 0;
        double last_duration_ms = 0;
        std::optional<int64_t> last_success_unix_ms;
        std::map<size_t, ActiveOperation> active;
        std::optional<uint64_t> last_job_id;
    };
    struct Entry {
        std::string name, scope = "undefined", config, phase = "idle", last_error;
        double period_ms = 0;
        size_t job_count = 0;
        bool timer_active = false, failed = false;
        std::map<std::string, Metric> metrics;
    };
    class Operation {
    public:
        Operation(std::shared_ptr<CollectorRuntime> owner, std::string name, std::string kind,
                  std::optional<uint64_t> job_id = std::nullopt);
        ~Operation();
        Operation(const Operation&) = delete;
        Operation& operator=(const Operation&) = delete;
        void fail(const std::string& message);
    private:
        std::shared_ptr<CollectorRuntime> owner_;
        std::string name_, kind_;
        size_t id_;
        int exceptions_;
        bool failed_ = false;
    };
    void configure(const std::string& name, const std::string& config,
                   const std::string& scope, double period_ms);
    void jobs(const std::string& name, size_t count);
    void timer(const std::string& name, bool active);
    void error(const std::string& name, const std::string& message);
    void lifecycle(const std::string& value);
    void schedulerStatusReader(std::function<nlohmann::json()> reader);
    nlohmann::json snapshot() const;
    nlohmann::json collector(const nlohmann::json& params) const;
private:
    mutable std::mutex mutex_;
    std::string lifecycle_ = "starting";
    std::map<std::string, Entry> entries_;
    std::function<nlohmann::json()> scheduler_status_;
};
