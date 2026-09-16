#include "core/collector_runtime.hpp"
#include <exception>

namespace {
int64_t wallNow() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
double elapsed(CollectorRuntime::Clock::time_point start,
               CollectorRuntime::Clock::time_point now) {
    return std::chrono::duration<double, std::milli>(now - start).count();
}
nlohmann::json serialize(const CollectorRuntime::Entry& entry,
                         CollectorRuntime::Clock::time_point now) {
    auto metrics = nlohmann::json::object();
    for (const auto& [kind, metric] : entry.metrics) {
        auto active = nlohmann::json::array();
        for (const auto& [id, start] : metric.active) active.push_back(elapsed(start, now));
        metrics[kind] = {{"success_count", metric.success}, {"error_count", metric.errors},
            {"active_count", metric.active.size()}, {"in_progress_elapsed_ms", active},
            {"last_duration_ms", metric.last_duration_ms},
            {"last_success_unix_ms", metric.last_success_unix_ms
                ? nlohmann::json(*metric.last_success_unix_ms) : nlohmann::json(nullptr)}};
    }
    std::string state = entry.failed ? "error" : (entry.timer_active ? "scheduled" : "idle");
    if (entry.phase != "idle") state = entry.phase;
    else if (!entry.metrics.at("callback").active.empty()) state = "running";
    return {{"name", entry.name}, {"scope", entry.scope}, {"config", entry.config},
        {"state", state}, {"has_error", entry.failed}, {"period_ms", entry.period_ms},
        {"job_count", entry.job_count}, {"timer_active", entry.timer_active},
        {"active_callback_count", entry.metrics.at("callback").active.size()},
        {"last_error", entry.last_error}, {"operations", metrics}};
}
}
void CollectorRuntime::configure(const std::string& name, const std::string& config,
                                 const std::string& scope, double period_ms) {
    std::lock_guard lock(mutex_);
    auto& entry = entries_[name];
    entry.name = name;
    entry.config = config;
    entry.scope = scope;
    entry.period_ms = period_ms;
    for (const auto* kind : {"init", "deinit", "callback", "collect", "write"})
        entry.metrics.try_emplace(kind);
}
void CollectorRuntime::jobs(const std::string& name, size_t count) {
    std::lock_guard lock(mutex_);
    entries_.at(name).job_count = count;
}
void CollectorRuntime::timer(const std::string& name, bool active) {
    std::lock_guard lock(mutex_);
    entries_.at(name).timer_active = active;
}
void CollectorRuntime::error(const std::string& name, const std::string& message) {
    std::lock_guard lock(mutex_);
    auto& entry = entries_.at(name);
    entry.failed = true;
    entry.last_error = message.substr(0, 512);
    for (auto& ch : entry.last_error)
        if (static_cast<unsigned char>(ch) >= 128) ch = '?';
}
void CollectorRuntime::lifecycle(const std::string& value) {
    std::lock_guard lock(mutex_);
    lifecycle_ = value;
    if (value == "stopped")
        for (auto& [name, entry] : entries_) entry.timer_active = false;
}
nlohmann::json CollectorRuntime::snapshot() const {
    std::map<std::string, Entry> entries;
    std::string lifecycle;
    Clock::time_point now;
    {
        std::lock_guard lock(mutex_);
        entries = entries_;
        lifecycle = lifecycle_;
        now = Clock::now();
    }
    auto collectors = nlohmann::json::array();
    size_t active = 0, errors = 0;
    for (const auto& [name, entry] : entries) {
        collectors.push_back(serialize(entry, now));
        active += entry.metrics.at("callback").active.size();
        errors += entry.failed;
    }
    return {{"status", "ok"}, {"lifecycle", lifecycle},
        {"observed_at_unix_ms", wallNow()}, {"collector_count", entries.size()},
        {"collectors_with_error", errors}, {"active_callback_count", active},
        {"collectors", collectors}};
}
nlohmann::json CollectorRuntime::collector(const nlohmann::json& params) const {
    if (!params.is_object() || !params.contains("name") || !params["name"].is_string())
        return {{"status", "error"}, {"code", "invalid_params"}, {"msg", "Expected string parameter 'name'"}};
    const auto name = params["name"].get<std::string>();
    Entry entry;
    Clock::time_point now;
    {
        std::lock_guard lock(mutex_);
        const auto it = entries_.find(name);
        if (it == entries_.end())
            return {{"status", "error"}, {"code", "unknown_collector"}, {"msg", "Unknown collector: " + name}};
        entry = it->second;
        now = Clock::now();
    }
    return {{"status", "ok"}, {"collector", serialize(entry, now)}, {"observed_at_unix_ms", wallNow()}};
}
CollectorRuntime::Operation::Operation(std::shared_ptr<CollectorRuntime> owner,
                                      std::string name, std::string kind)
    : owner_(std::move(owner)), name_(std::move(name)), kind_(std::move(kind)),
      exceptions_(std::uncaught_exceptions()) {
    std::lock_guard lock(owner_->mutex_);
    auto& entry = owner_->entries_.at(name_);
    auto& metric = entry.metrics.at(kind_);
    id_ = metric.next++;
    metric.active.emplace(id_, Clock::now());
    if (kind_ == "init") entry.phase = "initializing";
    if (kind_ == "deinit") entry.phase = "deinitializing";
}
void CollectorRuntime::Operation::fail(const std::string& message) {
    failed_ = true;
    owner_->error(name_, message);
}
CollectorRuntime::Operation::~Operation() {
    std::lock_guard lock(owner_->mutex_);
    auto& entry = owner_->entries_.at(name_);
    auto& metric = entry.metrics.at(kind_);
    metric.last_duration_ms = elapsed(metric.active.at(id_), Clock::now());
    metric.active.erase(id_);
    if (failed_ || std::uncaught_exceptions() > exceptions_) {
        ++metric.errors;
        entry.failed = true;
        if (!failed_) entry.last_error = "Operation threw an exception";
    } else {
        ++metric.success;
        metric.last_success_unix_ms = wallNow();
        // 空扫描不能掩盖初始化或采集错误；历史错误文本和计数保留。
        if (kind_ == "init" || kind_ == "write") entry.failed = false;
    }
    if (kind_ == "init" || kind_ == "deinit") entry.phase = "idle";
}
