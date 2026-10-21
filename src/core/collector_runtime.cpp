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
                         CollectorRuntime::Clock::time_point now,
                         const nlohmann::json& scheduling) {
    nlohmann::json current_job = nullptr;
    auto metrics = nlohmann::json::object();
    for (const auto& [kind, metric] : entry.metrics) {
        auto active = nlohmann::json::array();
        auto in_progress = nlohmann::json::array();
        for (const auto& [id, operation] : metric.active) {
            const double duration = elapsed(operation.started, now);
            active.push_back(duration);
            auto detail = nlohmann::json{{"job_id", operation.job_id
                ? nlohmann::json(*operation.job_id) : nlohmann::json(nullptr)}, {"elapsed_ms", duration}};
            in_progress.push_back(detail);
            if (operation.job_id) {
                current_job = detail;
                current_job["stage"] = kind;
            }
        }
        metrics[kind] = {{"success_count", metric.success}, {"error_count", metric.errors},
            {"active_count", metric.active.size()}, {"in_progress_elapsed_ms", active},
            {"last_duration_ms", metric.last_duration_ms}, {"in_progress", in_progress},
            {"last_job_id", metric.last_job_id ? nlohmann::json(*metric.last_job_id) : nlohmann::json(nullptr)},
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
        {"last_error", entry.last_error}, {"operations", metrics},
        {"current_job", current_job}, {"scheduling", scheduling}};
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
    for (const auto* kind : {"init", "deinit", "callback", "lookup_job", "wait_collector_lock", "collect", "write"})
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
void CollectorRuntime::schedulerStatusReader(std::function<nlohmann::json()> reader) {
    std::lock_guard lock(mutex_);
    scheduler_status_ = std::move(reader);
}

namespace {
std::map<std::string, nlohmann::json> schedules(const nlohmann::json& scheduler) {
    std::map<std::string, nlohmann::json> result;
    if (scheduler.is_object()) {
        for (const auto& task : scheduler.at("tasks")) {
            auto& slot = result[task.at("name").get<std::string>()];
            // A cancelled callback can be finishing while a replacement timer is registered.
            if (slot.is_null() || slot.at("state") != "running") slot = task;
        }
    }
    return result;
}
}

nlohmann::json CollectorRuntime::snapshot() const {
    std::map<std::string, Entry> entries;
    std::string lifecycle;
    std::function<nlohmann::json()> reader;
    Clock::time_point now;
    {
        std::lock_guard lock(mutex_);
        entries = entries_;
        lifecycle = lifecycle_;
        reader = scheduler_status_;
        now = Clock::now();
    }
    const auto scheduler = reader ? reader() : nlohmann::json(nullptr);
    auto scheduling = schedules(scheduler);
    auto collectors = nlohmann::json::array();
    size_t active = 0, errors = 0;
    for (const auto& [name, entry] : entries) {
        collectors.push_back(serialize(entry, now, scheduling[name]));
        active += entry.metrics.at("callback").active.size();
        errors += entry.failed;
    }
    return {{"status", "ok"}, {"lifecycle", lifecycle},
        {"observed_at_unix_ms", wallNow()}, {"collector_count", entries.size()},
        {"collectors_with_error", errors}, {"active_callback_count", active},
        {"collectors", collectors}, {"scheduler", scheduler}};
}
nlohmann::json CollectorRuntime::collector(const nlohmann::json& params) const {
    if (!params.is_object() || !params.contains("name") || !params["name"].is_string())
        return {{"status", "error"}, {"code", "invalid_params"}, {"msg", "Expected string parameter 'name'"}};
    const auto name = params["name"].get<std::string>();
    Entry entry;
    std::function<nlohmann::json()> reader;
    Clock::time_point now;
    {
        std::lock_guard lock(mutex_);
        const auto it = entries_.find(name);
        if (it == entries_.end())
            return {{"status", "error"}, {"code", "unknown_collector"}, {"msg", "Unknown collector: " + name}};
        entry = it->second;
        reader = scheduler_status_;
        now = Clock::now();
    }
    auto scheduling = schedules(reader ? reader() : nlohmann::json(nullptr));
    return {{"status", "ok"}, {"collector", serialize(entry, now, scheduling[name])}, {"observed_at_unix_ms", wallNow()}};
}
CollectorRuntime::Operation::Operation(std::shared_ptr<CollectorRuntime> owner,
                                      std::string name, std::string kind, std::optional<uint64_t> job_id)
    : owner_(std::move(owner)), name_(std::move(name)), kind_(std::move(kind)),
      exceptions_(std::uncaught_exceptions()) {
    std::lock_guard lock(owner_->mutex_);
    auto& entry = owner_->entries_.at(name_);
    auto& metric = entry.metrics.at(kind_);
    id_ = metric.next++;
    metric.active.emplace(id_, ActiveOperation{Clock::now(), job_id});
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
    metric.last_duration_ms = elapsed(metric.active.at(id_).started, Clock::now());
    metric.last_job_id = metric.active.at(id_).job_id;
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
