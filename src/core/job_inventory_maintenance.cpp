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
#include "core/job_inventory_maintenance.hpp"

#include "common/utils.hpp"
#include "core/inventory_status_provider.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

namespace {

// ---------------------------------------------------------------------------
// Status-provider seam storage
// ---------------------------------------------------------------------------

std::mutex& provider_mutex()
{
    static std::mutex mutex;
    return mutex;
}

std::function<nlohmann::json()>& provider_slot()
{
    static std::function<nlohmann::json()> provider;
    return provider;
}

std::function<bool()>& refresh_slot()
{
    static std::function<bool()> provider;
    return provider;
}

// ---------------------------------------------------------------------------
// Configuration helpers
// ---------------------------------------------------------------------------

[[noreturn]] void config_error(const std::string& message)
{
    throw std::invalid_argument("inventory configuration: " + message);
}

const nlohmann::json* member(const nlohmann::json& node, const char* key)
{
    if (!node.is_object()) return nullptr;
    const auto it = node.find(key);
    if (it == node.end() || it->is_null()) return nullptr;
    return &*it;
}

std::string scalar_text(const nlohmann::json& node, const std::string& path)
{
    if (node.is_string()) return node.get<std::string>();
    if (node.is_boolean()) return node.get<bool>() ? "true" : "false";
    if (node.is_number_integer()) return std::to_string(node.get<long long>());
    if (node.is_number_unsigned()) return std::to_string(node.get<unsigned long long>());
    if (node.is_number_float()) {
        std::ostringstream out;
        out << node.get<double>();
        return out.str();
    }
    config_error(path + " must be a scalar value");
}

bool parse_bool(const nlohmann::json& node, const std::string& path)
{
    if (node.is_boolean()) return node.get<bool>();
    if (node.is_number_integer()) {
        const auto value = node.get<long long>();
        if (value == 0) return false;
        if (value == 1) return true;
        config_error(path + " must be a boolean");
    }
    if (node.is_number_unsigned()) {
        const auto value = node.get<unsigned long long>();
        if (value == 0) return false;
        if (value == 1) return true;
        config_error(path + " must be a boolean");
    }
    const std::string text = scalar_text(node, path);
    if (text == "true" || text == "True" || text == "TRUE" || text == "yes" || text == "1")
        return true;
    if (text == "false" || text == "False" || text == "FALSE" || text == "no" || text == "0")
        return false;
    config_error(path + " must be a boolean (true/false), got '" + text + "'");
}

std::uint32_t parse_interval_sec(const nlohmann::json& node, const std::string& path)
{
    long long value = 0;
    if (node.is_number_unsigned()) {
        const auto raw = node.get<unsigned long long>();
        if (raw > 0xffffffffull) config_error(path + " is out of range");
        value = static_cast<long long>(raw);
    } else if (node.is_number_integer()) {
        value = node.get<long long>();
    } else if (node.is_number_float()) {
        config_error(path + " must be an integer number of seconds");
    } else {
        const std::string text = scalar_text(node, path);
        if (text.empty()) config_error(path + " must be an integer number of seconds");
        const bool digits = std::all_of(text.begin(), text.end(), [](unsigned char c) {
            return std::isdigit(c) != 0;
        });
        if (!digits)
            config_error(path + " must be an integer number of seconds, got '" + text + "'");
        try {
            value = std::stoll(text);
        } catch (const std::out_of_range&) {
            config_error(path + " is out of range");
        }
    }
    if (value < 1) config_error(path + " must be at least 1 second");
    return static_cast<std::uint32_t>(value);
}

std::string scalar_field(const nlohmann::json& node, const char* key, const std::string& path,
                         const std::string& fallback)
{
    const auto* child = member(node, key);
    if (child == nullptr) return fallback;
    return scalar_text(*child, path + "." + key);
}

// ---------------------------------------------------------------------------
// Process liveness helpers
// ---------------------------------------------------------------------------

// Reads field 22 (start time in clock ticks) of /proc/<pid>/stat, keeping errno
// so the caller can tell "gone" from "not allowed to look".
enum class StatRead {
    Ok = 0,
    Gone,
    Denied,
    Error,
};

StatRead read_start_ticks_raw(const std::string& proc_root, pid_t pid, std::uint64_t& ticks)
{
    const std::string path = proc_root + "/" + std::to_string(pid) + "/stat";
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT || errno == ESRCH) return StatRead::Gone;
        if (errno == EACCES || errno == EPERM) return StatRead::Denied;
        return StatRead::Error;
    }
    std::string content;
    char buffer[4096];
    while (true) {
        const ssize_t read_bytes = ::read(fd, buffer, sizeof(buffer));
        if (read_bytes < 0) {
            const int saved = errno;
            ::close(fd);
            if (saved == ENOENT || saved == ESRCH) return StatRead::Gone;
            if (saved == EACCES || saved == EPERM) return StatRead::Denied;
            return StatRead::Error;
        }
        if (read_bytes == 0) break;
        content.append(buffer, static_cast<std::size_t>(read_bytes));
    }
    ::close(fd);

    const auto last_rparen = content.rfind(')');
    if (last_rparen == std::string::npos) return StatRead::Error;

    std::istringstream fields(content.substr(last_rparen + 2));
    std::string field;
    // Field 3 (state) is first here; field 22 is index 19.
    for (int index = 3; index <= 22; ++index) {
        if (!(fields >> field)) return StatRead::Error;
        if (index == 22) {
            try {
                ticks = std::stoull(field);
            } catch (const std::exception&) {
                return StatRead::Error;
            }
            return StatRead::Ok;
        }
    }
    return StatRead::Error;
}

}  // namespace

// ---------------------------------------------------------------------------
// Status-provider seam
// ---------------------------------------------------------------------------

void set_inventory_status_provider(std::function<nlohmann::json()> provider)
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    provider_slot() = std::move(provider);
}

std::function<nlohmann::json()> get_inventory_status_provider()
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    return provider_slot();
}

void set_inventory_refresh_provider(std::function<bool()> provider)
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    refresh_slot() = std::move(provider);
}

std::function<bool()> get_inventory_refresh_provider()
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    return refresh_slot();
}

// ---------------------------------------------------------------------------
// Configuration contract (plan section 8)
// ---------------------------------------------------------------------------

const char* to_string(InventoryType type)
{
    switch (type) {
        case InventoryType::Condor: return "condor";
        case InventoryType::Slurm: return "slurm";
    }
    return "unknown";
}

std::optional<InventoryType> inventory_type_from_string(const std::string& raw)
{
    std::string normalized;
    normalized.reserve(raw.size());
    for (const char c : raw) {
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') continue;
        normalized.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    if (normalized == "condor") return InventoryType::Condor;
    if (normalized == "slurm") return InventoryType::Slurm;
    return std::nullopt;
}

nlohmann::json InventoryMaintenanceConfig::to_json() const
{
    nlohmann::json types_json = nlohmann::json::array();
    for (const auto type : types) types_json.push_back(to_string(type));

    nlohmann::json out;
    out["types"] = std::move(types_json);
    out["reconcile_interval_sec"] = reconcile_interval_sec;
    nlohmann::json sources = nlohmann::json::array();
    for (const auto& entry : settings) {
        sources.push_back(nlohmann::json{
            {"type", to_string(entry.first)},
            {"inventory", entry.second.inventory},
            {"auto_collect", entry.second.auto_collect},
            {"auto_collect_explicit", entry.second.auto_collect_explicit},
            {"legacy_auto_collect", entry.second.legacy_auto_collect},
            {"use_rules", entry.second.use_rules},
            {"rules_dir", entry.second.rules_dir},
            {"rules_prefix", entry.second.rules_prefix},
        });
    }
    out["sources"] = std::move(sources);
    return out;
}

InventoryMaintenanceConfig resolve_inventory_maintenance_config(const nlohmann::json& config)
{
    if (!config.is_object())
        config_error("the document root must be a mapping");

    const auto* registry = member(config, "job_registry_config");
    static const nlohmann::json empty_object = nlohmann::json::object();
    const nlohmann::json& registry_node = registry != nullptr ? *registry : empty_object;
    if (!registry_node.is_object())
        config_error("job_registry_config must be a mapping");

    InventoryMaintenanceConfig resolved;
    for (const auto type : {InventoryType::Condor, InventoryType::Slurm}) {
        InventoryTypeSettings settings;
        settings.legacy_auto_collect = [&]() {
            const char* key = type == InventoryType::Condor ? "auto_add_condorjob"
                                                            : "auto_add_slurmjob";
            const auto* node = member(registry_node, key);
            if (node == nullptr) return false;
            return parse_bool(*node, std::string("job_registry_config.") + key);
        }();
        resolved.settings.emplace(type, settings);
    }

    const auto* inventory = member(registry_node, "inventory");
    if (inventory != nullptr) {
        if (!inventory->is_object())
            config_error("job_registry_config.inventory must be a mapping");

        if (const auto* types = member(*inventory, "types")) {
            if (!types->is_array())
                config_error("job_registry_config.inventory.types must be a list");
            for (const auto& entry : *types) {
                const std::string raw = scalar_text(
                    entry, "job_registry_config.inventory.types[]");
                const auto type = inventory_type_from_string(raw);
                if (!type) {
                    config_error("unknown inventory type '" + raw +
                                 "' (expected condor or slurm)");
                }
                if (std::find(resolved.types.begin(), resolved.types.end(), *type) ==
                    resolved.types.end()) {
                    resolved.types.push_back(*type);
                }
            }
        }

        if (const auto* interval = member(*inventory, "reconcile_interval_sec")) {
            resolved.reconcile_interval_sec = parse_interval_sec(
                *interval, "job_registry_config.inventory.reconcile_interval_sec");
        }
    }

    const std::string legacy_rules_dir =
        scalar_field(registry_node, "rules_dir", "job_registry_config", std::string());

    for (const auto type : resolved.types) {
        resolved.settings[type].inventory = true;
    }

    for (const auto type : {InventoryType::Condor, InventoryType::Slurm}) {
        const char* section = type == InventoryType::Condor ? "condor_job_watcher"
                                                            : "slurm_job_watcher";
        const std::string prefix = std::string(section) + ".";
        const auto* node = member(config, section);
        if (node != nullptr && !node->is_object())
            config_error(std::string(section) + " must be a mapping");

        auto& settings = resolved.settings[type];
        if (node != nullptr) {
            if (const auto* auto_collect = member(*node, "auto_collect")) {
                settings.auto_collect_explicit = true;
                settings.auto_collect = parse_bool(*auto_collect, prefix + "auto_collect");
            }
            if (const auto* use_rules = member(*node, "use_rules")) {
                settings.use_rules = parse_bool(*use_rules, prefix + "use_rules");
            }
            settings.rules_prefix = scalar_field(
                *node, "rules_prefix", section,
                std::string(type == InventoryType::Condor ? "condor_job_" : "slurm_job_"));
            const std::string fallback_rules_dir =
                Utils::JobLensRootDir() + "../config/rules/" +
                (type == InventoryType::Condor ? "condor_jobs" : "slurm_jobs");
            // Per-type rules_dir wins over the legacy shared one, otherwise the
            // existing per-type default stays in place (plan section 8).
            settings.rules_dir = scalar_field(*node, "rules_dir", section,
                                              legacy_rules_dir.empty() ? fallback_rules_dir
                                                                       : legacy_rules_dir);
        } else {
            settings.rules_prefix = type == InventoryType::Condor ? "condor_job_"
                                                                  : "slurm_job_";
            settings.rules_dir = legacy_rules_dir.empty()
                                     ? Utils::JobLensRootDir() + "../config/rules/" +
                                           (type == InventoryType::Condor ? "condor_jobs"
                                                                          : "slurm_jobs")
                                     : legacy_rules_dir;
        }
        if (!settings.auto_collect_explicit) {
            settings.auto_collect = settings.legacy_auto_collect;
            if (settings.legacy_auto_collect) {
                spdlog::warn("inventory configuration: job_registry_config.auto_add_{}job is "
                             "deprecated; set {}.auto_collect instead",
                             type == InventoryType::Condor ? "condor" : "slurm", section);
            }
        }
    }

    if (!legacy_rules_dir.empty()) {
        spdlog::warn("inventory configuration: job_registry_config.rules_dir is deprecated; "
                     "set condor_job_watcher.rules_dir / slurm_job_watcher.rules_dir");
    }
    return resolved;
}

// ---------------------------------------------------------------------------
// Liveness probes
// ---------------------------------------------------------------------------

const char* to_string(LivenessState state)
{
    switch (state) {
        case LivenessState::Alive: return "alive";
        case LivenessState::Dead: return "dead";
        case LivenessState::Unknown: break;
    }
    return "unknown";
}

ProcessLivenessProbe make_proc_liveness_probe(const std::string& proc_root)
{
    return [proc_root](const ProcessIdentity& identity) -> LivenessResult {
        if (identity.pid <= 0) {
            return LivenessResult{LivenessState::Dead, "no pid"};
        }
        std::uint64_t ticks = 0;
        switch (read_start_ticks_raw(proc_root, identity.pid, ticks)) {
            case StatRead::Gone:
                return LivenessResult{LivenessState::Dead, "process exited"};
            case StatRead::Denied:
                return LivenessResult{LivenessState::Unknown,
                                      "EPERM reading " + proc_root + "/" +
                                          std::to_string(identity.pid) + "/stat"};
            case StatRead::Error:
                return LivenessResult{LivenessState::Unknown,
                                      "unreadable " + proc_root + "/" +
                                          std::to_string(identity.pid) + "/stat"};
            case StatRead::Ok:
                break;
        }
        if (identity.start_ticks != 0 && ticks != identity.start_ticks) {
            // The pid was reused: this is a different process, so the observed
            // execution is gone even though the number is alive.
            return LivenessResult{LivenessState::Dead, "pid reused (start ticks differ)"};
        }
        return LivenessResult{LivenessState::Alive, "pid live"};
    };
}

CgroupMembershipProbe make_cgroup_membership_probe()
{
    return [](const std::string& cgroup_path) -> CgroupMembership {
        CgroupMembership result;
        if (cgroup_path.empty()) {
            result.error = "no cgroup path";
            return result;
        }
        const std::string procs = cgroup_path + "/cgroup.procs";
        const int fd = ::open(procs.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            result.error = std::string("cannot read ") + procs + ": " + std::strerror(errno);
            return result;
        }
        std::string content;
        char buffer[4096];
        while (true) {
            const ssize_t read_bytes = ::read(fd, buffer, sizeof(buffer));
            if (read_bytes < 0) {
                result.error = std::string("read error on ") + procs + ": " +
                               std::strerror(errno);
                ::close(fd);
                return result;
            }
            if (read_bytes == 0) break;
            content.append(buffer, static_cast<std::size_t>(read_bytes));
        }
        ::close(fd);
        std::istringstream lines(content);
        std::string line;
        while (std::getline(lines, line)) {
            if (line.empty()) continue;
            try {
                result.members.push_back(static_cast<pid_t>(std::stol(line)));
            } catch (const std::exception&) {
                result.error = "unparsable cgroup member '" + line + "'";
                result.members.clear();
                return result;
            }
        }
        std::sort(result.members.begin(), result.members.end());
        result.readable = true;
        return result;
    };
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

namespace {
constexpr int kAbsencesBeforeRetirement = 2;
constexpr std::size_t kEndedGenerationCapacity = 4096;
}  // namespace

JobInventoryMaintenance::JobInventoryMaintenance(JobRegistry& registry,
                                                 InventoryMaintenanceConfig config,
                                                 std::vector<InventorySource> sources)
    : JobInventoryMaintenance(registry, std::move(config), std::move(sources), Options{})
{
}

JobInventoryMaintenance::JobInventoryMaintenance(JobRegistry& registry,
                                                 InventoryMaintenanceConfig config,
                                                 std::vector<InventorySource> sources,
                                                 Options options)
    : registry_(registry),
      config_(std::move(config)),
      sources_(std::move(sources)),
      options_(options),
      process_probe_(make_proc_liveness_probe()),
      cgroup_probe_(make_cgroup_membership_probe())
{
    if (config_.reconcile_interval_sec < 1) {
        throw std::invalid_argument(
            "inventory maintenance: reconcile_interval_sec must be at least 1 second");
    }
    if (options_.queue_capacity == 0) options_.queue_capacity = 1;
    if (options_.wait_slice_ms == 0) options_.wait_slice_ms = 100;
    for (const auto& source : sources_) {
        if (source.scheduler_type == SchedulerType::Unknown) {
            throw std::invalid_argument(
                "inventory maintenance: source '" + source.name +
                "' has no scheduler type");
        }
    }
}

JobInventoryMaintenance::~JobInventoryMaintenance()
{
    // The service lifecycle stops the controller explicitly. A controller that
    // was never stopped is stopped here, but one that already went through
    // stop() must not touch the registry again: static destruction may run the
    // store first, and a callback after DB close is exactly what plan 5.3
    // forbids.
    if (!explicit_stop_done_) {
        spdlog::warn("InventoryMaintenance: destroyed without an explicit stop; stopping now");
        stop();
    }
}

void JobInventoryMaintenance::setLivenessProbes(ProcessLivenessProbe process_probe,
                                               CgroupMembershipProbe cgroup_probe)
{
    std::lock_guard<std::mutex> lock(mutex_);
    process_probe_ = std::move(process_probe);
    cgroup_probe_ = std::move(cgroup_probe);
}

void JobInventoryMaintenance::setClock(ScanClock clock)
{
    std::lock_guard<std::mutex> lock(mutex_);
    clock_ = std::move(clock);
}

std::int64_t JobInventoryMaintenance::clockNow() const
{
    return scan_clock_now(clock_);
}

bool JobInventoryMaintenance::start()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (admitted_ && running_) return true;
    }

    const auto store = registry_.storeHealth();
    if (!store.db_running) {
        noteError("inventory store is not running");
        spdlog::error("InventoryMaintenance: refusing to start, the inventory store is not running");
        return false;
    }

    spdlog::info("InventoryMaintenance: resolved configuration {}",
                 config_.to_json().dump());

    {
        std::lock_guard<std::mutex> lock(mutex_);
        admitted_ = true;
        stopping_ = false;
        running_ = false;
        explicit_stop_done_ = false;
        worker_id_ = std::thread::id();
        hint_wakeup_ = false;
        degraded_ = false;
        incomplete_drain_ = false;
        last_error_.clear();
        queue_.clear();
        tracked_.clear();
        suspect_.clear();
        ended_.clear();
        closing_.clear();
        last_scan_start_ms_ = 0;
        last_scan_success_ms_ = 0;
        last_scan_complete_ = false;
        passes_ = 0;
    }

    // Plan 5.1 step 3/4: the sink exists before any source starts, and the
    // initial full scan runs after every source is running.
    const ObservationSink sink = [this](const Observation& observation) {
        onObservation(observation);
    };
    source_started_.assign(sources_.size(), false);
    source_start_note_.assign(sources_.size(), std::string());
    for (std::size_t index = 0; index < sources_.size(); ++index) {
        auto& source = sources_[index];
        if (!source.start) {
            source_start_note_[index] = "scan-only source (no event callback)";
            spdlog::info("InventoryMaintenance: source '{}' is scan-only", source.name);
            continue;
        }
        bool started = false;
        try {
            started = source.start(sink);
        } catch (const std::exception& error) {
            source_start_note_[index] = std::string("start failed: ") + error.what();
        }
        source_started_[index] = started;
        if (!started) {
            if (source_start_note_[index].empty())
                source_start_note_[index] = "event source unavailable (scan-only)";
            spdlog::warn("InventoryMaintenance: source '{}' has no event source, "
                         "continuing with scans only: {}",
                         source.name, source_start_note_[index]);
        }
    }

    // Plan 5.1 step 5/6: the initial pass retires stale records and publishes a
    // coherent snapshot before the periodic producer starts.
    runPass(true, true);
    set_inventory_status_provider([this]() { return statusJson(); });
    set_inventory_refresh_provider([this]() { return reconcileNow(); });
    registry_.setExitObserver([this](pid_t pid) { notifyExit(pid); });
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_published_ = true;
        if (!stopping_) {
            running_ = true;
            worker_ = std::thread([this]() { runWorker(); });
            worker_id_ = worker_.get_id();
        }
    }
    return true;
}

void JobInventoryMaintenance::stopDiscovery()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!admitted_ && !running_ && !worker_.joinable()) return;
        admitted_ = false;
        stopping_ = true;
    }
    wake_cv_.notify_all();

    // Plan 5.3: stop discovery polling before touching the producer thread.
    for (auto& source : sources_) {
        if (!source.stop) continue;
        try {
            source.stop();
        } catch (const std::exception& error) {
            spdlog::error("InventoryMaintenance: source '{}' stop failed: {}",
                          source.name, error.what());
            std::lock_guard<std::mutex> lock(mutex_);
            ++stop_errors_;
        }
    }

    if (worker_.joinable()) {
        if (worker_.get_id() == std::this_thread::get_id()) {
            // A callback from the maintenance thread must never self-join; the
            // shutdown is reported as incomplete instead of deadlocking.
            spdlog::error("InventoryMaintenance: stop() called from the maintenance "
                          "thread; detaching instead of self-joining");
            {
                std::lock_guard<std::mutex> lock(mutex_);
                ++stop_errors_;
            }
            worker_.detach();
        } else {
            worker_.join();
        }
    }
    bool on_maintenance_thread = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        on_maintenance_thread = worker_id_ != std::thread::id() &&
                                worker_id_ == std::this_thread::get_id();
        running_ = false;
        worker_id_ = std::thread::id();
    }

    // Wait for an explicit pass that was already in flight, so no pass can
    // touch the store after this returns (the stop path runs before the DB is
    // closed). A pass never waits for another pass, so this cannot deadlock.
    std::unique_lock<std::mutex> pass_guard(pass_mutex_, std::defer_lock);
    if (!on_maintenance_thread) pass_guard.lock();

    // "Finish queued bounded maintenance": accepted observations are committed
    // while the store is still open, never silently discarded.
    std::vector<PendingHint> hints;
    drainHints(hints);
    std::set<uint64_t> observed;
    for (auto& hint : hints) {
        if (hint.kind == PendingHint::Kind::Observation) {
            processObservation(hint.observation, observed);
        }
    }
    if (!hints.empty()) {
        spdlog::info("InventoryMaintenance: discovery stopped, {} buffered hint(s) completed",
                     hints.size());
    } else {
        spdlog::info("InventoryMaintenance: discovery stopped");
    }
}

bool JobInventoryMaintenance::finalize()
{
    std::vector<std::pair<uint64_t, ExecutionGeneration>> closing;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        closing.swap(closing_);
    }

    bool ok = true;
    for (auto& entry : closing) {
        const uint64_t internal_id = entry.first;
        const ExecutionGeneration& generation = entry.second;
        const auto record = registry_.inventoryRecord(internal_id);
        if (record && record->generation != generation) {
            // The internal id cannot be reused, but a stale acknowledgement
            // must still never speak for a newer execution.
            spdlog::info("InventoryMaintenance: JobID {} no longer carries the retired "
                         "generation; acknowledgement discarded", internal_id);
            continue;
        }
        if (!record) continue;  // already archived
        const bool archived = registry_.acknowledgeJobRemoval(internal_id);
        // hasInstance() only covers live jobs: a closing record left in
        // records_ is what proves the archive did not land.
        if (!archived && registry_.inventoryRecord(internal_id).has_value()) {
            ok = false;
            std::lock_guard<std::mutex> lock(mutex_);
            closing_.emplace_back(internal_id, generation);
        }
    }

    if (registry_.closingJobCount() > 0) ok = false;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // Report a shutdown that could not finish: an unarchived closing record
        // or a source/thread that failed to stop.
        incomplete_drain_ = !ok || stop_errors_ > 0;
    }
    if (!ok) {
        spdlog::error("InventoryMaintenance: shutdown could not finalize every retirement; "
                      "the incomplete closing state is persisted for recovery");
    }
    if (status_published_) {
        set_inventory_status_provider({});
        status_published_ = false;
    }
    set_inventory_refresh_provider({});
    registry_.setExitObserver({});
    {
        std::lock_guard<std::mutex> lock(mutex_);
        explicit_stop_done_ = true;
    }
    return ok;
}

void JobInventoryMaintenance::stop()
{
    stopDiscovery();
    finalize();
}

bool JobInventoryMaintenance::running() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return running_;
}

bool JobInventoryMaintenance::incompleteDrain() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return incomplete_drain_;
}

void JobInventoryMaintenance::onObservation(const Observation& observation)
{
    PendingHint hint;
    hint.kind = PendingHint::Kind::Observation;
    hint.observation = observation;
    hint.reason = "observation";
    enqueue(std::move(hint));
}

void JobInventoryMaintenance::notifyExit(pid_t pid)
{
    PendingHint hint;
    hint.kind = PendingHint::Kind::Exit;
    hint.pid = pid;
    hint.reason = "exit";
    enqueue(std::move(hint));
}

void JobInventoryMaintenance::notifyRefresh(const std::string& reason)
{
    PendingHint hint;
    hint.kind = PendingHint::Kind::Refresh;
    hint.reason = reason;
    enqueue(std::move(hint));
}

void JobInventoryMaintenance::enqueue(PendingHint hint)
{
    bool overflow = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++events_received_;
        if (!admitted_ || stopping_) {
            ++events_dropped_;
            return;
        }
        if (queue_.size() >= options_.queue_capacity) {
            ++events_dropped_;
            overflow = true;
        } else {
            queue_.push_back(std::move(hint));
            hint_wakeup_ = true;
        }
    }

    if (overflow) {
        // Bounded queue: the producer is never blocked; the drop is counted,
        // the sources are marked degraded and an authoritative rescan repairs
        // whatever the dropped hint would have refreshed.
        {
            std::lock_guard<std::mutex> lock(mutex_);
            degraded_ = true;
            last_error_ = "invariant hint queue overflow; awaiting authoritative rescan";
        }
        spdlog::warn("InventoryMaintenance: hint queue full ({} entries), dropping event; "
                     "requesting a rescan", options_.queue_capacity);
        requestRescan("hint queue overflow");
        return;
    }
    wake_cv_.notify_one();
}

bool JobInventoryMaintenance::reconcileNow()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // A callback running on the maintenance thread must never wait for a
        // pass it is itself inside of.
        if (worker_id_ != std::thread::id() &&
            worker_id_ == std::this_thread::get_id()) {
            return false;
        }
    }
    return runPass(true, true);
}

bool JobInventoryMaintenance::rescanRequested() const
{
    for (const auto& source : sources_) {
        if (!source.health) continue;
        try {
            if (source.health().rescan_requested) return true;
        } catch (const std::exception& error) {
            spdlog::debug("InventoryMaintenance: source '{}' health failed: {}",
                          source.name, error.what());
        }
    }
    return false;
}

void JobInventoryMaintenance::runWorker()
{
    const auto interval = std::chrono::milliseconds(
        static_cast<long long>(config_.reconcile_interval_sec) * 1000);
    const auto slice = std::chrono::milliseconds(
        static_cast<long long>(options_.wait_slice_ms));
    auto next_full = std::chrono::steady_clock::now() + interval;

    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
        // A slice-bounded wait keeps stop() bounded; hints are picked up on the
        // next slice (at most wait_slice_ms) without ever spinning.
        wake_cv_.wait_for(lock, slice, [this]() { return stopping_; });
        if (stopping_) break;

        const bool due = std::chrono::steady_clock::now() >= next_full;
        if (!due && !hint_wakeup_) {
            // Discovery events and queue overflows ask for an authoritative
            // pass. Reading source health needs no controller lock.
            lock.unlock();
            const bool rescan = rescanRequested();
            lock.lock();
            if (!rescan || stopping_) continue;
        }

        lock.unlock();
        bool executed = false;
        if (due) {
            executed = runPass(true, false);
            if (executed) next_full = std::chrono::steady_clock::now() + interval;
        } else if (rescanRequested()) {
            // Coalesce: one forced pass per second at most, so a job-start storm
            // cannot turn into a /proc scan storm.
            const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            const bool allowed = last_forced_scan_ms_ == 0 ||
                                 now_ms - last_forced_scan_ms_ >= 1000;
            if (allowed) {
                last_forced_scan_ms_ = now_ms;
                executed = runPass(true, false);
            } else {
                executed = runPass(false, false);
            }
        } else {
            executed = runPass(false, false);
        }
        lock.lock();
        // A pass already in flight (an explicit reconcileNow, for example)
        // owns the queued hints; retry on the next slice.
        if (!executed) hint_wakeup_ = true;
    }
}

bool JobInventoryMaintenance::runPass(bool do_scan, bool wait)
{
    // Passes are serialized by pass_mutex_. The worker never waits for a pass
    // (it retries on the next slice), while an explicit reconcileNow waits so
    // that returning means "a pass completed".
    std::unique_lock<std::mutex> pass_lock(pass_mutex_, std::defer_lock);
    if (wait) {
        pass_lock.lock();
    } else if (!pass_lock.try_lock()) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!admitted_ || stopping_) return false;
    }

    const std::int64_t started_ms = clockNow();
    std::vector<std::pair<SchedulerType, ScanResult>> results;
    std::map<SchedulerType, bool> source_complete;
    bool complete = true;
    std::string error;

    if (do_scan) {
        for (auto& source : sources_) {
            ScanResult result;
            if (source.scan) {
                try {
                    result = source.scan();
                } catch (const std::exception& exception) {
                    result.complete = false;
                    result.error = std::string("scan threw: ") + exception.what();
                }
            } else {
                result.complete = false;
                result.error = "source has no scan implementation";
            }
            if (!result.complete) {
                const std::string detail =
                    result.error.empty() ? "incomplete source pass" : result.error;
                if (error.empty()) {
                    error = source.name.empty() ? detail : source.name + ": " + detail;
                }
            }
            complete = complete && result.complete;
            const auto inserted = source_complete.emplace(source.scheduler_type, result.complete);
            if (!inserted.second) inserted.first->second = inserted.first->second && result.complete;
            results.emplace_back(source.scheduler_type, std::move(result));
        }
    }

    std::set<uint64_t> observed;
    for (const auto& entry : results) {
        for (const auto& observation : entry.second.observations) {
            processObservation(observation, observed);
        }
    }

    std::vector<PendingHint> hints;
    drainHints(hints);
    bool needs_authoritative_pass = false;
    for (auto& hint : hints) {
        if (hint.kind == PendingHint::Kind::Observation) {
            processObservation(hint.observation, observed);
        } else {
            // Exit/refresh hints are cheap by design: they ask for one
            // coalesced authoritative pass instead of doing discovery I/O on
            // the producing callback (plan 5.2).
            needs_authoritative_pass = true;
        }
    }
    if (needs_authoritative_pass) requestRescan("process/attribute hint");

    // Records no discovery source owns (legacy/manual registrations, or types
    // whose inventory source was removed) are refreshed here; reads never do it.
    if (do_scan) refreshLegacyRecords();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++passes_;
        if (do_scan) {
            last_scan_start_ms_ = started_ms;
            last_scan_complete_ = complete;
            if (complete) {
                last_scan_success_ms_ = clockNow();
                degraded_ = false;
                last_error_.clear();
            } else {
                degraded_ = true;
                last_error_ = error.empty() ? "source pass incomplete" : error;
            }
        }
    }

    // A targeted pass has no authoritative source coverage, so it never counts
    // as an absence; only positive termination evidence applies.
    static const std::map<SchedulerType, bool> no_coverage;
    evaluateRetirements(observed, do_scan ? source_complete : no_coverage);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // Only clear the wake-up when this pass consumed everything; a hint
        // that arrived after the drain must wake the next slice.
        if (queue_.empty()) hint_wakeup_ = false;
    }
    return true;
}

ObservedExecution JobInventoryMaintenance::to_execution(
    const Observation& observation) const
{
    ObservedExecution execution;
    execution.selector = observation.selector;
    execution.generation = observation.generation;
    execution.processes = observation.members;

    Job facts;
    facts.jobtype = JobType::Job;
    if (observation.scheduler_type == SchedulerType::Condor) {
        facts.subtype = JobSubType::Condor;
        CondorJobAttr attributes{};
        attributes.auto_update_child = false;
        attributes.starter_pid = observation.generation.manager_pid;
        attributes.scheduler_host = observation.scheduler_host;
        attributes.scheduler_name = observation.selector.cluster_tag;
        attributes.slots_cgroup_path = observation.cgroup_path;
        attributes.owner = observation.owner;
        attributes.collector_host = observation.selector.cluster_name;
        facts.sub_attr = attributes;
    } else if (observation.scheduler_type == SchedulerType::Slurm) {
        facts.subtype = JobSubType::Slurm;
        SlurmJobAttr attributes{};
        attributes.auto_update_child = false;
        attributes.stepd_pid = observation.generation.manager_pid;
        attributes.cluster_name = observation.selector.cluster_name;
        attributes.cgroup_path = observation.cgroup_path;
        attributes.user = observation.owner;
        attributes.job_name = observation.job_name;
        attributes.partition = observation.partition;
        attributes.step_id = slurm_step_value(observation.selector.step_id).value_or(0);
        try {
            attributes.job_id = std::stoull(observation.selector.native_job_id);
        } catch (const std::exception&) {
            // Canonicalization already rejected non-numeric Slurm ids; leaving
            // the field at 0 keeps the missing fact visible.
        }
        facts.sub_attr = attributes;
    }
    execution.facts = std::move(facts);
    return execution;
}

void JobInventoryMaintenance::processObservation(const Observation& observation,
                                                std::set<uint64_t>& observed_ids)
{
    const auto identity = canonicalize(observation.selector);
    if (identity && observation.generation.complete() &&
        isObsolete(*identity, observation.generation)) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++obsolete_hints_;
        spdlog::debug("InventoryMaintenance: discarded hint for the ended execution of '{}'",
                      identity->native_job_id);
        return;
    }

    if (!observation.identity_complete) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++incomplete_observations_;
        spdlog::debug("InventoryMaintenance: incomplete observation '{}': {}",
                      observation.selector.native_job_id, observation.incomplete_reason);
    }

    const auto result = registry_.upsertObservedJob(to_execution(observation));
    if (result.ok) {
        std::lock_guard<std::mutex> lock(mutex_);
        observed_ids.insert(result.internal_id);
        auto& tracked = tracked_[result.internal_id];
        tracked.generation = observation.generation;
        tracked.complete_absences = 0;
        tracked.last_seen_ms = clockNow();
        suspect_.erase(result.internal_id);
        return;
    }

    if (result.error == "incomplete_identity") {
        // An unresolvable identity is not an error of the pass: it stays out of
        // the identity index and is retried on the next observation.
        return;
    }

    if (result.error == "execution_generation_conflict" ||
        result.error == "identity_quarantined" ||
        result.error == "instance_closing" ||
        result.error == "identity_index_mismatch") {
        std::lock_guard<std::mutex> lock(mutex_);
        ++upsert_conflicts_;
        last_error_ = result.error + (result.message.empty() ? "" : ": " + result.message);
        spdlog::warn("InventoryMaintenance: observation for '{}' rejected: {}",
                     observation.selector.native_job_id, result.error);
        return;
    }

    noteError("inventory upsert failed: " + result.error +
              (result.message.empty() ? "" : ": " + result.message));
}

void JobInventoryMaintenance::refreshLegacyRecords()
{
    for (const auto& record : registry_.inventoryRecords()) {
        if (record.internal_id == kSystemSentinelJobId) continue;
        if (record.generation.complete()) continue;  // discovery owns these facts
        if (record.processes.empty()) continue;      // no liveness evidence yet
        registry_.refreshJobFacts(record.internal_id);
    }
}

void JobInventoryMaintenance::evaluateRetirements(
    const std::set<uint64_t>& observed_ids,
    const std::map<SchedulerType, bool>& source_complete)
{
    const auto records = registry_.inventoryRecords();
    const std::int64_t now = clockNow();

    for (const auto& record : records) {
        if (record.internal_id == 0) continue;
        if (isClosing(record.internal_id)) continue;

        int absences = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& tracked = tracked_[record.internal_id];
            if (!(tracked.generation == record.generation)) {
                tracked.generation = record.generation;
                tracked.complete_absences = 0;
            }
            if (observed_ids.count(record.internal_id) != 0) {
                tracked.complete_absences = 0;
                tracked.last_seen_ms = now;
                suspect_.erase(record.internal_id);
                continue;
            }
            const auto coverage = source_complete.find(record.selector.scheduler_type);
            const bool absence_allowed =
                coverage != source_complete.end() && coverage->second;
            if (absence_allowed && tracked.complete_absences < kAbsencesBeforeRetirement) {
                ++tracked.complete_absences;
            }
            absences = tracked.complete_absences;
            if (absences > 0) suspect_.insert(record.internal_id);
            else suspect_.erase(record.internal_id);
        }

        // Persisted lifecycle state is maintenance-owned; expose it through the
        // inventory API instead of keeping it only in this controller.
        if (absences > 0) {
            registry_.setInstanceState(record.internal_id, "suspect");
        } else if (record.state == "suspect") {
            registry_.setInstanceState(record.internal_id, "active");
        }

        // Records without a validated execution generation (legacy/manual
        // registrations, unresolved observations) have no manager to probe. The
        // refreshed process list is their only evidence: live or unreadable pids
        // keep the record, an all-dead list retires it exactly like the old
        // read-time cleanup did.
        if (!record.generation.complete()) {
            std::vector<ProcessIdentity> pids = record.processes;
            if (pids.empty()) {
                if (const auto job = registry_.findJob(record.internal_id)) {
                    for (const auto pid : job->JobPIDs) pids.push_back(ProcessIdentity{pid, 0, ""});
                }
            }
            if (pids.empty()) continue;  // no evidence: retry on the next pass

            bool any_live = false;
            for (const auto& process : pids) {
                const LivenessResult liveness = process_probe_
                                                    ? process_probe_(process)
                                                    : LivenessResult{};
                if (liveness.state != LivenessState::Dead) {
                    any_live = true;
                    break;
                }
            }
            if (!any_live)
                retireIfConfirmed(
                    record, "no live process for a legacy/manual registration");
            continue;
        }

        const ProcessIdentity manager{record.generation.manager_pid,
                                      record.generation.manager_start_ticks,
                                      record.generation.boot_id};
        const LivenessResult manager_liveness = process_probe_
                                                    ? process_probe_(manager)
                                                    : LivenessResult{};

        // A live manager always wins, whatever the PID list or cgroup says.
        if (manager_liveness.state == LivenessState::Alive) continue;

        bool cgroup_known = false;
        bool cgroup_live = false;
        if (!record.generation.cgroup_identity.empty() && cgroup_probe_) {
            const CgroupMembership membership =
                cgroup_probe_(record.generation.cgroup_identity);
            cgroup_known = membership.readable;
            if (membership.readable) {
                for (const pid_t pid : membership.members) {
                    const LivenessResult member = process_probe_
                                                      ? process_probe_(ProcessIdentity{pid, 0, ""})
                                                      : LivenessResult{};
                    if (member.state == LivenessState::Alive) {
                        cgroup_live = true;
                        break;
                    }
                }
            }
        }

        if (manager_liveness.state == LivenessState::Dead && cgroup_known && !cgroup_live) {
            retireIfConfirmed(
                record,
                "positive termination evidence (manager ended, execution cgroup empty)");
            continue;
        }

        if (absences < kAbsencesBeforeRetirement) continue;
        // Unknown is not death: an unreadable manager blocks the absence rule
        // exactly like a live one does.
        if (manager_liveness.state != LivenessState::Dead) continue;
        if (cgroup_live) continue;

        bool live_member = false;
        for (const auto& member : record.processes) {
            if (!member.complete()) continue;  // unvalidated, never proof of life
            const LivenessResult liveness = process_probe_ ? process_probe_(member)
                                                           : LivenessResult{};
            if (liveness.state == LivenessState::Alive) {
                live_member = true;
                break;
            }
        }
        if (live_member) continue;

        retireIfConfirmed(
            record,
            "two complete source passes without an observation and no live member");
    }

    pruneTracked(records);
}

bool JobInventoryMaintenance::retireIfConfirmed(const InventoryRecord& record,
                                               const std::string& reason)
{
    // Plan 5.2: only retire the generation the evidence was gathered for. A
    // late decision for an older execution must never remove newer ownership.
    const auto current = registry_.inventoryRecord(record.internal_id);
    if (!current) return false;
    if (!(current->generation == record.generation)) {
        spdlog::info("InventoryMaintenance: JobID {} changed execution generation before "
                     "retirement; keeping current ownership", record.internal_id);
        return false;
    }

    rememberEnded(record);
    if (!registry_.retireInstance(record.internal_id)) {
        noteError("inventory retirement failed for JobID " +
                  std::to_string(record.internal_id));
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++retirements_;
        closing_.emplace_back(record.internal_id, record.generation);
        tracked_.erase(record.internal_id);
        suspect_.erase(record.internal_id);
    }
    spdlog::info("InventoryMaintenance: retired JobID {}: {}", record.internal_id, reason);
    return true;
}

void JobInventoryMaintenance::rememberEnded(const InventoryRecord& record)
{
    const auto identity = canonicalize(record.selector);
    if (!identity) return;
    std::lock_guard<std::mutex> lock(mutex_);
    ended_[identity->key()] = record.generation;
    while (ended_.size() > kEndedGenerationCapacity) ended_.erase(ended_.begin());
}

bool JobInventoryMaintenance::isObsolete(const CanonicalIdentity& identity,
                                         const ExecutionGeneration& generation) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = ended_.find(identity.key());
    return it != ended_.end() && it->second == generation;
}

void JobInventoryMaintenance::drainHints(std::vector<PendingHint>& out)
{
    std::lock_guard<std::mutex> lock(mutex_);
    out.reserve(out.size() + queue_.size());
    while (!queue_.empty()) {
        out.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
}

void JobInventoryMaintenance::requestRescan(const std::string& reason)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++rescan_requests_;
    }
    for (auto& source : sources_) {
        if (!source.request_rescan) continue;
        try {
            source.request_rescan();
        } catch (const std::exception& error) {
            spdlog::error("InventoryMaintenance: source '{}' rescan request failed ({}): {}",
                          source.name, reason, error.what());
        }
    }
}

void JobInventoryMaintenance::noteError(const std::string& error)
{
    std::lock_guard<std::mutex> lock(mutex_);
    degraded_ = true;
    last_error_ = error;
}

bool JobInventoryMaintenance::isClosing(uint64_t internal_id) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& entry : closing_) {
        if (entry.first == internal_id) return true;
    }
    return false;
}

void JobInventoryMaintenance::pruneTracked(const std::vector<InventoryRecord>& records)
{
    std::set<uint64_t> live;
    live.insert(0);
    for (const auto& record : records) live.insert(record.internal_id);

    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = tracked_.begin(); it != tracked_.end();) {
        if (live.count(it->first) == 0) it = tracked_.erase(it);
        else ++it;
    }
    for (auto it = suspect_.begin(); it != suspect_.end();) {
        if (live.count(*it) == 0) it = suspect_.erase(it);
        else ++it;
    }
}

std::string JobInventoryMaintenance::stateString() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!admitted_) return "stopped";
    if (passes_ == 0) return "initializing";
    if (degraded_ || !last_scan_complete_) return "degraded";
    return "healthy";
}

nlohmann::json JobInventoryMaintenance::sourcesJson() const
{
    nlohmann::json out = nlohmann::json::array();
    for (std::size_t index = 0; index < sources_.size(); ++index) {
        const auto& source = sources_[index];
        SourceHealth health;
        if (source.health) {
            try {
                health = source.health();
            } catch (const std::exception&) {
                health.state = SourceState::Degraded;
            }
        }
        nlohmann::json entry;
        entry["name"] = source.name;
        entry["type"] = to_string(source.scheduler_type);
        entry["state"] = to_string(health.state);
        entry["polling"] = health.polling;
        entry["started"] = index < source_started_.size() ? source_started_[index] : false;
        entry["start_note"] = index < source_start_note_.size() ? source_start_note_[index]
                                                               : std::string();
        entry["last_scan_start_ms"] = health.last_scan_start_ms;
        entry["last_scan_success_ms"] = health.last_scan_success_ms;
        entry["last_error"] = health.last_error;
        entry["last_note"] = health.last_note;
        entry["rescan_requested"] = health.rescan_requested;
        entry["consecutive_failures"] = health.consecutive_failures;
        out.push_back(std::move(entry));
    }
    return out;
}

nlohmann::json JobInventoryMaintenance::statusJson() const
{
    // Registry queries happen without holding the controller mutex; the
    // registry takes its own locks briefly and never re-enters here.
    const StoreHealth store = registry_.storeHealth();
    const auto records = registry_.inventoryRecords();
    const std::size_t closing_records = registry_.closingJobCount();

    std::vector<std::pair<uint64_t, ExecutionGeneration>> closing_ids;
    std::set<uint64_t> suspect_ids;
    std::uint64_t events_received = 0;
    std::uint64_t events_dropped = 0;
    std::uint64_t rescan_requests = 0;
    std::uint64_t obsolete_hints = 0;
    std::uint64_t incomplete_observations = 0;
    std::uint64_t upsert_conflicts = 0;
    std::uint64_t retirements = 0;
    std::uint64_t passes = 0;
    std::size_t queue_depth = 0;
    std::int64_t last_scan_start_ms = 0;
    std::int64_t last_scan_success_ms = 0;
    bool last_scan_complete = false;
    bool incomplete_drain = false;
    std::string last_error;
    bool tracker_unavailable = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        closing_ids = closing_;
        suspect_ids = suspect_;
        events_received = events_received_;
        events_dropped = events_dropped_;
        rescan_requests = rescan_requests_;
        obsolete_hints = obsolete_hints_;
        incomplete_observations = incomplete_observations_;
        upsert_conflicts = upsert_conflicts_;
        retirements = retirements_;
        passes = passes_;
        queue_depth = queue_.size();
        last_scan_start_ms = last_scan_start_ms_;
        last_scan_success_ms = last_scan_success_ms_;
        last_scan_complete = last_scan_complete_;
        incomplete_drain = incomplete_drain_;
        last_error = last_error_;
    }
    for (std::size_t index = 0; index < source_started_.size(); ++index) {
        if (!source_started_[index]) tracker_unavailable = true;
    }

    std::size_t closing_present = 0;
    for (const auto& entry : closing_ids) {
        // Closing records stay in records_ until the acknowledgement archives
        // them, so this is the accurate overlap with the active count.
        if (registry_.inventoryRecord(entry.first).has_value()) ++closing_present;
    }

    std::size_t active_count = 0;
    std::size_t suspect_count = 0;
    std::size_t incomplete_count = 0;
    std::size_t quarantined_count = 0;
    for (const auto& record : records) {
        if (record.internal_id == 0) continue;  // system sentinel is never inventory
        if (!record.identity_complete) ++incomplete_count;
        if (record.quarantined) ++quarantined_count;
        const bool suspect = record.state == "suspect" ||
                             suspect_ids.count(record.internal_id) != 0;
        if (suspect) ++suspect_count;
        else if (record.state == "active") ++active_count;
    }
    if (active_count >= closing_present) active_count -= closing_present;
    else active_count = 0;

    nlohmann::json types = nlohmann::json::array();
    for (const auto type : config_.types) types.push_back(to_string(type));

    nlohmann::json out;
    out["state"] = stateString();
    out["configured_types"] = std::move(types);
    out["reconcile_interval_sec"] = config_.reconcile_interval_sec;
    out["last_scan_start_ms"] = last_scan_start_ms;
    out["last_scan_success_ms"] = last_scan_success_ms;
    out["last_scan_complete"] = last_scan_complete;
    out["last_error"] = last_error;
    out["active_count"] = active_count;
    out["suspect_count"] = suspect_count;
    out["closing_count"] = closing_records;
    out["incomplete_count"] = incomplete_count;
    out["quarantined_count"] = quarantined_count;
    out["events_received"] = events_received;
    out["events_dropped"] = events_dropped;
    out["queue_depth"] = queue_depth;
    out["rescan_requests"] = rescan_requests;
    out["obsolete_hints"] = obsolete_hints;
    out["incomplete_observations"] = incomplete_observations;
    out["upsert_conflicts"] = upsert_conflicts;
    out["retirements"] = retirements;
    out["pass_count"] = passes;
    out["tracker_unavailable"] = tracker_unavailable;
    out["incomplete_drain"] = incomplete_drain;
    out["store"] = nlohmann::json{
        {"db_running", store.db_running},
        {"migration_complete", store.migration_complete},
        {"migration_state", store.migration_state},
        {"schema_version", store.schema_version},
        {"degraded", store.degraded},
        {"write_failures", store.write_failures},
        {"last_write_error", store.last_write_error},
        {"active_instances", store.active_instances},
    };
    out["sources"] = sourcesJson();
    return out;
}
