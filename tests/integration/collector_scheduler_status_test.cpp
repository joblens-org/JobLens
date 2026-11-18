#include "core/collector_scheduler.hpp"
#include "core/output_context.hpp"
#include "core/output_record.h"
#include "common/local_rpc.hpp"
#include "core/job_registry.hpp"
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <condition_variable>
#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <vector>
#include <leveldb/db.h>

using namespace std::chrono_literals;
class DrainCaptureWriter : public BaseWriter {
public:
    using BaseWriter::BaseWriter;
    ~DrainCaptureWriter() override { shutdown(); }
    bool accepts(OutputKind kind) const override { return kind == OutputKind::Text; }
protected:
    bool flush_impl(const std::vector<OutputRecordPtr>&) override { return true; }
};
struct CollectorSchedulerTestAccess {
    static CollectorHandle makeHandle(std::function<void(const Job&)> collect,
                                      std::function<bool(const nlohmann::json&)> init,
                                      JobLifecycleFunc on_added = {},
                                      JobLifecycleFunc on_updated = {},
                                      JobLifecycleFunc on_removed = {},
                                      bool system = false) {
        CollectorHandle handle;
        handle.init = [init](const nlohmann::json& cfg, CollectorInitContext&) {
            return init(cfg);
        };
        if (system) {
            handle.scope = CollectorScope::System;
            handle.trigger = CollectorTrigger::Periodic;
            handle.binding = PeriodicSystemBinding{
                [collect](OutputContext& output) {
                    Job system_job;
                    system_job.JobID = 0;
                    system_job.jobtype = JobType::Sys;
                    system_job.subtype = JobSubType::Common;
                    collect(system_job);
                    (void)output.emit("text", Text{"probe"});
                    return CollectOutcome{};
                }};
        } else {
            handle.scope = CollectorScope::Job;
            handle.trigger = CollectorTrigger::Periodic;
            handle.binding = PeriodicJobBinding{
                [collect](const Job& job, OutputContext& output) {
                    collect(job);
                    (void)output.emit("text", Text{"probe"});
                    return CollectOutcome{};
                }};
        }
        handle.channels = {{"text", OutputKind::Text}};
        handle.deinit = [] {};
        handle.on_job_added = std::move(on_added);
        handle.on_job_updated = std::move(on_updated);
        handle.on_job_removed = std::move(on_removed);
        return handle;
    }
    static void add(CollectorScheduler& scheduler, const std::string& name,
                    std::function<void(const Job&)> collect,
                    std::function<bool(const nlohmann::json&)> init,
                    JobLifecycleFunc on_added = {}, JobLifecycleFunc on_updated = {},
                    JobLifecycleFunc on_removed = {}) {
        scheduler.addJobCollectFunc(name, "TestCollector", "test",
            makeHandle(std::move(collect), std::move(init),
                       std::move(on_added), std::move(on_updated), std::move(on_removed), false));
    }
    static void addSystem(CollectorScheduler& scheduler, const std::string& name, const std::string& config,
                          std::function<void(const Job&)> collect,
                          std::function<bool(const nlohmann::json&)> init,
                          JobLifecycleFunc on_added = {}, JobLifecycleFunc on_updated = {},
                          JobLifecycleFunc on_removed = {}) {
        scheduler.addSystemCollectFunc(name, "SystemTestCollector", config,
            makeHandle(std::move(collect), std::move(init),
                       std::move(on_added), std::move(on_updated), std::move(on_removed), true));
    }
    static Job makeJob(size_t jobid, std::vector<std::string> names) {
        Job job;
        job.JobID = jobid;
        job.CollectorNames = std::move(names);
        return job;
    }
    static bool attach(CollectorScheduler& scheduler, const std::string& name, size_t jobid = 123) {
        return scheduler.addJob2Collector(makeJob(jobid, {name}), name);
    }
    static void update(CollectorScheduler& scheduler, size_t jobid, std::vector<std::string> names) {
        scheduler.updateJobCollect(makeJob(jobid, std::move(names)));
    }
    static void remove(CollectorScheduler& scheduler, const std::string& name, size_t jobid = 123) {
        scheduler.rmJobCollect(makeJob(jobid, {name}));
    }
    // Drive the production convergence step synchronously (the same body the
    // reconcile worker runs) so hook fan-out is deterministic in this test.
    static void reconcile(CollectorScheduler& scheduler) {
        scheduler.reconcileOnce();
    }

    // ---- close/drain protocol test seam -----------------------------------
    // A real Job event collector built from the production handle contract.
    static void addEvent(CollectorScheduler& scheduler, const std::string& name,
                         JobLifecycleFunc on_added = {}, JobLifecycleFunc on_updated = {},
                         JobLifecycleFunc on_removed = {}) {
        CollectorHandle handle;
        handle.scope = CollectorScope::Job;
        handle.trigger = CollectorTrigger::Event;
        handle.binding = EventJobBinding{
            [](const ReadySources&, const WorkBudget&, JobEventContext&) {
                return DrainOutcome{};
            }};
        handle.init = [](const nlohmann::json&, CollectorInitContext&) { return true; };
        handle.deinit = [] {};
        handle.on_job_added = std::move(on_added);
        handle.on_job_updated = std::move(on_updated);
        handle.on_job_removed = std::move(on_removed);
        scheduler.addJobEventCollectFunc(name, "TestEventCollector", "ev_cfg", handle);
    }
    // A real Job periodic collector whose init is supplied by the test. The
    // explicit throw_cfg fixture keeps the failing init independent of the
    // close-protocol collectors and of any production collector config.
    static void addThrowing(CollectorScheduler& scheduler, const std::string& name,
                            std::function<bool(const nlohmann::json&)> init,
                            std::function<void()> deinit) {
        CollectorHandle handle;
        handle.scope = CollectorScope::Job;
        handle.trigger = CollectorTrigger::Periodic;
        handle.binding = PeriodicJobBinding{
            [](const Job&, OutputContext& output) {
                (void)output.emit("text", Text{"probe"});
                return CollectOutcome{};
            }};
        handle.channels = {{"text", OutputKind::Text}};
        handle.init = [init](const nlohmann::json& cfg, CollectorInitContext&) {
            return init(cfg);
        };
        handle.deinit = std::move(deinit);
        scheduler.addJobCollectFunc(name, "ThrowingTestCollector", "throw_cfg", handle);
    }
    // Pause the executor side of a real lane. The lane body still runs for real
    // when the test invokes it; only its background scheduling is suspended, so
    // "the lane has not run yet" is a controlled state, not a timeout.
    static void pauseLane(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        if (!state.lane_valid) return;
        scheduler.taskExecutor_.invalidate(state.lane);
        state.lane_valid = false;
    }
    static WorkResult drainLane(CollectorScheduler& scheduler, const std::string& name) {
        auto info_it = scheduler.collector_info_dict.find(name);
        auto state_it = scheduler.collector_state_dict.find(name);
        if (info_it == scheduler.collector_info_dict.end() ||
            state_it == scheduler.collector_state_dict.end())
            throw std::runtime_error("drainLane: unknown collector " + name);
        CollectorScheduler::CollectorLaneContext lane;
        lane.name = name;
        lane.info = info_it->second;
        lane.state = &state_it->second;
        return scheduler.runEventDrain(lane);
    }
    static void stopLane(CollectorScheduler& scheduler, const std::string& name) {
        CollectorScheduler::CollectorLaneContext lane;
        lane.name = name;
        lane.info = scheduler.collector_info_dict.at(name);
        lane.state = &scheduler.collector_state_dict.at(name);
        scheduler.stopCollectorInstance(lane);
    }
    static void setStopping(CollectorScheduler& scheduler, bool stopping) {
        std::lock_guard lock(scheduler.reconcile_m_);
        scheduler.reconcile_stopping_ = stopping;
    }
    static void notifyCompleted(CollectorScheduler& scheduler, uint64_t id) {
        scheduler.notifyDrained({id});
    }
    static bool restartLane(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard global(scheduler.m_);
        std::lock_guard lock(state.m_);
        return scheduler.startCollector(name);
    }
    static size_t signalFailedLane(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        auto registration = state.registrar->watch_manual("failed-epoch-test");
        if (!registration) throw std::runtime_error("manual registration failed");
        for (int i = 0; i < 8; ++i) {
            if (!state.registrar->notify_manual(registration->handle()))
                throw std::runtime_error("manual notification failed");
        }
        std::lock_guard lock(state.ready_m_);
        return state.ready.size();
    }
    static void expireBackoff(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.ready_m_);
        state.ready_after = std::chrono::steady_clock::time_point{};
    }
    static void holdBackoff(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.ready_m_);
        state.ready_after = std::chrono::steady_clock::now() + std::chrono::hours(1);
    }
    static std::vector<uint64_t> finalizeRemovals(CollectorScheduler& scheduler,
                                                  const std::string& name) {
        auto info_it = scheduler.collector_info_dict.find(name);
        auto state_it = scheduler.collector_state_dict.find(name);
        if (info_it == scheduler.collector_info_dict.end() ||
            state_it == scheduler.collector_state_dict.end())
            throw std::runtime_error("finalizeRemovals: unknown collector " + name);
        CollectorScheduler::CollectorLaneContext lane;
        lane.name = name;
        lane.info = info_it->second;
        lane.state = &state_it->second;
        return scheduler.finalizePendingRemovals(lane);
    }
    static size_t pendingCloses(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.m_);
        return state.pending_close.size();
    }
    static size_t memberCount(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.m_);
        return state.jobid_list.size();
    }
    static size_t snapshotCount(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.snapshot_m_);
        return state.job_snapshots.size();
    }
    static uint64_t snapshotGeneration(CollectorScheduler& scheduler, const std::string& name,
                                       uint64_t job_id) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.snapshot_m_);
        const auto it = state.job_generations.find(job_id);
        return it == state.job_generations.end() ? 0 : it->second;
    }
    static std::string snapshotNativeId(CollectorScheduler& scheduler, const std::string& name,
                                        uint64_t job_id) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.snapshot_m_);
        const auto it = state.job_snapshots.find(job_id);
        return it == state.job_snapshots.end() ? std::string{} : it->second.JobNativeID;
    }
    static uint64_t attachToken(CollectorScheduler& scheduler, uint64_t job_id) {
        std::lock_guard lock(scheduler.m_);
        const auto it = scheduler.attach_tokens_.find(job_id);
        return it == scheduler.attach_tokens_.end() ? 0 : it->second;
    }
    // A close entry whose generation cannot match the live snapshot: exactly
    // what a newer attachment would have installed. The two-field aggregate
    // initializer leaves the generation at its default.
    static void injectStaleClose(CollectorScheduler& scheduler, const std::string& name,
                                 uint64_t job_id, uint64_t token) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.m_);
        state.pending_close.push_back({job_id, token});
    }

    // ---- yielded-drain test seam -------------------------------------------
    // The per-round drain plan of a controlled event collector: call n returns
    // more=more_plan[n] (the last entry repeats), and each round records how
    // many readiness entries it was handed, so a test can pin the number of
    // on_ready rounds and prove a continuation ran without new readiness.
    struct EventDrainPlan {
        std::atomic<int> ready_calls{0};
        std::vector<bool> more_plan;
        std::string failure;
        uint64_t job_id = 0;
        std::atomic<int> accepted{0};
        std::atomic<int> rejected{0};
        mutable std::mutex mutex;
        std::condition_variable barrier_cv;
        bool block_first = false;
        bool entered = false;
        bool release = false;
        std::vector<size_t> source_sizes;
        bool more_for(int call) const {
            if (more_plan.empty()) return false;
            const auto index = static_cast<size_t>(call < 0 ? 0 : call);
            return more_plan[index < more_plan.size() ? index : more_plan.size() - 1];
        }
        void record_sources(size_t count) {
            std::lock_guard lock(mutex);
            source_sizes.push_back(count);
        }
        size_t sources_of(int call) const {
            std::lock_guard lock(mutex);
            if (call < 0 || static_cast<size_t>(call) >= source_sizes.size()) return 0;
            return source_sizes[static_cast<size_t>(call)];
        }
    };
    static void addPlannedEvent(CollectorScheduler& scheduler, const std::string& name,
                                const std::shared_ptr<EventDrainPlan>& plan,
                                JobLifecycleFunc on_added = {},
                                JobLifecycleFunc on_removed = {}) {
        CollectorHandle handle;
        handle.scope = CollectorScope::Job;
        handle.trigger = CollectorTrigger::Event;
        handle.binding = EventJobBinding{
            [plan](const ReadySources& sources, const WorkBudget&, JobEventContext& context) {
                const int call = plan->ready_calls.fetch_add(1);
                plan->record_sources(sources.size());
                if (call == 0 && plan->block_first) {
                    std::unique_lock lock(plan->mutex);
                    plan->entered = true;
                    plan->barrier_cv.notify_one();
                    plan->barrier_cv.wait(lock, [&] { return plan->release; });
                }
                if (call == 1 && plan->failure == "throw")
                    throw std::runtime_error("controlled event failure");
                if (call == 1 && plan->failure == "failed")
                    return DrainOutcome{{CollectState::Failed, "controlled event failure"}, 0, false};
                if (call < 2 && plan->failure == "output") {
                    auto session = context.open_session(plan->job_id);
                    if (!session) throw std::runtime_error("missing closing snapshot");
                    auto report = session->context().emit("text", Text{call == 0 ? "accepted" : std::string(65536, 'x')});
                    if (!report.writers.empty() && report.writers[0].code == EnqueueCode::Accepted)
                        ++plan->accepted;
                    else
                        ++plan->rejected;
                }
                return DrainOutcome{CollectOutcome{}, sources.size(), plan->more_for(call)};
            }};
        handle.init = [](const nlohmann::json&, CollectorInitContext&) { return true; };
        handle.deinit = [] {};
        handle.on_job_added = std::move(on_added);
        handle.on_job_removed = std::move(on_removed);
        handle.channels = {{"text", OutputKind::Text}};
        scheduler.addJobEventCollectFunc(name, "PlannedEventCollector",
                                        plan->failure == "output" ? "ev_output_cfg" : "ev_cfg", handle);
    }
    // Stage one poller readiness exactly as the sink would, but without waking
    // the executor: the test drives the lane body itself through drainLane.
    static void stageReadiness(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.ready_m_);
        state.ready.push_back(ReadySource{});
    }
    static void requestLane(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        scheduler.taskExecutor_.request(state.lane, RequestPolicy::Remember);
    }
    static size_t backoffTimers(CollectorScheduler& scheduler, const std::string& name) {
        size_t count = 0;
        const auto stats = scheduler.timerScheduler_.getPerformanceStats();
        for (const auto& timer : stats.at("tasks"))
            if (timer.at("name") == name + ":event-backoff") ++count;
        return count;
    }
    static size_t totalTimersRegistered(CollectorScheduler& scheduler) {
        return scheduler.timerScheduler_.getPerformanceStats().at("totalTasksRegistered").get<size_t>();
    }
    static bool hasTimerId(CollectorScheduler& scheduler, const std::string& name, size_t id) {
        const auto stats = scheduler.timerScheduler_.getPerformanceStats();
        for (const auto& timer : stats.at("tasks"))
            if (timer.at("name") == name + ":event-backoff" && timer.at("task_id") == id) return true;
        return false;
    }
    static void addAlwaysFailingEvent(CollectorScheduler& scheduler, const std::string& name,
                                      std::atomic<int>& calls) {
        CollectorHandle handle;
        handle.scope = CollectorScope::Job;
        handle.trigger = CollectorTrigger::Event;
        handle.binding = EventJobBinding{[&calls](const ReadySources&, const WorkBudget&, JobEventContext&) {
            ++calls;
            return DrainOutcome{{CollectState::Failed, "controlled cooldown failure"}, 0, false};
        }};
        handle.init = [](const nlohmann::json&, CollectorInitContext&) { return true; };
        handle.deinit = [] {};
        scheduler.addJobEventCollectFunc(name, "CooldownTestCollector", "ev_cfg", handle);
    }
    static bool inBackoff(CollectorScheduler& scheduler, const std::string& name) {
        auto& state = scheduler.collector_state_dict.at(name);
        std::lock_guard lock(state.ready_m_);
        return state.ready_after > std::chrono::steady_clock::now();
    }
};
void check(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

// JobRegistry::hasInstance() was removed as a production-dead API; active-only
// is derived from the record state so the assertion semantics stay unchanged.
static bool has_active(const JobRegistry& registry, uint64_t job_id)
{
    const auto record = registry.jobRecord(job_id);
    return record.has_value() && record->state != RecordState::Closing;
}
std::string hex_id(uint64_t id) {
    std::ostringstream out;
    out << std::hex << std::setw(16) << std::setfill('0') << id;
    return out.str();
}
// The live JobStore owns its LevelDB directory for its whole lifetime and
// LevelDB refuses a second open in the same process. The test therefore
// inspects a copy of the files taken between scheduler steps; no write is in
// flight at those points, so the copy is the durable image written so far.
std::vector<std::string> raw_keys_from_store_copy(const std::string& live_db,
                                                  const std::string& copy_dir,
                                                  const std::string& prefix) {
    std::filesystem::remove_all(copy_dir);
    std::filesystem::create_directories(copy_dir);
    for (const auto& entry : std::filesystem::directory_iterator(live_db)) {
        if (!entry.is_regular_file()) continue;
        const auto name = entry.path().filename().string();
        if (name == "LOCK") continue;
        std::filesystem::copy_file(entry.path(), copy_dir + "/" + name,
                                   std::filesystem::copy_options::overwrite_existing);
    }
    leveldb::Options options;
    options.create_if_missing = false;
    leveldb::DB* db = nullptr;
    const auto opened = leveldb::DB::Open(options, copy_dir, &db);
    check(opened.ok(), "open the copied store: " + opened.ToString());
    std::vector<std::string> keys;
    {
        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const auto key = it->key().ToString();
            if (key.rfind(prefix, 0) == 0) keys.push_back(key);
        }
        check(it->status().ok(), "iterate the copied store: " + it->status().ToString());
    }
    delete db;
    return keys;
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
void run(const std::string& base) {
    const auto path = base + ".sock";
    std::ofstream config(base + ".yaml");
    config << "lens_config:\n  max_collector_threads: 3\n"
              "collectors_config:\n  default_freq: 20\n  default_use_writers: []\n  collectors: []\n"
              "writers_config:\n  enable_writer_perf: false\n  max_record_bytes: 4096\n  writers:\n    - {name: drain_capture, type: DrainCaptureWriter, config: drain_writer_cfg}\n"
              "drain_writer_cfg: {}\nev_output_cfg:\n  outputs: {text: [drain_capture]}\n"
              "job_registry_config:\n  job_db_path: " << base << ".db\n"
              "sys_test:\n  freq: 20\n  use_writers: []\n  auto_start: true\n"
              "test:\n  freq: 20\n  use_writers: []\n"
              "ev_cfg: {}\n"
              "throw_cfg: {}\n";
    config.close();
    auto& server = RPCServer::instance(path);
    Config::instance(base + ".yaml");
    WriterManager::instance().registerWriter<DrainCaptureWriter>("DrainCaptureWriter");
    auto& scheduler = CollectorScheduler::instance();
    JobRegistry::instance().retireJob(0);
    std::vector<std::string> early_timer_failures;
    const auto timer_test = [&](const char* name, const std::function<void()>& body) {
        try { body(); }
        catch (const std::exception& error) {
            early_timer_failures.push_back(std::string(name) + ": " + error.what());
            std::cerr << "REGRESSION FAILED " << early_timer_failures.back() << '\n';
        }
    };
    timer_test("zero-timer-ownership", [&] {
        const std::string name = "zero_timer";
        CollectorSchedulerTestAccess::addEvent(scheduler, name);
        check(CollectorSchedulerTestAccess::attach(scheduler, name, 9001), "zero timer lane attach");
        CollectorSchedulerTestAccess::holdBackoff(scheduler, name);
        CollectorSchedulerTestAccess::stageReadiness(scheduler, name);
        CollectorSchedulerTestAccess::drainLane(scheduler, name);
        const bool first_is_zero = CollectorSchedulerTestAccess::hasTimerId(scheduler, name, 0);
        CollectorSchedulerTestAccess::drainLane(scheduler, name);
        const auto count = CollectorSchedulerTestAccess::backoffTimers(scheduler, name);
        CollectorSchedulerTestAccess::stopLane(scheduler, name);
        const auto remaining = CollectorSchedulerTestAccess::backoffTimers(scheduler, name);
        check(first_is_zero && count == 1 && remaining == 0, "timer ID zero must be tracked once and cancelled on teardown");
    });
    timer_test("fired-timer-ownership", [&] {
        const std::string name = "cycling_timer";
        std::atomic<int> calls{0};
        CollectorSchedulerTestAccess::addAlwaysFailingEvent(scheduler, name, calls);
        check(CollectorSchedulerTestAccess::attach(scheduler, name, 9002), "cooldown lane attach");
        const auto wait_for = [](const std::function<bool()>& predicate) {
            const auto end = std::chrono::steady_clock::now() + 3s;
            while (!predicate() && std::chrono::steady_clock::now() < end)
                std::this_thread::sleep_for(2ms);
            return predicate();
        };
        CollectorSchedulerTestAccess::stageReadiness(scheduler, name);
        CollectorSchedulerTestAccess::requestLane(scheduler, name);
        bool progressed = wait_for([&] { return calls.load() == 1 && CollectorSchedulerTestAccess::inBackoff(scheduler, name); });
        size_t max_timers = 0;
        for (int cycle = 0; cycle < 8 && progressed; ++cycle) {
            const auto previous = CollectorSchedulerTestAccess::totalTimersRegistered(scheduler);
            CollectorSchedulerTestAccess::stageReadiness(scheduler, name);
            CollectorSchedulerTestAccess::requestLane(scheduler, name);
            progressed = wait_for([&] { return CollectorSchedulerTestAccess::totalTimersRegistered(scheduler) > previous; });
            progressed = progressed && wait_for([&] { return calls.load() == cycle + 2 && CollectorSchedulerTestAccess::inBackoff(scheduler, name); });
            max_timers = std::max(max_timers, CollectorSchedulerTestAccess::backoffTimers(scheduler, name));
        }
        CollectorSchedulerTestAccess::stopLane(scheduler, name);
        const auto remaining = CollectorSchedulerTestAccess::backoffTimers(scheduler, name);
        check(progressed, "real cooldown timers must repeatedly deliver work to the live lane");
        check(max_timers <= 1 && remaining == 0, "fired backoff timers must remain bounded and be reclaimed at teardown");
    });
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false, released = false;
    struct Release {
        CollectorScheduler& scheduler;
        std::mutex& mutex;
        std::condition_variable& cv;
        bool& released;
        ~Release() {
            { std::lock_guard lock(mutex); released = true; }
            cv.notify_all();
            scheduler.shutdown();
        }
    };
    std::atomic<int> attempts{0};
    CollectorSchedulerTestAccess::add(scheduler, "blocked", [&](const Job&) {
        std::unique_lock lock(mutex);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return released; });
        return;
    }, [](const nlohmann::json&) { return true; });
    CollectorSchedulerTestAccess::add(scheduler, "failed", [](const Job&) { return; },
        [](const nlohmann::json&) { return false; });
    CollectorSchedulerTestAccess::add(scheduler, "idle", [](const Job&) { return; },
        [](const nlohmann::json&) { return true; });
    CollectorSchedulerTestAccess::add(scheduler, "recovery", [&](const Job&) {
        if (++attempts == 1) throw std::runtime_error("controlled collect failure");
        return;
    }, [](const nlohmann::json&) { return true; });
    Release release{scheduler, mutex, cv, released};
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
    check(blocked.at("current_job").at("job_id") == 123 &&
          blocked.at("current_job").at("stage") == "collect", "blocked Job and collect stage missing");
    check(blocked.at("scheduling").at("state") == "running", "running timer missing");
    CollectorSchedulerTestAccess::attach(scheduler, "recovery");
    for (int i = 0; i < 100 && status("blocked").at("scheduling").at("skipped_runs") < 3; ++i)
        std::this_thread::sleep_for(10ms);
    const auto busy = status("blocked");
    check(busy.at("scheduling").at("skipped_runs") >= 3, "overlapping ticks not counted");
    check(busy.at("active_callback_count") == 1, "blocked collector had overlapping callbacks");
    check(busy.at("current_job").at("elapsed_ms").get<double>() >= 100, "blocked Job elapsed time missing");
    for (int i = 0; i < 100 && status("recovery")["operations"]["write"]["success_count"] == 0; ++i)
        std::this_thread::sleep_for(10ms);
    check(status("recovery")["operations"]["write"]["success_count"] > 0,
          "slow collectors exhausted all workers");
    auto queue = rpc(path, "CollectorScheduler/status").at("scheduler");
    check(queue.at("worker_count") == 3 && queue.at("skippedRuns").get<size_t>() >= 3,
          "global scheduler statistics missing");
    check(queue.at("currentQueuedTasks").get<size_t>() <= 3, "ready queue grew with repeated ticks");
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

    // ---- Job 生命周期钩子：Job 级按绑定投递，System 级广播 ----
    // The registry is the desired-state source and the drain sink; the scheduler
    // converges by reading records, never by a synchronous registry callback.
    // The gate lets a test make the source unavailable without pretending the
    // the registry is empty.
    class GateSource : public IJobRecordSource {
    public:
        explicit GateSource(IJobRecordSource& inner) : inner_(inner) {}
        bool runtimeRecords(std::vector<JobRecord>& out) const override {
            if (!available_.load()) return false;
            return inner_.runtimeRecords(out);
        }
        bool runtimeRecord(uint64_t job_id,
                           std::optional<JobRecord>& out) const override {
            if (!available_.load()) return false;
            return inner_.runtimeRecord(job_id, out);
        }
        void setAvailable(bool value) { available_.store(value); }
    private:
        IJobRecordSource& inner_;
        std::atomic<bool> available_{true};
    };

    JobRegistry& registry = JobRegistry::instance();
    GateSource gate(registry);
    scheduler.attach(&gate, &registry);
    registry.setCollectionRuntime(&scheduler);
    // This test owns the collector set directly; the production name gate is
    // covered by the RPC/intent suites.
    registry.setCollectorValidator(nullptr);

    auto bump = [](std::atomic<int>& counter) {
        return [&counter](const Job&) { counter++; };
    };
    std::atomic<int> life_added{0}, life_updated{0}, life_removed{0};
    std::atomic<int> extra_added{0}, extra_removed{0};
    std::atomic<int> sys_added{0}, sys_updated{0}, sys_removed{0};
    CollectorSchedulerTestAccess::add(scheduler, "lifecycle",
        [](const Job&) { return; }, [](const nlohmann::json&) { return true; },
        bump(life_added), bump(life_updated), bump(life_removed));
    CollectorSchedulerTestAccess::add(scheduler, "extra",
        [](const Job&) { return; }, [](const nlohmann::json&) { return true; },
        bump(extra_added), {}, bump(extra_removed));
    CollectorSchedulerTestAccess::addSystem(scheduler, "sys_watch", "sys_test",
        [](const Job&) { return; }, [](const nlohmann::json&) { return true; },
        bump(sys_added), bump(sys_updated), bump(sys_removed));

    Job life_job = CollectorSchedulerTestAccess::makeJob(456, {"lifecycle"});
    set_pids(life_job, {getpid()});
    life_job.jobtype = JobType::Sys;
    life_job.subtype = JobSubType::Common;
    life_job.sub_attr = CommonJobAttr{.auto_update_child = false};
    check(registry.addJob(life_job), "register lifecycle job");
    CollectorSchedulerTestAccess::reconcile(scheduler);
    check(life_added.load() == 1, "on_job_added fired once for bound job collector");
    check(extra_added.load() == 0, "unbound job collector must not receive added");
    check(sys_added.load() == 1, "system collector must receive every job added");

    check(!CollectorSchedulerTestAccess::attach(scheduler, "lifecycle", 456), "duplicate attach");
    check(life_added.load() == 1, "duplicate attach must not fire added again");

    // update: 已绑定 → updated；新增采集器 → added
    CollectionRequest grow;
    grow.targets = {456};
    grow.collectors = std::vector<std::string>{"lifecycle", "extra"};
    const auto grown = registry.setCollectors(grow);
    check(grown.ok, "grow the selection: " + grown.message);
    CollectorSchedulerTestAccess::reconcile(scheduler);
    check(life_updated.load() == 1, "on_job_updated fired for retained collector");
    check(life_added.load() == 1, "retained collector must not receive added twice");
    check(extra_added.load() == 1, "collector added by update receives added");
    check(sys_updated.load() == 1, "system collector must receive updated");

    // update: 移除采集器 → removed
    CollectionRequest shrink;
    shrink.targets = {456};
    shrink.collectors = std::vector<std::string>{"lifecycle"};
    const auto shrunk = registry.setCollectors(shrink);
    check(shrunk.ok, "shrink the selection: " + shrunk.message);
    CollectorSchedulerTestAccess::reconcile(scheduler);
    check(extra_removed.load() == 1, "collector removed by update receives removed");
    check(life_removed.load() == 0, "retained collector must not receive removed");

    // An unavailable desired-state source is NOT an empty record set: the
    // runtime must keep the attachment (and its token) until it can read again.
    const auto attached_before = registry.actualCollection(456);
    check(attached_before.tracked &&
              attached_before.collectors == std::vector<std::string>{"lifecycle"},
          "the runtime must report the attached lane");
    const uint64_t token_before = attached_before.attach_token;
    check(token_before != 0, "a live attachment must carry a token");

    gate.setAvailable(false);
    CollectionRequest stop;
    stop.targets = {456};
    stop.collectors = std::vector<std::string>{};
    const auto stopped = registry.setCollectors(stop);
    check(stopped.ok, "stop the selection: " + stopped.message);
    CollectorSchedulerTestAccess::reconcile(scheduler);
    {
        const auto actual = registry.actualCollection(456);
        check(actual.tracked &&
                  actual.collectors == std::vector<std::string>{"lifecycle"},
              "an unavailable source must not detach the current attachment");
        check(actual.attach_token == token_before,
              "an unavailable source must not churn the attach token");
        check(life_removed.load() == 0,
              "an unavailable source must not emit Removed");
    }

    // The accepted stop applies once the source is readable again. The record
    // stays Active (persistent stop), so the System view is Updated, not Removed.
    gate.setAvailable(true);
    CollectorSchedulerTestAccess::reconcile(scheduler);
    check(life_removed.load() == 1,
          "the persistent stop must detach once the source is readable");
    check(registry.actualCollection(456).collectors.empty(),
          "nothing may remain attached after the persistent stop");
    check(sys_removed.load() == 0,
          "a stopped but still Active record must stay in the System view");

    // Reattach: the same scope gets a strictly greater token.
    CollectionRequest rebind;
    rebind.targets = {456};
    rebind.collectors = std::vector<std::string>{"lifecycle"};
    const auto rebound = registry.setCollectors(rebind);
    check(rebound.ok, "rebind the selection: " + rebound.message);
    CollectorSchedulerTestAccess::reconcile(scheduler);
    check(life_added.load() == 2, "the rebind must attach the lane again");
    const auto reattached = registry.actualCollection(456);
    check(reattached.tracked &&
              reattached.collectors == std::vector<std::string>{"lifecycle"},
          "the rebind must be reported as actual");
    check(reattached.attach_token > token_before,
          "a reattach must get a strictly greater attach token");

    // Job 结束 → removed；重复摘除不重复投递
    registry.retireJob(456);
    CollectorSchedulerTestAccess::reconcile(scheduler);
    check(life_removed.load() == 2, "on_job_removed fired on job removal");
    check(sys_removed.load() == 1,
          "system collector must receive job removed once, got " +
              std::to_string(sys_removed.load()));
    CollectorSchedulerTestAccess::remove(scheduler, "lifecycle", 456);
    check(life_removed.load() == 2, "repeated removal must not fire removed again");

    // ---- Closing / reselect close protocol (regressions) ------------------
    // Real Scheduler, real lanes, real drain accounting. The record source is a
    // test double of the desired-state seam only; the Scheduler is the
    // production object that owns actual state.
    class RecordSource final : public IJobRecordSource {
    public:
        void set(std::vector<JobRecord> next) {
            std::lock_guard lock(mutex_);
            records_ = std::move(next);
        }
        bool runtimeRecords(std::vector<JobRecord>& out) const override {
            std::lock_guard lock(mutex_);
            out = records_;
            return true;
        }
        bool runtimeRecord(uint64_t job_id,
                           std::optional<JobRecord>& out) const override {
            std::lock_guard lock(mutex_);
            for (const auto& record : records_) {
                if (record.job.JobID != job_id) continue;
                out = record;
                return true;
            }
            out.reset();
            return true;
        }
    private:
        mutable std::mutex mutex_;
        std::vector<JobRecord> records_;
    };
    class CompletionSink final : public IDrainCompletionSink {
    public:
        void onDrained(uint64_t job_id) override {
            std::lock_guard lock(mutex_);
            completed_.push_back(job_id);
            cv_.notify_all();
        }
        size_t count(uint64_t job_id) const {
            std::lock_guard lock(mutex_);
            return static_cast<size_t>(
                std::count(completed_.begin(), completed_.end(), job_id));
        }
        bool wait_for(uint64_t job_id) {
            std::unique_lock lock(mutex_);
            return cv_.wait_for(lock, std::chrono::seconds(5), [&] {
                return std::count(completed_.begin(), completed_.end(), job_id) != 0;
            });
        }
    private:
        mutable std::mutex mutex_;
        std::condition_variable cv_;
        std::vector<uint64_t> completed_;
    };
    auto record_of = [](uint64_t id, RecordState state,
                        std::vector<std::string> manual) {
        JobRecord record;
        record.job.JobID = id;
        record.state = state;
        record.revision = 1;
        record.job.JobID = id;
        record.job.JobNativeID = std::to_string(id);
        record.job.jobtype = JobType::Sys;
        record.job.subtype = JobSubType::Common;
        record.job.sub_attr = CommonJobAttr{.auto_update_child = false};
        record.selection.manual = std::move(manual);
        return record;
    };

    RecordSource close_source;
    CompletionSink close_sink;
    std::atomic<int> close_added{0}, close_removed{0};
    CollectorSchedulerTestAccess::addEvent(scheduler, "ev_a", bump(close_added), {},
                                           bump(close_removed));
    CollectorSchedulerTestAccess::addEvent(scheduler, "ev_b", bump(close_added), {},
                                           bump(close_removed));
    CollectorSchedulerTestAccess::addEvent(scheduler, "ev_c", bump(close_added), {},
                                           bump(close_removed));
    std::atomic<int> d_removed{0}, e_removed{0}, f_removed{0}, g_removed{0};
    CollectorSchedulerTestAccess::addEvent(scheduler, "ev_d", {}, {}, bump(d_removed));
    CollectorSchedulerTestAccess::addEvent(scheduler, "ev_e", {}, {}, bump(e_removed));
    CollectorSchedulerTestAccess::addEvent(scheduler, "ev_f", {}, {}, bump(f_removed));
    CollectorSchedulerTestAccess::addEvent(scheduler, "ev_g", {}, {}, bump(g_removed));
    // Real-registry archive fixture counters (registered by that scenario).
    std::atomic<int> ri_a_removed{0}, ri_b_removed{0}, ri_c_removed{0}, ri_d_removed{0};
    std::atomic<int> archive_cleanup_failures{0};
    // Init-throw regression fixture: the first start attempt of this collector
    // throws out of the real init handle; the retry succeeds.
    std::atomic<int> throw_init_calls{0}, throw_deinit_calls{0};
    CollectorSchedulerTestAccess::addThrowing(scheduler, "throwing",
        [&](const nlohmann::json&) {
            if (++throw_init_calls == 1)
                throw std::runtime_error("controlled init failure");
            return true;
        },
        [&] { ++throw_deinit_calls; });
    scheduler.attach(&close_source, &close_sink);

    // Both findings are reported, not just the first: on the pre-fix tree the
    // run must show each defect independently.
    std::vector<std::string> regression_failures = std::move(early_timer_failures);
    const auto run_regression = [&regression_failures](const char* name,
                                                       const std::function<void()>& body) {
        try {
            body();
        } catch (const std::exception& error) {
            regression_failures.push_back(std::string(name) + ": " + error.what());
            std::cerr << "REGRESSION FAILED " << regression_failures.back() << '\n';
        }
    };

    run_regression("closing-drain", [&] {
        // Regression 1: an Active record selecting two event collectors.
        close_source.set({record_of(7001, RecordState::Active, {"ev_a", "ev_b"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(close_added.load() == 2, "both event collectors must attach");
        // Suspend only the background scheduling of the two real lanes; the lane
        // body itself still runs, driven by the test at a chosen point.
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_a");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_b");
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_a") == 1 &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_b") == 1,
              "both event collectors must hold the job");
        const uint64_t token_one = CollectorSchedulerTestAccess::attachToken(scheduler, 7001);
        check(token_one != 0, "an attachment must carry an attach token");

        // Persist Closing and reconcile. The ordinary selection pass must not detach
        // these bindings: the single accounting path owns the removal, and neither
        // lane has produced its accepted tail yet.
        close_source.set({record_of(7001, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(close_sink.count(7001) == 0,
              "a Closing record must not complete while both lanes still owe a tail");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_a") == 1 &&
                  CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_b") == 1,
              "each attached event lane must hold its own deferred close");
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_a") == 0 &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_b") == 0,
              "the Closing detach must release membership");
        check(scheduler.actual(7001).draining,
              "the accepted drain must be reported as draining");

        // First lane's completion is one of two: it must not finalize or archive.
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_a");
        check(close_sink.count(7001) == 0,
              "the first of two lane completions must not finalize the drain");
        check(CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_a") == 0 &&
                  CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_a") == 0,
              "the drained lane must release exactly its own snapshot and close");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_b") == 1,
              "the second lane must still owe its accepted tail");
        check(close_removed.load() == 1, "exactly one Removed so far");

        // The second and final completion finalizes exactly once.
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_b");
        check(close_sink.count(7001) >= 1,
              "the final lane completion must report the completed drain");
        check(close_removed.load() == 2, "each lane must deliver exactly one Removed");
        check(!scheduler.actual(7001).draining, "the finalized drain must stop draining");

        // A cleanup failure keeps the record Closing: the completion must be
        // re-delivered on the next drain request, never lost.
        const size_t completions_before_retry = close_sink.count(7001);
        check(scheduler.beginDrain(7001), "a Closing record must accept a drain request");
        check(close_sink.count(7001) > completions_before_retry,
              "a retried Closing drain must receive the completion again");

        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("reselect-epoch", [&] {
        // Regression 2: Active -> persistent stop -> reselect while the stopped
        // attachment's tail is still pending. The old close must never consume the
        // reselected attachment's snapshot, token or membership.
        JobRecord epoch = record_of(7002, RecordState::Active, {"ev_c"});
        epoch.job.JobNativeID = "epoch-1";
        close_source.set({epoch});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_c") == 1,
              "the first epoch must attach");
        const uint64_t epoch_one_token = CollectorSchedulerTestAccess::attachToken(scheduler, 7002);
        const uint64_t epoch_one_generation =
            CollectorSchedulerTestAccess::snapshotGeneration(scheduler, "ev_c", 7002);
        check(epoch_one_token != 0 && epoch_one_generation != 0,
              "the first epoch must own a token and a snapshot generation");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_c");
        const int removed_before_reselect = close_removed.load();

        close_source.set({record_of(7002, RecordState::Active, std::vector<std::string>{})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_c") == 0,
              "the persistent stop must detach the membership");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_c") == 1,
              "the stopped epoch must keep its tail pending");
        check(CollectorSchedulerTestAccess::snapshotGeneration(scheduler, "ev_c", 7002) ==
                  epoch_one_generation,
              "the pending close must keep the snapshot of its own epoch");

        // Reselect before the lane ran. The reattach is deferred: it must not
        // install a newer snapshot/token that the old close would then see.
        JobRecord reselected = record_of(7002, RecordState::Active, {"ev_c"});
        reselected.job.JobNativeID = "epoch-2";
        close_source.set({reselected});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_c") == 0,
              "the reattach must wait for the previous tail to drain");
        check(CollectorSchedulerTestAccess::snapshotNativeId(scheduler, "ev_c", 7002) == "epoch-1",
              "the reselected facts must not replace the pending epoch's snapshot");
        check(CollectorSchedulerTestAccess::attachToken(scheduler, 7002) == 0,
              "the deferred reattach must not install a live attach token for the JobID");
        check(close_removed.load() == removed_before_reselect,
              "no Removed may be delivered for a deferred reattach");

        // Draining the old epoch releases exactly that epoch, then the reattach
        // happens with a strictly greater token and its own snapshot.
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_c");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_c") == 0 &&
                  CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_c") == 0,
              "the old close must release only its own epoch");
        check(close_removed.load() == removed_before_reselect + 1,
              "the old epoch must deliver exactly one Removed");
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_c") == 1,
              "the reattach must happen after the old tail finalized");
        const uint64_t epoch_two_token = CollectorSchedulerTestAccess::attachToken(scheduler, 7002);
        check(epoch_two_token > epoch_one_token,
              "the reattach must get a strictly greater attach token");
        check(CollectorSchedulerTestAccess::snapshotNativeId(scheduler, "ev_c", 7002) == "epoch-2",
              "the new attachment must own its own snapshot");
        check(CollectorSchedulerTestAccess::snapshotGeneration(scheduler, "ev_c", 7002) != 0,
              "the new attachment must carry a snapshot generation");
        check(close_added.load() == 4, "the reattach must deliver its own Added");

        // Epoch guard on both release paths: a close whose generation no longer
        // matches the live snapshot must not fire Removed, erase the snapshot, or
        // touch the newer membership/token.
        const int removed_before_guard = close_removed.load();
        CollectorSchedulerTestAccess::injectStaleClose(scheduler, "ev_c", 7002, epoch_two_token);
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_c");
        check(close_removed.load() == removed_before_guard,
              "a stale event close must not deliver Removed to a newer epoch");
        check(CollectorSchedulerTestAccess::snapshotNativeId(scheduler, "ev_c", 7002) == "epoch-2" &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_c") == 1 &&
                  CollectorSchedulerTestAccess::attachToken(scheduler, 7002) == epoch_two_token,
              "a stale event close must not mutate the newer attachment");

        CollectorSchedulerTestAccess::injectStaleClose(scheduler, "ev_c", 7002, epoch_two_token);
        CollectorSchedulerTestAccess::finalizeRemovals(scheduler, "ev_c");
        check(close_removed.load() == removed_before_guard,
              "a stale finalization must not deliver Removed to a newer epoch");
        check(CollectorSchedulerTestAccess::snapshotNativeId(scheduler, "ev_c", 7002) == "epoch-2" &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_c") == 1,
              "a stale finalization must not erase the newer snapshot or membership");

        // Leave the Scheduler holding nothing from this record source.
        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_c");
    });

    run_regression("init-throw", [&] {
        // Regression 3: a collector whose init throws. startCollector runs while
        // addJob2Collector holds this lane's state mutex, so the catch must not
        // take the stop path: finalizePendingRemovals re-locks that same
        // non-recursive mutex on the same thread. The failed start must release
        // its partial init, report no actual holder, and let the next reconcile
        // retry init.
        close_source.set({record_of(9001, RecordState::Active, {"throwing"})});
        // This call is where the pre-fix scheduler self-deadlocks. The sentinel
        // makes the bounded runner timeout name the hang instead of reporting
        // an empty log.
        std::cerr << "INIT-THROW: reconciling a collector whose init throws" << std::endl;
        CollectorSchedulerTestAccess::reconcile(scheduler);

        check(throw_init_calls.load() == 1, "the throwing init must run exactly once");
        check(throw_deinit_calls.load() == 1, "a failed init must release its partial init");
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "throwing") == 0,
              "a failed init must not leave membership behind");
        check(CollectorSchedulerTestAccess::snapshotCount(scheduler, "throwing") == 0,
              "a failed init must not leave a snapshot behind");
        check(CollectorSchedulerTestAccess::attachToken(scheduler, 9001) == 0,
              "a failed init must not leave an attach token behind");
        const auto failed_actual = scheduler.actual(9001);
        check(!failed_actual.tracked && failed_actual.collectors.empty(),
              "a failed init must report no actual holders");
        check(failed_actual.error.find("throwing") != std::string::npos,
              "a failed init must report an attach error naming the collector");

        // The rollback removed the dedup marker too, so the next reconcile
        // retries init; the second init succeeds and owns its token/membership.
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(throw_init_calls.load() == 2, "the next reconcile must retry init");
        check(throw_deinit_calls.load() == 1, "a successful retry must not deinit again");
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "throwing") == 1,
              "the retried init must own the job membership");
        check(CollectorSchedulerTestAccess::snapshotCount(scheduler, "throwing") == 1,
              "the retried init must own its snapshot");
        const uint64_t retry_token = CollectorSchedulerTestAccess::attachToken(scheduler, 9001);
        check(retry_token != 0, "the retried init must carry an attach token");
        const auto attached = scheduler.actual(9001);
        check(attached.tracked && attached.collectors == std::vector<std::string>{"throwing"},
              "the retried init must be reported as the actual holder");
        check(attached.attach_token == retry_token && attached.error.empty(),
              "the retried init must report its token and clear the attach error");

        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "throwing") == 0,
              "the periodic detach must release the retried membership");
    });

    run_regression("late-closing-one-stopped", [&] {
        // An Active manual stop leaves a deferred tail on a lane that no longer
        // holds membership. If the record becomes Closing before that tail is
        // drained, the Closing boundary must adopt that obligation: the count
        // is attached holders + that lane's existing pending close.
        close_source.set({record_of(7101, RecordState::Active, {"ev_d", "ev_e"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_d") == 1 &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_e") == 1,
              "both event lanes must attach before the manual stop");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_d");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_e");

        // Manual stop of ev_d while the record is still Active.
        close_source.set({record_of(7101, RecordState::Active, {"ev_e"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_d") == 1 &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_d") == 0,
              "the stopped lane must owe a tail and hold no membership");

        // The record becomes Closing while that tail is still pending.
        close_source.set({record_of(7101, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_d") == 1 &&
                  CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_e") == 1,
              "the Closing boundary must keep both tails pending");
        check(close_sink.count(7101) == 0,
              "a Closing record must not complete with an adopted tail still pending");

        // The still-attached lane acknowledges first: it must not finalize.
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_e");
        check(close_sink.count(7101) == 0,
              "the attached lane's ack must not finalize while the adopted tail is pending");
        check(e_removed.load() == 1 && d_removed.load() == 0,
              "the attached lane must deliver its own Removed only");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_d") == 1,
              "the adopted close must still gate the boundary");

        // The stopped lane is the last obligation and finalizes exactly once.
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_d");
        check(close_sink.count(7101) == 1,
              "the last obligation must complete the Closing drain exactly once");
        check(d_removed.load() == 1,
              "the adopted tail must deliver exactly one Removed");
        check(!scheduler.actual(7101).draining,
              "the finalized adoption must stop draining");
        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("late-closing-both-stopped", [&] {
        // Both lanes manually stopped before Closing: no membership remains, but
        // both tails are still owed. The Closing boundary must count them.
        close_source.set({record_of(7102, RecordState::Active, {"ev_f", "ev_g"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_f") == 1 &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_g") == 1,
              "both event lanes must attach before the manual stops");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_f");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_g");

        close_source.set({record_of(7102, RecordState::Active, {"ev_g"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        close_source.set({record_of(7102, RecordState::Active, std::vector<std::string>{})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_f") == 1 &&
                  CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_g") == 1,
              "both stopped lanes must owe a tail");
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_f") == 0 &&
                  CollectorSchedulerTestAccess::memberCount(scheduler, "ev_g") == 0,
              "neither stopped lane may hold membership");

        close_source.set({record_of(7102, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(close_sink.count(7102) == 0,
              "a Closing record with two adopted tails must not complete immediately");

        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_f");
        check(close_sink.count(7102) == 0,
              "the first adopted tail must not finalize the Closing drain");
        check(f_removed.load() == 1 && g_removed.load() == 0,
              "the first adopted lane must deliver exactly one Removed");

        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_g");
        check(close_sink.count(7102) == 1,
              "the last adopted tail must complete the Closing drain exactly once");
        check(g_removed.load() == 1,
              "the last adopted lane must deliver exactly one Removed");
        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("yield-defers-closing", [&] {
        // A close whose lane reported an unfinished drain (on_ready returned
        // more=true) must not be acknowledged, must not fire Removed, and must
        // not release the snapshot: the tail records of the next round resolve
        // through that snapshot. Only a round whose on_ready finished without
        // asking for more proves the drain and may process the close.
        auto plan = std::make_shared<CollectorSchedulerTestAccess::EventDrainPlan>();
        plan->more_plan = {true, false}; // round one exceeds its budget, round two finishes
        std::atomic<int> planned_added{0}, planned_removed{0};
        CollectorSchedulerTestAccess::addPlannedEvent(scheduler, "ev_yield", plan,
                                                      bump(planned_added), bump(planned_removed));
        close_source.set({record_of(7201, RecordState::Active, {"ev_yield"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_yield") == 1,
              "the yielding lane must attach");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_yield");

        // The record turns Closing while the lane still owes more than one
        // round of tail records.
        close_source.set({record_of(7201, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_yield") == 1,
              "the Closing boundary must defer the close to the lane");
        check(close_sink.count(7201) == 0,
              "a Closing record must not complete before the lane ran");

        // The input that exceeds one round's budget: a single readiness
        // notification whose backlog on_ready cannot finish inside the round.
        CollectorSchedulerTestAccess::stageReadiness(scheduler, "ev_yield");

        // Round one: the budget is exhausted (more=true). Nothing may be
        // acknowledged or released, even though the round itself succeeded.
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_yield");
        check(plan->ready_calls.load() == 1, "round one must run on_ready exactly once");
        check(planned_removed.load() == 0,
              "a budget-exhausted round must not deliver Removed");
        check(CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_yield") == 1,
              "a budget-exhausted round must not release the snapshot");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_yield") == 1,
              "a budget-exhausted round must not consume the close");
        check(close_sink.count(7201) == 0,
              "no completion may be reported while the tail is still being produced");

        // Round two: the executor requeues a yielded lane by itself, so this
        // round runs without any new readiness notification. Only now is the
        // drain proven and the close processed.
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_yield");
        check(plan->ready_calls.load() == 2,
              "the continuation must call on_ready again without new readiness");
        check(CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_yield") == 0,
              "the proven drain must release the snapshot");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_yield") == 0,
              "the proven drain must consume the close");
        check(planned_removed.load() == 1,
              "Removed must be delivered only after the drain is proven");
        check(close_sink.count(7201) == 1,
              "the completion must land exactly after the final round");
        check(planned_added.load() == 1, "the attachment must deliver exactly one Added");

        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    for (const std::string kind : {"throw", "failed", "output"}) {
        run_regression(("yield-failure-" + kind).c_str(), [&] {
            const uint64_t id = kind == "throw" ? 7203 : kind == "failed" ? 7204 : 7205;
            const std::string name = "ev_failure_" + kind;
            auto plan = std::make_shared<CollectorSchedulerTestAccess::EventDrainPlan>();
            plan->more_plan = {true, true, false};
            plan->failure = kind;
            plan->job_id = id;
            std::atomic<int> removed{0};
            CollectorSchedulerTestAccess::addPlannedEvent(scheduler, name, plan, {}, bump(removed));
            close_source.set({record_of(id, RecordState::Active, {name})});
            CollectorSchedulerTestAccess::reconcile(scheduler);
            CollectorSchedulerTestAccess::pauseLane(scheduler, name);
            close_source.set({record_of(id, RecordState::Closing, {})});
            CollectorSchedulerTestAccess::reconcile(scheduler);
            CollectorSchedulerTestAccess::stageReadiness(scheduler, name);
            check(CollectorSchedulerTestAccess::drainLane(scheduler, name) == WorkResult::Yield,
                  "initial unfinished drain must yield");
            check(CollectorSchedulerTestAccess::drainLane(scheduler, name) == WorkResult::Complete,
                  "failed drain must stop immediate requeue");
            check(removed.load() == 0 && close_sink.count(id) == 0 &&
                      CollectorSchedulerTestAccess::pendingCloses(scheduler, name) == 1 &&
                      CollectorSchedulerTestAccess::snapshotCount(scheduler, name) == 1,
                  "failure must retain close and snapshot without Removed or completion");
            check(status(name)["has_error"] == true, "failure must be visible in status");
            // The collector would now return success with an empty source list.
            // That callback cannot prove what the failed round lost.
            CollectorSchedulerTestAccess::drainLane(scheduler, name);
            CollectorSchedulerTestAccess::reconcile(scheduler);
            check(plan->ready_calls.load() == 2 && removed.load() == 0 && close_sink.count(id) == 0 &&
                      CollectorSchedulerTestAccess::pendingCloses(scheduler, name) == 1 &&
                      CollectorSchedulerTestAccess::snapshotCount(scheduler, name) == 1,
                  "empty success must not launder an unresolved failed drain");
            if (kind == "output")
                check(plan->accepted.load() == 1 && plan->rejected.load() == 1,
                      "accepted output must not be replayed after admission rejection");
            check(status(name)["has_error"] == true, "failed epoch must keep its visible error");
            close_source.set({});
            CollectorSchedulerTestAccess::reconcile(scheduler);
        });
    }

    run_regression("failed-drain-stop", [&] {
        auto plan = std::make_shared<CollectorSchedulerTestAccess::EventDrainPlan>();
        plan->more_plan = {true, true};
        plan->failure = "throw";
        plan->job_id = 7206;
        std::atomic<int> removed{0};
        CollectorSchedulerTestAccess::addPlannedEvent(scheduler, "ev_failed_stop", plan, {}, bump(removed));
        close_source.set({record_of(7206, RecordState::Active, {"ev_failed_stop"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_failed_stop");
        close_source.set({record_of(7206, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::stageReadiness(scheduler, "ev_failed_stop");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_stop");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_stop");
        CollectorSchedulerTestAccess::stopLane(scheduler, "ev_failed_stop");
        check(removed.load() == 0 && close_sink.count(7206) == 0,
              "stopping an unproven drain must not report Removed or completion");
        check(!CollectorSchedulerTestAccess::restartLane(scheduler, "ev_failed_stop"),
              "restart must reject a failed epoch with unresolved obligations");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_failed_stop") == 1 &&
                  CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_failed_stop") == 1 &&
                  status("ev_failed_stop")["has_error"] == true,
              "stop and rejected restart must retain the failed close and error");
        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("failed-readiness-suppressed", [&] {
        auto plan = std::make_shared<CollectorSchedulerTestAccess::EventDrainPlan>();
        plan->more_plan = {true, true, true, false};
        plan->failure = "throw";
        CollectorSchedulerTestAccess::addPlannedEvent(scheduler, "ev_failed_signal", plan);
        close_source.set({record_of(7207, RecordState::Active, {"ev_failed_signal"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_failed_signal");
        close_source.set({record_of(7207, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::stageReadiness(scheduler, "ev_failed_signal");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_signal");
        CollectorSchedulerTestAccess::stageReadiness(scheduler, "ev_failed_signal");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_signal");
        check(plan->sources_of(1) == 1, "failed callback must actually receive readiness");
        CollectorSchedulerTestAccess::holdBackoff(scheduler, "ev_failed_signal");
        check(CollectorSchedulerTestAccess::signalFailedLane(scheduler, "ev_failed_signal") > 0,
              "fresh readiness must remain available after failure");
        check(CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_signal") == WorkResult::Complete &&
                  plan->ready_calls.load() == 2,
              "immediate rearmed readiness must not hot-spin a failed callback");
        CollectorSchedulerTestAccess::expireBackoff(scheduler, "ev_failed_signal");
        check(CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_signal") == WorkResult::Yield,
              "fresh work may yield even when an earlier tail failed");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_signal");
        check(plan->ready_calls.load() == 4 && close_sink.count(7207) == 0 &&
                  CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_failed_signal") == 1 &&
                  CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_failed_signal") == 1,
              "fresh readiness must run without laundering the failed tail");
        check(status("ev_failed_signal")["has_error"] == true,
              "successful fresh Job events must not clear the earlier failed-tail error");
        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("failed-manual-stop", [&] {
        auto plan = std::make_shared<CollectorSchedulerTestAccess::EventDrainPlan>();
        plan->more_plan = {true, true};
        plan->failure = "throw";
        CollectorSchedulerTestAccess::addPlannedEvent(scheduler, "ev_failed_manual", plan);
        close_source.set({record_of(7208, RecordState::Active, {"ev_failed_manual"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_failed_manual");
        CollectorSchedulerTestAccess::stageReadiness(scheduler, "ev_failed_manual");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_manual");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_failed_manual");
        CollectorSchedulerTestAccess::update(scheduler, 7208, {});
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_failed_manual") == 1 &&
                  CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_failed_manual") == 1,
              "manual stop must retain the failed epoch obligation and snapshot");
        close_source.set({record_of(7208, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(close_sink.count(7208) == 0, "retirement must adopt the failed manual-stop tail");
        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("proven-completion-during-shutdown", [&] {
        auto plan = std::make_shared<CollectorSchedulerTestAccess::EventDrainPlan>();
        plan->block_first = true;
        CollectorSchedulerTestAccess::addPlannedEvent(scheduler, "ev_shutdown_proof", plan);
        close_source.set({record_of(7299, RecordState::Active, {"ev_shutdown_proof"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_shutdown_proof");
        close_source.set({record_of(7299, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        CollectorSchedulerTestAccess::stageReadiness(scheduler, "ev_shutdown_proof");
        std::thread worker([&] {
            CollectorSchedulerTestAccess::drainLane(scheduler, "ev_shutdown_proof");
        });
        {
            std::unique_lock lock(plan->mutex);
            plan->barrier_cv.wait(lock, [&] { return plan->entered; });
            CollectorSchedulerTestAccess::setStopping(scheduler, true);
            plan->release = true;
            plan->barrier_cv.notify_one();
        }
        worker.join();
        CollectorSchedulerTestAccess::setStopping(scheduler, false);
        check(close_sink.count(7299) == 1, "proven completion must survive shutdown start");
        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("single-notification-two-rounds", [&] {
        // One readiness notification, two on_ready rounds. The continuation
        // must run without a second notification (sources empty in the second
        // round) and carry the drain to its acknowledgement.
        auto plan = std::make_shared<CollectorSchedulerTestAccess::EventDrainPlan>();
        plan->more_plan = {true, false};
        std::atomic<int> cont_added{0}, cont_removed{0};
        CollectorSchedulerTestAccess::addPlannedEvent(scheduler, "ev_cont", plan,
                                                      bump(cont_added), bump(cont_removed));
        close_source.set({record_of(7202, RecordState::Active, {"ev_cont"})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::memberCount(scheduler, "ev_cont") == 1,
              "the continuing lane must attach");
        CollectorSchedulerTestAccess::pauseLane(scheduler, "ev_cont");

        // The single readiness notification, staged before the first round.
        CollectorSchedulerTestAccess::stageReadiness(scheduler, "ev_cont");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_cont");
        check(plan->ready_calls.load() == 1, "the first round must run on_ready once");
        check(plan->sources_of(0) == 1, "the first round must receive the staged readiness");
        check(cont_added.load() == 1, "the attachment must deliver exactly one Added");

        // The close arrives between the two rounds, while the drain is still
        // unfinished. The deferred state must survive the intervening
        // reconcile: the next round continues without new readiness.
        close_source.set({record_of(7202, RecordState::Closing, {})});
        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_cont") == 1,
              "the mid-drain Closing must defer the close");

        CollectorSchedulerTestAccess::drainLane(scheduler, "ev_cont");
        check(plan->ready_calls.load() == 2,
              "the second round must run without a second notification");
        check(plan->sources_of(1) == 0,
              "the continuation round must run on an empty readiness list");
        check(CollectorSchedulerTestAccess::pendingCloses(scheduler, "ev_cont") == 0 &&
                  CollectorSchedulerTestAccess::snapshotCount(scheduler, "ev_cont") == 0,
              "the completed drain must release the close and the snapshot");
        check(cont_removed.load() == 1,
              "the continuation must deliver exactly one Removed");
        check(close_sink.count(7202) == 1,
              "the single notification must carry the drain to its acknowledgement");

        close_source.set({});
        CollectorSchedulerTestAccess::reconcile(scheduler);
    });

    run_regression("real-archive-boundary", [&] {
        // The completion sink used by the other regressions is a fake: it never
        // reaches the checked cleanup or the durable archive. This fixture runs
        // the REAL JobRegistry and the REAL CollectorScheduler against their
        // own temporary LevelDB directory, with a real selection command and a
        // real retirement, and pauses/drains the lanes through test access.
        const std::string live_db = base + "-archive.db";
        std::filesystem::remove_all(live_db);
        JobRegistryOptions options;
        options.db_path = live_db;
        options.test_static_registry = true;
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry archive(options);
        struct SourceReset {
            CollectorScheduler& scheduler;
            ~SourceReset() { scheduler.attach(nullptr, nullptr); }
        } reset{scheduler};
        archive.setCollectorValidator(nullptr);
        archive.setKernelCleanupForTest([&](uint64_t, const Job&, std::string& error) {
            if (archive_cleanup_failures.load() > 0) {
                --archive_cleanup_failures;
                error = "injected checked-cleanup failure";
                return false;
            }
            return true;
        });
        scheduler.attach(&archive, &archive);
        archive.setCollectionRuntime(&scheduler);
        CollectorSchedulerTestAccess::addEvent(scheduler, "ri_ev_a", {}, {}, bump(ri_a_removed));
        CollectorSchedulerTestAccess::addEvent(scheduler, "ri_ev_b", {}, {}, bump(ri_b_removed));
        CollectorSchedulerTestAccess::addEvent(scheduler, "ri_ev_c", {}, {}, bump(ri_c_removed));
        CollectorSchedulerTestAccess::addEvent(scheduler, "ri_ev_d", {}, {}, bump(ri_d_removed));

        auto archive_job = [](uint64_t id) {
            Job job = CollectorSchedulerTestAccess::makeJob(id, {"ri_ev_a"});
            set_pids(job, {getpid()});
            job.jobtype = JobType::Job;
            job.subtype = JobSubType::Condor;
            CondorJobAttr attr{};
            attr.cluster_id = static_cast<size_t>(id);
            attr.proc_id = 0;
            job.sub_attr = attr;
            return job;
        };
        auto start_real_instance = [&](uint64_t id, const std::string& first,
                                       const std::string& second, bool stop_first_lane) {
            check(archive.addJob(archive_job(id)),
                  "register the real archive-boundary instance");
            CollectionRequest select;
            select.targets = {id};
            select.collectors = std::vector<std::string>{first, second};
            const auto selected = archive.setCollectors(select);
            check(selected.ok, "the real selection command must commit: " + selected.message);
            CollectorSchedulerTestAccess::reconcile(scheduler);
            check(CollectorSchedulerTestAccess::memberCount(scheduler, first) == 1 &&
                      CollectorSchedulerTestAccess::memberCount(scheduler, second) == 1,
                  "both real event lanes must attach to the real record");
            CollectorSchedulerTestAccess::pauseLane(scheduler, first);
            CollectorSchedulerTestAccess::pauseLane(scheduler, second);
            if (stop_first_lane) {
                // Real manual stop while the record is still Active: the lane owes
                // a tail but holds no membership when the record turns Closing.
                CollectionRequest stop_one;
                stop_one.targets = {id};
                stop_one.collectors = std::vector<std::string>{second};
                const auto stopped = archive.setCollectors(stop_one);
                check(stopped.ok, "the real manual stop must commit: " + stopped.message);
                CollectorSchedulerTestAccess::reconcile(scheduler);
                check(CollectorSchedulerTestAccess::memberCount(scheduler, first) == 0 &&
                          CollectorSchedulerTestAccess::pendingCloses(scheduler, first) == 1,
                      "the stopped lane must owe a tail with no membership");
            }
            check(archive.retireInstance(id), "the real retirement must be accepted");
            const auto record = archive.jobRecord(id);
            check(record.has_value() && record->state == RecordState::Closing,
                  "the real durable record must stay Closing until the drain completes");
            CollectorSchedulerTestAccess::reconcile(scheduler);
        };

        // Case 1: archive lands exactly after the last acknowledgement. The
        // stopped lane's adopted tail must gate the real boundary too.
        start_real_instance(8101, "ri_ev_a", "ri_ev_b", /*stop_first_lane=*/true);
        auto active_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-1",
                                                    "j:" + hex_id(8101));
        auto history_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-2",
                                                     "h:" + hex_id(8101) + ":");
        check(active_rows.size() == 1 && history_rows.empty(),
              "before the last ack the Closing row must be durable and no history row may exist");

        CollectorSchedulerTestAccess::drainLane(scheduler, "ri_ev_b");
        check(archive.jobRecord(8101).has_value(),
              "the attached lane's ack must not archive while the adopted tail is pending");
        history_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-3",
                                                "h:" + hex_id(8101) + ":");
        check(history_rows.empty(), "the first ack must not write a history row");
        check(ri_b_removed.load() == 1 && ri_a_removed.load() == 0,
              "an acknowledgement must not deliver another lane's Removed");

        // The last proven lane completes after shutdown has begun; the real
        // registry sink must still receive the acknowledgement.
        CollectorSchedulerTestAccess::setStopping(scheduler, true);
        CollectorSchedulerTestAccess::drainLane(scheduler, "ri_ev_a");
        CollectorSchedulerTestAccess::setStopping(scheduler, false);
        check(!archive.jobRecord(8101).has_value() && !has_active(archive, 8101),
              "the last ack must archive the record and release the instance");
        history_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-4",
                                                "h:" + hex_id(8101) + ":");
        active_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-5",
                                               "j:" + hex_id(8101));
        check(history_rows.size() == 1 && active_rows.empty(),
              "the archive must leave exactly one history row and no active row");
        check(ri_a_removed.load() == 1 && ri_b_removed.load() == 1,
              "each lane must deliver exactly one Removed");

        // Case 2: a checked-cleanup failure keeps the durable Closing row; the
        // next scheduler completion retry archives it with no new tail and no
        // duplicate Removed.
        start_real_instance(8102, "ri_ev_c", "ri_ev_d", /*stop_first_lane=*/false);
        archive_cleanup_failures = 1;
        CollectorSchedulerTestAccess::drainLane(scheduler, "ri_ev_c");
        CollectorSchedulerTestAccess::drainLane(scheduler, "ri_ev_d");
        const auto failed = archive.jobRecord(8102);
        check(failed.has_value() && failed->state == RecordState::Closing,
              "a failed checked cleanup must keep the Closing record");
        active_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-6",
                                               "j:" + hex_id(8102));
        history_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-7",
                                                "h:" + hex_id(8102) + ":");
        check(active_rows.size() == 1 && history_rows.empty(),
              "a failed cleanup must leave the durable row untouched");
        check(ri_c_removed.load() == 1 && ri_d_removed.load() == 1,
              "the failed archive must not repeat a lane's Removed");

        CollectorSchedulerTestAccess::reconcile(scheduler);
        check(!archive.jobRecord(8102).has_value(),
              "the completion retry must archive the retained Closing record");
        history_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-8",
                                                "h:" + hex_id(8102) + ":");
        active_rows = raw_keys_from_store_copy(live_db, base + "-archive-copy-9",
                                               "j:" + hex_id(8102));
        check(history_rows.size() == 1 && active_rows.empty(),
              "the retried archive must land exactly one history row");
        check(ri_c_removed.load() == 1 && ri_d_removed.load() == 1,
              "the retry must not deliver another Removed");
    });

    if (!regression_failures.empty()) {
        std::string joined;
        for (const auto& failure : regression_failures) {
            if (!joined.empty()) joined += "; ";
            joined += failure;
        }
        throw std::runtime_error("close protocol regressions failed: " + joined);
    }

    // The scheduler converges to the INJECTED record source, never the global
    // registry singleton: an instance only the injected source knows about is
    // attached, and the global registry does not know it exists.
    class StaticSource : public IJobRecordSource {
    public:
        std::vector<JobRecord> records;
        bool runtimeRecords(std::vector<JobRecord>& out) const override {
            out = records;
            return true;
        }
        bool runtimeRecord(uint64_t job_id,
                           std::optional<JobRecord>& out) const override {
            for (const auto& record : records) {
                if (record.job.JobID == job_id) {
                    out = record;
                    return true;
                }
            }
            out.reset();
            return true;
        }
    };

    StaticSource injected;
    JobRecord injected_record;
    injected_record.job.JobID = 777;
    injected_record.state = RecordState::Active;
    injected_record.revision = 1;
    injected_record.job.JobID = 777;
    injected_record.job.jobtype = JobType::Sys;
    injected_record.job.subtype = JobSubType::Common;
    injected_record.job.sub_attr = CommonJobAttr{.auto_update_child = false};
    injected_record.selection.manual = std::vector<std::string>{"idle"};
    injected.records.push_back(injected_record);

    scheduler.attach(&injected, nullptr);
    CollectorSchedulerTestAccess::reconcile(scheduler);
    const auto injected_actual = scheduler.actual(777);
    check(injected_actual.tracked &&
              injected_actual.collectors == std::vector<std::string>{"idle"},
          "the scheduler must converge to the injected record source");
    check(!has_active(registry, 777),
          "the global registry must not know the injected instance");

    scheduler.shutdown();
    check(rpc(path, "CollectorScheduler/status")["lifecycle"] == "stopped", "stopped");
    check(status("blocked")["timer_active"] == false, "timer stopped");
    check(status("blocked").at("current_job").is_null(), "stale current Job after completion");
    check(rpc(path, "CollectorScheduler/status").at("scheduler").at("currentQueuedTasks") == 0,
          "shutdown retained pending callbacks");
    server.stop();
    std::this_thread::sleep_for(150ms);
    unlink((base + ".yaml").c_str());
    std::cout << "PASS: a yielded event drain defers the closing acknowledgement until the tail is proven\n";
    std::cout << "PASS: a single readiness notification sustains a two-round drain to its final ack\n";
    std::cout << "PASS: actual scheduler + actual Unix-socket RPC boundary tests\n";
}

int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "Usage: scheduler_status_test TEMP_BASE\n"; return 2; }
    try { run(argv[1]); }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
