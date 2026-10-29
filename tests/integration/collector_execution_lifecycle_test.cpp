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
#include "common/config.hpp"
#include "common/local_rpc.hpp"
#include "core/collector_execution_context.hpp"
#include "core/collector_registry.hpp"
#include "core/collector_scheduler.hpp"
#include "core/job_collection_manager.hpp"
#include "core/job_registry.hpp"
#include "core/writer_manager.hpp"
#include "writer/base_writer.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std::chrono_literals;

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

template <typename Predicate>
void eventually(Predicate predicate, const std::string& message,
                std::chrono::milliseconds timeout = 5s)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline)
            throw std::runtime_error("timeout: " + message);
        std::this_thread::sleep_for(5ms);
    }
}

class CaptureWriter : public BaseWriter {
public:
    CaptureWriter(std::string name, std::string type, std::string config)
        : BaseWriter(std::move(name), std::move(type), std::move(config)) {}
    ~CaptureWriter() override { shutdown(); }
    bool accepts(OutputKind kind) const override { return kind == OutputKind::Text; }

protected:
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override
    {
        std::lock_guard lock(store().mutex);
        for (const auto& record : batch) store().records.push_back(record);
        store().cv.notify_all();
        return true;
    }

public:
    struct Store {
        std::mutex mutex;
        std::condition_variable cv;
        std::vector<OutputRecordPtr> records;
    };
    static Store& store()
    {
        static Store value;
        return value;
    }
    static std::vector<OutputRecordPtr> wait_for(const std::string& collector, size_t count)
    {
        std::unique_lock lock(store().mutex);
        const bool ok = store().cv.wait_for(lock, 5s, [&] {
            size_t matched = 0;
            for (const auto& record : store().records)
                if (record->sample->collector_name == collector) ++matched;
            return matched >= count;
        });
        check(ok, "collector " + collector + " did not deliver " + std::to_string(count) + " record(s)");
        std::vector<OutputRecordPtr> result;
        for (const auto& record : store().records)
            if (record->sample->collector_name == collector) result.push_back(record);
        return result;
    }
    static size_t count(const std::string& collector)
    {
        std::lock_guard lock(store().mutex);
        size_t matched = 0;
        for (const auto& record : store().records)
            if (record->sample->collector_name == collector) ++matched;
        return matched;
    }
};

struct FakePeriodicJob : IPeriodicJobCollector {
    static inline std::atomic<int> calls{0};
    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    CollectOutcome collect(const Job&, OutputContext& output) override
    {
        ++calls;
        (void)output.emit("text", Text{"periodic-job\n"});
        return {};
    }
    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }
};

struct FakePeriodicSystem : IPeriodicSystemCollector {
    static inline std::atomic<int> calls{0};
    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    CollectOutcome collect(OutputContext& output) override
    {
        ++calls;
        (void)output.emit("text", Text{"periodic-system\n"});
        return {};
    }
    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }
};

struct FakeJobEvent : IEventJobCollector {
    static inline std::atomic<int> ready_calls{0};
    static inline std::atomic<int> added{0};
    static inline std::atomic<int> removed{0};
    static inline std::atomic<int> unknown{0};
    static inline std::atomic<int> writer_fd{-1};
    static inline std::atomic<int> reader_fd{-1};
    // Target of byte '3', so a test can drive an event for a job id allocated at
    // runtime by the inventory.
    static inline std::atomic<uint64_t> extra_job_id{0};

    bool init(const nlohmann::json&, CollectorInitContext& context) override
    {
        int sockets[2] = {-1, -1};
        if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0) return false;
        reader_ = sockets[0];
        writer_ = sockets[1];
        const int flags = fcntl(reader_, F_GETFL, 0);
        if (fcntl(reader_, F_SETFL, flags | O_NONBLOCK) != 0) return false;
        registrar_ = &context.sources();
        auto registration = context.sources().watch_fd(reader_, Interest::Readable);
        if (!registration) return false;
        registration_ = std::move(*registration);
        handle_ = registration_->handle();
        reader_fd = reader_;
        writer_fd = writer_;
        return true;
    }

    void deinit() noexcept override
    {
        registration_.reset();
        if (reader_ >= 0) close(reader_);
        if (writer_ >= 0) close(writer_);
        reader_ = writer_ = -1;
        reader_fd = -1;
        writer_fd = -1;
    }

    void on_job_added(const Job&) override { ++added; }
    void on_job_removed(const Job&) override { ++removed; }

    DrainOutcome on_ready(const ReadySources&, const WorkBudget&, JobEventContext& context) override
    {
        ++ready_calls;
        size_t consumed = 0;
        char buffer[64];
        for (;;) {
            const ssize_t size = recv(reader_, buffer, sizeof(buffer), 0);
            if (size <= 0) break;
            for (ssize_t index = 0; index < size; ++index) {
                uint64_t job_id = 0;
                if (buffer[index] == '1') job_id = 101;
                else if (buffer[index] == '2') job_id = 202;
                else if (buffer[index] == '3') job_id = extra_job_id.load();
                else if (buffer[index] == 'x') job_id = 999;
                else continue;
                ++consumed;
                auto session = context.open_session(job_id, 1);
                if (!session) {
                    ++unknown;
                    continue;
                }
                (void)session->context().emit("text", Text{std::to_string(job_id) + "\n"});
            }
        }
        if (registrar_ && registration_) registrar_->rearm(handle_);
        return {CollectOutcome{}, consumed, false};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

private:
    int reader_ = -1;
    int writer_ = -1;
    SourceRegistrar* registrar_ = nullptr;
    std::optional<SourceRegistration> registration_;
    SourceHandle handle_{};
};

struct FakeSystemEvent : IEventSystemCollector {
    static inline std::atomic<int> ready_calls{0};
    static inline std::atomic<int> writer_fd{-1};
    static inline std::atomic<uint64_t> last_job_id{1};

    bool init(const nlohmann::json&, CollectorInitContext& context) override
    {
        int sockets[2] = {-1, -1};
        if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0) return false;
        reader_ = sockets[0];
        writer_ = sockets[1];
        const int flags = fcntl(reader_, F_GETFL, 0);
        if (fcntl(reader_, F_SETFL, flags | O_NONBLOCK) != 0) return false;
        registrar_ = &context.sources();
        auto registration = context.sources().watch_fd(reader_, Interest::Readable);
        if (!registration) return false;
        registration_ = std::move(*registration);
        handle_ = registration_->handle();
        writer_fd = writer_;
        return true;
    }

    void deinit() noexcept override
    {
        registration_.reset();
        if (reader_ >= 0) close(reader_);
        if (writer_ >= 0) close(writer_);
        reader_ = writer_ = -1;
        writer_fd = -1;
    }

    DrainOutcome on_ready(const ReadySources&, const WorkBudget&, SystemEventContext& context) override
    {
        ++ready_calls;
        size_t consumed = 0;
        char buffer[64];
        for (;;) {
            const ssize_t size = recv(reader_, buffer, sizeof(buffer), 0);
            if (size <= 0) break;
            for (ssize_t index = 0; index < size; ++index) {
                ++consumed;
                auto session = context.open_session();
                last_job_id = session.context().sample().job.JobID;
                (void)session.context().emit("text", Text{"system-event\n"});
            }
        }
        if (registrar_ && registration_) registrar_->rearm(handle_);
        return {CollectOutcome{}, consumed, false};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

private:
    int reader_ = -1;
    int writer_ = -1;
    SourceRegistrar* registrar_ = nullptr;
    std::optional<SourceRegistration> registration_;
    SourceHandle handle_{};
};

AUTO_REGISTER_JOB_COLLECTOR(FakePeriodicJob, "fake periodic Job");
AUTO_REGISTER_SYSTEM_COLLECTOR(FakePeriodicSystem, "fake periodic System");
AUTO_REGISTER_JOB_EVENT_COLLECTOR(FakeJobEvent, "fake Job event");
AUTO_REGISTER_SYSTEM_EVENT_COLLECTOR(FakeSystemEvent, "fake System event");

Job make_job(uint64_t id, std::vector<std::string> collectors)
{
    Job job;
    job.JobID = id;
    job.NativeJobID = std::to_string(id);
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    job.JobPIDs = {getpid()};
    job.CollectorNames = std::move(collectors);
    CommonJobAttr attr;
    attr.auto_update_child = false;
    job.sub_attr = attr;
    return job;
}

void send_byte(int fd, char value)
{
    check(fd >= 0, "source writer is not initialized");
    check(send(fd, &value, 1, MSG_NOSIGNAL) == 1, "send source byte");
}

nlohmann::json collector_status(CollectorScheduler& scheduler, const std::string& name)
{
    const auto status = scheduler.snapshot();
    for (const auto& collector : status.at("collectors"))
        if (collector.at("name") == name) return collector;
    throw std::runtime_error("missing collector status: " + name);
}

void run(const std::string& base)
{
    const std::string socket_path = base + ".sock";
    {
        std::ofstream config(base + ".yaml");
        config << "lens_config:\n  max_collector_threads: 2\n"
                  "collectors_config:\n"
                  "  enable_collector_perf: false\n"
                  "  default_freq: 20\n"
                  "  default_use_writers: []\n"
                  "  collectors:\n"
                  "    - {name: job_event, type: FakeJobEvent, config: job_event_cfg}\n"
                  "    - {name: sys_event, type: FakeSystemEvent, config: sys_event_cfg}\n"
                  "    - {name: job_periodic, type: FakePeriodicJob, config: job_periodic_cfg}\n"
                  "    - {name: sys_periodic, type: FakePeriodicSystem, config: sys_periodic_cfg}\n"
                  "writers_config:\n"
                  "  buffer_capacity: 256\n"
                  "  max_pending_records: 4096\n"
                  "  max_pending_bytes: 67108864\n"
                  "  max_record_bytes: 8388608\n"
                  "  writers:\n"
                  "    - {name: w, type: CaptureWriter, config: cap}\n"
                  "cap: {}\n"
                  "job_event_cfg:\n  outputs: {text: [w]}\n"
                  "sys_event_cfg:\n  auto_start: true\n  outputs: {text: [w]}\n"
                  "job_periodic_cfg:\n  freq: 50\n  outputs: {text: [w]}\n"
                  "sys_periodic_cfg:\n  freq: 20\n  auto_start: true\n  outputs: {text: [w]}\n"
                  "job_registry_config:\n  job_db_path: " << base << ".db\n";
    }

    auto& server = RPCServer::instance(socket_path);
    (void)server;
    Config::instance(base + ".yaml");
    WriterManager::instance().registerWriter<CaptureWriter>("CaptureWriter");

    auto& scheduler = CollectorScheduler::instance();
    scheduler.start();

    // System periodic runs with zero Jobs and System event uses Job 0.
    eventually([] { return FakePeriodicSystem::calls.load() > 0; },
               "System periodic collector did not run with zero Jobs");
    eventually([] { return FakeSystemEvent::writer_fd.load() >= 0; },
               "System event source was not initialized");
    send_byte(FakeSystemEvent::writer_fd.load(), 's');
    const auto system_event = CaptureWriter::wait_for("sys_event", 1);
    check(system_event[0]->sample->job.JobID == 0, "System event did not use Job 0 envelope");
    check(FakeSystemEvent::last_job_id.load() == 0, "System event sample used a real Job");
    check(FakeSystemEvent::ready_calls.load() >= 1, "System event handler did not run");

    const auto job_one = make_job(101, {"job_event", "job_periodic"});
    check(JobRegistry::instance().addJob(job_one), "add Job 101");
    eventually([] { return FakeJobEvent::writer_fd.load() >= 0; },
               "Job event source was not initialized on first binding");
    eventually([] { return FakePeriodicJob::calls.load() > 0; },
               "Job periodic collector did not start on first binding");

    send_byte(FakeJobEvent::writer_fd.load(), '1');
    const auto first = CaptureWriter::wait_for("job_event", 1);
    check(first[0]->sample->job.JobID == 101, "Job event used the wrong Job snapshot");
    check(FakeJobEvent::added.load() >= 1, "on_job_added did not run");

    const auto job_two = make_job(202, {"job_event", "job_periodic"});
    check(JobRegistry::instance().addJob(job_two), "add Job 202");
    send_byte(FakeJobEvent::writer_fd.load(), '2');
    const auto two = CaptureWriter::wait_for("job_event", 2);
    check(two[1]->sample->job.JobID == 202, "second Job event attribution");

    send_byte(FakeJobEvent::writer_fd.load(), 'x');
    eventually([&] { return collector_status(scheduler, "job_event").at("has_error").get<bool>(); },
               "unknown Job event did not produce a diagnostic");
    check(FakeJobEvent::unknown.load() >= 1, "unknown Job event was not counted");
    check(CaptureWriter::count("job_event") == 2, "unknown Job event produced output");

    // One Job ends; the other continues through the same source/lane.
    JobRegistry::instance().delJob(101);
    eventually([&] { return JobRegistry::instance().closingJobCount() == 0; },
               "closing Job was not acknowledged by all collectors");
    eventually([&] { return FakeJobEvent::removed.load() >= 1; },
               "on_job_removed did not run for the closing Job");
    send_byte(FakeJobEvent::writer_fd.load(), '2');
    const auto after_close = CaptureWriter::wait_for("job_event", 3);
    check(after_close[2]->sample->job.JobID == 202, "surviving Job event attribution");
    eventually([&] { return !collector_status(scheduler, "job_event").at("has_error").get<bool>(); },
               "successful event did not clear the unknown-Job error");

    const auto event_status = collector_status(scheduler, "job_event");
    check(event_status.at("scope") == "job", "job_event scope status");
    check(event_status.at("trigger") == "event", "job_event trigger status");
    check(event_status.at("output_policy") == "stream", "job_event output policy status");
    check(collector_status(scheduler, "sys_periodic").at("scope") == "system",
          "System periodic scope status");
    check(event_status.at("operations").contains("event_drain"),
          "event drain observation missing");
    check(scheduler.snapshot().at("scheduler").at("scheduling_semantics_version") == 2,
          "scheduling semantics version missing");

    JobRegistry::instance().delJob(202);
    eventually([&] { return JobRegistry::instance().closingJobCount() == 0; },
               "last Job was not acknowledged");

    // Removing the last real Job must not stop System collectors.
    const int system_before = FakePeriodicSystem::calls.load();
    eventually([&] { return FakePeriodicSystem::calls.load() > system_before; },
               "System periodic stopped after the last Job was removed");
    send_byte(FakeSystemEvent::writer_fd.load(), 's');
    const auto system_after = CaptureWriter::wait_for("sys_event", 2);
    check(system_after[1]->sample->job.JobID == 0,
          "System event lost its Job 0 envelope after Job removal");

    // ---------------------------------------------------------------------
    // Collection intent reaches the lanes (decoupling task 4/6).
    //
    // An inventoried-but-unbound instance must create no collector work; a
    // committed binding must attach the lane and report itself as applied; a
    // persistent stop must detach it; retirement must still acknowledge.
    // ---------------------------------------------------------------------
    JobCollectionManager collection(JobRegistry::instance());
    collection.setCollectorValidator([](const std::string& name) {
        return name == "job_event" || name == "job_periodic";
    });

    const auto upsert = [](const std::string& native) {
        ObservedExecution execution;
        execution.selector.adapter = "condor";
        execution.selector.cluster_name = "htcondor01";
        execution.selector.native_job_id = native;
        execution.generation.boot_id = "test-boot";
        execution.generation.manager_pid = getpid();
        execution.generation.manager_start_ticks = 1;
        execution.generation.cgroup_identity = "";
        execution.processes.push_back(ProcessIdentity{getpid(), 1, "test-boot"});
        Job facts;
        facts.jobtype = JobType::Job;
        facts.subtype = JobSubType::Condor;
        CondorJobAttr attr;
        attr.auto_update_child = false;
        attr.starter_pid = getpid();
        facts.sub_attr = attr;
        execution.facts = facts;
        return JobRegistry::instance().upsertObservedJob(execution);
    };

    const auto bound = upsert("700.0");
    check(bound.ok && bound.disposition == UpsertDisposition::Created,
          "inventory upsert must create the instance");
    check(collector_status(scheduler, "job_event").at("job_count").get<size_t>() == 0,
          "an unbound inventory instance must not attach a collector lane");
    const int added_before_bind = FakeJobEvent::added.load();

    CollectionRequest bind;
    bind.targets = {bound.internal_id};
    bind.collectors = std::vector<std::string>{"job_event"};
    const auto bound_outcome = collection.setCollectors(bind);
    check(bound_outcome.ok, "set must be accepted: " + bound_outcome.message);
    const auto& target = bound_outcome.targets.front();
    check(target.binding.revision == 1, "first binding is revision 1");
    check(target.applied.applied,
          "binding must be applied by the runtime, got: " + target.applied.error);
    check(target.applied.collectors == std::vector<std::string>{"job_event"},
          "applied collectors must name the attached lane");
    check(target.applied.revision == target.binding.revision,
          "applied revision must match the committed revision");
    check(FakeJobEvent::added.load() > added_before_bind,
          "on_job_added must run when intent attaches a lane");
    check(collector_status(scheduler, "job_event").at("job_count").get<size_t>() == 1,
          "the lane must hold exactly the bound instance");

    // The bound instance produces events through the existing source.
    FakeJobEvent::extra_job_id.store(bound.internal_id);
    send_byte(FakeJobEvent::writer_fd.load(), '3');
    eventually([&] {
        return CaptureWriter::count("job_event") >= 3;
    }, "bound inventory instance did not produce output");

    // A persistent stop detaches the lane but keeps the inventory record.
    const int removed_before_stop = FakeJobEvent::removed.load();
    CollectionRequest stop;
    stop.targets = {bound.internal_id};
    stop.collectors = std::vector<std::string>{};
    const auto stopped = collection.setCollectors(stop);
    check(stopped.ok, "stop must be accepted: " + stopped.message);
    check(stopped.targets.front().applied.applied, "stop must be applied");
    check(stopped.targets.front().applied.collectors.empty(),
          "nothing may remain applied after a persistent stop");
    check(FakeJobEvent::removed.load() > removed_before_stop,
          "on_job_removed must run when a stop detaches the lane");
    check(collector_status(scheduler, "job_event").at("job_count").get<size_t>() == 0,
          "the stopped instance must leave the lane");
    check(JobRegistry::instance().findJob(bound.internal_id).has_value(),
          "stopping collection must keep the inventory record");

    // Retirement of a stopped instance is acknowledged without leaving a lane.
    check(JobRegistry::instance().retireInstance(bound.internal_id),
          "retire the stopped instance");
    eventually([&] { return JobRegistry::instance().closingJobCount() == 0; },
               "stopped instance was not acknowledged after retirement");

    // Natural completion while bound must detach the lane exactly once.
    const auto bound_two = upsert("701.0");
    check(bound_two.ok, "second inventory upsert");
    CollectionRequest bind_two;
    bind_two.targets = {bound_two.internal_id};
    bind_two.collectors = std::vector<std::string>{"job_event"};
    check(collection.setCollectors(bind_two).ok, "second bind");
    const int removed_before_retire = FakeJobEvent::removed.load();
    check(JobRegistry::instance().retireInstance(bound_two.internal_id),
          "retire the bound instance");
    eventually([&] { return JobRegistry::instance().closingJobCount() == 0; },
               "bound instance was not acknowledged after retirement");
    check(FakeJobEvent::removed.load() > removed_before_retire,
          "natural completion must detach a still-bound lane");
    check(collector_status(scheduler, "job_event").at("job_count").get<size_t>() == 0,
          "the retired instance must leave the lane");

    scheduler.shutdown();
    WriterManager::instance().shutdown();
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: collector_execution_lifecycle_test TEMP_BASE\n";
        return 2;
    }
    try {
        run(argv[1]);
        std::cout << "PASS collector lifecycle: event dispatch, System envelope, "
                     "closing acknowledgement and scoped status\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
