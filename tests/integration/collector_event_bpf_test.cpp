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
/* Collector-owned eBPF source exercised through the real scheduler.
 * Requires BPF privileges; run via run_event_bpf_test.sh.
 *
 * Covers the Stream-mode parts of the source acceptance cases:
 *   A5  eBPF init (private probe/reader, ring epoll fd registration), bounded
 *       worker-side consumption, Job versus System envelopes
 *   A6  libbpf compatibility: ring_buffer__consume_n when available, plus the
 *       tested sample-callback budget adapter for older toolchains
 *   A7  records are copied out of the sample callback before processing
 *   A8  mixed ring + socket readiness in one instance, exact kernel-loss
 *       telemetry from the private stats map, distinct userspace overflow
 *
 * Final/Both source runs are completed together with the finalization task. */
#include <linux/types.h>

#include "collector_event_fixture.h"

#include "common/config.hpp"
#include "common/local_rpc.hpp"
#include "core/collector_execution_context.hpp"
#include "core/collector_registry.hpp"
#include "core/collector_scheduler.hpp"
#include "core/job_registry.hpp"
#include "core/writer_manager.hpp"
#include "ebpf/ring_buffer_probe.hpp"
#include "writer/base_writer.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

using namespace std::chrono_literals;

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

template <typename Predicate>
void eventually(Predicate predicate, const std::string& message,
                std::chrono::milliseconds timeout = 10s)
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
        const bool ok = store().cv.wait_for(lock, 10s, [&] {
            size_t matched = 0;
            for (const auto& record : store().records)
                if (record->sample->collector_name == collector) ++matched;
            return matched >= count;
        });
        check(ok, "collector " + collector + " did not deliver " + std::to_string(count) +
                      " record(s)");
        std::vector<OutputRecordPtr> result;
        for (const auto& record : store().records)
            if (record->sample->collector_name == collector) result.push_back(record);
        return result;
    }
    static std::string text_of(const OutputRecordPtr& record)
    {
        const auto* text = std::get_if<Text>(&record->payload);
        check(text != nullptr, "captured record is not Text");
        return text->value;
    }
};

size_t captured_count(const std::string& collector)
{
    std::lock_guard lock(CaptureWriter::store().mutex);
    size_t matched = 0;
    for (const auto& record : CaptureWriter::store().records)
        if (record->sample->collector_name == collector) ++matched;
    return matched;
}

size_t config_size(const nlohmann::json& config, const char* key, size_t fallback)
{
    const auto it = config.find(key);
    if (it == config.end()) return fallback;
    if (it->is_number()) return it->get<size_t>();
    if (it->is_string()) {
        try {
            return std::stoul(it->get<std::string>());
        } catch (const std::exception&) {
            return fallback;
        }
    }
    return fallback;
}

// ---------------------------------------------------------------------------
// Shared BPF source lifecycle: a private object/ring per instance, a budgeted
// sample callback, and a bounded drain that uses ring_buffer__consume_n when
// the linked libbpf provides it or the callback-budget adapter otherwise.
// ---------------------------------------------------------------------------

struct BpfEventSource {
    /* Queue bound for samples copied out of the callback before processing. */
    static constexpr size_t kMaxQueued = 4096;

    std::atomic<uint64_t> drained_total{0};
    std::atomic<uint64_t> userspace_overflow{0};
    std::atomic<uint64_t> kernel_dropped{0};
    std::atomic<bool> loss_telemetry{false};
    std::atomic<bool> budget_exhausted{false};

    bool open(const nlohmann::json& config, SourceRegistrar& registrar)
    {
        const auto object_path = config.at("bpf_object").get<std::string>();
        turn_limit_ = config_size(config, "turn_limit", CEV_DEFAULT_TURN_LIMIT);
        fallback_consume_ = config.value("consume_api", "auto") == "fallback";

        object_.reset(bpf_object__open(object_path.c_str()));
        if (!object_) return false;
        const auto ring_entries = config_size(config, "ring_entries", 0);
        if (ring_entries > 0) {
            bpf_map* ring = bpf_object__find_map_by_name(object_.get(), CEV_RING_MAP);
            if (!ring || bpf_map__set_max_entries(ring, ring_entries) != 0) return false;
        }
        if (bpf_object__load(object_.get()) != 0) return false;

        bpf_map* events = bpf_object__find_map_by_name(object_.get(), CEV_RING_MAP);
        bpf_map* stats = bpf_object__find_map_by_name(object_.get(), CEV_STATS_MAP);
        if (!events || !stats) return false;
        events_fd_ = bpf_map__fd(events);
        stats_fd_ = bpf_map__fd(stats);

        bpf_program* program = bpf_object__find_program_by_name(object_.get(), "cev_on_getpid");
        if (!program) return false;
        link_.reset(bpf_program__attach(program));
        if (!link_) return false;

        ring_ = ring_buffer__new(events_fd_, &BpfEventSource::on_sample, this, nullptr);
        if (!ring_) return false;
        auto registration = registrar.watch_probe(make_ring_buffer_probe(ring_, "cev_events"));
        if (!registration) return false;
        registration_ = std::move(*registration);
        handle_ = registration_->handle();
        registrar_ = &registrar;
        return true;
    }

    void close()
    {
        registration_.reset(); // unregister the epoll fd before freeing the ring
        if (ring_) {
            ring_buffer__free(ring_);
            ring_ = nullptr;
        }
        link_.reset();   // detaches the private tracepoint attachment
        object_.reset(); // unloads the private maps
        events_fd_ = stats_fd_ = -1;
        registrar_ = nullptr;
    }

    /* One bounded worker-side drain turn. Returns the number of records the
       sample callback delivered and whether the turn bound stopped the drain. */
    struct DrainResult {
        size_t consumed = 0;
        bool more = false;
        int error = 0;
    };

    DrainResult drain_bounded(size_t limit)
    {
        // Arm the callback budget: with consume_n the API already bounds the
        // callbacks; with the fallback adapter the callback aborts the
        // unbounded consume once the budget is reached. Records behind the
        // abort keep their cursor and are delivered by the next turn.
        turn_remaining_.store(limit);
        budget_exhausted.store(false);
        const size_t before = drained_total.load();
        if (fallback_consume_) {
            const int rc = ring_buffer__consume(ring_);
            const auto consumed = static_cast<size_t>(drained_total.load() - before);
            if (budget_exhausted.load()) return {consumed, true, 0};
            if (rc < 0) return {consumed, false, rc};
            return {consumed, false, 0};
        }
        const auto result = bounded_ring_buffer_consume(ring_, limit);
        return {static_cast<size_t>(drained_total.load() - before), result.more, result.error};
    }

    void read_kernel_stats()
    {
        __u32 key = 0;
        cev_stats stats{};
        if (bpf_map_lookup_elem(stats_fd_, &key, &stats) == 0) {
            kernel_dropped = stats.dropped;
            loss_telemetry = true; // this probe exposes exact loss counts
        }
    }

    static int on_sample(void* context, void* data, size_t size)
    {
        auto* self = static_cast<BpfEventSource*>(context);
        // A7: copy the record; nothing beyond this callback touches ring data.
        cev_sample sample{};
        std::memcpy(&sample, data, std::min(size, sizeof(sample)));
        // Budget adapter: abort the drain once this turn's bound is reached.
        if (self->turn_remaining_.load() == 0) {
            self->budget_exhausted = true;
            return -ENOBUFS;
        }
        self->turn_remaining_.fetch_sub(1);
        ++self->drained_total;
        std::lock_guard lock(self->queue_mutex_);
        if (self->queue_.size() >= kMaxQueued) {
            ++self->userspace_overflow;
            return 0;
        }
        self->queue_.push_back(sample);
        return 0;
    }

    std::deque<cev_sample> take_queued()
    {
        std::lock_guard lock(queue_mutex_);
        return std::exchange(queue_, {});
    }

    size_t turn_limit() const { return turn_limit_; }
    SourceHandle handle() const { return handle_; }
    SourceRegistrar* registrar() const { return registrar_; }

private:
    size_t turn_limit_ = CEV_DEFAULT_TURN_LIMIT;
    bool fallback_consume_ = false;
    int events_fd_ = -1;
    int stats_fd_ = -1;
    struct ring_buffer* ring_ = nullptr;
    std::unique_ptr<bpf_link, decltype(&bpf_link__destroy)> link_{nullptr, &bpf_link__destroy};
    std::unique_ptr<bpf_object, decltype(&bpf_object__close)> object_{nullptr, &bpf_object__close};
    SourceRegistrar* registrar_ = nullptr;
    std::optional<SourceRegistration> registration_;
    SourceHandle handle_{};
    std::mutex queue_mutex_;
    std::deque<cev_sample> queue_;
    std::atomic<size_t> turn_remaining_{0};
};

// ---------------------------------------------------------------------------
// Job scope: real probe events attributed to the attached Job when the sample
// comes from the monitored thread group; other producers are discarded with a
// counter and never acquire a fallback envelope.
//
// The test needs per-class observations although the lifecycle hooks are
// inherited once, so subclasses mirror the values through observe_turn().
// ---------------------------------------------------------------------------

struct BpfJobEvent : IEventJobCollector {
    static inline std::atomic<bool> init_done{false};
    static inline std::atomic<int> attached{0};
    static inline std::atomic<uint64_t> unattributed{0};
    static inline std::atomic<int> drain_turns{0};
    static inline std::atomic<uint64_t> mirror_dropped{0};
    static inline std::atomic<uint64_t> mirror_overflow{0};
    static inline std::atomic<bool> mirror_loss{false};

    BpfEventSource source;

    bool init(const nlohmann::json& config, CollectorInitContext& context) override
    {
        monitor_tgid_ = config_size(config, "monitor_tgid", 0);
        if (!source.open(config, context.sources())) return false;
        init_done = true;
        return true;
    }

    void deinit() noexcept override
    {
        source.close();
        init_done = false;
    }

    void on_job_added(const Job& job) override
    {
        job_id_ = job.JobID;
        ++attached;
    }
    void on_job_removed(const Job&) override { --attached; }

    DrainOutcome on_ready(const ReadySources&, const WorkBudget& budget,
                          JobEventContext& context) override
    {
        const size_t limit = budget.max_events > 0
                                 ? std::min(budget.max_events, source.turn_limit())
                                 : source.turn_limit();
        const auto drained = source.drain_bounded(limit);
        source.read_kernel_stats();
        observe_turn();
        size_t emitted = 0;
        for (const auto& sample : source.take_queued()) {
            if (sample.tgid != monitor_tgid_ || attached.load() == 0) {
                ++unattributed; // explicit discard policy for unknown producers
                continue;
            }
            auto session = context.open_session(job_id_, 1);
            if (!session) {
                ++unattributed;
                continue;
            }
            (void)session->context().emit(
                "text", Text{"bpf-job seq=" + std::to_string(sample.seq) +
                                 " pid=" + std::to_string(sample.pid) + "\n"});
            ++emitted;
        }
        if (source.registrar()) source.registrar()->rearm(source.handle());
        return {CollectOutcome{}, emitted, drained.more};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

protected:
    /* Mirrors per-instance source observations into the class the test can
       read; subclasses redeclare the statics and override this so registered
       types report independently. */
    virtual void observe_turn()
    {
        ++drain_turns;
        mirror_dropped = source.kernel_dropped.load();
        mirror_overflow = source.userspace_overflow.load();
        mirror_loss = source.loss_telemetry.load();
    }

    uint64_t monitor_tgid_ = 0;
    uint64_t job_id_ = 0;
};

/* Same collector, but forces the older-libbpf bounded adapter (A6) via its
 * consume_api configuration. Statics are redeclared and the observer is
 * overridden so the two registered types report independently. */
struct BpfJobEventFallback final : BpfJobEvent {
    static inline std::atomic<int> drain_turns{0};
    static inline std::atomic<uint64_t> mirror_dropped{0};
    static inline std::atomic<uint64_t> mirror_overflow{0};
    static inline std::atomic<bool> mirror_loss{false};

    void observe_turn() override
    {
        ++drain_turns;
        mirror_dropped = source.kernel_dropped.load();
        mirror_overflow = source.userspace_overflow.load();
        mirror_loss = source.loss_telemetry.load();
    }
};

// ---------------------------------------------------------------------------
// System scope: every probe event uses the Job 0 envelope.
// ---------------------------------------------------------------------------

struct BpfSystemEvent : IEventSystemCollector {
    static inline std::atomic<bool> init_done{false};
    static inline std::atomic<int> drain_turns{0};

    BpfEventSource source;

    bool init(const nlohmann::json& config, CollectorInitContext& context) override
    {
        if (!source.open(config, context.sources())) return false;
        init_done = true;
        return true;
    }

    void deinit() noexcept override
    {
        source.close();
        init_done = false;
    }

    DrainOutcome on_ready(const ReadySources&, const WorkBudget& budget,
                          SystemEventContext& context) override
    {
        const size_t limit = budget.max_events > 0
                                 ? std::min(budget.max_events, source.turn_limit())
                                 : source.turn_limit();
        ++drain_turns;
        const auto drained = source.drain_bounded(limit);
        source.read_kernel_stats();
        size_t emitted = 0;
        for (const auto& sample : source.take_queued()) {
            auto session = context.open_session(); // System envelope: Job 0
            (void)session.context().emit(
                "text", Text{"bpf-sys tgid=" + std::to_string(sample.tgid) + "\n"});
            ++emitted;
        }
        if (source.registrar()) source.registrar()->rearm(source.handle());
        return {CollectOutcome{}, emitted, drained.more};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }
};

// ---------------------------------------------------------------------------
// A8: one instance, two source kinds (private ring + owned datagram socket),
// one lane. A one-page ring with a one-event turn bound makes kernel reserve
// failures certain while datagrams keep arriving on the same lane.
// ---------------------------------------------------------------------------

struct BpfMixedSystemEvent : IEventSystemCollector {
    static inline std::atomic<bool> init_done{false};
    static inline std::atomic<uint64_t> dgram_messages{0};
    static inline std::atomic<uint64_t> mirror_dropped{0};
    static inline std::atomic<uint64_t> mirror_overflow{0};
    static inline std::atomic<bool> mirror_loss{false};

    BpfEventSource source;
    int fd_ = -1;
    std::string path_;
    std::optional<SourceRegistration> dgram_registration_;
    SourceHandle dgram_handle_{};

    bool init(const nlohmann::json& config, CollectorInitContext& context) override
    {
        path_ = config.at("socket_path").get<std::string>();
        fd_ = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
        if (fd_ < 0) return false;
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::strncpy(address.sun_path, path_.c_str(), sizeof(address.sun_path) - 1);
        if (bind(fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
            close(fd_);
            fd_ = -1;
            return false;
        }
        auto dgram = context.sources().watch_fd(fd_, Interest::Readable);
        if (!dgram) {
            close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
            return false;
        }
        dgram_registration_ = std::move(*dgram);
        dgram_handle_ = dgram_registration_->handle();
        if (!source.open(config, context.sources())) {
            dgram_registration_.reset();
            close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
            return false;
        }
        init_done = true;
        return true;
    }

    void deinit() noexcept override
    {
        source.close();
        dgram_registration_.reset();
        if (fd_ >= 0) close(fd_);
        fd_ = -1;
        if (!path_.empty()) ::unlink(path_.c_str());
        path_.clear();
        init_done = false;
    }

    DrainOutcome on_ready(const ReadySources& sources, const WorkBudget& budget,
                          SystemEventContext& context) override
    {
        const size_t limit = budget.max_events > 0
                                 ? std::min(budget.max_events, source.turn_limit())
                                 : source.turn_limit();
        size_t emitted = 0;
        bool more = false;
        for (const auto& ready : sources) {
            if (ready.source == source.handle()) {
                const auto drained = source.drain_bounded(limit);
                source.read_kernel_stats();
                mirror_dropped = source.kernel_dropped.load();
                mirror_overflow = source.userspace_overflow.load();
                mirror_loss = source.loss_telemetry.load();
                more = more || drained.more;
                for (const auto& sample : source.take_queued()) {
                    auto session = context.open_session();
                    (void)session.context().emit(
                        "text", Text{"bpf-mix seq=" + std::to_string(sample.seq) + "\n"});
                    ++emitted;
                }
                if (source.registrar()) source.registrar()->rearm(source.handle());
            } else if (ready.source == dgram_handle_) {
                emitted += drain_datagrams(context);
                if (source.registrar()) source.registrar()->rearm(dgram_handle_);
            }
        }
        return {CollectOutcome{}, emitted, more};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

private:
    size_t drain_datagrams(SystemEventContext& context)
    {
        size_t count = 0;
        unsigned char buffer[CEV_DGRAM_MAX];
        while (true) {
            const ssize_t size = recv(fd_, buffer, sizeof(buffer), 0);
            if (size < 0) break; // EAGAIN: no pending datagram
            if (size < static_cast<ssize_t>(offsetof(cev_datagram, payload))) continue;
            cev_datagram message{};
            std::memcpy(&message, buffer, offsetof(cev_datagram, payload));
            if (message.magic != CEV_DGRAM_MAGIC) continue;
            auto session = context.open_session();
            (void)session.context().emit("text",
                                         Text{"bpf-mix dgram=" + std::to_string(message.tag) +
                                              "\n"});
            ++dgram_messages;
            ++count;
        }
        return count;
    }
};

AUTO_REGISTER_JOB_EVENT_COLLECTOR(BpfJobEvent, "reference Job BPF source");
AUTO_REGISTER_JOB_EVENT_COLLECTOR(BpfJobEventFallback, "reference Job BPF source (fallback drain)");
AUTO_REGISTER_SYSTEM_EVENT_COLLECTOR(BpfSystemEvent, "reference System BPF source");
AUTO_REGISTER_SYSTEM_EVENT_COLLECTOR(BpfMixedSystemEvent, "reference mixed BPF + socket source");

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

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

void trigger_getpid(int count)
{
    for (int index = 0; index < count; ++index) (void)::getpid();
}

void run(const std::string& base, const std::string& probe_object)
{
    const std::string socket_path = base + ".rpc.sock";
    const std::string mixed_dgram = base + ".mix.dgram";

    {
        std::ofstream config(base + ".yaml");
        config << "lens_config:\n  max_collector_threads: 2\n"
                  "collectors_config:\n"
                  "  enable_collector_perf: false\n"
                  "  default_freq: 20\n"
                  "  default_use_writers: []\n"
                  "  collectors:\n"
                  "    - {name: bpf_job, type: BpfJobEvent, config: bpf_job_cfg}\n"
                  "    - {name: bpf_job_fb, type: BpfJobEventFallback, config: bpf_job_fb_cfg}\n"
                  "    - {name: bpf_sys, type: BpfSystemEvent, config: bpf_sys_cfg}\n"
                  "    - {name: bpf_mix, type: BpfMixedSystemEvent, config: bpf_mix_cfg}\n"
                  "writers_config:\n"
                  "  buffer_capacity: 256\n"
                  "  max_pending_records: 8192\n"
                  "  max_pending_bytes: 67108864\n"
                  "  max_record_bytes: 8388608\n"
                  "  writers:\n"
                  "    - {name: w, type: CaptureWriter, config: cap}\n"
                  "cap: {}\n"
                  "bpf_job_cfg:\n  bpf_object: " << probe_object << "\n"
                  "  monitor_tgid: " << getpid() << "\n"
                  "  turn_limit: 7\n"
                  "  outputs: {text: [w]}\n"
                  "bpf_job_fb_cfg:\n  bpf_object: " << probe_object << "\n"
                  "  monitor_tgid: " << getpid() << "\n"
                  "  turn_limit: 5\n"
                  "  consume_api: fallback\n"
                  "  outputs: {text: [w]}\n"
                  "bpf_sys_cfg:\n  bpf_object: " << probe_object << "\n"
                  "  auto_start: true\n"
                  "  turn_limit: 512\n"
                  "  outputs: {text: [w]}\n"
                  "bpf_mix_cfg:\n  bpf_object: " << probe_object << "\n"
                  "  socket_path: " << mixed_dgram << "\n"
                  "  auto_start: true\n"
                  "  ring_entries: 4096\n"
                  "  turn_limit: 1\n"
                  "  outputs: {text: [w]}\n"
                  "job_registry_config:\n  job_db_path: " << base << ".db\n";
    }

    auto& server = RPCServer::instance(socket_path);
    (void)server;
    Config::instance(base + ".yaml");
    WriterManager::instance().registerWriter<CaptureWriter>("CaptureWriter");

    auto& scheduler = CollectorScheduler::instance();
    scheduler.start();

    // ------------------------------------------------------------------
    // System scope with zero Jobs: probe events use the Job 0 envelope.
    // ------------------------------------------------------------------
    eventually(
        [] { return BpfSystemEvent::init_done.load() && BpfMixedSystemEvent::init_done.load(); },
        "BPF sources were not initialized");
    trigger_getpid(10);
    eventually([] { return captured_count("bpf_sys") >= 10; },
               "System BPF events were not delivered");
    for (const auto& record : CaptureWriter::wait_for("bpf_sys", 10))
        check(record->sample->job.JobID == 0, "System BPF event lost the Job 0 envelope");
    check(CaptureWriter::text_of(CaptureWriter::wait_for("bpf_sys", 1)[0])
                  .rfind("bpf-sys tgid=", 0) == 0,
          "System BPF payload malformed");

    // ------------------------------------------------------------------
    // Job scope: bounded consumption (turn_limit 7 over 100 events) with
    // correct real-Job attribution; other producers are discarded.
    // ------------------------------------------------------------------
    check(JobRegistry::instance().addJob(make_job(401, {"bpf_job"})), "add Job 401");
    eventually([] { return BpfJobEvent::init_done.load(); }, "Job BPF source was not initialized");
    eventually([] { return BpfJobEvent::attached.load() >= 1; }, "Job binding was not observed");
    trigger_getpid(100);
    eventually([] { return captured_count("bpf_job") >= 100; },
               "Job BPF events were not fully delivered");
    check(BpfJobEvent::drain_turns.load() >= 15,
          "100 events over a turn bound of 7 did not require bounded continuation turns");
    const auto test_tgid = std::to_string(getpid());
    for (const auto& record : CaptureWriter::wait_for("bpf_job", 100)) {
        check(record->sample->job.JobID == 401, "Job BPF attribution envelope");
        const auto text = CaptureWriter::text_of(record);
        check(text.rfind("bpf-job seq=", 0) == 0 &&
                  text.find("pid=" + test_tgid) != std::string::npos,
              "Job BPF payload malformed: " + text);
    }

    // ------------------------------------------------------------------
    // A6: the tested bounded adapter for libbpf without consume_n: an
    // unbounded consume aborted from the sample callback at the budget.
    // ------------------------------------------------------------------
    check(JobRegistry::instance().addJob(make_job(402, {"bpf_job_fb"})), "add Job 402");
    trigger_getpid(20);
    eventually([] { return captured_count("bpf_job_fb") >= 20; },
               "fallback-adapter events were not delivered");
    check(BpfJobEventFallback::drain_turns.load() >= 4,
          "fallback adapter did not bound its drain turns");
    check(BpfJobEventFallback::mirror_overflow.load() == 0,
          "fallback adapter lost records internally");

    // ------------------------------------------------------------------
    // A8: mixed readiness and exact kernel-loss telemetry. The one-page ring
    // plus a one-event turn bound makes reserve failures certain while
    // datagrams keep arriving on the second source of the same lane.
    // ------------------------------------------------------------------
    trigger_getpid(5000);
    {
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::strncpy(address.sun_path, mixed_dgram.c_str(), sizeof(address.sun_path) - 1);
        const int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
        check(fd >= 0, "mixed datagram send socket");
        for (__u64 tag = 1; tag <= 3; ++tag) {
            cev_datagram message{};
            message.magic = CEV_DGRAM_MAGIC;
            message.seq = static_cast<__u32>(tag);
            message.tag = tag;
            message.payload_len = 4;
            std::memcpy(message.payload, "mix", 4);
            const ssize_t sent =
                sendto(fd, &message, offsetof(cev_datagram, payload) + message.payload_len,
                       MSG_NOSIGNAL, reinterpret_cast<const sockaddr*>(&address),
                       sizeof(address));
            check(sent >= 0, "mixed datagram sendto");
        }
        close(fd);
    }
    eventually([] { return BpfMixedSystemEvent::dgram_messages.load() >= 3; },
               "mixed-lane datagrams were not delivered");
    eventually([] {
                 return BpfMixedSystemEvent::mirror_loss.load() &&
                        BpfMixedSystemEvent::mirror_dropped.load() > 0;
             },
             "kernel loss telemetry did not report exact drops");
    check(BpfMixedSystemEvent::mirror_overflow.load() == 0,
          "userspace queue overflow was confused with kernel loss");
    eventually([] { return captured_count("bpf_mix") >= 5; },
               "mixed-lane ring events were not delivered");

    scheduler.shutdown();
    WriterManager::instance().shutdown();
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 3) {
        std::cerr << "Usage: collector_event_bpf_test PROBE_OBJECT TEMP_BASE\n";
        return 2;
    }
    try {
        run(argv[2], argv[1]);
        std::cout << "PASS collector event bpf sources: private probe lifecycle, bounded "
                     "consumption (consume_n and fallback adapter), scope envelopes, mixed "
                     "readiness, exact kernel-loss telemetry\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
