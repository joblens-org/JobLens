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
/* Collector-owned Unix socket sources exercised through the real scheduler.
 *
 * Covers the Stream-mode parts of the source acceptance cases:
 *   A1  Unix datagrams under Job/System bindings, real envelopes
 *   A2  oversize / malformed / end-marker datagrams; silence never finishes
 *   A3  connection-oriented fixture: nonblocking accept, dynamic client
 *       registration, partial frames, disconnect with tail data, connection
 *       limit, slow-client isolation
 *   A4  path ownership: live endpoint fails init; only the owner unlinks
 *   I2  input that arrives before activation is handled without a second edge
 *   E4  one-shot continuation with more input than one turn permits
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
#include "writer/base_writer.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
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

// ---------------------------------------------------------------------------
// Shared datagram-drain helper used by both scoped reference collectors.
// ---------------------------------------------------------------------------

struct DgramCounters {
    std::atomic<uint64_t> consumed{0};
    std::atomic<uint64_t> oversize{0};
    std::atomic<uint64_t> malformed{0};
    std::atomic<uint64_t> end_markers{0};
    std::atomic<uint64_t> unattributed{0};
};

struct DgramMessage {
    uint64_t tag = 0;
    std::string payload;
    bool invalid = false;
};

struct DgramReadResult {
    size_t consumed = 0;
    bool more = false;
};

using DgramEmit = std::function<void(DgramMessage&&)>;

/* Bounded, nonblocking, message-safe drain. One datagram is one message, so
 * partial content is never interpreted as a trigger. more=true only when the
 * turn bound stopped the loop while datagrams may remain. */
DgramReadResult drain_dgram_socket(int fd, size_t turn_limit, DgramCounters& counters,
                                   const DgramEmit& emit)
{
    DgramReadResult result;
    std::vector<unsigned char> buffer(CEV_DGRAM_MAX);
    while (true) {
        iovec iov{buffer.data(), buffer.size()};
        msghdr header{};
        header.msg_iov = &iov;
        header.msg_iovlen = 1;
        const ssize_t size = recvmsg(fd, &header, 0);
        if (size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            break;
        }
        if (size == 0) break;
        if ((header.msg_flags & MSG_TRUNC) != 0) {
            ++counters.oversize;
            continue;
        }
        if (static_cast<size_t>(size) < offsetof(cev_datagram, payload)) {
            ++counters.malformed;
            DgramMessage invalid;
            invalid.invalid = true;
            emit(std::move(invalid));
            continue;
        }
        cev_datagram message{};
        std::memcpy(&message, buffer.data(), offsetof(cev_datagram, payload));
        if (message.magic != CEV_DGRAM_MAGIC) {
            ++counters.malformed;
            DgramMessage invalid;
            invalid.invalid = true;
            emit(std::move(invalid));
            continue;
        }
        if ((message.flags & CEV_FLAG_END) != 0) {
            // Producer-declared boundary: recorded, never treated as data and
            // never used to finish anything in Stream mode.
            ++counters.end_markers;
            continue;
        }
        const auto* payload_bytes = buffer.data() + offsetof(cev_datagram, payload);
        const size_t available = static_cast<size_t>(size) - offsetof(cev_datagram, payload);
        const uint32_t length =
            std::min<uint32_t>(message.payload_len, static_cast<uint32_t>(available));
        DgramMessage data;
        data.tag = message.tag;
        data.payload.assign(reinterpret_cast<const char*>(payload_bytes), length);
        ++counters.consumed;
        ++result.consumed;
        emit(std::move(data));
        if (result.consumed >= turn_limit) {
            result.more = true;
            break;
        }
    }
    return result;
}

int bind_dgram_endpoint(const std::string& path)
{
    check(path.size() < sizeof(sockaddr_un::sun_path) - 1, "socket path too long");
    const int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    check(fd >= 0, "socket(AF_UNIX, SOCK_DGRAM)");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1);
    if (bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        close(fd);
        return -1; // an existing live endpoint is an explicit init failure
    }
    return fd;
}

/* Collector init receives Utils::yamlToJson output: every YAML scalar arrives
 * as a JSON string, so numeric options need a tolerant reader. */
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
// Job-scope reference collector: init-owned datagram endpoint, bounded reads,
// per-message Job sessions, owned-path cleanup.
// ---------------------------------------------------------------------------

struct DgramJobEvent : IEventJobCollector {
    static inline std::atomic<bool> init_done{false};
    static inline std::atomic<int> attached{0};
    static inline DgramCounters counters{};
    static inline std::atomic<int> ready_calls{0};

    bool init(const nlohmann::json& config, CollectorInitContext& context) override
    {
        path_ = config.at("socket_path").get<std::string>();
        turn_limit_ = config_size(config, "turn_limit", CEV_DEFAULT_TURN_LIMIT);
        fd_ = bind_dgram_endpoint(path_);
        if (fd_ < 0) return false;
        registrar_ = &context.sources();
        auto registration = context.sources().watch_fd(fd_, Interest::Readable);
        if (!registration) {
            close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
            path_.clear();
            return false;
        }
        registration_ = std::move(*registration);
        handle_ = registration_->handle();
        init_done = true;
        return true;
    }

    void deinit() noexcept override
    {
        registration_.reset(); // unregister before closing the owned descriptor
        if (fd_ >= 0) close(fd_);
        fd_ = -1;
        if (!path_.empty()) ::unlink(path_.c_str()); // only the owned endpoint
        path_.clear();
        init_done = false;
    }

    void on_job_added(const Job& job) override
    {
        attached_.insert(job.JobID);
        ++attached;
    }
    void on_job_removed(const Job& job) override
    {
        attached_.erase(job.JobID);
        --attached;
    }

    DrainOutcome on_ready(const ReadySources&, const WorkBudget& budget,
                          JobEventContext& context) override
    {
        ++ready_calls;
        const size_t limit =
            budget.max_events > 0 ? std::min(budget.max_events, turn_limit_) : turn_limit_;
        const auto emit = [this, &context](DgramMessage&& message) {
            if (message.invalid) {
                context.record_error("malformed datagram rejected");
                return;
            }
            auto session = context.open_session(message.tag, 1);
            if (!session) {
                ++counters.unattributed;
                context.record_error("unknown job tag " + std::to_string(message.tag));
                return;
            }
            (void)session->context().emit("text", Text{message.payload + "\n"});
        };
        const auto result = drain_dgram_socket(fd_, limit, counters, emit);
        if (registrar_) registrar_->rearm(handle_);
        return {CollectOutcome{}, result.consumed, result.more};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

private:
    std::string path_;
    size_t turn_limit_ = CEV_DEFAULT_TURN_LIMIT;
    int fd_ = -1;
    std::set<uint64_t> attached_;
    SourceRegistrar* registrar_ = nullptr;
    std::optional<SourceRegistration> registration_;
    SourceHandle handle_{};
};

// A second instance configured with the same endpoint path: init must fail
// explicitly without removing the live endpoint owned by the first instance.
struct DgramJobEventConflict : IEventJobCollector {
    static inline std::atomic<int> init_failures{0};

    bool init(const nlohmann::json& config, CollectorInitContext& context) override
    {
        const auto path = config.at("socket_path").get<std::string>();
        const int fd = bind_dgram_endpoint(path);
        if (fd < 0) {
            ++init_failures;
            return false;
        }
        auto registration = context.sources().watch_fd(fd, Interest::Readable);
        if (!registration) {
            close(fd);
            ::unlink(path.c_str());
            return false;
        }
        registration_ = std::move(*registration);
        fd_ = fd;
        path_ = path;
        return true;
    }

    void deinit() noexcept override
    {
        registration_.reset();
        if (fd_ >= 0) close(fd_);
        if (!path_.empty()) ::unlink(path_.c_str());
        path_.clear();
    }

    DrainOutcome on_ready(const ReadySources&, const WorkBudget&, JobEventContext&) override
    {
        return {};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

private:
    std::string path_;
    int fd_ = -1;
    std::optional<SourceRegistration> registration_;
};

// ---------------------------------------------------------------------------
// System-scope reference collector on a datagram source; small turn bound so
// the one-shot continuation (E4) is exercised on a real socket.
// ---------------------------------------------------------------------------

struct DgramSystemEvent : IEventSystemCollector {
    static inline std::atomic<bool> init_done{false};
    static inline DgramCounters counters{};
    static inline std::atomic<int> ready_calls{0};

    bool init(const nlohmann::json& config, CollectorInitContext& context) override
    {
        path_ = config.at("socket_path").get<std::string>();
        turn_limit_ = config_size(config, "turn_limit", CEV_DEFAULT_TURN_LIMIT);
        fd_ = bind_dgram_endpoint(path_);
        if (fd_ < 0) return false;
        registrar_ = &context.sources();
        auto registration = context.sources().watch_fd(fd_, Interest::Readable);
        if (!registration) {
            close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
            path_.clear();
            return false;
        }
        registration_ = std::move(*registration);
        handle_ = registration_->handle();
        init_done = true;
        return true;
    }

    void deinit() noexcept override
    {
        registration_.reset();
        if (fd_ >= 0) close(fd_);
        if (!path_.empty()) ::unlink(path_.c_str());
        path_.clear();
        init_done = false;
    }

    DrainOutcome on_ready(const ReadySources&, const WorkBudget& budget,
                          SystemEventContext& context) override
    {
        ++ready_calls;
        const size_t limit =
            budget.max_events > 0 ? std::min(budget.max_events, turn_limit_) : turn_limit_;
        const auto emit = [this, &context](DgramMessage&& message) {
            if (message.invalid) {
                context.record_error("malformed datagram rejected");
                return;
            }
            auto session = context.open_session(); // System envelope: Job 0
            (void)session.context().emit("text",
                                         Text{"sys:" + std::to_string(message.tag) + ":" +
                                              message.payload + "\n"});
        };
        const auto result = drain_dgram_socket(fd_, limit, counters, emit);
        if (registrar_) registrar_->rearm(handle_);
        return {CollectOutcome{}, result.consumed, result.more};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

private:
    std::string path_;
    size_t turn_limit_ = CEV_DEFAULT_TURN_LIMIT;
    int fd_ = -1;
    SourceRegistrar* registrar_ = nullptr;
    std::optional<SourceRegistration> registration_;
    SourceHandle handle_{};
};

// ---------------------------------------------------------------------------
// Connection-oriented fixture (A3): nonblocking accept, dynamic client
// registration through the collector-bound registrar, per-connection framing
// state, disconnect with tail data, connection limit, slow-client isolation.
// Frames are 4-byte big-endian length (1..256) plus body bytes.
// ---------------------------------------------------------------------------

struct StreamSystemEvent : IEventSystemCollector {
    static inline std::atomic<bool> init_done{false};
    static inline std::atomic<uint64_t> refused_connections{0};
    static inline std::atomic<int> connections_active{0};

    bool init(const nlohmann::json& config, CollectorInitContext& context) override
    {
        path_ = config.at("socket_path").get<std::string>();
        max_connections_ = config_size(config, "max_connections", 8);
        check(path_.size() < sizeof(sockaddr_un::sun_path) - 1, "socket path too long");
        listener_ = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
        check(listener_ >= 0, "socket(AF_UNIX, SOCK_STREAM)");
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::strncpy(address.sun_path, path_.c_str(), sizeof(address.sun_path) - 1);
        if (bind(listener_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
            close(listener_);
            listener_ = -1;
            return false;
        }
        if (listen(listener_, 8) != 0) {
            close(listener_);
            listener_ = -1;
            ::unlink(path_.c_str());
            return false;
        }
        registrar_ = &context.sources();
        auto registration = context.sources().watch_fd(listener_, Interest::Readable);
        if (!registration) {
            close(listener_);
            listener_ = -1;
            ::unlink(path_.c_str());
            return false;
        }
        listener_registration_ = std::move(*registration);
        listener_handle_ = listener_registration_->handle();
        init_done = true;
        return true;
    }

    void deinit() noexcept override
    {
        for (auto& [id, connection] : connections_) {
            (void)id;
            connection.registration.reset(); // unregister before close
            close(connection.fd);
        }
        connections_.clear();
        listener_registration_.reset();
        if (listener_ >= 0) close(listener_);
        listener_ = -1;
        if (!path_.empty()) ::unlink(path_.c_str());
        path_.clear();
        init_done = false;
    }

    DrainOutcome on_ready(const ReadySources& sources, const WorkBudget&,
                          SystemEventContext& context) override
    {
        for (const auto& ready : sources) {
            if (ready.source == listener_handle_) {
                accept_clients();
                if (registrar_) registrar_->rearm(listener_handle_);
                continue;
            }
            auto it = connections_.find(ready.source.id);
            if (it == connections_.end()) continue; // stale readiness
            handle_client(it->second, context);
        }
        return {CollectOutcome{}, 0, false};
    }

    std::vector<OutputChannel> output_channels() const override
    {
        return {{"text", OutputKind::Text}};
    }

private:
    struct Connection {
        int fd = -1;
        std::optional<SourceRegistration> registration;
        SourceHandle handle{};
        std::vector<unsigned char> buffer;
    };

    void accept_clients()
    {
        for (int accepted = 0; accepted < 4; ++accepted) {
            const int fd = accept4(listener_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) break; // EAGAIN: no pending client
            if (connections_.size() >= max_connections_) {
                ++refused_connections;
                close(fd);
                continue;
            }
            auto registration = registrar_->watch_fd(fd, Interest::Readable);
            if (!registration) {
                ++refused_connections;
                close(fd);
                continue;
            }
            Connection connection;
            connection.fd = fd;
            connection.handle = registration->handle();
            connection.registration = std::move(*registration);
            const uint64_t id = connection.handle.id;
            connections_.emplace(id, std::move(connection));
            ++connections_active;
        }
    }

    void handle_client(Connection& connection, SystemEventContext& context)
    {
        unsigned char chunk[512];
        bool hangup = false;
        while (true) {
            const ssize_t size = recv(connection.fd, chunk, sizeof(chunk), 0);
            if (size < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                hangup = true;
                break;
            }
            if (size == 0) { // peer finished writing
                hangup = true;
                break;
            }
            connection.buffer.insert(connection.buffer.end(), chunk, chunk + size);
            if (connection.buffer.size() > 4096) { // protocol bound
                context.record_error("connection buffer bound exceeded");
                hangup = true;
                break;
            }
        }
        if (!emit_complete_frames(connection, context)) {
            context.record_error("invalid frame length");
            hangup = true;
        }
        if (hangup) drop_connection(connection);
        else if (registrar_) registrar_->rearm(connection.handle);
    }

    /* Returns false when the buffered bytes can never form a valid frame.
       Complete frames are emitted even when a disconnect carries tail data;
       an incomplete tail of a closing connection is discarded, never emitted. */
    bool emit_complete_frames(Connection& connection, SystemEventContext& context)
    {
        auto& buffer = connection.buffer;
        while (buffer.size() >= 4) {
            const uint32_t length = (uint32_t(buffer[0]) << 24) | (uint32_t(buffer[1]) << 16) |
                                    (uint32_t(buffer[2]) << 8) | uint32_t(buffer[3]);
            if (length == 0 || length > 256) return false;
            if (buffer.size() < 4 + length) break; // partial frame stays buffered
            std::string body(reinterpret_cast<const char*>(buffer.data()) + 4, length);
            buffer.erase(buffer.begin(), buffer.begin() + 4 + length);
            auto session = context.open_session(); // System envelope: Job 0
            (void)session.context().emit("text", Text{body + "\n"});
        }
        return true;
    }

    void drop_connection(Connection& connection)
    {
        const uint64_t id = connection.handle.id;
        connection.registration.reset();
        close(connection.fd);
        connections_.erase(id);
        --connections_active;
    }

    std::string path_;
    size_t max_connections_ = 8;
    int listener_ = -1;
    std::map<uint64_t, Connection> connections_;
    SourceRegistrar* registrar_ = nullptr;
    std::optional<SourceRegistration> listener_registration_;
    SourceHandle listener_handle_{};
};

AUTO_REGISTER_JOB_EVENT_COLLECTOR(DgramJobEvent, "reference Job datagram source");
AUTO_REGISTER_JOB_EVENT_COLLECTOR(DgramJobEventConflict, "reference conflicting datagram source");
AUTO_REGISTER_SYSTEM_EVENT_COLLECTOR(DgramSystemEvent, "reference System datagram source");
AUTO_REGISTER_SYSTEM_EVENT_COLLECTOR(StreamSystemEvent, "reference System stream source");

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

Job make_job(uint64_t id, std::vector<std::string> collectors)
{
    Job job;
    job.JobID = id;
    job.JobNativeID = std::to_string(id);
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    set_pids(job, {getpid()});
    job.CollectorNames = std::move(collectors);
    CommonJobAttr attr;
    attr.auto_update_child = false;
    job.sub_attr = attr;
    return job;
}

cev_datagram make_datagram(uint64_t tag, const std::string& payload, uint32_t flags = 0)
{
    static std::atomic<uint32_t> seq{0};
    cev_datagram message{};
    message.magic = CEV_DGRAM_MAGIC;
    message.seq = ++seq;
    message.tag = tag;
    message.flags = flags;
    message.payload_len = static_cast<uint32_t>(payload.size());
    std::memcpy(message.payload, payload.data(), payload.size());
    return message;
}

void send_to(const std::string& path, const cev_datagram& message)
{
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1);
    const int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    check(fd >= 0, "send socket");
    const ssize_t sent =
        sendto(fd, &message, offsetof(cev_datagram, payload) + message.payload_len, MSG_NOSIGNAL,
               reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    close(fd);
    check(sent >= 0, "sendto " + path);
}

void send_raw_to(const std::string& path, const void* data, size_t size)
{
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1);
    const int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    check(fd >= 0, "raw send socket");
    const ssize_t sent = sendto(fd, data, size, MSG_NOSIGNAL,
                                reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    close(fd);
    check(sent >= 0, "raw sendto " + path);
}

nlohmann::json collector_status(CollectorScheduler& scheduler, const std::string& name)
{
    const auto status = scheduler.snapshot();
    for (const auto& collector : status.at("collectors"))
        if (collector.at("name") == name) return collector;
    throw std::runtime_error("missing collector status: " + name);
}

size_t captured_count(const std::string& collector)
{
    std::lock_guard lock(CaptureWriter::store().mutex);
    size_t matched = 0;
    for (const auto& record : CaptureWriter::store().records)
        if (record->sample->collector_name == collector) ++matched;
    return matched;
}

std::string captured_text_at(const std::string& collector, size_t index)
{
    const auto records = CaptureWriter::wait_for(collector, index + 1);
    return CaptureWriter::text_of(records.at(index));
}

bool captured_contains(const std::string& collector, const std::string& needle)
{
    std::lock_guard lock(CaptureWriter::store().mutex);
    for (const auto& record : CaptureWriter::store().records) {
        if (record->sample->collector_name != collector) continue;
        const auto* text = std::get_if<Text>(&record->payload);
        if (text != nullptr && text->value.find(needle) != std::string::npos) return true;
    }
    return false;
}

/* Length-prefixed stream client used by the A3 fixture. */
struct StreamClient {
    int fd = -1;

    void connect_to(const std::string& path)
    {
        fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        check(fd >= 0, "client socket");
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1);
        const int result =
            connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
        check(result == 0, "client connect to " + path);
    }

    void send_frame(const std::string& body)
    {
        check(body.size() >= 1 && body.size() <= 256, "frame body bound");
        const uint32_t length = static_cast<uint32_t>(body.size());
        const unsigned char header[4] = {static_cast<unsigned char>(length >> 24),
                                         static_cast<unsigned char>(length >> 16),
                                         static_cast<unsigned char>(length >> 8),
                                         static_cast<unsigned char>(length)};
        ssize_t sent = send(fd, header, sizeof(header), MSG_NOSIGNAL);
        check(sent == 4, "frame header send");
        sent = send(fd, body.data(), body.size(), MSG_NOSIGNAL);
        check(sent == static_cast<ssize_t>(body.size()), "frame body send");
    }

    void send_bytes(const void* data, size_t size)
    {
        const ssize_t sent = send(fd, data, size, MSG_NOSIGNAL);
        check(sent == static_cast<ssize_t>(size), "raw stream send");
    }

    void finish() { check(shutdown(fd, SHUT_WR) == 0, "client shutdown write"); }

    void close_fd()
    {
        if (fd >= 0) close(fd);
        fd = -1;
    }
};

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

void run(const std::string& base)
{
    const std::string socket_path = base + ".rpc.sock";
    const std::string job_dgram = base + ".job.dgram";
    const std::string sys_dgram = base + ".sys.dgram";
    const std::string sys_stream = base + ".sys.stream";

    {
        std::ofstream config(base + ".yaml");
        config << "lens_config:\n  max_collector_threads: 2\n"
                  "collectors_config:\n"
                  "  enable_collector_perf: false\n"
                  "  default_freq: 20\n"
                  "  default_use_writers: []\n"
                  "  collectors:\n"
                  "    - {name: dgram_job, type: DgramJobEvent, config: dgram_job_cfg}\n"
                  "    - {name: dgram_conflict, type: DgramJobEventConflict, config: dgram_conflict_cfg}\n"
                  "    - {name: dgram_sys, type: DgramSystemEvent, config: dgram_sys_cfg}\n"
                  "    - {name: stream_sys, type: StreamSystemEvent, config: stream_sys_cfg}\n"
                  "writers_config:\n"
                  "  buffer_capacity: 256\n"
                  "  max_pending_records: 4096\n"
                  "  max_pending_bytes: 67108864\n"
                  "  max_record_bytes: 8388608\n"
                  "  writers:\n"
                  "    - {name: w, type: CaptureWriter, config: cap}\n"
                  "cap: {}\n"
                  "dgram_job_cfg:\n  socket_path: " << job_dgram << "\n"
                  "  outputs: {text: [w]}\n"
                  "dgram_conflict_cfg:\n  socket_path: " << job_dgram << "\n"
                  "  outputs: {text: [w]}\n"
                  "dgram_sys_cfg:\n  socket_path: " << sys_dgram << "\n  auto_start: true\n"
                  "  turn_limit: 3\n"
                  "  outputs: {text: [w]}\n"
                  "stream_sys_cfg:\n  socket_path: " << sys_stream << "\n  auto_start: true\n"
                  "  max_connections: 2\n"
                  "  outputs: {text: [w]}\n"
                  "job_registry_config:\n  job_db_path: " << base << ".db\n";
    }

    auto& server = RPCServer::instance(socket_path);
    (void)server;
    Config::instance(base + ".yaml");
    WriterManager::instance().registerWriter<CaptureWriter>("CaptureWriter");

    auto& scheduler = CollectorScheduler::instance();
    // Composition (the same wiring the service root performs): the scheduler is
    // the sole actual, the registry its desired-state source and drain sink.
    JobRegistry& registry = JobRegistry::instance();
    scheduler.attach(&registry, &registry);
    registry.setCollectionRuntime(&scheduler);
    scheduler.start();

    // ------------------------------------------------------------------
    // System datagram source starts with zero Jobs (auto_start) and proves
    // I2: the first datagram is sent immediately after init returns; the
    // pre-activation window must not lose it or wait for a second edge.
    // ------------------------------------------------------------------
    eventually([] { return DgramSystemEvent::init_done.load(); },
               "System datagram source was not initialized");
    send_to(sys_dgram, make_datagram(0, "early"));
    eventually([&] { return captured_contains("dgram_sys", "sys:0:early"); },
               "pre-activation datagram was lost");
    check(captured_count("dgram_sys") == 1, "unexpected extra System datagram output");

    // ------------------------------------------------------------------
    // E4: more input than one turn permits yields and continues; the source
    // stays disarmed between turns and no new edge is required.
    // ------------------------------------------------------------------
    for (int index = 0; index < 5; ++index) send_to(sys_dgram, make_datagram(0, "burst"));
    eventually([] { return DgramSystemEvent::counters.consumed.load() >= 5; },
               "System datagram burst was not fully consumed");
    check(DgramSystemEvent::ready_calls.load() >= 2,
          "turn bound of 3 did not require a continuation turn for 5 datagrams");
    eventually([] { return captured_count("dgram_sys") == 6; },
               "System datagram output count mismatch");

    // ------------------------------------------------------------------
    // A2: oversize, malformed and end-marker datagrams are rejected without
    // being treated as data; silence alone never produces output.
    // ------------------------------------------------------------------
    const std::vector<unsigned char> oversize(900, 'x');
    send_raw_to(sys_dgram, oversize.data(), oversize.size());
    eventually([] { return DgramSystemEvent::counters.oversize.load() >= 1; },
               "oversize datagram was not detected via MSG_TRUNC");
    eventually([] { return captured_count("dgram_sys") == 6; },
               "oversize datagram produced output");

    send_raw_to(sys_dgram, "bad", 3);
    eventually([] { return DgramSystemEvent::counters.malformed.load() >= 1; },
               "malformed datagram was not rejected");
    eventually([&] { return collector_status(scheduler, "dgram_sys").at("has_error").get<bool>(); },
               "malformed datagram did not surface a diagnostic");

    send_to(sys_dgram, make_datagram(0, "", CEV_FLAG_END));
    eventually([] { return DgramSystemEvent::counters.end_markers.load() >= 1; },
               "end-marker datagram was not recorded");
    eventually([] { return captured_count("dgram_sys") == 6; },
               "end-marker datagram produced output");

    // A valid message clears the error state and confirms the source stayed
    // healthy (EAGAIN remains a normal observation).
    send_to(sys_dgram, make_datagram(0, "recover"));
    eventually([&] { return captured_contains("dgram_sys", "sys:0:recover"); },
               "valid datagram after malformed input was lost");
    eventually(
        [&] { return !collector_status(scheduler, "dgram_sys").at("has_error").get<bool>(); },
        "successful drain did not clear the malformed-input error");

    // ------------------------------------------------------------------
    // A1 (Job binding): init-owned endpoint; interleaved messages for two
    // Jobs keep separate real-Job envelopes.
    // ------------------------------------------------------------------
    check(JobRegistry::instance().addJob(make_job(101, {"dgram_job"})), "add Job 101");
    eventually([] { return DgramJobEvent::init_done.load(); },
               "Job datagram source was not initialized on first binding");

    send_to(job_dgram, make_datagram(202, "wrong-way")); // Job 202 is not attached yet
    eventually([] { return DgramJobEvent::counters.unattributed.load() >= 1; },
               "datagram for an unattached Job was not counted");
    check(captured_count("dgram_job") == 0, "unattached Job tag produced output");

    check(JobRegistry::instance().addJob(make_job(202, {"dgram_job"})), "add Job 202");
    eventually([] { return DgramJobEvent::attached.load() >= 2; },
               "both Job bindings were not observed by the collector");

    send_to(job_dgram, make_datagram(202, "from-202"));
    send_to(job_dgram, make_datagram(101, "from-101"));
    send_to(job_dgram, make_datagram(101, "again-101"));
    eventually([&] { return captured_count("dgram_job") >= 3; },
               "interleaved Job datagrams were not all delivered");
    {
        const auto records = CaptureWriter::wait_for("dgram_job", 3);
        check(CaptureWriter::text_of(records[0]) == "from-202\n", "first Job attribution");
        check(records[0]->sample->job.JobID == 202, "first Job envelope");
        check(CaptureWriter::text_of(records[1]) == "from-101\n", "second Job attribution");
        check(records[1]->sample->job.JobID == 101, "second Job envelope");
        check(CaptureWriter::text_of(records[2]) == "again-101\n", "third Job attribution");
        check(records[2]->sample->job.JobID == 101, "third Job envelope");
    }

    // ------------------------------------------------------------------
    // A4: a second instance configured with the same live endpoint fails
    // init explicitly, does not remove the owned path and does not disturb
    // the healthy instance.
    // ------------------------------------------------------------------
    check(JobRegistry::instance().addJob(make_job(303, {"dgram_conflict"})),
          "add Job 303 (conflict binding)");
    eventually([] { return DgramJobEventConflict::init_failures.load() >= 1; },
               "conflicting endpoint did not fail init");
    eventually(
        [&] { return collector_status(scheduler, "dgram_conflict").at("has_error").get<bool>(); },
        "failed init did not surface a collector error");
    send_to(job_dgram, make_datagram(101, "still-owned"));
    eventually([&] { return captured_contains("dgram_job", "still-owned"); },
               "healthy instance was disturbed by the conflicting init");
    check(captured_count("dgram_conflict") == 0, "failed collector produced output");

    // ------------------------------------------------------------------
    // A3: connection-oriented fixture.
    // ------------------------------------------------------------------
    StreamClient first;
    StreamClient second;
    StreamClient third;
    first.connect_to(sys_stream);
    first.send_frame("one");
    eventually([&] { return captured_contains("stream_sys", "one"); },
               "first stream frame was not emitted");

    // Slow client: a partial frame must stay buffered without blocking the
    // lane or any other client.
    const unsigned char two_header[4] = {0, 0, 0, 3}; // frame "two", 3 bytes
    second.connect_to(sys_stream);
    second.send_bytes(two_header, sizeof(two_header));
    second.send_bytes("tw", 2); // one byte short of a complete frame

    // Connection limit: the third concurrent client is refused.
    third.connect_to(sys_stream);
    eventually([&] { return StreamSystemEvent::refused_connections.load() >= 1; },
               "connection limit did not refuse the third client");
    third.close_fd();

    // Disconnect with tail data: a complete frame followed by FIN must still
    // deliver the frame while the slow client stays pending.
    first.send_frame("three");
    first.finish();

    eventually([&] { return captured_contains("stream_sys", "three"); },
               "frame delivered together with hangup was lost");
    check(!captured_contains("stream_sys", "two"), "partial frame was emitted early");
    check(StreamSystemEvent::connections_active.load() == 1,
          "disconnected client was not removed");

    second.send_bytes("o", 1); // completes the buffered frame
    eventually([&] { return captured_contains("stream_sys", "two"); },
               "completed slow-client frame was not emitted");
    eventually([] { return captured_count("stream_sys") == 3; },
               "stream output count mismatch");

    // ------------------------------------------------------------------
    // Shutdown: owned endpoints are unlinked by their owners; a conflicting
    // instance never touches another instance's path.
    // ------------------------------------------------------------------
    scheduler.shutdown();
    WriterManager::instance().shutdown();

    check(access(job_dgram.c_str(), F_OK) != 0, "Job datagram endpoint was not unlinked");
    check(access(sys_dgram.c_str(), F_OK) != 0, "System datagram endpoint was not unlinked");
    check(access(sys_stream.c_str(), F_OK) != 0, "stream endpoint was not unlinked");
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: collector_event_source_test TEMP_BASE\n";
        return 2;
    }
    try {
        run(argv[1]);
        std::cout << "PASS collector event sources: owned datagram endpoints, bounded "
                     "continuation, protocol rejection, connection fixture, path ownership\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
