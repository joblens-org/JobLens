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
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/collector_execution_context.hpp"

namespace ready_flags {
constexpr uint32_t readable = 1u;
constexpr uint32_t hangup = 2u;
constexpr uint32_t error = 4u;
} // namespace ready_flags

// One shared epoll thread. It only records readiness and invokes a sink; it
// never reads business data, decodes records or calls collector work methods.
class EventPoller {
public:
    // Runs on the poll thread. It must only record/request work; it must not
    // read business data, block, commit/revoke registrations or call collector
    // work directly.
    using ReadySink = std::function<void(const ReadySources&)>;

    explicit EventPoller(size_t max_sources = 1024);
    ~EventPoller();

    EventPoller(const EventPoller&) = delete;
    EventPoller& operator=(const EventPoller&) = delete;

    void start();
    void shutdown();

    // One-shot readiness. The source stays disarmed until rearm() is called by
    // the worker after it has drained the descriptor. Cancelling never closes
    // the borrower's descriptor.
    std::optional<SourceHandle> watch_fd(int fd, Interest interest, ReadySink sink);
    bool rearm(SourceHandle handle);
    bool cancel(SourceHandle handle);

    size_t active_sources() const;
    nlohmann::json snapshot() const;

private:
    struct State;
    std::shared_ptr<State> state_;
    std::mutex shutdown_mutex_;
    std::thread thread_;

    void loop();
};

// Collector-bound staged registrar. Init-time watches stay inactive until
// commit(); a failed init can revoke them without racing an active handler.
class StagedSourceRegistrar final : public SourceRegistrar {
public:
    StagedSourceRegistrar(EventPoller& poller, EventPoller::ReadySink sink);
    ~StagedSourceRegistrar() override;

    StagedSourceRegistrar(const StagedSourceRegistrar&) = delete;
    StagedSourceRegistrar& operator=(const StagedSourceRegistrar&) = delete;

    std::optional<SourceRegistration> watch_fd(int fd, Interest interest) override;
    std::optional<SourceRegistration> watch_manual(std::string name) override;
    void cancel(SourceHandle handle) override;

    bool commit();
    void revoke();
    bool notify_manual(SourceHandle handle);

    size_t pending() const;
    size_t active() const;
    nlohmann::json snapshot() const;

private:
    struct Entry {
        bool manual = false;
        int fd = -1;
        Interest interest = Interest::Readable;
        SourceHandle staged{};
        SourceHandle poller{};
        bool committed = false;
        std::string name;
    };

    EventPoller& poller_;
    EventPoller::ReadySink sink_;
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, Entry> entries_;
    uint64_t next_id_ = 0;
    bool committed_ = false;
};
