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
#include "common/event_poller.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <utility>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

namespace {
uint32_t event_mask(ProbeKind kind)
{
    uint32_t mask = EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLONESHOT;
    if (kind == ProbeKind::Fd) mask |= EPOLLRDHUP;
    return mask;
}

uint32_t translate_events(uint32_t events)
{
    uint32_t readiness = 0;
    if (events & EPOLLIN) readiness |= ready_flags::readable;
    if (events & (EPOLLHUP | EPOLLRDHUP)) readiness |= ready_flags::hangup;
    if (events & EPOLLERR) readiness |= ready_flags::error;
    return readiness;
}
} // namespace

struct EventPoller::State {
    struct Registration {
        ProbeKind kind = ProbeKind::Fd;
        int fd = -1;
        std::string name;
        SourceHandle handle{};
        ReadySink sink;
        bool armed = true;
    };

    mutable std::mutex mutex;
    int epoll_fd = -1;
    int wake_fd = -1;
    bool started = false;
    bool stopped = false;
    size_t max_sources = 0;
    uint64_t next_id = 0;
    uint64_t wakeups = 0;
    uint64_t batches = 0;
    uint64_t errors = 0;
    std::unordered_map<uint64_t, Registration> registrations;
};

EventPoller::EventPoller(size_t max_sources)
    : state_(std::make_shared<State>())
{
    state_->max_sources = std::max<size_t>(1, max_sources);
}

EventPoller::~EventPoller()
{
    shutdown();
}

void EventPoller::start()
{
    std::lock_guard shutdown_lock(shutdown_mutex_);
    {
        std::lock_guard lock(state_->mutex);
        if (state_->started || state_->stopped) return;
        state_->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if (state_->epoll_fd < 0)
            throw std::runtime_error("EventPoller: epoll_create1 failed: " + std::string(std::strerror(errno)));
        state_->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (state_->wake_fd < 0) {
            close(state_->epoll_fd);
            state_->epoll_fd = -1;
            throw std::runtime_error("EventPoller: eventfd failed: " + std::string(std::strerror(errno)));
        }
        epoll_event wake{};
        wake.events = EPOLLIN;
        wake.data.u64 = 0;
        if (epoll_ctl(state_->epoll_fd, EPOLL_CTL_ADD, state_->wake_fd, &wake) != 0) {
            close(state_->wake_fd);
            close(state_->epoll_fd);
            state_->wake_fd = state_->epoll_fd = -1;
            throw std::runtime_error("EventPoller: cannot register wake fd");
        }
        state_->started = true;
    }
    try {
        thread_ = std::thread([this] { loop(); });
    } catch (...) {
        std::lock_guard lock(state_->mutex);
        state_->started = false;
        if (state_->wake_fd >= 0) close(state_->wake_fd);
        if (state_->epoll_fd >= 0) close(state_->epoll_fd);
        state_->wake_fd = state_->epoll_fd = -1;
        throw;
    }
}

void EventPoller::shutdown()
{
    std::lock_guard shutdown_lock(shutdown_mutex_);
    bool wake = false;
    {
        std::lock_guard lock(state_->mutex);
        if (state_->stopped) return;
        state_->stopped = true;
        if (state_->wake_fd >= 0) {
            const uint64_t one = 1;
            (void)write(state_->wake_fd, &one, sizeof(one));
            wake = true;
        }
    }
    (void)wake;
    if (thread_.joinable()) thread_.join();

    std::lock_guard lock(state_->mutex);
    state_->registrations.clear();
    if (state_->wake_fd >= 0) close(state_->wake_fd);
    if (state_->epoll_fd >= 0) close(state_->epoll_fd);
    state_->wake_fd = state_->epoll_fd = -1;
    state_->started = false;
}

std::optional<SourceHandle> EventPoller::watch_probe(const ProbeDescriptor& probe,
                                                     ReadySink sink)
{
    if (probe.readiness_fd < 0 || !sink) return std::nullopt;
    std::lock_guard lock(state_->mutex);
    if (!state_->started || state_->stopped) return std::nullopt;
    if (state_->registrations.size() >= state_->max_sources) return std::nullopt;

    const uint64_t id = ++state_->next_id;
    epoll_event event{};
    event.events = event_mask(probe.kind);
    event.data.u64 = id;
    if (epoll_ctl(state_->epoll_fd, EPOLL_CTL_ADD, probe.readiness_fd, &event) != 0) {
        ++state_->errors;
        spdlog::warn("EventPoller: cannot watch {} descriptor {}: {}",
                     to_string(probe.kind), probe.readiness_fd, std::strerror(errno));
        return std::nullopt;
    }
    State::Registration registration;
    registration.kind = probe.kind;
    registration.fd = probe.readiness_fd;
    registration.name = probe.name;
    registration.handle = SourceHandle{id, 1};
    registration.sink = std::move(sink);
    registration.armed = true;
    state_->registrations.emplace(id, std::move(registration));
    return SourceHandle{id, 1};
}

bool EventPoller::rearm(SourceHandle handle)
{
    std::lock_guard lock(state_->mutex);
    if (!state_->started || state_->stopped) return false;
    auto it = state_->registrations.find(handle.id);
    if (it == state_->registrations.end() || it->second.handle.generation != handle.generation)
        return false;
    epoll_event event{};
    event.events = event_mask(it->second.kind);
    event.data.u64 = it->second.handle.id;
    if (epoll_ctl(state_->epoll_fd, EPOLL_CTL_MOD, it->second.fd, &event) != 0) {
        ++state_->errors;
        return false;
    }
    it->second.armed = true;
    return true;
}

bool EventPoller::cancel(SourceHandle handle)
{
    std::lock_guard lock(state_->mutex);
    auto it = state_->registrations.find(handle.id);
    if (it == state_->registrations.end() || it->second.handle.generation != handle.generation)
        return false;
    if (state_->epoll_fd >= 0) epoll_ctl(state_->epoll_fd, EPOLL_CTL_DEL, it->second.fd, nullptr);
    state_->registrations.erase(it);
    return true;
}

size_t EventPoller::active_sources() const
{
    std::lock_guard lock(state_->mutex);
    return state_->registrations.size();
}

void EventPoller::loop()
{
    epoll_event events[64];
    while (true) {
        {
            std::lock_guard lock(state_->mutex);
            if (state_->stopped) return;
        }
        const int count = epoll_wait(state_->epoll_fd, events, 64, -1);
        if (count < 0) {
            if (errno == EINTR) continue;
            std::lock_guard lock(state_->mutex);
            ++state_->errors;
            spdlog::error("EventPoller: epoll_wait failed: {}", std::strerror(errno));
            return;
        }

        for (int index = 0; index < count; ++index) {
            const uint64_t id = events[index].data.u64;
            if (id == 0) {
                uint64_t value = 0;
                while (read(state_->wake_fd, &value, sizeof(value)) > 0) {
                }
                continue;
            }

            ReadySink sink;
            SourceHandle handle{};
            uint32_t readiness = 0;
            {
                std::lock_guard lock(state_->mutex);
                if (state_->stopped) return;
                auto it = state_->registrations.find(id);
                if (it == state_->registrations.end()) continue;
                it->second.armed = false;
                sink = it->second.sink;
                handle = it->second.handle;
                readiness = translate_events(events[index].events);
                ++state_->wakeups;
                ++state_->batches;
            }
            if (!sink) continue;
            try {
                sink(ReadySources{{handle, readiness}});
            } catch (const std::exception& error) {
                std::lock_guard lock(state_->mutex);
                ++state_->errors;
                spdlog::error("EventPoller: readiness sink failed: {}", error.what());
            } catch (...) {
                std::lock_guard lock(state_->mutex);
                ++state_->errors;
                spdlog::error("EventPoller: readiness sink failed with unknown exception");
            }
        }
    }
}

nlohmann::json EventPoller::snapshot() const
{
    std::lock_guard lock(state_->mutex);
    auto sources = nlohmann::json::array();
    for (const auto& [id, registration] : state_->registrations) {
        (void)id;
        sources.push_back({
            {"source_id", registration.handle.id},
            {"generation", registration.handle.generation},
            {"kind", to_string(registration.kind)},
            {"name", registration.name},
            {"fd", registration.fd},
            {"armed", registration.armed},
        });
    }
    return {
        {"started", state_->started},
        {"stopped", state_->stopped},
        {"active_sources", state_->registrations.size()},
        {"max_sources", state_->max_sources},
        {"wakeups", state_->wakeups},
        {"batches", state_->batches},
        {"errors", state_->errors},
        {"sources", std::move(sources)},
    };
}

StagedSourceRegistrar::StagedSourceRegistrar(EventPoller& poller, EventPoller::ReadySink sink)
    : poller_(poller),
      sink_(std::move(sink)),
      poller_sink_([this](const ReadySources& ready) { forward_translated(ready); })
{
}

StagedSourceRegistrar::~StagedSourceRegistrar()
{
    revoke();
}

void StagedSourceRegistrar::forward_translated(const ReadySources& ready)
{
    ReadySources translated;
    EventPoller::ReadySink sink;
    {
        std::lock_guard lock(mutex_);
        translated.reserve(ready.size());
        for (const auto& source : ready) {
            for (auto& [id, entry] : entries_) {
                (void)id;
                if (!entry.manual && entry.committed && entry.poller.id == source.source.id) {
                    translated.push_back(ReadySource{entry.staged, source.readiness});
                    break;
                }
            }
        }
        sink = sink_;
    }
    if (translated.empty() || !sink) return;
    sink(translated);
}

std::optional<SourceRegistration> StagedSourceRegistrar::watch_probe(
    const ProbeDescriptor& probe)
{
    if (probe.readiness_fd < 0 || !sink_) return std::nullopt;
    std::lock_guard lock(mutex_);
    const SourceHandle staged{++next_id_, 1};
    Entry entry;
    entry.probe = probe;
    entry.staged = staged;
    if (committed_) {
        auto poller_handle = poller_.watch_probe(probe, poller_sink_);
        if (!poller_handle) return std::nullopt;
        entry.poller = *poller_handle;
        entry.committed = true;
    }
    entries_.emplace(staged.id, std::move(entry));
    return SourceRegistration(this, staged);
}

std::optional<SourceRegistration> StagedSourceRegistrar::watch_manual(std::string name)
{
    if (!sink_) return std::nullopt;
    std::lock_guard lock(mutex_);
    const SourceHandle staged{++next_id_, 1};
    Entry entry;
    entry.manual = true;
    entry.probe.name = std::move(name);
    entry.staged = staged;
    entry.committed = committed_;
    entries_.emplace(staged.id, std::move(entry));
    return SourceRegistration(this, staged);
}

void StagedSourceRegistrar::cancel(SourceHandle handle)
{
    std::lock_guard lock(mutex_);
    auto it = entries_.find(handle.id);
    if (it == entries_.end() || it->second.staged.generation != handle.generation) return;
    if (it->second.committed && !it->second.manual) poller_.cancel(it->second.poller);
    entries_.erase(it);
}

bool StagedSourceRegistrar::rearm(SourceHandle handle)
{
    std::lock_guard lock(mutex_);
    auto it = entries_.find(handle.id);
    if (it == entries_.end() || it->second.staged.generation != handle.generation) return false;
    if (!it->second.committed || it->second.manual) return false;
    return poller_.rearm(it->second.poller);
}

bool StagedSourceRegistrar::commit()
{
    std::lock_guard lock(mutex_);
    if (committed_) return true;
    for (auto& [id, entry] : entries_) {
        (void)id;
        if (entry.manual) {
            entry.committed = true;
            continue;
        }
        if (entry.committed) continue;
        auto poller_handle = poller_.watch_probe(entry.probe, poller_sink_);
        if (!poller_handle) {
            for (auto& [registered_id, registered] : entries_) {
                (void)registered_id;
                if (registered.committed && !registered.manual)
                    poller_.cancel(registered.poller);
            }
            entries_.clear();
            committed_ = false;
            return false;
        }
        entry.poller = *poller_handle;
        entry.committed = true;
    }
    committed_ = true;
    return true;
}

void StagedSourceRegistrar::revoke()
{
    std::lock_guard lock(mutex_);
    for (auto& [id, entry] : entries_) {
        (void)id;
        if (entry.committed && !entry.manual) poller_.cancel(entry.poller);
    }
    entries_.clear();
    committed_ = false;
}

bool StagedSourceRegistrar::notify_manual(SourceHandle handle)
{
    EventPoller::ReadySink sink;
    SourceHandle staged{};
    {
        std::lock_guard lock(mutex_);
        auto it = entries_.find(handle.id);
        if (it == entries_.end() || it->second.staged.generation != handle.generation) return false;
        if (!it->second.manual || !it->second.committed) return false;
        sink = sink_;
        staged = it->second.staged;
    }
    try {
        sink(ReadySources{{staged, ready_flags::readable}});
    } catch (...) {
        return false;
    }
    return true;
}

size_t StagedSourceRegistrar::pending() const
{
    std::lock_guard lock(mutex_);
    size_t count = 0;
    for (const auto& [id, entry] : entries_) {
        (void)id;
        if (!entry.committed) ++count;
    }
    return count;
}

size_t StagedSourceRegistrar::active() const
{
    std::lock_guard lock(mutex_);
    size_t count = 0;
    for (const auto& [id, entry] : entries_) {
        (void)id;
        if (entry.committed) ++count;
    }
    return count;
}

nlohmann::json StagedSourceRegistrar::snapshot() const
{
    std::lock_guard lock(mutex_);
    auto sources = nlohmann::json::array();
    for (const auto& [id, entry] : entries_) {
        (void)id;
        sources.push_back({
            {"source_id", entry.staged.id},
            {"generation", entry.staged.generation},
            {"kind", to_string(entry.probe.kind)},
            {"fd", entry.probe.readiness_fd},
            {"manual", entry.manual},
            {"committed", entry.committed},
            {"name", entry.probe.name},
        });
    }
    return {{"committed", committed_}, {"sources", std::move(sources)}};
}
