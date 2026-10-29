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
// RbEventWait — wakeup-driven blocking for a watcher's eBPF ring buffer
// (the exec fast path, layer 1). The epoll holds libbpf's own ring-buffer
// epoll fd plus a stop eventfd, so exec events reach the sink in
// kernel-wakeup time instead of on the next polling slice — the same
// "watch libbpf's epoll fd" pattern the shared collector poller uses
// (include/ebpf/ring_buffer_probe.hpp). Callers that cannot init keep their
// legacy slice loop: a missing helper is a latency regression, never an
// outage.
#pragma once

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include <spdlog/spdlog.h>

class RbEventWait {
public:
    enum class Wakeup {
        Data = 0,     // the data fd (ring-buffer epoll) is readable
        Stop,         // wake() fired
        Timeout,      // the timeout elapsed (or EINTR; never fatal)
        Error,        // epoll_wait failed for a real reason
    };

    RbEventWait() = default;
    ~RbEventWait() { close(); }
    RbEventWait(const RbEventWait&) = delete;
    RbEventWait& operator=(const RbEventWait&) = delete;

    // data_fd: the ring buffer's epoll fd (ring_buffer__epoll_fd(rb)), or -1
    // for stop/deadline-only waiting. Returns false without leaking fds.
    bool init(int data_fd)
    {
        close();
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        stop_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (epoll_fd_ < 0 || stop_fd_ < 0) {
            close();
            return false;
        }
        epoll_event stop_event{};
        stop_event.events = EPOLLIN;
        stop_event.data.fd = stop_fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, stop_fd_, &stop_event) != 0) {
            close();
            return false;
        }
        if (data_fd >= 0) {
            epoll_event data_event{};
            data_event.events = EPOLLIN;
            data_event.data.fd = data_fd;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, data_fd, &data_event) != 0) {
                close();
                return false;
            }
            data_fd_ = data_fd;
        }
        return true;
    }

    bool ready() const { return epoll_fd_ >= 0; }

    // Blocks until the data fd, stop fd or timeout. timeout_ms: -1 waits
    // forever (the stop fd is the only exit); 0 polls once. The stop fd is
    // level-triggered, so a wake() that arrived before wait() is reported,
    // never lost.
    Wakeup wait(int timeout_ms)
    {
        epoll_event events[2];
        const int count = epoll_wait(epoll_fd_, events, 2, timeout_ms);
        if (count < 0) return errno == EINTR ? Wakeup::Timeout : Wakeup::Error;
        for (int index = 0; index < count; ++index) {
            if (events[index].data.fd == stop_fd_) return Wakeup::Stop;
            if (data_fd_ >= 0 && events[index].data.fd == data_fd_) return Wakeup::Data;
        }
        return Wakeup::Timeout;
    }

    // Idempotent stop signal; a no-op before init or after close.
    void wake()
    {
        if (stop_fd_ < 0) return;
        const std::uint64_t one = 1;
        if (write(stop_fd_, &one, sizeof(one)) < 0 && errno != EAGAIN)
            spdlog::warn("RbEventWait: stop wake failed (errno {})", errno);
    }

    // Closes the epoll and the stop fd. The data fd is caller-owned (libbpf
    // closes it with the ring buffer) and is never touched here.
    void close()
    {
        if (epoll_fd_ >= 0) {
            ::close(epoll_fd_);
            epoll_fd_ = -1;
        }
        if (stop_fd_ >= 0) {
            ::close(stop_fd_);
            stop_fd_ = -1;
        }
        data_fd_ = -1;
    }

private:
    int epoll_fd_{-1};
    int stop_fd_{-1};
    int data_fd_{-1};
};
