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
#include "common/task_executor.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
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
                std::chrono::milliseconds timeout = 3s)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline)
            throw std::runtime_error("timeout: " + message);
        std::this_thread::sleep_for(2ms);
    }
}

struct Fd {
    int value = -1;
    Fd() = default;
    explicit Fd(int fd) : value(fd) {}
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& other) noexcept : value(other.value) { other.value = -1; }
    Fd& operator=(Fd&& other) noexcept
    {
        if (this == &other) return *this;
        reset();
        value = other.value;
        other.value = -1;
        return *this;
    }
    ~Fd() { reset(); }
    void reset()
    {
        if (value >= 0) close(value);
        value = -1;
    }
    int release()
    {
        const int fd = value;
        value = -1;
        return fd;
    }
};

void make_pair(Fd& left, Fd& right)
{
    int sockets[2] = {-1, -1};
    check(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) == 0, "socketpair");
    left = Fd(sockets[0]);
    right = Fd(sockets[1]);
    const int flags = fcntl(left.value, F_GETFL, 0);
    check(fcntl(left.value, F_SETFL, flags | O_NONBLOCK) == 0, "nonblocking fd");
}

void send_byte(int fd, char value)
{
    check(send(fd, &value, 1, MSG_NOSIGNAL) == 1, "send byte");
}

char read_byte(int fd)
{
    char value = 0;
    const ssize_t size = recv(fd, &value, 1, 0);
    check(size == 1, "recv byte");
    return value;
}

void drain(int fd)
{
    char buffer[64];
    while (recv(fd, buffer, sizeof(buffer), 0) > 0) {
    }
}

size_t thread_count()
{
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0)
            return static_cast<size_t>(std::stoul(line.substr(8)));
    }
    return 0;
}

struct Sink {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<ReadySource> events;
    std::thread::id last_thread;

    void operator()(const ReadySources& ready)
    {
        std::lock_guard lock(mutex);
        events.insert(events.end(), ready.begin(), ready.end());
        last_thread = std::this_thread::get_id();
        cv.notify_all();
    }

    bool wait_for(size_t count, std::chrono::milliseconds timeout = 3s)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [&] { return events.size() >= count; });
    }

    size_t size()
    {
        std::lock_guard lock(mutex);
        return events.size();
    }

    ReadySource at(size_t index)
    {
        std::lock_guard lock(mutex);
        return events.at(index);
    }

    void clear()
    {
        std::lock_guard lock(mutex);
        events.clear();
    }
};

// I2/I5/E3: data that exists before commit is delivered after activation; the
// borrowed descriptor stays open after cancellation.
void staged_commit_and_borrowed_descriptor()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    Fd reader;
    Fd writer;
    make_pair(reader, writer);

    StagedSourceRegistrar registrar(poller, [&](const ReadySources& ready) { sink(ready); });
    auto registration = registrar.watch_fd(reader.value, Interest::Readable);
    check(registration.has_value(), "I2 stage fd");
    check(registrar.pending() == 1 && registrar.active() == 0, "I2 staged but inactive");
    send_byte(writer.value, 'a');
    std::this_thread::sleep_for(20ms);
    check(sink.size() == 0, "I2 handler observed a half-initialized source");

    check(registrar.commit(), "I2 commit");
    check(registrar.pending() == 0 && registrar.active() == 1, "I2 committed source");
    check(sink.wait_for(1), "I2 pre-existing data was not delivered");
    check(read_byte(reader.value) == 'a', "I2 readable data");
    const auto handle = sink.at(0).source;
    check(registrar.rearm(handle), "I2 rearm");

    registration->cancel();
    check(registrar.active() == 0, "I5 registration still active");
    // Cancel only unregisters interest; the collector-owned descriptor is open.
    send_byte(writer.value, 'b');
    check(read_byte(reader.value) == 'b', "I5 cancellation closed the borrowed fd");
    poller.shutdown();
}

// I3: a failed commit revokes staged and already-activated interests.
void partial_commit_rollback()
{
    Sink sink;
    EventPoller poller(1);
    poller.start();
    Fd first_reader;
    Fd first_writer;
    Fd second_reader;
    Fd second_writer;
    make_pair(first_reader, first_writer);
    make_pair(second_reader, second_writer);

    StagedSourceRegistrar registrar(poller, [&](const ReadySources& ready) { sink(ready); });
    auto first = registrar.watch_fd(first_reader.value, Interest::Readable);
    auto second = registrar.watch_fd(second_reader.value, Interest::Readable);
    check(first.has_value() && second.has_value(), "I3 stage two sources");
    check(!registrar.commit(), "I3 commit beyond source limit succeeded");
    check(registrar.active() == 0 && registrar.pending() == 0, "I3 rollback left state");
    check(poller.active_sources() == 0, "I3 poller retained staged sources");
    send_byte(first_writer.value, 'x');
    send_byte(second_writer.value, 'y');
    std::this_thread::sleep_for(30ms);
    check(sink.size() == 0, "I3 failed init reached a handler");
    check(read_byte(first_reader.value) == 'x', "I3 owned fd was closed");

    // The same registrar can be reused after a clean revoke.
    auto retry = registrar.watch_fd(first_reader.value, Interest::Readable);
    check(retry.has_value() && registrar.commit(), "I3 retry after rollback");
    check(registrar.active() == 1, "I3 retry active source");
    poller.shutdown();
}

// I6/I7: dynamic sources work after commit, and an FD reused by a new source
// only receives the new registration's readiness.
void dynamic_sources_and_fd_reuse()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    Fd reader;
    Fd writer;
    make_pair(reader, writer);

    StagedSourceRegistrar registrar(poller, [&](const ReadySources& ready) { sink(ready); });
    check(registrar.commit(), "I6 empty commit");
    auto dynamic = registrar.watch_fd(reader.value, Interest::Readable);
    check(dynamic.has_value(), "I6 dynamic registration");
    check(registrar.active() == 1, "I6 dynamic source active");
    send_byte(writer.value, 'd');
    check(sink.wait_for(1), "I6 dynamic source readiness");
    const auto first_handle = sink.at(0).source;
    drain(reader.value);
    check(registrar.rearm(first_handle), "I6 rearm dynamic source");
    dynamic->cancel();
    check(registrar.active() == 0, "I6 dynamic source cancel");

    const int reused_fd = reader.value;
    reader.reset();
    writer.reset();
    Fd reused_reader;
    Fd reused_writer;
    make_pair(reused_reader, reused_writer);
    check(reused_reader.value == reused_fd || reused_reader.value >= 0, "I7 fd reuse setup");
    auto replacement = registrar.watch_fd(reused_reader.value, Interest::Readable);
    check(replacement.has_value(), "I7 replacement registration");
    check(!poller.rearm(first_handle), "I7 stale handle rearmed");
    send_byte(reused_writer.value, 'r');
    check(sink.wait_for(2), "I7 replacement readiness");
    check(sink.at(1).source != first_handle, "I7 old generation leaked into new source");
    check(read_byte(reused_reader.value) == 'r', "I7 replacement data");
    poller.shutdown();
}

// E1: the poll thread only requests executor work; reads and collector work run
// on executor workers.
void poll_thread_isolation()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    TaskExecutor executor({1, 8});
    std::atomic<bool> work_ran{false};
    std::atomic<std::thread::id> work_thread{};
    WorkHandle work{};
    auto work_handle = executor.registerTask("event-lane", [&] {
        work_thread = std::this_thread::get_id();
        work_ran = true;
        return WorkResult::Complete;
    });
    check(work_handle.has_value(), "E1 register lane");
    work = *work_handle;

    Fd reader;
    Fd writer;
    make_pair(reader, writer);
    auto handle = poller.watch_fd(reader.value, Interest::Readable,
        [&](const ReadySources& ready) {
            sink(ready);
            drain(reader.value);
            check(executor.request(work, RequestPolicy::Remember), "E1 request lane");
        });
    check(handle.has_value(), "E1 watch fd");
    send_byte(writer.value, 'e');
    check(sink.wait_for(1), "E1 readiness");
    eventually([&] { return work_ran.load(); }, "E1 executor work");
    check(sink.last_thread != work_thread.load(), "E1 collector work ran on the poll thread");
    check(sink.last_thread == std::this_thread::get_id() ? false : true, "E1 sink thread sanity");
    poller.shutdown();
    executor.shutdown();
}

// E2: adding sources does not create per-source wait threads.
void no_per_source_threads()
{
    EventPoller poller(64);
    poller.start();
    const size_t baseline = thread_count();
    std::vector<Fd> readers;
    std::vector<Fd> writers;
    std::vector<SourceHandle> handles;
    for (int index = 0; index < 16; ++index) {
        Fd reader;
        Fd writer;
        make_pair(reader, writer);
        auto handle = poller.watch_fd(reader.value, Interest::Readable, [](const ReadySources&) {});
        check(handle.has_value(), "E2 watch source");
        handles.push_back(*handle);
        readers.push_back(std::move(reader));
        writers.push_back(std::move(writer));
    }
    check(thread_count() == baseline, "E2 source creation added a wait thread");
    check(poller.active_sources() == 16, "E2 source count");
    poller.shutdown();
}

// E4/E5: a source stays disarmed until the worker rearms it; data arriving
// between drain and rearm is still delivered exactly once.
void one_shot_and_rearm_race()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    Fd reader;
    Fd writer;
    make_pair(reader, writer);
    auto handle = poller.watch_fd(reader.value, Interest::Readable,
        [&](const ReadySources& ready) {
            sink(ready);
            if (sink.size() == 1) read_byte(reader.value);
        });
    check(handle.has_value(), "E4 watch");
    send_byte(writer.value, '1');
    send_byte(writer.value, '2');
    check(sink.wait_for(1), "E4 first readiness");
    std::this_thread::sleep_for(30ms);
    check(sink.size() == 1, "E4 disarmed source produced repeated readiness");
    // Data was already pending while the source was disarmed. Rearm must report
    // it without a second producer notification.
    check(poller.rearm(*handle), "E4 rearm");
    check(sink.wait_for(2), "E5 rearm race lost readiness");
    check(read_byte(reader.value) == '2', "E5 pending byte");
    poller.shutdown();
}

// E6: manual readiness is an explicit scan/notification path, not an FD
// threshold or a periodic trigger.
void manual_readiness()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    StagedSourceRegistrar registrar(poller, [&](const ReadySources& ready) { sink(ready); });
    auto manual = registrar.watch_manual("manual");
    check(manual.has_value(), "E6 manual source");
    check(registrar.commit(), "E6 commit");
    check(registrar.notify_manual(manual->handle()), "E6 manual notify");
    check(sink.wait_for(1), "E6 manual readiness");
    const auto event = sink.at(0);
    check(event.readiness & ready_flags::readable, "E6 manual readiness flags");
    check(event.source == manual->handle(), "E6 manual handle");
    poller.shutdown();
}

// E7: readable tail bytes and hangup are delivered together.
void tail_with_hangup()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    Fd reader;
    Fd writer;
    make_pair(reader, writer);
    auto handle = poller.watch_fd(reader.value, Interest::Readable,
        [&](const ReadySources& ready) { sink(ready); });
    check(handle.has_value(), "E7 watch");
    send_byte(writer.value, 't');
    shutdown(writer.value, SHUT_WR);
    check(sink.wait_for(1), "E7 hangup readiness");
    const auto first = sink.at(0);
    check(first.readiness & ready_flags::readable, "E7 tail data flag");
    check(read_byte(reader.value) == 't', "E7 tail data");
    if (!(first.readiness & ready_flags::hangup)) {
        // Linux may report the readable tail and the peer half-close in
        // separate readiness edges. Rearm and require the hangup edge.
        check(poller.rearm(*handle), "E7 rearm for hangup");
        check(sink.wait_for(2), "E7 hangup edge");
        check(sink.at(1).readiness & ready_flags::hangup, "E7 hangup flag");
    }
    poller.cancel(*handle);
    poller.shutdown();
}

// E8: one failing sink leaves its source disarmed without spinning the poll
// thread; unrelated sources continue and explicit recovery works.
void reader_failure_isolation()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    Fd bad_reader;
    Fd bad_writer;
    Fd good_reader;
    Fd good_writer;
    make_pair(bad_reader, bad_writer);
    make_pair(good_reader, good_writer);

    std::atomic<bool> fail_once{true};
    std::atomic<int> bad_calls{0};
    auto bad = poller.watch_fd(bad_reader.value, Interest::Readable,
        [&](const ReadySources& ready) {
            ++bad_calls;
            drain(bad_reader.value);
            if (fail_once.exchange(false)) throw std::runtime_error("synthetic reader failure");
            sink(ready);
        });
    auto good = poller.watch_fd(good_reader.value, Interest::Readable,
        [&](const ReadySources& ready) {
            drain(good_reader.value);
            sink(ready);
        });
    check(bad.has_value() && good.has_value(), "E8 watch sources");
    send_byte(bad_writer.value, 'b');
    check(sink.size() == 0, "E8 failing sink produced output");
    eventually([&] { return poller.snapshot().at("errors").get<size_t>() >= 1; },
               "E8 failure not recorded");

    send_byte(good_writer.value, 'g');
    check(sink.wait_for(1), "E8 healthy source stopped after sibling failure");
    std::this_thread::sleep_for(30ms);
    check(bad_calls.load() == 1, "E8 failing source spun");

    // No new readiness until explicit rearm.
    send_byte(bad_writer.value, 'c');
    std::this_thread::sleep_for(30ms);
    check(bad_calls.load() == 1, "E8 disarmed failing source was rearmed automatically");
    check(poller.rearm(*bad), "E8 rearm after failure");
    check(sink.wait_for(2), "E8 recovery readiness");
    check(bad_calls.load() == 2, "E8 recovery call count");
    poller.shutdown();
}

// RingBuffer probes use ring_buffer__epoll_fd(): the shared poller watches an
// inner epoll fd. This fixture uses an eventfd inside an inner epoll to verify
// the nested-epoll, one-shot and rearm contract without requiring BPF.
void nested_epoll_ring_buffer_probe()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();

    const int inner = epoll_create1(EPOLL_CLOEXEC);
    const int signal = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    check(inner >= 0 && signal >= 0, "nested epoll setup");
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = signal;
    check(epoll_ctl(inner, EPOLL_CTL_ADD, signal, &event) == 0, "inner epoll add");

    StagedSourceRegistrar registrar(poller, [&](const ReadySources& ready) { sink(ready); });
    auto registration = registrar.watch_probe(
        ProbeDescriptor{ProbeKind::RingBuffer, inner, "fake-ringbuf"});
    check(registration.has_value(), "RingBuffer probe staging");
    check(registrar.pending() == 1 && registrar.active() == 0,
          "RingBuffer probe activated before commit");
    check(registrar.commit(), "RingBuffer probe commit");
    const auto handle = registration->handle();
    check(poller.snapshot().at("sources").at(0).at("kind") == "ring_buffer",
          "RingBuffer probe kind status");

    uint64_t value = 1;
    check(write(signal, &value, sizeof(value)) == sizeof(value), "signal inner epoll");
    check(sink.wait_for(1), "nested epoll readiness");
    check(sink.at(0).readiness & ready_flags::readable, "nested epoll readable flag");
    check(read(signal, &value, sizeof(value)) == sizeof(value), "drain inner eventfd");

    // Outer epoll is one-shot; no rearm means no second notification even when
    // the inner epoll is readable again.
    check(write(signal, &value, sizeof(value)) == sizeof(value), "signal disarmed inner epoll");
    std::this_thread::sleep_for(50ms);
    check(sink.size() == 1, "RingBuffer probe ignored one-shot disarm");

    check(registrar.rearm(handle), "RingBuffer probe rearm");
    check(sink.wait_for(2), "RingBuffer probe rearm readiness");
    check(read(signal, &value, sizeof(value)) == sizeof(value), "drain rearmed eventfd");

    registration->cancel();
    check(fcntl(inner, F_GETFD) >= 0, "cancel closed the inner epoll fd");
    poller.shutdown();
    close(signal);
    close(inner);
}

// I4: commit is the activation boundary for a new registration group.
void activation_boundary()
{
    Sink sink;
    EventPoller poller(16);
    poller.start();
    Fd reader;
    Fd writer;
    make_pair(reader, writer);
    StagedSourceRegistrar registrar(poller, [&](const ReadySources& ready) { sink(ready); });
    auto registration = registrar.watch_fd(reader.value, Interest::Readable);
    send_byte(writer.value, 'a');
    check(!sink.wait_for(1, 50ms), "I4 data delivered before commit");
    check(registrar.commit(), "I4 commit");
    check(sink.wait_for(1), "I4 data after commit");
    poller.shutdown();
}

} // namespace

int main()
{
    try {
        staged_commit_and_borrowed_descriptor();
        partial_commit_rollback();
        dynamic_sources_and_fd_reuse();
        poll_thread_isolation();
        no_per_source_threads();
        one_shot_and_rearm_race();
        manual_readiness();
        tail_with_hangup();
        reader_failure_isolation();
        activation_boundary();
        nested_epoll_ring_buffer_probe();
        std::cout << "PASS event poller: staged registration, one-shot rearm, manual readiness, "
                     "hangup, failure isolation and thread ownership\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
