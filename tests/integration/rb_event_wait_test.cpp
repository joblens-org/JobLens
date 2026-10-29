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
/* RbEventWait acceptance: the watcher poll threads block on libbpf's
 * ring-buffer epoll fd plus a stop eventfd (the exec fast path, layer 1),
 * instead of draining every 100 ms slice.
 *
 *   W1  a write on the data fd wakes a blocked wait(-1) as Data
 *   W2  wake() unblocks a blocked wait(-1) as Stop
 *   W3  a wait with a timeout and no events returns Timeout
 *   W4  init(-1) (stop/deadline-only mode) still stops
 *   W5  a wake() issued before wait() still yields Stop (level-triggered fd)
 *
 * No eBPF is required: any readable fd stands in for the ring-buffer epoll
 * fd, which is exactly how the watcher hands it over.
 */
#include "job_watcher/rb_event_wait.hpp"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

void pass_case(const char* id, const char* description)
{
    std::cout << "PASS " << id << " " << description << '\n' << std::flush;
}

void notify_fd(int fd)
{
    const std::uint64_t one = 1;
    check(write(fd, &one, sizeof(one)) == sizeof(one), "test fd write failed");
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string base =
        argc > 1 ? std::string(argv[1]) : std::string("/tmp/joblens-rbevent-test");
    std::filesystem::create_directories(base);
    try {
        // W1 + W2 + W3: data wakeups, stop wakeups, timeouts.
        {
            const int data_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            check(data_fd >= 0, "data eventfd creation failed");

            RbEventWait wait;
            check(wait.init(data_fd), "init with a data fd failed");
            check(wait.ready(), "ready() false after successful init");

            std::thread waker([&data_fd]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                notify_fd(data_fd);
            });
            const auto data_rc = wait.wait(-1);
            waker.join();
            check(data_rc == RbEventWait::Wakeup::Data,
                  "W1: a data-fd write must wake a blocked wait");

            // The stand-in eventfd keeps its counter until read; a real ring
            // buffer goes quiet after ring_buffer__consume(). Drain here so
            // the later waits block like the watcher's would.
            check(wait.wait(0) == RbEventWait::Wakeup::Data,
                  "W1b: pending data must be reported without blocking");
            std::uint64_t drained = 0;
            check(read(data_fd, &drained, sizeof(drained)) == sizeof(drained),
                  "test data-fd drain failed");

            std::thread stopper([&wait]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                wait.wake();
            });
            const auto stop_rc = wait.wait(-1);
            stopper.join();
            check(stop_rc == RbEventWait::Wakeup::Stop,
                  "W2: wake() must unblock a blocked wait");

            wait.close();
            check(!wait.ready(), "ready() true after close");
            close(data_fd);
            pass_case("W1W2", "data and stop wakeups behave");
        }

        // W3: timeout on a quiet, fresh instance (stop fd unread).
        {
            RbEventWait wait;
            check(wait.init(-1), "init without a data fd failed");
            check(wait.wait(20) == RbEventWait::Wakeup::Timeout,
                  "W3: a quiet wait must time out");
        }

        // W4: stop-only mode (no ring buffer configured).
        {
            RbEventWait wait;
            check(wait.init(-1), "init without a data fd failed");
            std::thread stopper([&wait]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                wait.wake();
            });
            const auto stop_rc = wait.wait(-1);
            stopper.join();
            check(stop_rc == RbEventWait::Wakeup::Stop,
                  "W4: stop-only mode must still stop");
        }

        // W5: a stop signal that arrived before the wait is not lost.
        {
            RbEventWait wait;
            check(wait.init(-1), "init without a data fd failed");
            wait.wake();
            wait.wake();   // idempotent
            check(wait.wait(-1) == RbEventWait::Wakeup::Stop,
                  "W5: a pre-wait wake must still stop the next wait");
            wait.close();
            pass_case("W4W5", "stop-only mode and pre-wait wakes behave");
        }

        std::cout << "ALL PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
