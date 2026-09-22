#include "writer/base_writer.hpp"
#include <algorithm>
#include <future>
#include <iostream>
#include <numeric>
#include <stdexcept>

namespace {
void check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

struct CountedPayload {
    static std::atomic<int> copies;
    static std::function<void()> copy_hook;
    int value;
    explicit CountedPayload(int value) : value(value) {}
    CountedPayload(const CountedPayload& other) : value(other.value) {
        ++copies;
        if (copy_hook) copy_hook();
    }
    CountedPayload(CountedPayload&&) noexcept = default;
};
std::atomic<int> CountedPayload::copies{0};
std::function<void()> CountedPayload::copy_hook;

struct DeliveryState {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<int> delivered;
    bool block = false;
    bool entered = false;
    int shutdown_calls = 0;
};

class RecordingWriter : public BaseWriter {
public:
    explicit RecordingWriter(DeliveryState& state)
        : BaseWriter("recording", "test", "test"), state_(state) { set_perf(false, 0); }
    ~RecordingWriter() override { shutdown(); }
    using BaseWriter::write;
    void do_shutdown() override { ++state_.shutdown_calls; }
protected:
    bool flush_impl(const std::vector<write_data>& batch) override {
        std::unique_lock lock(state_.mutex);
        state_.entered = true;
        state_.cv.notify_all();
        state_.cv.wait(lock, [&] { return !state_.block; });
        for (const auto& [collector, job, data, ts] : batch) {
            const auto& payload = std::any_cast<const CountedPayload&>(data);
            check(collector == "counted" && job.JobID == static_cast<uint64_t>(payload.value),
                  "buffer changed a record's collector, job, or payload");
            state_.delivered.push_back(payload.value);
        }
        state_.cv.notify_all();
        return true;
    }
private:
    DeliveryState& state_;
};

Job job_for(int id) {
    Job job{};
    job.JobID = id;
    return job;
}

void copies_and_immediate_flush() {
    for (int path = 0; path < 3; ++path) {
        DeliveryState state;
        RecordingWriter writer(state);
        const Job job = job_for(7);
        const std::any payload = CountedPayload(7);
        const auto ts = std::chrono::system_clock::now();
        const write_data record{"counted", job, payload, ts};
        auto callback = writer.get_onFinishCallback();
        CountedPayload::copies = 0;
        if (path == 0) writer.on_finish("counted", job, payload, ts);
        if (path == 1) callback("counted", job, payload, ts);
        if (path == 2) writer.write(record);
        {
            std::unique_lock lock(state.mutex);
            check(state.cv.wait_for(lock, std::chrono::seconds(2), [&] { return state.delivered.size() == 1; }),
                  "one queued record did not flush immediately");
        }
        writer.shutdown();
        check(CountedPayload::copies == 1,
              "enqueue path " + std::to_string(path) + " copied an owned payload "
                  + std::to_string(CountedPayload::copies.load()) + " times instead of once");
        check(state.delivered == std::vector<int>{7}, "one record was lost or duplicated");
    }
}

void concurrent_delivery() {
    DeliveryState state;
    RecordingWriter writer(state);
    const auto callback = writer.get_onFinishCallback();
    std::vector<std::thread> producers;
    constexpr int producer_count = 8;
    constexpr int per_producer = 300;
    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (int i = 0; i < per_producer; ++i) {
                const int id = producer * per_producer + i;
                callback("counted", job_for(id), CountedPayload(id), std::chrono::system_clock::now());
            }
        });
    }
    for (auto& thread : producers) thread.join();
    writer.shutdown();
    std::sort(state.delivered.begin(), state.delivered.end());
    std::vector<int> expected(producer_count * per_producer);
    std::iota(expected.begin(), expected.end(), 0);
    check(state.delivered == expected, "concurrent delivery lost or duplicated records");
}

void shutdown_drains_once() {
    DeliveryState state;
    state.block = true;
    RecordingWriter writer(state);
    writer.on_finish("counted", job_for(0), CountedPayload(0), std::chrono::system_clock::now());
    {
        std::unique_lock lock(state.mutex);
        if (!state.cv.wait_for(lock, std::chrono::seconds(2), [&] { return state.entered; })) {
            state.block = false;
            state.cv.notify_all();
            throw std::runtime_error("flush worker did not start");
        }
    }
    for (int id = 1; id <= 400; ++id)
        writer.on_finish("counted", job_for(id), CountedPayload(id), std::chrono::system_clock::now());
    auto stopping = std::async(std::launch::async, [&] { writer.shutdown(); });
    const bool waited_for_active_flush = stopping.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
    {
        std::lock_guard lock(state.mutex);
        state.block = false;
    }
    state.cv.notify_all();
    stopping.get();
    check(waited_for_active_flush, "shutdown returned before the active flush finished");
    writer.shutdown();
    // Completion callbacks can race shutdown. A stopped writer must not retain new work.
    writer.on_finish("counted", job_for(999), CountedPayload(999), std::chrono::system_clock::now());
    writer.shutdown();
    std::vector<int> expected(401);
    std::iota(expected.begin(), expected.end(), 0);
    check(state.delivered == expected, "shutdown lost, replayed, or accepted records after stopping");
    check(state.shutdown_calls == 1, "shutdown invoked backend cleanup more than once");
}

}

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    spdlog::set_level(spdlog::level::off);
    Config::instance(argv[1]);
    int failures = 0;
    for (const auto& test : std::vector<std::pair<const char*, void(*)()>>{
            {"copies and immediate flush", copies_and_immediate_flush},
            {"concurrent delivery", concurrent_delivery},
            {"shutdown drains once", shutdown_drains_once}}) {
        try {
            test.second();
            std::cout << "PASS: " << test.first << '\n';
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "FAIL: " << test.first << ": " << error.what() << '\n';
        }
    }
    return failures == 0 ? 0 : 1;
}
