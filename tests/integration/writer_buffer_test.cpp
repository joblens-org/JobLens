#include "writer/base_writer.hpp"
#include "core/output_record.h"
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <future>
#include <iostream>
#include <numeric>
#include <stdexcept>

namespace {
void check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

OutputRecordPtr make_record(int id, std::string text = {}) {
    auto sample = std::make_shared<SampleMetadata>();
    sample->sample_id = "run";
    sample->collector_name = "counted";
    sample->collector_type = "TestCollector";
    sample->job.JobID = static_cast<uint64_t>(id);
    sample->sampled_at = std::chrono::system_clock::now();
    auto record = std::make_shared<OutputRecord>();
    record->sample = sample;
    record->channel = "text";
    record->sequence = static_cast<uint64_t>(id);
    record->record_id = "run:" + std::to_string(id);
    record->payload = Text{text.empty() ? std::to_string(id) : std::move(text)};
    record->accounted_bytes = estimate_output_bytes(record->payload);
    return record;
}

struct DeliveryState {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::string> delivered;
    bool block = false;
    bool entered = false;
    int shutdown_calls = 0;
};

class RecordingWriter : public BaseWriter {
public:
    explicit RecordingWriter(DeliveryState& state)
        : BaseWriter("recording", "test", "test"), state_(state) { set_perf(false, 0); }
    ~RecordingWriter() override { shutdown(); }
    void do_shutdown() override { ++state_.shutdown_calls; }
    bool accepts(OutputKind) const override { return true; }

protected:
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override {
        std::unique_lock lock(state_.mutex);
        state_.entered = true;
        state_.cv.notify_all();
        state_.cv.wait(lock, [&] { return !state_.block; });
        for (const auto& record : batch) {
            check(record->sample && record->sample->job.JobID == static_cast<uint64_t>(std::stoi(
                      std::get<Text>(record->payload).value)),
                  "buffer changed a record's job or payload");
            state_.delivered.push_back(std::get<Text>(record->payload).value);
        }
        state_.cv.notify_all();
        return true;
    }

private:
    DeliveryState& state_;
};

void owned_record_survives_producer() {
    for (int path = 0; path < 2; ++path) {
        DeliveryState state;
        RecordingWriter writer(state);
        auto record = make_record(7);
        const auto id = record->record_id;
        const auto status = writer.try_enqueue(record);
        check(status == EnqueueCode::Accepted, "record was not accepted");
        record.reset();
        {
            std::unique_lock lock(state.mutex);
            check(state.cv.wait_for(lock, std::chrono::seconds(2), [&] { return state.delivered.size() == 1; }),
                  "one queued record did not flush immediately");
        }
        writer.shutdown();
        check(state.delivered == std::vector<std::string>{"7"}, "one record was lost or duplicated");
        (void)id;
        (void)path;
    }
}

void concurrent_delivery() {
    DeliveryState state;
    RecordingWriter writer(state);
    std::vector<std::thread> producers;
    constexpr int producer_count = 8;
    constexpr int per_producer = 300;
    std::atomic<int> rejected{0};
    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (int i = 0; i < per_producer; ++i) {
                const int id = producer * per_producer + i;
                if (writer.try_enqueue(make_record(id)) != EnqueueCode::Accepted) ++rejected;
            }
        });
    }
    for (auto& thread : producers) thread.join();
    writer.shutdown();
    check(rejected == 0, "concurrent delivery rejected a record below the limit");
    std::sort(state.delivered.begin(), state.delivered.end(),
              [](const std::string& a, const std::string& b) { return std::stoi(a) < std::stoi(b); });
    std::vector<std::string> expected(producer_count * per_producer);
    for (int i = 0; i < producer_count * per_producer; ++i) expected[i] = std::to_string(i);
    check(state.delivered == expected, "concurrent delivery lost or duplicated records");
}

void shutdown_drains_once() {
    DeliveryState state;
    state.block = true;
    RecordingWriter writer(state);
    check(writer.try_enqueue(make_record(0)) == EnqueueCode::Accepted, "first enqueue");
    {
        std::unique_lock lock(state.mutex);
        if (!state.cv.wait_for(lock, std::chrono::seconds(2), [&] { return state.entered; })) {
            state.block = false;
            state.cv.notify_all();
            throw std::runtime_error("flush worker did not start");
        }
    }
    for (int id = 1; id <= 400; ++id)
        check(writer.try_enqueue(make_record(id)) == EnqueueCode::Accepted, "drain enqueue");
    auto stopping = std::async(std::launch::async, [&] { writer.shutdown(); });
    const bool waited_for_active_flush =
        stopping.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
    {
        std::lock_guard lock(state.mutex);
        state.block = false;
    }
    state.cv.notify_all();
    stopping.get();
    check(waited_for_active_flush, "shutdown returned before the active flush finished");
    writer.shutdown();
    // 停止后的提交必须被拒绝，而不是静默保留。
    check(writer.try_enqueue(make_record(999)) == EnqueueCode::Closed,
          "a stopped writer accepted new work");
    writer.shutdown();
    check(state.delivered.size() == 401, "shutdown lost, replayed, or accepted records after stopping");
    check(state.shutdown_calls == 1, "shutdown invoked backend cleanup more than once");
    const auto status = writer.output_status();
    check(status["rejected_closed"] == 1, "closed rejection counter");
    check(status["accepted"] == 401, "accepted counter");
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    spdlog::set_level(spdlog::level::off);
    Config::instance(argv[1]);
    int failures = 0;
    for (const auto& test : std::vector<std::pair<const char*, void(*)()>>{
            {"owned record survives producer", owned_record_survives_producer},
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
