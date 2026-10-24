#include "output_test_support.hpp"

#include <atomic>
#include <numeric>
#include <thread>

using namespace output_test;

namespace {

void set_limit(const char* key, int value) {
    Config::instance().getRawNode("sink_cfg")[key] = value;
}

void accepted_and_delivered() {
    NativeSink sink("sink");
    auto record = make_record("text", Text{"hello\n"});
    const auto id = record->record_id;
    check(sink.try_enqueue(record) == EnqueueCode::Accepted, "record was not accepted");
    const auto delivered = sink.wait_delivered(1);
    check(delivered.size() == 1, "accepted record was not delivered");
    check(delivered[0]->record_id == id, "record identity changed in flight");
    check(std::get<Text>(delivered[0]->payload).value == "hello\n", "payload changed in flight");
    sink.shutdown();
    const auto status = sink.output_status();
    check(status["accepted"] == 1, "accepted counter");
    check(status["released_records"] == 1, "released counter");
    check(status["pending_records"] == 0, "pending records did not drain");
    check(status["accepted"] == status["pending_records"].get<int>() + status["released_records"].get<int>(),
          "accepted != pending + released");
}

void immutable_fan_out() {
    NativeSink first("first");
    NativeSink second("second");
    auto record = make_record("document", Document{nlohmann::json{{"value", 7}}});
    check(first.try_enqueue(record) == EnqueueCode::Accepted, "first enqueue");
    check(second.try_enqueue(record) == EnqueueCode::Accepted, "second enqueue");
    record.reset();
    const auto a = first.wait_delivered(1);
    const auto b = second.wait_delivered(1);
    check(a.size() == 1 && b.size() == 1, "fan-out lost a destination");
    check(a[0]->record_id == b[0]->record_id, "fan-out changed record identity");
    check(a[0]->payload == b[0]->payload, "fan-out changed payload");
    check(a[0]->sample == b[0]->sample, "fan-out did not share immutable sample metadata");
    first.shutdown();
    second.shutdown();
}

void queue_full_boundary() {
    set_limit("max_pending_records", 2);
    set_limit("max_pending_bytes", 1 << 20);
    set_limit("max_record_bytes", 1 << 19);
    NativeSink sink("bounded");
    sink.block(true);
    check(sink.try_enqueue(make_record("text", Text{"a"})) == EnqueueCode::Accepted, "record 1");
    check(sink.try_enqueue(make_record("text", Text{"b"})) == EnqueueCode::Accepted, "record 2");
    check(sink.try_enqueue(make_record("text", Text{"c"})) == EnqueueCode::QueueFull, "record 3 must be rejected");
    sink.block(false);
    sink.shutdown();
    check(sink.delivered_count() == 2, "rejected record was delivered");
    check(sink.output_status()["rejected_full"] == 1, "rejected_full counter");
}

void too_large_and_unsupported() {
    set_limit("max_pending_records", 16);
    set_limit("max_pending_bytes", 1 << 20);
    set_limit("max_record_bytes", 512);
    NativeSink sink("limits");
    std::string huge(2048, 'x');
    check(sink.try_enqueue(make_record("text", Text{huge})) == EnqueueCode::TooLarge, "oversized record accepted");
    sink.accept_all = false;
    check(sink.try_enqueue(make_record("metrics", MetricBatch{})) == EnqueueCode::Unsupported,
          "unsupported payload accepted");
    sink.shutdown();
    const auto status = sink.output_status();
    check(status["rejected_too_large"] == 1, "rejected_too_large counter");
    check(status["rejected_invalid"] == 1, "rejected_invalid counter");
}

void closed_after_shutdown() {
    NativeSink sink("closed");
    sink.shutdown();
    check(sink.try_enqueue(make_record("text", Text{"x"})) == EnqueueCode::Closed, "closed writer accepted a record");
    check(sink.output_status()["rejected_closed"] == 1, "rejected_closed counter");
}

void concurrent_delivery() {
    set_limit("max_pending_records", 100000);
    set_limit("max_pending_bytes", 1 << 30);
    set_limit("max_record_bytes", 1 << 20);
    NativeSink sink("concurrent");
    constexpr int producers = 8;
    constexpr int per_producer = 250;
    std::vector<std::thread> threads;
    std::atomic<int> accepted{0}, rejected{0};
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            for (int i = 0; i < per_producer; ++i) {
                auto record = make_record("text", Text{std::to_string(p) + ":" + std::to_string(i)},
                                          "collector", 1, "run:" + std::to_string(p * per_producer + i));
                if (sink.try_enqueue(record) == EnqueueCode::Accepted) ++accepted;
                else ++rejected;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    sink.shutdown();
    check(rejected == 0, "concurrent enqueue rejected a record below the limit");
    check(accepted == producers * per_producer, "concurrent enqueue lost an accepted count");
    check(sink.delivered_count() == static_cast<size_t>(accepted), "delivered count differs from accepted");
    std::vector<std::string> ids;
    for (const auto& record : sink.delivered()) ids.push_back(record->record_id);
    std::sort(ids.begin(), ids.end());
    check(std::adjacent_find(ids.begin(), ids.end()) == ids.end(), "duplicate delivered record identity");
}

void context_emit_and_disable() {
    // 无路由的上下文：未知通道 -> Invalid；采样契约错误可被观测。
    auto router = std::make_shared<OutputRouter>();
    OutputContext context(router, make_sample("collector", "TestCollector"));
    const auto report = context.emit("text", Text{"x"});
    check(report.code == EmitCode::Invalid, "unknown channel must be invalid");
    check(context.had_error(), "invalid emit was not recorded");
    check(!context.enabled("text"), "unknown channel reported enabled");
    check(context.submitted() == 0, "invalid emit counted as submitted");
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    spdlog::set_level(spdlog::level::off);
    Config::instance(argv[1]);
    int failures = 0;
    for (const auto& test : std::vector<std::pair<const char*, void(*)()>>{
             {"accepted record delivered", accepted_and_delivered},
             {"immutable fan-out", immutable_fan_out},
             {"queue full boundary", queue_full_boundary},
             {"too large and unsupported", too_large_and_unsupported},
             {"closed after shutdown", closed_after_shutdown},
             {"concurrent delivery", concurrent_delivery},
             {"context emit and disable", context_emit_and_disable}}) {
        try {
            set_limit("max_pending_records", 4096);
            set_limit("max_pending_bytes", 67108864);
            set_limit("max_record_bytes", 8388608);
            test.second();
            std::cout << "PASS: " << test.first << '\n';
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "FAIL: " << test.first << ": " << error.what() << '\n';
        }
    }
    return failures == 0 ? 0 : 1;
}
