#include "writer/es_writer.hpp"
#include "core/output_record.h"
#include "thirdparty/httplib.h"
#include <iostream>
#include <condition_variable>
#include <mutex>
#include <thread>

using json = nlohmann::json;

namespace {
std::mutex curl_fault_mutex;
std::condition_variable curl_fault_cv;
bool curl_fault_armed = false;
bool curl_fault_reached = false;
bool curl_fault_release = false;
bool curl_fault_thrown = false;
int curl_fault_calls = 0;
}

extern "C" CURLcode __real_curl_easy_perform(CURL* handle);
extern "C" CURLcode __wrap_curl_easy_perform(CURL* handle) {
    std::unique_lock<std::mutex> lock(curl_fault_mutex);
    if (curl_fault_armed && ++curl_fault_calls == 2) {
        curl_fault_reached = true;
        curl_fault_cv.notify_all();
        curl_fault_cv.wait_for(lock, std::chrono::seconds(5), [] { return curl_fault_release; });
        curl_fault_armed = false;
        curl_fault_thrown = true;
        throw std::runtime_error("injected retry transport exception");
    }
    lock.unlock();
    return __real_curl_easy_perform(handle);
}

struct TestWriter : ESWriter {
    using ESWriter::ESWriter;
    using ESWriter::on_flush_error;
};

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    auto& config = Config::instance(argv[1]);
    spdlog::set_level(spdlog::level::warn);
    httplib::Server server;
    std::mutex mutex;
    std::vector<std::string> requests;
    std::vector<std::chrono::steady_clock::time_point> times;
    std::string scenario;
    int forced_status = 0;
    std::condition_variable barrier;
    int blocked_attempt = 0;
    int released_attempt = 0;
    server.Get("/", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("{}", "application/json");
    });
    server.Post("/_bulk", [&](const httplib::Request& req, httplib::Response& res) {
        std::unique_lock<std::mutex> lock(mutex);
        requests.push_back(req.body);
        times.push_back(std::chrono::steady_clock::now());
        const auto attempt = requests.size();
        if ((scenario == "barrier" && (attempt == 2 || attempt == 3))
            || (scenario == "exception" && attempt == 2)) {
            blocked_attempt = static_cast<int>(attempt);
            barrier.notify_all();
            barrier.wait_for(lock, std::chrono::seconds(5), [&] { return released_attempt >= static_cast<int>(attempt); });
        }
        if (scenario == "timeout" && attempt == 1) std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        if (forced_status != 0) {
            res.status = forced_status;
            res.set_content("rejected", "text/plain");
            return;
        }
        if (scenario == "permanent_retry" && attempt > 1) {
            res.status = 400;
            res.set_content("rejected", "text/plain");
            return;
        }
        if (scenario == "503" || ((scenario == "recover" || scenario == "permanent_retry" || scenario == "exception") && attempt == 1)
            || ((scenario == "recover2" || scenario == "barrier") && attempt <= 2)) {
            res.status = 503;
            res.set_content("unavailable", "text/plain");
            return;
        }
        if (scenario == "413") {
            res.status = 413;
            res.set_content("too large", "text/plain");
            return;
        }
        if (scenario == "malformed" && attempt == 1) {
            res.set_content("not json", "text/plain");
            return;
        }
        json items = json::array();
        std::istringstream input(req.body);
        std::string action, source;
        while (std::getline(input, action) && std::getline(input, source)) {
            const auto meta = json::parse(action)["index"];
            const auto data = json::parse(source);
            int status = 201;
            const int value = data["data"]["value"].get<int>();
            if (scenario == "mixed" && attempt == 1) status = value == 1 ? 429 : value == 2 ? 400 : 201;
            if (scenario == "odd2xx" && attempt == 1) status = value == 1 ? 202 : value == 2 ? 204 : 201;
            json item{{"_id", meta["_id"]}, {"_index", meta["_index"]}, {"status", status}};
            if (status >= 400) item["error"] = {{"type", status == 429 ? "es_rejected_execution_exception" : "mapper_parsing_exception"}, {"reason", "模拟错误"}};
            items.push_back({{"index", item}});
        }
        if (scenario == "missing" && attempt == 1) items = json::array();
        if (scenario == "baditem" && attempt == 1) items[1] = {{"index", {{"status", "bad"}}}};
        res.set_content(json{{"errors", scenario == "mixed" && attempt == 1}, {"items", items}}.dump(), "application/json");
    });
    const int port = server.bind_to_any_port("127.0.0.1");
    check(port > 0, "无法绑定本地端口");
    config.getRawNode("es_test")["port"] = port;
    std::thread serving([&] { server.listen_after_bind(); });
    Job job{};
    job.JobID = 123;
    job.JobNativeID = "123";
    job.clusterName = "test";
    job.clusterTag = "test";
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    auto make_record = [](const Job& job, int i, json value) {
        auto sample = std::make_shared<SampleMetadata>();
        sample->sample_id = "run";
        sample->collector_name = "retry_test";
        sample->collector_type = "RetryTestCollector";
        sample->job = job;
        sample->sampled_at = std::chrono::system_clock::time_point(std::chrono::seconds(1700000000 + i));
        auto record = std::make_shared<OutputRecord>();
        record->sample = sample;
        record->channel = "document";
        record->sequence = static_cast<uint64_t>(i);
        record->record_id = "run:" + std::to_string(i);
        record->payload = Document{std::move(value)};
        record->accounted_bytes = estimate_output_bytes(record->payload);
        return record;
    };
    std::vector<OutputRecordPtr> batch;
    for (int i = 0; i < 3; ++i) batch.push_back(make_record(job, i, json{{"value", i}, {"text", "中文\n\""}}));
    int result = 0;
    try {
        auto run = [&](const std::string& mode, int retries) {
            scenario = mode;
            requests.clear();
            times.clear();
            config.getRawNode("es_test")["max_retries"] = retries;
            TestWriter writer("test", "ESWriter", "es_test");
            writer.set_perf(true, 32);
            const bool ok = writer.flush_impl(batch);
            if (!ok) writer.on_flush_error(batch);
            writer.shutdown();
            return ok;
        };
        check(!run("mixed", 2), "HTTP 200 部分失败被误判成功");
        check(requests.size() == 2, "失败项未重试或永久错误重复发送");
        check(requests[1].find("\"value\":1") != std::string::npos && requests[1].find("\"value\":0") == std::string::npos
            && requests[1].find("\"value\":2") == std::string::npos, "重试包含成功或永久失败项");
        check(!run("503", 2) && requests.size() == 3, "重试次数不受限");
        check(requests[0] == requests[1] && requests[1] == requests[2], "重试改变了序列化文档");
        check(times[1] - times[0] >= std::chrono::milliseconds(10)
            && times[2] - times[1] >= std::chrono::milliseconds(20), "未执行指数退避");
        check(!run("413", 2) && requests.size() == 1, "413 被盲目重试");
        check(!run("recover", 2) && requests.size() == 2, "临时错误未恢复");
        for (const auto& mode : {"malformed", "missing", "baditem"}) {
            check(!run(mode, 2) && requests.size() == 2, "异常响应未按未知结果重试");
        }
        check(!run("503", 0) && requests.size() == 1, "零次重试未生效");
        check(run("success", 2) && requests.size() == 1, "成功请求重复发送");
        for (int status : {400, 401, 403, 404, 409}) {
            forced_status = status;
            check(!run("permanent", 2) && requests.size() == 1, "永久 HTTP 错误被重试");
        }
        forced_status = 0;
        check(!run("odd2xx", 2) && requests.size() == 2, "非预期 2xx 被误判成功");
        check(requests[1].find("\"value\":0") == std::string::npos, "异常 2xx 导致成功项重发");
        check(!run("timeout", 2) && requests.size() == 2 && requests[0] == requests[1], "超时未按原文重试");
        run("success", 2);
        const auto first_end = requests[0].find('\n', requests[0].find('\n') + 1) + 1;
        const int one_document_bytes = static_cast<int>(first_end);
        config.getRawNode("es_test")["max_bulk_bytes"] = one_document_bytes;
        check(!run("503", 2) && requests.size() == 9, "重试分包未遵守字节预算");
        for (const auto& request : requests) check(request.size() <= first_end, "请求超过字节预算");
        scenario = "success";
        requests.clear();
        TestWriter oversized("oversized", "ESWriter", "es_test");
        oversized.set_perf(false, 0);
        auto huge = make_record(job, 9, json{{"value", 9}, {"text", std::string(2000, 'x')}});
        check(!oversized.flush_impl({huge}), "超限文档误判成功");
        oversized.on_flush_error({huge});
        oversized.shutdown();
        check(requests.empty(), "超限文档被重试");
        config.getRawNode("es_test")["max_bulk_bytes"] = 1048576;
        // 预算只需容纳实际同时存活的重试副本：连续两轮 503 后第三轮成功，
        // 每个存活副本在重试期间只允许占用一份字节预算。
        scenario = "success";
        requests.clear();
        run("success", 2);
        const auto batch_bytes = requests[0].size();
        config.getRawNode("writers_config")["max_pending_bytes"] = static_cast<int>(batch_bytes);
        config.getRawNode("writers_config")["max_record_bytes"] = static_cast<int>(batch_bytes);
        scenario = "recover2";
        requests.clear();
        times.clear();
        {
            TestWriter flaky("flaky", "ESWriter", "es_test");
            flaky.set_perf(false, 0);
            const bool first_ok = flaky.flush_impl(batch);
            check(!first_ok, "首轮 503 误判成功");
            flaky.on_flush_error(batch);
            check(requests.size() == 3, "连续两轮 503 后必须重试到第三轮成功");
            check(requests[1] == requests[0] && requests[2] == requests[0], "重试改变了序列化文档");
            flaky.shutdown();
        }
        std::cout << "PASS: the retry budget tracks only the live retry set\n";
        // Exercise BaseWriter's actual worker and admission accounting while the
        // HTTP server holds each retry in flight. The original record, encoded
        // retry and producer are all simultaneously live at the barrier.
        scenario = "barrier";
        requests.clear();
        times.clear();
        blocked_attempt = released_attempt = 0;
        auto original = batch[0];
        auto producer = make_record(job, 100, json{{"value", 100}});
        producer->accounted_bytes = 11;
        const auto limit = original->accounted_bytes + first_end + 10;
        config.getRawNode("writers_config")["max_pending_bytes"] = static_cast<int>(limit);
        config.getRawNode("writers_config")["max_record_bytes"] = static_cast<int>(limit);
        bool reached[2] = {false, false};
        bool charged[2] = {false, false};
        bool rejected[2] = {false, false};
        json final_status;
        {
            TestWriter concurrent("concurrent", "ESWriter", "es_test");
            check(concurrent.try_enqueue(original) == EnqueueCode::Accepted, "initial enqueue failed");
            for (int round = 0; round < 2; ++round) {
                const int attempt = round + 2;
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    reached[round] = barrier.wait_for(lock, std::chrono::seconds(4), [&] { return blocked_attempt >= attempt; });
                }
                if (reached[round]) {
                    charged[round] = concurrent.output_status()["pending_bytes"] == original->accounted_bytes + first_end;
                    rejected[round] = concurrent.try_enqueue(producer) == EnqueueCode::QueueFull;
                }
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    released_attempt = attempt;
                }
                barrier.notify_all();
            }
            concurrent.shutdown();
            final_status = concurrent.output_status();
        }
        check(reached[0] && reached[1], "retry HTTP barriers not reached");
        check(charged[0] && charged[1], "live retry bytes released during HTTP request");
        check(rejected[0] && rejected[1], "producer admitted while retry occupies its budget");
        check(final_status["pending_bytes"] == 0 && final_status["pending_records"] == 0
              && final_status["accepted"] == final_status["released_records"], "queue accounting not conserved");
        check(requests.size() == 3 && requests[0] == requests[1] && requests[1] == requests[2],
              "retry rounds did not preserve the document");
        std::cout << "PASS: in-flight retries retain budget against concurrent producers\n";
        config.getRawNode("writers_config")["max_pending_bytes"] = 67108864;
        config.getRawNode("writers_config")["max_record_bytes"] = 8388608;
        // Throw on the retry transport call, after the 503 created a real
        // reservation. Hold the next producer's HTTP call while examining the
        // accounting after the original worker buffer has been released.
        scenario = "exception";
        requests.clear();
        times.clear();
        blocked_attempt = released_attempt = 0;
        auto survivor = make_record(job, 101, json{{"value", 101}});
        survivor->accounted_bytes = 4096;
        bool fault_reached = false;
        bool retry_charged = false;
        bool survivor_accepted = false;
        bool producer_blocked = false;
        bool fault_thrown = false;
        json intermediate, exception_final;
        {
            TestWriter exceptional("exceptional", "ESWriter", "es_test");
            {
                std::lock_guard<std::mutex> lock(curl_fault_mutex);
                curl_fault_armed = true;
                curl_fault_reached = curl_fault_release = curl_fault_thrown = false;
                curl_fault_calls = 0;
            }
            const auto first_admission = exceptional.try_enqueue(original);
            {
                std::unique_lock<std::mutex> lock(curl_fault_mutex);
                fault_reached = curl_fault_cv.wait_for(lock, std::chrono::seconds(4), [] { return curl_fault_reached; });
            }
            if (fault_reached) {
                retry_charged = exceptional.output_status()["pending_bytes"] == original->accounted_bytes + first_end;
                survivor_accepted = exceptional.try_enqueue(survivor) == EnqueueCode::Accepted;
            }
            {
                std::lock_guard<std::mutex> lock(curl_fault_mutex);
                curl_fault_release = true;
            }
            curl_fault_cv.notify_all();
            {
                std::unique_lock<std::mutex> lock(mutex);
                producer_blocked = barrier.wait_for(lock, std::chrono::seconds(4), [&] { return blocked_attempt == 2; });
            }
            if (producer_blocked) intermediate = exceptional.output_status();
            {
                std::lock_guard<std::mutex> lock(mutex);
                released_attempt = 2;
            }
            barrier.notify_all();
            exceptional.shutdown();
            exception_final = exceptional.output_status();
            {
                std::lock_guard<std::mutex> lock(curl_fault_mutex);
                fault_thrown = curl_fault_thrown;
                curl_fault_armed = false;
            }
            check(first_admission == EnqueueCode::Accepted, "exception test initial enqueue failed");
        }
        check(fault_reached && fault_thrown && retry_charged, "exception was not injected with a live retry reservation");
        check(survivor_accepted && producer_blocked, "producer was not retained across the retry exception");
        check(intermediate["pending_bytes"] == survivor->accounted_bytes
              && intermediate["pending_records"] == 1 && intermediate["accepted"] == 2
              && intermediate["released_records"] == 1,
              "retry exception stole producer bytes or violated queue conservation");
        check(exception_final["pending_bytes"] == 0 && exception_final["pending_records"] == 0
              && exception_final["accepted"] == 2 && exception_final["released_records"] == 2,
              "exception test did not finish with a conserved queue");
        check(requests.size() == 2 && requests[1].find("\"value\":101") != std::string::npos,
              "producer did not reach the actual HTTP endpoint after the exception");
        std::cout << "PASS: post-reservation exception preserves the pending producer's exact byte charge\n";
        for (const auto& terminal : {"503", "permanent_retry", "encoding"}) {
            scenario = terminal;
            requests.clear();
            times.clear();
            TestWriter terminal_writer("terminal", "ESWriter", "es_test");
            auto record = scenario == "encoding"
                ? make_record(job, 20, json{{"value", 20}, {"text", std::string("\xff", 1)}})
                : make_record(job, 20, json{{"value", 20}});
            check(terminal_writer.try_enqueue(record) == EnqueueCode::Accepted, "terminal record not admitted");
            terminal_writer.shutdown();
            const auto status = terminal_writer.output_status();
            check(status["pending_bytes"] == 0 && status["pending_records"] == 0
                  && status["accepted"] == 1 && status["released_records"] == 1,
                  "terminal outcome leaked or double-released queue budget");
            const std::size_t expected = scenario == "503" ? 3 : scenario == "permanent_retry" ? 2 : 0;
            check(requests.size() == expected, "terminal outcome made an unexpected number of requests");
        }
        std::cout << "PASS: exhaustion, permanent retry failure and encoding failure conserve the queue\n";
        for (const auto& invalid : std::vector<std::pair<std::string, int>>{{"max_retries", -1}, {"max_retries", 11},
                {"retry_initial_backoff_ms", 0}, {"retry_max_backoff_ms", 1}, {"write_timeout", 0}}) {
            auto node = config.getRawNode("es_test");
            const int saved = node[invalid.first].as<int>();
            node[invalid.first] = invalid.second;
            bool rejected = false;
            try { TestWriter bad("bad", "ESWriter", "es_test"); }
            catch (const std::invalid_argument&) { rejected = true; }
            node[invalid.first] = saved;
            check(rejected, "非法配置未拒绝");
        }
        scenario = "recover";
        requests.clear();
        times.clear();
        config.getRawNode("es_test")["max_retries"] = 2;
        TestWriter worker("worker", "ESWriter", "es_test");
        worker.set_perf(true, 32);
        worker.try_enqueue(make_record(job, 1, json{{"value", 1}}));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (worker.get_perf_snapshot()->call_cnt == 0 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        worker.shutdown();
        check(requests.size() == 2, "基类钩子没有重试");
        check(worker.get_perf_snapshot()->call_cnt == 1, "writer call_cnt 重复计数");
        check(worker.get_perf_snapshot()->err_cnt == 1, "初始 flush 失败统计错误");
        std::cout << "PASS: partial errors, selective retries, stable NDJSON, backoff, exhaustion, permanent errors, malformed responses, hook and call_cnt\n";
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        result = 1;
    }
    server.stop();
    serving.join();
    return result;
}
