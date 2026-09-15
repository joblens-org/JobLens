#include "writer/es_writer.hpp"
#include "core/collector_registry.hpp"
#include "thirdparty/httplib.h"
#include <iostream>
#include <mutex>
#include <thread>

using json = nlohmann::json;

struct TestCollector : ICollector {
    bool init(const json&) override { return true; }
    void deinit() noexcept override {}
    CollectDataParseFunc get_writer_parser(const std::string&) override {
        return [](std::any data) -> std::any { return std::any_cast<json>(data); };
    }
};

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
    server.Get("/", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("{}", "application/json");
    });
    server.Post("/_bulk", [&](const httplib::Request& req, httplib::Response& res) {
        std::lock_guard<std::mutex> lock(mutex);
        requests.push_back(req.body);
        times.push_back(std::chrono::steady_clock::now());
        const auto attempt = requests.size();
        if (scenario == "timeout" && attempt == 1) std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        if (forced_status != 0) {
            res.status = forced_status;
            res.set_content("rejected", "text/plain");
            return;
        }
        if (scenario == "503" || (scenario == "recover" && attempt == 1)) {
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
    auto& registry = CollectorRegistry::instance();
    registry.registerCollector<TestCollector>("RetryTestCollector", CollectorScope::Job, "重试验证");
    registry.createCollector("RetryTestCollector", "retry_test");
    Job job{};
    job.JobID = 123;
    job.NativeJobID = "123";
    job.cluster_name = "test";
    job.clusterTag = "test";
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    std::vector<write_data> batch;
    for (int i = 0; i < 3; ++i) batch.emplace_back("retry_test", job, json{{"value", i}, {"text", "中文\n\""}},
        std::chrono::system_clock::time_point(std::chrono::seconds(1700000000 + i)));
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
        auto huge = write_data{"retry_test", job, json{{"value", 9}, {"text", std::string(2000, 'x')}},
            std::chrono::system_clock::now()};
        check(!oversized.flush_impl({huge}), "超限文档误判成功");
        oversized.on_flush_error({huge});
        oversized.shutdown();
        check(requests.empty(), "超限文档被重试");
        config.getRawNode("es_test")["max_bulk_bytes"] = 1048576;
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
        worker.on_finish("retry_test", job, json{{"value", 1}}, std::chrono::system_clock::now());
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
