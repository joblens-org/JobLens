#include "core/collector_registry.hpp"
#include "writer/es_writer.hpp"
#include "writer/file_writer.hpp"
#include "writer/kafka_writer.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include "thirdparty/httplib.h"
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <sstream>

namespace {
using State = PrometheusExporterWriter::prometheus_job_state;
using Process = PrometheusExporterWriter::prometheus_process_state;

void check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

struct ParserCounts {
    std::string mode = "v2";
    int v2 = 0;
    int v1 = 0;
    int parsed = 0;
};
std::unordered_map<std::string, ParserCounts> counts;

struct BatchCollector : ICollector {
    bool init(const json& config) override {
        if (config.contains("reported_type")) set_type(config["reported_type"]);
        return true;
    }
    void deinit() noexcept override {}
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override {
        auto& counter = counts[get_name()];
        ++counter.v1;
        if (counter.mode != "v1") return nullptr;
        return [name = get_name(), writer_type](std::any data) -> std::any {
            ++counts[name].parsed;
            if (writer_type == "FileWriter")
                return "v1:" + std::to_string(std::any_cast<int>(data)) + "\n";
            return json{{"value", std::any_cast<int>(data)}, {"parser", "v1"}};
        };
    }
    CollectDataParseFuncV2 get_writer_parser_v2(const std::string& writer_type) override {
        auto& counter = counts[get_name()];
        ++counter.v2;
        if (counter.mode != "v2") return nullptr;
        return [name = get_name(), writer_type](const WriterParseContext& ctx, std::any data) -> std::any {
            ++counts[name].parsed;
            check(ctx.collector_name == name && ctx.writer_type == writer_type
                      && !ctx.writer_name.empty() && !ctx.writer_config_name.empty(),
                  "cached parser received the wrong per-record context");
            if (writer_type == "FileWriter")
                return "v2:" + std::to_string(ctx.job.JobID) + ":"
                    + std::to_string(std::any_cast<int>(data)) + "\n";
            if (writer_type == "PrometheusExporterWriter") return data;
            return json{{"value", std::any_cast<int>(data)}, {"parser", "v2"},
                        {"job_id", ctx.job.JobID},
                        {"timestamp", std::chrono::duration_cast<std::chrono::milliseconds>(
                            ctx.timestamp.time_since_epoch()).count()}};
        };
    }
};

void add_collector(const std::string& name, const std::string& mode,
                   const std::string& reported_type = "") {
    counts[name] = ParserCounts{mode};
    auto handle = CollectorRegistry::instance().createCollector("WriterBatchCollector", name);
    check(static_cast<bool>(handle.init), "test collector was not registered");
    check(handle.init(json{{"reported_type", reported_type}}), "test collector initialization failed");
}

write_data record(const std::string& collector, int id, std::any data) {
    Job job{};
    job.JobID = id;
    job.NativeJobID = std::to_string(id);
    job.cluster_name = "batch";
    job.clusterTag = "test";
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    return {collector, std::move(job), std::move(data),
            std::chrono::system_clock::time_point(std::chrono::milliseconds(1700000000000LL + id))};
}

class TestFileWriter : public FileWriter {
public:
    using FileWriter::FileWriter;
    using FileWriter::flush_impl;
};
class TestPromWriter : public PrometheusExporterWriter {
public:
    using PrometheusExporterWriter::PrometheusExporterWriter;
    using PrometheusExporterWriter::flush_impl;
};
class TestKafkaWriter : public KafkaWriter {
public:
    using KafkaWriter::KafkaWriter;
    using KafkaWriter::flush_impl;
};

void file_batch() {
    const std::string path = "/tmp/joblens-writer-batch-" + std::to_string(getpid()) + ".txt";
    unlink(path.c_str());
    Config::instance().getRawNode("es_test")["path"] = path;
    add_collector("file_v2", "v2");
    add_collector("file_v1", "v1");
    add_collector("file_raw", "missing");
    TestFileWriter writer("file_batch", "FileWriter", "es_test");
    check(writer.flush_impl({record("file_v2", 11, 1), record("file_v1", 11, 2),
                             record("file_raw", 11, std::string("raw-a\n")), record("file_v2", 12, 3),
                             record("file_v1", 12, 4), record("file_raw", 12, std::string("raw-b\n"))}),
          "file batch did not accept V2, V1, and raw-string records");
    check(counts["file_v2"].v2 == 1 && counts["file_v2"].v1 == 0 && counts["file_v2"].parsed == 2,
          "FileWriter resolved a V2 parser more than once in one batch");
    check(counts["file_v1"].v2 == 1 && counts["file_v1"].v1 == 1 && counts["file_v1"].parsed == 2,
          "FileWriter did not cache the explicit V1 fallback");
    check(counts["file_raw"].v2 == 1 && counts["file_raw"].v1 == 1,
          "FileWriter did not cache a missing parser");
    counts["file_raw"].mode = "v2";
    check(writer.flush_impl({record("file_raw", 13, 5)}), "next batch did not see the newly available parser");
    check(counts["file_raw"].v2 == 2 && counts["file_raw"].parsed == 1,
          "FileWriter retained a stale parser cache between batches");
    check(!writer.flush_impl({record("file_v2", 14, std::string("bad type")), record("file_v2", 15, 6)}),
          "a parser failure was not reported");
    check(counts["file_v2"].v2 == 2, "a parser exception discarded the cached parser");
    writer.shutdown();
    std::ifstream input(path);
    const std::string actual((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    unlink(path.c_str());
    check(actual == "v2:11:1\nv1:2\nraw-a\nv2:12:3\nv1:4\nraw-b\nv2:13:5\nv2:15:6\n",
          "FileWriter changed formatting, routing order, fallback, or error continuation");
}

class HttpFixture {
public:
    httplib::Server server;
    std::thread thread;
    std::vector<json> documents;
    std::mutex mutex;
    HttpFixture() {
        server.Get("/", [](const httplib::Request&, httplib::Response& response) {
            response.set_content("{}", "application/json");
        });
        server.Post("/_bulk", [&](const httplib::Request& request, httplib::Response& response) {
            std::lock_guard lock(mutex);
            json items = json::array();
            std::istringstream input(request.body);
            std::string action, source;
            while (std::getline(input, action) && std::getline(input, source)) {
                const auto metadata = json::parse(action)["index"];
                documents.push_back(json::parse(source));
                items.push_back({{"index", {{"_id", metadata["_id"]}, {"_index", metadata["_index"]}, {"status", 201}}}});
            }
            response.set_content(json{{"errors", false}, {"items", items}}.dump(), "application/json");
        });
        const int port = server.bind_to_any_port("127.0.0.1");
        check(port > 0, "could not bind the local ES fixture");
        Config::instance().getRawNode("es_test")["port"] = port;
        thread = std::thread([&] { server.listen_after_bind(); });
    }
    ~HttpFixture() { server.stop(); if (thread.joinable()) thread.join(); }
};

void es_batch() {
    HttpFixture fixture;
    add_collector("es_v2", "v2");
    add_collector("es_v1", "v1");
    add_collector("es_missing", "missing");
    ESWriter writer("es_batch", "ESWriter", "es_test");
    check(!writer.flush_impl({record("es_v2", 11, 1), record("es_v1", 11, 2), record("es_missing", 11, 0),
                              record("es_v2", 12, 3), record("es_v1", 12, 4), record("es_missing", 12, 0)}),
          "ESWriter did not report missing parsers");
    writer.shutdown();
    std::lock_guard lock(fixture.mutex);
    check(counts["es_v2"].v2 == 1 && counts["es_v2"].v1 == 0 && counts["es_v2"].parsed == 2,
          "ESWriter did not cache the V2 parser");
    check(counts["es_v1"].v2 == 1 && counts["es_v1"].v1 == 1 && counts["es_v1"].parsed == 2,
          "ESWriter did not cache the V1 fallback");
    check(counts["es_missing"].v2 == 1 && counts["es_missing"].v1 == 1,
          "ESWriter did not cache a missing parser");
    check(fixture.documents.size() == 4, "ESWriter lost or added serialized documents");
    check(fixture.documents[0]["data"] == json{{"value", 1}, {"parser", "v2"}, {"job_id", 11}, {"timestamp", 1700000000011LL}},
          "ESWriter changed its JSON data or per-record parser context");
    check(fixture.documents[1]["data"] == json{{"value", 2}, {"parser", "v1"}}, "ESWriter changed V1 data");
    check(fixture.documents[2]["data"]["value"] == 3 && fixture.documents[2]["data"]["job_id"] == 12,
          "ESWriter cached parsed data instead of the parser");
}

class KafkaFixture {
public:
    KafkaFixture() {
        // Fail cleanly when local binds are restricted; some librdkafka versions
        // assert inside mock-cluster construction if a broker cannot bind.
        const int probe = socket(AF_INET, SOCK_STREAM, 0);
        check(probe >= 0, "could not create the local Kafka fixture socket");
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        const int bound = bind(probe, reinterpret_cast<sockaddr*>(&address), sizeof(address));
        close(probe);
        check(bound == 0, "could not bind the local Kafka fixture socket");
        char error[512];
        auto* config = rd_kafka_conf_new();
        rd_kafka_conf_set(config, "log_level", "0", error, sizeof(error));
        broker_ = rd_kafka_new(RD_KAFKA_PRODUCER, config, error, sizeof(error));
        check(broker_ != nullptr, "could not create Kafka mock owner");
        cluster_ = rd_kafka_mock_cluster_new(broker_, 1);
        check(cluster_ != nullptr, "could not create the local Kafka mock cluster");
        brokers = rd_kafka_mock_cluster_bootstraps(cluster_);
        for (const auto* name : {"test_topic", "writer_batch_kafka_v2", "writer_batch_kafka_v1", "writer_batch_kafka_missing", "writer_batch_kafka_raii"})
            check(rd_kafka_mock_topic_create(cluster_, name, 1, 1) == RD_KAFKA_RESP_ERR_NO_ERROR,
                  "could not create a Kafka fixture topic");
        config = rd_kafka_conf_new();
        rd_kafka_conf_set(config, "bootstrap.servers", brokers.c_str(), error, sizeof(error));
        rd_kafka_conf_set(config, "log_level", "0", error, sizeof(error));
        consumer_ = rd_kafka_new(RD_KAFKA_CONSUMER, config, error, sizeof(error));
        check(consumer_ != nullptr, "could not create the Kafka fixture consumer");
    }
    ~KafkaFixture() {
        if (consumer_) rd_kafka_destroy(consumer_);
        if (cluster_) rd_kafka_mock_cluster_destroy(cluster_);
        if (broker_) rd_kafka_destroy(broker_);
    }
    std::vector<json> consume(const std::string& topic_name, std::size_t count) {
        auto* topic = rd_kafka_topic_new(consumer_, topic_name.c_str(), nullptr);
        check(topic != nullptr, "could not open a Kafka fixture topic");
        struct TopicGuard {
            rd_kafka_topic_t* topic;
            ~TopicGuard() { rd_kafka_consume_stop(topic, 0); rd_kafka_topic_destroy(topic); }
        } guard{topic};
        check(rd_kafka_consume_start(topic, 0, RD_KAFKA_OFFSET_BEGINNING) == 0, "could not start the Kafka fixture consumer");
        std::vector<json> result;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (result.size() < count && std::chrono::steady_clock::now() < deadline) {
            auto* message = rd_kafka_consume(topic, 0, 100);
            if (!message) continue;
            struct MessageGuard {
                rd_kafka_message_t* message;
                ~MessageGuard() { rd_kafka_message_destroy(message); }
            } release{message};
            if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) continue;
            check(message->err == RD_KAFKA_RESP_ERR_NO_ERROR, "Kafka fixture consume failed");
            result.push_back(json::parse(std::string(static_cast<const char*>(message->payload), message->len)));
        }
        check(result.size() == count, "KafkaWriter lost or did not deliver a message");
        return result;
    }
    std::string brokers;
private:
    rd_kafka_t* broker_ = nullptr;
    rd_kafka_t* consumer_ = nullptr;
    rd_kafka_mock_cluster_t* cluster_ = nullptr;
};

void configure_kafka(const std::string& brokers) {
    auto config = Config::instance().getRawNode("es_test");
    config["brokers"] = std::vector<std::string>{brokers};
    config["topic_prefix"] = "writer_batch_";
    config["topic_dlq"] = "writer_batch_dlq";
    config["client_id"] = "writer_batch_test";
    config["transactional_id"] = "writer_batch_test";
    config["batch_rows"] = 100;
    config["linger_ms"] = 0;
    config["enable_transaction"] = false;
    config["security_protocol"] = "plaintext";
}

void kafka_batch() {
    KafkaFixture fixture;
    configure_kafka(fixture.brokers);
    add_collector("kafka_v2", "v2");
    add_collector("kafka_v1", "v1");
    add_collector("kafka_missing", "missing");
    TestKafkaWriter writer("kafka_batch", "KafkaWriter", "es_test");
    check(writer.flush_impl({record("kafka_v2", 11, 1), record("kafka_v1", 11, 2), record("kafka_missing", 11, 0),
                            record("kafka_v2", 12, 3), record("kafka_v1", 12, 4), record("kafka_missing", 12, 0)}),
          "KafkaWriter changed batch completion behavior");
    writer.shutdown();
    check(counts["kafka_v2"].v2 == 1 && counts["kafka_v2"].v1 == 0 && counts["kafka_v2"].parsed == 2,
          "KafkaWriter did not cache its V2 parser");
    check(counts["kafka_v1"].v2 == 1 && counts["kafka_v1"].v1 == 1 && counts["kafka_v1"].parsed == 2,
          "KafkaWriter did not cache the V1 fallback");
    check(counts["kafka_missing"].v2 == 1 && counts["kafka_missing"].v1 == 1,
          "KafkaWriter did not cache a missing parser");
    const auto v2 = fixture.consume("writer_batch_kafka_v2", 2);
    const auto v1 = fixture.consume("writer_batch_kafka_v1", 2);
    const auto missing = fixture.consume("writer_batch_kafka_missing", 2);
    // The existing Kafka wire format is a JSON string containing the envelope.
    check(v2[0].is_string() && v1[0].is_string(), "KafkaWriter changed its existing wire encoding");
    const auto first = json::parse(v2[0].get<std::string>());
    const auto second = json::parse(v2[1].get<std::string>());
    check(first["collector_name"] == "kafka_v2" && first["job_info"]["JobID"] == 11
              && first["@timestamp"] == 1700000000011LL
              && first["data"] == json{{"value", 1}, {"parser", "v2"}, {"job_id", 11}, {"timestamp", 1700000000011LL}},
          "KafkaWriter changed its serialized envelope or data");
    check(second["data"]["value"] == 3 && second["data"]["job_id"] == 12,
          "KafkaWriter reused the first record's context or data");
    check(json::parse(v1[0].get<std::string>())["data"] == json{{"value", 2}, {"parser", "v1"}},
          "KafkaWriter changed its legacy parser data");
    check(missing[0].is_null() && missing[1].is_null(), "KafkaWriter changed its missing-parser fallback");
}

json call_rpc(const std::string& path, const std::string& method) {
    const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    check(fd >= 0, "could not create the RPC client");
    struct CloseSocket { int fd; ~CloseSocket() { close(fd); } } close_socket{fd};
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    check(path.size() < sizeof(address.sun_path), "RPC socket path is too long");
    std::copy(path.begin(), path.end(), address.sun_path);
    timeval timeout{5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    check(connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "RPC connect failed");
    const std::string request = json{{"method", method}, {"params", json::object()}}.dump();
    check(send(fd, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size()), "RPC send failed");
    std::string response;
    char buffer[4096];
    ssize_t size;
    while ((size = recv(fd, buffer, sizeof(buffer), 0)) > 0) response.append(buffer, size);
    check(size == 0, "RPC receive timed out");
    return json::parse(response);
}

void prometheus_batch(const std::string& socket_path) {
    add_collector("prom_cpu", "v2", "CPUMemCollector");
    add_collector("prom_net", "v2", "NetUsageCollector");
    add_collector("prom_fs", "v2", "FSMetadataCollector");
    TestPromWriter writer("prom_batch", "PrometheusExporterWriter", "es_test");
    Process p33{}; p33.pid = 33; p33.name = "first"; p33.cpu_usage_percent = 3.5; p33.mem_rss_kb = 330;
    Process p11{}; p11.pid = 11; p11.name = "second"; p11.cpu_usage_percent = 1.5;
    Process p22{}; p22.pid = 22; p22.name = "third"; p22.cpu_usage_percent = 2.5;
    Process p44{}; p44.pid = 44; p44.net_sent_bytes_total = 4400;
    State cpu1{}; cpu1.processes_state = {p33, p11};
    p11.cpu_usage_percent = 9.5;
    State cpu2{}; cpu2.processes_state = {p11, p22};
    p22.cpu_usage_percent = 8.5;
    cpu2.processes_state.push_back(p22);
    p11.net_sent_bytes_total = 1100; p11.tcp_rtt_us = 42;
    State net{}; net.processes_state = {p11, p44};
    p44.fs_metadata_ops_total = 44; p44.fs_metadata_errors_per_sec = 0.5;
    State fs{}; fs.processes_state = {p44};
    check(writer.flush_impl({record("prom_cpu", 101, cpu1), record("prom_net", 101, net),
                             record("prom_cpu", 101, cpu2), record("prom_fs", 101, fs)}), "Prometheus batch failed");
    check(counts["prom_cpu"].v2 == 1 && counts["prom_cpu"].parsed == 2, "Prometheus did not cache its batch parser");
    const auto metrics = call_rpc(socket_path, "prom_batch/metrics");
    const auto& processes = metrics.at("101").at("process_state");
    check(processes.size() == 4 && processes[0]["pid"] == 33 && processes[1]["pid"] == 11
              && processes[2]["pid"] == 44 && processes[3]["pid"] == 22,
          "Prometheus changed insertion order or duplicated an existing PID");
    check(processes[0]["cpu_usage_percent"] == 3.5 && processes[0]["mem_rss_kb"] == 330,
          "Prometheus changed an untouched process");
    check(processes[1]["cpu_usage_percent"] == 9.5 && processes[1]["net_sent_bytes_total"] == 1100
              && processes[1]["tcp_rtt_us"] == 42 && processes[1]["name"] == "second" && processes[1]["job_id"] == 101,
          "Prometheus lost fields while merging collectors");
    check(processes[2]["net_sent_bytes_total"] == 4400 && processes[2]["fs_metadata_ops_total"] == 44
              && processes[2]["fs_metadata_errors_per_sec"] == 0.5 && processes[3]["cpu_usage_percent"] == 8.5,
          "Prometheus lost metadata fields or failed last-update-wins for a repeated PID");

    std::atomic<bool> done{false};
    std::exception_ptr scrape_error;
    std::thread scraper([&] {
        try {
            do {
                check(call_rpc(socket_path, "prom_batch/metrics").is_object(), "invalid metrics during concurrent updates");
                check(call_rpc(socket_path, "prom_batch/info")["job_count"].get<int>() >= 1,
                      "invalid job count during concurrent updates");
            } while (!done);
        } catch (...) { scrape_error = std::current_exception(); }
    });
    for (int id = 200; id < 264; ++id) writer.flush_impl({record("prom_cpu", id, cpu1)});
    done = true;
    scraper.join();
    if (scrape_error) std::rethrow_exception(scrape_error);
    writer.shutdown();
    check(call_rpc(socket_path, "prom_batch/info")["job_count"] == 65, "Prometheus lost jobs during concurrent RPC reads");
}

void writer_raii() {
    const std::string path = "/tmp/joblens-writer-raii-" + std::to_string(getpid()) + ".txt";
    unlink(path.c_str());
    Config::instance().getRawNode("es_test")["path"] = path;
    add_collector("file_raii", "missing");
    std::string expected;
    {
        FileWriter writer("file_raii", "FileWriter", "es_test");
        for (int i = 0; i < 512; ++i) {
            const auto text = std::to_string(i) + ":" + std::string(1024, 'x') + "\n";
            expected += text;
            const auto row = record("file_raii", i + 1, text);
            writer.on_finish(std::get<0>(row), std::get<1>(row), std::get<2>(row), std::get<3>(row));
        }
    }
    std::ifstream input(path);
    const std::string actual((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    unlink(path.c_str());
    check(actual == expected, "FileWriter destructor lost accepted records");

    HttpFixture http;
    add_collector("es_raii", "v2");
    {
        ESWriter writer("es_raii", "ESWriter", "es_test");
        for (int i = 0; i < 64; ++i) {
            const auto row = record("es_raii", i + 1, i);
            writer.on_finish(std::get<0>(row), std::get<1>(row), std::get<2>(row), std::get<3>(row));
        }
    }
    {
        std::lock_guard lock(http.mutex);
        check(http.documents.size() == 64, "ESWriter destructor lost accepted records");
        for (int i = 0; i < 64; ++i)
            check(http.documents[i]["data"]["value"] == i, "ESWriter destructor reordered or duplicated records");
    }

    add_collector("prom_raii", "v2", "CPUMemCollector");
    {
        PrometheusExporterWriter writer("prom_raii", "PrometheusExporterWriter", "es_test");
        for (int i = 0; i < 128; ++i) {
            const auto row = record("prom_raii", i + 1, State{});
            writer.on_finish(std::get<0>(row), std::get<1>(row), std::get<2>(row), std::get<3>(row));
        }
    }
    check(counts["prom_raii"].parsed == 128, "Prometheus destructor lost accepted records");

    KafkaFixture kafka;
    configure_kafka(kafka.brokers);
    add_collector("kafka_raii", "v2");
    {
        TestKafkaWriter writer("kafka_raii", "KafkaWriter", "es_test");
        for (int i = 0; i < 64; ++i) {
            const auto row = record("kafka_raii", i + 1, i);
            writer.on_finish(std::get<0>(row), std::get<1>(row), std::get<2>(row), std::get<3>(row));
        }
    }
    const auto messages = kafka.consume("writer_batch_kafka_raii", 64);
    for (int i = 0; i < 64; ++i)
        check(json::parse(messages[i].get<std::string>())["data"]["value"] == i,
              "KafkaWriter destructor lost, reordered, or duplicated records");
}
}

int main(int argc, char** argv) {
    if (argc != 2 && argc != 3) return 2;
    spdlog::set_level(spdlog::level::off);
    Config::instance(argv[1]);
    const std::string socket_path = "/tmp/joblens-writer-batch-" + std::to_string(getpid()) + ".sock";
    auto& rpc = RPCServer::instance(socket_path);
    CollectorRegistry::instance().registerCollector<BatchCollector>("WriterBatchCollector", CollectorScope::Job, "Writer batch tests");
    int failures = 0;
    for (const auto& test : std::vector<std::pair<const char*, std::function<void()>>>{
            {"FileWriter batch parser cache and fallback", file_batch},
            {"ESWriter batch parser cache and JSON", es_batch},
            {"Prometheus PID merging, order, and concurrent RPC", [&] { rpc.start(); prometheus_batch(socket_path); }},
            {"KafkaWriter batch parser cache and wire format", kafka_batch},
            {"writer destructors drain accepted records", writer_raii}}) {
        if (argc == 3 && std::string(test.first) != "writer destructors drain accepted records") continue;
        try {
            test.second();
            std::cout << "PASS: " << test.first << '\n';
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "FAIL: " << test.first << ": " << error.what() << '\n';
        }
    }
    rpc.stop();
    return failures == 0 ? 0 : 1;
}
