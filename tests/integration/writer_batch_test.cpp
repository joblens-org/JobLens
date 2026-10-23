#include "writer/es_writer.hpp"
#include "writer/file_writer.hpp"
#include "writer/kafka_writer.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include "core/output_record.h"
#include "common/local_rpc.hpp"
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
void check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

Job make_job(int id, const std::string& cluster = "batch") {
    Job job{};
    job.JobID = id;
    job.NativeJobID = std::to_string(id);
    job.cluster_name = cluster;
    job.clusterTag = "test";
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    return job;
}

OutputRecordPtr make_record(const std::string& collector, const Job& job, int id,
                            OutputPayload payload, const std::string& channel = "text",
                            int64_t timestamp_ms = 1700000000000LL,
                            const std::string& collector_type = "TestCollector") {
    auto sample = std::make_shared<SampleMetadata>();
    sample->sample_id = "run:" + std::to_string(id);
    sample->collector_name = collector;
    sample->collector_type = collector_type;
    sample->job = job;
    sample->sampled_at = std::chrono::system_clock::time_point(std::chrono::milliseconds(timestamp_ms + id));
    auto record = std::make_shared<OutputRecord>();
    record->sample = sample;
    record->channel = channel;
    record->sequence = static_cast<uint64_t>(id);
    record->record_id = sample->sample_id + ":" + std::to_string(id);
    record->payload = std::move(payload);
    record->accounted_bytes = estimate_output_bytes(record->payload);
    return record;
}

nlohmann::json text_data(const nlohmann::json& document) {
    return document.at("data");
}

void file_batch() {
    const std::string base = "/tmp/joblens-writer-batch-" + std::to_string(getpid());
    const std::string path_a = base + "-a.txt";
    const std::string path_b = base + "-b.txt";
    unlink(path_a.c_str());
    unlink(path_b.c_str());
    auto& config = Config::instance();
    config.getRawNode("es_test")["path"] = base + "-default.txt";
    config.getRawNode("es_test")["outputs"] = YAML::Load(
        "- {collector_name: file_a, file_path: " + path_a + "}\n"
        "- {collector_name: file_b, file_path: " + path_b + "}\n");
    {
        FileWriter writer("file_batch", "FileWriter", "es_test");
        const auto job = make_job(11);
        check(writer.try_enqueue(make_record("file_a", job, 1, Text{"a1\n"})) == EnqueueCode::Accepted, "a1");
        check(writer.try_enqueue(make_record("file_b", job, 2, Text{"b1\n"})) == EnqueueCode::Accepted, "b1");
        check(writer.try_enqueue(make_record("file_a", job, 3, Text{"a2\n"})) == EnqueueCode::Accepted, "a2");
        writer.shutdown();
    }
    auto read = [](const std::string& path) {
        std::ifstream input(path);
        return std::string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    };
    const std::string a = read(path_a);
    const std::string b = read(path_b);
    unlink(path_a.c_str());
    unlink(path_b.c_str());
    unlink((base + "-default.txt").c_str());
    check(a == "a1\na2\n", "FileWriter changed text bytes or per-collector routing");
    check(b == "b1\n", "FileWriter changed routing or ordering");
    config.getRawNode("es_test")["outputs"] = YAML::Node(YAML::NodeType::Sequence);
    config.getRawNode("es_test")["path"] = "/tmp/joblens-writer-batch-default.txt";
}

class TestKafkaWriter : public KafkaWriter {
public:
    using KafkaWriter::KafkaWriter;
    using KafkaWriter::flush_impl;
};
class TestPromWriter : public PrometheusExporterWriter {
public:
    using PrometheusExporterWriter::PrometheusExporterWriter;
    using PrometheusExporterWriter::flush_impl;
};

class HttpFixture {
public:
    httplib::Server server;
    std::thread thread;
    std::vector<nlohmann::json> documents;
    std::vector<std::string> ids;
    std::mutex mutex;
    HttpFixture() {
        server.Get("/", [](const httplib::Request&, httplib::Response& response) {
            response.set_content("{}", "application/json");
        });
        server.Post("/_bulk", [&](const httplib::Request& request, httplib::Response& response) {
            std::lock_guard lock(mutex);
            nlohmann::json items = nlohmann::json::array();
            std::istringstream input(request.body);
            std::string action, source;
            while (std::getline(input, action) && std::getline(input, source)) {
                const auto metadata = nlohmann::json::parse(action)["index"];
                documents.push_back(nlohmann::json::parse(source));
                ids.push_back(metadata["_id"].get<std::string>());
                items.push_back({{"index", {{"_id", metadata["_id"]}, {"_index", metadata["_index"]}, {"status", 201}}}});
            }
            response.set_content(nlohmann::json{{"errors", false}, {"items", items}}.dump(), "application/json");
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
    ESWriter writer("es_batch", "ESWriter", "es_test");
    const auto job = make_job(11);
    std::vector<OutputRecordPtr> batch{
        make_record("es_doc", job, 1, Document{nlohmann::json{{"value", 1}}}, "document"),
        make_record("es_doc", job, 2, Document{nlohmann::json{{"value", 2}}}, "document"),
        make_record("es_doc", job, 3, Document{nlohmann::json{{"value", 3}}}, "document"),
        make_record("es_doc", job, 4, Document{nlohmann::json{{"value", 4}}}, "document")};
    check(writer.flush_impl(batch), "ESWriter rejected valid native documents");
    writer.shutdown();
    std::lock_guard lock(fixture.mutex);
    check(fixture.documents.size() == 4, "ESWriter lost or added documents");
    check(text_data(fixture.documents[0]) == nlohmann::json{{"value", 1}}, "ESWriter changed data");
    check(fixture.documents[0]["job_info"]["JobID"] == 11, "ESWriter changed the job envelope");
    check(fixture.ids.size() == 4, "ESWriter did not emit one id per document");
    for (size_t i = 0; i < 4; ++i) {
        check(fixture.ids[i] == batch[i]->record_id,
              "ESWriter did not use the stable record identity as _id");
        for (size_t j = i + 1; j < 4; ++j)
            check(fixture.ids[i] != fixture.ids[j], "same-time documents overwrite each other");
    }
}

class KafkaFixture {
public:
    KafkaFixture() {
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
        for (const auto* name : {"writer_batch_kafka_doc", "writer_batch_kafka_raii"})
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
    std::vector<nlohmann::json> consume(const std::string& topic_name, std::size_t count) {
        auto* topic = rd_kafka_topic_new(consumer_, topic_name.c_str(), nullptr);
        check(topic != nullptr, "could not open a Kafka fixture topic");
        struct TopicGuard {
            rd_kafka_topic_t* topic;
            ~TopicGuard() { rd_kafka_consume_stop(topic, 0); rd_kafka_topic_destroy(topic); }
        } guard{topic};
        check(rd_kafka_consume_start(topic, 0, RD_KAFKA_OFFSET_BEGINNING) == 0, "could not start the Kafka fixture consumer");
        std::vector<nlohmann::json> result;
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
            result.push_back(nlohmann::json::parse(std::string(static_cast<const char*>(message->payload), message->len)));
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
    TestKafkaWriter writer("kafka_batch", "KafkaWriter", "es_test");
    const auto job = make_job(11);
    check(writer.flush_impl({
              make_record("kafka_doc", job, 1, Document{nlohmann::json{{"value", 1}}}, "document"),
              make_record("kafka_doc", job, 2, Document{nlohmann::json{{"value", 2}}}, "document")}),
          "KafkaWriter rejected valid native documents");
    writer.shutdown();
    const auto messages = fixture.consume("writer_batch_kafka_doc", 2);
    // 既有线格式：Kafka 消息体是包含信封的 JSON 字符串。
    check(messages[0].is_string() && messages[1].is_string(), "KafkaWriter changed its wire encoding");
    const auto first = nlohmann::json::parse(messages[0].get<std::string>());
    const auto second = nlohmann::json::parse(messages[1].get<std::string>());
    check(first["collector_name"] == "kafka_doc" && first["job_info"]["JobID"] == 11
              && first["@timestamp"] == 1700000000001LL
              && first["data"] == nlohmann::json{{"value", 1}},
          "KafkaWriter changed its serialized envelope or data");
    check(second["data"] == nlohmann::json{{"value", 2}}, "KafkaWriter reused the first record's data");
}

nlohmann::json call_rpc(const std::string& path, const std::string& method) {
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
    const std::string request = nlohmann::json{{"method", method}, {"params", nlohmann::json::object()}}.dump();
    check(send(fd, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size()), "RPC send failed");
    std::string response;
    char buffer[4096];
    ssize_t size;
    while ((size = recv(fd, buffer, sizeof(buffer), 0)) > 0) response.append(buffer, size);
    check(size == 0, "RPC receive timed out");
    return nlohmann::json::parse(response);
}

ProcessMetricSnapshot process(pid_t pid, const std::string& name, double cpu) {
    ProcessMetricSnapshot p{};
    p.pid = pid;
    p.name = name;
    p.cpu_usage_percent = cpu;
    return p;
}

void prometheus_batch(const std::string& socket_path) {
    TestPromWriter writer("prom_batch", "PrometheusExporterWriter", "es_test");
    const auto job = make_job(101);
    auto cpu1 = MetricSnapshot{}; cpu1.processes_state = {process(33, "first", 3.5), process(11, "second", 1.5)};
    cpu1.processes_state[0].mem_rss_kb = 330;
    auto cpu2 = MetricSnapshot{}; cpu2.processes_state = {process(11, "second", 9.5), process(22, "third", 2.5)};
    auto net = MetricSnapshot{}; net.processes_state = {process(11, "second", 0), process(44, "fourth", 0)};
    net.processes_state[0].net_sent_bytes_total = 1100; net.processes_state[0].tcp_rtt_us = 42;
    net.processes_state[0].name = "second"; net.processes_state[0].job_id = 101;
    auto fs = MetricSnapshot{}; fs.processes_state = {process(44, "fourth", 0)};
    fs.processes_state[0].fs_metadata_ops_total = 44;
    fs.processes_state[0].fs_metadata_errors_per_sec = 0.5;
    check(writer.flush_impl({
              make_record("prom_cpu", job, 1, cpu1, "metrics", 1700000000000LL, "CPUMemCollector"),
              make_record("prom_net", job, 2, net, "metrics", 1700000000000LL, "NetUsageCollector"),
              make_record("prom_cpu", job, 3, cpu2, "metrics", 1700000000000LL, "CPUMemCollector"),
              make_record("prom_fs", job, 4, fs, "metrics", 1700000000000LL, "FSMetadataCollector")}),
          "Prometheus batch failed");
    const auto metrics = call_rpc(socket_path, "prom_batch/metrics");
    const auto& processes = metrics.at("101").at("process_state");
    check(processes.size() == 4 && processes[0]["pid"] == 33 && processes[1]["pid"] == 11
              && processes[2]["pid"] == 44 && processes[3]["pid"] == 22,
          "Prometheus changed insertion order or duplicated an existing PID");
    check(processes[0]["cpu_usage_percent"] == 3.5 && processes[0]["mem_rss_kb"] == 330,
          "Prometheus changed an untouched process");
    check(processes[1]["cpu_usage_percent"] == 9.5 && processes[1]["net_sent_bytes_total"] == 1100
              && processes[1]["tcp_rtt_us"] == 42 && processes[1]["name"] == "second"
              && processes[1]["job_id"] == 101,
          "Prometheus lost fields while merging collectors");
    check(processes[2]["net_sent_bytes_total"] == 0 && processes[2]["fs_metadata_ops_total"] == 44
              && processes[2]["fs_metadata_errors_per_sec"] == 0.5 && processes[3]["cpu_usage_percent"] == 2.5,
          "Prometheus lost metadata fields or failed last-update-wins for a repeated PID");
    writer.shutdown();
}

void writer_raii() {
    const std::string path = "/tmp/joblens-writer-raii-" + std::to_string(getpid()) + ".txt";
    unlink(path.c_str());
    Config::instance().getRawNode("es_test")["path"] = path;
    {
        FileWriter writer("file_raii", "FileWriter", "es_test");
        for (int i = 0; i < 512; ++i) {
            const auto text = std::to_string(i) + ":" + std::string(1024, 'x') + "\n";
            check(writer.try_enqueue(make_record("file_raii", make_job(i + 1), i, Text{text})) == EnqueueCode::Accepted,
                  "raii enqueue rejected");
        }
    }
    std::ifstream input(path);
    const std::string actual((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    unlink(path.c_str());
    check(actual.find("0:") == 0 && actual.find("511:") != std::string::npos,
          "FileWriter destructor lost accepted records");

    HttpFixture http;
    {
        ESWriter writer("es_raii", "ESWriter", "es_test");
        for (int i = 0; i < 64; ++i) {
            check(writer.try_enqueue(make_record("es_raii", make_job(i + 1), i,
                                                 Document{nlohmann::json{{"value", i}}}, "document"))
                      == EnqueueCode::Accepted, "es raii enqueue rejected");
        }
    }
    {
        std::lock_guard lock(http.mutex);
        check(http.documents.size() == 64, "ESWriter destructor lost accepted records");
        for (int i = 0; i < 64; ++i)
            check(text_data(http.documents[i])["value"] == i,
                  "ESWriter destructor reordered or duplicated records");
    }

    KafkaFixture kafka;
    configure_kafka(kafka.brokers);
    {
        KafkaWriter writer("kafka_raii", "KafkaWriter", "es_test");
        for (int i = 0; i < 32; ++i) {
            check(writer.try_enqueue(make_record("kafka_raii", make_job(i + 1), i,
                                                 Document{nlohmann::json{{"value", i}}}, "document"))
                      == EnqueueCode::Accepted, "kafka raii enqueue rejected");
        }
    }
    const auto messages = kafka.consume("writer_batch_kafka_raii", 32);
    for (int i = 0; i < 32; ++i)
        check(nlohmann::json::parse(messages[i].get<std::string>())["data"]["value"] == i,
              "KafkaWriter destructor lost, reordered, or duplicated records");
}
}

void file_writer_rejects_symlink() {
    const std::string link = "/tmp/joblens-writer-symlink-" + std::to_string(getpid()) + ".txt";
    unlink(link.c_str());
    check(symlink("/etc/hostname", link.c_str()) == 0, "create symlink fixture");

    auto& config = Config::instance();
    config.getRawNode("symlink_test")["path"] = link;

    bool threw = false;
    try {
        FileWriter writer("symlink", "FileWriter", "symlink_test");
    } catch (const std::exception&) {
        threw = true;
    }
    check(threw, "file_writer must refuse a symlink output path");
    unlink(link.c_str());
}

int main(int argc, char** argv) {
    if (argc != 2 && argc != 3) return 2;
    spdlog::set_level(spdlog::level::off);
    Config::instance(argv[1]);
    const std::string socket_path = "/tmp/joblens-writer-batch-" + std::to_string(getpid()) + ".sock";
    auto& rpc = RPCServer::instance(socket_path);
    int failures = 0;
    for (const auto& test : std::vector<std::pair<const char*, std::function<void()>>>{
            {"FileWriter text routing", file_batch},
            {"ESWriter document envelope and identity", es_batch},
            {"Prometheus PID merging, order, and RPC", [&] { rpc.start(); prometheus_batch(socket_path); }},
            {"KafkaWriter document wire format", kafka_batch},
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
