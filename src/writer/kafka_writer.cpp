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
#include "writer/kafka_writer.hpp"
#include "core/collector_registry.hpp"
#include <chrono>
#include <sstream>
#include <common/config.hpp>
#include "collector/collector_utils.hpp"
#include "core/writer_manager.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <fmt/ranges.h>

AUTO_REGISTER_WRITER(
    KafkaWriter,
    "Writer for Apache Kafka",
    ConfigParams{
        {"brokers", "Kafka broker list, e.g., 'your-kafka-broker:9092'"},
        {"topic_prefix", "Kafka topic prefix for normal messages, e.g., 'collector-data'"},
        {"topic_dlq", "Kafka topic for dead-letter-queue messages, e.g., 'collector-data-dlq'"},
        {"client_id", "Kafka client ID"},
        {"transactional_id", "Kafka transactional ID"},
        {"batch_rows", "Number of messages to batch before sending to Kafka"},
        {"linger_ms", "Maximum time in milliseconds to wait before sending a batch"},
        {"enable_dlq", "Enable dead-letter-queue for failed messages"},
        {"enable_transaction", "Enable Kafka transactions"},
        {"security_protocol", "Security protocol for Kafka connection, e.g., 'sasl_plaintext'"},
        {"sasl_mechanism", "SASL mechanism for Kafka authentication, e.g., 'PLAIN'"},
        {"username", "SASL username for Kafka authentication"},
        {"password", "SASL password for Kafka authentication"}
    }
)

using json = nlohmann::json;

// -------------------------- 构造/析构 --------------------------
KafkaWriter::KafkaWriter(std::string name, std::string type, std::string config_name)
    : BaseWriter(name,type,config_name) {
    // 1. 读取配置
    opt_.brokers            = Config::instance().getArray<std::string>(config_name, "brokers");
    opt_.topic_prefix       = Config::instance().getString(config_name, "topic_prefix");
    opt_.topic_dlq          = Config::instance().getString(config_name, "topic_dlq");
    opt_.client_id          = Config::instance().getString(config_name, "client_id");
    opt_.transactional_id   = Config::instance().getString(config_name, "transactional_id");
    opt_.batch_rows         = Config::instance().getInt   (config_name, "batch_rows");
    opt_.linger_ms          = Config::instance().getInt   (config_name, "linger_ms");
    opt_.enable_transaction = Config::instance().getBool (config_name, "enable_transaction");
    opt_.security_protocol  = Config::instance().getString(config_name, "security_protocol");
    if (opt_.security_protocol.compare("plaintext") != 0) {
        opt_.sasl_mechanism     = Config::instance().getString(config_name, "sasl_mechanism");
        opt_.username           = Config::instance().getString(config_name, "username");
        opt_.password           = Config::instance().getString(config_name, "password");
    } else {
        opt_.enable_dlq = false; // 不启用DLQ
    }
    

    // 2. 初始化 Kafka Producer
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    int ret;
    std::string errstr;
    ret = conf->set("bootstrap.servers",fmt::format("{}", fmt::join(opt_.brokers, ",")), errstr);
    ret = conf->set("client.id", opt_.client_id, errstr);
    ret = conf->set("linger.ms", std::to_string(opt_.linger_ms), errstr);
    ret = conf->set("queue.buffering.max.messages", std::to_string(opt_.batch_rows * 10), errstr);
    ret = conf->set("security.protocol", opt_.security_protocol, errstr);
    if (opt_.security_protocol.compare("plaintext") != 0) {
        ret = conf->set("sasl.username", opt_.username, errstr);
        ret = conf->set("sasl.password", opt_.password, errstr);
        ret = conf->set("sasl.mechanism", opt_.sasl_mechanism, errstr);
    }
    if (ret != RdKafka::Conf::CONF_OK) {
        spdlog::error("KafkaWriter: failed to set Kafka config: {}", errstr);
    }
    try
    {
        spdlog::info("KafkaWriter: initializing producer with brokers '{}', topic_prefix '{}'", opt_.brokers, opt_.topic_prefix);
        producer_ = RdKafka::Producer::create(conf, errstr);
        spdlog::info("KafkaWriter: errstr from producer create: {}", errstr);
        producer_->produce(
            "test_topic",
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            nullptr,
            0,
            nullptr,0,
            0,
            nullptr
        );// 测试发送一条空消息
        producer_->poll(0);
    }
    catch(const std::exception& e)
    {
        spdlog::error("KafkaWriter: test produce failed: {}", e.what());
    }
    
    
    if (!producer_) {
        spdlog::error("KafkaWriter: failed to create producer: {}", errstr);
    }
    delete conf;
}


void KafkaWriter::do_shutdown() {
     producer_->flush(10000);
    if (producer_->outq_len() > 0) {
        spdlog::warn("KafkaWriter: {} message(s) still in out queue on shutdown", producer_->outq_len());
    }
}


// -------------------------- flush_impl --------------------------
bool KafkaWriter::flush_impl(const std::vector<write_data>& batch) {
    for (const auto& w : batch) {
        json payload;
        try {
            payload = serialize(w);
        } catch (const std::exception& ex) {
            if (opt_.enable_dlq) {
                std::string dlq = json{
                    {"exception", ex.what()},
                    {"collector_name",       std::get<0>(w) /*collector_name*/},
                    {"job_info",  job_to_json(std::get<1>(w))},
                }.dump();
                producer_->produce(
                    opt_.topic_dlq,
                    RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_COPY,
                    const_cast<char*>(dlq.c_str()),
                    dlq.size(),
                    nullptr,0,
                    0,
                    nullptr
                );
            }
        }
        auto payload_str = payload.dump();
retry:
        RdKafka::ErrorCode err = producer_->produce(
            opt_.topic_prefix + std::get<0>(w),
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(payload_str.c_str()),
            payload_str.size(),
            nullptr,0,
            0,
            nullptr
        );
        if (err != RdKafka::ERR_NO_ERROR) {
            spdlog::error("KafkaWriter: failed to produce message: {}", RdKafka::err2str(err));

            if (err == RdKafka::ERR__QUEUE_FULL) {
                producer_->poll(1000);
                goto retry;
            }

            } else {
                spdlog::error("KafkaWriter: produced message to collector '{}', size {} bytes",
                            std::get<0>(w), payload_str.size());
            }
        producer_->poll(0);
    }

    return true;
}

// -------------------------- serialize --------------------------
json KafkaWriter::serialize(const write_data& w) {
    const auto& [collector_name, job, data_any, tp] = w;
    std::any parser = CollectorRegistry::instance().getCollectorParser(collector_name, type_);
    if (parser.type() == typeid(std::function<std::any(const std::any&)>) ) {
        auto& func = std::any_cast<std::function<std::any(const std::any&)>&>(parser);
        try {
            std::any parsed = func(data_any);   // 业务自己返回 json string
            auto parsed_data = std::any_cast<json>(parsed);
            // 2. 再包一层元数据
            json wrap = {
                {"collector_name", collector_name},
                {"hostname",      collector_utils::get_hostname()},
                {"@timestamp",  std::chrono::duration_cast<std::chrono::milliseconds>(
                                tp.time_since_epoch()).count()},
                {"job_info",  job_to_json(job)},
                {"data",      parsed_data}   // 保证是 object
            };
            return wrap.dump();
        } catch (const std::exception& ex) {
            spdlog::error("KafkaWriter: error parsing data for collector '{}', writer '{}': {}", collector_name, type_, ex.what());
            throw;
        }
    } else {
        spdlog::error("KafkaWriter: parser type mismatch for collector '{}', writer '{}'", collector_name, type_);
        throw std::runtime_error("parser type mismatch");
    }
}