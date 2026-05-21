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
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "writer/base_writer.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>          // 需要 json 库，也可用 rapidjson 替代
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>

using json = nlohmann::json;

class KafkaWriter : public BaseWriter {
public:
    struct Options {
        std::vector<std::string> brokers          = {"127.0.0.1:9092"};
        std::string topic_prefix     = "collector-data";
        std::string topic_dlq        = "collector-data-dlq";
        std::string client_id        = "joblens_kafka_writer";
        std::string transactional_id = "txn-kafka-writer-1";
        size_t      batch_rows       = 1000;      // 攒批条数
        size_t      linger_ms        = 100;       // 最长攒批时间
        bool        enable_dlq       = true;
        bool        enable_transaction= true;
        std::string security_protocol = "plaintext";
        std::string sasl_mechanism    = "plain";
        std::string username         = "";
        std::string password         = "";
    };

    explicit KafkaWriter(std::string name, std::string type, std::string config_name);
protected:
    // 覆写基类
    bool flush_impl(const std::vector<write_data>&) override;
    void do_shutdown() override;

private:

    // 工具：把 write_data 序列化成 json string
    json serialize(const write_data& w);

    Options                              opt_;
    RdKafka::Producer*                   producer_;
    std::thread                          worker_;
    std::mutex                           mq_mtx_;
    std::condition_variable              mq_cv_;
    std::queue<std::vector<write_data>>  mq_;   // 待写队列
    bool                                 running_ = true;
};