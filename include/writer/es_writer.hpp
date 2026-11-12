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
// elasticsearch_writer.h
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "writer/base_writer.hpp"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <unordered_set>

class ESWriter : public BaseWriter
{
public:
    struct options
    {
        std::string host         = "localhost";
        int         port         = 9200;
        std::string index_prefix = "joblens";
        std::size_t batch_size   = 500;   // 达到多少条就打包
        std::size_t max_bulk_bytes = 1048576; // 单次请求体上限，包含元数据和换行
        int max_retries = 3;
        int retry_initial_backoff_ms = 200;
        int retry_max_backoff_ms = 5000;
        std::string user         = "";
        std::string passwd       = "";
        bool        insecure     = false; // skip SSL verification
    };

    explicit ESWriter(std::string name, std::string type, std::string config_name);

    ~ESWriter() override;

    bool accepts(OutputKind kind) const override { return kind == OutputKind::Document; }

    /* 真正写 ES：被 BaseWriter 后台线程调用 */
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override;

protected:
    void on_flush_error(const std::vector<OutputRecordPtr>& batch) override;

private:
    struct BulkDocument {
        struct Reservation {
            ESWriter* owner = nullptr;
            std::size_t bytes = 0;
            Reservation() = default;
            Reservation(const Reservation&) = delete;
            Reservation& operator=(const Reservation&) = delete;
            Reservation(Reservation&& other) noexcept : owner(other.owner), bytes(other.bytes) { other.owner = nullptr; }
            Reservation& operator=(Reservation&&) = delete;
            ~Reservation() { if (owner) owner->release_additional_bytes(bytes); }
        } reservation; // Destroyed after the strings it accounts for.
        std::string ndjson;
        std::string index;
        std::string id;
        std::string collector;
    };
    bool flush_documents(std::vector<BulkDocument> documents);
    struct BulkResponse {
        CURLcode transport;
        long status;
        std::string body;
    };
    BulkResponse post_bulk(const std::string& bulk_body);
    bool send_documents(std::vector<BulkDocument>& documents);
    void run_retry_rounds();
    bool reserve_retry_budget(BulkDocument& document);
    bool build_document(const std::string& collector_name, const Job& job,
                        std::chrono::system_clock::time_point ts, const std::string& record_id,
                        nlohmann::json data, BulkDocument& out);
    std::vector<BulkDocument> retry_documents_;
    bool test_server();
    std::string try_get_index_name(const std::string& collector_name);
    int write_timeout;
    options opt_;
    CURL* curl_ = nullptr;
    // 每个实例独立的 collector→index 映射，不能使用函数级 static 缓存。
    std::unordered_map<std::string, std::string> index_map_;
    std::unordered_map<std::string, std::string> default_index_cache_;
    std::unordered_set<std::string> warned_index_;
};
