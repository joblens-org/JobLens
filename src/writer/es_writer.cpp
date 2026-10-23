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
// elasticsearch_writer.cpp
#include "writer/es_writer.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <fmt/chrono.h>
#include <date/date.h>
#include <unordered_set>
#include "common/config.hpp"
#include "collector/collector_utils.hpp"
#include "core/writer_manager.hpp"
#include "common/utils.hpp"

AUTO_REGISTER_WRITER(
    ESWriter,
    "Writer for Elasticsearch",
    ConfigParams{
        {"host", "Elasticsearch server hostname or IP"},
        {"port", "Elasticsearch server port"},
        {"index_prefix", "Prefix for index names, e.g., 'collector_name' -> 'index_name'"},
        {"batch_size", "Number of records to batch before sending to ES"},
        {"max_bulk_bytes", "Maximum bulk request body bytes including metadata and newlines (default: 1048576)"},
        {"max_retries", "Additional retry rounds, 0 disables retries (default: 3, maximum: 10)"},
        {"retry_initial_backoff_ms", "Initial retry backoff in milliseconds (default: 200)"},
        {"retry_max_backoff_ms", "Maximum retry backoff in milliseconds (default: 5000)"},
        {"write_timeout", "Timeout in seconds for write operations"},
        {"indexs", "List of mappings from collector_name to index_name"}
    }
)

using json = nlohmann::json;


std::string format_utc8(std::chrono::system_clock::time_point tp)
{
    // 1. 转 time_t
    std::time_t t = std::chrono::system_clock::to_time_t(tp);

    // 2. 转 UTC 日历时间
    std::tm utc;
    gmtime_r(&t, &utc);

    // 3. 手动加 8 小时（东八区）
    utc.tm_hour += 8;
    std::mktime(&utc);          // 自动处理进位（年月日）

    // 4. 格式化字符串
    std::ostringstream os;
    os << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S+0800");
    // os << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S");
    return os.str();
}

static size_t discard_cb(char*, size_t size, size_t n, void*) { return size * n; }

/* ---------- 构造/析构 ---------- */
ESWriter::ESWriter(std::string name, std::string type, std::string config_name)
    : BaseWriter(name,type,config_name)
{
    opt_.batch_size = Config::instance().getInt(config_name, "batch_size", 1);
    const int max_bulk_bytes = Config::instance().getInt(config_name, "max_bulk_bytes", 1048576);
    if (max_bulk_bytes <= 0) {
        throw std::invalid_argument("ESWriter max_bulk_bytes must be positive");
    }
    opt_.max_bulk_bytes = static_cast<std::size_t>(max_bulk_bytes);
    opt_.max_retries = Config::instance().getInt(config_name, "max_retries", 3);
    opt_.retry_initial_backoff_ms = Config::instance().getInt(config_name, "retry_initial_backoff_ms", 200);
    opt_.retry_max_backoff_ms = Config::instance().getInt(config_name, "retry_max_backoff_ms", 5000);
    if (opt_.max_retries < 0 || opt_.max_retries > 10 || opt_.retry_initial_backoff_ms <= 0
        || opt_.retry_max_backoff_ms < opt_.retry_initial_backoff_ms || opt_.retry_max_backoff_ms > 60000) {
        throw std::invalid_argument("ESWriter invalid retry limits: retries 0..10, backoff 1..60000 ms, initial <= maximum");
    }
    opt_.host = Config::instance().getString(config_name, "host");
    opt_.port = Config::instance().getInt(config_name, "port");
    opt_.user = Config::instance().getString(config_name, "user", "");
    opt_.passwd = Config::instance().getString(config_name, "passwd", "");
    write_timeout = Config::instance().getInt(config_name, "write_timeout", 5);
    if (write_timeout <= 0) throw std::invalid_argument("ESWriter write_timeout must be positive");
    opt_.index_prefix = Config::instance().getString(config_name, "index_prefix", "collector");
    opt_.insecure = Config::instance().getBool(config_name, "insecure", false);
    if (Config::instance().has(config_name, "indexs")) {
        struct es_index_config {
            std::string collector_name;
            std::string index_name;
        };
        auto config_indexs = Config::instance().getArray<es_index_config>(
            config_name, "indexs",
            [](const YAML::Node& node) {
                es_index_config c;
                c.collector_name = node["collector_name"].as<std::string>();
                c.index_name = node["index_name"].as<std::string>();
                return c;
            });
        for (const auto& c : config_indexs) index_map_[c.collector_name] = c.index_name;
    }
    
    spdlog::debug("elasticsearch_writer {}: options - host={} port={} user='{}' index_prefix='{}' batch_size={} write_timeout={}",
                 name, opt_.host, opt_.port, opt_.user, opt_.index_prefix, opt_.batch_size, write_timeout);

    spdlog::debug("elasticsearch_writer: initializing curl...");
    curl_global_init(CURL_GLOBAL_ALL);
    spdlog::debug("elasticsearch_writer: curl_global_init done");
    curl_ = curl_easy_init();
    if (!curl_) throw std::runtime_error("curl_easy_init failed");
    if (!test_server()) {
        throw std::runtime_error("Failed to connect to Elasticsearch server at " +
                                 opt_.host + ":" + std::to_string(opt_.port));
    }
    spdlog::info("elasticsearch_writer {}: initialized with host={} port={} index_prefix='{}' batch_size={}",
                name, opt_.host, opt_.port, opt_.index_prefix, opt_.batch_size);
}

ESWriter::~ESWriter()
{
    shutdown();
    if (curl_) curl_easy_cleanup(curl_);
    curl_global_cleanup();
}

bool ESWriter::test_server()
{
    if (!curl_) return false;
    std::string url;
    if (opt_.host.find("http") != std::string::npos){
        url = fmt::format("{}:{}/", opt_.host, opt_.port);
    }else {
        url = fmt::format("http://{}:{}/", opt_.host, opt_.port);
    }

    spdlog::debug("elasticsearch_writer: testing connection to {}", url);
    curl_easy_reset(curl_);
    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_NOBODY, 1L); // HEAD 请求
    std::string userpwd = opt_.user+":"+opt_.passwd;
    curl_easy_setopt(curl_, CURLOPT_USERPWD, userpwd.c_str());
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, discard_cb); // 丢弃响应体
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT, write_timeout); // 设置超时
    if (opt_.insecure) {
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 0L);
    }

    CURLcode res = curl_easy_perform(curl_);
    
    if (res != CURLE_OK) {
        spdlog::error("elasticsearch_writer: curl_easy_perform() failed: {}", curl_easy_strerror(res));
        return false;
    }

    long response_code = 0;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &response_code);
    if (response_code != 200) {
        spdlog::error("elasticsearch_writer: server responded with code {}", response_code);
        return false;
    }

    spdlog::info("elasticsearch_writer: successfully connected to Elasticsearch at {}:{}", opt_.host, opt_.port);
    return true;
}

std::string ESWriter::try_get_index_name(const std::string& collector_name)
{
    auto it = index_map_.find(collector_name);
    if (it != index_map_.end()) return it->second;

    auto default_it = default_index_cache_.find(collector_name);
    if (default_it != default_index_cache_.end()) return default_it->second;

    std::string default_index = fmt::format("{}_{}", opt_.index_prefix, collector_name);
    default_index_cache_[collector_name] = default_index;

    if (warned_index_.insert(default_index).second) {
        spdlog::warn("elasticsearch_writer: using default index for collector '{}'", default_index);
    }
    return default_index;
}

/* ---------- 文档构造与批量发送 ---------- */
bool ESWriter::build_document(const std::string& collector_name, const Job& job,
                              std::chrono::system_clock::time_point ts, const std::string& record_id,
                              json data, BulkDocument& out)
{
    json action;
    auto index_name = try_get_index_name(collector_name);
    index_name += date::format("_%Y.%m.%d", date::floor<date::days>(ts));
    action["index"]["_index"] = index_name;
    action["index"]["_id"] = record_id;
    spdlog::trace("elasticsearch_writer: indexing to '{}'", index_name);
    json src;
    src["@timestamp"] = format_utc8(ts); //国产软件不能自己识别时区，要求东八区时间
    src["hostname"] = collector_utils::get_hostname();
    src["job_info"] = job_to_json(job);
    if (Utils::has_template_vars(index_name)) {
        json jobinfo;
        jobinfo["job_info"] = src["job_info"];
        auto flat_job = Utils::flatten_json(jobinfo);
        action["index"]["_index"] = Utils::render_bracket(index_name, flat_job);
        spdlog::trace("elasticsearch_writer: rendered index name '{}'",
                      action["index"]["_index"].get<std::string>());
    }
    src["data"] = std::move(data);
    std::string document = action.dump();
    document += '\n';
    document += src.dump();
    document += '\n';

    // 单文档不可切断；拒绝超限记录，但不阻塞同批次的正常记录。
    if (document.size() > opt_.max_bulk_bytes) {
        spdlog::error("elasticsearch_writer: oversized document not sent, collector={}, record={}, job_id={}, document_bytes={}, max_bulk_bytes={}",
                      collector_name, record_id, job.JobID, document.size(), opt_.max_bulk_bytes);
        return false;
    }
    out = {std::move(document), action["index"]["_index"].get<std::string>(),
           action["index"]["_id"].get<std::string>(), collector_name};
    return true;
}

bool ESWriter::flush_documents(const std::vector<BulkDocument>& documents)
{
    retry_documents_.clear();
    if (documents.empty()) return true;

    std::vector<BulkDocument> chunk;
    std::size_t chunk_bytes = 0;
    bool flush_ret = true;
    auto send_chunk = [&]() {
        if (chunk.empty()) return;
        if (!send_documents(chunk)) flush_ret = false;
        chunk.clear();
        chunk_bytes = 0;
    };

    for (const auto& document : documents) {
        if (chunk_bytes > opt_.max_bulk_bytes - document.ndjson.size()) send_chunk();
        chunk_bytes += document.ndjson.size();
        chunk.push_back(document);
    }
    send_chunk();
    return flush_ret;
}

bool ESWriter::flush_impl(const std::vector<OutputRecordPtr>& batch)
{
    if (batch.empty()) return true;
    std::vector<BulkDocument> documents;
    bool flush_ret = true;

    for (const auto& record : batch)
    {
        try {
            if (!record || !record->sample) {
                spdlog::error("elasticsearch_writer: native record without sample metadata, writer={}", name_);
                flush_ret = false;
                continue;
            }
            const auto* payload = std::get_if<Document>(&record->payload);
            if (!payload || !payload->value.is_object()) {
                spdlog::error("elasticsearch_writer: native payload is not a JSON object, collector={}, record={}",
                              record->sample->collector_name, record->record_id);
                flush_ret = false;
                continue;
            }
            BulkDocument document;
            if (!build_document(record->sample->collector_name, record->sample->job,
                                record->sample->sampled_at, record->record_id,
                                payload->value, document)) {
                flush_ret = false;
                continue;
            }
            documents.push_back(std::move(document));
        } catch (const std::exception& error) {
            spdlog::error("elasticsearch_writer: native document processing failed, reason={}", error.what());
            flush_ret = false;
        } catch (...) {
            spdlog::error("elasticsearch_writer: native document processing failed, unknown exception");
            flush_ret = false;
        }
    }

    return flush_documents(documents) && flush_ret;
}
