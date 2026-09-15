#include "writer/es_writer.hpp"
#include <algorithm>
#include <random>

namespace {
bool retryable_status(long status) {
    return status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
}

bool retryable_transport(CURLcode code) {
    return code == CURLE_COULDNT_RESOLVE_HOST || code == CURLE_COULDNT_RESOLVE_PROXY
        || code == CURLE_COULDNT_CONNECT || code == CURLE_OPERATION_TIMEDOUT
        || code == CURLE_SEND_ERROR || code == CURLE_RECV_ERROR || code == CURLE_GOT_NOTHING
        || code == CURLE_PARTIAL_FILE;
}

size_t receive_body(char* ptr, size_t size, size_t count, void* userdata) {
    try {
        static_cast<std::string*>(userdata)->append(ptr, size * count);
        return size * count;
    } catch (...) {
        return 0;
    }
}
}

ESWriter::BulkResponse ESWriter::post_bulk(const std::string& bulk)
{
    BulkResponse result{CURLE_FAILED_INIT, 0, {}};
    if (!curl_) return result;
    curl_easy_reset(curl_);
    const auto host = opt_.host.find("http") != std::string::npos ? opt_.host : "http://" + opt_.host;
    const auto url = fmt::format("{}:{}/_bulk", host, opt_.port);
    const auto userpwd = opt_.user + ":" + opt_.passwd;
    if (!opt_.user.empty()) curl_easy_setopt(curl_, CURLOPT_USERPWD, userpwd.c_str());
    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, bulk.data());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(bulk.size()));
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT, static_cast<long>(write_timeout));
    curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);
    if (opt_.insecure) {
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(
        curl_slist_append(nullptr, "Content-Type: application/x-ndjson"), curl_slist_free_all);
    if (!headers) return result;
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers.get());
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, receive_body);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &result.body);
    result.transport = curl_easy_perform(curl_);
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &result.status);
    return result;
}

bool ESWriter::send_documents(const std::vector<BulkDocument>& documents)
{
    std::string body;
    for (const auto& document : documents) body += document.ndjson;
    const auto response = post_bulk(body);
    bool success = true;
    auto fail = [&](const BulkDocument& document, bool retry, long status, const std::string& reason) {
        success = false;
        spdlog::error("elasticsearch_writer: write failed, collector={}, index={}, id={}, status={}, retryable={}, reason={}",
                      document.collector, document.index, document.id, status, retry, reason.substr(0, 512));
        if (retry) retry_documents_.push_back(document);
        else spdlog::warn("elasticsearch_writer: dropping permanent failure, index={}, id={}", document.index, document.id);
    };
    if (response.transport != CURLE_OK || (response.status != 200 && response.status != 201)) {
        const bool retry = response.transport != CURLE_OK ? retryable_transport(response.transport) : retryable_status(response.status);
        std::string reason = response.transport != CURLE_OK
            ? std::string(curl_easy_strerror(response.transport)) + "; delivery unknown" : "HTTP request rejected";
        if (response.transport == CURLE_OK) {
            const auto error = nlohmann::json::parse(response.body, nullptr, false);
            if (error.is_object() && error.contains("error")) reason = error["error"].dump();
        }
        for (const auto& document : documents) fail(document, retry, response.status, reason);
        return false;
    }
    const auto parsed = nlohmann::json::parse(response.body, nullptr, false);
    if (!parsed.is_object() || !parsed.contains("errors") || !parsed["errors"].is_boolean()
        || !parsed.contains("items") || !parsed["items"].is_array()
        || parsed["items"].size() != documents.size()) {
        for (const auto& document : documents) fail(document, true, response.status, "invalid bulk response; delivery unknown");
        return false;
    }
    // 仅在条目数量匹配时按提交顺序确认；损坏条目不能覆盖其他已确认成功项。
    for (std::size_t i = 0; i < documents.size(); ++i) {
        const auto& document = documents[i];
        const auto& item = parsed["items"][i];
        if (!item.is_object() || item.size() != 1 || !item.contains("index") || !item["index"].is_object()) {
            fail(document, true, 0, "invalid bulk item; delivery unknown");
            continue;
        }
        const auto& operation = item["index"];
        if (!operation.contains("status") || !operation["status"].is_number_integer()
            || operation["status"] < 100 || operation["status"] > 599
            || (operation.contains("_id") && operation["_id"] != document.id)) {
            fail(document, true, 0, "invalid bulk status or id; delivery unknown");
            continue;
        }
        const auto status = operation["status"].get<int>();
        if ((status == 200 || status == 201) && !operation.contains("error")) continue;
        if (status >= 200 && status < 300) {
            fail(document, true, status, "inconsistent bulk item; delivery unknown");
            continue;
        }
        fail(document, retryable_status(status), status,
             operation.contains("error") ? operation["error"].dump() : "bulk item failed without reason");
    }
    if (success && parsed["errors"].get<bool>()) {
        spdlog::error("elasticsearch_writer: bulk errors flag contradicts successful items; confirmed items will not be retried");
        return false;
    }
    return success;
}

void ESWriter::on_flush_error(const std::vector<write_data>& batch)
{
    BaseWriter::on_flush_error(batch);
    auto delay = opt_.retry_initial_backoff_ms;
    thread_local std::mt19937 random(std::random_device{}());
    for (int attempt = 1; attempt <= opt_.max_retries && !retry_documents_.empty(); ++attempt) {
        const int wait_ms = std::uniform_int_distribution<int>(std::max(1, delay / 2), delay)(random);
        spdlog::warn("elasticsearch_writer: retry round={}/{}, documents={}, backoff_ms={}",
                     attempt, opt_.max_retries, retry_documents_.size(), wait_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
        auto pending = std::move(retry_documents_);
        retry_documents_.clear();
        std::vector<BulkDocument> chunk;
        std::size_t bytes = 0;
        for (auto& document : pending) {
            if (bytes > opt_.max_bulk_bytes - document.ndjson.size()) {
                send_documents(chunk);
                chunk.clear();
                bytes = 0;
            }
            bytes += document.ndjson.size();
            chunk.push_back(std::move(document));
        }
        if (!chunk.empty()) send_documents(chunk);
        delay = std::min(delay * 2, opt_.retry_max_backoff_ms);
    }
    for (const auto& document : retry_documents_) {
        spdlog::error("elasticsearch_writer: retries exhausted, dropping index={}, id={}, max_retries={}",
                      document.index, document.id, opt_.max_retries);
    }
    retry_documents_.clear();
}
