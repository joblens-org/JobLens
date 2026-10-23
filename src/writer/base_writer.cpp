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
#include "writer/base_writer.hpp"
#include <stdexcept>
#include <algorithm>

// 内部类型完整定义
struct BaseWriter::Buffer
{
    explicit Buffer(std::size_t reserve)
    {
        records.reserve(reserve);
        bytes.reserve(reserve);
    }
    void clear()
    {
        records.clear();
        bytes.clear();
    }
    std::size_t size() const { return records.size(); }
    std::size_t total_bytes() const
    {
        std::size_t total = 0;
        for (auto b : bytes) total += b;
        return total;
    }

    std::vector<OutputRecordPtr> records;
    std::vector<std::size_t> bytes;
};

namespace {

std::size_t limit_for(const std::string& config_name, const char* key, int fallback)
{
    auto& config = Config::instance();
    if (config.has(config_name, key)) return static_cast<std::size_t>(config.getInt(config_name, key, fallback));
    return static_cast<std::size_t>(config.getInt("writers_config", key, fallback));
}

} // namespace

// -------------------- 构造 / 析构 --------------------
BaseWriter::BaseWriter(std::string name, std::string type, std::string config_name)
    : name_(std::move(name)),
      type_(std::move(type)),
      config_name_(std::move(config_name)),
      buf_capacity_(static_cast<std::size_t>(Config::instance().getInt("writers_config", "buffer_capacity", 1024))),
      max_pending_records_(limit_for(config_name_, "max_pending_records", 4096)),
      max_pending_bytes_(limit_for(config_name_, "max_pending_bytes", 67108864)),
      max_record_bytes_(limit_for(config_name_, "max_record_bytes", 8388608)),
      front_(std::make_unique<Buffer>(buf_capacity_)),
      back_(std::make_unique<Buffer>(buf_capacity_))
{
    if (max_pending_records_ == 0 || max_pending_bytes_ == 0 || max_record_bytes_ == 0)
        throw std::invalid_argument("BaseWriter '" + name_ + "': queue limits must be positive integers");
    if (max_record_bytes_ > max_pending_bytes_)
        throw std::invalid_argument("BaseWriter '" + name_ + "': max_record_bytes cannot exceed max_pending_bytes");
    // 所有 worker 可见字段初始化完成后才启动线程。
    flush_thread_ = std::thread(&BaseWriter::flush_worker, this);
}

BaseWriter::~BaseWriter()
{
    shutdown();
}

void BaseWriter::shutdown()
{
    // Serialize shutdown callers so cleanup cannot run before another caller's join.
    std::lock_guard shutdown_lock(shutdown_mtx_);
    {
        std::lock_guard lg(mtx_);
        if (closing_) return;
        closing_ = true;
    }
    cv_.notify_one();
    if (flush_thread_.joinable())
        flush_thread_.join();
    do_shutdown();
    spdlog::info("BaseWriter: shutdown complete for writer '{}'", name_);
}

// -------------------- 公有接口 --------------------
EnqueueCode BaseWriter::try_enqueue(OutputRecordPtr record)
{
    if (!record) {
        std::lock_guard<std::mutex> lg(mtx_);
        ++stats_.rejected_invalid;
        return EnqueueCode::Failed;
    }
    // 锁外完成 kind 校验与字节估算。
    const OutputKind kind = kind_of(record->payload);
    const std::size_t bytes = record->accounted_bytes != 0
                                  ? record->accounted_bytes
                                  : estimate_output_bytes(record->payload);

    std::lock_guard<std::mutex> lg(mtx_);
    if (closing_) {
        ++stats_.rejected_closed;
        return EnqueueCode::Closed;
    }
    if (!accepts(kind)) {
        ++stats_.rejected_invalid;
        return EnqueueCode::Unsupported;
    }
    if (bytes > max_record_bytes_) {
        ++stats_.rejected_too_large;
        return EnqueueCode::TooLarge;
    }
    if (stats_.pending_records + 1 > max_pending_records_
        || bytes > max_pending_bytes_ - stats_.pending_bytes) {
        ++stats_.rejected_full;
        return EnqueueCode::QueueFull;
    }
    stats_.pending_records += 1;
    stats_.pending_bytes += bytes;
    ++stats_.accepted;
    stats_.peak_pending_records = std::max(stats_.peak_pending_records, stats_.pending_records);
    stats_.peak_pending_bytes = std::max(stats_.peak_pending_bytes, stats_.pending_bytes);
    front_->records.push_back(std::move(record));
    front_->bytes.push_back(bytes);
    const bool notify = !need_flush_;
    need_flush_ = true;
    if (notify) cv_.notify_one();
    return EnqueueCode::Accepted;
}

nlohmann::json BaseWriter::output_status() const
{
    std::lock_guard<std::mutex> lg(mtx_);
    return {
        {"name", name_},
        {"type", type_},
        {"config_name", config_name_},
        {"status", closing_ ? "closing" : "running"},
        {"accepted", stats_.accepted},
        {"rejected_full", stats_.rejected_full},
        {"rejected_too_large", stats_.rejected_too_large},
        {"rejected_closed", stats_.rejected_closed},
        {"rejected_invalid", stats_.rejected_invalid},
        {"rejected_failed", stats_.rejected_failed},
        {"pending_records", stats_.pending_records},
        {"pending_bytes", stats_.pending_bytes},
        {"peak_pending_records", stats_.peak_pending_records},
        {"peak_pending_bytes", stats_.peak_pending_bytes},
        {"released_records", stats_.released_records},
        {"flush_errors", stats_.flush_errors},
        {"limits", {
            {"buffer_capacity", buf_capacity_},
            {"max_pending_records", max_pending_records_},
            {"max_pending_bytes", max_pending_bytes_},
            {"max_record_bytes", max_record_bytes_}
        }}
    };
}

// -------------------- 保护 / 私有实现 --------------------
bool BaseWriter::flush_impl(const std::vector<OutputRecordPtr>&)
{
    spdlog::error("BaseWriter: writer '{}' ({}) does not implement flush_impl", name_, type_);
    return false;
}

void BaseWriter::on_flush_error(const std::vector<OutputRecordPtr>& batch)
{
    spdlog::error("BaseWriter: flush failed for writer '{}', attempted_records={}", name_, batch.size());
}

bool BaseWriter::reserve_additional_bytes(size_t bytes)
{
    std::lock_guard<std::mutex> lg(mtx_);
    if (bytes > max_pending_bytes_ - stats_.pending_bytes) return false;
    stats_.pending_bytes += bytes;
    stats_.peak_pending_bytes = std::max(stats_.peak_pending_bytes, stats_.pending_bytes);
    return true;
}

void BaseWriter::release_additional_bytes(size_t bytes)
{
    std::lock_guard<std::mutex> lg(mtx_);
    stats_.pending_bytes = bytes >= stats_.pending_bytes ? 0 : stats_.pending_bytes - bytes;
}

void BaseWriter::release_budget(size_t records, size_t bytes)
{
    stats_.pending_records = records >= stats_.pending_records ? 0 : stats_.pending_records - records;
    stats_.pending_bytes = bytes >= stats_.pending_bytes ? 0 : stats_.pending_bytes - bytes;
    stats_.released_records += records;
}

void BaseWriter::flush_worker()
{
    std::unique_lock<std::mutex> lk(mtx_);
    for (;;)
    {
        cv_.wait(lk, [this] { return closing_ || need_flush_; });
        if (closing_ && front_->size() == 0){
            spdlog::info("BaseWriter: flush worker stopping...");
            break;
        }

        front_.swap(back_);
        need_flush_ = false;
        lk.unlock();

        flush_buffer(*back_);

        lk.lock();
        release_budget(back_->size(), back_->total_bytes());
        back_->clear();
    }
}

void BaseWriter::flush_buffer(const Buffer& buf)
{
    bool flush_ret = true;
    auto start = std::chrono::steady_clock::now();

    if (!buf.records.empty())
    {
        spdlog::debug("BaseWriter: flushing {} items for writer '{}'", buf.records.size(), name_);
        bool native_ret = false;
        try {
            native_ret = flush_impl(buf.records);
        } catch(const std::exception& e){
            spdlog::error("BaseWriter: flush_impl failed for writer '{}': {}", name_, e.what());
        } catch(...){
            spdlog::error("BaseWriter: flush_impl failed for writer '{}': unknown exception", name_);
        }
        if (!native_ret) {
            flush_ret = false;
            try {
                on_flush_error(buf.records);
            } catch(const std::exception& e){
                spdlog::error("BaseWriter: on_flush_error failed for writer '{}': {}", name_, e.what());
            } catch(...){
                spdlog::error("BaseWriter: on_flush_error failed for writer '{}': unknown exception", name_);
            }
        }
    }

    if (!flush_ret) {
        std::lock_guard<std::mutex> lg(mtx_);
        ++stats_.flush_errors;
    }

    if (use_perf && buf.size() > 0){
        if (!flush_ret) perf_->err_cnt++;
        auto us = std::chrono::duration<double, std::micro>(
                  std::chrono::steady_clock::now() - start).count();
        perf_->append(us);
    }
}
