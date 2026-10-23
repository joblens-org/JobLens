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


#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include <optional>

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "core/collector_type.h"
#include "core/output_record.h"
#include "common/config.hpp"
#include "common/perf_counter.hpp"

using ConfigParam = std::pair<std::string, std::string>; // <param_name, description>
using ConfigParams = std::vector<ConfigParam>;
struct WriterHelpInfo {
    std::string help_text;
    ConfigParams config_params; //<param_name, description>
};

/// 所有后端 writer 的基类。
///
/// 负责：有界队列准入、前后缓冲交换、后台 flush 线程、预算记账与观测。
/// 子类只负责把一批已准备好的不可变记录写到后端。
class BaseWriter
{
public:
    explicit BaseWriter(std::string name, std::string type, std::string config_name);
    // Derived destructors must call shutdown() before their sink state is destroyed.
    virtual ~BaseWriter();
    virtual void do_shutdown(){}; // 子类可重载
    void shutdown();

    /// 有界入队。校验在锁外完成；容量检查与预算预留在同一临界区内完成。
    /// 不接受 payload 类型时返回 Unsupported； никогда 不等待队列空位。
    EnqueueCode try_enqueue(OutputRecordPtr record);

    std::string get_name(void) const { return name_; }
    std::string get_type(void) const { return type_; }
    std::string get_config_name(void) const { return config_name_; }

    /// 该 writer 是否接受某种 payload。默认不接受，由各 writer 显式声明。
    virtual bool accepts(OutputKind kind) const { (void)kind; return false; }

    /// 队列/生命周期快照。不等待 flush，不持 collector 锁。
    nlohmann::json output_status() const;

    void set_perf(bool use, int window_size){
        use_perf = use;
        if (use){
            perf_ = std::make_unique<PerfCounter>(window_size);
        }
        return;
    }

    std::optional<PerfCounter::Snapshot> get_perf_snapshot(){
        if (use_perf){
            return perf_->snapshot();
        }
        return std::nullopt;
    }
    
protected:
    /// 唯一的后端写入接口：writer 只消费已准备好的记录。
    virtual bool flush_impl(const std::vector<OutputRecordPtr>& batch);
    /// 原生批次失败回调。引用仅在回调期间有效。
    virtual void on_flush_error(const std::vector<OutputRecordPtr>& batch);
    /// 为已编码的重试副本额外预留字节预算；失败表示预算耗尽，调用方必须终止该重试。
    bool reserve_additional_bytes(size_t bytes);
    void release_additional_bytes(size_t bytes);
    std::string name_;
    std::string type_;
    std::string config_name_;

private:
    struct Buffer;
    struct Stats {
        uint64_t accepted = 0;
        uint64_t rejected_full = 0;
        uint64_t rejected_too_large = 0;
        uint64_t rejected_closed = 0;
        uint64_t rejected_invalid = 0;
        uint64_t rejected_failed = 0;
        uint64_t released_records = 0;
        uint64_t flush_errors = 0;
        size_t pending_records = 0;
        size_t pending_bytes = 0;
        size_t peak_pending_records = 0;
        size_t peak_pending_bytes = 0;
    };

    void flush_worker();
    void flush_buffer(const Buffer& buf);
    void release_budget(size_t records, size_t bytes);

    size_t buf_capacity_;
    size_t max_pending_records_;
    size_t max_pending_bytes_;
    size_t max_record_bytes_;
    std::unique_ptr<Buffer> front_;
    std::unique_ptr<Buffer> back_;

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::mutex shutdown_mtx_;
    bool closing_ = false;
    bool need_flush_ = false;
    Stats stats_;

    //perf
    std::unique_ptr<PerfCounter> perf_;
    bool use_perf = false;

    // Start only after every field used by the worker has been initialized.
    std::thread flush_thread_;

};
