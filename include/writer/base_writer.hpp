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
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include <optional>

#include <spdlog/spdlog.h>

#include "core/collector_type.h"
#include "common/config.hpp"
#include "common/perf_counter.hpp"
#include <nlohmann/json.hpp>

// 前向声明 fmt 为头文件减负；cpp 里再真正 include <fmt/core.h>
namespace fmt {}  // 占位，无实质依赖

using write_data = std::tuple<std::string,
                              const Job,
                              const std::any,
                              std::chrono::system_clock::time_point>;


using ConfigParam = std::pair<std::string, std::string>; // <param_name, description>
using ConfigParams = std::vector<ConfigParam>;
struct WriterHelpInfo {
    std::string help_text;
    ConfigParams config_params; //<param_name, description>
};

class BaseWriter
{
public:
    explicit BaseWriter(std::string name, std::string type, std::string config_name);
    virtual ~BaseWriter();
    virtual void do_shutdown(){}; // 子类可重载
    void shutdown() {
        {
            std::lock_guard lg(mtx_);
            stop_ = true;
            need_flush_ = false;
        }
        
        cv_.notify_one();
        spdlog::info("BaseWriter: shutting down...");
        if (flush_thread_.joinable()){
            spdlog::info("BaseWriter: waiting for flush worker to finish...");
            flush_thread_.join();
        }
        flush_buffer(*front_);
        do_shutdown();
        spdlog::info("BaseWriter: shutdown complete for writer '{}'", name_);
    }

    void on_finish(std::string collect_name,
                   const Job job,
                   const std::any data,
                   std::chrono::system_clock::time_point ts);

    OnFinish get_onFinishCallback();

    std::string get_name(void){
        return this->name_;
    };

    std::string get_type(void){
        return this->type_;
    };

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
    virtual bool flush_impl(const std::vector<write_data>& batch);
    void write(const write_data& t);
    std::string name_;
    std::string type_;
    std::string config_name_;

private:
    struct Buffer;

    
    void flush_worker();
    void flush_buffer(const Buffer& buf);
    void trigger_async_flush();
    
    const std::size_t buf_capacity_;
    std::unique_ptr<Buffer> front_;
    std::unique_ptr<Buffer> back_;

    std::mutex mtx_;
    std::condition_variable cv_;
    std::thread flush_thread_;
    bool stop_ = false;
    bool need_flush_ = false;

    //perf
    std::unique_ptr<PerfCounter> perf_;
    bool use_perf;

};