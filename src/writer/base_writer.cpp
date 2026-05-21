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
#include <fmt/core.h>   // 仅在实现文件真正用到 fmt

// 内部类型完整定义
struct BaseWriter::Buffer
{
    explicit Buffer(std::size_t reserve) { vec.reserve(reserve); }
    void push_back(const write_data j) { vec.push_back(j); }
    void clear() { vec.clear(); }
    std::size_t size() const { return vec.size(); }
    std::vector<write_data> vec;
};

// -------------------- 构造 / 析构 --------------------
BaseWriter::BaseWriter(std::string name, std::string type, std::string config_name)
    : name_(std::move(name)),
      type_(std::move(type)),
      config_name_(std::move(config_name)),
      buf_capacity_(Config::instance().getInt("writers_config", "buffer_capacity")),
      front_(std::make_unique<Buffer>(buf_capacity_)),
      back_(std::make_unique<Buffer>(buf_capacity_)),
      flush_thread_(&BaseWriter::flush_worker, this)
{
}

BaseWriter::~BaseWriter()
{
    {
        std::lock_guard lg(mtx_);
        stop_ = true;
    }
    cv_.notify_one();
    if (flush_thread_.joinable())
        flush_thread_.join();

    flush_buffer(*front_);
}

// -------------------- 公有接口 --------------------
void BaseWriter::on_finish(std::string collect_name,
                            const Job job,
                            const std::any data,
                            std::chrono::system_clock::time_point ts)
{
    spdlog::debug("BaseWriter: on_finish called for writer '{}', collector '{}'", name_, collect_name);
    spdlog::debug("BaseWriter: job info: ID={}", job.JobID);
    auto t = std::make_tuple(collect_name, job, data, ts);
    write(std::move(t));
    trigger_async_flush();
}

OnFinish BaseWriter::get_onFinishCallback()
{
    return [this](const std::string& collect_name,
                  const Job& job,
                  const std::any data,
                  std::chrono::system_clock::time_point ts)
    { on_finish(collect_name, job, data, ts); };
}

// -------------------- 保护 / 私有实现 --------------------
bool BaseWriter::flush_impl(const std::vector<write_data>&)
{
    return true;
    // 默认空实现，留给派生类覆写
}

void BaseWriter::write(const write_data& t)
{
    
    spdlog::debug("BaseWriter: write called for writer '{}', collector '{}'", name_, std::get<0>(t));
    {
        std::lock_guard<std::mutex> lg(mtx_);
        front_->push_back(t);
    }
    if (front_->size() >= buf_capacity_)
        trigger_async_flush();
}

void BaseWriter::flush_worker()
{
    std::unique_lock<std::mutex> lk(mtx_);
    while (stop_ == false)
    {
        cv_.wait(lk, [this] { return stop_ || need_flush_; });
        if (stop_){
            spdlog::info("BaseWriter: flush worker stopping...");
            break;
        }

        front_.swap(back_);
        need_flush_ = false;
        lk.unlock();

        flush_buffer(*back_);
        back_->clear();
        lk.lock();
    }
}

void BaseWriter::flush_buffer(const Buffer& buf)
{
    bool flush_ret = false;

    if (!buf.vec.empty())
    {
        spdlog::debug("BaseWriter: flushing {} items for writer '{}'", buf.vec.size(), name_);
        auto start = std::chrono::steady_clock::now();
        if (use_perf){
            bool ok = true;
        }
        try{
            flush_ret = flush_impl(buf.vec);
        }catch(const std::exception& e){
            spdlog::error("BaseWriter: flush_impl failed for writer '{}': {}", name_, e.what());
            if (use_perf){
                perf_->err_cnt++;
            }
        }
        if (use_perf){
            perf_->call_cnt++;
            if (!flush_ret){
                perf_->err_cnt++;
            }
            // TODO: 这里需要实现报错调用计数，但是之前设计的逻辑并非这样，所以先空着
            auto us = std::chrono::duration<double, std::micro>(
                      std::chrono::steady_clock::now() - start).count();
            perf_->append(us);
        }
    }
        
}

void BaseWriter::trigger_async_flush()
{
    {
        std::lock_guard lg(mtx_);
        need_flush_ = true;
    }
    cv_.notify_one();
}