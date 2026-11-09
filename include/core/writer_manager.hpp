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
#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <unordered_map>
#include <string>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include "common/perf_counter.hpp"
#include "writer/base_writer.hpp"


class WriterManager {
public:
    ~WriterManager();

    /// 显式、幂等的初始化入口。路由绑定和 System 采集器自动启动之前必须调用。
    void initialize();
    void shutdown();

    BaseWriter* find(const std::string& name);
    const BaseWriter* find(const std::string& name) const;
    std::vector<std::string> names() const;

    /// 进程运行标识 + 采样序号，用于构造稳定的 sample_id / record_id。
    std::string process_run_id() const { return process_run_id_; }
    uint64_t next_sample_counter() { return sample_counter_.fetch_add(1, std::memory_order_relaxed); }

    nlohmann::json output_status() const;

    template <typename T>
    void registerWriter(std::string name) {
        factories_.emplace(std::move(name), [](std::string name, std::string type, std::string config_name) -> std::shared_ptr<BaseWriter> {
            return std::make_shared<T>(name, type, config_name);
        });
    };

    void registerHelp(const std::string& name, WriterHelpInfo info) {
        help_[name] = std::move(info);
    }

    std::vector<std::string> list() const;

    static WriterManager& instance();

    const std::string getFmtHelp(const std::string& name, int width) const;

private:
    using Factory = std::function<std::shared_ptr<BaseWriter>(std::string name, std::string type, std::string config_name)>;
    std::unordered_map<std::string, Factory> factories_;
    // Shared ownership: a status snapshot hands out shared_ptr copies under
    // the manager lock and uses them outside it, so a concurrent shutdown can
    // swap and shut the writers down without destroying one mid-query.
    std::vector<std::shared_ptr<BaseWriter>> writers_;
    mutable std::mutex m_;
    bool initialized_ = false;
    std::string process_run_id_;
    std::atomic<uint64_t> sample_counter_{0};
    std::unordered_map<std::string, WriterHelpInfo> help_;
    void createAllWritersLocked();
    WriterManager();
    void addWriter(std::shared_ptr<BaseWriter> writer);
    // writer perf
    void initWriterPerf();
    bool enable_writer_perf = false;
    bool perf_inited = false;
    int perf_win_size = 0;
    bool status_rpc_registered = false;

};

#define AUTO_REGISTER_WRITER(WriterClass, ...)        \
namespace {                                                     \
    struct AutoReg_##WriterClass {                           \
        AutoReg_##WriterClass() {                            \
            WriterManager::instance()                       \
                .registerWriter<WriterClass>(#WriterClass);    \
        }                                                       \
    };                                                          \
    static AutoReg_##WriterClass _auto_reg_##WriterClass;         \
    struct AutoHelpReg_##WriterClass {                                \
        AutoHelpReg_##WriterClass() {                                 \
            WriterManager::instance()                                \
                .registerHelp(#WriterClass, {__VA_ARGS__});           \
        }                                                                \
    };                                                                   \
    static AutoHelpReg_##WriterClass _auto_help_reg_##WriterClass; \
}
