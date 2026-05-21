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

#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <unordered_map>
#include <string>
#include <spdlog/spdlog.h>
#include "common/perf_counter.hpp"
#include "writer/base_writer.hpp"


class WriterManager {
public:
    ~WriterManager();
    
    void shutdown();

    std::vector<std::tuple<std::string, OnFinish>> get_onFinishCallbacks();

    std::string getWriterType(const std::string& name){
        for(auto& writer: writers_){
            if(writer->get_name().compare(name) == 0){
                return writer->get_type();
            }
        }
        return "";
    }

    template <typename T>
    void registerWriter(std::string name) {
        factories_.emplace(std::move(name), [](std::string name, std::string type, std::string config_name) -> std::unique_ptr<BaseWriter> {
            return std::make_unique<T>(name, type, config_name);
        });
    };

    void registerHelp(const std::string& name, WriterHelpInfo info) {
        help_[name] = std::move(info);
    }

    std::vector<std::string> list() const;

    static WriterManager& instance();

    const std::string getFmtHelp(const std::string& name, int width) const;

private:
    using Factory = std::function<std::unique_ptr<BaseWriter>(std::string name, std::string type, std::string config_name)>;
    std::unordered_map<std::string, Factory> factories_;
    std::vector<std::unique_ptr<BaseWriter>> writers_;
    std::mutex m_;
    bool initialized_ = false;
    std::unordered_map<std::string, WriterHelpInfo> help_;
    void createAllWriters();
    WriterManager();
    void addWriter(std::unique_ptr<BaseWriter> writer);
    // writer perf
    void initWriterPerf();
    bool enable_writer_perf;
    bool perf_inited;
    int perf_win_size;

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
