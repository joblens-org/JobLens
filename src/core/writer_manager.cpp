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
#include "core/writer_manager.hpp"

#include "common/config.hpp"
#include "common/print_fmt.hpp"
#include <nlohmann/json.hpp>
#include "core/collector_type.h"
#include "common/local_rpc.hpp"

#include <random>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <unordered_set>
#include <fmt/core.h>


void WriterManager::initWriterPerf() {
    enable_writer_perf = Config::instance().getBool("writers_config", "enable_writer_perf", true);
    if (enable_writer_perf) {
        perf_win_size = Config::instance().getInt("writers_config", "perf_window_size", 1000);

        RPCServer::instance().register_method("WriterManager/WriterPerfCount",
            [this](const nlohmann::json& req) -> nlohmann::json {
                nlohmann::json j;
                j["status"] = "ok";
                j["writers_perf"] = json::array();
                try {
                    std::lock_guard<std::mutex> lg(m_);
                    for (auto& k: writers_){
                        auto optional_s = k->get_perf_snapshot();
                        if (optional_s.has_value()){
                            auto& s = optional_s.value();
                            nlohmann::json count;
                            count["name"] = k->get_name();
                            count["call_cnt"] = s.call_cnt;
                            count["err_cnt"] = s.err_cnt;
                            count["max_us"] = s.max_us;
                            count["mean_us"] = s.mean_us;
                            count["min_us"] = s.min_us;
                            count["variance"] = s.variance;
                            j["writers_perf"].push_back(count);
                        }
                    }
                } catch (const std::exception& e) {
                    j["status"] = "error";
                    j["msg"] = fmt::format("failed to dump perf info: {}", e.what());
                }
                return j;
            }
        );

    }else {
        perf_win_size = 0;
    }
    perf_inited = true;
}

WriterManager::WriterManager() {
    // WriterManager is constructed by the AUTO_REGISTER_WRITER static initializers
    // before main(). Touch the spdlog registry here so it is constructed no later
    // than WriterManager; static destruction order is then guaranteed to destroy
    // WriterManager (and its writers, which log during shutdown) first, rather
    // than logging into an already-destroyed spdlog registry at process exit.
    (void)spdlog::default_logger_raw();

    // 128-bit 进程运行标识：由操作系统随机源生成，配合单调采样计数器构成 sample_id。
    std::random_device device;
    std::ostringstream os;
    os << std::hex << device() << device() << device() << device();
    process_run_id_ = os.str();
}

WriterManager::~WriterManager() {
    shutdown();
}

void WriterManager::initialize(){
    std::lock_guard lg(m_);
    if (initialized_) return;
    initialized_ = true;
    if (!perf_inited) initWriterPerf();
    if (!status_rpc_registered) {
        status_rpc_registered = true;
        // 始终可用，不依赖性能计数器开关。
        RPCServer::instance().register_method("WriterManager/output_status",
            [this](const nlohmann::json&) -> nlohmann::json {
                return output_status();
            });
    }
    createAllWritersLocked();
}

void WriterManager::createAllWritersLocked(){
    struct Writer{
        std::string name;
        std::string type;
        std::string config;
    };
    auto global_config = Config::instance();
    spdlog::info("WriterManager: initializing...");
    auto writers = global_config.getArray<Writer>("writers_config", "writers",
        [](const YAML::Node& node) {
            Writer w;
            w.name = node["name"].as<std::string>();
            w.type = node["type"].as<std::string>();
            w.config = node["config"].as<std::string>();
            return w;
        });
    spdlog::info("WriterManager: found {} writers", writers.size());
    std::unordered_set<std::string> names;
    for (const auto& writer : writers){
        if (!names.insert(writer.name).second) {
            spdlog::error("WriterManager: duplicate writer instance name '{}', skipping", writer.name);
            continue;
        }
        auto it = factories_.find(writer.type);
        if (it == factories_.end()){
            spdlog::error("WriterManager: unknown writer type '{}', skipping", writer.type);
            continue;
        }
        try{
            auto newInst = it->second(writer.name, writer.type, writer.config);  // 调用工厂
            if (!newInst){
                spdlog::error("WriterManager: factory for writer type '{}' returned nullptr, skipping", writer.type);
                continue;
            }
            newInst->set_perf(enable_writer_perf, perf_win_size);
            writers_.push_back(std::move(newInst));
            spdlog::info("WriterManager: registered writer '{}' of type '{}'", writer.name, writer.type);
        }catch(const std::exception& e){
            spdlog::error("WriterManager: exception creating writer '{}': {}", writer.name, e.what());
            continue;
        }
    }
    spdlog::info("WriterManager: initialized with {} writers", writers_.size());
}

void WriterManager::shutdown() {
    std::vector<std::shared_ptr<BaseWriter>> writers;
    {
        std::lock_guard lg(m_);
        spdlog::info("WriterManager: shutting down...");
        writers.swap(writers_);
    }
    // 不持 registry 锁 join worker，避免与 writer 回调中的查询互相等待。
    for (auto& writer : writers) {
        writer->shutdown();
    }
    // The writers die when this vector leaves scope. A status query that
    // copied shared_ptrs under the manager lock keeps every writer it saw
    // alive until its snapshot is finished, even past this scope.
}

BaseWriter* WriterManager::find(const std::string& name)
{
    std::lock_guard lg(m_);
    for (auto& writer : writers_)
        if (writer->get_name() == name) return writer.get();
    return nullptr;
}

const BaseWriter* WriterManager::find(const std::string& name) const
{
    std::lock_guard lg(m_);
    for (const auto& writer : writers_)
        if (writer->get_name() == name) return writer.get();
    return nullptr;
}

std::vector<std::string> WriterManager::names() const
{
    std::lock_guard lg(m_);
    std::vector<std::string> result;
    result.reserve(writers_.size());
    for (const auto& writer : writers_) result.push_back(writer->get_name());
    return result;
}

nlohmann::json WriterManager::output_status() const
{
    // Snapshot under the lock, use outside it: the shared_ptr copies keep the
    // writers alive even if shutdown() swaps and destroys the set in between.
    std::vector<std::shared_ptr<BaseWriter>> writers;
    {
        std::lock_guard lg(m_);
        writers = writers_;
    }
    nlohmann::json result;
    result["status"] = "ok";
    result["process_run_id"] = process_run_id_;
    result["writer_count"] = writers.size();
    result["writers"] = nlohmann::json::array();
    for (const auto& writer : writers) result["writers"].push_back(writer->output_status());
    return result;
}

void WriterManager::addWriter(std::shared_ptr<BaseWriter> writer) {
    std::lock_guard lg(m_);
    writers_.push_back(std::move(writer));
}

std::vector<std::string> WriterManager::list() const{
    std::vector<std::string> names;
    for (const auto& writer : factories_) {
        names.push_back(writer.first);
    }
    return names;
}

WriterManager& WriterManager::instance() {
    static WriterManager instance;
    return instance;
}

const std::string WriterManager::getFmtHelp(const std::string& name, int width) const{
    auto it = help_.find(name);
    if (it == help_.end()) return "No help available.";
    const auto& info = it->second;
    std::ostringstream os;
    os << "\nTYPE:  " << name << '\n';
    os << wordWrapAlign(info.help_text, width, "HELP:  ", "       ") << '\n';

    if (!info.config_params.empty()) {
        // 1. 计算最长参数名
        size_t nameWidth = 0;
        for (auto& [n, _] : info.config_params)
            nameWidth = std::max(nameWidth, n.size());
        const size_t descStart = nameWidth + 4;          // 留 2 空格 + ": "

        os << "PARAMS:\n";
        for (auto& [name, desc] : info.config_params) {
            std::string firstPrefix = "  " + name + std::string(nameWidth - name.size(), ' ') + "  ";
            std::string nextPrefix(descStart + 2, ' ');  // 后续行对齐到同一列
            os << wordWrapAlign(desc, width, firstPrefix, nextPrefix);
        }
    }
    return os.str();
}
