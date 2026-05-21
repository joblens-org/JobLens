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

#include <stdexcept>
#include <tuple>
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

}

WriterManager::~WriterManager() {

}

void WriterManager::createAllWriters(){
    if(initialized_){
        spdlog::warn("WriterManager: createAllWriters called more than once!");
        return;
    }
    initialized_ = true;
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
    for (const auto& writer : writers){
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
            addWriter(std::move(newInst));
            spdlog::info("WriterManager: registered writer '{}' of type '{}'", writer.name, writer.type);
        }catch(const std::exception& e){
            spdlog::error("WriterManager: exception creating writer '{}': {}", writer.name, e.what());
            continue;
        }
    }
    spdlog::info("WriterManager: initialized with {} writers", writers_.size());
}

void WriterManager::shutdown() {
    std::lock_guard lg(m_);
    spdlog::info("WriterManager: shutting down...");
    for (auto& writer : writers_) {
        writer->shutdown();
    }
    // writers_.clear();
}

std::vector<std::tuple<std::string, OnFinish>> WriterManager::get_onFinishCallbacks() {
    if(!initialized_){
        initWriterPerf();
        createAllWriters();
    }
    std::lock_guard lg(m_);
    std::vector<std::tuple<std::string, OnFinish>> callbacks;
    for (const auto& writer : writers_) {
        auto name = writer->get_name();
        auto func = writer->get_onFinishCallback();
        std::tuple<std::string, OnFinish> t = std::make_tuple(name,func);
        callbacks.push_back(t);
    }
    return callbacks;
}

void WriterManager::addWriter(std::unique_ptr<BaseWriter> writer) {
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