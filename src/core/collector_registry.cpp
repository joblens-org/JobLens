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
// collector_registry.cpp
#include "core/collector_registry.hpp"
#include "core/collector_type.h"
#include "common/local_rpc.hpp"
#include <mutex>
#include <iostream>
#include <spdlog/spdlog.h>
#include <thread>


CollectorRegistry& CollectorRegistry::instance() {
    static CollectorRegistry reg;
    return reg;
}

void CollectorRegistry::initCollectorPerf() {
    enable_collector_perf = Config::instance().getBool("collectors_config", "enable_collector_perf");
    if (enable_collector_perf) {
        perf_win_size = Config::instance().getInt("collectors_config", "perf_window_size");

        RPCServer::instance().register_method("CollectorRegistry/CollectorsPerfCount",
            [this](const nlohmann::json& req) -> nlohmann::json {
                nlohmann::json j;
                j["status"] = "ok";
                j["collectors_perf"] = json::array();
                try {
                    for (auto& k: collector_perf_){
                        auto s = k.second->snapshot();
                        nlohmann::json count;
                        count["name"] = k.first;
                        count["call_cnt"] = s.call_cnt;
                        count["err_cnt"] = s.err_cnt;
                        count["max_us"] = s.max_us;
                        count["mean_us"] = s.mean_us;
                        count["min_us"] = s.min_us;
                        count["variance"] = s.variance;
                        j["collectors_perf"].push_back(count);
                    }
                } catch (const std::exception& e) {
                    j["status"] = "error";
                    j["msg"] = fmt::format("failed to dump perf info: {}", e.what());
                }
                return j;
            }
        );
    } else {
        perf_win_size = 0;
    }

    perf_inited = true;
}

CollectFunc CollectorRegistry::makePerfFunc(std::string name, CollectFunc func) {
    std::unique_ptr<PerfCounter>& perf = collector_perf_[name];
    if (!perf) {
        perf = std::make_unique<PerfCounter>(perf_win_size);
    }

    return [func, &perf](const Job& job) -> std::any {
        auto start = std::chrono::steady_clock::now();
        std::any ret;
        bool ok = true;
        try {
            ret = func(job); 
            perf->call_cnt++;
        } catch (...) {
            ok = false;
            perf->err_cnt++;
            throw;
        }
        auto us = std::chrono::duration<double, std::micro>(
                      std::chrono::steady_clock::now() - start).count();
        perf->append(us);
        return ret;
    };
}

CollectorHandle CollectorRegistry::createCollector(const std::string& type, const std::string& name) {
    if (!perf_inited){
        initCollectorPerf();
    }
    auto it = collectors_.find(type);
    if (it == collectors_.end())
        return {};                     // 空句柄，调用方可判空
    
    auto newInst = it->second.factory();  // 调用工厂函数
    if (!newInst)                  // 工厂可能返回 nullptr
        return {};
    

    // 存储实例（注意：这里需要重新考虑实例存储策略）
    // 从逻辑上来说，jobinfocollect不会触发销毁collector，只会触发init和deinit过程，所以这里是安全的。
    newInst->set_name(name);
    newInst->set_type(type);
    auto inst = newInst.get();
    collector_instances_[name] = std::move(newInst);
    
    CollectFunc c_func;

    if (enable_collector_perf) {
        c_func = makePerfFunc(name, [inst](const Job& job) { return inst->collect(job); });
    }else {
        c_func = [inst](const Job& job) { return inst->collect(job); };
    }

    if (it->second.scope == CollectorScope::Job) {
        return {
            [inst](const nlohmann::json& cfg) { return inst->init(cfg); },
            c_func,
            [inst](){ inst->deinit(); }
        };
    } else { // System scope
        return {
            [inst](const nlohmann::json& cfg) { return inst->init(cfg); },
            c_func, //保证接口一致，忽略job参数
            [inst](){ inst->deinit(); }
        };
    }
}

CollectorScope CollectorRegistry::getScope(const std::string& name) const {
    auto it = collectors_.find(name);
    if (it == collectors_.end())
        return CollectorScope::Undefined;                     // 空句柄，调用方可判空
    return it->second.scope;
}

const CollectorHelpInfo* CollectorRegistry::getHelp(const std::string& name) const {
    auto it = collectors_.find(name);
    return it == collectors_.end() ? nullptr : &it->second.help_info;
}

const std::string CollectorRegistry::getFmtHelp(const std::string& name, int width) const{
    auto it = collectors_.find(name);
    if (it == collectors_.end()) return "No help available.";
    
    const auto& info = it->second.help_info;
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

std::vector<std::string> CollectorRegistry::list() const {
    std::vector<std::string> out;
    for (const auto& [k, _] : collectors_) out.push_back(k);
    return out;
}

CollectDataParseFunc CollectorRegistry::getCollectorParser(const std::string& collector_name, const std::string& writer_type){
    if(!collector_instances_.count(collector_name)){
        spdlog::error("CollectorRegistry: no such collector instance: {}", collector_name);
        return nullptr;
    }
    return collector_instances_[collector_name]->get_writer_parser(writer_type);
}

std::string CollectorRegistry::getCollectorType(const std::string& collector_name){
    if(!collector_instances_.count(collector_name)){
        spdlog::error("CollectorRegistry: no such collector instance: {}", collector_name);
        return "";
    }
    return collector_instances_[collector_name]->get_type();
}
