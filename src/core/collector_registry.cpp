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
#include "core/collector_execution_context.hpp"
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
                (void)req;
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

NativeCollectFunc CollectorRegistry::makeNativePerfFunc(std::string name, NativeCollectFunc func) {
    std::unique_ptr<PerfCounter>& slot = collector_perf_[name];
    if (!slot) slot = std::make_unique<PerfCounter>(perf_win_size);
    PerfCounter* perf = slot.get();

    return [func, perf](const Job& job, OutputContext& output) -> CollectOutcome {
        auto start = std::chrono::steady_clock::now();
        CollectOutcome ret;
        try {
            ret = func(job, output);
        } catch (...) {
            perf->err_cnt++;
            throw;
        }
        auto us = std::chrono::duration<double, std::micro>(
                      std::chrono::steady_clock::now() - start).count();
        perf->append(us);
        return ret;
    };
}

SystemCollectFunc CollectorRegistry::makeSystemPerfFunc(std::string name, SystemCollectFunc func) {
    std::unique_ptr<PerfCounter>& slot = collector_perf_[name];
    if (!slot) slot = std::make_unique<PerfCounter>(perf_win_size);
    PerfCounter* perf = slot.get();

    return [func, perf](OutputContext& output) -> CollectOutcome {
        auto start = std::chrono::steady_clock::now();
        CollectOutcome ret;
        try {
            ret = func(output);
        } catch (...) {
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
    if (!perf_inited) initCollectorPerf();

    auto info = collectors_.find(type);
    if (info == collectors_.end()) {
        CollectorHandle error;
        error.error = "collector type '" + type + "' is not registered";
        return error;
    }

    auto newInst = info->second.factory();
    if (!newInst) {
        CollectorHandle error;
        error.error = "collector type '" + type + "' returned a null instance";
        return error;
    }

    newInst->set_name(name);
    newInst->set_type(type);
    newInst->set_scope(info->second.scope);
    newInst->set_trigger(info->second.trigger);

    auto* inst = newInst.get();
    auto* periodic_job = dynamic_cast<IPeriodicJobCollector*>(inst);
    auto* periodic_system = dynamic_cast<IPeriodicSystemCollector*>(inst);
    auto* event_job = dynamic_cast<IEventJobCollector*>(inst);
    auto* event_system = dynamic_cast<IEventSystemCollector*>(inst);

    const int matches = (periodic_job ? 1 : 0) + (periodic_system ? 1 : 0) +
                        (event_job ? 1 : 0) + (event_system ? 1 : 0);
    if (matches != 1) {
        CollectorHandle error;
        error.error = "collector type '" + type + "' must implement exactly one work interface, found " +
                      std::to_string(matches);
        return error;
    }

    const auto scope = info->second.scope;
    const auto trigger = info->second.trigger;
    const bool shape_ok =
        (scope == CollectorScope::Job && trigger == CollectorTrigger::Periodic && periodic_job) ||
        (scope == CollectorScope::System && trigger == CollectorTrigger::Periodic && periodic_system) ||
        (scope == CollectorScope::Job && trigger == CollectorTrigger::Event && event_job) ||
        (scope == CollectorScope::System && trigger == CollectorTrigger::Event && event_system);
    if (!shape_ok) {
        CollectorHandle error;
        error.error = "collector type '" + type + "' interface does not match scope " +
                      std::string(to_string(scope)) + " and trigger " + to_string(trigger);
        return error;
    }

    auto keepalive = std::shared_ptr<ICollector>(newInst.release());
    CollectorHandle handle;
    handle.scope = scope;
    handle.trigger = trigger;
    handle.channels = inst->output_channels();
    handle.supports_final = inst->supports_final();
    handle.init = [keepalive](const nlohmann::json& cfg, CollectorInitContext& context) {
        return keepalive->init(cfg, context);
    };
    handle.deinit = [keepalive]() { keepalive->deinit(); };
    handle.on_job_added = [keepalive](const Job& job) { keepalive->on_job_added(job); };
    handle.on_job_updated = [keepalive](const Job& job) { keepalive->on_job_updated(job); };
    handle.on_job_removed = [keepalive](const Job& job) { keepalive->on_job_removed(job); };

    if (periodic_job) {
        NativeCollectFunc fn = [periodic_job](const Job& job, OutputContext& output) {
            return periodic_job->collect(job, output);
        };
        if (enable_collector_perf) fn = makeNativePerfFunc(name, std::move(fn));
        handle.binding = PeriodicJobBinding{std::move(fn)};
    } else if (periodic_system) {
        SystemCollectFunc fn = [periodic_system](OutputContext& output) {
            return periodic_system->collect(output);
        };
        if (enable_collector_perf) fn = makeSystemPerfFunc(name, std::move(fn));
        handle.binding = PeriodicSystemBinding{std::move(fn)};
    } else if (event_job) {
        EventJobFunc fn = [event_job](const ReadySources& sources, const WorkBudget& budget,
                                      JobEventContext& context) {
            return event_job->on_ready(sources, budget, context);
        };
        handle.binding = EventJobBinding{std::move(fn)};
    } else {
        EventSystemFunc fn = [event_system](const ReadySources& sources, const WorkBudget& budget,
                                            SystemEventContext& context) {
            return event_system->on_ready(sources, budget, context);
        };
        handle.binding = EventSystemBinding{std::move(fn)};
    }
    return handle;
}

CollectorScope CollectorRegistry::getScope(const std::string& name) const {
    auto it = collectors_.find(name);
    if (it == collectors_.end())
        return CollectorScope::Undefined;                     // 空句柄，调用方可判空
    return it->second.scope;
}

CollectorTrigger CollectorRegistry::getTrigger(const std::string& name) const {
    auto it = collectors_.find(name);
    if (it == collectors_.end()) return CollectorTrigger::Periodic;
    return it->second.trigger;
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
