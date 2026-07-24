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
#include "core/collector_manager_utils.hpp"

#include "common/config.hpp"

#include <spdlog/spdlog.h>

std::vector<OnFinish> resolveCollectorFinishCallbacks(
    const std::string& collector_name,
    const std::string& config_name,
    const FinishCallbackMap& finish_callbacks,
    const std::vector<std::string>& default_callback_names) {
    std::vector<OnFinish> callbacks;
    try {
        auto writers = Config::instance().getArray<std::string>(config_name, "use_writers");
        for (const auto& writer_name: writers){
            if(!finish_callbacks.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            callbacks.push_back(finish_callbacks.at(writer_name));
        }
    }
    catch(const std::exception& e){
        spdlog::info("CollectorScheduler: can not get collect {} writers, use default writers", collector_name);
        for (const auto& writer_name: default_callback_names){
            if(!finish_callbacks.count(writer_name)){
                spdlog::error("CollectorScheduler: writer name {} is error, skip it", writer_name);
                continue;
            }
            callbacks.push_back(finish_callbacks.at(writer_name));
        }
    }
    return callbacks;
}

double resolveCollectorFreq(
    const std::string& collector_name,
    const std::string& config_name,
    double default_freq,
    bool allow_period_key) {
    if (Config::instance().has(config_name, "freq")) {
        return Config::instance().getDouble(config_name, "freq");
    }
    if (allow_period_key && Config::instance().has(config_name, "period")) {
        auto period = Config::instance().getDouble(config_name, "period");
        return 1.0 / period;
    }
    spdlog::info("CollectorScheduler: can not get collect {} freq, use default freq", collector_name);
    return default_freq;
}

EventBatchConfig resolveEventBatchConfig(const std::string& config_name) {
    EventBatchConfig config;
    if (Config::instance().has(config_name, "flush_threshold")) {
        config.flush_threshold = Config::instance().getInt(config_name, "flush_threshold");
    }
    if (Config::instance().has(config_name, "max_batch_size")) {
        config.max_batch_size = Config::instance().getInt(config_name, "max_batch_size");
    }
    if (Config::instance().has(config_name, "max_wait_ms")) {
        config.max_wait = std::chrono::milliseconds(Config::instance().getInt(config_name, "max_wait_ms"));
    }
    return config;
}

bool resolveAutoStart(const std::string& config_name) {
    return Config::instance().getBool(config_name, "auto_start", false);
}
