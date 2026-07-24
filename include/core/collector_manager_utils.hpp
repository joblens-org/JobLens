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

#include <chrono>
#include <string>
#include <vector>

#include "core/collector_runtime.hpp"

struct EventBatchConfig {
    size_t flush_threshold{100};
    size_t max_batch_size{1000};
    std::chrono::milliseconds max_wait{1000};
};

std::vector<OnFinish> resolveCollectorFinishCallbacks(
    const std::string& collector_name,
    const std::string& config_name,
    const FinishCallbackMap& finish_callbacks,
    const std::vector<std::string>& default_callback_names);

double resolveCollectorFreq(
    const std::string& collector_name,
    const std::string& config_name,
    double default_freq,
    bool allow_period_key);

EventBatchConfig resolveEventBatchConfig(const std::string& config_name);

// 从采集器自己的 config 段读取 auto_start，控制是否在注册时自动启动。
// 缺省（未配置）时返回 false，即默认不自动启动，改由 RPC 控制启停。
bool resolveAutoStart(const std::string& config_name);
