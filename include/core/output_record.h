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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/collector_type.h"

// ============================================================================
// 采集器输出契约
//
// 采集器在 collect() 内构造 0..N 条带类型的输出记录，经 OutputContext 提交。
// writer 不再反查 collector / parser；记录本身携带全部渲染所需的元数据。
// ============================================================================

// 输出类型标签与 OutputChannel 定义在 core/collector_type.h（供 CollectorHandle 使用），
// 这里只定义 payload 容器与记录结构。

struct Document { nlohmann::json value; };
struct Text { std::string value; };

inline bool operator==(const Document& a, const Document& b) { return a.value == b.value; }
inline bool operator==(const Text& a, const Text& b) { return a.value == b.value; }

// Generic metric payload. Collectors choose metric names, types and labels;
// writers and the trigger do not need a per-collector schema.
enum class MetricType { Gauge, Counter };

inline const char* to_string(MetricType type)
{
    switch (type) {
        case MetricType::Gauge:   return "gauge";
        case MetricType::Counter: return "counter";
    }
    return "unknown";
}

struct MetricLabel {
    std::string name;
    std::string value;
};

struct MetricSample {
    std::string name;
    MetricType type = MetricType::Gauge;
    std::string help;
    std::vector<MetricLabel> labels;
    double value = 0.0;
};

struct MetricBatch {
    std::vector<MetricSample> samples;
};

inline bool operator==(const MetricLabel& a, const MetricLabel& b)
{
    return a.name == b.name && a.value == b.value;
}

inline bool operator==(const MetricSample& a, const MetricSample& b)
{
    return a.name == b.name && a.type == b.type && a.help == b.help &&
           a.labels == b.labels && a.value == b.value;
}

inline bool operator==(const MetricBatch& a, const MetricBatch& b)
{
    return a.samples == b.samples;
}

using OutputPayload = std::variant<Document, Text, MetricBatch>;

inline OutputKind kind_of(const OutputPayload& payload)
{
    switch (payload.index()) {
        case 0: return OutputKind::Document;
        case 1: return OutputKind::Text;
        case 2: return OutputKind::Metrics;
    }
    return OutputKind::Document;
}

/// 采样级只读元数据：同一次 collect() 的所有记录共享同一份。
struct SampleMetadata {
    std::string sample_id;
    std::string collector_name;
    std::string collector_type;
    Job job;
    std::chrono::system_clock::time_point sampled_at;
};

struct OutputRecord {
    std::shared_ptr<const SampleMetadata> sample;
    std::string channel;
    uint64_t sequence = 0;
    std::string record_id;
    OutputPayload payload;
    size_t accounted_bytes = 0;
};

using OutputRecordPtr = std::shared_ptr<const OutputRecord>;

/// 入队结果。放在 output_record.h 中，BaseWriter 在路由层之前即可使用。
enum class EnqueueCode {
    Accepted, QueueFull, TooLarge, Closed, Unsupported, Failed
};

inline const char* to_string(EnqueueCode code)
{
    switch (code) {
        case EnqueueCode::Accepted:    return "accepted";
        case EnqueueCode::QueueFull:   return "queue_full";
        case EnqueueCode::TooLarge:    return "too_large";
        case EnqueueCode::Closed:      return "closed";
        case EnqueueCode::Unsupported: return "unsupported";
        case EnqueueCode::Failed:      return "failed";
    }
    return "unknown";
}

namespace output_size {

/// JSON 节点内容的保守估算：不编码为文本，只累加字符串容量、数组长度与固定开销。
inline size_t estimate_json(const nlohmann::json& value)
{
    // 每个 JSON 节点自身的固定开销（类型标签 + 容器头），保守取值。
    constexpr size_t kNodeOverhead = 64;
    size_t total = kNodeOverhead;
    switch (value.type()) {
        case nlohmann::json::value_t::string:
            total += value.get_ref<const std::string&>().capacity();
            break;
        case nlohmann::json::value_t::object:
            total += value.size() * (kNodeOverhead + 32);
            for (auto it = value.begin(); it != value.end(); ++it) {
                total += it.key().capacity() + 32;
                total += estimate_json(it.value());
            }
            break;
        case nlohmann::json::value_t::array:
            total += value.size() * kNodeOverhead;
            for (const auto& element : value) total += estimate_json(element);
            break;
        default:
            break;
    }
    return total;
}

} // namespace output_size

/// 估算一条输出记录实际占用的字节数（payload + 元数据 + 固定开销）。
inline size_t estimate_output_bytes(const OutputPayload& payload)
{
    constexpr size_t kRecordOverhead = 256;
    size_t total = kRecordOverhead;
    std::visit([&total](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, Document>) {
            total += output_size::estimate_json(value.value);
        } else if constexpr (std::is_same_v<T, Text>) {
            total += value.value.capacity() + 32;
        } else if constexpr (std::is_same_v<T, MetricBatch>) {
            total += value.samples.capacity() * sizeof(MetricSample);
            for (const auto& sample : value.samples) {
                total += sample.name.capacity() + sample.help.capacity();
                total += sample.labels.capacity() * sizeof(MetricLabel);
                for (const auto& label : sample.labels)
                    total += label.name.capacity() + label.value.capacity();
                total += 32;
            }
        }
    }, payload);
    return total;
}

/// 逐条累加时的上限截断，避免超大记录触发无界遍历。
inline size_t estimate_output_bytes(const OutputPayload& payload, size_t cap)
{
    if (cap == 0) return estimate_output_bytes(payload);
    return std::min(estimate_output_bytes(payload), cap);
}
