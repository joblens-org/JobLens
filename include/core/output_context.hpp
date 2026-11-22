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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "core/output_record.h"
#include "core/output_router.hpp"

enum class EmitCode { Submitted, Disabled, Invalid };

inline const char* to_string(EmitCode code)
{
    switch (code) {
        case EmitCode::Submitted: return "submitted";
        case EmitCode::Disabled:  return "disabled";
        case EmitCode::Invalid:   return "invalid";
    }
    return "unknown";
}

struct WriterAcceptance {
    std::string writer_name;
    EnqueueCode code = EnqueueCode::Failed;
    std::string error;
};

struct EmitReport {
    EmitCode code = EmitCode::Invalid;
    std::vector<WriterAcceptance> writers;
};

/// 一次 collect() 调用期间的输出上下文。
/// 只是 API 与路由对象，不是队列：提交路径不做后端 IO、不睡眠、不等待队列空位。
class OutputContext {
public:
    OutputContext(std::shared_ptr<const OutputRouter> router,
                  std::shared_ptr<const SampleMetadata> sample);

    const SampleMetadata& sample() const { return *sample_; }

    /// 该通道是否已声明（构造开销优化，不保证队列容量）。
    bool enabled(std::string_view channel) const;

    /// 通道已绑定的目标；未知通道返回空列表。
    const std::vector<OutputTarget>& targets(std::string_view channel) const;

    /// 向通道的全部目标广播一条记录。
    [[nodiscard]] EmitReport emit(std::string_view channel, OutputPayload payload);

    /// 只向通道上已绑定的某个 writer 提交（用于按 writer 实例渲染的元数据）。
    [[nodiscard]] EmitReport emit_to(std::string_view channel,
                                     std::string_view writer_name,
                                     OutputPayload payload);

    /// 本次上下文是否出现过无效/被拒绝的提交（与采集是否成功独立）。
    bool had_error() const { return !errors_.empty(); }
    const std::vector<std::string>& errors() const { return errors_; }
    std::string error_summary() const;

    /// 已提交的记录数（Skipped 契约校验用）。
    size_t submitted() const { return submitted_; }

    /// 每次 emit/emit_to 调用前后的观测钩子（调度器用于嵌套 write 操作）。
    using WriteBegin = std::function<void(std::string_view channel)>;
    using WriteEnd = std::function<void(std::string_view channel, bool failed)>;
    void set_write_observer(WriteBegin begin, WriteEnd end);

private:
    EmitReport emit_internal(std::string_view channel, OutputPayload payload);
    EmitReport emit_to_internal(std::string_view channel, std::string_view writer_name,
                                OutputPayload payload);
    bool begin_write(std::string_view channel);
    void end_write(std::string_view channel, const EmitReport& report, bool began);
    EmitReport submit(const ResolvedChannel& route,
                      const std::vector<BaseWriter*>& destinations,
                      const std::vector<OutputTarget>& targets,
                      std::string_view channel,
                      OutputPayload payload);
    void record_error(std::string message);

    std::shared_ptr<const OutputRouter> router_;
    std::shared_ptr<const SampleMetadata> sample_;
    uint64_t next_sequence_ = 0;
    size_t submitted_ = 0;
    std::vector<std::string> errors_;
    WriteBegin write_begin_;
    WriteEnd write_end_;
};
