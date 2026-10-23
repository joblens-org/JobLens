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

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/output_record.h"

class BaseWriter;

struct OutputTarget {
    std::string writer_name;
    std::string writer_type;
    std::string writer_config_name;
};

struct ResolvedChannel {
    OutputChannel channel;
    std::vector<OutputTarget> targets;
    std::vector<BaseWriter*> writers;
};

/// 每个采集器实例一份、运行期不可变的路由表。
/// 在采集开始前完成配置解析、writer 能力校验与目标绑定；emit 热路径不再查配置/注册表。
class OutputRouter {
public:
    struct BuildResult {
        std::shared_ptr<OutputRouter> router;
        std::string error;
        bool ok() const { return router != nullptr; }
    };

    /// 解析采集器输出路由。
    /// - `outputs:` 显式映射优先；同时出现 `outputs` 与 `use_writers` 视为配置错误。
    /// - 否则回退 `use_writers`，再回退 `collectors_config.default_use_writers`。
    /// - 空列表显式禁用输出，不回退默认值。
    static BuildResult build(const std::string& collector_name,
                             const std::string& config_name,
                             const std::vector<OutputChannel>& declared);

    const std::vector<OutputChannel>& channels() const { return channels_; }
    const ResolvedChannel* find(std::string_view name) const;
    bool has(std::string_view name) const { return find(name) != nullptr; }

private:
    std::vector<OutputChannel> channels_;
    std::unordered_map<std::string, ResolvedChannel> routes_;
};
