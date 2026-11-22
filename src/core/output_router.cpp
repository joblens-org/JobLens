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
#include "core/output_router.hpp"

#include <stdexcept>
#include <unordered_set>

#include "common/config.hpp"
#include "core/writer_manager.hpp"
#include "writer/base_writer.hpp"

namespace {

const char* kind_or_unknown(OutputKind kind) { return to_string(kind); }

std::string error_for(const std::string& collector_name, const std::string& reason)
{
    return "collector '" + collector_name + "' output route: " + reason;
}

} // namespace

OutputRouter::BuildResult OutputRouter::build(const std::string& collector_name,
                                              const std::string& config_name,
                                              const std::vector<OutputChannel>& declared)
{
    BuildResult result;
    try {
        std::unordered_set<std::string> declared_names;
        for (const auto& channel : declared) {
            if (channel.name.empty())
                throw std::runtime_error("empty channel name is not allowed");
            if (!declared_names.insert(channel.name).second)
                throw std::runtime_error("duplicate channel name '" + channel.name + "'");
        }

        auto router = std::shared_ptr<OutputRouter>(new OutputRouter());
        router->channels_ = declared;
        for (const auto& channel : declared) {
            router->routes_[channel.name].channel = channel;
        }
        // 未声明任何通道的采集器（例如尚无输出的 Taskstats）无需配置与 writer 绑定。
        if (declared.empty()) {
            result.router = std::move(router);
            return result;
        }

        auto& config = Config::instance();
        auto& manager = WriterManager::instance();
        manager.initialize();

        auto bind = [&](const std::string& channel_name, const std::vector<std::string>& writer_names) {
            auto route_it = router->routes_.find(channel_name);
            if (route_it == router->routes_.end())
                throw std::runtime_error("unknown channel '" + channel_name + "'");
            auto& route = route_it->second;
            std::unordered_set<std::string> seen;
            for (const auto& writer_name : writer_names) {
                if (writer_name.empty())
                    throw std::runtime_error("empty writer name on channel '" + channel_name + "'");
                if (!seen.insert(writer_name).second) continue;  // 去重，保留首次出现
                BaseWriter* writer = manager.find(writer_name);
                if (!writer)
                    throw std::runtime_error("unknown writer '" + writer_name + "' on channel '" + channel_name + "'");
                if (!writer->accepts(route.channel.kind))
                    throw std::runtime_error("writer '" + writer_name + "' does not accept " +
                                             kind_or_unknown(route.channel.kind) +
                                             " payloads required by channel '" + channel_name + "'");
                route.targets.push_back(OutputTarget{writer->get_name(), writer->get_type(),
                                                     writer->get_config_name()});
                route.writers.push_back(writer);
            }
        };

        const bool has_outputs = config.has(config_name, "outputs");
        const bool has_use_writers = config.has(config_name, "use_writers");
        if (has_outputs && has_use_writers)
            throw std::runtime_error("'outputs' and 'use_writers' are mutually exclusive");

        if (has_outputs) {
            YAML::Node outputs = config.getRawNode(config_name)["outputs"];
            if (outputs.IsNull()) outputs = YAML::Node(YAML::NodeType::Map);  // outputs: 表示全部禁用
            if (!outputs.IsMap())
                throw std::runtime_error("'outputs' must be a mapping of channel to writer list");
            for (auto it = outputs.begin(); it != outputs.end(); ++it) {
                if (!it->first.IsScalar())
                    throw std::runtime_error("'outputs' contains a non-scalar channel name");
                const std::string channel_name = it->first.as<std::string>();
                if (!declared_names.count(channel_name))
                    throw std::runtime_error("'outputs' configures undeclared channel '" + channel_name + "'");
                YAML::Node list = it->second;
                if (list.IsNull()) continue;  // 已声明通道、无目标 = 禁用
                if (!list.IsSequence())
                    throw std::runtime_error("channel '" + channel_name + "' must map to a list of writers");
                std::vector<std::string> writer_names;
                for (const auto& node : list) {
                    if (!node.IsScalar())
                        throw std::runtime_error("channel '" + channel_name + "' contains a non-scalar writer name");
                    writer_names.push_back(node.as<std::string>());
                }
                bind(channel_name, writer_names);
            }
        } else {
            // 兼容旧配置：writer 名 → 该 collector 唯一匹配其 payload 类型的通道
            std::vector<std::string> writer_names;
            if (has_use_writers) {
                writer_names = config.getArray<std::string>(config_name, "use_writers");
            } else if (config.has("collectors_config", "default_use_writers")) {
                writer_names = config.getArray<std::string>("collectors_config", "default_use_writers");
            }
            for (const auto& writer_name : writer_names) {
                BaseWriter* writer = manager.find(writer_name);
                if (!writer)
                    throw std::runtime_error("unknown writer '" + writer_name + "'");
                std::vector<std::string> matches;
                for (const auto& channel : declared) {
                    if (writer->accepts(channel.kind)) matches.push_back(channel.name);
                }
                if (matches.size() != 1) {
                    throw std::runtime_error("legacy 'use_writers' entry '" + writer_name +
                                             "' matches " + std::to_string(matches.size()) +
                                             " declared channels; configure 'outputs' explicitly");
                }
                bind(matches.front(), {writer_name});
            }
        }

        result.router = std::move(router);
        return result;
    } catch (const std::exception& e) {
        result.error = error_for(collector_name, e.what());
        return result;
    }
}

const ResolvedChannel* OutputRouter::find(std::string_view name) const
{
    auto it = routes_.find(std::string(name));
    return it == routes_.end() ? nullptr : &it->second;
}
