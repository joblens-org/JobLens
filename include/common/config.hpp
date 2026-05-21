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

// 避免与 spdlog 的默认定义冲突
#ifdef SPDLOG_ACTIVE_LEVEL
#undef SPDLOG_ACTIVE_LEVEL
#endif
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include <yaml-cpp/yaml.h>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>
#include <iostream>
#include <nlohmann/json.hpp>
#include "utils.hpp"

class Config {
public:
    // 从文件加载
    explicit Config(const std::string& filePath);




    // 基本类型读取
    int         getInt   (const std::string& parentKey,
                          const std::string& key) const;
    double      getDouble(const std::string& parentKey,
                          const std::string& key) const;
    bool        getBool  (const std::string& parentKey,
                          const std::string& key) const;
    std::string getString(const std::string& parentKey,
                          const std::string& key) const;
    
    YAML::Node getRawNode(const std::string& parentKey) const;

    int         getInt   (const std::string& parentKey,
                          const std::string& key,
                          int defaultValue) const;
    double      getDouble(const std::string& parentKey,
                          const std::string& key,
                          double defaultValue) const;
    bool        getBool  (const std::string& parentKey,
                          const std::string& key,
                          bool defaultValue) const;
    std::string getString(const std::string& parentKey,
                          const std::string& key,
                          const std::string& defaultValue) const;
    
    YAML::Node getRawNode(const std::string& parentKey,
                          const YAML::Node& defaultValue) const;

    bool hasParent(const std::string& parentKey) const noexcept
    {
        return root_[parentKey] && !root_[parentKey].IsNull();
    }

    bool has(const std::string& parentKey,
         const std::string& key) const noexcept
    {
        try {
            const auto& node = root_[parentKey][key];
            return node && !node.IsNull();   // YAML::Node 隐式 bool 转换已足够
        } catch (const YAML::Exception&) {
            // yaml-cpp 在 operator[] 遇到不存在的 key 时会抛 BadSubscript
            return false;
        }
    }

    // 数组读取
    template<typename T>
    std::vector<T> getArray(const std::string& parentKey,
                                    const std::string& key) const
    {
        try {
            return root_[parentKey][key].as<std::vector<T>>();
        } catch (const YAML::Exception& e) {
            spdlog::warn("Config: error decoding array [{}][{}]: {}", parentKey, key, e.what());
            throw std::runtime_error("Config: missing or bad type for [" +
                                    parentKey + "][" + key + "]");
        }
    }

    template <typename T>
    std::vector<T> getArray(const std::string& parentKey,
                            const std::string& key,
                            std::function<T(const YAML::Node&)> decoder) const
    {
        try {
            std::vector<T> out;
            const auto& list = root_[parentKey][key];
            if (!list.IsSequence())
                throw YAML::Exception(YAML::Mark::null_mark(),
                                    "not a sequence");

            spdlog::trace("Config: decoding array [{}][{}]", parentKey, key);

            out.reserve(list.size());
            for (const auto& node : list){
                out.push_back(decoder(node));
            }

            spdlog::trace("Config: decoded {} items from [{}][{}]", out.size(), parentKey, key);

            return out;
        } catch (const YAML::Exception& e) {
            spdlog::warn("Config: error decoding array [{}][{}]: {}", parentKey, key, e.what());
            throw std::runtime_error("Config: missing or bad array [" +
                                    parentKey + "][" + key + "]");
                }
    }
        
    // 点分隔路径访问
    YAML::Node getNodeByPath(const std::string& path) const;
    
    // 导出完整配置为JSON
    nlohmann::json dumpConfig() const;
    
    // 注册RPC方法
    void registerRPCMethods();
        
    static Config& instance(std::string path = "") {
        static Config c = Config(initOnce(path));
        return c;
    }

private:
    static const std::string& initOnce(const std::string& path) {
        static std::string stored;
        if (!stored.empty()) return stored;          // 已初始化过
        if (path.empty())
            throw std::runtime_error("Config path not provided on first call");
        stored = path;
        return stored;
    }
    
    // 递归处理YAML节点中的环境变量
    void processEnvVariables(YAML::Node node);
    
    // 解析字符串中的环境变量占位符 {{ENV:default}}
    std::string resolveEnvVars(const std::string& value);
    
    YAML::Node root_;
};