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
#include "common/config.hpp"
#include "common/local_rpc.hpp"
#include "common/utils.hpp"
#include <stdexcept>
#include <sstream>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <cstdlib>
#include <regex>

Config::Config(const std::string& filePath)
{
    try {
        root_ = YAML::LoadFile(filePath);
        // 递归处理环境变量
        processEnvVariables(root_);
        spdlog::info("Config: loaded configuration from {}", filePath);
    } catch (const YAML::BadFile& e) {
        spdlog::warn("Config: failed to load configuration file {}: {}", filePath, e.what());
        throw std::runtime_error("Config: cannot open file: " + filePath);
    }
}

// 递归处理YAML节点中的环境变量
void Config::processEnvVariables(YAML::Node node)
{
    if (node.IsScalar()) {
        // 标量节点：如果是字符串则处理环境变量
        try {
            std::string value = node.as<std::string>();
            std::string resolved = resolveEnvVars(value);
            if (resolved != value) {
                node = resolved;
            }
        } catch (const YAML::Exception&) {
            // 非字符串标量，忽略
        }
    } else if (node.IsSequence()) {
        // 序列节点：递归处理每个元素
        for (size_t i = 0; i < node.size(); ++i) {
            processEnvVariables(node[i]);
        }
    } else if (node.IsMap()) {
        // 映射节点：递归处理每个值
        for (auto kv : node) {
            processEnvVariables(kv.second);
        }
    }
}

// 解析字符串中的环境变量占位符 {{ENV:default}} 或 {{ENV}}
std::string Config::resolveEnvVars(const std::string& value)
{
    // 匹配 {{ENV_NAME}} 或 {{ENV_NAME:default_value}}
    // 支持默认值的格式：环境变量名后跟可选的冒号和默认值
    std::regex envPattern(R"(\{\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}\})");
    std::string result = value;
    
    // 使用 while 循环处理字符串中所有环境变量占位符
    std::smatch match;
    while (std::regex_search(result, match, envPattern)) {
        std::string fullMatch = match[0].str();
        std::string envName = match[1].str();
        std::string defaultValue = match[2].matched ? match[2].str() : "";
        
        // 获取环境变量
        const char* envValue = std::getenv(envName.c_str());
        std::string replacement;
        
        if (envValue != nullptr) {
            // 环境变量存在，使用其值
            replacement = std::string(envValue);
            spdlog::debug("Config: resolved env variable '{}' to '{}'", envName, replacement);
        } else if (!defaultValue.empty()) {
            // 环境变量不存在但有默认值，使用默认值
            replacement = defaultValue;
            spdlog::debug("Config: env variable '{}' not found, using default '{}'", envName, defaultValue);
        } else {
            // 环境变量不存在且没有默认值，抛出错误
            spdlog::error("Config: required environment variable '{}' is not set and no default provided", envName);
            throw std::runtime_error("Config: required environment variable '" + envName + "' is not set");
        }
        
        // 替换当前匹配项（只替换第一个匹配）
        size_t matchPos = result.find(fullMatch);
        if (matchPos != std::string::npos) {
            result.replace(matchPos, fullMatch.length(), replacement);
        }
    }
    
    return result;
}

int Config::getInt(const std::string& parentKey,
                   const std::string& key) const
{
    try {
        return root_[parentKey][key].as<int>();
    } catch (const YAML::Exception& e) {
        spdlog::warn("Config: error decoding int [{}][{}]: {}", parentKey, key, e.what());
        throw std::runtime_error("Config: missing or bad type for [" +
                                 parentKey + "][" + key + "]");
    }
}

double Config::getDouble(const std::string& parentKey,
                         const std::string& key) const
{
    try {
        return root_[parentKey][key].as<double>();
    } catch (const YAML::Exception& e) {
        spdlog::warn("Config: error decoding double [{}][{}]: {}", parentKey, key, e.what());
        throw std::runtime_error("Config: missing or bad type for [" +
                                 parentKey + "][" + key + "]");
    }
}

bool Config::getBool(const std::string& parentKey,
                     const std::string& key) const
{
    try {
        return root_[parentKey][key].as<bool>();
    } catch (const YAML::Exception& e) {
        spdlog::warn("Config: error decoding bool [{}][{}]: {}", parentKey, key, e.what());
        throw std::runtime_error("Config: missing or bad type for [" +
                                 parentKey + "][" + key + "]");
    }
}

std::string Config::getString(const std::string& parentKey,
                              const std::string& key) const
{
    try {
        return root_[parentKey][key].as<std::string>();
    } catch (const YAML::Exception& e) {
        spdlog::warn("Config: error decoding string [{}][{}]: {}", parentKey, key, e.what());
        throw std::runtime_error("Config: missing or bad type for [" +
                                 parentKey + "][" + key + "]");
    }
}

YAML::Node Config::getRawNode(const std::string& parentKey) const
{
    try {
        return root_[parentKey];
    } catch (const YAML::Exception& e) {
        spdlog::warn("Config: error decoding raw node [{}][{}]: {}", parentKey, e.what());
        throw std::runtime_error("Config: missing or bad type for [" +
                                 parentKey + "]");
    }
}

int Config::getInt(const std::string& parentKey,
                   const std::string& key,
                   int defaultValue) const
{
    try {
        return root_[parentKey][key].as<int>();
    } catch (const YAML::Exception& e) {
        spdlog::info("Config: using default value {} for [{}][{}]", 
                     defaultValue, parentKey, key);
        return defaultValue;
    }
}

double Config::getDouble(const std::string& parentKey,
                         const std::string& key,
                         double defaultValue) const
{
    try {
        return root_[parentKey][key].as<double>();
    } catch (const YAML::Exception& e) {
        spdlog::info("Config: using default value {} for [{}][{}]", 
                     defaultValue, parentKey, key);
        return defaultValue;
    }
}

bool Config::getBool(const std::string& parentKey,
                     const std::string& key,
                     bool defaultValue) const
{
    try {
        return root_[parentKey][key].as<bool>();
    } catch (const YAML::Exception& e) {
        spdlog::info("Config: using default value {} for [{}][{}]", 
                     defaultValue, parentKey, key);
        return defaultValue;
    }
}

std::string Config::getString(const std::string& parentKey,
                              const std::string& key,
                              const std::string& defaultValue) const
{
    try {
        return root_[parentKey][key].as<std::string>();
    } catch (const YAML::Exception& e) {
        spdlog::info("Config: using default value '{}' for [{}][{}]", 
                     defaultValue, parentKey, key);
        return defaultValue;
    }
}

YAML::Node Config::getRawNode(const std::string& parentKey,
                              const YAML::Node& defaultValue) const
{
    try {
        return root_[parentKey];
    } catch (const YAML::Exception& e) {
        spdlog::info("Config: using default node for [{}]", parentKey);
        return defaultValue;
    }
}

// 常见模板实例化
template std::vector<int>    Config::getArray<int>    (const std::string&, const std::string&) const;
template std::vector<double> Config::getArray<double> (const std::string&, const std::string&) const;
template std::vector<std::string> Config::getArray<std::string>(const std::string&, const std::string&) const;

// 点分隔路径访问实现
YAML::Node Config::getNodeByPath(const std::string& path) const {
    if (path.empty()) {
        return YAML::Node(); // 空路径返回空节点
    }
    
    std::istringstream iss(path);
    std::string segment;
    YAML::Node current = root_;
    
    try {
        while (std::getline(iss, segment, '.')) {
            if (current.IsNull() || !current[segment]) {
                return YAML::Node(); // 返回空节点表示路径不存在
            }
            current = current[segment];
        }
    } catch (const YAML::Exception& e) {
        spdlog::debug("Config: error accessing path '{}' at segment '{}': {}", path, segment, e.what());
        return YAML::Node();
    }
    
    return current;
}

// 导出完整配置为JSON
nlohmann::json Config::dumpConfig() const {
    try {
        return Utils::yamlToJson(root_);
    } catch (const std::exception& e) {
        spdlog::error("Config: error dumping config to JSON: {}", e.what());
        throw std::runtime_error("Config: failed to dump config to JSON: " + std::string(e.what()));
    }
}

// 注册RPC方法
void Config::registerRPCMethods() {
    // Config/get_config - 按路径获取配置字段
    // 使用点分隔路径访问，例如 "lens_config.log_level"
    RPCServer::instance().register_method("Config/get_config",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json response;
            try {
                if (!req.contains("path") || !req["path"].is_string()) {
                    response["status"] = "error";
                    response["msg"] = "Missing or invalid 'path' parameter";
                    return response;
                }
                
                std::string path = req["path"].get<std::string>();
                YAML::Node node = getNodeByPath(path);
                
                if (node.IsNull()) {
                    response["status"] = "error";
                    response["msg"] = "Config path not found: " + path;
                    return response;
                }
                
                // 将YAML节点转换为JSON值
                auto value = Utils::yamlToJson(node);
                response["status"] = "ok";
                response["value"] = value;
                
            } catch (const std::exception& e) {
                response["status"] = "error";
                response["msg"] = fmt::format("Failed to get config: {}", e.what());
            }
            return response;
        }
    );
    
    // Config/dump_config - 导出完整配置
    RPCServer::instance().register_method("Config/dump_config",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json response;
            try {
                auto config_json = dumpConfig();
                response["status"] = "ok";
                response["config"] = config_json;
            } catch (const std::exception& e) {
                response["status"] = "error";
                response["msg"] = fmt::format("Failed to dump config: {}", e.what());
            }
            return response;
        }
    );
    
    // Config/list_sections - 列出所有顶级章节
    RPCServer::instance().register_method("Config/list_sections",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json response;
            try {
                nlohmann::json sections = nlohmann::json::array();
                for (const auto& kv : root_) {
                    sections.push_back(kv.first.as<std::string>());
                }
                response["status"] = "ok";
                response["sections"] = sections;
            } catch (const std::exception& e) {
                response["status"] = "error";
                response["msg"] = fmt::format("Failed to list sections: {}", e.what());
            }
            return response;
        }
    );
    
    // Config/validate_config - 验证配置（基础实现）
    RPCServer::instance().register_method("Config/validate_config",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json response;
            try {
                // 基本验证：检查必需字段
                std::vector<std::string> required_sections = {"lens_config"};
                std::vector<std::string> missing;
                
                for (const auto& section : required_sections) {
                    if (!hasParent(section)) {
                        missing.push_back(section);
                    }
                }
                
                response["status"] = "ok";
                response["valid"] = missing.empty();
                response["missing_sections"] = missing;
                
                if (!missing.empty()) {
                    response["msg"] = "Missing required sections: " + fmt::format("{}", fmt::join(missing, ", "));
                }
            } catch (const std::exception& e) {
                response["status"] = "error";
                response["msg"] = fmt::format("Failed to validate config: {}", e.what());
            }
            return response;
        }
    );
    
    spdlog::info("Config: registered RPC methods");
}