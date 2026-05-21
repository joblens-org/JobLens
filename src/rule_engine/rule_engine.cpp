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
#include "rule_engine/rule_engine.hpp"
#include "common/utils.hpp"
#include <sol/sol.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include "common/struct2ltable.hpp"

// RuleEngine 构造函数实现
RuleEngine::RuleEngine() : L(nullptr) {
    create_sandbox();
    if (!L) {
        spdlog::error("RuleEngine: Failed to create Lua state");
        throw std::runtime_error("Failed to create Lua state");
    }
}

// RuleEngine 析构函数实现
RuleEngine::~RuleEngine() {
    // sol::state 会自动清理资源，无需手动释放函数引用
    spdlog::info("RuleEngine: Destroyed");
}

// 加载单个规则文件
bool RuleEngine::load_rule(const std::string& filepath) {
    try {
        // 1. 执行Lua文件
        sol::protected_function_result result = L->script_file(filepath);
        if (!result.valid()) {
            sol::error err = result;
            spdlog::error("RuleEngine: Failed to load {}: {}", filepath, err.what());
            return false;
        }
        
        // 2. 获取规则表（全局变量）
        sol::object rule_obj = result.get<sol::object>();
        if (!rule_obj.is<sol::table>()) {
            spdlog::error("RuleEngine: 'rule' is not a table in {}", filepath);
            return false;
        }
        
        sol::table rule_table = rule_obj.as<sol::table>();
        Rule rule;
        
        rule.rule_hash = Utils::file_hash_xxh64(filepath);
        
        // 3. 使用sol2直接读取表字段
        sol::optional<std::string> name_opt = rule_table["name"];
        if (name_opt == sol::nullopt) {
            spdlog::error("RuleEngine: 'name' field missing in {}", filepath);
            return false;
        }
        rule.name = name_opt.value();
        spdlog::debug("RuleEngine: Loading rule '{}'", rule.name);
        rule.description = rule_table.get_or<std::string>("description", "");
        spdlog::debug("RuleEngine: Rule '{}' description: {}", rule.name, rule.description);
        rule.priority = rule_table.get_or<uint32_t>("priority", 0);
        spdlog::debug("RuleEngine: Rule '{}' priority: {}", rule.name, rule.priority);
        
        // 4. 获取condition函数
        sol::object condition_obj = rule_table["condition"];
        if (!condition_obj.is<sol::function>()) {
            spdlog::error("RuleEngine: 'condition' is not a function in {}", filepath);
            return false;
        }
        
        // sol2会自动管理函数生命周期，无需手动创建引用
        rule.condition = condition_obj.as<sol::protected_function>();
        
        // 5. 存储规则
        std::string filename = fs::path(filepath).filename().string();
        rules[filename] = std::move(rule);
        spdlog::info("RuleEngine: Stored rule '{}' from file '{}'", rules[filename].name, filename);
        return true;
    } catch (const std::exception& e) {
        spdlog::error("RuleEngine: Exception loading {}: {}", filepath, e.what());
        return false;
    }
}

// 加载目录下所有规则文件
int RuleEngine::load_rules_from_directory(const std::string& dir_path) {
    int loaded = 0;
    dir_path_ = dir_path;
    spdlog::debug("RuleEngine: Loading rules from directory '{}'", dir_path);
    if (!fs::exists(dir_path)) {
        spdlog::warn("RuleEngine: Directory not found:{}", dir_path);
        return 0;
    }
    fs::path dir(dir_path);
    for (const auto& entry : fs::directory_iterator(dir)) {
        spdlog::debug("RuleEngine: Checking entry '{}'", entry.path().string());
        if (entry.is_regular_file() && entry.path().extension().compare(".lua") == 0) {
            spdlog::debug("RuleEngine: Found rule file '{}'", entry.path().string());
            if (load_rule(entry.path().string())) {
                loaded++;
            } else {
                spdlog::error("RuleEngine: Failed to load rule file '{}'", entry.path().string());
            }
        }
    }
    
    spdlog::info("RuleEngine: Loaded {} rules from directory '{}'", loaded, dir_path);
    return loaded;
}

// 更新规则
int RuleEngine::update_rules() {
    int loaded = 0;
    if (!fs::exists(dir_path_)) {
        spdlog::error("RuleEngine: Directory not found:{}", dir_path_);
        return 0;
    }
    
    for (const auto& entry : fs::directory_iterator(dir_path_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".lua") {
            auto str_path = entry.path().string();
            auto it = rules.find(str_path);
            
            if (it == rules.end()) {
                // 新规则
                if (load_rule(str_path)) {
                    loaded++;
                }
            } else {
                // 检查是否需要更新
                if (it->second.rule_hash != Utils::file_hash_xxh64(str_path)) {
                    spdlog::info("RuleEngine: Updating rule '{}'", str_path);
                    if (load_rule(str_path)) {
                        loaded++;
                    }
                }
            }
        }
    }
    return loaded;
}

// 添加单个规则
int RuleEngine::add_rule(const std::string& rule_path) {
    if (!fs::exists(rule_path)) {
        spdlog::error("RuleEngine: Directory not found:{}", rule_path);
        return 0;
    }
    return load_rule(rule_path) ? 1 : 0;
}

// 移除规则
int RuleEngine::remove_rule(const std::string& rule_path) {
    std::string filename = fs::path(rule_path).filename().string();
    auto it = rules.find(filename);
    if (it != rules.end()) {
        rules.erase(it);
        spdlog::info("RuleEngine: Removed rule '{}'", filename);
        return 1;
    } else {
        spdlog::warn("RuleEngine: Rule '{}' not found for removal", filename);
        return 0;
    }
}


// 启用/禁用规则
void RuleEngine::set_enabled(const std::string& rule_filename, bool enabled) {
    auto it = rules.find(rule_filename);
    if (it != rules.end()) {
        it->second.enabled = enabled;
        spdlog::info("RuleEngine: Rule '{}' is now {}", rule_filename, enabled ? "enabled" : "disabled");
    }
}

// 打印所有规则信息
nlohmann::json RuleEngine::list_rules() const {
    nlohmann::json j_rules = nlohmann::json::array();
    for (const auto& [filename, rule] : rules) {
        j_rules.push_back({
            {"filename", filename},
            {"name", rule.name},
            {"description", rule.description},
            {"enabled", rule.enabled},
            {"priority", rule.priority}
        });
    }
    return j_rules;
}

// 创建沙箱环境
void RuleEngine::create_sandbox() {
    try {
        // 创建Lua状态
        L = std::make_unique<sol::state>();
        
        // 打开安全库
        L->open_libraries(
            sol::lib::base,
            sol::lib::string,
            sol::lib::table,
            sol::lib::math,
            sol::lib::io,
            sol::lib::package
        );
        
        // 移除危险函数
        for (const char* func : {"dofile", "loadfile", "load", "loadstring"}) {
            (*L)[func] = sol::nil;
        }
        
        // 自定义print函数
        (*L)["print"] = [this](sol::variadic_args args) {
            std::string msg;
            for (auto arg : args) {
                if (!msg.empty()) msg += "\t";
                // 尝试将参数转换为字符串
                sol::object obj = arg;
                if (obj.is<std::string>()) {
                    msg += obj.as<std::string>();
                } else {
                    // 对其他类型使用Lua的tostring
                    std::string str = L->get<sol::protected_function>("tostring")(obj);
                    msg += str;
                }
            }
            spdlog::debug("RuleEngine: [Lua] {}", msg);
        };
        
        // 自定义require函数
        (*L)["require"] = [this](const std::string& modname) -> sol::object {
            static const std::vector<std::string> blacklist = {
                "os"
            };
            
            if (std::find(blacklist.begin(), blacklist.end(), modname) != blacklist.end()) {
                throw sol::error("module '" + modname + "' is banned in sandbox");
            }
            
            auto package = (*L)["package"].get<sol::table>();
            auto require_func = package["require"].get<sol::protected_function>();
            return require_func(modname);
        };
        
        // 禁用io.write和io.popen
        sol::table io = (*L)["io"].get_or(L->create_table());
        io["write"] = sol::nil;
        io["popen"] = sol::nil;
        (*L)["io"] = io;
        
    } catch (const std::exception& e) {
        spdlog::error("RuleEngine: Failed to create sandbox: {}", e.what());
        throw ;
    }
}

// 验证Lua代码语法
bool RuleEngine::validate_rule_code(const std::string& lua_code) {
    bool is_valid = check_lua_syntax(lua_code);

    return is_valid;
}

bool RuleEngine::check_lua_syntax(const std::string& lua_code) {
    try {
        // 创建临时Lua状态
        sol::state temp_state;
        temp_state.open_libraries(sol::lib::base);
        
        // 加载代码但不执行
        sol::load_result result = temp_state.load(lua_code);
        
        if (!result.valid()) {
            sol::error err = result;
            spdlog::error("RuleEngine: Lua syntax error: {}", err.what());
            return false;
        }
        
        return true;
    } catch (const std::exception& e) {
        spdlog::error("RuleEngine: Exception validating code: {}", e.what());
        return false;
    }
}


