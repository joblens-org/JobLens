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

#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <lua.hpp>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sol/sol.hpp>
#include "common/struct2ltable.hpp"


struct Rule {
    std::string name;           // 规则名称
    std::string description;    // 规则描述
    bool enabled;              // 是否启用
    int priority;              // 优先级（越大越高）
    sol::protected_function condition;
    uint64_t rule_hash;
    
    Rule() : enabled(true), priority(0) {}
};

class RuleEngine {
public:
    RuleEngine();
    ~RuleEngine();
    
    // 禁止拷贝，允许移动
    RuleEngine(const RuleEngine&) = delete;
    RuleEngine& operator=(const RuleEngine&) = delete;
    
    // 加载单个规则文件
    bool load_rule(const std::string& filepath);
    
    // 加载目录下所有规则文件
    int load_rules_from_directory(const std::string& dir_path);

    // 更新目录下的所有规则文件
    int update_rules();

    // 添加一个新文件
    int add_rule(const std::string& rule_path);
    
    // 删除一个规则
    int remove_rule(const std::string& rule_path);

    // 执行规则判断
    // 返回值: std::pair<bool, std::vector<std::string>>
    // 第一个元素: 是否通过
    // 第二个元素: 该规则添加的采集器名称列表
    template <typename RuleArg>
    std::pair<bool, std::vector<std::string>> evaluate(const std::string& rule_filename, const RuleArg& arg) {
        auto it = rules.find(rule_filename);
        if (it == rules.end()) {
            spdlog::error("RuleEngine: Rule '{}' not found", rule_filename);
            return {false, {}};
        }
        
        Rule& rule = it->second;
        if (!rule.enabled) {
            spdlog::info("RuleEngine: Rule '{}' is disabled", rule.name);
            return {false, {}};
        }
        
        try {
            // 创建参数表
            auto args_table = to_lua_table(*L, arg);
            
            // 调用condition函数
            sol::protected_function_result result = rule.condition(args_table);
            if (!result.valid()) {
                sol::error err = result;
                spdlog::error("RuleEngine: Lua error in {}: {}", rule_filename, err.what());
                return {false, {}};
            }
            
            // 获取结果 - 支持新旧两种格式
            // 新格式: {passed = bool, collectors = {"collector1", "collector2"}}
            // 旧格式: bool (为了兼容性，但用户选择不兼容，所以主要支持新格式)
            if (result.return_count() > 0) {
                sol::object first_result = result.get<sol::object>();
                
                // 检查是否是表（新格式）
                if (first_result.is<sol::table>()) {
                    sol::table result_table = first_result.as<sol::table>();
                    
                    // 获取passed字段
                    bool passed = result_table.get_or("passed", false);
                    
                    // 获取collectors字段
                    std::vector<std::string> collectors;
                    if (result_table["collectors"].valid()) {
                        sol::object collectors_obj = result_table["collectors"];
                        if (collectors_obj.is<sol::table>()) {
                            sol::table collectors_table = collectors_obj.as<sol::table>();
                            for (const auto& [key, value] : collectors_table) {
                                // value是sol::object类型，需要检查并转换为string
                                sol::object value_obj = value;
                                if (value_obj.is<std::string>()) {
                                    collectors.push_back(value_obj.as<std::string>());
                                }
                            }
                        }
                    }
                    
                    return {passed, collectors};
                } else if (first_result.is<bool>()) {
                    // 旧格式：只返回bool
                    bool passed = first_result.as<bool>();
                    return {passed, {}};
                }
            }
            
            // 默认返回false
            return {false, {}};
        } catch (const std::exception& e) {
            spdlog::error("RuleEngine: Exception evaluating {}: {}", rule_filename, e.what());
            return {false, {}};
        }
    }

    
    // 执行所有启用的规则
    // 返回值: std::pair<bool, std::vector<std::string>>
    // 第一个元素: 是否所有规则都通过
    // 第二个元素: 所有规则添加的采集器名称列表（去重）
    template <typename RuleArg>
    std::pair<bool, std::vector<std::string>> evaluate_all(const RuleArg& arg) {
        std::vector<std::pair<int, std::string>> sorted_rules;
        
        // 收集并排序
        for (auto& [filename, rule] : rules) {
            if (rule.enabled) {
                sorted_rules.push_back({-rule.priority, filename});  // 负号实现降序
            }
        }
        std::sort(sorted_rules.begin(), sorted_rules.end());
        
        // 执行
        bool all_passed = true;
        std::vector<std::string> all_collectors;
        
        for (auto& [neg_priority, filename] : sorted_rules) {
            auto [passed, collectors] = evaluate(filename, arg);
            
            if (passed) {
                // 添加采集器名称到列表
                all_collectors.insert(all_collectors.end(), collectors.begin(), collectors.end());
            } else {
                all_passed = false;
                break;  // 遇到未通过的规则就停止
            }
        }
        
        // 去重
        std::sort(all_collectors.begin(), all_collectors.end());
        all_collectors.erase(std::unique(all_collectors.begin(), all_collectors.end()), all_collectors.end());
        
        return {all_passed, all_collectors};
    }

    template <typename RuleArg>
    std::pair<bool, std::vector<std::string>> evaluate_all(const RuleArg& arg, bool& all_passed) {
        auto result = evaluate_all<RuleArg>(arg);
        all_passed = result.first;
        return result;
    }
    
    // 启用/禁用规则
    void set_enabled(const std::string& rule_filename, bool enabled);
    
    // 打印所有规则信息
    nlohmann::json list_rules() const;

    // 检测规则代码合法性
    bool validate_rule_code(const std::string& lua_code);

private:
    std::unique_ptr<sol::state> L;
    std::unordered_map<std::string, Rule> rules;  // key: 规则文件名
    std::string dir_path_;

    // 创建安全执行沙箱
    void create_sandbox();

    // 检测规则文件lua语法
    bool check_lua_syntax(const std::string& filepath);
};
