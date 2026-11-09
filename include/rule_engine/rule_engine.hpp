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

#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <filesystem>
#include <memory>
#include <lua.hpp>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sol/sol.hpp>

#include "common/struct2ltable.hpp"


namespace fs = std::filesystem;

struct Rule {
    std::string name;           // 规则名称
    std::string description;    // 规则描述
    bool enabled;              // 是否启用
    int priority;              // 优先级（越大越高）
    sol::protected_function condition;
    uint64_t rule_hash;

    Rule() : enabled(true), priority(0) {}
};

// One rule-walk result. The condition contract is `return bool[, string]`:
// the bool is the rule's own verdict, the optional string is a comma-separated
// collector list (empty or absent = "not specified" — the caller decides what
// that means, e.g. fall back to configured defaults). `error` is non-empty
// when the rule violated the contract or the engine failed; such a walk has no
// verdict at all. `rule_name` names the winning rule of an accepted walk.
struct RuleOutcome {
    bool accepted = false;
    std::vector<std::string> collectors;
    std::string error;
    std::string rule_name;
};

// Split "a, b,,c" into trimmed non-empty tokens.
inline std::vector<std::string> split_collector_list(const std::string& text) {
    std::vector<std::string> out;
    std::string token;
    auto flush = [&] {
        const auto begin = token.find_first_not_of(" \t\r\n");
        if (begin == std::string::npos) return;
        const auto end = token.find_last_not_of(" \t\r\n");
        out.push_back(token.substr(begin, end - begin + 1));
    };
    for (const char c : text) {
        if (c == ',') { flush(); token.clear(); }
        else { token.push_back(c); }
    }
    flush();
    return out;
}

inline const char* lua_type_name(sol::type type) {
    switch (type) {
        case sol::type::nil: return "nil";
        case sol::type::boolean: return "boolean";
        case sol::type::number: return "number";
        case sol::type::string: return "string";
        case sol::type::table: return "table";
        case sol::type::function: return "function";
        default: return "value";
    }
}

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
    // 契约: condition(job) 返回 (bool[, string])
    //   bool   - 规则自己的判定（true 接受 / false 不接受）
    //   string - 可选的逗号分隔采集器列表；缺省或空 = 未指定
    // 契约违规（无返回值、首返回值非 bool、第二返回值非 string）与引擎
    // 失败一样填 outcome.error：这次评估没有判定，调用方保持原状。
    template <typename RuleArg>
    RuleOutcome evaluate(const std::string& rule_filename, const RuleArg& arg) {
        auto it = rules.find(rule_filename);
        if (it == rules.end()) {
            return {false, {}, "RuleEngine: Rule '" + rule_filename + "' not found", {}};
        }

        Rule& rule = it->second;
        if (!rule.enabled) {
            spdlog::info("RuleEngine: Rule '{}' is disabled", rule.name);
            return {false, {}, "", {}};
        }

        try {
            // 每条规则单次执行都有指令/内存预算；超限由 Lua hook 中断。
            struct BudgetGuard {
                RuleEngine* engine;
                explicit BudgetGuard(RuleEngine* e) : engine(e) { engine->begin_rule_execution(); }
                ~BudgetGuard() { engine->end_rule_execution(); }
            } budget_guard(this);

            // 创建参数表
            auto args_table = to_lua_table(*L, arg);

            // 调用condition函数
            sol::protected_function_result result = rule.condition(args_table);
            if (!result.valid()) {
                sol::error err = result;
                return {false, {},
                        "RuleEngine: Lua error in " + rule_filename + ": " + err.what(), {}};
            }

            if (result.return_count() == 0) {
                return {false, {},
                        "RuleEngine: rule '" + rule.name + "' returned nothing; "
                        "the contract is (bool[, string])", {}};
            }
            sol::object first = result.get<sol::object>(0);
            if (!first.is<bool>()) {
                return {false, {},
                        "RuleEngine: rule '" + rule.name + "' must return (bool[, string]); "
                        "first return is " + std::string(lua_type_name(first.get_type())), {}};
            }
            RuleOutcome outcome;
            outcome.rule_name = rule.name;
            outcome.accepted = first.as<bool>();
            if (!outcome.accepted) return outcome;   // 拒绝：第二返回值无关紧要

            if (result.return_count() >= 2) {
                sol::object second = result.get<sol::object>(1);
                if (second.get_type() != sol::type::nil) {
                    if (!second.is<std::string>()) {
                        return {false, {},
                                "RuleEngine: rule '" + rule.name + "' must return "
                                "(bool[, string]); second return is " +
                                    std::string(lua_type_name(second.get_type())), {}};
                    }
                    outcome.collectors = split_collector_list(second.as<std::string>());
                }
            }
            return outcome;
        } catch (const std::exception& e) {
            return {false, {},
                    "RuleEngine: Exception evaluating " + rule_filename + ": " + e.what(), {}};
        }
    }

    
    // 执行所有启用的规则：首个接受者胜出。
    // 按优先级降序逐条评估：任何一条出错 → 整次评估无判定（outcome.error）；
    // 返回 true 的第一条规则胜出，它的采集器列表就是结果（空列表 = 未指定，
    // 由调用方决定回退）；一条返回 false 的规则只是不接受，不否决后面的规则；
    // 全部不接受 → accepted=false。
    template <typename RuleArg>
    RuleOutcome evaluate_all(const RuleArg& arg) {
        std::vector<std::pair<int, std::string>> sorted_rules;

        // 收集并排序
        for (auto& [filename, rule] : rules) {
            if (rule.enabled) {
                sorted_rules.push_back({-rule.priority, filename});  // 负号实现降序
            }
        }
        std::sort(sorted_rules.begin(), sorted_rules.end());

        for (auto& [neg_priority, filename] : sorted_rules) {
            RuleOutcome outcome = evaluate(filename, arg);
            if (!outcome.error.empty()) return outcome;
            if (outcome.accepted) return outcome;
        }

        return {};   // 没有任何规则接受
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

    // 规则执行预算：进入/退出时重置计数器，hook 在超限时抛 Lua 错误。
    void begin_rule_execution();
    void end_rule_execution();

    // 检测规则文件lua语法
    bool check_lua_syntax(const std::string& filepath);
};