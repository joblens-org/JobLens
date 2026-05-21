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
#include "common/directory_watcher.hpp"
#include "rule_engine/rule_engine.hpp"
#include <spdlog/spdlog.h>

class RulesManager
{
public:
    RulesManager(const std::string& name, const std::string& rules_dir, const std::string& rules_prefix);
    ~RulesManager();

    // 执行所有规则评估
    // 返回值: std::pair<bool, std::vector<std::string>>
    // 第一个元素: 是否所有规则都通过
    // 第二个元素: 所有规则添加的采集器名称列表（去重）
    template<typename RuleArg>
    std::pair<bool, std::vector<std::string>> evaluate_all(const RuleArg& arg) {
        auto result = rule_engine_.evaluate_all<RuleArg>(arg);
        current_passed_rules_ = {};  // 清空旧的通过规则列表（如果需要保留，可以修改逻辑）
        current_collectors_ = result.second;
        return result;
    }

    std::string failed_rule() const {
        // 返回第一个未通过的规则名称
        return "";
    }

    std::vector<std::string> passed_rules() const {
        return current_passed_rules_;
    }

    std::vector<std::string> get_collectors() const {
        return current_collectors_;
    }

    std::string name() const {
        return name_;
    }

private:
    std::string name_;
    std::string rules_dir_;
    std::string rules_prefix_;
    RuleEngine rule_engine_;
    DirectoryWatcher dir_watcher_;

    std::vector<std::string> current_passed_rules_;
    std::vector<std::string> current_collectors_;
    
    void register_rpc_methods();
    bool add_rule(const std::string& rule_filename, const std::string& lua_code);
    bool remove_rule(const std::string& rule_filename);
    bool update_rule(const std::string& rule_filename, const std::string& lua_code);
};

