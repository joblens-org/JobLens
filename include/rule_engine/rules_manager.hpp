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

    // 执行所有规则评估（首个接受者胜出；error 非空 = 整次评估无判定）
    template<typename RuleArg>
    RuleOutcome evaluate_all(const RuleArg& arg) {
        return rule_engine_.evaluate_all<RuleArg>(arg);
    }

    // 规则数（已加载的 .lua 文件数）
    std::size_t rule_count() const;

    std::string name() const {
        return name_;
    }

private:
    std::string name_;
    std::string rules_dir_;
    std::string rules_prefix_;
    RuleEngine rule_engine_;
    DirectoryWatcher dir_watcher_;
    
    void register_rpc_methods();
    bool add_rule(const std::string& rule_filename, const std::string& lua_code);
    bool remove_rule(const std::string& rule_filename);
    bool update_rule(const std::string& rule_filename, const std::string& lua_code);
};

