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
#include "core/job_auto_policy.hpp"

#include <spdlog/spdlog.h>

AutoCollectorPolicy AutoCollectorPolicy::defaults(std::vector<std::string> collectors)
{
    AutoCollectorPolicy policy;
    policy.mode_ = Mode::Defaults;
    policy.defaults_ = std::move(collectors);
    return policy;
}

AutoCollectorPolicy AutoCollectorPolicy::rules(std::string rules_dir, std::string rules_prefix,
                                                std::vector<std::string> fallback_collectors)
{
    AutoCollectorPolicy policy;
    policy.mode_ = Mode::Rules;
    policy.rules_dir_ = std::move(rules_dir);
    policy.rules_prefix_ = std::move(rules_prefix);
    policy.defaults_ = std::move(fallback_collectors);
    policy.rules_ = std::make_shared<RulesState>();
    return policy;
}

AutoCollectorPolicy::Decision AutoCollectorPolicy::select(const Job& job,
                                                          std::string& policy_revision,
                                                          std::string& error) const
{
    policy_revision.clear();
    error.clear();

    if (mode_ == Mode::Disabled) return {};

    if (mode_ == Mode::Defaults) {
        // use_rules=false: the configured collectors go to every job of the
        // type; there is no per-job decision to make.
        policy_revision = "defaults";
        return Decision{true, defaults_};
    }

    // use_rules=true: the rule engine decides per job — the first accepting
    // rule (priority order) wins and its collector list is the selection. A
    // rule that accepts without naming collectors falls back to the
    // configured defaults; with no defaults configured the decision is
    // refused (an error, not a verdict). An evaluation error is reported so
    // the caller keeps the previous decision instead of guessing.
    try {
        std::lock_guard<std::mutex> lock(rules_->mutex);
        if (!rules_->manager) {
            rules_->manager =
                std::make_unique<RulesManager>("AutoCollectorPolicy", rules_dir_, rules_prefix_);
        }
        const RuleOutcome outcome = rules_->manager->evaluate_all<Job>(job);
        policy_revision = "rules:" + rules_prefix_;
        if (!outcome.error.empty()) {
            error = outcome.error;
            return {};
        }
        if (!outcome.accepted) return {};
        if (!outcome.collectors.empty()) return Decision{true, outcome.collectors};

        if (defaults_.empty()) {
            spdlog::error("AutoCollectorPolicy: rule '{}' accepted job '{}' without "
                          "collectors and no auto_add_collectors defaults are configured; "
                          "refusing the decision",
                          outcome.rule_name, job.NativeJobID);
            error = "rule '" + outcome.rule_name +
                    "' accepted without collectors but no default collectors are configured";
            return {};
        }
        spdlog::info("AutoCollectorPolicy: rule '{}' accepted job '{}' without "
                     "collectors; using the auto_add_collectors defaults",
                     outcome.rule_name, job.NativeJobID);
        return Decision{true, defaults_};
    } catch (const std::exception& failure) {
        error = std::string("rule evaluation failed: ") + failure.what();
        return {};
    }
}
