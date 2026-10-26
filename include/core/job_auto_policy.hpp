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
// Automatic collector selection for newly registered instances
// (doc/job_inventory_collection_decoupling_plan.md section 6.1).
//
// The policy answers exactly one question: "which collectors does this job
// get?" - never "is this job allowed to exist". A rejected job stays in the
// inventory with an empty selection.
//
//   * disabled:      nothing is selected automatically.
//   * defaults:      the configured default list, verbatim, for every job
//                    (use_rules=false: no per-job decision at all).
//   * rules:         the Lua rule engine decides per job (use_rules=true:
//                    accept = the rule's collector list, reject = empty
//                    selection). An evaluation error is reported to the
//                    caller, which keeps the previous decision instead of
//                    guessing.
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "core/collector_type.h"
#include "rule_engine/rules_manager.hpp"

class AutoCollectorPolicy {
public:
    AutoCollectorPolicy() = default;

    static AutoCollectorPolicy disabled();
    static AutoCollectorPolicy defaults(std::vector<std::string> collectors);
    static AutoCollectorPolicy rules(std::string rules_dir, std::string rules_prefix);

    bool enabled() const { return mode_ != Mode::Disabled; }

    // `policy_revision` identifies the decision source in the binding record;
    // `error` is non-empty when the rule engine failed (the caller then keeps
    // the previous decision rather than applying the returned list).
    std::vector<std::string> select(const Job& job, std::string& policy_revision,
                                    std::string& error) const;

private:
    enum class Mode { Disabled, Defaults, Rules };

    // The rule engine is created lazily on first use: a policy that never sees
    // a job costs no Lua state. Guarded because evaluation can run on the
    // maintenance worker and on an on-demand refresh thread; held behind a
    // shared_ptr so the policy stays movable inside the registry's map.
    struct RulesState {
        std::mutex mutex;
        std::unique_ptr<RulesManager> manager;
    };

    Mode mode_ = Mode::Disabled;
    std::vector<std::string> defaults_;
    std::string rules_dir_;
    std::string rules_prefix_;
    std::shared_ptr<RulesState> rules_;
};
