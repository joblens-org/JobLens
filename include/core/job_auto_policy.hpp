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
// Automatic collector selection for newly registered instances.
//
// The policy answers exactly one question: "which collectors does this job
// get?" - never "is this job allowed to exist". A rejected job stays in the
// registry with an empty selection.
//
//   * disabled:      nothing is selected automatically.
//   * defaults:      the configured default list, verbatim, for every job
//                    (`auto_add: default`: no per-job decision at all).
//   * rules:         the Lua rule engine decides per job (`auto_add: rule`:
//                    the first accepting rule in priority order wins and
//                    its list is the selection). An accepted rule that does
//                    not name collectors falls back to the configured
//                    default list; with no defaults configured that is an
//                    error and the decision is refused. An evaluation error
//                    is reported to the caller, which keeps the previous
//                    decision instead of guessing.
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "core/collector_type.h"
#include "rule_engine/rules_manager.hpp"

class AutoCollectorPolicy {
public:
    // One selection verdict. `accepted` is the rule engine's own verdict
    // (rules mode); `collectors` is the selection to apply — a rule's list,
    // the configured defaults when the rule accepted without naming any, or
    // the verbatim configured list in defaults mode.
    struct Decision {
        bool accepted = false;
        std::vector<std::string> collectors;
    };

    AutoCollectorPolicy() = default;

    static AutoCollectorPolicy defaults(std::vector<std::string> collectors);
    // `fallback_collectors` is the per-type auto_add_collectors list: it is
    // the selection for a rule that accepts without naming collectors.
    static AutoCollectorPolicy rules(std::string rules_dir, std::string rules_prefix,
                                     std::vector<std::string> fallback_collectors = {});

    bool enabled() const { return mode_ != Mode::Disabled; }
    // True when this policy owns a Lua rule engine (auto_add: rule). Only such
    // a policy has rules to load or a zero-rule condition worth reporting.
    bool uses_rules() const { return mode_ == Mode::Rules; }

    // Loads the rule engine now instead of on the first select(): the registry
    // arms its policies during startup, so a missing directory, an unreadable
    // file or a broken rule is a startup fact rather than a surprise on the
    // first observed job. Returns the number of loaded rule files (0 for a
    // non-rules policy); `error` is set when the engine could not be created.
    std::size_t load(std::string& error);

    // `policy_revision` identifies the decision source in the binding record;
    // `error` is non-empty when the rule engine failed or an accepted rule
    // named no collectors and no fallback list is configured (the caller then
    // keeps the previous decision rather than applying the returned list).
    Decision select(const Job& job, std::string& policy_revision, std::string& error) const;

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
    // Defaults mode: the verbatim selection for every job. Rules mode: the
    // fallback for a rule that accepts without naming collectors.
    std::vector<std::string> defaults_;
    std::string rules_dir_;
    std::string rules_prefix_;
    std::shared_ptr<RulesState> rules_;
};
