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
// CondorSchedulerAdapter — the HTCondor inventory adapter. Owns the discovery
// watcher, the Condor typed facts and every projection between those facts and
// the Job/JobRecord fields; registered under the name "condor".
//
// The facts struct is declared here, next to the adapter that interprets it.
// Its members are the schema: `field_names` is the only place the JSON keys are
// written down, and `valid_schema` fails the build when the two drift.
#pragma once

#include <sys/types.h>

#include <array>
#include <memory>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include "core/scheduler_adapter.hpp"
#include "core/sub_attr_reflect.hpp"

struct CondorJobAttr {
    bool        auto_update_child{};
    pid_t       starter_pid{};
    size_t      cluster_id{};   //将会逐步将condor中的jobid切换到这里
    size_t      proc_id{};
    std::string scheduler_host{};
    std::string scheduler_name{};
    std::string slots_cgroup_path{};
    std::string owner{};
    std::string job_ad_path{};
    std::string collector_host{};  // COLLECTOR_HOST 值，用于查询
    std::string acct_group{};      // AcctGroup
    std::string unix_group{};      // PrimaryUnixGroup

    static constexpr std::array field_names{
        "auto_update_child", "starter_pid", "cluster_id", "proc_id",
        "scheduler_host", "scheduler_name", "slots_cgroup_path", "owner",
        "job_ad_path", "collector_host", "acct_group", "unix_group"};
    // Existing records need not carry scheduler_name or the new group facts.
    static constexpr std::array optional_fields{"scheduler_name", "acct_group", "unix_group"};

    static constexpr bool is_optional(std::string_view name)
    {
        for (const char* candidate : optional_fields)
            if (name == candidate) return true;
        return false;
    }
};

static_assert(joblens::facts::valid_schema<CondorJobAttr>(),
              "CondorJobAttr::field_names must cover every reflected member");

class CondorSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit CondorSchedulerAdapter(const InventoryTypeSettings& settings);
    ~CondorSchedulerAdapter() override;

    const std::string& name() const override;
    std::string configSection() const override;
    IDiscoverySource& source() override;
    AdapterTraits traits() const override;

    DiscoveryOptions discoveryOptions(const InventoryTypeSettings& settings) const override;

    // ---- typed facts / record projection ---------------------------------
    Job decodeFacts(const nlohmann::json& row) const override;
    NativeSelector legacySelector(const Job& job) const override;
    void projectFacts(Job& job) const override;

    NativeSelector registrationSelector(const Job& job) const override;
    void applyRequestFacts(Job& job, const nlohmann::json& request) const override;
    SubAttrMatcher duplicateMatcher(const Job& job) const override;
    void refreshLegacyFacts(Job& job) const override;

    // ---- structured namespace: pool routing label + schedd name -----------
    NamespaceFacts namespaceFacts(const Job& job) const override;
    NamespaceFacts selectorNamespaceFacts(const NativeSelector& selector) const override;
    bool adoptNamespaceAliases(Job& job, const std::string& cluster_name,
                               const std::string& cluster_tag) const override;
    void renderNamespace(Job& job) const override;
    std::string cgroupPathOf(const Job& job) const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
