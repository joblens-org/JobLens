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
// SlurmSchedulerAdapter — the Slurm discovery adapter. Owns the discovery
// watcher, the Slurm typed facts and every projection between those facts and
// the Job/JobRecord fields; registered under the name "slurm".
#pragma once

#include <sys/types.h>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include <nlohmann/json.hpp>

#include "core/scheduler_adapter.hpp"
#include "core/job_groups.hpp"
#include "core/sub_attr_reflect.hpp"

struct SlurmJobAttr {
    bool     auto_update_child{};
    pid_t    stepd_pid{};          // slurmstepd进程PID
    size_t   job_id{};             // Slurm Job ID
    uint32_t step_id{};            // Slurm Step ID
    std::string clusterName{};     // 集群名称
    std::string cgroup_path{};     // cgroup路径
    std::string user{};            // 作业用户
    std::string job_name{};        // 作业名称
    std::string partition{};       // 分区/队列名称
    std::string acct_group{};      // SLURM_JOB_ACCOUNT
    std::string unix_group{};      // SLURM_JOB_GID resolved through NSS
    int64_t unix_gid{-1};          // -1 = unavailable

    static constexpr std::array field_names{
        "auto_update_child", "stepd_pid", "job_id", "step_id",
        "clusterName", "cgroup_path", "user", "job_name", "partition",
        "acct_group", "unix_group", "unix_gid"};
    static constexpr std::array optional_fields{"acct_group", "unix_group", "unix_gid"};

    static constexpr bool is_optional(std::string_view name)
    {
        for (const char* candidate : optional_fields)
            if (name == candidate) return true;
        return false;
    }
};

static_assert(joblens::facts::valid_schema<SlurmJobAttr>(),
              "SlurmJobAttr::field_names must cover every reflected member");

inline constexpr auto legacy_matcher_fields(const SlurmJobAttr*)
{
    return std::make_tuple(
        std::pair{"slurm_job_id", &SlurmJobAttr::job_id},
        std::pair{"slurm_step_id", &SlurmJobAttr::step_id},
        std::pair{"slurm_user", &SlurmJobAttr::user},
        std::pair{"slurm_job_name", &SlurmJobAttr::job_name},
        std::pair{"slurm_stepd_pid", &SlurmJobAttr::stepd_pid});
}

JobGroupView scheduler_groups(const SlurmJobAttr& facts);
bool backfill_scheduler_groups(SlurmJobAttr& target, const SlurmJobAttr& source);

class SlurmSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit SlurmSchedulerAdapter(const SchedulerAdapterSettings& settings);
    ~SlurmSchedulerAdapter() override;

    const std::string& name() const override;
    std::string configSection() const override;
    IDiscoverySource& source() override;
    AdapterTraits traits() const override;

    DiscoveryOptions discoveryOptions(const SchedulerAdapterSettings& settings) const override;

    // ---- typed facts / record projection ---------------------------------
    Job decodeFacts(const nlohmann::json& row) const override;
    NativeSelector legacySelector(const Job& job) const override;
    void projectFacts(Job& job) const override;

    NativeSelector registrationSelector(const Job& job) const override;
    void applyRequestFacts(Job& job, const nlohmann::json& request) const override;
    SubAttrMatcher duplicateMatcher(const Job& job) const override;

    // ---- per-record fact refresh (slurmstepd is the manager) --------------
    std::optional<ProcessIdentity> managerOfPid(pid_t job_pid) const override;
    RefreshResult refreshFacts(Job& job,
                               const ProcessLivenessProbe& process_probe,
                               const CgroupMembershipProbe& cgroup_probe) const override;

    // ---- structured namespace: the cluster name only ----------------------
    NamespaceFacts namespaceFacts(const Job& job) const override;
    NamespaceFacts selectorNamespaceFacts(const NativeSelector& selector) const override;
    bool adoptNamespaceAliases(Job& job, const std::string& clusterName,
                               const std::string& clusterTag) const override;
    void renderNamespace(Job& job) const override;
    std::string cgroupPathOf(const Job& job) const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
