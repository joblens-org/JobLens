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
// SchedulerAdapterRegistry implementation (see scheduler_adapter.hpp).
#include "core/scheduler_adapter.hpp"
#include "core/collector_type.h"
#include "common/procfs.hpp"

SubAttrMatcher ISchedulerAdapter::duplicateMatcher(const Job& job) const
{
    SubAttrMatcher matcher;
    matcher.subtype = job.subtype;
    return matcher;
}

NamespaceFacts ISchedulerAdapter::namespaceFacts(const Job& job) const
{
    return {job.clusterName, job.clusterTag};
}

pid_t ISchedulerAdapter::firstPid(const Job& job)
{
    for (const auto& pid_identity : pid_identities_of(job)) {
        if (pid_identity.pid > 0) return pid_identity.pid;
    }
    return 0;
}

RefreshResult ISchedulerAdapter::probeExecutionEvidence(
    const Job& job, const ProcessLivenessProbe& process_probe,
    const CgroupMembershipProbe& cgroup_probe) const
{
    RefreshResult result;

    // A record without a complete manager identity has no manager to probe:
    // its pid identity list is the only evidence (exactly like the removal pass).
    const ProcessIdentity& manager = manager_of(job);
    if (manager.complete()) {
        const LivenessResult liveness =
            process_probe ? process_probe(manager) : LivenessResult{};
        result.manager = liveness.state;
    }

    // The cgroup is the execution scope: "read and empty" is positive evidence,
    // "unreadable" is none. Members are probed by bare pid, because the cgroup
    // names numbers, not saved identities.
    const std::string cgroup_path = cgroupPathOf(job);
    if (!cgroup_path.empty() && cgroup_probe) {
        const CgroupMembership membership = cgroup_probe(cgroup_path);
        if (membership.readable) {
            if (membership.pids.empty()) result.cgroup_empty_confirmed = true;
            for (const pid_t pid : membership.pids) {
                const LivenessResult member = process_probe
                                                  ? process_probe(ProcessIdentity{pid, 0, {}})
                                                  : LivenessResult{};
                if (member.state == LivenessState::Alive) {
                    result.any_pid_identity_live = true;
                    break;
                }
                if (member.state == LivenessState::Unknown) result.pid_identity_inconclusive = true;
            }
        }
    }

    if (!result.any_pid_identity_live) {
        for (const auto& pid_identity : pid_identities_of(job)) {
            const LivenessResult liveness =
                process_probe ? process_probe(pid_identity) : LivenessResult{};
            if (liveness.state == LivenessState::Alive) {
                result.any_pid_identity_live = true;
                break;
            }
            // An unreadable pid identity (or one with an unvalidated identity)
            // might be alive: it is never death evidence.
            if (liveness.state == LivenessState::Unknown) result.pid_identity_inconclusive = true;
        }
    }
    return result;
}

void ISchedulerAdapter::applyPidIdentityEvidence(Job& job, const std::string& proc_root,
                                            const std::vector<pid_t>& member_pids,
                                            bool authoritative)
{
    if (!authoritative && member_pids.empty()) return;  // no evidence, not absence

    const std::string boot_id = read_boot_id(proc_root);
    ExecutionEvidence& execution = job.execution;
    execution.pid_identities.clear();
    execution.pid_identities.reserve(member_pids.size());
    execution.pid_identities_authoritative = authoritative;
    for (const pid_t pid : member_pids) {
        const ProcessIdentity pid_identity = read_process_identity(proc_root, boot_id, pid);
        if (!pid_identity.complete()) execution.pid_identities_authoritative = false;
        execution.pid_identities.push_back(pid_identity);
    }
}

RefreshResult ISchedulerAdapter::refreshPidIdentities(
    Job& job, const std::string& proc_root, const std::string& cgroup_root,
    const std::string& recorded_cgroup_path,
    const ProcessLivenessProbe& process_probe,
    const CgroupMembershipProbe& cgroup_probe) const
{
    const ProcessIdentity& manager = manager_of(job);
    if (!manager.complete()) return probeExecutionEvidence(job, process_probe, cgroup_probe);

    const JobMembershipRead membership = read_job_membership(
        proc_root, cgroup_root, manager.pid, recorded_cgroup_path);
    applyPidIdentityEvidence(job, proc_root, membership.pids, membership.authoritative);

    RefreshResult evidence = probeExecutionEvidence(job, process_probe, cgroup_probe);
    // A job-scoped cgroup that was read and listed no process is positive
    // termination evidence even when the record never carried its path: the
    // read proves the execution scope is empty, which is what the removal
    // rule needs. A shared daemon tree is discarded by read_job_membership and
    // never reaches this branch.
    if (membership.authoritative && membership.pids.empty())
        evidence.cgroup_empty_confirmed = true;
    return evidence;
}

bool namespace_facts_compatible(const NamespaceFacts& left, const NamespaceFacts& right)
{
    if (left.size() != right.size()) return false;
    for (size_t i = 0; i < left.size(); ++i) {
        if (left[i].empty() || right[i].empty()) continue;  // unknown proves nothing
        if (left[i] != right[i]) return false;
    }
    return true;
}

#include <spdlog/spdlog.h>

SchedulerAdapterRegistry& SchedulerAdapterRegistry::instance()
{
    static SchedulerAdapterRegistry registry;
    return registry;
}

void SchedulerAdapterRegistry::registerAdapter(const std::string& name, Factory factory)
{
    std::lock_guard<std::mutex> lock(mutex_);
    // Find-then-store, never emplace-then-assign: a failed emplace may already
    // have moved-from `factory`, and assigning it here would brick a stored
    // factory with an empty function (every TU including job_registry.hpp runs
    // the REGISTER_SCHEDULER_ADAPTER registrar once, so duplicates are the
    // norm). Replacement is intentional: last registration wins.
    auto it = factories_.find(name);
    if (it == factories_.end())
        factories_.emplace_hint(it, name, std::move(factory));
    else
        it->second = std::move(factory);
}

bool SchedulerAdapterRegistry::known(const std::string& name) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return factories_.count(name) != 0;
}

std::vector<std::string> SchedulerAdapterRegistry::names() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> out;
    out.reserve(factories_.size());
    for (const auto& [name, factory] : factories_) out.push_back(name);
    return out;
}

std::unique_ptr<ISchedulerAdapter> SchedulerAdapterRegistry::build(
    const std::string& name, const DiscoveryTypeSettings& settings) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = factories_.find(name);
    if (it == factories_.end()) return nullptr;
    return it->second(settings);
}
