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
// SlurmSchedulerAdapter implementation. The facts struct lives in the adapter
// header; this TU owns every projection between those facts and the
// Job/JobRecord, so no scheduler branch leaks into the registry or the store.
#include "job_watcher/slurm_adapter.hpp"

#include <utility>
#include <variant>
#include <stdexcept>

#include <fmt/format.h>

#include "core/collector_type.h"
#include "job_watcher/slurm_job_watcher.hpp"
#include "common/procfs.hpp"
#include "common/slurm_job.hpp"

struct SlurmSchedulerAdapter::Impl {
    explicit Impl(DiscoveryOptions options) : watcher(std::move(options)) {}
    SlurmJobWatcher watcher;
};

SlurmSchedulerAdapter::SlurmSchedulerAdapter(const DiscoveryTypeSettings& settings)
    : impl_(std::make_unique<Impl>(discoveryOptions(settings)))
{
}

SlurmSchedulerAdapter::~SlurmSchedulerAdapter() = default;

const std::string& SlurmSchedulerAdapter::name() const
{
    static const std::string value{"slurm"};
    return value;
}

std::string SlurmSchedulerAdapter::configSection() const
{
    return "slurm_job_watcher";
}

IDiscoverySource& SlurmSchedulerAdapter::source()
{
    return impl_->watcher;
}

AdapterTraits SlurmSchedulerAdapter::traits() const
{
    AdapterTraits traits;
    traits.cgroup_scoped_membership = true;
    traits.kernel_attribution = true;
    return traits;
}

DiscoveryOptions SlurmSchedulerAdapter::discoveryOptions(
    const DiscoveryTypeSettings& settings) const
{
    DiscoveryOptions options;
    options.enable_event_source = true;
    if (!settings.clusterName.empty())
        options.discovery.clusterName = settings.clusterName;
    return options;
}

Job SlurmSchedulerAdapter::decodeFacts(const nlohmann::json& row) const
{
    Job job;
    job.jobtype = static_cast<JobType>(row.value("jobtype", 0));
    job.subtype = static_cast<JobSubType>(row.value("subtype", 0));
    job.subtype_name = row.value("subtype_name", name());
    set_pids(job, row.value("pids", nlohmann::json::array()).get<std::vector<pid_t>>());
    job.JobNativeID = row.value("JobNativeID", "");
    job.clusterName = row.value("clusterName", "");
    job.clusterTag = row.value("clusterTag", "");
    job.sub_attr = row.contains("sub_attr")
                       ? row["sub_attr"].get<SlurmJobAttr>()
                       : SlurmJobAttr{};
    projectFacts(job);
    return job;
}

NativeSelector SlurmSchedulerAdapter::legacySelector(const Job& job) const
{
    NativeSelector selector;
    selector.adapter = name();

    const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    if (attr == nullptr && job.subtype != JobSubType::Slurm) return selector;
    if (attr != nullptr) {
        // The typed facts are the namespace source; the common field is
        // their render (kept as a fallback for facts built before one existed).
        selector.clusterName = attr->clusterName;
        if (attr->step_id != 0) {
            if (const auto name = slurm_step_name(attr->step_id)) {
                selector.step_id = *name;
            } else {
                selector.step_id = std::to_string(attr->step_id);
            }
        }
    }
    if (selector.clusterName.empty()) selector.clusterName = job.clusterName;
    // The original native string wins over the typed number: a zero-valued
    // field must not shadow the render.
    if (selector.job_native_id.empty()) selector.job_native_id = job.JobNativeID;
    if (selector.job_native_id.empty() && attr != nullptr)
        selector.job_native_id = std::to_string(attr->job_id);

    return selector;
}

void SlurmSchedulerAdapter::projectFacts(Job& job) const
{
    job.subtype = JobSubType::Slurm;
    job.subtype_name = name();
    if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys)
        job.jobtype = JobType::Job;

    const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    if (attr == nullptr) return;
    if (job.JobNativeID.empty()) job.JobNativeID = std::to_string(attr->job_id);
    if (job.clusterName.empty()) job.clusterName = attr->clusterName;
    // clusterTag is not a Slurm namespace component and stays as-is:
    // discovery may carry a display value there.
}

NativeSelector SlurmSchedulerAdapter::registrationSelector(const Job& job) const
{
    NativeSelector selector = legacySelector(job);
    if (selector.clusterName.empty())
        selector.clusterName = SlurmJob::clusterName();
    if (selector.clusterName.empty() || selector.job_native_id.empty()) return {};
    return selector;
}

void SlurmSchedulerAdapter::applyRequestFacts(Job& job,
                                              const nlohmann::json& request) const
{
    const auto sub = request.value("sub_attr", nlohmann::json::object());
    const auto native_job_number = sub.value("job_id", nlohmann::json(0u)).get<size_t>();
    const auto step_id = sub.value("step_id", nlohmann::json(0u)).get<uint32_t>();
    // A registration must name a scheduler job; 0 is not a Slurm job id.
    if (native_job_number == 0) {
        throw std::runtime_error("sub_attr.job_id is required");
    }

    SlurmJobAttr attr;
    attr.auto_update_child = request.value("auto_update_child", true);
    attr.job_id = native_job_number;
    attr.step_id = step_id;
    attr.clusterName = sub.value("clusterName", "");
    attr.user = sub.value("user", "");

    job.sub_attr = attr;
    job.subtype = JobSubType::Slurm;
    job.subtype_name = name();
    job.JobNativeID = std::to_string(native_job_number);
    projectFacts(job);
}

SubAttrMatcher SlurmSchedulerAdapter::duplicateMatcher(const Job& job) const
{
    SubAttrMatcher matcher;
    matcher.subtype = JobSubType::Slurm;
    if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
        matcher.slurm_job_id = attr->job_id;
        matcher.slurm_step_id = attr->step_id;
    }
    return matcher;
}

std::optional<ProcessIdentity> SlurmSchedulerAdapter::managerOfPid(pid_t job_pid) const
{
    if (job_pid <= 0) return std::nullopt;
    const DiscoveryEnvironment env = impl_->watcher.discovery_environment();
    const auto parent = read_proc_ppid(env.proc_root, job_pid);
    if (!parent || *parent <= 0) return std::nullopt;

    // The per-job manager is the step daemon, not slurmd (one per node, shared
    // by every job): the parent must be a slurmstepd, which also keeps a
    // recycled pid from fabricating an execution.
    std::string cmdline;
    std::string error;
    if (!read_proc_cmdline(env.proc_root, *parent, cmdline, error)) return std::nullopt;
    if (cmdline.find("slurmstepd") == std::string::npos) return std::nullopt;

    const ProcessIdentity manager =
        read_process_identity(env.proc_root, read_boot_id(env.proc_root), *parent);
    if (!manager.complete()) return std::nullopt;  // a partial identity is no manager
    return manager;
}

RefreshResult SlurmSchedulerAdapter::refreshFacts(
    Job& job, const ProcessLivenessProbe& process_probe,
    const CgroupMembershipProbe& cgroup_probe) const
{
    const DiscoveryEnvironment env = impl_->watcher.discovery_environment();
    ExecutionEvidence& execution = job.execution;
    // Assigned before the first read so the store can reject an older pass
    // overwriting newer facts.
    execution.read_order = next_discovery_read_order();

    const pid_t member_hint = firstPid(job);
    if (!execution.manager.complete() && member_hint > 0) {
        if (const auto manager = managerOfPid(member_hint)) execution.manager = *manager;
    }

    // Targeted refresh through the watcher. It seeds the member map with the
    // one hinted pid and merges the step cgroup, so this stays O(job): the
    // scan's whole-process-table parent map is never rebuilt per record.
    if (execution.manager.complete() && member_hint > 0) {
        if (auto observed =
                impl_->watcher.observeForHint(member_hint, execution.manager.pid)) {
            // Execution evidence only. JobNativeID, cluster, typed attrs and
            // the record's JobID/selection/state are the creation snapshot and
            // are never rewritten by a refresh.
            execution.manager = observed->execution.manager;
            execution.pid_identities = observed->execution.pid_identities;
            execution.pid_identities_authoritative =
                observed->execution.pid_identities_authoritative;
            execution.read_order = observed->execution.read_order;
            return probeExecutionEvidence(job, process_probe, cgroup_probe);
        }
    }

    // No targeted read: the stepd is gone or the record has no member pid to
    // anchor on. Without a manager there is no execution scope to read, so the
    // recorded member identities are the only evidence; with one, keep the
    // membership current from the step cgroup or the stepd's own children —
    // never a process-table scan.
    if (!execution.manager.complete())
        return probeExecutionEvidence(job, process_probe, cgroup_probe);
    const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    return refreshPidIdentities(
        job, env.proc_root, env.cgroup_root,
        attr != nullptr ? attr->cgroup_path : std::string{},
        process_probe, cgroup_probe);
}

std::string SlurmSchedulerAdapter::cgroupPathOf(const Job& job) const
{
    const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    return attr ? attr->cgroup_path : std::string{};
}

NamespaceFacts SlurmSchedulerAdapter::namespaceFacts(const Job& job) const
{
    const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    return {attr ? attr->clusterName : std::string{}};
}

NamespaceFacts SlurmSchedulerAdapter::selectorNamespaceFacts(
    const NativeSelector& selector) const
{
    return {selector.clusterName};
}

bool SlurmSchedulerAdapter::adoptNamespaceAliases(Job& job,
                                                  const std::string& clusterName,
                                                  const std::string& clusterTag) const
{
    (void)clusterTag;  // Slurm has no schedd-tag namespace component
    auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    if (attr == nullptr) return true;
    if (!clusterName.empty() && !attr->clusterName.empty()
            && clusterName != attr->clusterName) return false;
    if (attr->clusterName.empty()) attr->clusterName = clusterName;
    return true;
}

void SlurmSchedulerAdapter::renderNamespace(Job& job) const
{
    if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
        job.clusterName = attr->clusterName;
        // clusterTag is not a Slurm namespace component and stays as-is:
        // discovery may carry a display value there.
    }
}

// The adapter owns its native-id rule, registered alongside it: Slurm
// native ids are the logical job number without a step; a step refines the
// identity (normalized, special names preserved).
REGISTER_NATIVE_ID_RULE("slurm", [](const NativeSelector& selector)
    -> std::optional<CanonicalParts> {
    auto job_number = strip_leading_zeros(selector.job_native_id);
    if (!job_number) return std::nullopt;
    CanonicalParts parts;
    if (!selector.step_id.empty()) {
        auto step = normalize_slurm_step_id(selector.step_id);
        if (!step) return std::nullopt;
        parts.step_id = *step;
    }
    parts.cluster_namespace = selector.clusterName;
    parts.canonical_native = *job_number;
    return parts;
})

REGISTER_SCHEDULER_ADAPTER("slurm", SlurmSchedulerAdapter)
