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
// CondorSchedulerAdapter implementation. The facts struct lives in the
// adapter header; this TU owns every projection between those facts and the
// Job/JobRecord, so no scheduler branch leaks into the registry or the store.
#include "job_watcher/condor_adapter.hpp"

#include <utility>
#include <variant>

#include <fmt/format.h>

#include "core/collector_type.h"
#include "job_watcher/condor_job_watcher.hpp"
#include "common/condor_job.hpp"
#include "common/procfs.hpp"
#include "common/utils.hpp"

struct CondorSchedulerAdapter::Impl {
    explicit Impl(DiscoveryOptions options) : watcher(std::move(options)) {}
    CondorJobWatcher watcher;
};

CondorSchedulerAdapter::CondorSchedulerAdapter(const InventoryTypeSettings& settings)
    : impl_(std::make_unique<Impl>(discoveryOptions(settings)))
{
}

CondorSchedulerAdapter::~CondorSchedulerAdapter() = default;

const std::string& CondorSchedulerAdapter::name() const
{
    static const std::string value{"condor"};
    return value;
}

std::string CondorSchedulerAdapter::configSection() const
{
    return "condor_job_watcher";
}

IDiscoverySource& CondorSchedulerAdapter::source()
{
    return impl_->watcher;
}

AdapterTraits CondorSchedulerAdapter::traits() const
{
    AdapterTraits traits;
    traits.cgroup_scoped_membership = true;
    traits.kernel_attribution = true;
    return traits;
}

DiscoveryOptions CondorSchedulerAdapter::discoveryOptions(
    const InventoryTypeSettings& settings) const
{
    DiscoveryOptions options;
    options.enable_event_source = true;
    // Verified on the integration node (192.168.82.26): the startd keeps
    // one directory per slot under this subtree, each slot's cgroup.procs
    // listing its processes (starter included).
    options.starter_cgroup_subtree = "system.slice/htcondor";
    if (!settings.cluster_name.empty())
        options.discovery.cluster_name = settings.cluster_name;
    return options;
}

Job CondorSchedulerAdapter::decodeFacts(const nlohmann::json& row) const
{
    Job job;
    job.jobtype = static_cast<JobType>(row.value("jobtype", 0));
    job.subtype = static_cast<JobSubType>(row.value("subtype", 0));
    job.subtype_name = row.value("subtype_name", name());
    set_pids(job, row.value("pids", nlohmann::json::array()).get<std::vector<pid_t>>());
    job.NativeJobID = row.value("native_job_id", "");
    job.cluster_name = row.value("cluster_name", "");
    job.clusterTag = row.value("clusterTag", "");
    job.sub_attr = row.contains("sub_attr")
                       ? row["sub_attr"].get<CondorJobAttr>()
                       : CondorJobAttr{};
    projectFacts(job);
    return job;
}

NativeSelector CondorSchedulerAdapter::legacySelector(const Job& job) const
{
    NativeSelector selector;
    selector.adapter = name();

    const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    if (attr == nullptr && job.subtype != JobSubType::Condor) return selector;
    if (attr != nullptr) {
        // The typed facts are the namespace source; the common fields are
        // their render (kept as a fallback for facts built before one existed).
        selector.cluster_name = attr->collector_host;
        selector.cluster_tag = attr->scheduler_name;
    }
    if (selector.cluster_name.empty()) selector.cluster_name = job.cluster_name;
    if (selector.cluster_tag.empty()) selector.cluster_tag = job.clusterTag;
    // The original native string wins over the typed ids: a row that carries
    // only a render still names its job, and a zero-initialized typed field
    // must not shadow it.
    if (selector.native_job_id.empty()) selector.native_job_id = job.NativeJobID;
    if (selector.native_job_id.empty() && attr != nullptr)
        selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);

    return selector;
}

void CondorSchedulerAdapter::projectFacts(Job& job) const
{
    job.subtype = JobSubType::Condor;
    job.subtype_name = name();
    if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys)
        job.jobtype = JobType::Job;

    const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    if (attr == nullptr) return;
    if (job.NativeJobID.empty())
        job.NativeJobID = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
    if (job.cluster_name.empty()) job.cluster_name = attr->collector_host;
    if (job.clusterTag.empty()) job.clusterTag = attr->scheduler_name;
}


NativeSelector CondorSchedulerAdapter::registrationSelector(const Job& job) const
{
    NativeSelector selector = legacySelector(job);
    // A manual registration carries no discovery environment; fall back to
    // the local collector for the pool-routing component.
    if (selector.cluster_name.empty())
        selector.cluster_name = Utils::get_condor_collector_host();
    if (selector.cluster_name.empty() || selector.native_job_id.empty()) return {};
    return selector;
}

void CondorSchedulerAdapter::applyRequestFacts(Job& job,
                                               const nlohmann::json& request) const
{
    const auto sub = request.value("sub_attr", nlohmann::json::object());
    const auto cluster_id = sub.value("cluster_id", nlohmann::json(0u)).get<size_t>();
    const auto proc_id = sub.value("proc_id", nlohmann::json(0u)).get<size_t>();

    CondorJobAttr attr;
    attr.auto_update_child = request.value("auto_update_child", true);
    attr.cluster_id = cluster_id;
    attr.proc_id = proc_id;
    attr.scheduler_host = sub.value("scheduler_host", "");
    attr.scheduler_name = sub.value("scheduler_name", "");
    attr.owner = sub.value("owner", "");
    attr.collector_host = sub.value("collector_host", "");
    if (attr.collector_host.empty())
        attr.collector_host = Utils::get_condor_collector_host();

    job.sub_attr = attr;
    job.subtype = JobSubType::Condor;
    job.subtype_name = name();
    job.NativeJobID = fmt::format("{}.{}", cluster_id, proc_id);
    projectFacts(job);
}

SubAttrMatcher CondorSchedulerAdapter::duplicateMatcher(const Job& job) const
{
    SubAttrMatcher matcher;
    matcher.subtype = JobSubType::Condor;
    if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
        matcher.condor_cluster_id = attr->cluster_id;
        matcher.condor_proc_id = attr->proc_id;
    }
    return matcher;
}

std::optional<ProcessIdentity> CondorSchedulerAdapter::managerOfPid(pid_t job_pid) const
{
    if (job_pid <= 0) return std::nullopt;
    const DiscoveryEnvironment env = impl_->watcher.discovery_environment();
    const auto parent = read_proc_ppid(env.proc_root, job_pid);
    if (!parent || *parent <= 0) return std::nullopt;

    // A recycled pid must not fabricate an execution: the parent has to be a
    // condor_starter, the same check the exec-hint path applies.
    std::string cmdline;
    std::string error;
    if (!read_proc_cmdline(env.proc_root, *parent, cmdline, error)) return std::nullopt;
    if (cmdline.find("condor_starter") == std::string::npos) return std::nullopt;

    const ProcessIdentity manager =
        read_process_identity(env.proc_root, read_boot_id(env.proc_root), *parent);
    if (!manager.complete()) return std::nullopt;  // a partial identity is no manager
    return manager;
}

RefreshResult CondorSchedulerAdapter::refreshFacts(
    Job& job, const ProcessLivenessProbe& process_probe,
    const CgroupMembershipProbe& cgroup_probe) const
{
    const DiscoveryEnvironment env = impl_->watcher.discovery_environment();
    ExecutionEvidence& execution = job.execution;
    // Assigned before the first read so the store can reject an older pass
    // overwriting newer facts.
    execution.read_order = next_discovery_read_order();

    // The manager is the execution generation. A record that never carried a
    // complete one (a manual row, an interrupted discovery) recovers it from
    // any member pid it still knows.
    const pid_t member_hint = firstMemberPid(job);
    if (!execution.manager.complete() && member_hint > 0) {
        if (const auto manager = managerOfPid(member_hint)) execution.manager = *manager;
    }

    // Targeted refresh through the watcher: this one execution's job ad and
    // membership, the same reader an exec hint uses (no new order is invented
    // here — the observation's own, newer read_order is adopted).
    if (execution.manager.complete() && member_hint > 0) {
        if (auto observed =
                impl_->watcher.observeForHint(member_hint, execution.manager.pid)) {
            // Execution evidence only. NativeJobID, cluster, typed attrs and
            // the record's JobID/selection/state are the creation snapshot and
            // are never rewritten by a refresh.
            execution.manager = observed->execution.manager;
            execution.members = observed->execution.members;
            execution.members_authoritative = observed->execution.members_authoritative;
            execution.read_order = observed->execution.read_order;
            return probeExecutionEvidence(job, process_probe, cgroup_probe);
        }
    }

    // No targeted read: the manager is gone, its ad is unreadable, or the
    // record has no member pid to anchor on. Without a manager there is no
    // execution scope to read, so the recorded member identities are the only
    // evidence; with one, keep the membership current with a single cheap
    // cgroup/children read (never the process table).
    if (!execution.manager.complete())
        return probeExecutionEvidence(job, process_probe, cgroup_probe);
    const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    return refreshManagerMembership(
        job, env.proc_root, env.cgroup_root,
        attr != nullptr ? attr->slots_cgroup_path : std::string{},
        process_probe, cgroup_probe);
}

std::string CondorSchedulerAdapter::cgroupPathOf(const Job& job) const
{
    const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    return attr ? attr->slots_cgroup_path : std::string{};
}

NamespaceFacts CondorSchedulerAdapter::namespaceFacts(const Job& job) const
{
    const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    return {attr ? attr->collector_host : std::string{},
            attr ? attr->scheduler_name : std::string{}};
}

NamespaceFacts CondorSchedulerAdapter::selectorNamespaceFacts(
    const NativeSelector& selector) const
{
    return {selector.cluster_name, selector.cluster_tag};
}

bool CondorSchedulerAdapter::adoptNamespaceAliases(Job& job,
                                                   const std::string& cluster_name,
                                                   const std::string& cluster_tag) const
{
    auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    if (attr == nullptr) return true;  // no typed facts: nothing contradicts
    if (!cluster_name.empty() && !attr->collector_host.empty()
            && cluster_name != attr->collector_host) return false;
    if (!cluster_tag.empty() && !attr->scheduler_name.empty()
            && cluster_tag != attr->scheduler_name) return false;
    if (attr->collector_host.empty()) attr->collector_host = cluster_name;
    if (attr->scheduler_name.empty()) attr->scheduler_name = cluster_tag;
    return true;
}

void CondorSchedulerAdapter::renderNamespace(Job& job) const
{
    if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
        job.cluster_name = attr->collector_host;
        job.clusterTag = attr->scheduler_name;
    }
}

// The adapter owns its native-id rule, registered alongside it: Condor
// native ids are "<ClusterId>.<ProcId>" with decimal parts; leading zeros
// are tolerated and canonicalized ("012.3" == "12.3") so digit
// concatenation can never merge two different jobs ("12.34" versus
// "123.4"). No steps. The namespace separates schedds.
REGISTER_NATIVE_ID_RULE("condor", [](const NativeSelector& selector)
    -> std::optional<CanonicalParts> {
    if (!selector.step_id.empty()) return std::nullopt;  // Condor has no steps
    auto parsed = parse_condor_native(selector.native_job_id);
    if (!parsed) return std::nullopt;
    CanonicalParts parts;
    parts.cluster_namespace = selector.cluster_name;
    if (!selector.cluster_tag.empty())
        parts.cluster_namespace += "@" + selector.cluster_tag;
    parts.canonical_native = parsed->cluster + "." + parsed->proc;
    return parts;
})

REGISTER_SCHEDULER_ADAPTER("condor", CondorSchedulerAdapter)
