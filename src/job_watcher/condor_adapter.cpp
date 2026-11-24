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

#include <filesystem>
#include <optional>
#include <utility>
#include <variant>
#include <stdexcept>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "core/collector_type.h"
#include "job_watcher/condor_job_watcher.hpp"
#include "common/condor_job.hpp"
#include "common/procfs.hpp"
#include "common/utils.hpp"

namespace {

// A request may carry a typed fact inside sub_attr (the scheduler-owned facts
// object) or at the request root (the legacy trigger shape). sub_attr wins:
// it is the namespace source every other fact is read from.
const nlohmann::json* requestFact(const nlohmann::json& request,
                                  const nlohmann::json& sub, const char* key)
{
    if (sub.is_object()) {
        const auto it = sub.find(key);
        if (it != sub.end() && !it->is_null()) return &*it;
    }
    const auto it = request.find(key);
    if (it != request.end() && !it->is_null()) return &*it;
    return nullptr;
}

std::optional<pid_t> requestPid(const nlohmann::json& request,
                                const nlohmann::json& sub, const char* key)
{
    const nlohmann::json* const value = requestFact(request, sub, key);
    if (value == nullptr || !value->is_number_integer()) return std::nullopt;
    const auto raw = value->get<long long>();
    if (raw <= 0) return std::nullopt;
    return static_cast<pid_t>(raw);
}

std::optional<std::string> requestString(const nlohmann::json& request,
                                        const nlohmann::json& sub, const char* key)
{
    const nlohmann::json* const value = requestFact(request, sub, key);
    if (value == nullptr || !value->is_string()) return std::nullopt;
    auto text = value->get<std::string>();
    if (text.empty()) return std::nullopt;
    return text;
}

// A cgroup path is evidence about this host only when it names a directory
// below the configured cgroup root. Component-wise, never a string prefix:
// "/sys/fs/cgroupX" is not below "/sys/fs/cgroup".
bool cgroup_path_within(const std::string& path, const std::string& root)
{
    if (path.empty() || root.empty()) return false;
    const std::filesystem::path candidate(path);
    if (!candidate.is_absolute()) return false;
    const auto normalized_path = candidate.lexically_normal();
    const auto normalized_root = std::filesystem::path(root).lexically_normal();
    auto path_it = normalized_path.begin();
    for (auto root_it = normalized_root.begin(); root_it != normalized_root.end();
         ++root_it, ++path_it) {
        if (path_it == normalized_path.end() || *path_it != *root_it) return false;
    }
    return true;
}

// A fact that could not be derived is worth a warning once — at registration,
// where the operator can still fix the request — but the periodic refresh
// repeats the same condition every pass, so it stays at debug there.
void report_derivation_issue(bool first_report, const std::string& message)
{
    if (first_report)
        spdlog::warn("CondorSchedulerAdapter: {}", message);
    else
        spdlog::debug("CondorSchedulerAdapter: {}", message);
}

}  // namespace

struct CondorSchedulerAdapter::Impl {
    // Declared before the watcher: the slot-layout hint must be read while
    // `options` is still a live object (the watcher takes it by move).
    explicit Impl(DiscoveryOptions options)
        : starter_cgroup_subtree(options.starter_cgroup_subtree),
          watcher(std::move(options)) {}
    std::string starter_cgroup_subtree;
    CondorJobWatcher watcher;
};

CondorSchedulerAdapter::CondorSchedulerAdapter(const SchedulerAdapterSettings& settings)
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
    const SchedulerAdapterSettings& settings) const
{
    DiscoveryOptions options;
    options.enable_event_source = true;
    // Verified on the integration node (192.168.82.26): the startd keeps
    // one directory per slot under this subtree, each slot's cgroup.procs
    // listing its processes (starter included).
    options.starter_cgroup_subtree = "system.slice/htcondor";
    if (!settings.clusterName.empty())
        options.discovery.clusterName = settings.clusterName;
    return options;
}

Job CondorSchedulerAdapter::decodeFacts(const nlohmann::json& row) const
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
        selector.clusterName = attr->collector_host;
        selector.clusterTag = attr->scheduler_name;
    }
    if (selector.clusterName.empty()) selector.clusterName = job.clusterName;
    if (selector.clusterTag.empty()) selector.clusterTag = job.clusterTag;
    // The original native string wins over the typed ids: a row that carries
    // only a render still names its job, and a zero-initialized typed field
    // must not shadow it.
    if (selector.job_native_id.empty()) selector.job_native_id = job.JobNativeID;
    if (selector.job_native_id.empty() && attr != nullptr)
        selector.job_native_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);

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
    if (job.JobNativeID.empty())
        job.JobNativeID = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
    if (job.clusterName.empty()) job.clusterName = attr->collector_host;
    if (job.clusterTag.empty()) job.clusterTag = attr->scheduler_name;
}


NativeSelector CondorSchedulerAdapter::registrationSelector(const Job& job) const
{
    NativeSelector selector = legacySelector(job);
    // A manual registration carries no discovery environment; fall back to
    // the local collector for the pool-routing component.
    if (selector.clusterName.empty())
        selector.clusterName = Utils::get_condor_collector_host();
    if (selector.clusterName.empty() || selector.job_native_id.empty()) return {};
    return selector;
}

void CondorSchedulerAdapter::applyRequestFacts(Job& job,
                                               const nlohmann::json& request) const
{
    const auto sub = request.value("sub_attr", nlohmann::json::object());
    const auto cluster_id = sub.value("cluster_id", nlohmann::json(0u)).get<size_t>();
    const auto proc_id = sub.value("proc_id", nlohmann::json(0u)).get<size_t>();
    // A registration must name a scheduler job. Cluster ids start at 1, so a
    // missing cluster_id is a missing identity, never the job "0.<proc>".
    if (cluster_id == 0) {
        throw std::runtime_error("sub_attr.cluster_id is required");
    }

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

    // Optional explicit execution evidence. The caller may already know where
    // the job runs (the trigger resolved the starter, or a restart replays a
    // saved request); validated here, then it wins over derivation.
    const DiscoveryEnvironment env = impl_->watcher.discovery_environment();
    const pid_t member_hint = firstPid(job);
    if (const auto starter = requestPid(request, sub, "starter_pid")) {
        const auto identity = read_process_identity(
            env.proc_root, read_boot_id(env.proc_root), *starter);
        if (identity.complete()) {
            attr.starter_pid = *starter;
            // The member's parent is the usual corroboration, but docker
            // universe and similar layouts legitimately break it (the job
            // process is parented outside the starter tree). Explicit
            // evidence wins; an unconfirmed relation is only reported.
            const auto parent = member_hint > 0
                                    ? read_proc_ppid(env.proc_root, member_hint)
                                    : std::nullopt;
            if (member_hint > 0 && (!parent || *parent != *starter))
                spdlog::warn("CondorSchedulerAdapter: explicit starter pid {} is not the "
                             "parent of member pid {}; keeping the explicit evidence",
                             *starter, member_hint);
        } else {
            spdlog::warn("CondorSchedulerAdapter: explicit starter pid {} is not a "
                         "readable live process; ignoring it", *starter);
        }
    }
    // The scope proof needs the supervisor: the validated explicit starter
    // when the request carried one, otherwise the member's own starter parent.
    // A cgroup is only accepted when it reads as this execution's scope — the
    // same rule the discovery reader and the derivation apply.
    pid_t supervisor = attr.starter_pid;
    if (supervisor <= 0 && member_hint > 0) {
        if (const auto manager = managerOfPid(member_hint)) supervisor = manager->pid;
    }
    const auto job_scoped = [&](const std::string& candidate, pid_t scope_owner) {
        if (scope_owner <= 0) return false;
        return read_job_membership(env.proc_root, env.cgroup_root, scope_owner, candidate)
            .authoritative;
    };

    if (const auto explicit_path = requestString(request, sub, "slots_cgroup_path")) {
        if (!cgroup_path_within(*explicit_path, env.cgroup_root)) {
            spdlog::warn("CondorSchedulerAdapter: explicit slots_cgroup_path '{}' is "
                         "outside cgroup_root '{}'; ignoring it",
                         *explicit_path, env.cgroup_root);
        } else if (!job_scoped(*explicit_path, supervisor)) {
            // Explicit evidence wins over derivation, never over the scope
            // proof: an unproven path could map the shared daemon tree (or a
            // dead entry) into cgroup2job and silently mis-attribute every
            // process of the node.
            spdlog::warn("CondorSchedulerAdapter: explicit slots_cgroup_path '{}' cannot be "
                         "proven job-scoped (supervisor {}); ignoring it",
                         *explicit_path, supervisor);
        } else {
            attr.slots_cgroup_path = *explicit_path;
            // A disagreement with the member's own cgroup is still worth
            // saying out loud once, at registration.
            const auto member_path =
                member_hint > 0
                    ? read_proc_cgroup_path(env.proc_root, member_hint, env.cgroup_root)
                    : std::string{};
            if (!member_path.empty() && member_path != attr.slots_cgroup_path)
                spdlog::warn("CondorSchedulerAdapter: explicit slots_cgroup_path '{}' "
                             "differs from member pid {}'s cgroup '{}'; keeping the "
                             "explicit path",
                             attr.slots_cgroup_path, member_hint, member_path);
        }
    }
    if (attr.slots_cgroup_path.empty()) {
        // A slot name is only a layout hint: resolve it under the configured
        // slot subtree and keep it solely when that directory really exists
        // and passes the same scope proof.
        if (const auto slot = requestString(request, sub, "slot")) {
            const bool component = slot->find('/') == std::string::npos &&
                                   slot->find('\\') == std::string::npos;
            if (!component || impl_->starter_cgroup_subtree.empty()) {
                spdlog::warn("CondorSchedulerAdapter: slot '{}' is not usable as a slot "
                             "cgroup component; ignoring it", *slot);
            } else {
                const auto candidate =
                    (std::filesystem::path(env.cgroup_root) / impl_->starter_cgroup_subtree /
                     *slot)
                        .lexically_normal()
                        .string();
                std::string error;
                if (!ensure_readable_directory(candidate, error)) {
                    spdlog::warn("CondorSchedulerAdapter: slot '{}' resolves to '{}', which "
                                 "is not a readable cgroup directory; ignoring it",
                                 *slot, candidate);
                } else if (!job_scoped(candidate, supervisor)) {
                    spdlog::warn("CondorSchedulerAdapter: slot '{}' resolves to '{}', which "
                                 "cannot be proven job-scoped (supervisor {}); ignoring it",
                                 *slot, candidate, supervisor);
                } else {
                    attr.slots_cgroup_path = candidate;
                }
            }
        }
    }
    if (const auto ad_path = requestString(request, sub, "job_ad_path")) {
        if (std::filesystem::path(*ad_path).is_absolute())
            attr.job_ad_path = *ad_path;
        else
            spdlog::warn("CondorSchedulerAdapter: explicit job_ad_path '{}' is not "
                         "absolute; ignoring it", *ad_path);
    }

    job.sub_attr = attr;
    job.subtype = JobSubType::Condor;
    job.subtype_name = name();
    job.JobNativeID = fmt::format("{}.{}", cluster_id, proc_id);
    projectFacts(job);
}

void CondorSchedulerAdapter::deriveExecutionFacts(Job& job) const
{
    auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    if (attr == nullptr) return;
    // A manual registration names member pids, never the supervisor. Both
    // facts are derived from that evidence and only when missing: a request
    // value or a discovery watcher fact is never re-derived.
    //
    // Docker universe note: its job processes are parented outside the starter
    // tree (host pids from `docker top`), so no supervisor is resolvable and the
    // cgroup stays unrecorded by design; that layout needs the caller to supply
    // explicit evidence (`starter_pid` plus `slots_cgroup_path`/`slot`).
    const pid_t member_hint = firstPid(job);
    if (member_hint <= 0) return;

    // The anchor has to be a member whose parent is a real condor_starter. A
    // manual request carries the members in the caller's order (the trigger
    // sends a sorted pid list, so the first entry can be a grandchild), and
    // anchoring on a fixed position made the derivation give up even when the
    // record already named a resolvable member.
    pid_t anchor = member_hint;
    if (attr->starter_pid <= 0) {
        // A manager the refresh already resolved from the same member is the
        // validated supervisor; otherwise resolve it from the members' parents.
        const ProcessIdentity& resolved = manager_of(job);
        if (resolved.complete()) {
            attr->starter_pid = resolved.pid;
        } else {
            for (const auto& candidate : pid_identities_of(job)) {
                if (candidate.pid <= 0) continue;
                const auto manager = managerOfPid(candidate.pid);
                if (!manager) continue;
                attr->starter_pid = manager->pid;
                anchor = candidate.pid;
                break;
            }
        }
        if (attr->starter_pid > 0)
            spdlog::debug("CondorSchedulerAdapter: derived starter pid {} for job '{}' "
                          "(JobID {}) from member pid {}",
                          attr->starter_pid, job.JobNativeID, job.JobID, anchor);
    }

    if (attr->slots_cgroup_path.empty()) {
        const bool first_report = job.execution.read_order == 0;
        const DiscoveryEnvironment env = impl_->watcher.discovery_environment();
        const auto derived =
            read_proc_cgroup_path(env.proc_root, anchor, env.cgroup_root);
        if (derived.empty()) {
            // No derivable scope is a degraded read, not a failure: the record
            // stays valid with the pids it knows, and the refresh retries.
            report_derivation_issue(
                first_report,
                fmt::format("no execution cgroup derivable for job '{}' (JobID {}, "
                            "member pid {}); the execution scope stays unexpanded and "
                            "kernel attribution gets no cgroup mapping",
                            job.JobNativeID, job.JobID, anchor));
            return;
        }
        // The derived cgroup must PROVE it is the job's own scope: cgroup.procs
        // has to be readable and must not list the supervisor's parent (the
        // shared daemon tree). An unreadable cgroup is no proof, and a node
        // without per-job cgroups would otherwise map every daemon and every
        // other job to this one (the discovery reader applies the same rule).
        if (attr->starter_pid <= 0) {
            report_derivation_issue(
                first_report,
                fmt::format("member pid {} has no resolvable supervisor, so cgroup "
                            "'{}' cannot be proven job-scoped; not recording it for job "
                            "'{}' (JobID {})",
                            anchor, derived, job.JobNativeID, job.JobID));
            return;
        }
        if (!read_job_membership(env.proc_root, env.cgroup_root, attr->starter_pid, derived)
                 .authoritative) {
            report_derivation_issue(
                first_report,
                fmt::format("cgroup '{}' of member pid {} is unreadable or shared with "
                            "the daemon tree (supervisor {}); not recording it for job "
                            "'{}' (JobID {})",
                            derived, anchor, attr->starter_pid, job.JobNativeID,
                            job.JobID));
            return;
        }
        attr->slots_cgroup_path = derived;
        spdlog::debug("CondorSchedulerAdapter: derived execution cgroup '{}' for job "
                      "'{}' (JobID {}) from member pid {}",
                      derived, job.JobNativeID, job.JobID, anchor);
    }
}

bool CondorSchedulerAdapter::completeRefreshedFacts(Job& target,
                                                    const Job& refreshed) const
{
    // A refresh completes facts the RECORD never carried; it never rewrites a
    // value, the identity or the namespace the record was created with. Both
    // facts here are execution-scope facts the adapter derives, so filling
    // them lets the existing kernel reconcile pick up the cgroup mapping
    // without re-registering the job (docker universe jobs whose supervisor is
    // not a starter tree keep them empty by design; see deriveExecutionFacts).
    auto* to = std::get_if<CondorJobAttr>(&target.sub_attr);
    const auto* from = std::get_if<CondorJobAttr>(&refreshed.sub_attr);
    if (to == nullptr || from == nullptr) return false;
    bool completed = false;
    if (to->slots_cgroup_path.empty() && !from->slots_cgroup_path.empty()) {
        to->slots_cgroup_path = from->slots_cgroup_path;
        completed = true;
    }
    if (to->starter_pid <= 0 && from->starter_pid > 0) {
        to->starter_pid = from->starter_pid;
        completed = true;
    }
    if (completed)
        spdlog::info("CondorSchedulerAdapter: completed execution facts for job '{}' "
                     "(JobID {}): cgroup='{}' starter_pid={}",
                     target.JobNativeID, target.JobID, to->slots_cgroup_path,
                     to->starter_pid);
    return completed;
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

    // The manager IS the execution identity. A record that never carried a
    // complete one (a manual row, an interrupted discovery) recovers it from
    // any member pid it still knows.
    const pid_t member_hint = firstPid(job);
    if (!execution.manager.complete() && member_hint > 0) {
        if (const auto manager = managerOfPid(member_hint)) execution.manager = *manager;
    }

    // A record the discovery watcher never touched (manual registration, a row
    // restored from before the fix) carries no slot cgroup, so the membership
    // read below would only ever see the recorded pids. Derive the missing
    // facts from the member's own /proc/<pid>/cgroup first — the 0.3.8
    // fallback. A recorded path is the fact and is never re-derived.
    if (member_hint > 0) {
        const auto* recorded = std::get_if<CondorJobAttr>(&job.sub_attr);
        if (recorded != nullptr && recorded->slots_cgroup_path.empty())
            deriveExecutionFacts(job);
    }

    // Targeted refresh through the watcher: this one execution's job ad and
    // membership, the same reader an exec hint uses (no new order is invented
    // here — the observation's own, newer read_order is adopted).
    if (execution.manager.complete() && member_hint > 0) {
        if (auto observed =
                impl_->watcher.observeForHint(member_hint, execution.manager.pid,
                    !job.group_probed && job.group_probe_attempts < ClassAdValue::kMaxGroupProbeAttempts)) {
            // Do not backfill from a different scheduler identity, even on the
            // same PID hint. Other scheduler metadata stays frozen.
            if (const auto key = keyOf(*observed); key && key == keyOf(job)) {
                if (!observed->group_probed && observed->group_probe_attempts != 0 && !job.group_probed)
                    observed->group_probe_attempts += job.group_probe_attempts;
                fill_missing_groups(job, *observed);
            }
            execution.manager = observed->execution.manager;
            // The record's job-scoped cgroup is the membership truth. The ad
            // read starts at the starter, which on nodes with per-slot cgroups
            // lives in the daemon cgroup and therefore only sees its direct
            // children; letting that observation replace the membership kept
            // the whole execution scope out of the record.
            const auto* recorded_attr = std::get_if<CondorJobAttr>(&job.sub_attr);
            if (recorded_attr != nullptr && !recorded_attr->slots_cgroup_path.empty()) {
                execution.read_order = observed->execution.read_order;
                return refreshPidIdentities(job, env.proc_root, env.cgroup_root,
                                            recorded_attr->slots_cgroup_path,
                                            process_probe, cgroup_probe);
            }
            execution.pid_identities = observed->execution.pid_identities;
            execution.pid_identities_authoritative =
                observed->execution.pid_identities_authoritative;
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
    return refreshPidIdentities(
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
    return {selector.clusterName, selector.clusterTag};
}

bool CondorSchedulerAdapter::adoptNamespaceAliases(Job& job,
                                                   const std::string& clusterName,
                                                   const std::string& clusterTag) const
{
    auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
    if (attr == nullptr) return true;  // no typed facts: nothing contradicts
    if (!clusterName.empty() && !attr->collector_host.empty()
            && clusterName != attr->collector_host) return false;
    if (!clusterTag.empty() && !attr->scheduler_name.empty()
            && clusterTag != attr->scheduler_name) return false;
    if (attr->collector_host.empty()) attr->collector_host = clusterName;
    if (attr->scheduler_name.empty()) attr->scheduler_name = clusterTag;
    return true;
}

void CondorSchedulerAdapter::renderNamespace(Job& job) const
{
    if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
        job.clusterName = attr->collector_host;
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
    auto parsed = parse_condor_native(selector.job_native_id);
    if (!parsed) return std::nullopt;
    CanonicalParts parts;
    parts.cluster_namespace = selector.clusterName;
    if (!selector.clusterTag.empty())
        parts.cluster_namespace += "@" + selector.clusterTag;
    parts.canonical_native = parsed->cluster + "." + parsed->proc;
    return parts;
})

REGISTER_SCHEDULER_ADAPTER("condor", CondorSchedulerAdapter)
