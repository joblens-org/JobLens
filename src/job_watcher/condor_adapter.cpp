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
#include "common/common_job.hpp"
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
    options.scan_interval_ms = 0;
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

NativeSelector CondorSchedulerAdapter::legacySelector(Job& job) const
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
        if (selector.native_job_id.empty())
            selector.native_job_id =
                fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
    }
    if (selector.cluster_name.empty()) selector.cluster_name = job.cluster_name;
    if (selector.cluster_tag.empty()) selector.cluster_tag = job.clusterTag;
    if (selector.native_job_id.empty()) selector.native_job_id = job.NativeJobID;

    // The selector folds onto the record: its derived fields backfill the
    // Job's empty ones (non-empty facts win — the adapter derived FROM them).
    job.subtype = JobSubType::Condor;
    job.subtype_name = name();
    if (job.cluster_name.empty()) job.cluster_name = selector.cluster_name;
    if (job.clusterTag.empty()) job.clusterTag = selector.cluster_tag;
    if (job.NativeJobID.empty()) job.NativeJobID = selector.native_job_id;
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

void CondorSchedulerAdapter::applyIdentityToFacts(Job& job,
                                                  const NativeSelector& selector,
                                                  const JobIdentity& identity) const
{
    apply_identity_projection(job, selector, identity);
    job.subtype = JobSubType::Condor;
    if (!std::holds_alternative<CondorJobAttr>(job.sub_attr))
        job.sub_attr = CondorJobAttr{};
    auto& attr = std::get<CondorJobAttr>(job.sub_attr);

    // The typed facts are the single namespace source; the resolved
    // selector's namespace folds into them (facts win), then the common
    // fields render from the facts.
    if (attr.collector_host.empty() && !selector.cluster_name.empty())
        attr.collector_host = selector.cluster_name;
    if (attr.scheduler_name.empty() && !selector.cluster_tag.empty())
        attr.scheduler_name = selector.cluster_tag;
    renderNamespace(job);

    const auto dot = identity.canonical_native.find('.');
    if (dot != std::string::npos) {
        try {
            attr.cluster_id = std::stoull(identity.canonical_native.substr(0, dot));
            attr.proc_id = std::stoull(identity.canonical_native.substr(dot + 1));
        } catch (const std::exception&) {
            // Canonical validation already guaranteed decimal parts.
        }
    }
    projectFacts(job);
}

NativeSelector CondorSchedulerAdapter::registrationSelector(Job& job) const
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

void CondorSchedulerAdapter::refreshLegacyFacts(Job& job) const
{
    CondorJob::update_job_info(job);
    if (!CondorJob::update_job_pids(job))
        CommonJob::update_job_child_process(job);
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
