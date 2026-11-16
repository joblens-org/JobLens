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

#include <fmt/format.h>

#include "core/collector_type.h"
#include "job_watcher/slurm_job_watcher.hpp"
#include "common/common_job.hpp"
#include "common/slurm_job.hpp"

struct SlurmSchedulerAdapter::Impl {
    explicit Impl(DiscoveryOptions options) : watcher(std::move(options)) {}
    SlurmJobWatcher watcher;
};

SlurmSchedulerAdapter::SlurmSchedulerAdapter(const InventoryTypeSettings& settings)
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
    const InventoryTypeSettings& settings) const
{
    DiscoveryOptions options;
    options.enable_event_source = true;
    options.scan_interval_ms = 0;
    if (!settings.cluster_name.empty())
        options.discovery.cluster_name = settings.cluster_name;
    return options;
}

Job SlurmSchedulerAdapter::decodeFacts(const nlohmann::json& row) const
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
        selector.cluster_name = attr->cluster_name;
        if (attr->step_id != 0) {
            if (const auto name = slurm_step_name(attr->step_id)) {
                selector.step_id = *name;
            } else {
                selector.step_id = std::to_string(attr->step_id);
            }
        }
    }
    if (selector.cluster_name.empty()) selector.cluster_name = job.cluster_name;
    // The original native string wins over the typed number: a zero-valued
    // field must not shadow the render.
    if (selector.native_job_id.empty()) selector.native_job_id = job.NativeJobID;
    if (selector.native_job_id.empty() && attr != nullptr)
        selector.native_job_id = std::to_string(attr->job_id);

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
    if (job.NativeJobID.empty()) job.NativeJobID = std::to_string(attr->job_id);
    if (job.cluster_name.empty()) job.cluster_name = attr->cluster_name;
    // clusterTag is not a Slurm namespace component and stays as-is:
    // discovery may carry a display value there.
}

NativeSelector SlurmSchedulerAdapter::registrationSelector(const Job& job) const
{
    NativeSelector selector = legacySelector(job);
    if (selector.cluster_name.empty())
        selector.cluster_name = SlurmJob::get_cluster_name();
    if (selector.cluster_name.empty() || selector.native_job_id.empty()) return {};
    return selector;
}

void SlurmSchedulerAdapter::applyRequestFacts(Job& job,
                                              const nlohmann::json& request) const
{
    const auto sub = request.value("sub_attr", nlohmann::json::object());
    const auto job_id = sub.value("job_id", nlohmann::json(0u)).get<size_t>();
    const auto step_id = sub.value("step_id", nlohmann::json(0u)).get<uint32_t>();

    SlurmJobAttr attr;
    attr.auto_update_child = request.value("auto_update_child", true);
    attr.job_id = job_id;
    attr.step_id = step_id;
    attr.cluster_name = sub.value("cluster_name", "");
    attr.user = sub.value("user", "");

    job.sub_attr = attr;
    job.subtype = JobSubType::Slurm;
    job.subtype_name = name();
    job.NativeJobID = std::to_string(job_id);
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

void SlurmSchedulerAdapter::refreshLegacyFacts(Job& job) const
{
    SlurmJob::update_job_info(job);
    CommonJob::update_job_child_process(job);
}

std::string SlurmSchedulerAdapter::cgroupPathOf(const Job& job) const
{
    const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    return attr ? attr->cgroup_path : std::string{};
}

NamespaceFacts SlurmSchedulerAdapter::namespaceFacts(const Job& job) const
{
    const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    return {attr ? attr->cluster_name : std::string{}};
}

NamespaceFacts SlurmSchedulerAdapter::selectorNamespaceFacts(
    const NativeSelector& selector) const
{
    return {selector.cluster_name};
}

bool SlurmSchedulerAdapter::adoptNamespaceAliases(Job& job,
                                                  const std::string& cluster_name,
                                                  const std::string& cluster_tag) const
{
    (void)cluster_tag;  // Slurm has no schedd-tag namespace component
    auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
    if (attr == nullptr) return true;
    if (!cluster_name.empty() && !attr->cluster_name.empty()
            && cluster_name != attr->cluster_name) return false;
    if (attr->cluster_name.empty()) attr->cluster_name = cluster_name;
    return true;
}

void SlurmSchedulerAdapter::renderNamespace(Job& job) const
{
    if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
        job.cluster_name = attr->cluster_name;
        // clusterTag is not a Slurm namespace component and stays as-is:
        // discovery may carry a display value there.
    }
}

// The adapter owns its native-id rule, registered alongside it: Slurm
// native ids are the logical job number without a step; a step refines the
// identity (normalized, special names preserved).
REGISTER_NATIVE_ID_RULE("slurm", [](const NativeSelector& selector)
    -> std::optional<CanonicalParts> {
    auto job_number = strip_leading_zeros(selector.native_job_id);
    if (!job_number) return std::nullopt;
    CanonicalParts parts;
    if (!selector.step_id.empty()) {
        auto step = normalize_slurm_step_id(selector.step_id);
        if (!step) return std::nullopt;
        parts.step_id = *step;
    }
    parts.cluster_namespace = selector.cluster_name;
    parts.canonical_native = *job_number;
    return parts;
})

REGISTER_SCHEDULER_ADAPTER("slurm", SlurmSchedulerAdapter)
