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
// SlurmSchedulerAdapter — the Slurm inventory adapter. Owns the discovery
// watcher and every Slurm-specific fact encoding; registered under the name
// "slurm".
#pragma once

#include "core/scheduler_adapter.hpp"
#include "job_watcher/slurm_job_watcher.hpp"

class SlurmSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit SlurmSchedulerAdapter(const InventoryTypeSettings& settings)
        : watcher_(discoveryOptions(settings)) {}

    const std::string& name() const override { return name_; }
    std::string configSection() const override { return "slurm_job_watcher"; }
    IDiscoverySource& source() override { return watcher_; }
    AdapterTraits traits() const override
    {
        AdapterTraits traits;
        traits.cgroup_scoped_membership = true;
        traits.kernel_attribution = true;
        return traits;
    }

    DiscoveryOptions discoveryOptions(const InventoryTypeSettings& settings) const override
    {
        DiscoveryOptions options;
        options.enable_event_source = true;
        options.scan_interval_ms = 0;
        if (!settings.cluster_name.empty())
            options.discovery.cluster_name = settings.cluster_name;
        return options;
    }

    // Typed facts for a discovered Job are filled by the watcher itself where
    // it parses its sources (no encodeFacts detour through neutral fields).
    // This adapter only decodes legacy rows and derives identity fields.

    Job decodeFacts(const nlohmann::json& row) const override
    {
        Job job;
        job.jobtype = static_cast<JobType>(row.value("jobtype", 0));
        job.subtype = static_cast<JobSubType>(row.value("subtype", 0));
        job.NativeJobID = row.value("native_job_id", "");
        job.cluster_name = row.value("cluster_name", "");
        job.clusterTag = row.value("clusterTag", "");
        if (job.subtype == JobSubType::Slurm) {
            job.sub_attr = row.contains("sub_attr")
                               ? row["sub_attr"].get<SlurmJobAttr>()
                               : SlurmJobAttr{};
        }
        return job;
    }

    NativeSelector legacySelector(const Job& job) const override
    {
        NativeSelector selector;
        if (job.subtype != JobSubType::Slurm) return selector;
        selector.adapter = name_;
        // The typed facts are the namespace source; the common field is
        // their render (kept as a fallback for facts built before one existed).
        if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
            selector.cluster_name = attr->cluster_name;
            if (selector.native_job_id.empty())
                selector.native_job_id = std::to_string(attr->job_id);
            if (attr->step_id != 0) {
                if (const auto name = slurm_step_name(attr->step_id)) {
                    selector.step_id = *name;
                } else {
                    selector.step_id = std::to_string(attr->step_id);
                }
            }
        }
        if (selector.cluster_name.empty()) selector.cluster_name = job.cluster_name;
        if (selector.native_job_id.empty()) selector.native_job_id = job.NativeJobID;
        return selector;
    }

    // Delegates to the shared identity derivation (moved from the store's
    // apply_identity_to_facts; idempotent for watcher-filled facts).
    void applyIdentityToFacts(Job& job, const NativeSelector& selector,
                              const JobIdentity& identity) const override
    {
        apply_identity(job, selector, identity);
    }

    // ---- structured namespace: the cluster name only ----------------------
    NamespaceFacts namespaceFacts(const Job& job) const override
    {
        const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
        return {attr ? attr->cluster_name : std::string{}};
    }
    NamespaceFacts selectorNamespaceFacts(const NativeSelector& selector) const override
    {
        return {selector.cluster_name};
    }
    bool adoptNamespaceAliases(Job& job, const std::string& cluster_name,
                               const std::string& cluster_tag) const override
    {
        (void)cluster_tag;  // Slurm has no schedd-tag namespace component
        auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
        if (attr == nullptr) return true;
        if (!cluster_name.empty() && !attr->cluster_name.empty()
                && cluster_name != attr->cluster_name) return false;
        if (attr->cluster_name.empty()) attr->cluster_name = cluster_name;
        return true;
    }
    void renderNamespace(Job& job) const override
    {
        if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
            job.cluster_name = attr->cluster_name;
            // clusterTag is not a Slurm namespace component and stays as-is:
            // discovery may carry a display value there.
        }
    }

    std::string cgroupPathOf(const Job& job) const override
    {
        const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
        return attr ? attr->cgroup_path : std::string{};
    }

private:
    static void apply_identity(Job& job, const NativeSelector& selector,
                               const JobIdentity& identity)
    {
        job.NativeJobID = identity.native_job_id;
        if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys) {
            job.jobtype = JobType::Job;
        }
        job.subtype = JobSubType::Slurm;
        if (!std::holds_alternative<SlurmJobAttr>(job.sub_attr)) {
            job.sub_attr = SlurmJobAttr{};
        }
        auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
        // The typed facts are the single namespace source; fold the resolved
        // selector's cluster into them (facts win), then render the common
        // field from the facts (clusterTag is not a Slurm component).
        if (attr.cluster_name.empty() && !selector.cluster_name.empty())
            attr.cluster_name = selector.cluster_name;
        job.cluster_name = attr.cluster_name;
        if (!selector.step_id.empty()) {
            if (const auto numeric = slurm_step_value(selector.step_id)) {
                attr.step_id = *numeric;
            }
        }
    }

    std::string name_{"slurm"};
    SlurmJobWatcher watcher_;
};

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
