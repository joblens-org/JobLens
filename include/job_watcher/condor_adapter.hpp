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
// watcher and every Condor-specific fact encoding; registered under the name
// "condor".
#pragma once

#include "core/scheduler_adapter.hpp"
#include "job_watcher/condor_job_watcher.hpp"

class CondorSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit CondorSchedulerAdapter(const InventoryTypeSettings& settings)
        : watcher_(discoveryOptions(settings)) {}

    const std::string& name() const override { return name_; }
    std::string configSection() const override { return "condor_job_watcher"; }
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
        // Verified on the integration node (192.168.82.26): the startd keeps
        // one directory per slot under this subtree, each slot's cgroup.procs
        // listing its processes (starter included).
        options.starter_cgroup_subtree = "system.slice/htcondor";
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
        if (job.subtype == JobSubType::Condor) {
            job.sub_attr = row.contains("sub_attr")
                               ? row["sub_attr"].get<CondorJobAttr>()
                               : CondorJobAttr{};
        }
        return job;
    }

    NativeSelector legacySelector(const Job& job) const override
    {
        NativeSelector selector;
        if (job.subtype != JobSubType::Condor) return selector;
        selector.adapter = name_;
        // The typed facts are the namespace source; the common fields are
        // their render (kept as a fallback for facts built before one existed).
        if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
            selector.cluster_name = attr->collector_host;
            selector.cluster_tag = attr->scheduler_name;
            if (selector.native_job_id.empty())
                selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
        }
        if (selector.cluster_name.empty()) selector.cluster_name = job.cluster_name;
        if (selector.cluster_tag.empty()) selector.cluster_tag = job.clusterTag;
        if (selector.native_job_id.empty()) selector.native_job_id = job.NativeJobID;
        return selector;
    }

    // The native string and namespace are authoritative; scheduler numeric
    // fields derive from the canonical form so they can never disagree with
    // the identity index. Moved from the store's apply_identity_to_facts.
    void applyIdentityToFacts(Job& job, const NativeSelector& selector,
                              const JobIdentity& identity) const override
    {
        job.subtype = JobSubType::Condor;
        if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys) {
            job.jobtype = JobType::Job;
        }
        if (!std::holds_alternative<CondorJobAttr>(job.sub_attr)) {
            job.sub_attr = CondorJobAttr{};
        }
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
    }

    std::string cgroupPathOf(const Job& job) const override
    {
        const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
        return attr ? attr->slots_cgroup_path : std::string{};
    }

    // ---- structured namespace: pool routing label + schedd name -----------
    NamespaceFacts namespaceFacts(const Job& job) const override
    {
        const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr);
        return {attr ? attr->collector_host : std::string{},
                attr ? attr->scheduler_name : std::string{}};
    }
    NamespaceFacts selectorNamespaceFacts(const NativeSelector& selector) const override
    {
        return {selector.cluster_name, selector.cluster_tag};
    }
    bool adoptNamespaceAliases(Job& job, const std::string& cluster_name,
                               const std::string& cluster_tag) const override
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
    void renderNamespace(Job& job) const override
    {
        if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
            job.cluster_name = attr->collector_host;
            job.clusterTag = attr->scheduler_name;
        }
    }

private:
    std::string name_{"condor"};
    CondorJobWatcher watcher_;
};

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
