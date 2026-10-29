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
// CondorSchedulerAdapter — the HTCondor inventory adapter
// (doc/job_inventory_v2_architecture.md §2.2). Owns the discovery watcher and
// every Condor-specific fact encoding; registered under the name "condor".
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
        if (!settings.cluster_name.empty())
            options.discovery.cluster_name = settings.cluster_name;
        return options;
    }

    // Typed facts for one observation, identity fields included. JobID,
    // JobPIDs and CollectorNames stay empty: they are store/intent owned.
    Job encodeFacts(const Observation& observation) const override
    {
        Job facts;
        facts.jobtype = JobType::Job;
        facts.subtype = JobSubType::Condor;

        CondorJobAttr attributes{};
        attributes.auto_update_child = false;
        attributes.starter_pid = observation.generation.manager_pid;
        attributes.scheduler_host = observation.scheduler_host;
        attributes.scheduler_name = observation.selector.cluster_tag;
        attributes.slots_cgroup_path = observation.cgroup_path;
        attributes.owner = observation.owner;
        attributes.collector_host = observation.selector.cluster_name;
        facts.sub_attr = attributes;

        if (const auto identity = canonicalize(observation.selector)) {
            apply_identity(facts, observation.selector, *identity);
        }
        return facts;
    }

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
        selector.cluster_name = job.cluster_name;
        selector.cluster_tag = job.clusterTag;
        selector.native_job_id = job.NativeJobID;
        if (selector.native_job_id.empty()) {
            if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
                selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
            }
        }
        return selector;
    }

private:
    // The native string and namespace are authoritative; scheduler numeric
    // fields derive from the canonical form so they can never disagree with
    // the identity index.
    static void apply_identity(Job& job, const NativeSelector& selector,
                               const CanonicalIdentity& identity)
    {
        job.NativeJobID = identity.native_job_id;
        if (!selector.cluster_name.empty()) job.cluster_name = selector.cluster_name;
        if (!selector.cluster_tag.empty()) job.clusterTag = selector.cluster_tag;
        if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys) {
            job.jobtype = JobType::Job;
        }
        job.subtype = JobSubType::Condor;
        if (!std::holds_alternative<CondorJobAttr>(job.sub_attr)) {
            job.sub_attr = CondorJobAttr{};
        }
        auto& attr = std::get<CondorJobAttr>(job.sub_attr);
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

    std::string name_{"condor"};
    CondorJobWatcher watcher_;
};

REGISTER_SCHEDULER_ADAPTER("condor", CondorSchedulerAdapter)
