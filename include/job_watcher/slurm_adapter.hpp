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
// SlurmSchedulerAdapter — the Slurm inventory adapter
// (doc/job_inventory_v2_architecture.md §2.2). Owns the discovery watcher and
// every Slurm-specific fact encoding; registered under the name "slurm".
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
        options.registration_batch_size = settings.registration_batch_size;
        if (!settings.cluster_name.empty())
            options.discovery.cluster_name = settings.cluster_name;
        return options;
    }

    Job encodeFacts(const Observation& observation) const override
    {
        Job facts;
        facts.jobtype = JobType::Job;
        facts.subtype = JobSubType::Slurm;

        SlurmJobAttr attributes{};
        attributes.auto_update_child = false;
        attributes.stepd_pid = observation.generation.manager_pid;
        attributes.cluster_name = observation.selector.cluster_name;
        attributes.cgroup_path = observation.cgroup_path;
        attributes.user = observation.owner;
        attributes.job_name = observation.job_name;
        attributes.partition = observation.partition;
        attributes.step_id = slurm_step_value(observation.selector.step_id).value_or(0);
        try {
            attributes.job_id = std::stoull(observation.selector.native_job_id);
        } catch (const std::exception&) {
            // Canonicalization already rejected non-numeric Slurm ids; leaving
            // the field at 0 keeps the missing fact visible.
        }
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
        selector.cluster_name = job.cluster_name;
        selector.native_job_id = job.NativeJobID;
        if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
            if (selector.cluster_name.empty()) selector.cluster_name = attr->cluster_name;
            if (selector.native_job_id.empty()) {
                selector.native_job_id = std::to_string(attr->job_id);
            }
            if (attr->step_id != 0) {
                if (const auto name = slurm_step_name(attr->step_id)) {
                    selector.step_id = *name;
                } else {
                    selector.step_id = std::to_string(attr->step_id);
                }
            }
        }
        return selector;
    }

    // Delegates to the same derivation encodeFacts uses (moved from the
    // store's apply_identity_to_facts; idempotent for encoded facts).
    void applyIdentityToFacts(Job& job, const NativeSelector& selector,
                              const CanonicalIdentity& identity) const override
    {
        apply_identity(job, selector, identity);
    }

    std::string cgroupPathOf(const Job& job) const override
    {
        const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr);
        return attr ? attr->cgroup_path : std::string{};
    }

private:
    static void apply_identity(Job& job, const NativeSelector& selector,
                               const CanonicalIdentity& identity)
    {
        job.NativeJobID = identity.native_job_id;
        if (!selector.cluster_name.empty()) job.cluster_name = selector.cluster_name;
        if (!selector.cluster_tag.empty()) job.clusterTag = selector.cluster_tag;
        if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys) {
            job.jobtype = JobType::Job;
        }
        job.subtype = JobSubType::Slurm;
        if (!std::holds_alternative<SlurmJobAttr>(job.sub_attr)) {
            job.sub_attr = SlurmJobAttr{};
        }
        auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
        if (attr.cluster_name.empty()) attr.cluster_name = selector.cluster_name;
        if (!selector.step_id.empty()) {
            if (const auto numeric = slurm_step_value(selector.step_id)) {
                attr.step_id = *numeric;
            }
        }
    }

    std::string name_{"slurm"};
    SlurmJobWatcher watcher_;
};

REGISTER_SCHEDULER_ADAPTER("slurm", SlurmSchedulerAdapter)
