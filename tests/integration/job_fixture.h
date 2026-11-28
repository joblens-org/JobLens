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
// Shared test fixture for the registration unit: builds the Job a discovery
// source hands over: facts plus ONE execution evidence group.
#pragma once

#include "core/collector_type.h"
#include "job_watcher/condor_identity.hpp"
#include "job_watcher/slurm_step.hpp"
#include "core/job_identity.hpp"

#include <cstdint>
#include <utility>
#include <vector>

inline Job discovered_job(NativeSelector selector, ProcessIdentity manager,
                          std::vector<ProcessIdentity> pid_identities,
                          bool pid_identities_complete,
                          std::int64_t read_at_us = 0)
{
    Job job;
    job.subtype_name = selector.adapter;
    job.JobNativeID = selector.job_native_id;
    // A discovery source hands over typed facts (the single namespace
    // source) plus their rendered common fields, exactly like the watchers.
    if (selector.adapter == "condor") {
        job.subtype = JobSubType::Condor;
        auto attr = CondorJobAttr{};
        attr.collector_host = selector.clusterName;
        attr.scheduler_name = selector.clusterTag;
        if (const auto parsed = CondorJob::parse_native_id(selector.job_native_id)) {
            attr.cluster_id = std::stoull(parsed->cluster);
            attr.proc_id = std::stoull(parsed->proc);
        }
        job.sub_attr = std::move(attr);
    } else if (selector.adapter == "slurm") {
        job.subtype = JobSubType::Slurm;
        auto attr = SlurmJobAttr{};
        attr.clusterName = selector.clusterName;
        try {
            attr.job_id = std::stoull(selector.job_native_id);
        } catch (const std::exception&) {
            attr.job_id = 0;
        }
        if (!selector.step_id.empty()) {
            if (const auto numeric = SlurmJob::step_value(selector.step_id)) attr.step_id = *numeric;
        }
        job.sub_attr = std::move(attr);
    }
    job.clusterName = selector.clusterName;
    job.clusterTag = selector.clusterTag;
    job.jobtype = JobType::Job;
    job.execution.manager = std::move(manager);
    job.execution.pid_identities = std::move(pid_identities);
    job.execution.pid_identities_authoritative = pid_identities_complete;
    (void)read_at_us;
    return job;
}
