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
#include "core/job_identity.hpp"

#include <cstdint>
#include <utility>
#include <vector>

inline Job discovered_job(NativeSelector selector, ExecutionGeneration generation,
                          std::vector<ProcessIdentity> members, bool members_complete,
                          std::int64_t read_at_us = 0)
{
    Job job;
    job.subtype_name = selector.adapter;
    job.NativeJobID = selector.native_job_id;
    job.cluster_name = selector.cluster_name;
    job.clusterTag = selector.cluster_tag;
    job.jobtype = JobType::Job;
    JobExecution execution;
    execution.generation = std::move(generation);
    execution.members = std::move(members);
    execution.members_authoritative = members_complete;
    if (!selector.step_id.empty()) execution.step_id = selector.step_id;
    execution.read_at_us = read_at_us;
    job.execution = std::move(execution);
    return job;
}
