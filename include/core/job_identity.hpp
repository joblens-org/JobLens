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
// Identity foundation for the job-inventory/collection decoupling
// (doc/job_inventory_collection_decoupling_plan.md section 4).
//
// Layers kept strictly apart:
//   * internal storage key  - uint64 JobID allocated by this Agent DB,
//                             0 reserved for the system sentinel
//   * logical scheduler identity - NativeSelector/CanonicalIdentity here
//   * local execution identity   - ExecutionGeneration (+ ProcessIdentity)
//   * external instance handle   - opaque string derived from the Agent DB
//                             UUID plus the internal ID
//
// A native scheduler number, a caller hash or a bare PID is never an internal
// ID and never a complete identity. Incomplete identities fail explicitly and
// must stay out of committed identity indexes.
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

// ---------------------------------------------------------------------------
// Scheduler type
// ---------------------------------------------------------------------------

enum class SchedulerType {
    Unknown = 0,
    Condor,
    Slurm,
};

inline const char* to_string(SchedulerType type)
{
    switch (type) {
        case SchedulerType::Condor: return "condor";
        case SchedulerType::Slurm:  return "slurm";
        case SchedulerType::Unknown: break;
    }
    return "unknown";
}

std::optional<SchedulerType> scheduler_type_from_string(const std::string& raw);

// ---------------------------------------------------------------------------
// Internal IDs (uint64-safe parse/format)
// ---------------------------------------------------------------------------

// JobID 0 is the system sentinel; no inventory record may use it.
inline constexpr uint64_t kSystemSentinelJobId = 0;
inline bool is_reserved_internal_id(uint64_t id) { return id == kSystemSentinelJobId; }

// Strict decimal parse of an internal ID string: digits only, no sign, no
// whitespace, no overflow. New APIs carry internal IDs as decimal strings so
// JavaScript consumers keep uint64 precision.
std::optional<uint64_t> parse_internal_id(const std::string& decimal);
std::string format_internal_id(uint64_t id);

// Legacy JSON nodes may carry JobID as a JSON number or as a decimal string.
// Parsing is explicitly uint64: an unsigned integer up to UINT64_MAX or an
// integer in [0, INT64_MAX] is accepted, anything else (negative, fractional,
// oversized, non-decimal text) fails. Callers decide whether absence, an
// invalid value or the reserved sentinel is an error.
enum class LegacyIdStatus { Absent, Invalid, Reserved, Valid };
struct LegacyId {
    LegacyIdStatus status = LegacyIdStatus::Absent;
    uint64_t value = 0;
};
LegacyId parse_legacy_job_id(const nlohmann::json& node);

// ---------------------------------------------------------------------------
// Slurm step identity
// ---------------------------------------------------------------------------

// Wire step_id is a string. Numeric steps canonicalize without leading zeros
// ("07" -> "7"); the special step names keep their identity and are never
// parsed as numbers (in particular they never collapse to the numeric step
// "0"). Returns nullopt for values that cannot name a step.
std::optional<std::string> normalize_slurm_step_id(const std::string& raw);

// Explicit conversion table at the adapter boundary: scheduler-specific
// numeric constants map to and from the canonical special step names. The
// values mirror slurm/slurm.h (SLURM_BATCH_SCRIPT / SLURM_EXTERN_CONT) used by
// slurmstepd: the batch step is 0xfffffffe, the extern step is 0xfffffffd.
inline constexpr uint32_t kSlurmBatchStepValue = 0xfffffffeu;
inline constexpr uint32_t kSlurmExternStepValue = 0xfffffffeu - 1u;  // 0xfffffffd
std::optional<std::string> slurm_step_name(uint32_t numeric_step);
std::optional<uint32_t> slurm_step_value(const std::string& step_id);

// ---------------------------------------------------------------------------
// Native selector and canonical logical identity
// ---------------------------------------------------------------------------

struct NativeSelector {
    SchedulerType scheduler_type = SchedulerType::Unknown;
    std::string cluster_name;  // resolved cluster namespace (never empty)
    // Condor schedd / tag namespace; required by Condor when the cluster has
    // more than one schedd. Optional for Slurm.
    std::string cluster_tag;
    std::string native_job_id;  // original native string, preserved verbatim
    // Slurm only; empty means "the logical job, no step claim". Use
    // scope=instance selectors with a step for step-exact targeting.
    std::string step_id;
};

struct CanonicalIdentity {
    SchedulerType scheduler_type = SchedulerType::Unknown;
    std::string cluster_namespace;  // cluster_name plus tag when applicable
    std::string native_job_id;      // original native string, preserved
    std::string canonical_native;   // numeric-normalized form used for keys
    std::string step_id;            // normalized Slurm step, or empty

    // Total-order key covering type, namespace, native identity and step.
    // Equal keys mean the same logical scheduler identity.
    std::string key() const;
};

bool operator==(const CanonicalIdentity& lhs, const CanonicalIdentity& rhs);
bool operator!=(const CanonicalIdentity& lhs, const CanonicalIdentity& rhs);
bool operator<(const CanonicalIdentity& lhs, const CanonicalIdentity& rhs);

// Validates and completes a selector. Condor native ids must be
// "<ClusterId>.<ProcId>" with decimal parts; leading zeros are tolerated and
// canonicalized ("012.3" == "12.3") while the original string is preserved,
// so digit concatenation can never merge two different jobs ("12.34" versus
// "123.4"). Slurm native ids are the logical job number without a step.
// A missing cluster namespace, an unknown scheduler type or an unparsable
// native id yields nullopt: an incomplete observation, never a guess.
std::optional<CanonicalIdentity> canonicalize(const NativeSelector& selector);

// Serialization round trips preserve the original native string, the
// normalized step and every namespace field. scheduler_type serializes as its
// lowercase name; unknown values fail deserialization instead of guessing.
void to_json(nlohmann::json& out, const NativeSelector& selector);
void from_json(const nlohmann::json& in, NativeSelector& selector);
void to_json(nlohmann::json& out, const CanonicalIdentity& identity);
void from_json(const nlohmann::json& in, CanonicalIdentity& identity);

// ---------------------------------------------------------------------------
// Process and execution-generation identity
// ---------------------------------------------------------------------------

// A numeric PID alone is insufficient: the same number maps to different
// processes across forks and reboots. Equality uses the full triple.
struct ProcessIdentity {
    pid_t pid = 0;
    uint64_t start_ticks = 0;  // /proc/<pid>/stat field 22
    std::string boot_id;       // /proc/sys/kernel/random/boot_id

    bool complete() const { return pid > 0 && start_ticks > 0 && !boot_id.empty(); }
};
bool operator==(const ProcessIdentity& lhs, const ProcessIdentity& rhs);
bool operator!=(const ProcessIdentity& lhs, const ProcessIdentity& rhs);

// Evidence that two observations belong to the same local execution of one
// logical job. Neither a slot name nor a cgroup path alone proves generation
// equality (Condor slots and cgroups are reused), which is why manager
// identity is mandatory and the cgroup is only corroborating evidence.
struct ExecutionGeneration {
    std::string boot_id;
    pid_t manager_pid = 0;
    uint64_t manager_start_ticks = 0;
    std::string cgroup_identity;  // optional; empty = unavailable

    bool complete() const
    {
        return manager_pid > 0 && manager_start_ticks > 0 && !boot_id.empty();
    }
};
bool operator==(const ExecutionGeneration& lhs, const ExecutionGeneration& rhs);
bool operator!=(const ExecutionGeneration& lhs, const ExecutionGeneration& rhs);

// ---------------------------------------------------------------------------
// Opaque external instance handles
// ---------------------------------------------------------------------------

// Derives the external handle from the persisted Agent/DB identity UUID and
// the allocated internal ID. Stable for one execution's lifetime, opaque to
// callers, not globally ordered and not a scheduler ID. A recreated DB gets a
// different UUID and therefore different handles; nothing ever parses the
// handle back into parts.
std::string derive_instance_id(const std::string& db_uuid, uint64_t internal_id);
