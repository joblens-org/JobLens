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
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

// ---------------------------------------------------------------------------
// Scheduler identity — the adapter name (string)
// ---------------------------------------------------------------------------
//
// v2 G4 completion: shared code never branches on a scheduler. The adapter
// registry name IS the identity ("condor", "slurm", "ssh", ...); the built-in
// names below are data, not a type. Empty string means "unresolved" (the old
// Unknown). Per-scheduler native-id knowledge lives in self-registered rules
// (NativeIdRuleRegistry), mirroring REGISTER_SCHEDULER_ADAPTER.
inline constexpr const char* kAdapterCondor = "condor";
inline constexpr const char* kAdapterSlurm = "slurm";

// Normalizes a wire/persisted scheduler name: lowercase, whitespace-stripped.
// Unknown names are NOT errors here (open world); call sites that need a
// registered adapter validate against SchedulerAdapterRegistry.
std::string normalize_adapter_name(const std::string& raw);

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
    std::string adapter;            // adapter registry name; empty = unresolved
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
    std::string adapter;             // adapter registry name (never empty)
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

// ---------------------------------------------------------------------------
// Native-id rule helpers (used by the built-in adapters' rules)
// ---------------------------------------------------------------------------

// Leading zeros are tolerated on native components; the canonical numeric
// value removes them so "012.3" and "12.3" name the same job. Nullopt for
// anything that is not pure decimal digits.
std::optional<std::string> strip_leading_zeros(const std::string& decimal);

// Condor native id: "<ClusterId>.<ProcId>", both decimal, both non-empty,
// canonicalized through strip_leading_zeros.
struct CondorNativeId {
    std::string cluster;
    std::string proc;
};
std::optional<CondorNativeId> parse_condor_native(const std::string& raw);

// ---------------------------------------------------------------------------
// Native-id rules (per-adapter, self-registered)
// ---------------------------------------------------------------------------

// What one adapter's rule derives from a selector: the canonical native form
// used in identity keys, the cluster namespace and the normalized step. The
// rule owns every scheduler-specific detail (Condor: "cluster.proc" plus the
// schedd tag namespace, no steps; Slurm: job number with optional normalized
// step). Returning nullopt means "this selector cannot name a job of mine" —
// an incomplete observation, never a guess.
struct CanonicalParts {
    std::string canonical_native;
    std::string cluster_namespace;
    std::string step_id;   // normalized, or empty
};
using NativeIdRule =
    std::function<std::optional<CanonicalParts>(const NativeSelector&)>;

class NativeIdRuleRegistry {
public:
    static NativeIdRuleRegistry& instance();

    // Registers (or replaces) the rule for one adapter name. Replacement is
    // intentional (last wins), mirroring SchedulerAdapterRegistry.
    void registerRule(const std::string& adapter, NativeIdRule rule);
    // nullptr when no rule exists: callers must treat the selector as
    // unresolvable, never fall back to a generic guess.
    const NativeIdRule* find(const std::string& adapter) const;

private:
    mutable std::mutex mutex_;
    std::map<std::string, NativeIdRule> rules_;
};

// Registers a native-id rule at TU scope. Every adapter — the built-ins
// included — registers its rule alongside the adapter, e.g.
//   REGISTER_NATIVE_ID_RULE("ssh", [](const NativeSelector& s) { ... })
// JOBLENS_GLUE makes __COUNTER__ expand before token pasting: one TU may
// include several adapters' headers (job_registry.hpp includes both
// built-ins), and a literal paste would collide on the second registration.
#define JOBLENS_GLUE_(a, b) a##b
#define JOBLENS_GLUE(a, b) JOBLENS_GLUE_(a, b)
#define REGISTER_NATIVE_ID_RULE(NAME, FN)                                       \
    static bool JOBLENS_GLUE(joblens_native_id_reg_, __COUNTER__) = [] {        \
        NativeIdRuleRegistry::instance().registerRule(NAME, FN);                \
        return true;                                                            \
    }();

// Validates and completes a selector through the adapter's registered rule.
// No rule, an empty adapter name, a missing cluster namespace or an
// unparsable native id yields nullopt: an incomplete observation, never a
// guess. The generic parts (key assembly, verbatim native preservation) live
// here; everything scheduler-specific lives in the rule.
std::optional<CanonicalIdentity> canonicalize(const NativeSelector& selector);

// Serialization round trips preserve the original native string, the
// normalized step and every namespace field. The adapter name is written
// under BOTH "adapter" and the legacy "scheduler_type" key (same lowercase
// name, byte-identical to what to_string(SchedulerType) produced, so old
// readers and persisted rows keep working); deserialization accepts either.
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
