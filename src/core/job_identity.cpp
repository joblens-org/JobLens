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
#include "core/job_identity.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdio>
#include <limits>

namespace {

bool is_decimal(const std::string& text)
{
    if (text.empty()) return false;
    return std::all_of(text.begin(), text.end(),
                       [](unsigned char c) { return std::isdigit(c) != 0; });
}

// Leading zeros are tolerated on native components; the canonical numeric
// value removes them so "012.3" and "12.3" name the same job.
std::optional<std::string> strip_leading_zeros(const std::string& decimal)
{
    if (!is_decimal(decimal)) return std::nullopt;
    const auto first_non_zero = decimal.find_first_not_of('0');
    if (first_non_zero == std::string::npos) return std::string{"0"};
    return decimal.substr(first_non_zero);
}

// Condor native id: "<ClusterId>.<ProcId>", both decimal, both non-empty.
struct CondorNativeId {
    std::string cluster;
    std::string proc;
};

std::optional<CondorNativeId> parse_condor_native(const std::string& raw)
{
    const auto dot = raw.find('.');
    if (dot == std::string::npos) return std::nullopt;
    const auto cluster = raw.substr(0, dot);
    const auto proc = raw.substr(dot + 1);
    if (cluster.empty() || proc.empty()) return std::nullopt;
    if (proc.find('.') != std::string::npos) return std::nullopt;
    auto canonical_cluster = strip_leading_zeros(cluster);
    auto canonical_proc = strip_leading_zeros(proc);
    if (!canonical_cluster || !canonical_proc) return std::nullopt;
    return CondorNativeId{*canonical_cluster, *canonical_proc};
}

uint64_t mix_fnv1a(uint64_t seed, const void* data, size_t size)
{
    const auto* bytes = static_cast<const unsigned char*>(data);
    uint64_t hash = seed;
    for (size_t index = 0; index < size; ++index) {
        hash ^= bytes[index];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

void append_hex(char* out, uint64_t value)
{
    std::snprintf(out, 17, "%016llx", static_cast<unsigned long long>(value));
}

} // namespace

// ---------------------------------------------------------------------------
// Scheduler type
// ---------------------------------------------------------------------------

std::optional<SchedulerType> scheduler_type_from_string(const std::string& raw)
{
    if (raw == "condor" || raw == "Condor" || raw == "CONDOR") return SchedulerType::Condor;
    if (raw == "slurm" || raw == "Slurm" || raw == "SLURM") return SchedulerType::Slurm;
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Internal IDs
// ---------------------------------------------------------------------------

std::optional<uint64_t> parse_internal_id(const std::string& decimal)
{
    if (decimal.empty() || decimal.size() > 20) return std::nullopt;
    if (!is_decimal(decimal)) return std::nullopt;
    uint64_t value = 0;
    const auto* begin = decimal.data();
    const auto* end = decimal.data() + decimal.size();
    const auto result = std::from_chars(begin, end, value, 10);
    if (result.ec != std::errc() || result.ptr != end) return std::nullopt;
    return value;
}

std::string format_internal_id(uint64_t id)
{
    return std::to_string(id);
}

LegacyId parse_legacy_job_id(const nlohmann::json& node)
{
    LegacyId parsed;
    if (node.is_null()) {
        parsed.status = LegacyIdStatus::Absent;
        return parsed;
    }
    uint64_t value = 0;
    if (node.is_number_unsigned()) {
        value = node.get<uint64_t>();
    } else if (node.is_number_integer()) {
        const auto signed_value = node.get<int64_t>();
        if (signed_value < 0) {
            parsed.status = LegacyIdStatus::Invalid;
            return parsed;
        }
        value = static_cast<uint64_t>(signed_value);
    } else if (node.is_string()) {
        const auto decimal = parse_internal_id(node.get<std::string>());
        if (!decimal) {
            parsed.status = LegacyIdStatus::Invalid;
            return parsed;
        }
        value = *decimal;
    } else {
        // Floats, booleans, containers: never silently coerced.
        parsed.status = LegacyIdStatus::Invalid;
        return parsed;
    }
    parsed.value = value;
    parsed.status = is_reserved_internal_id(value) ? LegacyIdStatus::Reserved
                                                   : LegacyIdStatus::Valid;
    return parsed;
}

// ---------------------------------------------------------------------------
// Slurm step identity
// ---------------------------------------------------------------------------

std::optional<std::string> normalize_slurm_step_id(const std::string& raw)
{
    if (raw == "batch" || raw == "extern") return raw;
    if (raw.empty()) return std::nullopt;
    return strip_leading_zeros(raw);
}

std::optional<std::string> slurm_step_name(uint32_t numeric_step)
{
    if (numeric_step == kSlurmBatchStepValue) return std::string{"batch"};
    if (numeric_step == kSlurmExternStepValue) return std::string{"extern"};
    return std::nullopt;
}

std::optional<uint32_t> slurm_step_value(const std::string& step_id)
{
    if (step_id == "batch") return kSlurmBatchStepValue;
    if (step_id == "extern") return kSlurmExternStepValue;
    if (!is_decimal(step_id)) return std::nullopt;
    uint32_t value = 0;
    const auto* begin = step_id.data();
    const auto* end = step_id.data() + step_id.size();
    const auto result = std::from_chars(begin, end, value, 10);
    if (result.ec != std::errc() || result.ptr != end) return std::nullopt;
    return value;
}

// ---------------------------------------------------------------------------
// Canonical identity
// ---------------------------------------------------------------------------

std::string CanonicalIdentity::key() const
{
    std::string joined;
    joined.reserve(cluster_namespace.size() + canonical_native.size() + step_id.size() + 8);
    joined += to_string(scheduler_type);
    joined += '|';
    joined += cluster_namespace;
    joined += '|';
    joined += canonical_native;
    if (!step_id.empty()) {
        joined += '|';
        joined += step_id;
    }
    return joined;
}

bool operator==(const CanonicalIdentity& lhs, const CanonicalIdentity& rhs)
{
    return lhs.scheduler_type == rhs.scheduler_type &&
           lhs.cluster_namespace == rhs.cluster_namespace &&
           lhs.canonical_native == rhs.canonical_native &&
           lhs.step_id == rhs.step_id;
}

bool operator!=(const CanonicalIdentity& lhs, const CanonicalIdentity& rhs)
{
    return !(lhs == rhs);
}

bool operator<(const CanonicalIdentity& lhs, const CanonicalIdentity& rhs)
{
    return lhs.key() < rhs.key();
}

std::optional<CanonicalIdentity> canonicalize(const NativeSelector& selector)
{
    if (selector.scheduler_type == SchedulerType::Unknown) return std::nullopt;
    if (selector.cluster_name.empty()) return std::nullopt;
    if (selector.native_job_id.empty()) return std::nullopt;

    CanonicalIdentity identity;
    identity.scheduler_type = selector.scheduler_type;
    identity.native_job_id = selector.native_job_id; // preserved verbatim

    if (selector.scheduler_type == SchedulerType::Condor) {
        if (!selector.step_id.empty()) return std::nullopt; // Condor has no steps
        auto parsed = parse_condor_native(selector.native_job_id);
        if (!parsed) return std::nullopt;
        identity.cluster_namespace = selector.cluster_name;
        if (!selector.cluster_tag.empty())
            identity.cluster_namespace += "@" + selector.cluster_tag;
        // The namespace separates schedds; the canonical native form removes
        // leading zeros so one job cannot fork into two identities, while the
        // original wire string stays preserved in native_job_id.
        identity.canonical_native = parsed->cluster + "." + parsed->proc;
        return identity;
    }

    // Slurm: logical job number without a step; a step refines the identity.
    auto job_number = strip_leading_zeros(selector.native_job_id);
    if (!job_number) return std::nullopt;
    if (!selector.step_id.empty()) {
        auto step = normalize_slurm_step_id(selector.step_id);
        if (!step) return std::nullopt;
        identity.step_id = *step;
    }
    identity.cluster_namespace = selector.cluster_name;
    identity.canonical_native = *job_number;
    return identity;
}

// ---------------------------------------------------------------------------
// Process / generation identity
// ---------------------------------------------------------------------------

bool operator==(const ProcessIdentity& lhs, const ProcessIdentity& rhs)
{
    return lhs.pid == rhs.pid && lhs.start_ticks == rhs.start_ticks &&
           lhs.boot_id == rhs.boot_id;
}

bool operator!=(const ProcessIdentity& lhs, const ProcessIdentity& rhs)
{
    return !(lhs == rhs);
}

bool operator==(const ExecutionGeneration& lhs, const ExecutionGeneration& rhs)
{
    return lhs.boot_id == rhs.boot_id && lhs.manager_pid == rhs.manager_pid &&
           lhs.manager_start_ticks == rhs.manager_start_ticks;
}

bool operator!=(const ExecutionGeneration& lhs, const ExecutionGeneration& rhs)
{
    return !(lhs == rhs);
}

// ---------------------------------------------------------------------------
// Opaque instance handles
// ---------------------------------------------------------------------------

std::string derive_instance_id(const std::string& db_uuid, uint64_t internal_id)
{
    constexpr uint64_t kSeed = 0xcbf29ce484222325ULL;
    const uint64_t first =
        mix_fnv1a(kSeed, db_uuid.data(), db_uuid.size());
    const uint64_t second =
        mix_fnv1a(first, &internal_id, sizeof(internal_id));
    const uint64_t third = mix_fnv1a(second, db_uuid.data(), db_uuid.size());
    char hex[33];
    append_hex(hex, second);
    append_hex(hex + 16, third);
    return std::string{"ji1-"} + std::string(hex, 32);
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

void to_json(nlohmann::json& out, const NativeSelector& selector)
{
    out = nlohmann::json::object();
    out["scheduler_type"] = to_string(selector.scheduler_type);
    out["cluster_name"] = selector.cluster_name;
    out["cluster_tag"] = selector.cluster_tag;
    out["native_job_id"] = selector.native_job_id;
    out["step_id"] = selector.step_id;
}

void from_json(const nlohmann::json& in, NativeSelector& selector)
{
    const auto type = in.at("scheduler_type").get<std::string>();
    const auto parsed_type = scheduler_type_from_string(type);
    if (!parsed_type)
        throw std::runtime_error("unknown scheduler_type '" + type + "'");
    selector.scheduler_type = *parsed_type;
    selector.cluster_name = in.at("cluster_name").get<std::string>();
    selector.cluster_tag = in.value("cluster_tag", "");
    selector.native_job_id = in.at("native_job_id").get<std::string>();
    selector.step_id = in.value("step_id", "");
}

void to_json(nlohmann::json& out, const CanonicalIdentity& identity)
{
    out = nlohmann::json::object();
    out["scheduler_type"] = to_string(identity.scheduler_type);
    out["cluster_namespace"] = identity.cluster_namespace;
    out["native_job_id"] = identity.native_job_id;
    out["canonical_native"] = identity.canonical_native;
    out["step_id"] = identity.step_id;
}

void from_json(const nlohmann::json& in, CanonicalIdentity& identity)
{
    const auto type = in.at("scheduler_type").get<std::string>();
    const auto parsed_type = scheduler_type_from_string(type);
    if (!parsed_type)
        throw std::runtime_error("unknown scheduler_type '" + type + "'");
    identity.scheduler_type = *parsed_type;
    identity.cluster_namespace = in.at("cluster_namespace").get<std::string>();
    identity.native_job_id = in.at("native_job_id").get<std::string>();
    identity.canonical_native = in.at("canonical_native").get<std::string>();
    identity.step_id = in.value("step_id", "");
}
