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

} // namespace

// ---------------------------------------------------------------------------
// Scheduler identity (adapter name) and native-id rules
// ---------------------------------------------------------------------------

std::string normalize_adapter_name(const std::string& raw)
{
    std::string normalized;
    normalized.reserve(raw.size());
    for (const char c : raw) {
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') continue;
        normalized.push_back(static_cast<char>(
            std::tolower(static_cast<unsigned char>(c))));
    }
    return normalized;
}

NativeIdRuleRegistry& NativeIdRuleRegistry::instance()
{
    static NativeIdRuleRegistry registry;
    return registry;
}

void NativeIdRuleRegistry::registerRule(const std::string& adapter, NativeIdRule rule)
{
    std::lock_guard<std::mutex> lock(mutex_);
    rules_[adapter] = std::move(rule);
}

const NativeIdRule* NativeIdRuleRegistry::find(const std::string& adapter) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = rules_.find(adapter);
    return it == rules_.end() ? nullptr : &it->second;
}

// Shared helpers of the adapters' native-id rules (declared in the header so
// the adapter headers' REGISTER_NATIVE_ID_RULE bodies can call them).

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
    joined.reserve(adapter.size() + cluster_namespace.size() +
                   canonical_native.size() + step_id.size() + 8);
    joined += adapter;
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
    return lhs.adapter == rhs.adapter &&
           lhs.cluster_namespace == rhs.cluster_namespace &&
           lhs.canonical_native == rhs.canonical_native &&
           lhs.step_id == rhs.step_id;
}

bool operator!=(const CanonicalIdentity& lhs, const CanonicalIdentity& rhs)
{
    return !(lhs == rhs);
}

std::optional<CanonicalIdentity> canonicalize(const NativeSelector& selector)
{
    if (selector.adapter.empty()) return std::nullopt;
    if (selector.cluster_name.empty()) return std::nullopt;
    if (selector.native_job_id.empty()) return std::nullopt;

    // Everything scheduler-specific lives in the adapter's registered rule;
    // no rule means this layer cannot name the job — explicitly incomplete.
    const NativeIdRule* rule = NativeIdRuleRegistry::instance().find(selector.adapter);
    if (rule == nullptr || !*rule) return std::nullopt;
    const auto parts = (*rule)(selector);
    if (!parts) return std::nullopt;

    CanonicalIdentity identity;
    identity.adapter = selector.adapter;
    identity.native_job_id = selector.native_job_id; // preserved verbatim
    identity.cluster_namespace = parts->cluster_namespace;
    identity.canonical_native = parts->canonical_native;
    identity.step_id = parts->step_id;
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
// Serialization
// ---------------------------------------------------------------------------

void to_json(nlohmann::json& out, const NativeSelector& selector)
{
    out = nlohmann::json::object();
    // Both keys, same lowercase name: new readers prefer "adapter", the
    // legacy key keeps old binaries and rollback scenarios reading new rows.
    out["adapter"] = selector.adapter;
    out["scheduler_type"] = selector.adapter;
    out["cluster_name"] = selector.cluster_name;
    out["cluster_tag"] = selector.cluster_tag;
    out["native_job_id"] = selector.native_job_id;
    out["step_id"] = selector.step_id;
}

void from_json(const nlohmann::json& in, NativeSelector& selector)
{
    std::string name;
    if (in.contains("adapter") && in.at("adapter").is_string())
        name = in.at("adapter").get<std::string>();
    else
        name = in.at("scheduler_type").get<std::string>();
    selector.adapter = normalize_adapter_name(name);
    selector.cluster_name = in.at("cluster_name").get<std::string>();
    selector.cluster_tag = in.value("cluster_tag", "");
    selector.native_job_id = in.at("native_job_id").get<std::string>();
    selector.step_id = in.value("step_id", "");
}

void to_json(nlohmann::json& out, const CanonicalIdentity& identity)
{
    out = nlohmann::json::object();
    out["adapter"] = identity.adapter;
    out["scheduler_type"] = identity.adapter;
    out["cluster_namespace"] = identity.cluster_namespace;
    out["native_job_id"] = identity.native_job_id;
    out["canonical_native"] = identity.canonical_native;
    out["step_id"] = identity.step_id;
}

void from_json(const nlohmann::json& in, CanonicalIdentity& identity)
{
    std::string name;
    if (in.contains("adapter") && in.at("adapter").is_string())
        name = in.at("adapter").get<std::string>();
    else
        name = in.at("scheduler_type").get<std::string>();
    identity.adapter = normalize_adapter_name(name);
    identity.cluster_namespace = in.at("cluster_namespace").get<std::string>();
    identity.native_job_id = in.at("native_job_id").get<std::string>();
    identity.canonical_native = in.at("canonical_native").get<std::string>();
    identity.step_id = in.value("step_id", "");
}
