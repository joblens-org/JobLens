#pragma once

#include <array>
#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "core/job_identity.hpp"

namespace SlurmJob {

// Slurm's reserved step numbers; keep its wire/cgroup translation in one place.
inline constexpr uint32_t kSlurmBatchStepValue = 0xfffffffbu;   // SLURM_BATCH_SCRIPT
inline constexpr uint32_t kSlurmExternStepValue = 0xfffffffcu;  // SLURM_EXTERN_CONT
inline constexpr uint32_t kSlurmMaxNormalStepId = 0xfffffff0u;

inline constexpr std::array kSpecialSteps = {
    std::pair<std::string_view, uint32_t>{"batch", kSlurmBatchStepValue},
    std::pair<std::string_view, uint32_t>{"extern", kSlurmExternStepValue},
};

inline std::optional<std::string> special_step_name(uint32_t value)
{
    for (const auto& [name, number] : kSpecialSteps) {
        if (number == value) return std::string(name);
    }
    return std::nullopt;
}

inline std::optional<uint32_t> special_step_value(std::string_view name)
{
    for (const auto& [label, number] : kSpecialSteps) {
        if (label == name) return number;
    }
    return std::nullopt;
}

inline std::optional<std::string> normalize_step_id(const std::string& raw)
{
    if (special_step_value(raw)) return raw;
    return strip_leading_zeros(raw);
}

inline std::optional<uint32_t> step_value(const std::string& name)
{
    if (const auto special = special_step_value(name)) return special;
    uint32_t value = 0;
    const auto* end = name.data() + name.size();
    const auto result = std::from_chars(name.data(), end, value, 10);
    if (result.ec != std::errc() || result.ptr != end) return std::nullopt;
    return value;
}

inline std::string step_id_name(uint32_t value)
{
    if (const auto special = special_step_name(value)) return *special;
    return std::to_string(value);
}

}  // namespace SlurmJob
