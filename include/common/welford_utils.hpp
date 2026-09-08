#pragma once

#include "ebpf/bpf_types.h"

#include <cmath>
#include <limits>

namespace WelfordUtils {

struct Aggregate {
    u64 count{0};
    u64 mean{0};
    s64 m2{0};
};

struct MergeStatus {
    bool invalid_input{false};
    bool count_saturated{false};
    bool mean_saturated{false};
    bool m2_saturated{false};

    bool saturated() const noexcept
    {
        return count_saturated || mean_saturated || m2_saturated;
    }

    bool ok() const noexcept
    {
        return !invalid_input && !saturated();
    }

    void absorb(const MergeStatus& other) noexcept
    {
        invalid_input = invalid_input || other.invalid_input;
        count_saturated = count_saturated || other.count_saturated;
        mean_saturated = mean_saturated || other.mean_saturated;
        m2_saturated = m2_saturated || other.m2_saturated;
    }
};

namespace detail {

inline u64 saturate_u64(long double value, bool& saturated) noexcept
{
    const auto max_value = static_cast<long double>(std::numeric_limits<u64>::max());
    if (!std::isfinite(value) || value > max_value) {
        saturated = true;
        return std::numeric_limits<u64>::max();
    }
    if (value < 0.0L) {
        saturated = true;
        return 0;
    }
    return static_cast<u64>(value);
}

inline s64 saturate_m2(long double value, bool& saturated) noexcept
{
    const auto max_value = static_cast<long double>(std::numeric_limits<s64>::max());
    if (!std::isfinite(value) || value > max_value) {
        saturated = true;
        return std::numeric_limits<s64>::max();
    }
    if (value < 0.0L) {
        saturated = true;
        return 0;
    }
    return static_cast<s64>(value);
}

}

inline MergeStatus merge_into(Aggregate& target, const Aggregate& source) noexcept
{
    MergeStatus status;
    if (target.m2 < 0 || source.m2 < 0) {
        status.invalid_input = true;
        status.m2_saturated = true;
        target.m2 = std::numeric_limits<s64>::max();
        return status;
    }
    if (source.count == 0) {
        return status;
    }
    if (target.count == 0) {
        target = source;
        return status;
    }

    const auto target_count = static_cast<long double>(target.count);
    const auto source_count = static_cast<long double>(source.count);
    const auto merged_count = target_count + source_count;
    const auto delta = static_cast<long double>(source.mean) - static_cast<long double>(target.mean);
    const auto merged_mean = static_cast<long double>(target.mean) + delta * source_count / merged_count;
    const auto merged_m2 = static_cast<long double>(target.m2) + static_cast<long double>(source.m2) +
        delta * delta * target_count * source_count / merged_count;

    status.count_saturated = source.count > std::numeric_limits<u64>::max() - target.count;
    target.count = status.count_saturated ? std::numeric_limits<u64>::max() : target.count + source.count;

    bool mean_saturated = false;
    target.mean = detail::saturate_u64(merged_mean, mean_saturated);
    status.mean_saturated = mean_saturated;

    bool m2_saturated = false;
    target.m2 = detail::saturate_m2(merged_m2, m2_saturated);
    status.m2_saturated = m2_saturated;

    return status;
}

}
