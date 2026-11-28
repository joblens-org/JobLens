#pragma once

#include <optional>
#include <string>
#include <utility>

#include "core/job_identity.hpp"

namespace CondorJob {

struct CondorNativeId {
    std::string cluster;
    std::string proc;
};

// "<ClusterId>.<ProcId>": both components are nonempty decimal strings.
inline std::optional<CondorNativeId> parse_native_id(const std::string& raw)
{
    const auto dot = raw.find('.');
    if (dot == std::string::npos) return std::nullopt;
    auto cluster = strip_leading_zeros(raw.substr(0, dot));
    auto proc = strip_leading_zeros(raw.substr(dot + 1));
    if (!cluster || !proc) return std::nullopt;
    return CondorNativeId{std::move(*cluster), std::move(*proc)};
}

}  // namespace CondorJob
