#pragma once

#include <optional>
#include <string_view>
#include <tuple>
#include <variant>

#include <nlohmann/json.hpp>

#include "job_watcher/condor_adapter.hpp"
#include "job_watcher/slurm_adapter.hpp"

// Compatibility assembly for persisted JobSubType values, Lua and typed
// legacy facts. New adapters use the name + JSON alternatives without edits
// here. Keep the numeric tags and variant order stable.
enum class JobSubType { Unknown = 0, Common = 1, Condor = 2, Slurm = 3 };

// Preserve the example carried by the legacy native-selector diagnostic.
inline constexpr char kLegacyNativeSelectorFormatHint[] =
    " (Condor native ids are '<ClusterId>.<ProcId>')";

template <class CommonFacts>
using JobFactStorage = std::variant<CondorJobAttr, CommonFacts, SlurmJobAttr, nlohmann::json>;

#define JOBLENS_BUILTIN_FACTS(X) \
    X(condor, CondorJobAttr, Condor) \
    X(slurm, SlurmJobAttr, Slurm)

inline const char* to_string(JobSubType subtype)
{
#define JOBLENS_LEGACY_NAME(name, type, tag) case JobSubType::tag: return #tag;
    switch (subtype) {
        case JobSubType::Common: return "Common";
        JOBLENS_BUILTIN_FACTS(JOBLENS_LEGACY_NAME)
        default: return "Unknown";
    }
#undef JOBLENS_LEGACY_NAME
}

inline std::optional<JobSubType> legacy_subtype_named(std::string_view name)
{
    if (name == "Common") return JobSubType::Common;
#define JOBLENS_LEGACY_TAG(adapter, type, tag) if (name == #tag) return JobSubType::tag;
    JOBLENS_BUILTIN_FACTS(JOBLENS_LEGACY_TAG)
#undef JOBLENS_LEGACY_TAG
    return std::nullopt;
}

inline const char* adapter_name_for(JobSubType subtype)
{
#define JOBLENS_ADAPTER_NAME(name, type, tag) case JobSubType::tag: return #name;
    switch (subtype) {
        JOBLENS_BUILTIN_FACTS(JOBLENS_ADAPTER_NAME)
        default: return nullptr;
    }
#undef JOBLENS_ADAPTER_NAME
}

template <class Attr>
constexpr JobSubType legacy_subtype_of(const Attr*) { return JobSubType::Common; }

#define JOBLENS_FACT_TAG(name, type, tag)                       \
    inline constexpr JobSubType legacy_subtype_of(const type*) \
    { return JobSubType::tag; }
JOBLENS_BUILTIN_FACTS(JOBLENS_FACT_TAG)
#undef JOBLENS_FACT_TAG
#undef JOBLENS_BUILTIN_FACTS
