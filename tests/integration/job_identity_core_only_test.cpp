// Link only the generic identity implementation, without any production adapter.
#include "core/job_identity.hpp"

#include <iostream>

int main()
{
    NativeSelector selector;
    selector.adapter = "fixture";
    selector.clusterName = "farm";
    selector.job_native_id = "job-abc";
    selector.step_id = "phase-a";

    if (canonicalize(selector)) {
        std::cerr << "core inferred identity without a registered rule\n";
        return 1;
    }

    NativeIdRuleRegistry::instance().registerRule("fixture", [](const NativeSelector& s)
        -> std::optional<CanonicalParts> {
        if (s.job_native_id != "job-abc" || s.step_id != "phase-a") return std::nullopt;
        CanonicalParts parts;
        parts.cluster_namespace = s.clusterName;
        parts.canonical_native = "abc";
        parts.step_id = s.step_id;
        return parts;
    });

    const auto key = canonicalize(selector);
    if (!key || key->key() != "fixture|farm|abc|phase-a" ||
        key->job_native_id != "job-abc") {
        std::cerr << "core did not use the registered scheduler's identity grammar\n";
        return 1;
    }
    selector.step_id = "0";
    if (canonicalize(selector)) {
        std::cerr << "core accepted identity rejected by the adapter rule\n";
        return 1;
    }
    std::cout << "job_identity_core_only_test: all passed\n";
}
