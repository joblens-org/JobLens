#!/usr/bin/env bash
# Build the production identity TU in isolation, without scheduler-specific
# identity code or adapter objects. GCC and Clang both support GCC poison.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cat > "$work/identity_boundary.h" <<'EOF'
#pragma GCC poison kAdapterCondor kAdapterSlurm CondorNativeId parse_condor_native
#pragma GCC poison kSlurmBatchStepValue kSlurmExternStepValue kSlurmMaxNormalStepId
#pragma GCC poison normalize_slurm_step_id slurm_step_name slurm_step_value parse_cgroup_job_step
EOF

"${CXX:-c++}" -std=c++17 -Wall -Wextra -Werror -pthread -I"$root/include" \
    -include "$work/identity_boundary.h" "$root/src/core/job_identity.cpp" \
    "$root/tests/integration/job_identity_core_only_test.cpp" -o "$work/identity_core_only"
"$work/identity_core_only"
