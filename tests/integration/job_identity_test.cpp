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
/* Deterministic identity acceptance for the job inventory.
 *
 *   * Condor namespace separation, "12.34" versus "123.4", original native
 *     strings preserved, no digit concatenation.
 *   * Internal IDs above UINT32_MAX and 2^53: exact uint64 round trips,
 *     decimal-string output, JobID 0 reserved, ES JobID stays numeric.
 *   * Slurm steps stay distinct (batch/extern never collapse to zero), PID
 *     reuse detected through start ticks and boot identity.
 */
#include "core/job_identity.hpp"
#include "core/collector_type.h"
// The built-in rules live next to their adapters now: including the adapter
// headers registers them, and the tests below exercise those production
// rules (the only ones a real deployment ever sees).
#include "job_watcher/condor_adapter.hpp"
#include "job_watcher/slurm_adapter.hpp"

#include <cstdint>
#include <iostream>
#include <string>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

NativeSelector condor(const std::string& native, const std::string& tag = "schedd-a",
                      const std::string& cluster = "htcondor01")
{
    NativeSelector selector;
    selector.adapter = "condor";
    selector.cluster_name = cluster;
    selector.cluster_tag = tag;
    selector.native_job_id = native;
    return selector;
}

NativeSelector slurm(const std::string& native, const std::string& step = "",
                     const std::string& cluster = "slurm-prod")
{
    NativeSelector selector;
    selector.adapter = "slurm";
    selector.cluster_name = cluster;
    selector.native_job_id = native;
    selector.step_id = step;
    return selector;
}
void condor_namespace_separation()
{
    const auto first = canonicalize(condor("12.34"));
    const auto second = canonicalize(condor("123.4"));
    check(first && second, "valid Condor selectors must canonicalize");
    check(first->key() != second->key(),
          "'12.34' and '123.4' must stay distinct (no digit concatenation)");
    check(first->native_job_id == "12.34" && second->native_job_id == "123.4",
          "original native strings must be preserved");

    // Two schedds in one cluster are different namespaces for the same number.
    const auto schedd_a = canonicalize(condor("12.34", "schedd-a"));
    const auto schedd_b = canonicalize(condor("12.34", "schedd-b"));
    check(schedd_a && schedd_b && schedd_a->key() != schedd_b->key(),
          "two schedds must not merge into one identity");
    check(schedd_a->cluster_namespace == "htcondor01@schedd-a",
          "Condor cluster namespace must include the tag");

    // Two scheduler types never merge, even with identical numbers.
    const auto as_condor = canonicalize(condor("7.0"));
    const auto as_slurm = canonicalize(slurm("7"));
    check(as_condor && as_slurm && as_condor->key() != as_slurm->key(),
          "scheduler types must stay distinct");

    // Leading zeros are the same job, not a new identity.
    const auto padded = canonicalize(condor("012.3"));
    const auto plain = canonicalize(condor("12.3"));
    check(padded && plain && padded->key() == plain->key(),
          "leading zeros must not fork one Condor job into two identities");
    check(padded->native_job_id == "012.3" && plain->native_job_id == "12.3",
          "preserved strings keep their original form");
    check(padded->canonical_native == "12.3", "canonical native drops padding");

    // Incomplete identities fail explicitly and never reach an index.
    const char* invalid_natives[] = {"1234", "12.", ".3", "1.2.3", "a.b", "", "12.-3"};
    for (const auto* native : invalid_natives) {
        check(!canonicalize(condor(native)).has_value(),
              std::string("invalid Condor native '") + native + "' must not canonicalize");
    }
    NativeSelector no_cluster = condor("12.34");
    no_cluster.cluster_name.clear();
    check(!canonicalize(no_cluster), "missing cluster namespace must fail");
    NativeSelector unknown_type = condor("12.34");
    unknown_type.adapter.clear();
    check(!canonicalize(unknown_type), "unknown scheduler type must fail");
    NativeSelector with_step = condor("12.34");
    with_step.step_id = "1";
    check(!canonicalize(with_step), "Condor selectors cannot carry a step");
}
void internal_id_bounds()
{
    const uint64_t above_u32 = 4294967296ULL;              // 2^32
    const uint64_t above_js = 9007199254740993ULL;         // 2^53 + 1
    const uint64_t max_u64 = 18446744073709551615ULL;      // UINT64_MAX
    const uint64_t values[] = {uint64_t{1}, above_u32, above_js, max_u64};
    for (const uint64_t value : values) {
        const auto text = format_internal_id(value);
        check(text == std::to_string(value), "decimal-string output");
        const auto parsed = parse_internal_id(text);
        check(parsed && *parsed == value,
              "uint64 round trip failed for " + std::to_string(value));
    }

    const char* invalid[] = {"", "-1", "+1", " 1", "1 ", "0x10", "1.5", "abc",
                             "18446744073709551616", "99999999999999999999999"};
    for (const auto* text : invalid)
        check(!parse_internal_id(text).has_value(),
              std::string("internal ID parse must reject '") + text + "'");

    check(format_internal_id(0) == "0", "sentinel formats as decimal zero");
    check(is_reserved_internal_id(0), "JobID 0 is the reserved system sentinel");
    check(!is_reserved_internal_id(1), "JobID 1 is not reserved");

    // Legacy JSON nodes: explicit uint64 semantics, no silent coercion.
    const auto big_number = parse_legacy_job_id(nlohmann::json(above_js));
    check(big_number.status == LegacyIdStatus::Valid && big_number.value == above_js,
          "JSON number above 2^53 must round trip exactly");
    const auto big_string =
        parse_legacy_job_id(nlohmann::json(std::string("18446744073709551615")));
    check(big_string.status == LegacyIdStatus::Valid &&
              big_string.value == 18446744073709551615ULL,
          "decimal-string IDs must parse to UINT64_MAX exactly");
    check(parse_legacy_job_id(nlohmann::json(nullptr)).status == LegacyIdStatus::Absent,
          "absent node reports Absent");
    check(parse_legacy_job_id(nlohmann::json(0)).status == LegacyIdStatus::Reserved,
          "reserved sentinel is reported, not treated as a valid ID");
    check(parse_legacy_job_id(nlohmann::json(-5)).status == LegacyIdStatus::Invalid,
          "negative IDs are invalid");
    check(parse_legacy_job_id(nlohmann::json(1.5)).status == LegacyIdStatus::Invalid,
          "fractional IDs are invalid");
    check(parse_legacy_job_id(nlohmann::json(true)).status == LegacyIdStatus::Invalid,
          "booleans are invalid IDs");
    check(parse_legacy_job_id(nlohmann::json("12x")).status == LegacyIdStatus::Invalid,
          "non-decimal text is an invalid ID");

    // ES-facing documents keep the internal JobID numeric (existing contract).
    Job job;
    job.JobID = above_js;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    job.sub_attr = CommonJobAttr{};
    const auto serialized = job_to_json(job);
    check(serialized.at("JobID").is_number_unsigned(),
          "ES job_info.JobID must remain a JSON number");
    check(serialized.at("JobID").get<uint64_t>() == above_js,
          "numeric JobID must survive serialization exactly");
}
void slurm_steps_and_pid_reuse()
{
    const auto step0 = canonicalize(slurm("7", "0"));
    const auto step1 = canonicalize(slurm("7", "1"));
    const auto batch = canonicalize(slurm("7", "batch"));
    const auto extern_step = canonicalize(slurm("7", "extern"));
    check(step0 && step1 && batch && extern_step, "step selectors must canonicalize");
    check(step0->key() != step1->key(), "two steps of one job stay distinct");
    check(batch->key() != extern_step->key(), "batch and extern stay distinct");
    check(batch->key() != step0->key(), "batch must never collapse to numeric step 0");
    check(extern_step->key() != step0->key(), "extern must never collapse to numeric step 0");
    check(batch->step_id == "batch" && extern_step->step_id == "extern",
          "special step names are preserved verbatim");

    const auto logical = canonicalize(slurm("7"));
    const auto padded = canonicalize(slurm("07", "07"));
    check(logical && logical->step_id.empty(), "no step means the logical job");
    check(logical->key() != step0->key(), "logical job differs from any step");
    check(padded && padded->key() == canonicalize(slurm("7", "7"))->key(),
          "leading zeros canonicalize in both native id and step");

    check(!normalize_slurm_step_id("").has_value(), "empty step is invalid");
    check(!normalize_slurm_step_id("x1").has_value(), "non-numeric step is invalid");
    check(normalize_slurm_step_id("0042") == std::string("42"),
          "numeric steps drop leading zeros");

    // Adapter-boundary conversion table for scheduler-specific constants.
    check(slurm_step_name(kSlurmBatchStepValue) == std::string("batch"),
          "batch conversion");
    check(slurm_step_name(kSlurmExternStepValue) == std::string("extern"),
          "extern conversion");
    // These are the real slurm/slurm.h constants, not self-referential checks:
    // a swapped table would label every batch step as extern.
    check(kSlurmBatchStepValue == 0xfffffffeu, "SLURM_BATCH_SCRIPT must be 0xfffffffe");
    check(kSlurmExternStepValue == 0xfffffffdu, "SLURM_EXTERN_CONT must be 0xfffffffd");
    check(slurm_step_name(0xfffffffeu) == std::string("batch"),
          "SLURM_BATCH_SCRIPT numeric value must map to batch");
    check(slurm_step_name(0xfffffffdu) == std::string("extern"),
          "SLURM_EXTERN_CONT numeric value must map to extern");
    check(!slurm_step_name(5).has_value(), "plain numbers have no special name");
    check(slurm_step_value("batch") == kSlurmBatchStepValue, "batch numeric value");
    check(slurm_step_value("extern") == kSlurmExternStepValue, "extern numeric value");
    check(slurm_step_value("7") == 7u, "plain step numeric value");
    check(!slurm_step_value("nope").has_value(), "unknown step name has no value");

    // PID reuse: the same PID with different start ticks or boot ids is a
    // different process.
    const ProcessIdentity base{4242, 123456, "boot-1"};
    check(base.complete(), "fully populated process identity is complete");
    check(base == ProcessIdentity{4242, 123456, "boot-1"}, "identical triples are equal");
    check(!(base == ProcessIdentity{4242, 123457, "boot-1"}),
          "same PID with new start ticks is a reused PID, not the same process");
    check(!(base == ProcessIdentity{4242, 123456, "boot-2"}),
          "same PID across reboots is not the same process");
    check(!ProcessIdentity{4242, 0, ""}.complete(), "partial process identity");

    // The manager triple distinguishes executions of one logical job.
    const ProcessIdentity manager{900, 777, "boot-1"};
    check(manager.complete(), "complete manager identity");
    check(manager == ProcessIdentity{900, 777, "boot-1"},
          "the same manager triple is the same execution");
    check(manager != ProcessIdentity{900, 778, "boot-1"},
          "a restarted manager is a new execution");
    check(manager != ProcessIdentity{900, 777, "boot-2"},
          "a reboot is a new execution");
    check(!ProcessIdentity{0, 0, ""}.complete(),
          "manager identity without pid/start ticks/boot is incomplete");
}

// Serialization round trips ---------------------------------------------------

void serialization_round_trip()
{
    CondorJobAttr condor_facts;
    condor_facts.scheduler_name = "schedd#one@submit.example";
    nlohmann::json facts_wire = condor_facts;
    check(facts_wire.value("scheduler_name", std::string{}) ==
              "schedd#one@submit.example",
          "Condor typed facts must persist the intact schedd name");
    check(facts_wire.get<CondorJobAttr>().scheduler_name ==
              "schedd#one@submit.example",
          "Condor typed facts must restore the intact schedd name");
    facts_wire.erase("scheduler_name");
    check(facts_wire.get<CondorJobAttr>().scheduler_name.empty(),
          "old Condor facts without scheduler_name must remain readable");

    const NativeSelector selector = condor("012.34");
    const nlohmann::json wire = selector;
    const NativeSelector restored = wire.get<NativeSelector>();
    check(restored.adapter == "condor" &&
              restored.cluster_name == "htcondor01" &&
              restored.cluster_tag == "schedd-a" &&
              restored.native_job_id == "012.34" && restored.step_id.empty(),
          "NativeSelector round trip preserves every field");
    check(restored.native_job_id == selector.native_job_id,
          "original native string survives the round trip");

    const auto key = canonicalize(slurm("7", "batch"));
    check(key.has_value(), "slurm step selector canonicalizes");
    check(key->key() == canonicalize(slurm("007", "batch"))->key(),
          "the derived key is normalization-invariant");
    check(key->key() != canonicalize(slurm("7", "1"))->key(),
          "the derived key separates steps");
    check(key->index_key() == canonicalize(slurm("7", "batch"))->index_key(),
          "the durable index key is stable across derivations");

    // Open-world names: an unregistered adapter deserializes verbatim (never
    // guessed, never dropped — records of an uninstalled adapter stay
    // readable), and canonicalize refuses it: no rule means the selector
    // cannot name a job, an incomplete observation.
    const auto foreign = nlohmann::json{{"scheduler_type", "pbs"},
                                        {"cluster_name", "x"},
                                        {"native_job_id", "1"}}
                             .get<NativeSelector>();
    check(foreign.adapter == "pbs" && foreign.native_job_id == "1",
          "unknown adapter names round trip verbatim");
    check(!canonicalize(foreign).has_value(),
          "unknown adapter names never canonicalize (no rule, no guess)");
}

void run()
{
    condor_namespace_separation();
    internal_id_bounds();
    slurm_steps_and_pid_reuse();
    serialization_round_trip();
}

} // namespace

int main()
{
    try {
        run();
        std::cout << "PASS job identity: Condor/Slurm canonical identities, uint64 "
                     "internal IDs, step normalization, PID-reuse detection, "
                     "serialization round trips\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
