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
// ISchedulerAdapter — the extension point that turns one scheduler type into
// a registry citizen.
//
// Everything scheduler-specific lives behind this interface: discovery
// (through IDiscoverySource), legacy-row
// decoding, identity validation and capability traits. Nothing in the store,
// the registry or the RPC surface names a scheduler.
//
// Adding a scheduler type = declare its facts and its projections in a
// self-contained adapter header/TU, add one line to the closed-variant
// manifest in collector_type.h and one REGISTER_SCHEDULER_ADAPTER line. The
// adapter then exists for jobs from every source; enabling its discovery
// watcher is a separate deployment choice
// (job_registry_config.auto_discovery_types).
#pragma once

#include <sys/types.h>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/job_identity.hpp"
#include "core/liveness.hpp"

// The adapter contract is declared before the Job aggregate exists: scheduler
// facts are part of Job, and their declarations live in the adapters. Only
// references and by-value declarations appear here; every definition lives in
// the adapter translation unit, after collector_type.h.
struct Job;
struct SubAttrMatcher;
struct DiscoveryOptions;
class IDiscoverySource;

// Tri-state automatic-addition policy for one scheduler type (the resolved
// form of the auto_collect/use_rules pair and the legacy auto_add_<type>job
// flags). Off is the default: the automatic path takes no action for the
// type. Default registers discovered jobs and arms the configured default
// collector list. Rule registers them and lets the Lua rule engine decide.
enum class AutoAdd {
    Off,
    Default,
    Rule,
};

// Resolved settings for one scheduler adapter, keyed by adapter name. They
// exist for every registered adapter, whether or not its discovery source is
// armed: an adapter answers for jobs from any source (discovery, manual
// registration, restored rows).
// `auto_discovery_enabled` mirrors job_registry_config.auto_discovery_types
// membership: it arms the discovery watcher only, never the adapter itself.
// `auto_add` resolution precedence:
//   explicit <type>_job_watcher.auto_add (off/default/rule)  wins
//   otherwise derived from the legacy pair:
//     auto_collect=false                 -> Off
//     auto_collect=true + use_rules=false -> Default
//     auto_collect=true + use_rules=true  -> Rule
//   with job_registry_config.auto_add_<type>job (legacy) folded into
//   auto_collect first.
struct SchedulerAdapterSettings {
    bool auto_discovery_enabled = false;
    // DEPRECATED as an input contract: auto_collect/use_rules (and the
    // explicit/legacy provenance flags below) remain parsed and tracked as
    // the mapping sources for one more schema version; consumers read the
    // resolved `auto_add` instead.
    bool auto_collect = false;
    bool auto_collect_explicit = false;
    bool legacy_auto_collect = false;
    bool use_rules = false;
    AutoAdd auto_add = AutoAdd::Off;
    bool auto_add_explicit = false;
    std::string rules_dir;
    // Derived, never configured: the rule file prefix is the adapter name, so
    // a shared rules directory cannot feed one type's rules into another.
    std::string rules_prefix;
    // Legacy auto_add_* folding: collectors added to every job of this type
    // when automatic collection is on and use_rules is false.
    std::vector<std::string> default_collectors;
    // Explicit cluster namespace fallback for discovery (wins over the
    // environment-derived value, empty = derive from the source).
    std::string clusterName;
};

// Capability traits of one adapter. A trait being absent is a normal state,
// never an error: jobs of an adapter without the cgroup trait simply never
// get cgroup-based removal evidence or kernel attribution.
struct AdapterTraits {
    bool cgroup_scoped_membership = false;  // cgroup.procs is authoritative membership
    bool kernel_attribution = false;        // participates in pid2job/cgroup2job
};

// Structured namespace facts one adapter discriminates by: one value per
// component, empty = unknown. Which typed sub_attr fields feed a component,
// how request-level aliases fold into them and how the query-side common
// fields (clusterName/clusterTag) render from them are adapter implementation
// details — the virtuals below own every bit of that mapping.
using NamespaceFacts = std::vector<std::string>;

// What one fact-refresh read saw, in the terms the removal decision uses.
// The registry owns the decision; the adapter only reports evidence:
//   * manager                — liveness of the execution manager (Unknown is
//                              never death evidence);
//   * cgroup_empty_confirmed — the job cgroup was read and listed no process
//                              (positive evidence, not missing data);
//   * any_pid_identity_live        — at least one member process answered Alive;
//   * pid_identity_inconclusive    — at least one member could not be judged.
struct RefreshResult {
    LivenessState manager = LivenessState::Unknown;
    bool cgroup_empty_confirmed = false;
    bool any_pid_identity_live = false;
    bool pid_identity_inconclusive = false;
};

// Component-wise compatibility: a component where both sides hold a known,
// different value conflicts; an unknown component (either side empty) proves
// neither equality nor contradiction. Different shapes never match.
bool namespace_facts_compatible(const NamespaceFacts& left, const NamespaceFacts& right);

class ISchedulerAdapter {
public:
    virtual ~ISchedulerAdapter() = default;

    // Registry key and config section ("condor" -> "condor_job_watcher").
    virtual const std::string& name() const = 0;
    virtual std::string configSection() const = 0;

    // Discovery: the authoritative passes and targeted hints.
    virtual IDiscoverySource& source() = 0;
    // Per-type discovery options derived from the resolved settings.
    virtual DiscoveryOptions discoveryOptions(const SchedulerAdapterSettings& settings) const = 0;

    // Legacy (schema < 2) decode: typed facts from a stored JSON row. The
    // adapter owns the whole record projection: common fields, pids, typed
    // facts and the native-id/namespace render.
    virtual Job decodeFacts(const nlohmann::json& row) const = 0;
    // Best-effort selector reconstruction for a legacy row (no scheduler
    // identity -> incomplete selector, never a guess). The adapter also folds
    // the derived common fields back onto the Job (fill-empty only), so the
    // record projection is produced in one place.
    virtual NativeSelector legacySelector(const Job& job) const = 0;

    // Publishes the record-visible fields from the typed facts: subtype,
    // subtype_name, JobNativeID and the common namespace fields. Fill-empty
    // only; a watcher-provided value is never overwritten.
    virtual void projectFacts(Job& job) const
    {
        (void)job;
    }

    // The selectors a manual registration request is resolved by, derived
    // from the typed facts the adapter just parsed. Empty = no scheduler
    // identity (generic/manual registration).
    virtual NativeSelector registrationSelector(const Job& job) const
    {
        (void)job;
        return {};
    }

    // Parses the scheduler-specific part of a manual registration request
    // into the typed facts and their record projection. The caller owns the
    // common request fields (type, JobID, pids, collectors).
    virtual void applyRequestFacts(Job& job, const nlohmann::json& request) const
    {
        (void)job; (void)request;
    }

    // Registration-time fact completion: derives the execution-scope facts the
    // request did not carry (the execution cgroup and its supervisor pid) from
    // the process evidence the job already names. Fill-empty only — a fact the
    // request or a discovery watcher supplied always wins. The registry calls
    // this on the manual create path before the record is built, so the first
    // kernel activation already has a cgroup mapping to seed and the periodic
    // refresh expands the whole execution scope instead of the named pids.
    virtual void deriveExecutionFacts(Job& job) const
    {
        (void)job;
    }

    // Fill-empty counterpart of deriveExecutionFacts for the periodic refresh:
    // completes an adapter-owned execution fact the RECORD never carried (for
    // example a cgroup derived after registration, when the member pid was not
    // readable yet) from the refreshed image. Only empty slots are written;
    // identity, namespace and every existing value stay frozen. Returns true
    // when a fact was completed, so the caller persists and re-reports it.
    virtual bool completeRefreshedFacts(Job& target, const Job& refreshed) const
    {
        (void)target; (void)refreshed;
        return false;
    }

    // The typed-facts duplicate screen. Default: no typed fields; callers
    // still get the subtype the adapter was registered under.
    virtual SubAttrMatcher duplicateMatcher(const Job& job) const;

    // ---- per-record fact refresh -------------------------------------------
    //
    // The framework supervisor that launched this job process: Condor's
    // condor_starter, Slurm's slurmstepd. nullopt = not a job process of this
    // adapter (or its identity cannot be read), never a guess.
    virtual std::optional<ProcessIdentity> managerOfPid(pid_t job_pid) const
    {
        (void)job_pid;
        return std::nullopt;
    }

    // The single execution-evidence reader for one record: refreshes the
    // execution group (manager, pid identity triples, pid_identities_authoritative,
    // read_order) IN PLACE and reports what the caller's probes saw. The
    // record's scheduler metadata is the creation snapshot and is never
    // rewritten here. The registry calls this for every active record,
    // whatever its FactSource; the probes are passed in because tests inject
    // fakes and production builds the `/proc` ones. No persistence, no registry
    // lock, no registry state. read_order is assigned before the first read (an
    // observation that reads later keeps its own, newer order).
    virtual RefreshResult refreshFacts(Job& job,
                                       const ProcessLivenessProbe& process_probe,
                                       const CgroupMembershipProbe& cgroup_probe) const = 0;

    // The execution cgroup of one job as told by its typed facts, used by
    // kernel attribution (empty = none; the kernel_attribution trait gates
    // whether the answer is ever consumed). Replaces subtype dispatch.
    virtual std::string cgroupPathOf(const Job& job) const
    {
        (void)job;
        return {};
    }

    // ---- structured namespace (scheduler-owned semantics) -----------------
    //
    // The typed sub_attr is the single fact source. The common fields
    // clusterName/clusterTag are a rendered projection of those facts —
    // the ES job_info contract the query side filters on — never an
    // independent second source. Defaults implement the generic business
    // (Common jobs, open-world schedulers without a configured adapter):
    // the common fields ARE the storage, so extract reads them, adoption
    // has nothing to fold and rendering is a no-op.

    // The namespace facts of one job of this adapter, from its typed facts.
    virtual NamespaceFacts namespaceFacts(const Job& job) const;
    // The namespace facts of a selector addressing jobs of this adapter.
    virtual NamespaceFacts selectorNamespaceFacts(const NativeSelector& selector) const
    {
        return {selector.clusterName, selector.clusterTag};
    }
    // Fold request-level namespace aliases into the typed facts; existing
    // facts win. False = the aliases contradict known facts (the caller
    // rejects the request instead of guessing which side is right).
    virtual bool adoptNamespaceAliases(Job& job, const std::string& clusterName,
                                       const std::string& clusterTag) const
    {
        (void)job; (void)clusterName; (void)clusterTag;
        return true;
    }
    // Render the query-side common fields from the typed facts. Idempotent
    // for facts a watcher already rendered.
    virtual void renderNamespace(Job& job) const
    {
        (void)job;
    }

    // The record's derived logical identity. The adapter owns the native-id
    // rule, so it also owns how its facts become a key; nullopt means the
    // facts do not name a job.
    virtual std::optional<SchedulerKey> keyOf(const Job& job) const
    {
        return canonicalize(legacySelector(job));
    }

    virtual AdapterTraits traits() const = 0;

protected:
    // Liveness evidence for one job from the caller's probes, applying the
    // same rule the removal pass uses: manager liveness from its complete
    // identity, "cgroup empty" only from a readable job cgroup, member
    // liveness/inconclusiveness from the cgroup members and the recorded
    // pid identities. Shared by the adapters' refreshFacts.
    RefreshResult probeExecutionEvidence(const Job& job,
                                         const ProcessLivenessProbe& process_probe,
                                         const CgroupMembershipProbe& cgroup_probe) const;

    // The pid of the first recorded pid identity (0 when the record carries
    // none): the anchor a targeted per-job read starts from.
    static pid_t firstPid(const Job& job);

    // Replaces the record's pid identity evidence with one membership read.
    // `authoritative` means the read covered the whole execution scope, so an
    // empty list really clears the pid identities; a partial read that found
    // nothing is no evidence at all and keeps the recorded identities.
    static void applyPidIdentityEvidence(Job& job, const std::string& proc_root,
                                    const std::vector<pid_t>& member_pids,
                                    bool authoritative);

    // The adapter-independent fallback of refreshFacts: reads one execution's
    // membership from the manager's own scope (the recorded cgroup, else
    // /proc/<manager>/cgroup, else the manager's children — never the process
    // table), applies it as pid identity evidence and reports what the caller's
    // probes saw. Only `execution` is written: the record's creation metadata
    // stays the snapshot it was created with.
    RefreshResult refreshPidIdentities(Job& job, const std::string& proc_root,
                                           const std::string& cgroup_root,
                                           const std::string& recorded_cgroup_path,
                                           const ProcessLivenessProbe& process_probe,
                                           const CgroupMembershipProbe& cgroup_probe) const;
};

// Self-registration registry (same convention as collectors/writers).
class SchedulerAdapterRegistry {
public:
    using Factory = std::function<std::unique_ptr<ISchedulerAdapter>(
        const SchedulerAdapterSettings& settings)>;

    static SchedulerAdapterRegistry& instance();

    // Registers (or replaces) the factory for `name`. Replacement keeps the
    // deterministic test harness simple.
    void registerAdapter(const std::string& name, Factory factory);

    bool known(const std::string& name) const;
    std::vector<std::string> names() const;
    // Builds one adapter; nullptr for an unknown name (callers report the
    // registered names instead of guessing).
    std::unique_ptr<ISchedulerAdapter> build(const std::string& name,
                                             const SchedulerAdapterSettings& settings) const;

private:
    mutable std::mutex mutex_;
    std::map<std::string, Factory> factories_;
};

// Registers an adapter factory at TU scope (dynamic init), mirroring
// AUTO_REGISTER_COLLECTOR. Example:
//   REGISTER_SCHEDULER_ADAPTER("condor", CondorSchedulerAdapter)
#define REGISTER_SCHEDULER_ADAPTER(NAME, CLASS)                                \
    static bool joblens_adapter_reg_##CLASS = [] {                             \
        SchedulerAdapterRegistry::instance().registerAdapter(                  \
            NAME, [](const SchedulerAdapterSettings& settings)                    \
                  -> std::unique_ptr<ISchedulerAdapter> {                      \
                      return std::make_unique<CLASS>(settings);                \
                  });                                                          \
        return true;                                                           \
    }();
