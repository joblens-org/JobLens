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
// an inventory citizen.
//
// Everything scheduler-specific lives behind this interface: discovery
// (through IDiscoverySource), legacy-row
// decoding, identity validation and capability traits. Nothing in the store,
// the registry or the RPC surface names a scheduler.
//
// Adding a scheduler type = implement ISchedulerAdapter in a self-contained
// header/TU + one REGISTER_SCHEDULER_ADAPTER line + one inventory.types entry.
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/collector_type.h"
#include "core/job_discovery.hpp"

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

// Resolved settings for one scheduler type, keyed by adapter name.
// `inventory` mirrors job_registry_config.inventory.types membership;
// `auto_add` resolution precedence:
//   explicit <type>_job_watcher.auto_add (off/default/rule)  wins
//   otherwise derived from the legacy pair:
//     auto_collect=false                 -> Off
//     auto_collect=true + use_rules=false -> Default
//     auto_collect=true + use_rules=true  -> Rule
//   with job_registry_config.auto_add_<type>job (legacy) folded into
//   auto_collect first.
struct InventoryTypeSettings {
    bool inventory = false;
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
    std::string rules_prefix;
    // Legacy auto_add_* folding: collectors added to every job of this type
    // when automatic collection is on and use_rules is false.
    std::vector<std::string> default_collectors;
    // Explicit cluster namespace fallback for discovery (wins over the
    // environment-derived value, empty = derive from the source).
    std::string cluster_name;
};

// Capability traits of one adapter. A trait being absent is a normal state,
// never an error: jobs of an adapter without the cgroup trait simply never
// get cgroup-based retirement evidence or kernel attribution.
struct AdapterTraits {
    bool cgroup_scoped_membership = false;  // cgroup.procs is authoritative membership
    bool kernel_attribution = false;        // participates in pid2job/cgroup2job
};

class ISchedulerAdapter {
public:
    virtual ~ISchedulerAdapter() = default;

    // Registry key and config section ("condor" -> "condor_job_watcher").
    virtual const std::string& name() const = 0;
    virtual std::string configSection() const = 0;

    // Discovery: the authoritative passes and targeted hints.
    virtual IDiscoverySource& source() = 0;
    // Per-type discovery options derived from the resolved settings.
    virtual DiscoveryOptions discoveryOptions(const InventoryTypeSettings& settings) const = 0;

    // Legacy (schema < 2) decode: typed facts from a stored JSON row.
    virtual Job decodeFacts(const nlohmann::json& row) const = 0;
    // Best-effort selector reconstruction for a legacy row (no scheduler
    // identity -> incomplete selector, never a guess).
    virtual NativeSelector legacySelector(const Job& job) const = 0;

    // Applies a resolved identity to the typed facts (derives the typed attr
    // fields the schema defines from the canonical form). The store calls it
    // through the registry-injected applier on upsert; it is idempotent for
    // facts a watcher already filled from its parsed sources.
    // Default: nothing to derive — open-world (JSON) facts are kept verbatim.
    virtual void applyIdentityToFacts(Job& facts, const NativeSelector& selector,
                                      const CanonicalIdentity& identity) const
    {
        (void)facts; (void)selector; (void)identity;
    }

    // The execution cgroup of one job as told by its typed facts, used by
    // kernel attribution (empty = none; the kernel_attribution trait gates
    // whether the answer is ever consumed). Replaces subtype dispatch.
    virtual std::string cgroupPathOf(const Job& job) const
    {
        (void)job;
        return {};
    }

    virtual AdapterTraits traits() const = 0;
};

// Self-registration registry (same convention as collectors/writers).
class SchedulerAdapterRegistry {
public:
    using Factory = std::function<std::unique_ptr<ISchedulerAdapter>(
        const InventoryTypeSettings& settings)>;

    static SchedulerAdapterRegistry& instance();

    // Registers (or replaces) the factory for `name`. Replacement keeps the
    // deterministic test harness simple.
    void registerAdapter(const std::string& name, Factory factory);

    bool known(const std::string& name) const;
    std::vector<std::string> names() const;
    // Builds one adapter; nullptr for an unknown name (callers report the
    // registered names instead of guessing).
    std::unique_ptr<ISchedulerAdapter> build(const std::string& name,
                                             const InventoryTypeSettings& settings) const;

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
            NAME, [](const InventoryTypeSettings& settings)                    \
                  -> std::unique_ptr<ISchedulerAdapter> {                      \
                      return std::make_unique<CLASS>(settings);                \
                  });                                                          \
        return true;                                                           \
    }();
