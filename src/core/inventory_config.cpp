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
#include "core/inventory_config.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>

#include "common/utils.hpp"

namespace {

[[noreturn]] void config_error(const std::string& message)
{
    throw std::invalid_argument("inventory configuration: " + message);
}

const nlohmann::json* member(const nlohmann::json& node, const std::string& key)
{
    if (!node.is_object()) return nullptr;
    const auto it = node.find(key);
    if (it == node.end() || it->is_null()) return nullptr;
    return &*it;
}

std::string scalar_text(const nlohmann::json& node, const std::string& path)
{
    if (node.is_string()) return node.get<std::string>();
    if (node.is_boolean()) return node.get<bool>() ? "true" : "false";
    if (node.is_number_integer()) return std::to_string(node.get<long long>());
    if (node.is_number_unsigned()) return std::to_string(node.get<unsigned long long>());
    if (node.is_number_float()) {
        std::ostringstream out;
        out << node.get<double>();
        return out.str();
    }
    config_error(path + " must be a scalar value");
}

bool parse_bool(const nlohmann::json& node, const std::string& path)
{
    if (node.is_boolean()) return node.get<bool>();
    if (node.is_number_integer()) {
        const auto value = node.get<long long>();
        if (value == 0) return false;
        if (value == 1) return true;
        config_error(path + " must be a boolean");
    }
    if (node.is_number_unsigned()) {
        const auto value = node.get<unsigned long long>();
        if (value == 0) return false;
        if (value == 1) return true;
        config_error(path + " must be a boolean");
    }
    const std::string text = scalar_text(node, path);
    if (text == "true" || text == "True" || text == "TRUE" || text == "yes" || text == "1")
        return true;
    if (text == "false" || text == "False" || text == "FALSE" || text == "no" || text == "0")
        return false;
    config_error(path + " must be a boolean (true/false), got '" + text + "'");
}

std::uint32_t parse_interval_sec(const nlohmann::json& node, const std::string& path)
{
    long long value = 0;
    if (node.is_number_unsigned()) {
        const auto raw = node.get<unsigned long long>();
        if (raw > 0xffffffffull) config_error(path + " is out of range");
        value = static_cast<long long>(raw);
    } else if (node.is_number_integer()) {
        value = node.get<long long>();
    } else if (node.is_number_float()) {
        config_error(path + " must be an integer number of seconds");
    } else {
        const std::string text = scalar_text(node, path);
        if (text.empty()) config_error(path + " must be an integer number of seconds");
        const bool digits = std::all_of(text.begin(), text.end(), [](unsigned char c) {
            return std::isdigit(c) != 0;
        });
        if (!digits)
            config_error(path + " must be an integer number of seconds, got '" + text + "'");
        try {
            value = std::stoll(text);
        } catch (const std::out_of_range&) {
            config_error(path + " is out of range");
        }
    }
    if (value < 1) config_error(path + " must be at least 1 second");
    return static_cast<std::uint32_t>(value);
}

std::string scalar_field(const nlohmann::json& node, const char* key, const std::string& path,
                         const std::string& fallback)
{
    const auto* child = member(node, key);
    if (child == nullptr) return fallback;
    return scalar_text(*child, path + "." + key);
}

}  // namespace

nlohmann::json InventoryMaintenanceConfig::to_json() const
{
    nlohmann::json types_json = nlohmann::json::array();
    for (const auto& name : types) types_json.push_back(name);

    nlohmann::json out;
    out["types"] = std::move(types_json);
    out["reconcile_interval_sec"] = reconcile_interval_sec;
    out["registration_batch_size"] = registration_batch_size;
    nlohmann::json sources = nlohmann::json::array();
    for (const auto& entry : settings) {
        sources.push_back(nlohmann::json{
            {"type", entry.first},
            {"inventory", entry.second.inventory},
            {"auto_collect", entry.second.auto_collect},
            {"auto_collect_explicit", entry.second.auto_collect_explicit},
            {"legacy_auto_collect", entry.second.legacy_auto_collect},
            {"use_rules", entry.second.use_rules},
            {"rules_dir", entry.second.rules_dir},
            {"rules_prefix", entry.second.rules_prefix},
            {"default_collectors", entry.second.default_collectors},
            {"cluster_name", entry.second.cluster_name},
        });
    }
    out["sources"] = std::move(sources);
    return out;
}

namespace {

constexpr std::string_view kSectionSuffix = "_job_watcher";

// The legacy auto_add flags predate the adapter model; they stay explicit
// built-in special cases forever (folding semantics, deprecation warnings).
struct LegacyFlag {
    const char* adapter;
    const char* key;
};
constexpr LegacyFlag kLegacyFlags[] = {
    {"condor", "auto_add_condorjob"},
    {"slurm", "auto_add_slurmjob"},
};

std::string section_of(const std::string& adapter)
{
    return adapter + std::string(kSectionSuffix);
}

}  // namespace

InventoryMaintenanceConfig resolve_inventory_maintenance_config(const nlohmann::json& config)
{
    if (!config.is_object())
        config_error("the document root must be a mapping");

    const auto* registry = member(config, "job_registry_config");
    static const nlohmann::json empty_object = nlohmann::json::object();
    const nlohmann::json& registry_node = registry != nullptr ? *registry : empty_object;
    if (!registry_node.is_object())
        config_error("job_registry_config must be a mapping");

    InventoryMaintenanceConfig resolved;

    // Candidate names, open world: every REGISTERED adapter (so the resolved
    // picture is complete and a new scheduler appears without edits), every
    // top-level "<name>_job_watcher" section the configuration carries, and
    // the two legacy auto_add flags.
    std::set<std::string> candidates;
    for (const auto& name : SchedulerAdapterRegistry::instance().names())
        candidates.insert(name);
    for (auto it = config.begin(); it != config.end(); ++it) {
        const std::string& key = it.key();
        if (key.size() <= kSectionSuffix.size() ||
            key.compare(key.size() - kSectionSuffix.size(), kSectionSuffix.size(),
                        kSectionSuffix) != 0)
            continue;
        candidates.insert(key.substr(0, key.size() - kSectionSuffix.size()));
    }
    for (const auto& legacy : kLegacyFlags) {
        if (member(registry_node, legacy.key) != nullptr)
            candidates.insert(legacy.adapter);
    }

    for (const auto& name : candidates)
        resolved.settings.emplace(name, InventoryTypeSettings{});

    // Legacy flags resolve first (per-type precedence input).
    for (const auto& legacy : kLegacyFlags) {
        const auto* node = member(registry_node, legacy.key);
        if (node == nullptr) continue;
        resolved.settings.at(legacy.adapter).legacy_auto_collect =
            parse_bool(*node, std::string("job_registry_config.") + legacy.key);
    }

    const auto* inventory = member(registry_node, "inventory");
    if (inventory != nullptr) {
        if (!inventory->is_object())
            config_error("job_registry_config.inventory must be a mapping");

        if (const auto* types = member(*inventory, "types")) {
            if (!types->is_array())
                config_error("job_registry_config.inventory.types must be a list");
            for (const auto& entry : *types) {
                const std::string raw = scalar_text(
                    entry, "job_registry_config.inventory.types[]");
                const std::string name = normalize_adapter_name(raw);
                if (name.empty() || name == "unknown")
                    config_error("unknown inventory type '" + raw + "'");
                if (!SchedulerAdapterRegistry::instance().known(name)) {
                    config_error("unknown inventory type '" + raw + "' (registered: " +
                                 [&] {
                                     std::string joined;
                                     for (const auto& known :
                                          SchedulerAdapterRegistry::instance().names())
                                         joined += (joined.empty() ? "" : ", ") + known;
                                     return joined;
                                 }() +
                                 ")");
                }
                if (std::find(resolved.types.begin(), resolved.types.end(), name) ==
                    resolved.types.end()) {
                    resolved.types.push_back(name);
                }
            }
        }

        if (const auto* interval = member(*inventory, "reconcile_interval_sec")) {
            resolved.reconcile_interval_sec = parse_interval_sec(
                *interval, "job_registry_config.inventory.reconcile_interval_sec");
        }
        if (const auto* batch = member(*inventory, "registration_batch_size")) {
            // An illegal batch size is a configuration error, never a reason to
            // silently disable inventory maintenance.
            long long value = 0;
            if (batch->is_number_integer() || batch->is_number_unsigned()) {
                value = batch->get<long long>();
            } else if (batch->is_string()) {
                try {
                    value = std::stoll(batch->get<std::string>());
                } catch (const std::exception&) {
                    throw std::runtime_error(
                        "job_registry_config.inventory.registration_batch_size must be an "
                        "integer between 1 and 32");
                }
            } else {
                throw std::runtime_error(
                    "job_registry_config.inventory.registration_batch_size must be an "
                    "integer between 1 and 32");
            }
            if (value < 1 || value > 32) {
                throw std::runtime_error(
                    "job_registry_config.inventory.registration_batch_size must be between "
                    "1 and 32, got " + std::to_string(value));
            }
            resolved.registration_batch_size = static_cast<std::size_t>(value);
        }
        if (member(*inventory, "auto_policy_budget_us") != nullptr) {
            spdlog::warn("inventory: auto_policy_budget_us was removed; the initial policy "
                         "decision is always evaluated synchronously");
        }
    }

    const std::string legacy_rules_dir =
        scalar_field(registry_node, "rules_dir", "job_registry_config", std::string());

    for (const auto& name : resolved.types) {
        resolved.settings[name].inventory = true;
    }

    // Per-adapter sections resolve by convention: <name>_job_watcher. The
    // defaults follow the same convention (prefix <name>_job_, rules under
    // config/rules/<name>_jobs), so a new scheduler needs no resolver edit.
    for (auto& [name, settings] : resolved.settings) {
        const std::string section = section_of(name);
        const std::string prefix = section + ".";
        const auto* node = member(config, section);
        if (node != nullptr && !node->is_object())
            config_error(section + " must be a mapping");
        if (node != nullptr && !SchedulerAdapterRegistry::instance().known(name))
            spdlog::warn("inventory configuration: section '{}' present but no adapter "
                         "'{}' is registered; the settings stay inert",
                         section, name);

        settings.rules_prefix = name + "_job_";
        settings.rules_dir = Utils::JobLensRootDir() + "../config/rules/" + name + "_jobs";
        if (node != nullptr) {
            if (const auto* auto_collect = member(*node, "auto_collect")) {
                settings.auto_collect_explicit = true;
                settings.auto_collect = parse_bool(*auto_collect, prefix + "auto_collect");
            }
            if (const auto* use_rules = member(*node, "use_rules")) {
                settings.use_rules = parse_bool(*use_rules, prefix + "use_rules");
            }
            if (const auto* defaults = member(*node, "auto_add_collectors")) {
                if (!defaults->is_array())
                    config_error(prefix + "auto_add_collectors must be a list");
                for (const auto& item : *defaults) {
                    settings.default_collectors.push_back(
                        scalar_text(item, prefix + "auto_add_collectors[]"));
                }
            }
            settings.cluster_name =
                scalar_field(*node, "cluster_name", section, std::string{});
            settings.rules_prefix = scalar_field(*node, "rules_prefix", section,
                                                 settings.rules_prefix);
            // Per-type rules_dir wins over the legacy shared one; otherwise
            // the per-type convention default stays in place (plan section 8).
            settings.rules_dir = scalar_field(
                *node, "rules_dir", section,
                legacy_rules_dir.empty() ? settings.rules_dir : legacy_rules_dir);
        } else if (!legacy_rules_dir.empty()) {
            settings.rules_dir = legacy_rules_dir;
        }
        if (!settings.auto_collect_explicit)
            settings.auto_collect = settings.legacy_auto_collect;
    }

    if (!legacy_rules_dir.empty()) {
        spdlog::warn("inventory configuration: job_registry_config.rules_dir is deprecated; "
                     "set <adapter>_job_watcher.rules_dir instead");
    }

    // Legacy auto-add folding (user-defined semantics): auto_add_<type>job=true
    // means "inventory this type and add collectors to every discovered job";
    // use_rules only decides whether the collector list comes from the rule
    // engine (accept = rule result, reject = empty) or verbatim from
    // auto_add_collectors. An explicit per-type auto_collect or an explicit
    // inventory.types entry always wins over the legacy flag.
    for (const auto& legacy : kLegacyFlags) {
        auto& settings = resolved.settings.at(legacy.adapter);
        const bool explicit_type =
            std::find(resolved.types.begin(), resolved.types.end(),
                      legacy.adapter) != resolved.types.end();
        if (!settings.legacy_auto_collect || explicit_type || settings.auto_collect_explicit)
            continue;
        resolved.types.push_back(legacy.adapter);
        settings.inventory = true;
        settings.auto_collect = true;
        spdlog::warn("inventory configuration: job_registry_config.{} is folded "
                     "into inventory discovery with automatic collection; set {}.auto_collect "
                     "explicitly to override",
                     legacy.key, section_of(legacy.adapter));
    }
    // One global knob, applied to every adapter's source: the batch size is a
    // per-source poll-turn bound, not a per-scheduler policy.
    for (auto& [name, settings] : resolved.settings) {
        (void)name;
        settings.registration_batch_size = resolved.registration_batch_size;
    }
    return resolved;
}
