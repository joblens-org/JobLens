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
/* Registry lifecycle acceptance (plan section 5.1 / 5.3):
 *
 *   * construction is the single assembly point: the store opens, the
 *     inventory maintenance controller starts (providers published) even with
 *     no configured discovery type;
 *   * stopInventoryDiscovery() is idempotent and keeps the status provider
 *     published while records drain;
 *   * deinit() finalizes retirements, closes the store, retracts the
 *     providers and is idempotent; the destructor only backstops it.
 */
#include "common/config.hpp"
#include "core/inventory_status_provider.hpp"
#include "core/job_registry.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

void pass_case(const char* id, const char* description)
{
    std::cout << "PASS " << id << " " << description << '\n' << std::flush;
}

std::string fresh_dir(const std::string& base, const std::string& name)
{
    const auto path = std::filesystem::path(base) / name;
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path.string();
}

Job make_common_job()
{
    Job job;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    job.JobPIDs = {getpid()};
    job.CollectorNames = {"cpumem_collector"};
    CommonJobAttr attr;
    attr.auto_update_child = false;
    job.sub_attr = attr;
    return job;
}

void run(const std::string& base)
{
    // A minimal configuration: no inventory types, so the maintenance
    // controller starts with zero discovery sources.
    const auto config_path = std::filesystem::path(base) / "config.yaml";
    std::filesystem::create_directories(base);
    {
        std::ofstream config(config_path);
        config << "job_registry_config:\n"
                  "  job_db_path: " << base << "/cfg.db\n";
    }
    Config::instance(config_path.string());

    // Construction with an empty inventory configuration: no watchers, but the
    // maintenance controller still starts and publishes its providers.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-empty");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        check(static_cast<bool>(get_inventory_status_provider()),
              "construction must publish the inventory status provider");
        check(static_cast<bool>(get_inventory_refresh_provider()),
              "construction must publish the on-demand refresh provider");
        check(registry.storeHealth().db_running,
              "construction must leave the store running");

        // stopInventoryDiscovery is idempotent and keeps the status visible
        // while records drain (retraction belongs to finalize).
        registry.stopInventoryDiscovery();
        registry.stopInventoryDiscovery();
        check(static_cast<bool>(get_inventory_status_provider()),
              "the status provider must stay published while draining");

        check(registry.deinit(), "deinit must succeed");
        check(!registry.storeHealth().db_running, "deinit must close the store");
        check(!static_cast<bool>(get_inventory_status_provider()),
              "deinit must retract the status provider");
        check(!static_cast<bool>(get_inventory_refresh_provider()),
              "deinit must retract the refresh provider");
        check(registry.deinit(), "a second deinit must be a no-op that keeps the result");
    }
    // The providers of the previous registry are retracted by its deinit.
    check(!static_cast<bool>(get_inventory_status_provider()),
          "the previous registry must not leave providers installed");

    // A closing record is archived by deinit; the destructor does not need to.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-archive");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        const auto job = make_common_job();
        check(registry.addJob(job), "legacy registration must succeed");
        const uint64_t id = registry.inventoryRecords().front().internal_id;
        check(registry.retireInstance(id), "retire the instance");
        check(!registry.inventoryRecord(id).has_value(),
              "retirement without subscribers archives immediately");

        check(registry.deinit(), "deinit after retirement must succeed");
    }

    pass_case("LIFECYCLE",
              "construction starts inventory, stop/deinit are idempotent and ordered");
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string base =
        argc > 1 ? std::string(argv[1]) : std::string("/tmp/opencode/joblens-lifecycle-test");
    std::filesystem::create_directories(base);
    try {
        run(base + "/data");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAILED: " << error.what() << '\n';
        return 1;
    }
}
