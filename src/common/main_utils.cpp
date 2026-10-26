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
#include "common/main_utils.hpp"
#include "common/config.hpp"
#include "common/print_fmt.hpp"
#include "core/collector_registry.hpp"
#include "core/writer_manager.hpp"
#include "core/job_registry.hpp"
#include "core/collector_scheduler.hpp"
#include "core/job_inventory_maintenance.hpp"
#include "common/permission_opt.hpp"
#include "common/local_rpc.hpp"
#include "common/version.h"

#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <fmt/ranges.h>
#include <signal.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <condition_variable>

namespace MainUtils {

std::pair<int, std::vector<char*>>
makeArgs(const std::vector<std::string>& v)
{
    std::vector<char*> ptr;
    ptr.reserve(v.size());
    for (auto& s : const_cast<std::vector<std::string>&>(v)) ptr.push_back(&s[0]);
    return { static_cast<int>(ptr.size()), std::move(ptr) };
}

void print_logo() {
    std::cout<< R"(    _____          __       __                                 
   |     \        |  \     |  \
    \$$$$$ ______ | $$____ | $$      ______  _______   _______ 
      | $$/      \| $$    \| $$     /      \|       \ /       \
 __   | $|  $$$$$$| $$$$$$$| $$    |  $$$$$$| $$$$$$$|  $$$$$$$
|  \  | $| $$  | $| $$  | $| $$    | $$    $| $$  | $$\$$    \
| $$__| $| $$__/ $| $$__/ $| $$____| $$$$$$$| $$  | $$_\$$$$$$\
 \$$    $$\$$    $| $$    $| $$     \$$     | $$  | $|       $$
  \$$$$$$  \$$$$$$ \$$$$$$$ \$$$$$$$\$$$$$$$\$$   \$$\$$$$$$$ 
                                                               
                                                               
                                                               )" << std::endl;
}

void init(const std::string& config_path) {
    print_logo();
    
    // 首先初始化配置
    Config::instance(config_path);
    
    // 初始化日志系统
    const static auto log_level_map = std::map<std::string, spdlog::level::level_enum>{
        {"trace", spdlog::level::trace},
        {"debug", spdlog::level::debug},
        {"info", spdlog::level::info},
        {"warn", spdlog::level::warn},
        {"error", spdlog::level::err},
        {"critical", spdlog::level::critical},
        {"off", spdlog::level::off}
    };
    auto log_level = Config::instance().getString("lens_config", "log_level");
    auto it = log_level_map.find(log_level);
    if (it == log_level_map.end()) {
        log_level = "info"; // 默认 info 级别
    }
    auto level_enum = log_level_map.at(log_level);
    spdlog::set_level(level_enum); // 设置日志级别
    // spdlog::set_pattern("[%Y-%m-%d %H:%M:%S] [%l] %v"); // 设置日志格式

    // 打印已注册的采集器和writer
    auto collector_list = CollectorRegistry::instance().list();
    spdlog::debug("Main: Registered collectors: {}", fmt::join(collector_list, ", "));
    auto writer_list = WriterManager::instance().list();
    spdlog::debug("Main: Registered writers: {}", fmt::join(writer_list, ", "));

    // 注册本地RPC方法，并且第一次初始化全局实例
    auto RPC_Socket = Config::instance().getString("lens_config", "rpc_socket_path");
    if (RPC_Socket.empty()) {
        spdlog::error("Main: rpc_socket_path is empty in config");
        throw std::runtime_error("Main: rpc_socket_path is empty in config");
    }
    RPCServer::instance(RPC_Socket).start();
    
    // 注册Config的RPC方法
    Config::instance().registerRPCMethods();
}

bool already_running() {
    auto PIDFILE = Config::instance().getString("lens_config", "lock_path");
    auto dir = std::filesystem::path(PIDFILE).parent_path();
    if (!dir.empty() && !std::filesystem::exists(dir)) {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        if (ec) {
            spdlog::error("Main: already_running: failed to create dir {}: {}", dir.c_str(), ec.message());
            throw std::runtime_error("create_directories");
        }
        spdlog::info("Main: already_running: created directory {}", dir.c_str());
    }
    std::ifstream ifs(PIDFILE);
    if (ifs) {
        pid_t oldpid;
        ifs >> oldpid;
        if (oldpid > 0 && kill(oldpid, 0) == 0)   // 0 信号仅检测
            return true;                          // 同名进程存活
    }

    /* 把当前 pid 写进去 */
    std::ofstream ofs(PIDFILE);
    if (!ofs) {
        std::cerr << "cannot create " << PIDFILE << "\n";
        return false;                             // 保守起见，允许启动
    }
    ofs << getpid() << std::endl;
    return false;                                 // 可以继续跑
}

void onBecomeService() {
    spdlog::info("Main: app start as service mode.");
    // Plan 5.1: inventory start is explicit and precedes the scheduler, so an
    // inventory-only deployment never depends on CollectorScheduler.
    if (!inventory_service::start()) {
        spdlog::error("Main: inventory maintenance is unavailable; continuing without it");
    }
    CollectorScheduler::instance().start();
}

int version() {
    std::cout << "v" << PROJ_VERSION
              << " build-" << PROJ_BUILD_ID
              << " (" << PROJ_BUILD_TIME << ")\n";
    return 0;
}

} // namespace MainUtils

namespace inventory_service {

namespace {

// Process-lifetime owners for the inventory stack. This is explicit lifecycle
// state owned by MainUtils, not a new singleton: the controller must be
// destructible independently of the scheduler and keeps the discovery adapters
// alive for as long as its source closures reference them.
struct InventoryRuntime {
    std::unique_ptr<CondorJobWatcher> condor;
    std::unique_ptr<SlurmJobWatcher> slurm;
    std::unique_ptr<JobInventoryMaintenance> maintenance;
};

InventoryRuntime& runtime()
{
    static InventoryRuntime state;
    return state;
}

namespace {

// Splits the resolved per-type settings into the adapter's DiscoveryOptions.
// The maintenance controller owns pacing and the automatic policy, so the
// adapter must not read Config and must not poll on its own. The explicit
// cluster_name (when set) is the namespace fallback that wins over
// environment-derived values.
DiscoveryOptions discovery_options_for(const InventoryTypeSettings& settings,
                                       SchedulerType type)
{
    DiscoveryOptions options;
    options.enable_event_source = true;
    options.scan_interval_ms = 0;
    if (!settings.cluster_name.empty()) {
        if (type == SchedulerType::Condor)
            options.discovery.condor_cluster_name = settings.cluster_name;
        else
            options.discovery.slurm_cluster_name = settings.cluster_name;
    }
    return options;
}

}  // namespace

}  // namespace

bool start()
{
    // Construct the registry (and its store) before the runtime holder so static
    // destruction tears the maintenance controller down while the store is still
    // alive: the controller may need to flush a closing record.
    JobRegistry& registry = JobRegistry::instance();
    auto& state = runtime();
    if (state.maintenance) return true;

    // Section 8: validate at startup and reject malformed values instead of
    // guessing. This throws std::invalid_argument, which main() reports.
    const InventoryMaintenanceConfig config =
        resolve_inventory_maintenance_config(Config::instance().dumpConfig());

    std::vector<IDiscoverySource*> sources;
    for (const auto type : config.types) {
        const auto& settings = config.settings.at(type);
        if (type == InventoryType::Condor) {
            state.condor = std::make_unique<CondorJobWatcher>(
                discovery_options_for(settings, SchedulerType::Condor));
            sources.push_back(state.condor.get());
        } else {
            state.slurm = std::make_unique<SlurmJobWatcher>(
                discovery_options_for(settings, SchedulerType::Slurm));
            sources.push_back(state.slurm.get());
        }
    }

    if (config.types.empty()) {
        spdlog::info("InventoryMaintenance: job_registry_config.inventory.types is empty; "
                     "broad discovery is disabled, existing records are still maintained");
    }

    // The controller runs even without discovery sources: legacy/manual records
    // and records of a disabled type still need fact refresh and retirement
    // (plan section 8: removing a type must not strand persisted live records).
    state.maintenance = std::make_unique<JobInventoryMaintenance>(
        registry, config, std::move(sources));
    if (!state.maintenance->start()) {
        spdlog::error("InventoryMaintenance: inventory maintenance did not start; "
                      "inventory status will report it as unavailable");
        state.maintenance.reset();
        return false;
    }
    spdlog::info("InventoryMaintenance: inventory maintenance running with {} discovery type(s)",
                 config.types.size());
    return true;
}

void stop_discovery()
{
    if (runtime().maintenance) runtime().maintenance->stopDiscovery();
}

bool finalize()
{
    if (!runtime().maintenance) return true;
    return runtime().maintenance->finalize();
}

}  // namespace inventory_service
