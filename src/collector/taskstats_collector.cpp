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
#include "collector/taskstats_collector.hpp"
#include "core/output_context.hpp"
#include "core/collector_registry.hpp"
#include "common/netlink_utils.hpp"

#include <linux/netlink.h>
#include <linux/taskstats.h>
#include <sys/socket.h>

AUTO_REGISTER_JOB_COLLECTOR(
    TaskstatsCollector, 
    "Collect task statistics using taskstats interface",
    ConfigParams{
        {"freq", "Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds"}
    }
)


bool TaskstatsCollector::init_taskstats() {
    // 初始化 taskstats 相关逻辑
    netlink_fd = NetLinkUtils::create_nl_socket(NETLINK_GENERIC, 4 * 1024);
    if(netlink_fd < 0){
        spdlog::error("NetUsageCollector: netlink init error");
        return false;
    }
    netlink_inited = true;
    return true;
}



bool TaskstatsCollector::init(const nlohmann::json& cfg) {
    spdlog::info("TaskstatsCollector: Initializing with config: {}", cfg.dump());
    return init_taskstats();
}

void TaskstatsCollector::deinit() noexcept {
    if (netlink_inited) {
        close(netlink_fd);
        netlink_inited = false;
    }
    spdlog::info("TaskstatsCollector: Deinitialized");
}



CollectOutcome TaskstatsCollector::collect(const Job& job, OutputContext& output) {
    (void)job;
    (void)output;
    // 采样尚未实现：明确返回 Skipped 且不输出任何记录。
    return {CollectState::Skipped, {}};
}
