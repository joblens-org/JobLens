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
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <sys/types.h>

namespace permission_opt {

// 返回 true 表示当前进程是 root
inline bool am_i_root() {
    return geteuid() == 0;
}

// 一条 cgroup 记录
struct Entry {
    std::string hierarchy_id;   // 例：0
    std::string subsystems;     // 例：cpu,cpuacct
    std::string group_path;     // 例：/docker/abc
};

// 解析 /proc/self/cgroup
inline std::vector<Entry> my_cgroups() {
    std::vector<Entry> out;
    std::ifstream f("/proc/self/cgroup");
    if (!f) {
        std::cerr << "cannot open /proc/self/cgroup\n";
        return out;
    }
    std::string line;
    while (std::getline(f, line)) {
        std::stringstream ss(line);
        Entry e;
        std::getline(ss, e.hierarchy_id, ':');
        std::getline(ss, e.subsystems, ':');
        std::getline(ss, e.group_path, ':');
        out.push_back(std::move(e));
    }
    return out;
}

// 将当前进程迁移到某个子系统的根 cgroup
// subsys 形如 "memory", "cpu", "pids" ...
// 成功返回 true
inline bool move_to_root_cgroup(const std::string& subsys) {
    std::string tasks_path = "/sys/fs/cgroup/" + subsys + "/cgroup.procs";
    pid_t pid = getpid();
    std::ofstream f(tasks_path);
    if (!f) {
        std::cerr << "cannot open " << tasks_path << " for write\n";
        return false;
    }
    f << pid;
    return true;
}

inline bool check_permission() {
//TODO
    if (am_i_root()) {
        std::cout << "Running as root, permission check skipped.\n";
        return true;
    }

    auto cgroups = my_cgroups();
    if (cgroups.empty()) {
        std::cerr << "No cgroup entries found, permission check failed.\n";
        return false;
    }

    // 检查是否有 cpu 子系统
    for (const auto& entry : cgroups) {
        if (entry.subsystems.find("cpu") != std::string::npos) {
            std::cout << "Permission check passed: CPU subsystem found.\n";
            return true;
        }
    }

    std::cerr << "Permission check failed: No CPU subsystem found.\n";
    return false;
}


} // namespace cgroup