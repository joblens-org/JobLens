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

#include <vector>
#include <string>
#include <fstream>
#include <dirent.h>
#include <sys/types.h>
#include <sstream>
#include <iostream>
#include <signal.h>
#include <unordered_set>
#include <spdlog/spdlog.h>
#include <fmt/ranges.h>

#include "core/collector_type.h"

namespace CommonJob
{
    
/**
 * @brief 获取指定进程的直接子进程列表
 * @param pid 父进程ID
 * @return 子进程ID列表（仅直接子进程）
 */
inline std::vector<pid_t> get_child_processes(pid_t pid) {
    std::vector<pid_t> children;
    if (pid <= 0) return children;

    // 方法1：优先使用内核提供的 children 文件（Linux 3.5+，高效）
    std::string children_path = "/proc/" + std::to_string(pid) + "/task/" + std::to_string(pid) + "/children";
    std::ifstream fin(children_path);
    if (fin) {
        pid_t cpid;
        while (fin >> cpid) children.push_back(cpid);
        return children;
    }

    // 方法2：扫描 /proc（兼容旧内核）
    DIR* d = opendir("/proc");
    if (!d) {
        spdlog::error("Failed to open /proc");
        return children;
    }

    struct dirent* de;
    while ((de = readdir(d)) != nullptr) {
        std::string name = de->d_name;
        // 仅处理数字目录（进程目录）
        if (!std::all_of(name.begin(), name.end(), ::isdigit)) continue;

        pid_t other_pid = static_cast<pid_t>(std::stoi(name));
        std::string stat_path = "/proc/" + name + "/stat";
        std::ifstream stat_file(stat_path);
        if (!stat_file) continue; // 进程已退出或无权访问

        std::string line;
        if (!std::getline(stat_file, line)) continue;

        // 解析 stat 文件：pid (comm) state ppid ...
        auto rparen_pos = line.rfind(')');
        if (rparen_pos == std::string::npos) continue;

        std::istringstream iss(line.substr(rparen_pos + 1));
        std::string state;
        pid_t ppid;
        if (!(iss >> state >> ppid)) continue;

        if (ppid == pid) children.push_back(other_pid);
    }
    closedir(d);
    return children;
}

/**
 * @brief 检查进程是否真实存在（排除僵尸进程）
 * @param pid 进程ID
 * @return true 如果进程存在且不是僵尸
 */
inline bool is_process_alive(pid_t pid) {
    if (pid <= 0) return false;
    
    // 检查进程是否存在
    if (kill(pid, 0) != 0) {
        return errno != ESRCH; // ESRCH=进程不存在，EPERM=存在但无权限
    }

    // 进一步检查是否为僵尸进程（可选，但建议）
    std::string stat_path = "/proc/" + std::to_string(pid) + "/stat";
    std::ifstream stat_file(stat_path);
    if (stat_file) {
        std::string line;
        if (std::getline(stat_file, line)) {
            auto rparen_pos = line.rfind(')');
            if (rparen_pos != std::string::npos) {
                std::istringstream iss(line.substr(rparen_pos + 1));
                std::string state;
                if (iss >> state) {
                    return state != "Z" && state != "z"; // 僵尸状态
                }
            }
        }
    }
    return true; // 默认认为存活
}

/**
 * @brief 收集PID列表的完整进程树（所有子孙）
 * @param seed_pids 初始PID列表（种子进程）
 * @return 完整的进程树（包含原始PID），自动去重
 */
inline std::vector<pid_t> collect_process_tree(const std::vector<pid_t>& seed_pids) {
    std::vector<pid_t> result;
    if (seed_pids.empty()) return result;

    std::vector<pid_t> queue = seed_pids;
    std::unordered_set<pid_t> seen;

    // 初始化：去重并验证种子进程
    for (pid_t pid : seed_pids) {
        if (pid > 0 && seen.insert(pid).second && is_process_alive(pid)) {
            result.push_back(pid); // 保持原始PID在前
        }
    }

    size_t idx = 0;
    while (idx < queue.size()) {
        pid_t cur = queue[idx++];
        
        // 获取子进程（包含已退出的，需后续过滤）
        for (pid_t child : get_child_processes(cur)) {
            if (child <= 0 || seen.find(child) != seen.end()) continue;

            // 关键修复：严格验证子进程存活
            if (!is_process_alive(child)) continue; // 已退出或僵尸进程，跳过

            seen.insert(child);
            result.push_back(child);
            queue.push_back(child); // BFS继续遍历
        }
    }

    return result;
}

/**
 * @brief 更新作业的进程列表为完整进程树
 * @param job 作业对象
 * @return true 如果更新后列表非空
 */
inline bool update_job_child_process(Job& job) {
    if (job.JobPIDs.empty()) return false;
    
    job.JobPIDs = collect_process_tree(job.JobPIDs);
    spdlog::debug("update_job_child_process: JobPIDs: {}",fmt::join(job.JobPIDs, ", "));
    return !job.JobPIDs.empty();
}

} // namespace CommonJob
