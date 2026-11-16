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
#include "core/collector_type.h"
#include "core/job_identity.hpp"
#include "core/group_probe_stats.hpp"
#include "common/classad_decode.hpp"
#include "common/utils.hpp"
#include <unistd.h>
#include <grp.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <charconv>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <variant>
#include <spdlog/spdlog.h>
#include <unordered_set>
#include <fmt/core.h>

namespace fs = std::filesystem;

namespace SlurmJob
{

// 使用Utils命名空间中的公共函数
using Utils::get_ppid_of;
using Utils::get_cmdline_of;
using Utils::get_env_field;
using Utils::v2_cgroup_absolute_path;
using Utils::get_pids_in_cgroup;

inline void update_job_pids(Job& job){
    auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
    if(slurm_attr.stepd_pid == 0){
        //初始化相关内容
        if(pids_of(job).size() > 1){
            spdlog::warn("SlurmJob: multi pids, use first");
        }
        slurm_attr.stepd_pid = get_ppid_of(pids_of(job)[0]);
        slurm_attr.cgroup_path = v2_cgroup_absolute_path(pids_of(job)[0]);
    }   
    auto pid_cgroup = get_pids_in_cgroup(slurm_attr.cgroup_path);
    // 移除stepd_pid本身
    std::remove(pid_cgroup.begin(), pid_cgroup.end(), slurm_attr.stepd_pid);
    set_pids(job, std::move(pid_cgroup));
}

/* 获取Slurm Job ID */
inline uint64_t getJobID(pid_t pid){
    std::string job_id_str = get_env_field(pid, "SLURM_JOB_ID");
    spdlog::debug("SlurmJob: pid {} SLURM_JOB_ID {}", pid, job_id_str);
    if (job_id_str.empty()) return 0;

    try {
        return std::stoull(job_id_str);
    } catch (const std::exception& e) {
        spdlog::error("SlurmJob: failed to parse job ID '{}': {}", job_id_str, e.what());
        return 0;
    }
}

/* 获取Slurm Step ID */
inline uint32_t getStepID(pid_t pid){
    std::string step_id_str = get_env_field(pid, "SLURM_STEP_ID");
    spdlog::debug("SlurmJob: pid {} SLURM_STEP_ID {}", pid, step_id_str);
    if (step_id_str.empty()) return 0;

    try {
        return static_cast<uint32_t>(std::stoul(step_id_str));
    } catch (const std::exception& e) {
        spdlog::error("SlurmJob: failed to parse step ID '{}': {}", step_id_str, e.what());
        return 0;
    }
}

/* 获取作业用户 */
inline std::string getUser(pid_t pid){
    return get_env_field(pid, "SLURM_JOB_USER");
}

/* 获取作业名称 */
inline std::string getJobName(pid_t pid){
    return get_env_field(pid, "SLURM_JOB_NAME");
}

/* 获取分区/队列名称 */
inline std::string getPartition(pid_t pid){
    return get_env_field(pid, "SLURM_JOB_PARTITION");
}

// ---------------------------------------------------------------------------
// Strict, error-reporting parsing for the discovery adapter
// ---------------------------------------------------------------------------
// getJobID()/getStepID() collapse "missing", "unparsable" and "zero" into 0. A
// discovery observation must keep them apart: a guessed step merges two
// executions, a guessed job id invents a job.

namespace detail {
inline std::optional<std::uint64_t> parse_decimal_u64(const std::string& raw){
    if (raw.empty() || raw.size() > 20) return std::nullopt;
    std::uint64_t value = 0;
    const auto* begin = raw.data();
    const auto* end = raw.data() + raw.size();
    const auto parsed = std::from_chars(begin, end, value, 10);
    if (parsed.ec != std::errc() || parsed.ptr != end) return std::nullopt;
    return value;
}
} // namespace detail

// SLURM_JOB_ID -> logical job number. Missing/garbage is an explicit failure.
inline std::optional<std::uint64_t> parse_job_id(const std::string& raw, std::string& error){
    const auto value = detail::parse_decimal_u64(raw);
    if (!value) {
        error = raw.empty() ? "SLURM_JOB_ID missing" : "SLURM_JOB_ID '" + raw + "' is not decimal";
        return std::nullopt;
    }
    if (*value == 0) {
        error = "SLURM_JOB_ID is 0";
        return std::nullopt;
    }
    return value;
}

// SLURM_STEP_ID -> numeric step value. The special batch/extern constants stay
// distinct from numeric steps (see slurm_step_name in core/job_identity.hpp).
inline std::optional<std::uint32_t> parse_step_id(const std::string& raw, std::string& error){
    const auto value = detail::parse_decimal_u64(raw);
    if (!value || *value > std::numeric_limits<std::uint32_t>::max()) {
        error = raw.empty() ? "SLURM_STEP_ID missing" : "SLURM_STEP_ID '" + raw + "' is not a step value";
        return std::nullopt;
    }
    return static_cast<std::uint32_t>(*value);
}

// Adapter-boundary conversion: numeric step -> canonical wire step_id. batch and
// extern keep their names, every other value is its decimal form; nothing is
// ever mapped to a different step.
inline std::string step_id_name(std::uint32_t numeric_step){
    if (const auto special = slurm_step_name(numeric_step)) return *special;
    return std::to_string(numeric_step);
}

/* 获取所有slurmstepd进程PID列表 */
inline std::vector<pid_t> get_all_slurmstepd_pids(){
    std::vector<pid_t> slurm_pids;
    namespace fs = std::filesystem;
    for (const auto& entry : fs::directory_iterator("/proc")) {
        if (!entry.is_directory()) continue;
        std::string filename = entry.path().filename().string();
        if (!std::all_of(filename.begin(), filename.end(), ::isdigit)) continue;
        pid_t pid = static_cast<pid_t>(std::stol(filename));
        std::string cmdline = get_cmdline_of(pid);
        // slurmstepd 命令行通常包含 "slurmstepd"
        if (cmdline.find("slurmstepd") != std::string::npos) {
            slurm_pids.push_back(pid);
        }
    }
    return slurm_pids;
}

/* 获取指定slurmstepd下的所有子进程 */
inline std::vector<pid_t> get_slurm_job_pids(pid_t stepd_pid){
    std::vector<pid_t> job_pids;
    namespace fs = std::filesystem;
    
    for (const auto& entry : fs::directory_iterator("/proc")) {
        if (!entry.is_directory()) continue;
        std::string filename = entry.path().filename().string();
        if (!std::all_of(filename.begin(), filename.end(), ::isdigit)) continue;
        pid_t pid = static_cast<pid_t>(std::stol(filename));
        
        // 检查父进程是否是该stepd
        pid_t ppid = get_ppid_of(pid);
        if (ppid == stepd_pid) {
            // 检查是否有SLURM_JOB_ID环境变量
            if (getJobID(pid) > 0) {
                job_pids.push_back(pid);
            }
        }
    }
    return job_pids;
}

/* 获取所有正在运行的Slurm作业PID（去重） */
inline std::vector<pid_t> get_all_slurm_job_pids(){
    std::vector<pid_t> all_job_pids;
    std::unordered_set<pid_t> seen;
    
    auto stepd_pids = get_all_slurmstepd_pids();
    for (auto stepd_pid : stepd_pids) {
        auto job_pids = get_slurm_job_pids(stepd_pid);
        for (auto pid : job_pids) {
            if (seen.insert(pid).second) {
                all_job_pids.push_back(pid);
            }
        }
    }
    return all_job_pids;
}

inline std::string get_cluster_name(pid_t pid = 0){
    return get_env_field(pid, "SLURM_CLUSTER_NAME");
}

inline std::string get_native_job_id(pid_t pid){
    auto jid = getJobID(pid);
    return fmt::format("{}", jid);
}

/* ===================== 组归属（只读上游标准变量） =====================
 * - acct_group  ← SLURM_JOB_ACCOUNT（计费账户，与 Condor AcctGroup 同语义）
 * - unix_gid    ← SLURM_JOB_GID（不回退 /proc/<pid>/status 的 Gid:——那是进程
 *   real gid，可能因 newgrp/setgid 与作业主 gid 不同；缺失就 -1，不猜）
 * - unix_group  ← gid 经 getgrgid_r 解析；解析失败为空，不用 account 顶替
 * - env 读取顺序：作业用户进程优先（SLURM_JOB_* 由 slurmstepd 注入作业环境），
 *   stepd 自身 env 是 slurmd 派生的、可能不含作业变量，仅作兜底
 * - 名称解析与 env 探测分离完成态：gid 已知但组名为空时按周期重试
 *   （受负缓存/熔断限频），避免 LDAP 抖动把 unix_group 永久固定为空
 */
inline std::string getAccount(pid_t pid){
    return get_env_field(pid, "SLURM_JOB_ACCOUNT");
}

inline int64_t getGid(pid_t pid){
    std::string gid_str = get_env_field(pid, "SLURM_JOB_GID");
    if (gid_str.empty()) return -1;
    try {
        size_t pos = 0;
        long long v = std::stoll(gid_str, &pos);
        if (pos != gid_str.size()) return -1;
        if (v < 0 || v > 0xFFFFFFFFLL) return -1;  // gid_t 为 uint32；越界按未知处理
        return static_cast<int64_t>(v);
    } catch (const std::exception&) {
        return -1;
    }
}

enum class NssOutcome { Ok, Miss, Breaker };

// NSS 熔断/缓存参数：
//  - 单次解析 >500ms 或连续失败 5 次 → 全局暂停解析 60s（防止 LDAP 故障阻塞发现/采集）
//  - 成功条目缓存（组名基本不变），容量上限 4096（满则整体清空重建）
//  - 失败 gid 负缓存 60s（TTL 过后重试，即“独立完成态”的限频器）
inline constexpr auto kNssSlowThreshold = std::chrono::milliseconds(500);
inline constexpr uint32_t kNssConsecutiveFailLimit = 5;
inline constexpr auto kNssBreakerSeconds = std::chrono::seconds(60);
inline constexpr auto kNssNegativeTtl = std::chrono::seconds(60);
inline constexpr size_t kNssCacheMaxEntries = 4096;

inline std::string resolve_unix_group_name(int64_t gid, NssOutcome* outcome = nullptr) {
    struct NssState {
        std::mutex mtx;
        std::unordered_map<int64_t, std::string> positive;
        std::unordered_map<int64_t, std::chrono::steady_clock::time_point> negative;
        std::chrono::steady_clock::time_point breaker_until{};
        uint32_t consecutive_fail = 0;
    };
    // inline 函数的函数局部 static 在各编译单元间唯一（C++17）
    static NssState state;

    if (outcome) *outcome = NssOutcome::Miss;
    if (gid < 0) return "";

    std::lock_guard<std::mutex> lk(state.mtx);
    const auto now = std::chrono::steady_clock::now();

    // 熔断打开：全局跳过（不计失败，由调用方计 skipped）
    if (now < state.breaker_until) {
        if (outcome) *outcome = NssOutcome::Breaker;
        return "";
    }
    // 成功缓存
    auto pit = state.positive.find(gid);
    if (pit != state.positive.end()) return pit->second;
    // 负缓存未到期：本周期跳过
    auto nit = state.negative.find(gid);
    if (nit != state.negative.end() && now < nit->second) {
        if (outcome) *outcome = NssOutcome::Breaker;  // 限频跳过，非解析失败
        return "";
    }

    long bufsize = sysconf(_SC_GETGR_R_SIZE_MAX);
    if (bufsize <= 0) bufsize = 16 * 1024;
    std::vector<char> buf(static_cast<size_t>(bufsize));
    struct group grp{};
    struct group* result = nullptr;
    const auto t0 = now;
    int rc = getgrgid_r(static_cast<gid_t>(gid), &grp, buf.data(), buf.size(), &result);
    const auto elapsed = std::chrono::steady_clock::now() - t0;

    const bool slow = elapsed > kNssSlowThreshold;
    if (rc != 0 || result == nullptr) {
        state.negative[gid] = now + kNssNegativeTtl;
        if (++state.consecutive_fail >= kNssConsecutiveFailLimit || slow) {
            state.breaker_until = now + kNssBreakerSeconds;
            state.consecutive_fail = 0;
            spdlog::warn("SlurmJob: NSS breaker open for 60s (gid {} rc={} slow={} elapsed_ms={})",
                         gid, rc, slow,
                         std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
        }
        return "";
    }
    state.consecutive_fail = 0;
    state.negative.erase(gid);
    std::string name = result->gr_name ? result->gr_name : "";
    if (state.positive.size() >= kNssCacheMaxEntries) state.positive.clear();
    state.positive[gid] = name;
    if (slow) {
        spdlog::warn("SlurmJob: NSS resolve for gid {} took {}ms (still succeeded)",
                     gid, std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
    }
    if (outcome) *outcome = NssOutcome::Ok;
    return name;
}

// 探测并填充组字段；返回是否取到任一非空组值（供 restore 路径决定 group_need_persist）。
inline bool probe_groups(Job& job) {
    auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
    auto& stats = GroupProbeStats::instance();
    bool got_any = false;

    // 1) env 探测（account/gid）
    if (!job.group_probed) {
        const auto S = GroupProbeStats::Source::SlurmEnv;
        stats.record_attempt(S);
        pid_t pid = !job.JobPIDs.empty() ? job.JobPIDs[0] : attr.stepd_pid;
        bool ok = false;
        if (pid > 0) {
            auto account = getAccount(pid);
            if (ClassAdValue::valid_group_value(account, ClassAdValue::kGroupValueMaxLen)) {
                attr.acct_group = account;
                attr.unix_gid = getGid(pid);
                ok = true;
            }
        }
        if (!ok) {
            stats.record_failure(S);
            ++job.group_probe_attempts;
            spdlog::debug("SlurmJob: group env probe failed for JobID {} (attempt {})",
                          job.JobID, job.group_probe_attempts);
            if (job.group_probe_attempts >= ClassAdValue::kMaxGroupProbeAttempts) {
                job.group_probed = true;  // 有界重试后放弃
                spdlog::warn("SlurmJob: give up group env probe for JobID {} after {} attempts",
                             job.JobID, job.group_probe_attempts);
            }
        } else {
            if (!attr.acct_group.empty()) {
                stats.record_non_empty(S, attr.acct_group);
            } else {
                stats.record_empty(S);  // 读成功但无 account：合法缺失
            }
            job.group_probed = true;
            job.group_probe_attempts = 0;
            got_any = !attr.acct_group.empty() || attr.unix_gid >= 0;
            spdlog::debug("SlurmJob: JobID {} groups acct_group='{}' unix_gid={}",
                          job.JobID, attr.acct_group, attr.unix_gid);
        }
    }

    // 2) 名称解析：独立完成态，gid 已知且组名为空时重试（负缓存 60s 限频）
    if (attr.unix_gid >= 0 && attr.unix_group.empty()) {
        const auto S = GroupProbeStats::Source::SlurmNss;
        stats.record_attempt(S);
        NssOutcome outcome = NssOutcome::Miss;
        auto name = resolve_unix_group_name(attr.unix_gid, &outcome);
        if (!name.empty()) {
            if (ClassAdValue::valid_group_value(name, ClassAdValue::kGroupValueMaxLen)) {
                attr.unix_group = name;
                stats.record_non_empty(S, std::string());
                got_any = true;
            } else {
                stats.record_failure(S);  // 组名异常（超长/控制字符）
            }
        } else if (outcome == NssOutcome::Breaker) {
            stats.record_skipped(S);      // 熔断/负缓存限频，非失败
        } else {
            stats.record_failure(S);      // 解析失败或过慢
        }
    }
    return got_any;
}


inline void update_job_info(Job& job){
    if (pids_of(job).empty()) return;

    auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
    pid_t representative_pid = pids_of(job)[0]; // 以第一个PID为代表

    // 标识性字段：只在首次未设置时填充
    if (slurm_attr.job_id == 0) {
        slurm_attr.job_id = getJobID(representative_pid);
        slurm_attr.step_id = getStepID(representative_pid);
        job.NativeJobID = fmt::format("{}", slurm_attr.job_id);
    }

    // 描述性字段：每次检查，为空/0则从进程环境变量补全
    if (slurm_attr.user.empty()) {
        slurm_attr.user = getUser(representative_pid);
    }
    if (slurm_attr.job_name.empty()) {
        slurm_attr.job_name = getJobName(representative_pid);
    }
    if (slurm_attr.partition.empty()) {
        slurm_attr.partition = getPartition(representative_pid);
    }
    if (slurm_attr.cluster_name.empty()) {
        slurm_attr.cluster_name = get_cluster_name(representative_pid);
    }
    if (job.cluster_name.empty()) {
        job.cluster_name = slurm_attr.cluster_name;
    }
    if (job.clusterTag.empty()) {
        job.clusterTag = slurm_attr.cluster_name;
    }
    // stepd_pid 取 JobPIDs 中最小的 PID (slurmstepd 通常为父进程, PID 最小)
    if (slurm_attr.stepd_pid == 0) {
        slurm_attr.stepd_pid = *std::min_element(pids_of(job).begin(), pids_of(job).end());
    }

    // 组字段：env 探测未完成时补（有界重试）；组名解析独立完成态，按周期重试。
    probe_groups(job);

    return;
}

} // namespace SlurmJob
