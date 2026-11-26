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
using Utils::get_env_field;

// ---------------------------------------------------------------------------
// Strict, error-reporting parsing for the discovery adapter
// ---------------------------------------------------------------------------
// A discovery observation must keep "missing", "unparsable" and "zero" apart:
// a guessed step merges two executions, a guessed job id invents a job.

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

// cgroup 路径里的 step 段。Slurm 在 ".../job_<id>/step_<name>" 下为每个 step
// 建 cgroup；batch step 不导出 SLURM_STEP_ID，step 身份只在这里有权威名字。
// 只接受 step_batch / step_extern / step_<十进制普通 step>，其它形态返回
// nullopt——发现层不据此猜身份。
struct CgroupJobStep {
    std::uint64_t job_id{0};
    std::uint32_t step{0};
};

inline std::optional<CgroupJobStep> parse_cgroup_job_step(const std::string& path)
{
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start <= path.size()) {
        const std::size_t end = path.find('/', start);
        const std::size_t stop = end == std::string::npos ? path.size() : end;
        if (stop > start) parts.push_back(path.substr(start, stop - start));
        if (end == std::string::npos) break;
        start = end + 1;
    }
    for (std::size_t index = 0; index + 1 < parts.size(); ++index) {
        const std::string& job = parts[index];
        if (job.size() <= 4 || job.compare(0, 4, "job_") != 0) continue;
        std::string error;
        const auto job_id = parse_job_id(job.substr(4), error);
        if (!job_id) continue;
        const std::string& step = parts[index + 1];
        if (step.size() <= 5 || step.compare(0, 5, "step_") != 0) continue;
        const std::string name = step.substr(5);
        if (name == "batch")
            return CgroupJobStep{*job_id, kSlurmBatchStepValue};
        if (name == "extern")
            return CgroupJobStep{*job_id, kSlurmExternStepValue};
        const auto value = detail::parse_decimal_u64(name);
        // SLURM_MAX_NORMAL_STEP_ID = 0xfffffff0；更小的保留值不属于普通 step。
        if (!value || *value >= 0xfffffff0ull) continue;
        return CgroupJobStep{*job_id, static_cast<std::uint32_t>(*value)};
    }
    return std::nullopt;
}

inline std::string clusterName(pid_t pid = 0){
    return get_env_field(pid, "SLURM_CLUSTER_NAME");
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
    return Utils::get_env_field(pid, "SLURM_JOB_ACCOUNT");
}

inline int64_t parseGroupGid(const std::string& gid_str){
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

inline int64_t getGid(pid_t pid){
    return parseGroupGid(Utils::get_env_field(pid, "SLURM_JOB_GID"));
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

// 探测并填充组字段；返回是否取到任一非空组值。
inline bool probe_groups(Job& job) {
    const auto pids = pids_of(job);
    auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
    auto& stats = GroupProbeStats::instance();
    bool got_any = false;

    // 1) env 探测（account/gid）
    if (!job.group_probed) {
        const auto S = GroupProbeStats::Source::SlurmEnv;
        stats.record_attempt(S);
        pid_t pid = !pids.empty() ? pids[0] : attr.stepd_pid;
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


} // namespace SlurmJob
