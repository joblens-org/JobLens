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
#include "core/group_probe_stats.hpp"
#include "common/classad_decode.hpp"
#include "common/utils.hpp"
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <climits>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem>
#include <iostream>
#include <optional>
#include <variant>
#include <algorithm>
#include <cctype>
#include <optional>
#include <spdlog/spdlog.h>
#include <fmt/core.h>

namespace fs = std::filesystem;

namespace CondorJob
{

// 使用Utils命名空间中的公共函数
using Utils::get_ppid_of;
using Utils::get_cmdline_of;
using Utils::get_env_field;
using Utils::v2_cgroup_absolute_path;
using Utils::get_pids_in_cgroup;

inline bool update_job_pids(Job& job){
    auto& condor_attr = std::get<CondorJobAttr>(job.sub_attr);

    if(condor_attr.starter_pid == 0){
        if (pids_of(job).empty()) {
            spdlog::debug("CondorJob: cannot initialize starter_pid, JobPIDs is empty");
            return false;
        }
        //初始化相关内容
        if(pids_of(job).size()>1){
            spdlog::warn("CondorJob: multi pids, use first");
        }
        condor_attr.starter_pid = get_ppid_of(pids_of(job)[0]);
    }

    if (condor_attr.starter_pid <= 0) {
        spdlog::debug("CondorJob: cannot update pids by cgroup, invalid starter_pid {}", condor_attr.starter_pid);
        return false;
    }

    if (condor_attr.slots_cgroup_path.empty()) {
        if (pids_of(job).empty()) {
            spdlog::debug("CondorJob: cannot discover cgroup path, JobPIDs is empty");
            return false;
        }
        condor_attr.slots_cgroup_path = v2_cgroup_absolute_path(pids_of(job)[0]);
    }

    if (condor_attr.slots_cgroup_path.empty()) {
        spdlog::debug("CondorJob: cannot update pids by cgroup, empty cgroup path for starter_pid {}", condor_attr.starter_pid);
        return false;
    }

    auto pid_slots_cgroup = get_pids_in_cgroup(condor_attr.slots_cgroup_path);
    pid_slots_cgroup.erase(
        std::remove(pid_slots_cgroup.begin(), pid_slots_cgroup.end(), condor_attr.starter_pid),
        pid_slots_cgroup.end());

    if (pid_slots_cgroup.empty()) {
        spdlog::debug("CondorJob: cgroup {} has no job pids after filtering starter_pid {}",
                      condor_attr.slots_cgroup_path,
                      condor_attr.starter_pid);
        return false;
    }

    set_pids(job, std::move(pid_slots_cgroup));
    spdlog::trace("CondorJob: updated JobPIDs from cgroup {}", condor_attr.slots_cgroup_path);
    return true;
}

/* 解析 GlobalJobId */
inline static std::string parse_job_id(const std::string &job_ad)
{
    std::ifstream f(job_ad);
    std::string line;
    while (std::getline(f, line)) {
        auto p = line.find(" = ");
        if (p != std::string::npos && line.substr(0, p) == "GlobalJobId") {
            std::string v = line.substr(p + 3);
            auto sharp = v.find('#');
            return sharp == std::string::npos ? "" : v.substr(sharp + 1);
        }
    }
    return "";
}

inline static uint64_t parse_uint64(std::string_view s)
{
    uint64_t val = 0;
    for (char c : s) {
        if (c == '#') break;
        if (c == '.') continue;
        if (c >= '0' && c <= '9') {
            val = val * 10 + (c - '0');
        } else {
            return 0;  // invalid
        }
    }
    return val;
}

inline std::string get_ad_field(std::string ad_path, const std::string &field){
    std::ifstream f(ad_path);
    std::string line;
    while (std::getline(f, line)) {
        auto p = line.find(" = ");
        if (p != std::string::npos && line.substr(0, p).compare(field) == 0) {
            std::string v = line.substr(p + 3);
            return v;
        }
    }
    return "";
}

// ---------------------------------------------------------------------------
// Structured job-ad parsing for the discovery adapter
// ---------------------------------------------------------------------------
// The helpers above return "" for every failure, which cannot distinguish "no
// ad" from "unreadable ad" from "ad without a GlobalJobId". The observation
// adapter must tell the difference, because each of those is an *incomplete*
// observation with its own reason, never a guessed identity.

struct CondorAdIdentity {
    std::string global_job_id;   // raw field value as written in the ad
    std::string scheduler_name;  // schedd namespace, e.g. "scheduler@schedd07.example"
    std::string scheduler_host;  // schedd host part, e.g. "schedd07.example"
    std::string native_job_id;   // "ClusterId.ProcId" as written in the ad
    std::string owner;
};

inline bool read_ad_content(const std::string& ad_path, std::string& content,
                            std::string& error){
    std::ifstream f(ad_path, std::ios::binary);
    if (!f) {
        error = "cannot open '" + ad_path + "'";
        return false;
    }
    std::ostringstream buffer;
    buffer << f.rdbuf();
    if (f.bad()) {
        error = "read error on '" + ad_path + "'";
        return false;
    }
    content = buffer.str();
    if (content.empty()) {
        error = "job ad '" + ad_path + "' is empty";
        return false;
    }
    return true;
}

inline std::string get_ad_field_from_content(const std::string& content,
                                             const std::string &field){
    std::istringstream stream(content);
    std::string line;
    while (std::getline(stream, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        auto p = line.find(" = ");
        if (p != std::string::npos && line.compare(0, p, field) == 0) {
            return line.substr(p + 3);
        }
    }
    return "";
}

namespace detail {
inline std::string strip_quotes(std::string value){
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"')
        return value.substr(1, value.size() - 2);
    return value;
}
inline bool is_decimal_part(const std::string& part){
    if (part.empty()) return false;
    return std::all_of(part.begin(), part.end(),
                       [](unsigned char c){ return std::isdigit(c) != 0; });
}
} // namespace detail

// Parses the identity-bearing fields of a condor job ad. Returns nullopt with a
// reason when the ad cannot name a job; the caller turns that into an
// incomplete observation.
inline std::optional<CondorAdIdentity> parse_ad_identity(const std::string& content,
                                                         std::string& error){
    CondorAdIdentity ad;
    ad.global_job_id = detail::strip_quotes(get_ad_field_from_content(content, "GlobalJobId"));
    if (ad.global_job_id.empty()) {
        error = "no GlobalJobId field";
        return std::nullopt;
    }
    ad.owner = detail::strip_quotes(get_ad_field_from_content(content, "Owner"));

    // Format: "scheduler@schedd.example#ClusterId.ProcId#timestamp"
    const auto first_sharp = ad.global_job_id.find('#');
    if (first_sharp == std::string::npos) {
        error = "GlobalJobId '" + ad.global_job_id + "' has no schedd segment";
        return std::nullopt;
    }
    const auto second_sharp = ad.global_job_id.find('#', first_sharp + 1);
    if (second_sharp == std::string::npos) {
        error = "GlobalJobId '" + ad.global_job_id + "' has no job id segment";
        return std::nullopt;
    }

    const auto quote = ad.global_job_id.find('"');
    if (quote != std::string::npos && quote < first_sharp)
        ad.scheduler_name = ad.global_job_id.substr(quote + 1, first_sharp - quote - 1);
    else
        ad.scheduler_name = ad.global_job_id.substr(0, first_sharp);

    const auto at = ad.global_job_id.find('@');
    if (at != std::string::npos && at < first_sharp)
        ad.scheduler_host = ad.global_job_id.substr(at + 1, first_sharp - at - 1);

    ad.native_job_id =
        ad.global_job_id.substr(first_sharp + 1, second_sharp - first_sharp - 1);
    const auto dot = ad.native_job_id.find('.');
    if (dot == std::string::npos ||
        !detail::is_decimal_part(ad.native_job_id.substr(0, dot)) ||
        !detail::is_decimal_part(ad.native_job_id.substr(dot + 1))) {
        error = "GlobalJobId '" + ad.global_job_id + "' has an unparsable native job id";
        return std::nullopt;
    }
    return ad;
}

inline uint64_t getJobID(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    spdlog::debug("CondorJob: pid {} _CONDOR_JOB_AD {}", pid, ad_path);
    if (ad_path.empty()) return 0;

    std::string condor_jobid = parse_job_id(ad_path);
    spdlog::debug("CondorJob: pid {} condor jobid {}", pid, condor_jobid);
    if (condor_jobid.empty()) return 0;

    return parse_uint64(condor_jobid);
}

inline std::string get_scheduler_host(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    if (ad_path.empty()) return "";
    auto global_job_id = get_ad_field(ad_path, "GlobalJobId");
    std::string host_part;
    auto at_pos = global_job_id.find('@');
    if (at_pos != std::string::npos) {
        auto sharp_pos = global_job_id.find('#', at_pos);
        if (sharp_pos != std::string::npos) {
            host_part = global_job_id.substr(at_pos + 1, sharp_pos - at_pos - 1);
        }
    }   
    return host_part;
}

inline std::string get_scheduler_name(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    if (ad_path.empty()) return "";
    auto global_job_id = get_ad_field(ad_path, "GlobalJobId");
    std::string scheduler_name;
    // GlobalJobId格式示例："scheduler@schedd07.ihep.ac.cn#76226337.0#1775723895"
    auto sharp_pos = global_job_id.find('#');
    if (sharp_pos != std::string::npos) {
        auto quotation_pos = global_job_id.find('"');
        if (quotation_pos != std::string::npos) {
            scheduler_name = global_job_id.substr(quotation_pos + 1, sharp_pos - quotation_pos - 1);
        }
    }
    return scheduler_name;
}

inline std::pair<size_t, size_t> get_cluster_and_proc_id(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    if (ad_path.empty()) return {0, 0};
    auto global_job_id = get_ad_field(ad_path, "GlobalJobId");
    auto first_sharp = global_job_id.find('#');
    if (first_sharp == std::string::npos) return {0, 0};
    auto second_sharp = global_job_id.find('#', first_sharp + 1);
    if (second_sharp == std::string::npos) return {0, 0};
    
    std::string cluster_proc = global_job_id.substr(first_sharp + 1, second_sharp - first_sharp - 1);
    auto dot_pos = cluster_proc.find('.');
    if (dot_pos == std::string::npos) return {0, 0};
    
    size_t cluster_id = parse_uint64(cluster_proc.substr(0, dot_pos));
    size_t proc_id = parse_uint64(cluster_proc.substr(dot_pos + 1));
    
    return {cluster_id, proc_id};
}

/* ===================== 组归属（只读上游标准 ClassAd 属性） =====================
 * - AcctGroup：计费组，完整三段（实验.子组.队列等级）；PrimaryUnixGroup：作业主动
 *   声明的主 Unix 组（primary_unix_group），未声明为空。不读 AccountingGroup
 *   （= acct_group + "." + 用户的派生物）与任何站点显示组（如 IHEP_RealGroup）。
 * - ad 文件来源优先级：starter 进程 env（condor 守护启动，作业用户改不了）>
 *   作业用户进程 env（兜底，需通过符号链接/属主校验）。
 * - GlobalJobId 只做“防串号”一致性检查；防伪造靠来源校验（降低风险，非消除——
 *   同 uid 的作业进程仍可能伪造，残余风险见设计文档信任模型）。
 * - 读失败不置 group_probed，由 group_probe_attempts 有界重试；采集周期会反复
 *   走 update_job → update_job_info，probed 标志防止组本就为空的站点每周期重读。
 */
// 解析 GlobalJobId 中的 cluster.proc（"sched@host#123.0#ts" → {123, 0}）。
inline std::optional<std::pair<size_t, size_t>> cluster_proc_from_global_job_id(const std::string& g) {
    auto first = g.find('#');
    if (first == std::string::npos) return std::nullopt;
    auto second = g.find('#', first + 1);
    if (second == std::string::npos) return std::nullopt;
    std::string cp = g.substr(first + 1, second - first - 1);
    auto dot = cp.find('.');
    if (dot == std::string::npos) return std::nullopt;
    size_t cluster = parse_uint64(cp.substr(0, dot));
    size_t proc = parse_uint64(cp.substr(dot + 1));
    if (cluster == 0) return std::nullopt;  // cluster 0 不是合法作业；proc 0 合法
    return std::make_pair(cluster, proc);
}

inline bool is_regular_nonsymlink(const std::string& path) {
    struct stat st{};
    if (::lstat(path.c_str(), &st) != 0) return false;
    return S_ISREG(st.st_mode);
}

inline uid_t proc_owner_uid(pid_t pid) {
    struct stat st{};
    if (::stat((fs::path("/proc") / std::to_string(pid)).c_str(), &st) != 0) return static_cast<uid_t>(-1);
    return st.st_uid;
}

inline uid_t file_owner_uid(const std::string& path) {
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0) return static_cast<uid_t>(-1);
    return st.st_uid;
}

// 探测并填充组字段；返回是否取到任一非空组值（供 restore 路径决定 group_need_persist）。
inline bool probe_groups(Job& job) {
    auto& attr = std::get<CondorJobAttr>(job.sub_attr);
    auto& stats = GroupProbeStats::instance();
    const auto S = GroupProbeStats::Source::CondorAd;
    stats.record_attempt(S);

    auto bail = [&](const std::string& why) {
        stats.record_failure(S);
        ++job.group_probe_attempts;
        spdlog::debug("CondorJob: group probe failed for JobID {} (attempt {}): {}",
                      job.JobID, job.group_probe_attempts, why);
        if (job.group_probe_attempts >= ClassAdValue::kMaxGroupProbeAttempts) {
            job.group_probed = true;  // 有界重试后放弃，避免每周期重读
            spdlog::warn("CondorJob: give up group probe for JobID {} after {} attempts", job.JobID, job.group_probe_attempts);
        }
        return false;
    };

    // 1) ad 文件来源：starter env 优先；用户进程 env 兜底并做来源校验
    std::string starter_ad;
    if (attr.starter_pid > 0) starter_ad = get_env_field(attr.starter_pid, "_CONDOR_JOB_AD");
    std::string user_ad;
    if (!job.JobPIDs.empty()) user_ad = get_env_field(job.JobPIDs[0], "_CONDOR_JOB_AD");

    std::string ad_path;
    if (!starter_ad.empty()) {
        ad_path = starter_ad;
    } else if (!user_ad.empty()) {
        ad_path = user_ad;
        if (!is_regular_nonsymlink(ad_path)) return bail("ad path is not a regular file (symlink?)");
        uid_t owner = file_owner_uid(ad_path);
        uid_t job_uid = proc_owner_uid(job.JobPIDs[0]);
        uid_t starter_uid = attr.starter_pid > 0 ? proc_owner_uid(attr.starter_pid) : static_cast<uid_t>(-1);
        if (owner != 0 && owner != job_uid && owner != starter_uid) {
            return bail("ad file owner mismatch");
        }
    } else {
        return bail("no _CONDOR_JOB_AD in starter/user env");
    }

    // 2) GlobalJobId 一致性（防 ad 文件串号；不是防伪造）
    auto cp = cluster_proc_from_global_job_id(get_ad_field(ad_path, "GlobalJobId"));
    if (cp && !job.NativeJobID.empty()) {
        auto expect = fmt::format("{}.{}", cp->first, cp->second);
        if (expect != job.NativeJobID) return bail("GlobalJobId mismatch with NativeJobID");
    }

    // 3) 读取并校验组值
    auto acct = ClassAdValue::decode_string(get_ad_field(ad_path, "AcctGroup"));
    auto unix_grp = ClassAdValue::decode_string(get_ad_field(ad_path, "PrimaryUnixGroup"));
    if (!ClassAdValue::valid_group_value(acct, ClassAdValue::kGroupValueMaxLen) ||
        !ClassAdValue::valid_group_value(unix_grp, ClassAdValue::kGroupValueMaxLen)) {
        return bail("group value over length limit or contains control chars");
    }

    attr.acct_group = acct;
    attr.unix_group = unix_grp;
    if (!acct.empty()) {
        stats.record_non_empty(S, acct);
    } else {
        stats.record_empty(S);  // 读成功但无组：合法缺失
    }
    job.group_probed = true;
    job.group_probe_attempts = 0;
    spdlog::debug("CondorJob: JobID {} groups acct_group='{}' unix_group='{}'",
                  job.JobID, attr.acct_group, attr.unix_group);
    return !acct.empty() || !unix_grp.empty();
}


inline void update_job_info(Job& job){
    auto& condor_attr = std::get<CondorJobAttr>(job.sub_attr);
    const bool has_job_pid = !pids_of(job).empty();

    // 确保 starter_pid 已知 (其他字段依赖它)
    if (condor_attr.starter_pid == 0) {
        if (has_job_pid) {
            condor_attr.starter_pid = get_ppid_of(pids_of(job)[0]);
        } else {
            spdlog::debug("CondorJob: cannot initialize starter_pid in update_job_info, JobPIDs is empty");
        }
    }

    // 逐字段检查，为空则从系统补全
    if (condor_attr.scheduler_host.empty() && condor_attr.starter_pid > 0) {
        condor_attr.scheduler_host = get_scheduler_host(condor_attr.starter_pid);
    }
    if (condor_attr.scheduler_name.empty() && condor_attr.starter_pid > 0) {
        condor_attr.scheduler_name = get_scheduler_name(condor_attr.starter_pid);
    }
    job.clusterTag = condor_attr.scheduler_name;

    if (condor_attr.slots_cgroup_path.empty()) {
        if (has_job_pid) {
            condor_attr.slots_cgroup_path = v2_cgroup_absolute_path(pids_of(job)[0]);
        } else {
            spdlog::debug("CondorJob: cannot discover cgroup path in update_job_info, JobPIDs is empty");
        }
    }
    if (condor_attr.collector_host.empty()) {
        condor_attr.collector_host = Utils::get_condor_collector_host();
    }
    if (job.cluster_name.empty()) {
        job.cluster_name = condor_attr.collector_host;
    }
    if ((condor_attr.cluster_id == 0 || condor_attr.proc_id == 0) && condor_attr.starter_pid > 0) {
        auto [cid, pid_] = get_cluster_and_proc_id(condor_attr.starter_pid);
        if (condor_attr.cluster_id == 0) condor_attr.cluster_id = cid;
        if (condor_attr.proc_id == 0) condor_attr.proc_id = pid_;
        job.NativeJobID = fmt::format("{}.{}", condor_attr.cluster_id, condor_attr.proc_id);
    }

    // 组字段：只在尚未成功探测过时补（读失败由 group_probe_attempts 有界重试）。
    if (!job.group_probed) {
        probe_groups(job);
    }

    return;
}
} // namespace CondorJob
