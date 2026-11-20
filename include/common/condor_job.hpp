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
#include "common/utils.hpp"
#include <unistd.h>
#include "core/group_probe_stats.hpp"
#include "common/classad_decode.hpp"
#include <sys/stat.h>
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

// ---------------------------------------------------------------------------
// Structured job-ad parsing for the discovery adapter
// ---------------------------------------------------------------------------
// An unreadable ad, an ad without a GlobalJobId and a GlobalJobId with an
// unparsable native job id are three different *incomplete* observations, each
// with its own reason, never a guessed identity.

struct CondorAdIdentity {
    std::string global_job_id;   // raw field value as written in the ad
    std::string scheduler_name;  // schedd namespace, e.g. "scheduler@schedd07.example"
    std::string scheduler_host;  // schedd host part, e.g. "schedd07.example"
    std::string job_native_id;   // "ClusterId.ProcId" as written in the ad
    size_t      cluster_id{};    // typed ClusterId parsed from GlobalJobId
    size_t      proc_id{};       // typed ProcId parsed from GlobalJobId
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

    ad.job_native_id =
        ad.global_job_id.substr(first_sharp + 1, second_sharp - first_sharp - 1);
    const auto dot = ad.job_native_id.find('.');
    const auto cluster_part = dot == std::string::npos
                                  ? std::string{}
                                  : ad.job_native_id.substr(0, dot);
    const auto proc_part = dot == std::string::npos
                               ? std::string{}
                               : ad.job_native_id.substr(dot + 1);
    if (dot == std::string::npos ||
        !detail::is_decimal_part(cluster_part) ||
        !detail::is_decimal_part(proc_part)) {
        error = "GlobalJobId '" + ad.global_job_id + "' has an unparsable native job id";
        return std::nullopt;
    }
    // The typed pair comes from the same segment the string render does — one
    // source, so the two can never disagree. parse_job_id_text is the shared
    // strict decimal parser; a cluster id of 0 keeps the identity unparsable.
    const auto typed_cluster = parse_job_id_text(cluster_part);
    const auto typed_proc = parse_job_id_text(proc_part);
    if (!typed_cluster || !typed_proc || *typed_cluster == 0) {
        error = "GlobalJobId '" + ad.global_job_id + "' has an unparsable native job id";
        return std::nullopt;
    }
    ad.cluster_id = static_cast<size_t>(*typed_cluster);
    ad.proc_id = static_cast<size_t>(*typed_proc);
    return ad;
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
    const auto pids = pids_of(job);
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
    if (attr.starter_pid > 0) starter_ad = Utils::get_env_field(attr.starter_pid, "_CONDOR_JOB_AD");
    std::string user_ad;
    if (!pids.empty()) user_ad = Utils::get_env_field(pids[0], "_CONDOR_JOB_AD");

    std::string ad_path;
    if (!starter_ad.empty()) {
        ad_path = starter_ad;
    } else if (!user_ad.empty()) {
        ad_path = user_ad;
        if (!is_regular_nonsymlink(ad_path)) return bail("ad path is not a regular file (symlink?)");
        uid_t owner = file_owner_uid(ad_path);
        uid_t job_uid = proc_owner_uid(pids[0]);
        uid_t starter_uid = attr.starter_pid > 0 ? proc_owner_uid(attr.starter_pid) : static_cast<uid_t>(-1);
        if (owner != 0 && owner != job_uid && owner != starter_uid) {
            return bail("ad file owner mismatch");
        }
    } else {
        return bail("no _CONDOR_JOB_AD in starter/user env");
    }

    // 2) GlobalJobId 一致性（防 ad 文件串号；不是防伪造）
    std::string content, error;
    if (!read_ad_content(ad_path, content, error)) return bail("cannot read job ad");
    const auto identity = parse_ad_identity(content, error);
    if (!identity || (!job.JobNativeID.empty() && identity->job_native_id != job.JobNativeID)) {
        return bail("GlobalJobId missing or does not match JobNativeID");
    }

    // 3) 读取并校验组值
    auto acct = ClassAdValue::decode_string(get_ad_field_from_content(content, "AcctGroup"));
    auto unix_grp = ClassAdValue::decode_string(get_ad_field_from_content(content, "PrimaryUnixGroup"));
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


} // namespace CondorJob
