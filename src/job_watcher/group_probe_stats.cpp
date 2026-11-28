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
#include "job_watcher/group_probe_stats.hpp"

GroupProbeStats& GroupProbeStats::instance() {
    static GroupProbeStats stats;
    return stats;
}

const char* GroupProbeStats::source_name(Source s) {
    switch (s) {
        case Source::CondorAd: return "condor_ad";
        case Source::SlurmEnv: return "slurm_env";
        case Source::SlurmNss: return "slurm_nss";
        default: return "unknown";
    }
}

void GroupProbeStats::record_attempt(Source s) {
    if (static_cast<size_t>(s) < kSourceCount) counters_[static_cast<size_t>(s)].attempts.fetch_add(1, std::memory_order_relaxed);
}

void GroupProbeStats::record_failure(Source s) {
    if (static_cast<size_t>(s) < kSourceCount) counters_[static_cast<size_t>(s)].failures.fetch_add(1, std::memory_order_relaxed);
}

void GroupProbeStats::record_empty(Source s) {
    if (static_cast<size_t>(s) < kSourceCount) counters_[static_cast<size_t>(s)].empty.fetch_add(1, std::memory_order_relaxed);
}

void GroupProbeStats::record_skipped(Source s) {
    if (static_cast<size_t>(s) < kSourceCount) counters_[static_cast<size_t>(s)].skipped.fetch_add(1, std::memory_order_relaxed);
}

void GroupProbeStats::record_non_empty(Source s, const std::string& acct_group) {
    if (static_cast<size_t>(s) >= kSourceCount) return;
    counters_[static_cast<size_t>(s)].non_empty.fetch_add(1, std::memory_order_relaxed);

    // 段数分布只对 acct_group 本体有意义（condor_ad / slurm_env）；NSS 组名不参与。
    if (s != Source::CondorAd && s != Source::SlurmEnv) return;
    if (acct_group.empty()) return;
    // 段数分布：AcctGroup 预期三段（实验.子组.队列等级）；异常（>200 段）记 255。
    uint32_t segs = 1;
    for (char c : acct_group) {
        if (c == '.') ++segs;
    }
    if (segs > 200) segs = 255;
    std::lock_guard<std::mutex> lk(seg_mtx_);
    ++segments_[segs];
}

void GroupProbeStats::record_truncated() {
    truncated_.fetch_add(1, std::memory_order_relaxed);
}

nlohmann::json GroupProbeStats::to_json() const {
    nlohmann::json j = nlohmann::json::object();
    nlohmann::json by = nlohmann::json::object();
    for (size_t i = 0; i < kSourceCount; ++i) {
        const auto& c = counters_[i];
        nlohmann::json e = nlohmann::json::object();
        e["attempts"]  = c.attempts.load(std::memory_order_relaxed);
        e["failures"]  = c.failures.load(std::memory_order_relaxed);
        e["empty"]     = c.empty.load(std::memory_order_relaxed);
        e["non_empty"] = c.non_empty.load(std::memory_order_relaxed);
        e["skipped"]   = c.skipped.load(std::memory_order_relaxed);
        by[source_name(static_cast<Source>(i))] = std::move(e);
    }
    j["by_source"] = std::move(by);
    j["truncated"] = truncated_.load(std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(seg_mtx_);
        nlohmann::json segs = nlohmann::json::object();
        for (const auto& [k, v] : segments_) {
            segs[k == 255 ? "other" : std::to_string(k)] = v;
        }
        j["acct_group_segments"] = std::move(segs);
    }
    return j;
}
