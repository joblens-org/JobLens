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
#include <nlohmann/json.hpp>
#include <atomic>
#include <array>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

// 组字段探测统计：JobRegistry/group_probe_stats 的数据源。
//
// 计数语义（一次 attempt 必落入 failure / empty / non_empty 之一）：
//   - attempt   ：一次探测尝试（ad 文件读取 / env 读取 / NSS 解析）
//   - failure   ：读失败（文件/env/NSS 错误、校验不过、GlobalJobId 不匹配）
//   - empty     ：读成功但主字段（acct_group）为空 —— 合法缺失，不是错误
//   - non_empty ：成功取到 acct_group（顺带记录段数分布）
//
// 能力边界：本统计只能发现“格式”问题（读失败、空值、段数异常）；
// “AcctGroup 第 N 段 = 站点显示组”之类的语义漂移 core 检测不了，责任在消费方。
class GroupProbeStats {
public:
    enum class Source { CondorAd = 0, SlurmEnv = 1, SlurmNss = 2, COUNT = 3 };
    static constexpr size_t kSourceCount = static_cast<size_t>(Source::COUNT);

    static GroupProbeStats& instance();

    void record_attempt(Source s);
    void record_failure(Source s);
    void record_empty(Source s);
    // 成功取到 acct_group 时调用；segments 分布按 '.' 切分统计，异常段数记 255。
    void record_non_empty(Source s, const std::string& acct_group);
    // 熔断/负缓存期间跳过的 NSS 解析。
    void record_skipped(Source s);
    // Prometheus label 截断（>128 字节）计数。
    void record_truncated();

    nlohmann::json to_json() const;

private:
    GroupProbeStats() = default;

    struct Counters {
        std::atomic<uint64_t> attempts{0};
        std::atomic<uint64_t> failures{0};
        std::atomic<uint64_t> empty{0};
        std::atomic<uint64_t> non_empty{0};
        std::atomic<uint64_t> skipped{0};
    };
    std::array<Counters, kSourceCount> counters_{};
    std::atomic<uint64_t> truncated_{0};

    mutable std::mutex seg_mtx_;
    std::map<uint32_t, uint64_t> segments_{};

    static const char* source_name(Source s);
};
