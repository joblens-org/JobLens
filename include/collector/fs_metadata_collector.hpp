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

#include "core/collector_type.h"
#include "icollector.h"
#include <spdlog/spdlog.h>
#include "common/ebpf_common.hpp"
#include "ebpf/fs_metadata.h"
#include <chrono>
#include <unordered_map>
#include <string>
#include <vector>

/**
 * @brief 单操作类型的元数据统计（对标 new_io 的 IoCounters）
 *
 * 每个操作类型（如 open、close、getattr 等）的详细统计信息。
 */
struct FSMetaOpCounters {
    uint32_t op_id{0};            ///< 操作类型ID，对应 fs_meta_op 枚举值
    std::string op_name;          ///< 操作类型的人类可读名称（如 "open"、"getattr"）
    u64 calls{0};                 ///< 该操作的总调用次数
    u64 success{0};               ///< 成功完成的调用次数
    u64 errors{0};                ///< 失败的调用次数
    s64 last_errno{0};            ///< 最后一次错误的错误码（0表示无错误）
    u64 total_latency_ns{0};      ///< 该操作的总延迟（纳秒）
    u64 max_latency_ns{0};        ///< 单次调用的最大延迟（纳秒）
    double calls_rate{0};         ///< 每秒调用次数（用户态差分）
    double error_rate{0};         ///< 每秒错误次数（用户态差分）
};

/**
 * @brief 单操作类型的时延直方图（读/写不区分，元数据操作无读写之分）
 *
 * hist[bucket] = 落入该时延桶的调用次数。桶边界见 fs_metadata.bpf.c: latency_bucket()。
 */
struct FSMetaLatencyHist {
    u64 hist[FS_META_LATENCY_BUCKETS]{};
};

/**
 * @brief 进程级元数据统计（含短命进程，对标 new_io 的 ProcIOStat）
 */
struct FSMetaProcStat {
    pid_t pid{0};
    std::string source;                       ///< "alive" | "ephemeral"
    bool alive{false};
    std::string mount_point;                  ///< 挂载点路径（未知则为空）
    std::string fs_type;                      ///< 文件系统类型（未知则为空）
    std::unordered_map<uint32_t, FSMetaOpCounters> ops;  ///< 按 op_id 索引的操作统计
    u64 metadata_ops_total{0};                ///< 所有操作的总调用次数
    double metadata_ops_rate{0};              ///< 每秒元数据操作总数
};

/**
 * @brief 顶层：一次 collect 返回一个 Job 的完整元数据快照（对标 new_io 的 JobIOStat）
 */
struct JobFSMetaStat {
    uint64_t job_id{0};
    double collect_period{0};                            ///< 距上次采集的墙钟间隔（秒）
    std::unordered_map<uint32_t, FSMetaOpCounters> job_ops;  ///< Job 级按 op 聚合（含短命进程）
    std::unordered_map<uint32_t, FSMetaLatencyHist> job_latency;  ///< Job 级各 op 的时延直方图
    u64 job_metadata_ops_total{0};                       ///< Job 级总调用次数
    double job_metadata_ops_rate{0};                     ///< Job 级每秒元数据操作总数
    std::unordered_map<pid_t, FSMetaProcStat> processes; ///< 按进程细分
};

/**
 * @brief 文件系统元数据压力采集器
 *
 * 通过 eBPF 探针采集进程级别的文件系统元数据操作统计，包括 open、close、getattr、
 * readdir 等操作的频率、延迟（桶计数分布）和错误率。该采集器不依赖特定文件系统。
 *
 * 数据链路对标 NewIOUsageCollector：
 *   - 内核态: eBPF hook 各元数据 syscall → 全量累加到 {job_id,pid,op} 明细 map、
 *             {job_id,op} Job 级 map、{job_id,op,bucket} 时延直方图；
 *   - 用户态: batch dump 明细 map（周期缓存）按 Job 切片聚合进程；Job 级 map + 直方图
 *             按 job_id 直接切片；速率由用户态差分。
 */
class FSMetadataCollector : public ICollector {
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;

private:
    bool init_ebpf();
    void deinit_ebpf();

    // 周期缓存：DUMP_TTL 内共享一次全表遍历（对标 new_io 的 refresh_dump_cache_if_needed）
    void refresh_dump_cache_if_needed();
    // 清理已死且已输出过的短命进程的 eBPF 明细条目（保证"至少输出一次"后延迟清理）
    void cleanup_dead_pids();

    double collect_period{1.0};             ///< 采集周期（秒，来自 freq 配置）
    bool summary{false};                    ///< 是否输出聚合摘要

    // eBPF 对象文件路径与 map 名（对标 new_io）
    std::string bpf_o_path = JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/fs_metadata.bpf.o";
    std::string fs_meta_map_name{"fs_meta_stat"};             ///< {job_id,pid,op} 明细 map
    std::string fs_meta_job_map_name{"fs_meta_job_stat"};     ///< {job_id,op} Job 级 map
    std::string fs_meta_latency_map_name{"fs_meta_latency_hist"};  ///< {job_id,op,bucket} 时延桶

    bpf_object* bpf_obj_{nullptr};
    std::vector<bpf_link*> bpf_links_;

    // 周期遍历缓存（对标 new_io 的 dump_keys_/dump_vals_）
    static constexpr int DUMP_TTL_MS = 200;
    std::chrono::steady_clock::time_point last_dump_time_{};
    std::vector<fs_meta_key> dump_keys_;
    std::vector<fs_meta_stat> dump_vals_;

    // 短命进程生命周期状态
    struct EphemeralState { uint64_t job_id{0}; uint64_t output_count{0}; bool alive{false}; };
    std::unordered_map<pid_t, EphemeralState> known_pids_;

    // 速率差分基线（(pid<<32|op) → 上次 calls；job 级 op → 上次 calls）
    std::unordered_map<uint64_t, u64> last_proc_op_calls_;
    std::unordered_map<uint32_t, u64> last_job_op_calls_;
    std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> last_job_time_;
};
