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
 * @brief 文件系统元数据操作统计信息（单操作类型）
 * 
 * 每个操作类型（如 open、close、getattr 等）的详细统计信息
 */
struct FSMetadataOpInfo {
    uint32_t op_id;              ///< 操作类型ID，对应 fs_meta_op 枚举值
    std::string op_name;         ///< 操作类型的人类可读名称（如 "open"、"getattr"）
    u64 calls;                   ///< 该操作的总调用次数
    u64 success;                 ///< 成功完成的调用次数
    u64 errors;                  ///< 失败的调用次数
    int64_t last_errno;          ///< 最后一次错误的错误码（0表示无错误）
    u64 total_latency_ns;        ///< 该操作的总延迟（纳秒）
    u64 max_latency_ns;          ///< 单次调用的最大延迟（纳秒）
    double calls_rate;           ///< 每秒调用次数（由采集器计算）
    double error_rate;           ///< 每秒错误次数（由采集器计算）
};

/**
 * @brief 进程级文件系统元数据统计信息
 * 
 * 聚合单个进程的所有文件系统元数据操作统计
 */
struct FSMetadataProcessInfo {
    pid_t pid;                              ///< 进程ID
    std::string mount_point;                ///< 挂载点路径（未知则为空字符串）
    std::string fs_type;                    ///< 文件系统类型（未知则为空字符串）
    std::vector<FSMetadataOpInfo> ops;      ///< 每个跟踪的操作类型统计信息
    u64 metadata_ops_total;                 ///< 所有操作的总调用次数
    double metadata_ops_rate;               ///< 每秒元数据操作总数
};

/**
 * @brief 文件系统元数据压力采集器
 * 
 * 通过 eBPF 探针或 /proc 接口采集进程级别的文件系统元数据操作统计，
 * 包括 open、close、getattr、readdir 等操作的频率、延迟和错误率。
 * 
 * 该采集器是通用的，不依赖于特定文件系统（如 Lustre）。
 */
class FSMetadataCollector : public ICollector {
public:
    /**
     * @brief 初始化采集器
     * @param cfg 配置JSON对象
     * @return 初始化成功返回 true，失败返回 false
     */
    bool init(const nlohmann::json& cfg) override;

    /**
     * @brief 执行采集操作
     * @param job 要采集的作业对象
     * @return 采集结果
     */
    CollectResult collect(const Job& job) override;

    /**
     * @brief 反初始化采集器，释放资源
     */
    void deinit() noexcept override;

    /**
     * @brief 获取指定writer类型的数据解析函数
     * @param writer_type writer类型名称
     * @return 数据解析函数
     */
    CollectDataParseFunc get_writer_parser(const std::string& writer_type) override;

private:
    double collect_period{1.0};             ///< 采集周期（秒）
    bool use_ebpf{false};                   ///< 是否使用eBPF模式
    bool summary{false};                    ///< 是否输出聚合摘要

    // eBPF相关配置和状态
    std::string bpf_o_path = JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/fs_metadata.bpf.o";  ///< BPF对象文件路径
    std::string pid2jobid_map_name{"pid2job"};      ///< PID到JobID映射的map名称
    std::string fs_meta_map_name{"fs_meta_stat"};   ///< 元数据统计map名称

    bpf_object* bpf_obj_{nullptr};          ///< BPF对象指针
    std::vector<bpf_link*> bpf_links_;      ///< BPF探针链接列表

    /**
     * @brief 初始化eBPF子系统
     * @return 初始化成功返回 true，失败返回 false
     */
    bool init_ebpf();

    /**
     * @brief 反初始化eBPF子系统，释放所有eBPF资源
     */
    void deinit_ebpf();

    /**
     * @brief 单个操作的状态跟踪（用于速率计算）
     * 
     * 记录每个(pid, op)组合的上次采集状态，用于计算速率
     */
    struct pid_op_state {
        std::chrono::steady_clock::time_point last_time{};  ///< 上次采集时间戳
        u64 last_calls{};                                     ///< 上次采集时的调用次数
    };

    /**
     * @brief 操作状态字典
     * 
     * 键为 (pid << 32) | op_id，值为该(pid, op)的状态
     */
    std::unordered_map<uint64_t, pid_op_state> op_state_dict_;
};
