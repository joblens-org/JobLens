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

#include <cstdint>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

// 中立的指标快照类型。从 PrometheusExporterWriter 的私有状态中抽出，
// 供采集器在 collect() 内直接构造（MetricSnapshot），writer 只负责合并。
//
// 迁移约束：字段名、单位、整数宽度保持与原 prometheus_job_state /
// prometheus_process_state 完全一致；这里是同一 schema 的具名搬移，
// 不是新指标模型。

struct ProcessMetricSnapshot
{
    uint32_t job_id = 0;      // job_id
    pid_t    pid = 0;         // pid
    std::string name;         // name   （进程名，低基数即可）

    /* ========== CPU ========== */
    double cpu_usage_percent = 0.0; // job_cpu_usage_percent  (Gauge)

    /* ========== 内存 ========== */
    double mem_usage_percent = 0.0; // job_memory_usage_percent (Gauge)
    int64_t mem_rss_kb = 0;   // job_memory_rss_kb        (Gauge)
    int64_t mem_vm_kb = 0;    // job_memory_vm_kb         (Gauge)
    int32_t threads_cnt = 0;  // job_threads_count        (Gauge)

    /* ========== I/O ========== */
    int64_t io_read_bytes_total = 0;      // job_io_read_bytes_total   (Counter)
    int64_t io_write_bytes_total = 0;     // job_io_write_bytes_total  (Counter)
    double  io_read_bytes_per_sec = 0.0;  // job_io_read_bytes_per_sec  (Gauge)
    double  io_write_bytes_per_sec = 0.0; // job_io_write_bytes_per_sec (Gauge)

    // 累计字节（TCP only）
    uint32_t tcp_conns             = 0; // tcp_connections                (Counter)
    int64_t net_sent_bytes_total   = 0; // job_network_sent_bytes_total   (Counter)
    int64_t net_recv_bytes_total   = 0; // job_network_recv_bytes_total   (Counter)
    // 瞬时速率
    double  net_send_bytes_per_sec = 0; // job_network_send_bytes_per_sec (Gauge)
    double  net_recv_bytes_per_sec = 0; // job_network_recv_bytes_per_sec (Gauge)
    // 重传 & RTT（TCP only）
    uint64_t tcp_retrans_total = 0;     // job_network_tcp_retrans_total (Counter)
    uint32_t tcp_rtt_us = 0;            // job_network_tcp_rtt_us        (Gauge)

    /* ========== FS Metadata ========== */
    uint64_t fs_metadata_ops_total = 0;     // job_fs_metadata_ops_total   (Counter)
    uint64_t fs_metadata_errors_total = 0;  // job_fs_metadata_errors_total (Counter)
    double  fs_metadata_ops_per_sec = 0.0;  // job_fs_metadata_ops_per_sec (Gauge)
    double  fs_metadata_errors_per_sec = 0.0;  // job_fs_metadata_errors_per_sec (Gauge)
};

struct MetricSnapshot
{
    int JobID{};
    std::vector<ProcessMetricSnapshot> processes_state;
    // Job 级 I/O（NewIOUsageCollector：VFS syscall 层，弃用物理块字段）
    int64_t io_rchar_total = 0;
    int64_t io_wchar_total = 0;
    double  io_rchar_per_sec = 0.0;
    double  io_wchar_per_sec = 0.0;
};

// JSON 序列化：字段名与既有 writer 输出保持一致。
nlohmann::json process_metric_to_json(const ProcessMetricSnapshot& s);
nlohmann::json metric_snapshot_to_json(const MetricSnapshot& j);

inline bool operator==(const ProcessMetricSnapshot& a, const ProcessMetricSnapshot& b)
{
    return a.job_id == b.job_id && a.pid == b.pid && a.name == b.name
        && a.cpu_usage_percent == b.cpu_usage_percent
        && a.mem_usage_percent == b.mem_usage_percent
        && a.mem_rss_kb == b.mem_rss_kb && a.mem_vm_kb == b.mem_vm_kb
        && a.threads_cnt == b.threads_cnt
        && a.io_read_bytes_total == b.io_read_bytes_total
        && a.io_write_bytes_total == b.io_write_bytes_total
        && a.io_read_bytes_per_sec == b.io_read_bytes_per_sec
        && a.io_write_bytes_per_sec == b.io_write_bytes_per_sec
        && a.tcp_conns == b.tcp_conns
        && a.net_sent_bytes_total == b.net_sent_bytes_total
        && a.net_recv_bytes_total == b.net_recv_bytes_total
        && a.net_send_bytes_per_sec == b.net_send_bytes_per_sec
        && a.net_recv_bytes_per_sec == b.net_recv_bytes_per_sec
        && a.tcp_retrans_total == b.tcp_retrans_total
        && a.tcp_rtt_us == b.tcp_rtt_us
        && a.fs_metadata_ops_total == b.fs_metadata_ops_total
        && a.fs_metadata_errors_total == b.fs_metadata_errors_total
        && a.fs_metadata_ops_per_sec == b.fs_metadata_ops_per_sec
        && a.fs_metadata_errors_per_sec == b.fs_metadata_errors_per_sec;
}

inline bool operator==(const MetricSnapshot& a, const MetricSnapshot& b)
{
    return a.JobID == b.JobID && a.processes_state == b.processes_state
        && a.io_rchar_total == b.io_rchar_total && a.io_wchar_total == b.io_wchar_total
        && a.io_rchar_per_sec == b.io_rchar_per_sec && a.io_wchar_per_sec == b.io_wchar_per_sec;
}
