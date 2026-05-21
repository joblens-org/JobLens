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
// ========================= prometheus_exporter_writer.hpp =========================
#pragma once
#include "writer/base_writer.hpp"
#include "common/local_rpc.hpp"
#include <nlohmann/json.hpp>
#include <thread>
#include <sstream>
#include <shared_mutex>
#include <atomic>
#include <unordered_map>

using json = nlohmann::json;

class PrometheusExporterWriter : public BaseWriter
{
public:
    // config_name 里放 fifo 路径，例如 "/tmp/prom_metrics.fifo"
    explicit PrometheusExporterWriter(const std::string& name,
                                      std::string type,
                                      const std::string& config_name);

    ~PrometheusExporterWriter() override;

    struct prometheus_process_state
    {
        uint32_t job_id;          // job_id
        pid_t    pid;             // pid
        std::string name;         // name   （进程名，低基数即可）

        /* ========== CPU ========== */
        double cpu_usage_percent; // job_cpu_usage_percent  (Gauge)

        /* ========== 内存 ========== */
        double mem_usage_percent; // job_memory_usage_percent (Gauge)
        int64_t mem_rss_kb;       // job_memory_rss_kb        (Gauge)
        int64_t mem_vm_kb;        // job_memory_vm_kb         (Gauge)
        int32_t threads_cnt;      // job_threads_count        (Gauge)

        /* ========== I/O ========== */
        // 累计值 → Counter
        int64_t io_read_bytes_total;   // job_io_read_bytes_total   (Counter)
        int64_t io_write_bytes_total;  // job_io_write_bytes_total  (Counter)
        // 瞬时速率 → Gauge（若不想用 rate() 可直用）
        double  io_read_bytes_per_sec;  // job_io_read_bytes_per_sec  (Gauge)
        double  io_write_bytes_per_sec; // job_io_write_bytes_per_sec (Gauge)

        // 累计字节（TCP  only）
        uint32_t tcp_conns             = 0; // tcp_connections                (Counter)
        int64_t net_sent_bytes_total   = 0; // job_network_sent_bytes_total   (Counter)
        int64_t net_recv_bytes_total   = 0; // job_network_recv_bytes_total   (Counter)
        // 瞬时速率
        double  net_send_bytes_per_sec = 0; // job_network_send_bytes_per_sec (Gauge)
        double  net_recv_bytes_per_sec = 0; // job_network_recv_bytes_per_sec (Gauge)
        // 重传 & RTT（TCP only）
        uint64_t tcp_retrans_total = 0;     // job_network_tcp_retrans_total (Counter)
        uint32_t tcp_rtt_us = 0;            // job_network_tcp_rtt_us        (Gauge)

    };

    struct prometheus_job_state
    {
        int JobID{};
        std::vector<prometheus_process_state> processes_state;
    };

protected:
    bool flush_impl(const std::vector<write_data>& batch) override;

private:
    void register_rpc_methods();
    void update_job_metrics(const prometheus_job_state& data,const std::string& type);
    json json_metrics();

    std::string fifo_path_;
    std::thread server_thread_;
    std::atomic<bool> stop_{false};

    mutable std::shared_mutex mtx_;
    std::unordered_map<int, prometheus_job_state> job_metrics_;
};

/*-------------------- nlohmann JSON 序列化 --------------------*/
template <>
struct nlohmann::adl_serializer<PrometheusExporterWriter::prometheus_process_state>
{
    static json to_json(const PrometheusExporterWriter::prometheus_process_state& s);
};

template <>
struct nlohmann::adl_serializer<PrometheusExporterWriter::prometheus_job_state>
{
    static json to_json(const PrometheusExporterWriter::prometheus_job_state& j);
};