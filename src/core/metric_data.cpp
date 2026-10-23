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
#include "core/metric_data.h"

using json = nlohmann::json;

json process_metric_to_json(const ProcessMetricSnapshot& s)
{
    return json{
        {"job_id",                     s.job_id},
        {"pid",                        s.pid},
        {"name",                       s.name},

        {"cpu_usage_percent",          s.cpu_usage_percent},

        {"mem_usage_percent",          s.mem_usage_percent},
        {"mem_rss_kb",                 s.mem_rss_kb},
        {"mem_vm_kb",                  s.mem_vm_kb},
        {"threads_cnt",                s.threads_cnt},

        {"io_read_bytes_total",        s.io_read_bytes_total},
        {"io_write_bytes_total",       s.io_write_bytes_total},
        {"io_read_bytes_per_sec",      s.io_read_bytes_per_sec},
        {"io_write_bytes_per_sec",     s.io_write_bytes_per_sec},

        {"net_sent_bytes_total",       s.net_sent_bytes_total},
        {"net_recv_bytes_total",       s.net_recv_bytes_total},
        {"net_send_bytes_per_sec",     s.net_send_bytes_per_sec},
        {"net_recv_bytes_per_sec",     s.net_recv_bytes_per_sec},
        {"tcp_retrans_total",          s.tcp_retrans_total},
        {"tcp_rtt_us",                 s.tcp_rtt_us},

        {"fs_metadata_ops_total",      s.fs_metadata_ops_total},
        {"fs_metadata_ops_per_sec",    s.fs_metadata_ops_per_sec},
        {"fs_metadata_errors_total",   s.fs_metadata_errors_total},
        {"fs_metadata_errors_per_sec", s.fs_metadata_errors_per_sec}
    };
}

json metric_snapshot_to_json(const MetricSnapshot& j)
{
    json proc_array = json::array();
    for (const auto& p : j.processes_state)
        proc_array.push_back(process_metric_to_json(p));

    json result{{"JobID", j.JobID}};
    result["process_state"] = std::move(proc_array);
    return result;
}
