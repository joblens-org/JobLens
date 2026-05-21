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
// ========================= prometheus_exporter_writer.cpp =========================
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "writer/prometheus_exporter_writer.hpp"
#include <spdlog/spdlog.h>
#include "core/collector_registry.hpp"
#include "core/writer_manager.hpp"
#include <ctime>

AUTO_REGISTER_WRITER(
    PrometheusExporterWriter,
    "Writer for Prometheus Exporter via FIFO",
    ConfigParams{
        {"fifo_path", "Path to the FIFO file, e.g., /tmp/prom_metrics.fifo"}
    }
)

PrometheusExporterWriter::PrometheusExporterWriter(const std::string& name,
                                                   std::string type,
                                                   const std::string& config_name)
    : BaseWriter(name, "PrometheusExporterWriter", config_name)
      {

    // 注册RPC方法
    register_rpc_methods();
    
}

PrometheusExporterWriter::~PrometheusExporterWriter(){

}

enum class PrmxsCollectorType{
    CPUMem,
    IO,
    Net,
    UNKNOWN
};

static inline PrmxsCollectorType type2enum(const std::string& type){
    static const std::unordered_map<std::string, PrmxsCollectorType> map = {
        {"CPUMemCollector", PrmxsCollectorType::CPUMem},
        {"NetUsageCollector", PrmxsCollectorType::Net},
        {"IOUsageCollector", PrmxsCollectorType::IO}
    };
    auto it = map.find(type);
    return (it != map.end()) ? it->second : PrmxsCollectorType::UNKNOWN;
}

void PrometheusExporterWriter::update_job_metrics(const prometheus_job_state& data,const std::string& type){
    if (!data.JobID) return;
    auto& metric = job_metrics_[data.JobID];
    prometheus_process_state* update_ref;
    prometheus_process_state new_state = {};
    
    metric.JobID = data.JobID;
    spdlog::debug("PrometheusExporterWriter: update job id: {}", metric.JobID);
    
    // 这里采用细粒度锁的模式可以获得更高的性能，但要注意并发写的问题
    // 开始写操作，加锁
    std::unique_lock lk(mtx_);
    
    for(auto& s: data.processes_state){
        bool push = false;
        auto pid = s.pid;
        auto it = std::find_if(metric.processes_state.begin(), metric.processes_state.end(),
            [pid](prometheus_process_state data_s){
                return (pid == data_s.pid) ? true : false;
            });
        if (it != metric.processes_state.end()){
            update_ref = &(*it);
        }else{
            update_ref = &new_state;
            update_ref->job_id = data.JobID;
            update_ref->pid = s.pid;
            push = true;
        }
        spdlog::debug("PrometheusExporterWriter: use type {}", static_cast<int>(type2enum(type)));
        spdlog::debug("PrometheusExporterWriter: update job pid: {}", pid);
        switch (type2enum(type))
        {
            case PrmxsCollectorType::CPUMem:
                update_ref->name = s.name;
                update_ref->cpu_usage_percent = s.cpu_usage_percent;
                update_ref->mem_rss_kb = s.mem_rss_kb;
                update_ref->threads_cnt = s.threads_cnt;
                update_ref->mem_usage_percent = s.mem_usage_percent;
                update_ref->mem_vm_kb = s.mem_vm_kb;
                break;
            
            case PrmxsCollectorType::IO:
                update_ref->io_read_bytes_per_sec = s.io_read_bytes_per_sec;
                update_ref->io_read_bytes_total = s.io_read_bytes_total;
                update_ref->io_write_bytes_per_sec = s.io_write_bytes_per_sec;
                update_ref->io_write_bytes_total = s.io_write_bytes_total;
                break;
            
            case PrmxsCollectorType::Net:
                update_ref->net_recv_bytes_per_sec = s.net_recv_bytes_per_sec;
                update_ref->net_recv_bytes_total = s.net_recv_bytes_total;
                update_ref->net_send_bytes_per_sec = s.net_send_bytes_per_sec;
                update_ref->net_sent_bytes_total = s.net_sent_bytes_total;
                update_ref->tcp_retrans_total = s.tcp_retrans_total;
                update_ref->tcp_rtt_us = s.tcp_rtt_us;
                break;
            case PrmxsCollectorType::UNKNOWN:
                spdlog::warn("PrometheusExporterWriter: type2enum get UNKNOW for {}", type);
                break;
            default:
                break;
        }

        if(push){
            metric.processes_state.push_back(*update_ref);
        }
    }

    return;
}

bool PrometheusExporterWriter::flush_impl(const std::vector<write_data>& batch){
    for (const auto& [collect_name, job, any_data, ts] : batch)
    {
        spdlog::debug("PrometheusExporterWriter: get in flush");
        auto parser_func = CollectorRegistry::instance().getCollectorParser(collect_name, type_);
        auto collector_type = CollectorRegistry::instance().getCollectorType(collect_name);
        auto parsed = std::any_cast<PrometheusExporterWriter::prometheus_job_state>(parser_func(any_data));
        spdlog::debug("PrometheusExporterWriter: size of parsed: {}", parsed.processes_state.size());
        parsed.JobID = job.JobID;
        update_job_metrics(parsed, collector_type);
    }
    return true;
}

void PrometheusExporterWriter::register_rpc_methods() {
    auto& rpc_server_ = RPCServer::instance();
    // 注册metrics方法，返回序列化的指标数据
    rpc_server_.register_method(name_ + "/metrics", [this](const json& params) -> json {
        spdlog::debug("PrometheusExporterWriter: handling metrics request");
        try {
            return json_metrics();
        } catch (const std::exception& e) {
            spdlog::error("PrometheusExporterWriter: error serializing metrics: {}", e.what());
            json error_response = {{ "error", e.what() }};
            return error_response;
        }
    });
    
    // 注册info方法，返回基本信息
    rpc_server_.register_method(name_ + "/info", [this](const json& params) -> json {
        json response = {
            {"name", name_},
            {"type", type_},
            {"config_name", config_name_},
            {"fifo_path", fifo_path_},
            {"job_count", job_metrics_.size()},
            {"running", !stop_.load()}
        };
        return response;
    });

    spdlog::info("PrometheusExporterWriter: registered RPC methods: metrics, info");
}

json PrometheusExporterWriter::json_metrics(){
    std::shared_lock lk(mtx_);
    json wrapper = json::object();
    for (const auto& [key, value] : job_metrics_)
        wrapper[std::to_string(key)] = nlohmann::adl_serializer<prometheus_job_state>::to_json(value);
    return wrapper;
}

/*-------------------- process_state 序列化 --------------------*/
json nlohmann::adl_serializer<PrometheusExporterWriter::prometheus_process_state>::to_json(
    const PrometheusExporterWriter::prometheus_process_state& s){
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
        {"tcp_rtt_us",                 s.tcp_rtt_us}
    };
}

/*-------------------- job_state 序列化 --------------------*/
json nlohmann::adl_serializer<PrometheusExporterWriter::prometheus_job_state>::to_json(
    const PrometheusExporterWriter::prometheus_job_state& j){
    json proc_array = json::array();
    for (const auto& p : j.processes_state)
        proc_array.push_back(nlohmann::adl_serializer<PrometheusExporterWriter::prometheus_process_state>::to_json(p));

    return json{
        {"JobID",         j.JobID},
        {"process_state", proc_array}
    };
}