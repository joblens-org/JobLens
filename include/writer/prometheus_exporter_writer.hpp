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
#include "core/metric_data.h"
#include "common/local_rpc.hpp"
#include <nlohmann/json.hpp>
#include <thread>
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

    // 迁移期别名：旧 parser 代码/测试仍可通过原名称引用同一 schema。
    using prometheus_process_state = ProcessMetricSnapshot;
    using prometheus_job_state = MetricSnapshot;

    bool accepts(OutputKind kind) const override { return kind == OutputKind::Metrics; }

protected:
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override;

private:
    void register_rpc_methods();
    void update_job_metrics(const MetricSnapshot& data, const std::string& type);
    json json_metrics();

    std::string fifo_path_;
    std::thread server_thread_;
    std::atomic<bool> stop_{false};

    mutable std::shared_mutex mtx_;
    std::unordered_map<int, MetricSnapshot> job_metrics_;
};
