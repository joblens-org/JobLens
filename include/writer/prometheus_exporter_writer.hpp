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
#include "core/output_record.h"
#include <nlohmann/json.hpp>
#include <chrono>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class PrometheusExporterWriter : public BaseWriter
{
public:
    explicit PrometheusExporterWriter(const std::string& name,
                                      std::string type,
                                      const std::string& config_name);

    ~PrometheusExporterWriter() override;

    bool accepts(OutputKind kind) const override { return kind == OutputKind::Metrics; }

protected:
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override;

private:
    struct SeriesKey {
        std::string name;
        std::vector<MetricLabel> labels;

        bool operator==(const SeriesKey& other) const
        {
            return name == other.name && labels == other.labels;
        }
        bool operator<(const SeriesKey& other) const;
    };

    struct SeriesKeyHash {
        size_t operator()(const SeriesKey& key) const;
    };

    struct Series {
        MetricType type = MetricType::Gauge;
        std::string help;
        double value = 0.0;
        uint64_t job_id = 0;
    };

    // Called under mtx_; group is projected at scrape time so a late group
    // discovery doesn't leave a duplicate series under the old labels.
    std::vector<MetricLabel> labels_with_group(const SeriesKey& key, const Series& series) const;
    void register_rpc_methods();
    void update_batch(const MetricBatch& batch, const SampleMetadata& sample);
    nlohmann::json json_metrics() const;
    std::string prometheus_text() const;

    mutable std::shared_mutex mtx_;
    std::unordered_map<SeriesKey, Series, SeriesKeyHash> series_;
    std::unordered_set<uint64_t> job_ids_;
    std::unordered_map<uint64_t, std::string> job_groups_;
    uint64_t invalid_samples_ = 0;
};
