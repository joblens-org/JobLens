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
#include "core/collector_type.h"
#include "job_watcher/group_probe_stats.hpp"
#include "core/writer_manager.hpp"

#include <algorithm>
#include <cctype>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

namespace {
bool valid_metric_name(std::string_view name)
{
    if (name.empty()) return false;
    const auto is_first = [](unsigned char c) {
        return std::isalpha(c) || c == '_' || c == ':';
    };
    const auto is_rest = [](unsigned char c) {
        return std::isalnum(c) || c == '_' || c == ':';
    };
    if (!is_first(static_cast<unsigned char>(name.front()))) return false;
    for (size_t index = 1; index < name.size(); ++index) {
        if (!is_rest(static_cast<unsigned char>(name[index]))) return false;
    }
    return true;
}

bool valid_label_name(std::string_view name)
{
    if (name.empty()) return false;
    const auto is_first = [](unsigned char c) {
        return std::isalpha(c) || c == '_';
    };
    const auto is_rest = [](unsigned char c) {
        return std::isalnum(c) || c == '_';
    };
    if (!is_first(static_cast<unsigned char>(name.front()))) return false;
    for (size_t index = 1; index < name.size(); ++index) {
        if (!is_rest(static_cast<unsigned char>(name[index]))) return false;
    }
    return true;
}

std::string escape_help(std::string_view help)
{
    std::string out;
    out.reserve(help.size());
    for (char value : help) {
        if (value == '\\') out += "\\\\";
        else if (value == '\n') out += "\\n";
        else out.push_back(value);
    }
    return out;
}

std::string escape_label(std::string_view value)
{
    std::string out;
    out.reserve(value.size());
    for (char character : value) {
        if (character == '\\') out += "\\\\";
        else if (character == '"') out += "\\\"";
        else if (character == '\n') out += "\\n";
        else out.push_back(character);
    }
    return out;
}
} // namespace

AUTO_REGISTER_WRITER(
    PrometheusExporterWriter,
    "Generic Prometheus metrics writer exposed through RPC",
    ConfigParams{}
)

PrometheusExporterWriter::PrometheusExporterWriter(const std::string& name,
                                                   std::string type,
                                                   const std::string& config_name)
    : BaseWriter(name, "PrometheusExporterWriter", config_name)
{
    register_rpc_methods();
}

PrometheusExporterWriter::~PrometheusExporterWriter()
{
    shutdown();
}

bool PrometheusExporterWriter::SeriesKey::operator<(const SeriesKey& other) const
{
    if (name != other.name) return name < other.name;
    const size_t count = std::min(labels.size(), other.labels.size());
    for (size_t index = 0; index < count; ++index) {
        if (labels[index].name != other.labels[index].name)
            return labels[index].name < other.labels[index].name;
        if (labels[index].value != other.labels[index].value)
            return labels[index].value < other.labels[index].value;
    }
    return labels.size() < other.labels.size();
}

size_t PrometheusExporterWriter::SeriesKeyHash::operator()(const SeriesKey& key) const
{
    size_t hash = std::hash<std::string>{}(key.name);
    for (const auto& label : key.labels) {
        hash ^= std::hash<std::string>{}(label.name) + 0x9e3779b97f4a7c15ULL +
                (hash << 6) + (hash >> 2);
        hash ^= std::hash<std::string>{}(label.value) + 0x9e3779b97f4a7c15ULL +
                (hash << 6) + (hash >> 2);
    }
    return hash;
}

void PrometheusExporterWriter::update_batch(const MetricBatch& batch,
                                            const SampleMetadata& sample)
{
    auto group = getGroups(sample.job).acct_group;
    if (group.size() > 128) {
        group = group.substr(0, 125) + "...";
        GroupProbeStats::instance().record_truncated();
    }
    std::unique_lock lock(mtx_);
    if (!group.empty()) job_groups_[sample.job.JobID] = std::move(group);
    for (const auto& incoming : batch.samples) {
        if (!valid_metric_name(incoming.name)) {
            ++invalid_samples_;
            continue;
        }

        std::vector<MetricLabel> labels;
        labels.reserve(incoming.labels.size() + 1);
        bool invalid = false;
        for (const auto& label : incoming.labels) {
            if (!valid_label_name(label.name)) {
                invalid = true;
                break;
            }
            auto existing = std::find_if(labels.begin(), labels.end(),
                [&](const MetricLabel& value) { return value.name == label.name; });
            if (existing != labels.end()) existing->value = label.value;
            else labels.push_back(label);
        }
        if (invalid) {
            ++invalid_samples_;
            continue;
        }
        if (std::none_of(labels.begin(), labels.end(),
                         [](const MetricLabel& label) { return label.name == "jobid"; })) {
            labels.push_back({"jobid", std::to_string(sample.job.JobID)});
        }
        std::sort(labels.begin(), labels.end(),
                  [](const MetricLabel& lhs, const MetricLabel& rhs) {
                      return lhs.name < rhs.name;
                  });

        SeriesKey key{incoming.name, std::move(labels)};
        auto [it, inserted] = series_.try_emplace(std::move(key), Series{});
        Series& series = it->second;
        if (inserted) {
            series.type = incoming.type;
            series.help = incoming.help;
            series.job_id = sample.job.JobID;
            job_ids_.insert(sample.job.JobID);
        } else {
            if (series.type != incoming.type) {
                spdlog::warn("PrometheusExporterWriter: metric {} changed type from {} to {}; keeping {}",
                             it->first.name, to_string(series.type), to_string(incoming.type),
                             to_string(series.type));
            }
            if (series.help.empty()) series.help = incoming.help;
        }
        series.value = incoming.value;
    }
}

bool PrometheusExporterWriter::flush_impl(const std::vector<OutputRecordPtr>& batch)
{
    for (const auto& record : batch) {
        if (!record || !record->sample) {
            spdlog::error("PrometheusExporterWriter: native record without sample metadata");
            continue;
        }
        const auto* metrics = std::get_if<MetricBatch>(&record->payload);
        if (!metrics) {
            spdlog::error("PrometheusExporterWriter: native payload is not a MetricBatch, collector={}, record={}",
                          record->sample->collector_name, record->record_id);
            continue;
        }
        update_batch(*metrics, *record->sample);
    }
    return true;
}

void PrometheusExporterWriter::register_rpc_methods()
{
    auto& rpc_server = RPCServer::instance();
    rpc_server.register_method(name_ + "/metrics", [this](const json&) -> json {
        return json_metrics();
    });
    rpc_server.register_method(name_ + "/metrics_text", [this](const json&) -> json {
        return prometheus_text();
    });
    rpc_server.register_method(name_ + "/info", [this](const json&) -> json {
        std::shared_lock lock(mtx_);
        return {
            {"name", name_},
            {"type", type_},
            {"config_name", config_name_},
            {"series_count", series_.size()},
            {"job_count", job_ids_.size()},
            {"invalid_samples", invalid_samples_},
        };
    });
}

std::vector<MetricLabel> PrometheusExporterWriter::labels_with_group(
    const SeriesKey& key, const Series& series) const
{
    auto labels = key.labels;
    if (auto group = job_groups_.find(series.job_id); group != job_groups_.end()) {
        auto pos = std::lower_bound(labels.begin(), labels.end(), "group",
            [](const MetricLabel& label, std::string_view name) { return label.name < name; });
        if (pos != labels.end() && pos->name == "group") pos->value = group->second;
        else labels.insert(pos, MetricLabel{"group", group->second});
    }
    return labels;
}

nlohmann::json PrometheusExporterWriter::json_metrics() const
{
    std::shared_lock lock(mtx_);
    std::vector<const std::pair<const SeriesKey, Series>*> ordered;
    ordered.reserve(series_.size());
    for (const auto& entry : series_) ordered.push_back(&entry);
    std::sort(ordered.begin(), ordered.end(),
              [](const auto* lhs, const auto* rhs) { return lhs->first < rhs->first; });

    auto metrics = json::array();
    for (const auto* entry : ordered) {
        json labels = json::object();
        for (const auto& label : labels_with_group(entry->first, entry->second)) labels[label.name] = label.value;
        metrics.push_back({
            {"name", entry->first.name},
            {"type", to_string(entry->second.type)},
            {"help", entry->second.help},
            {"labels", std::move(labels)},
            {"value", entry->second.value},
        });
    }
    return {{"metrics", std::move(metrics)}};
}

std::string PrometheusExporterWriter::prometheus_text() const
{
    std::shared_lock lock(mtx_);
    std::vector<const std::pair<const SeriesKey, Series>*> ordered;
    ordered.reserve(series_.size());
    for (const auto& entry : series_) ordered.push_back(&entry);
    std::sort(ordered.begin(), ordered.end(),
              [](const auto* lhs, const auto* rhs) { return lhs->first < rhs->first; });

    std::string output;
    std::string current_name;
    for (const auto* entry : ordered) {
        const auto& key = entry->first;
        const auto& series = entry->second;
        if (key.name != current_name) {
            current_name = key.name;
            if (!series.help.empty())
                output += "# HELP " + key.name + " " + escape_help(series.help) + "\n";
            output += "# TYPE " + key.name + " " + to_string(series.type) + "\n";
        }
        output += key.name;
        const auto labels = labels_with_group(key, series);
        if (!labels.empty()) {
            output.push_back('{');
            for (size_t index = 0; index < labels.size(); ++index) {
                if (index) output.push_back(',');
                output += labels[index].name;
                output += "=\"";
                output += escape_label(labels[index].value);
                output.push_back('"');
            }
            output.push_back('}');
        }
        output.push_back(' ');
        output += fmt::format("{}", series.value);
        output.push_back('\n');
    }
    return output;
}
