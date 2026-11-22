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
#include "core/output_context.hpp"

#include <limits>

#include "writer/base_writer.hpp"

namespace {
const std::vector<OutputTarget> kNoTargets{};
}

OutputContext::OutputContext(std::shared_ptr<const OutputRouter> router,
                             std::shared_ptr<const SampleMetadata> sample)
    : router_(std::move(router)), sample_(std::move(sample))
{
}

bool OutputContext::enabled(std::string_view channel) const
{
    const auto* route = router_ ? router_->find(channel) : nullptr;
    return route != nullptr && !route->writers.empty();
}

const std::vector<OutputTarget>& OutputContext::targets(std::string_view channel) const
{
    const auto* route = router_ ? router_->find(channel) : nullptr;
    return route ? route->targets : kNoTargets;
}

EmitReport OutputContext::emit(std::string_view channel, OutputPayload payload)
{
    const bool began = begin_write(channel);
    EmitReport report = emit_internal(channel, std::move(payload));
    end_write(channel, report, began);
    return report;
}

EmitReport OutputContext::emit_to(std::string_view channel,
                                  std::string_view writer_name,
                                  OutputPayload payload)
{
    const bool began = begin_write(channel);
    EmitReport report = emit_to_internal(channel, writer_name, std::move(payload));
    end_write(channel, report, began);
    return report;
}

void OutputContext::set_write_observer(WriteBegin begin, WriteEnd end)
{
    write_begin_ = std::move(begin);
    write_end_ = std::move(end);
}

bool OutputContext::begin_write(std::string_view channel)
{
    if (!write_begin_) return false;
    write_begin_(channel);
    return true;
}

void OutputContext::end_write(std::string_view channel, const EmitReport& report, bool began)
{
    if (!began || !write_end_) return;
    bool failed = report.code == EmitCode::Invalid;
    for (const auto& acceptance : report.writers) {
        if (acceptance.code != EnqueueCode::Accepted) failed = true;
    }
    try {
        write_end_(channel, failed);
    } catch (...) {
    }
}

EmitReport OutputContext::emit_internal(std::string_view channel, OutputPayload payload)
{
    const auto* route = router_ ? router_->find(channel) : nullptr;
    if (!route) {
        record_error("unknown output channel '" + std::string(channel) + "'");
        return {EmitCode::Invalid, {}};
    }
    if (route->writers.empty()) return {EmitCode::Disabled, {}};
    return submit(*route, route->writers, route->targets, channel, std::move(payload));
}

EmitReport OutputContext::emit_to_internal(std::string_view channel,
                                           std::string_view writer_name,
                                           OutputPayload payload)
{
    const auto* route = router_ ? router_->find(channel) : nullptr;
    if (!route) {
        record_error("unknown output channel '" + std::string(channel) + "'");
        return {EmitCode::Invalid, {}};
    }
    for (std::size_t i = 0; i < route->targets.size(); ++i) {
        if (route->targets[i].writer_name != writer_name) continue;
        std::vector<BaseWriter*> destination{route->writers[i]};
        std::vector<OutputTarget> target{route->targets[i]};
        return submit(*route, destination, target, channel, std::move(payload));
    }
    record_error("writer '" + std::string(writer_name) + "' is not bound to channel '" +
                 std::string(channel) + "'");
    return {EmitCode::Invalid, {}};
}

EmitReport OutputContext::submit(const ResolvedChannel& route,
                                 const std::vector<BaseWriter*>& destinations,
                                 const std::vector<OutputTarget>& targets,
                                 std::string_view channel,
                                 OutputPayload payload)
{
    EmitReport report;
    if (!sample_) {
        record_error("missing sample metadata");
        return {EmitCode::Invalid, {}};
    }
    if (kind_of(payload) != route.channel.kind) {
        record_error("payload kind " + std::string(to_string(kind_of(payload))) +
                     " does not match channel '" + std::string(channel) + "' kind " +
                     to_string(route.channel.kind));
        return {EmitCode::Invalid, {}};
    }
    if (next_sequence_ == std::numeric_limits<uint64_t>::max()) {
        record_error("record sequence counter exhausted on channel '" + std::string(channel) + "'");
        return {EmitCode::Invalid, {}};
    }

    const uint64_t sequence = next_sequence_++;
    auto record = std::make_shared<OutputRecord>();
    record->sample = sample_;
    record->channel = std::string(channel);
    record->sequence = sequence;
    record->record_id = sample_->sample_id + ":" + std::to_string(sequence);
    record->payload = std::move(payload);
    record->accounted_bytes = estimate_output_bytes(record->payload);
    OutputRecordPtr owned = record;

    report.code = EmitCode::Submitted;
    report.writers.reserve(destinations.size());
    for (std::size_t i = 0; i < destinations.size(); ++i) {
        WriterAcceptance acceptance;
        acceptance.writer_name = targets[i].writer_name;
        try {
            acceptance.code = destinations[i]->try_enqueue(owned);
        } catch (const std::exception& e) {
            acceptance.code = EnqueueCode::Failed;
            acceptance.error = e.what();
        } catch (...) {
            acceptance.code = EnqueueCode::Failed;
            acceptance.error = "unknown exception";
        }
        if (acceptance.code != EnqueueCode::Accepted) {
            if (acceptance.error.empty()) acceptance.error = to_string(acceptance.code);
            record_error("writer '" + acceptance.writer_name + "' on channel '" +
                         std::string(channel) + "' rejected record: " + acceptance.error);
        }
        report.writers.push_back(std::move(acceptance));
    }
    ++submitted_;
    return report;
}

void OutputContext::record_error(std::string message)
{
    if (errors_.size() < 32) errors_.push_back(std::move(message));
}

std::string OutputContext::error_summary() const
{
    std::string summary;
    for (const auto& error : errors_) {
        if (!summary.empty()) summary += "; ";
        summary += error;
    }
    return summary;
}
