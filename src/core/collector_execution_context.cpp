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
#include "core/collector_execution_context.hpp"

#include <chrono>
#include <utility>

namespace {
constexpr size_t kMaxErrors = 32;
constexpr size_t kMaxErrorLength = 512;

std::string bounded(std::string message)
{
    if (message.size() > kMaxErrorLength) message.resize(kMaxErrorLength);
    return message;
}

std::string join_errors(const std::vector<std::string>& errors)
{
    std::string summary;
    for (const auto& error : errors) {
        if (!summary.empty()) summary += "; ";
        summary += error;
    }
    return summary;
}
} // namespace

OutputSession::OutputSession(std::shared_ptr<const OutputRouter> router,
                             std::shared_ptr<const SampleMetadata> sample,
                             std::function<void(std::string)> error_sink)
    : router_(std::move(router)),
      sample_(std::move(sample)),
      context_(std::make_unique<OutputContext>(router_, sample_)),
      error_sink_(std::move(error_sink))
{
}

OutputSession::~OutputSession()
{
    fold_error();
}

OutputSession::OutputSession(OutputSession&& other) noexcept
    : router_(std::move(other.router_)),
      sample_(std::move(other.sample_)),
      context_(std::move(other.context_)),
      error_sink_(std::move(other.error_sink_))
{
    other.context_.reset();
    other.error_sink_ = nullptr;
}

OutputSession& OutputSession::operator=(OutputSession&& other) noexcept
{
    if (this == &other) return *this;
    fold_error();
    router_ = std::move(other.router_);
    sample_ = std::move(other.sample_);
    context_ = std::move(other.context_);
    error_sink_ = std::move(other.error_sink_);
    other.context_.reset();
    other.error_sink_ = nullptr;
    return *this;
}

void OutputSession::fold_error()
{
    if (!context_ || !error_sink_) return;
    if (context_->had_error()) error_sink_(context_->error_summary());
    error_sink_ = nullptr;
}

JobEventContext::JobEventContext(std::string collector_name,
                                 std::string collector_type,
                                 std::shared_ptr<const OutputRouter> router,
                                 std::function<std::string()> sample_id_factory,
                                 JobBindingResolver* resolver)
    : collector_name_(std::move(collector_name)),
      collector_type_(std::move(collector_type)),
      router_(std::move(router)),
      sample_id_factory_(std::move(sample_id_factory)),
      resolver_(resolver)
{
}

std::optional<OutputSession> JobEventContext::open_session(JobKey key)
{
    if (!resolver_) {
        record_error("job event context has no binding resolver");
        return std::nullopt;
    }
    auto job = resolver_->resolve(key.job_id, key.monitoring_generation);
    if (!job) {
        record_error("unknown or stale job binding " + std::to_string(key.job_id) +
                     "/" + std::to_string(key.monitoring_generation));
        return std::nullopt;
    }
    return open_session(*job);
}

std::optional<OutputSession> JobEventContext::open_session(uint64_t job_id,
                                                           uint64_t monitoring_generation)
{
    return open_session(JobKey{job_id, monitoring_generation});
}

OutputSession JobEventContext::open_session(const Job& job)
{
    auto sample = make_sample(job);
    return OutputSession(router_, std::move(sample),
                         [this](std::string error) { record_error(std::move(error)); });
}

std::shared_ptr<const SampleMetadata> JobEventContext::make_sample(const Job& job) const
{
    auto sample = std::make_shared<SampleMetadata>();
    sample->sample_id = next_sample_id();
    sample->collector_name = collector_name_;
    sample->collector_type = collector_type_;
    sample->job = job;
    sample->sampled_at = std::chrono::system_clock::now();
    return sample;
}

std::string JobEventContext::next_sample_id() const
{
    if (sample_id_factory_) return sample_id_factory_();
    return collector_name_ + ":" + std::to_string(next_sequence_++);
}

void JobEventContext::record_error(std::string message)
{
    if (errors_.size() >= kMaxErrors) return;
    errors_.push_back(bounded(std::move(message)));
}

std::string JobEventContext::error_summary() const
{
    return join_errors(errors_);
}

SystemEventContext::SystemEventContext(std::string collector_name,
                                       std::string collector_type,
                                       std::shared_ptr<const OutputRouter> router,
                                       std::function<std::string()> sample_id_factory)
    : collector_name_(std::move(collector_name)),
      collector_type_(std::move(collector_type)),
      router_(std::move(router)),
      sample_id_factory_(std::move(sample_id_factory))
{
}

OutputSession SystemEventContext::open_session()
{
    auto sample = make_sample();
    return OutputSession(router_, std::move(sample),
                         [this](std::string error) { record_error(std::move(error)); });
}

std::shared_ptr<const SampleMetadata> SystemEventContext::make_sample() const
{
    Job job;
    job.JobID = 0;
    job.jobtype = JobType::Sys;
    job.subtype = JobSubType::Common;
    job.sub_attr = CommonJobAttr{.auto_update_child = false};

    auto sample = std::make_shared<SampleMetadata>();
    sample->sample_id = next_sample_id();
    sample->collector_name = collector_name_;
    sample->collector_type = collector_type_;
    sample->job = job;
    sample->sampled_at = std::chrono::system_clock::now();
    return sample;
}

std::string SystemEventContext::next_sample_id() const
{
    if (sample_id_factory_) return sample_id_factory_();
    return collector_name_ + ":" + std::to_string(next_sequence_++);
}

void SystemEventContext::record_error(std::string message)
{
    if (errors_.size() >= kMaxErrors) return;
    errors_.push_back(bounded(std::move(message)));
}

std::string SystemEventContext::error_summary() const
{
    return join_errors(errors_);
}
