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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/collector_type.h"
#include "core/output_context.hpp"
#include "core/output_record.h"
#include "core/output_router.hpp"

// ---------------------------------------------------------------------------
// Collector-owned source registration
// ---------------------------------------------------------------------------

class SourceRegistration;

class SourceRegistrar {
public:
    virtual ~SourceRegistrar() = default;

    // The registrar borrows the descriptor. Cancelling a registration never
    // closes it; the collector owns the descriptor and its lifetime.
    virtual std::optional<SourceRegistration> watch_fd(int fd, Interest interest) = 0;
    virtual std::optional<SourceRegistration> watch_manual(std::string name) = 0;
    virtual void cancel(SourceHandle handle) = 0;
};

class SourceRegistration {
public:
    SourceRegistration() = default;
    SourceRegistration(SourceRegistrar* owner, SourceHandle handle)
        : owner_(owner), handle_(handle) {}

    ~SourceRegistration() { cancel(); }

    SourceRegistration(const SourceRegistration&) = delete;
    SourceRegistration& operator=(const SourceRegistration&) = delete;

    SourceRegistration(SourceRegistration&& other) noexcept
        : owner_(other.owner_), handle_(other.handle_)
    {
        other.owner_ = nullptr;
    }

    SourceRegistration& operator=(SourceRegistration&& other) noexcept
    {
        if (this == &other) return *this;
        cancel();
        owner_ = other.owner_;
        handle_ = other.handle_;
        other.owner_ = nullptr;
        return *this;
    }

    SourceHandle handle() const { return handle_; }
    bool active() const { return owner_ != nullptr; }

    // Idempotent. The registrar keeps cancellation safe during rollback.
    void cancel()
    {
        if (!owner_) return;
        SourceRegistrar* owner = owner_;
        owner_ = nullptr;
        owner->cancel(handle_);
    }

private:
    SourceRegistrar* owner_ = nullptr;
    SourceHandle handle_{};
};

class NullSourceRegistrar final : public SourceRegistrar {
public:
    std::optional<SourceRegistration> watch_fd(int, Interest) override { return std::nullopt; }
    std::optional<SourceRegistration> watch_manual(std::string) override { return std::nullopt; }
    void cancel(SourceHandle) override {}
};

class CollectorInitContext {
public:
    CollectorInitContext(CollectorScope scope, SourceRegistrar& sources)
        : scope_(scope), sources_(sources) {}

    CollectorScope scope() const { return scope_; }
    SourceRegistrar& sources() { return sources_; }

private:
    CollectorScope scope_;
    SourceRegistrar& sources_;
};

// ---------------------------------------------------------------------------
// Scoped output sessions
// ---------------------------------------------------------------------------

class OutputSession {
public:
    OutputSession(std::shared_ptr<const OutputRouter> router,
                  std::shared_ptr<const SampleMetadata> sample,
                  std::function<void(std::string)> error_sink);
    ~OutputSession();

    OutputSession(const OutputSession&) = delete;
    OutputSession& operator=(const OutputSession&) = delete;

    OutputSession(OutputSession&& other) noexcept;
    OutputSession& operator=(OutputSession&& other) noexcept;

    OutputContext& context() { return *context_; }
    const OutputContext& context() const { return *context_; }
    bool had_error() const { return context_ && context_->had_error(); }
    std::string error_summary() const { return context_ ? context_->error_summary() : std::string{}; }
    size_t submitted() const { return context_ ? context_->submitted() : 0; }

private:
    void fold_error();

    std::shared_ptr<const OutputRouter> router_;
    std::shared_ptr<const SampleMetadata> sample_;
    std::unique_ptr<OutputContext> context_;
    std::function<void(std::string)> error_sink_;
};

// ---------------------------------------------------------------------------
// Event execution contexts
// ---------------------------------------------------------------------------

class JobBindingResolver {
public:
    virtual ~JobBindingResolver() = default;
    virtual std::optional<Job> resolve(uint64_t job_id, uint64_t monitoring_generation) = 0;
};

class JobEventContext {
public:
    JobEventContext(std::string collector_name,
                    std::string collector_type,
                    std::shared_ptr<const OutputRouter> router,
                    std::function<std::string()> sample_id_factory,
                    JobBindingResolver* resolver);

    JobEventContext(const JobEventContext&) = delete;
    JobEventContext& operator=(const JobEventContext&) = delete;

    std::optional<OutputSession> open_session(JobKey key);
    std::optional<OutputSession> open_session(uint64_t job_id,
                                              uint64_t monitoring_generation = 1);
    OutputSession open_session(const Job& job);

    bool had_error() const { return !errors_.empty(); }
    std::string error_summary() const;
    void record_error(std::string message);

private:
    std::shared_ptr<const SampleMetadata> make_sample(const Job& job) const;
    std::string next_sample_id() const;

    std::string collector_name_;
    std::string collector_type_;
    std::shared_ptr<const OutputRouter> router_;
    std::function<std::string()> sample_id_factory_;
    JobBindingResolver* resolver_ = nullptr;
    mutable uint64_t next_sequence_ = 0;
    std::vector<std::string> errors_;
};

class SystemEventContext {
public:
    SystemEventContext(std::string collector_name,
                       std::string collector_type,
                       std::shared_ptr<const OutputRouter> router,
                       std::function<std::string()> sample_id_factory);

    SystemEventContext(const SystemEventContext&) = delete;
    SystemEventContext& operator=(const SystemEventContext&) = delete;

    OutputSession open_session();

    bool had_error() const { return !errors_.empty(); }
    std::string error_summary() const;
    void record_error(std::string message);

private:
    std::shared_ptr<const SampleMetadata> make_sample() const;
    std::string next_sample_id() const;

    std::string collector_name_;
    std::string collector_type_;
    std::shared_ptr<const OutputRouter> router_;
    std::function<std::string()> sample_id_factory_;
    mutable uint64_t next_sequence_ = 0;
    std::vector<std::string> errors_;
};
