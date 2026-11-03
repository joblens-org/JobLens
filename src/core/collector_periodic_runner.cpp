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
#include "core/collector_scheduler.hpp"

#include "core/collector_execution_context.hpp"
#include "core/output_context.hpp"
#include "core/writer_manager.hpp"

#include <chrono>
#include <memory>
#include <optional>
#include <variant>

#include <spdlog/spdlog.h>

WorkResult CollectorScheduler::runPeriodicJobSweep(CollectorLaneContext& lane) {
    CollectorRuntime::Operation callback(runtime_, lane.name, "callback");
    auto& state = *lane.state;

    std::unique_lock<std::mutex> lock(state.m_);
    if (!state.running) return WorkResult::Complete;

    if (!state.sweep_active) {
        if (state.jobid_list.empty()) {
            lock.unlock();
            stopCollectorInstance(lane);
            return WorkResult::Complete;
        }
        state.sweep_jobs.assign(state.jobid_list.begin(), state.jobid_list.end());
        state.sweep_cursor = 0;
        state.sweep_active = true;
    }

    if (state.sweep_cursor >= state.sweep_jobs.size()) {
        state.sweep_active = false;
        return WorkResult::Complete;
    }

    const size_t jobid = state.sweep_jobs[state.sweep_cursor++];
    const bool more = state.sweep_cursor < state.sweep_jobs.size();
    collectJobSample(lane, jobid, callback);
    if (!more) state.sweep_active = false;
    return more ? WorkResult::Yield : WorkResult::Complete;
}


WorkResult CollectorScheduler::runPeriodicSystemTick(CollectorLaneContext& lane) {
    CollectorRuntime::Operation callback(runtime_, lane.name, "callback");
    auto& state = *lane.state;

    std::unique_lock<std::mutex> lock(state.m_);
    if (!state.running) return WorkResult::Complete;
    collectSystemSample(lane, callback);
    return WorkResult::Complete;
}


void CollectorScheduler::collectJobSample(CollectorLaneContext& lane, size_t jobid,
                                          CollectorRuntime::Operation& callback) {
    // The lane's immutable attachment snapshot is the source of truth, not a
    // reverse read of the registry singleton. A sweep that outlived its
    // attachment finds no snapshot and produces no sample.
    auto& state = *lane.state;
    Job job;
    {
        std::lock_guard snapshot_lock(state.snapshot_m_);
        const auto it = state.job_snapshots.find(jobid);
        if (it == state.job_snapshots.end()) return;
        job = it->second;
    }
    if (!state.running) return;

    OutputContext output(lane.info.router, makeSample(lane.name, lane.info.type, job));
    std::unique_ptr<CollectorRuntime::Operation> write_op;
    attachWriteObserver(output, lane.name, jobid, write_op);

    const auto result = invokePeriodicJob(lane.info, job, output, jobid);
    applyCollectResult(lane.name, jobid, result, output, callback);
}


void CollectorScheduler::collectSystemSample(CollectorLaneContext& lane,
                                             CollectorRuntime::Operation& callback) {
    Job system_job;
    system_job.JobID = 0;
    system_job.jobtype = JobType::Sys;
    system_job.subtype = JobSubType::Common;
    CommonJobAttr attr;
    attr.auto_update_child = false;
    system_job.sub_attr = attr;

    OutputContext output(lane.info.router, makeSample(lane.name, lane.info.type, system_job));
    std::unique_ptr<CollectorRuntime::Operation> write_op;
    attachWriteObserver(output, lane.name, std::nullopt, write_op);

    const auto result = invokePeriodicSystem(lane.info, output);
    applyCollectResult(lane.name, std::nullopt, result, output, callback);
}


std::shared_ptr<SampleMetadata> CollectorScheduler::makeSample(
    const std::string& name, const std::string& type, const Job& job) const {
    auto& manager = WriterManager::instance();
    auto sample = std::make_shared<SampleMetadata>();
    sample->sample_id = fmt::format("{}:{}", manager.process_run_id(), manager.next_sample_counter());
    sample->collector_name = name;
    sample->collector_type = type;
    sample->job = job;
    sample->sampled_at = std::chrono::system_clock::now();
    return sample;
}


void CollectorScheduler::attachWriteObserver(
    OutputContext& output, const std::string& name, std::optional<size_t> jobid,
    std::unique_ptr<CollectorRuntime::Operation>& write_op) {
    output.set_write_observer(
        [this, name, jobid, &write_op](std::string_view /*channel*/) {
            write_op = std::make_unique<CollectorRuntime::Operation>(
                runtime_, name, "write", jobid);
        },
        [&write_op](std::string_view channel, bool failed) {
            if (failed && write_op)
                write_op->fail("output rejected on channel " + std::string(channel));
            write_op.reset();
        });
}


CollectorScheduler::CollectInvocation CollectorScheduler::invokePeriodicJob(
    const collector_info& info, const Job& job, OutputContext& output, size_t jobid) {
    CollectInvocation result;
    try {
        CollectorRuntime::Operation collecting(runtime_, info.name, "collect", jobid);
        result.outcome = std::get<PeriodicJobBinding>(info.binding).collect(job, output);
    } catch (const std::exception& e) {
        result.ok = false;
        result.error = e.what();
    } catch (...) {
        result.ok = false;
        result.error = "collector threw an unknown exception";
    }
    return result;
}


CollectorScheduler::CollectInvocation CollectorScheduler::invokePeriodicSystem(
    const collector_info& info, OutputContext& output) {
    CollectInvocation result;
    try {
        CollectorRuntime::Operation collecting(runtime_, info.name, "collect", 0);
        result.outcome = std::get<PeriodicSystemBinding>(info.binding).collect(output);
    } catch (const std::exception& e) {
        result.ok = false;
        result.error = e.what();
    } catch (...) {
        result.ok = false;
        result.error = "collector threw an unknown exception";
    }
    return result;
}


void CollectorScheduler::applyCollectResult(const std::string& name,
                                            std::optional<size_t> jobid,
                                            const CollectInvocation& result,
                                            const OutputContext& output,
                                            CollectorRuntime::Operation& callback) {
    bool ok = result.ok;
    std::string error = result.error;
    if (ok) {
        if (result.outcome.state == CollectState::Failed) {
            ok = false;
            error = result.outcome.error.empty() ? "collector reported failure"
                                                 : result.outcome.error;
        } else if (result.outcome.state == CollectState::Skipped && output.submitted() > 0) {
            ok = false;
            error = "collector returned Skipped after emitting output";
        }
        if (output.had_error()) {
            ok = false;
            error = output.error_summary();
        }
    }
    if (!ok) {
        callback.fail(error);
        runtime_->error(name, error);
        if (jobid)
            spdlog::error("CollectorScheduler: collector {} job {} native collect failed: {}",
                          name, *jobid, error);
        else
            spdlog::error("CollectorScheduler: collector {} system collect failed: {}",
                          name, error);
    } else if (result.outcome.state == CollectState::Success) {
        runtime_->clear_error(name);
    }
}

