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
#include "core/writer_manager.hpp"

#include <chrono>
#include <variant>

#include <spdlog/spdlog.h>

WorkResult CollectorScheduler::runEventDrain(CollectorLane& lane) {
    auto& state = *lane.state;
    std::vector<ReadySource> ready;
    {
        std::lock_guard lock(state.ready_m_);
        ready.swap(state.ready);
    }

    std::unique_lock<std::mutex> lock(state.m_);
    if (!state.running) return WorkResult::Complete;

    std::vector<uint64_t> closes;
    closes.swap(state.pending_close);

    WorkResult result = WorkResult::Complete;
    if (!ready.empty()) {
        CollectorRuntime::Operation callback(runtime_, lane.name, "callback");
        CallbackJobBindingResolver resolver(
            [state_ptr = &state](uint64_t job_id, uint64_t generation) -> std::optional<Job> {
                std::lock_guard lock(state_ptr->snapshot_m_);
                auto it = state_ptr->job_snapshots.find(job_id);
                if (it == state_ptr->job_snapshots.end()) return std::nullopt;
                auto gen = state_ptr->job_generations.find(job_id);
                if (gen == state_ptr->job_generations.end() || gen->second != generation)
                    return std::nullopt;
                return it->second;
            });
        const auto sample_id = [name = lane.name] {
            auto& manager = WriterManager::instance();
            return fmt::format("{}:{}", manager.process_run_id(), manager.next_sample_counter());
        };

        WorkBudget budget{512, 1024 * 1024, 0,
                          std::chrono::steady_clock::now() + std::chrono::milliseconds(5)};
        bool ok = true;
        std::string error;
        DrainOutcome outcome;
        bool context_error = false;

        try {
            if (lane.info.scope == CollectorScope::System) {
                SystemEventContext context(lane.name, lane.info.type, lane.info.router, sample_id);
                CollectorRuntime::Operation collecting(runtime_, lane.name, "event_drain");
                outcome = std::get<EventSystemBinding>(lane.info.binding)
                              .on_ready(ready, budget, context);
                context_error = context.had_error();
                if (context_error) error = context.error_summary();
            } else {
                JobEventContext context(lane.name, lane.info.type, lane.info.router,
                                        sample_id, &resolver);
                CollectorRuntime::Operation collecting(runtime_, lane.name, "event_drain");
                outcome = std::get<EventJobBinding>(lane.info.binding)
                              .on_ready(ready, budget, context);
                context_error = context.had_error();
                if (context_error) error = context.error_summary();
            }
        } catch (const std::exception& e) {
            ok = false;
            error = e.what();
        } catch (...) {
            ok = false;
            error = "collector threw an unknown exception";
        }

        if (ok && outcome.outcome.state == CollectState::Failed) {
            ok = false;
            error = outcome.outcome.error.empty() ? "collector reported failure"
                                                  : outcome.outcome.error;
        }
        if (ok && context_error) {
            ok = false;
            if (error.empty()) error = "event output admission failed";
        }
        if (!ok) {
            callback.fail(error);
            runtime_->error(lane.name, error);
            spdlog::error("CollectorScheduler: collector {} event drain failed: {}",
                          lane.name, error);
        } else if (outcome.outcome.state == CollectState::Success) {
            runtime_->clear_error(lane.name);
        }
        if (outcome.more) result = WorkResult::Yield;
    }

    if (!closes.empty()) {
        std::vector<std::pair<uint64_t, Job>> snapshots;
        {
            std::lock_guard lock(state.snapshot_m_);
            for (auto job_id : closes) {
                auto it = state.job_snapshots.find(job_id);
                if (it == state.job_snapshots.end()) continue;
                snapshots.emplace_back(job_id, it->second);
            }
        }
        for (const auto& [job_id, job] : snapshots) {
            notifyJobHook(lane.info, JobEvent::Removed, job);
            {
                std::lock_guard lock(state.snapshot_m_);
                state.job_snapshots.erase(job_id);
                state.job_generations.erase(job_id);
                state.job_generation_counter.erase(job_id);
            }
            recordRemovalAck(job_id);
        }
    }
    return result;
}

