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
#include "common/config.hpp"
#include "core/collector_execution_context.hpp"
#include "core/collector_registry.hpp"
#include "core/collector_scheduler.hpp"
#include "core/output_context.hpp"

#include <atomic>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <variant>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

struct FakePeriodicJob : IPeriodicJobCollector {
    static inline std::atomic<int> init_calls{0};
    static inline std::atomic<int> collect_calls{0};
    static inline CollectorScope seen_scope = CollectorScope::Undefined;
    static inline bool final_support = false;

    bool init(const nlohmann::json&, CollectorInitContext& context) override
    {
        ++init_calls;
        seen_scope = context.scope();
        return true;
    }
    void deinit() noexcept override {}
    CollectOutcome collect(const Job&, OutputContext&) override
    {
        ++collect_calls;
        return {};
    }
    std::vector<OutputChannel> output_channels() const override { return {}; }
    bool supports_final() const override { return final_support; }
};

struct FakePeriodicSystem : IPeriodicSystemCollector {
    static inline std::atomic<int> init_calls{0};
    static inline std::atomic<int> collect_calls{0};
    static inline CollectorScope seen_scope = CollectorScope::Undefined;

    bool init(const nlohmann::json&, CollectorInitContext& context) override
    {
        ++init_calls;
        seen_scope = context.scope();
        return true;
    }
    void deinit() noexcept override {}
    CollectOutcome collect(OutputContext&) override
    {
        ++collect_calls;
        return {};
    }
    std::vector<OutputChannel> output_channels() const override { return {}; }
};

struct FakeEventJob : IEventJobCollector {
    static inline std::atomic<int> init_calls{0};
    static inline std::atomic<int> ready_calls{0};
    static inline uint64_t last_job_id{0};
    static inline size_t last_source_count{0};

    bool init(const nlohmann::json&, CollectorInitContext& context) override
    {
        ++init_calls;
        (void)context;
        return true;
    }
    void deinit() noexcept override {}
    DrainOutcome on_ready(const ReadySources& sources, const WorkBudget&, JobEventContext& context) override
    {
        ++ready_calls;
        last_source_count = sources.size();
        auto session = context.open_session(101, 1);
        if (session) last_job_id = session->context().sample().job.JobID;
        return {CollectOutcome{}, sources.size(), false};
    }
    std::vector<OutputChannel> output_channels() const override { return {}; }
};

struct FakeEventSystem : IEventSystemCollector {
    static inline std::atomic<int> ready_calls{0};
    static inline uint64_t last_job_id{1};
    static inline size_t last_source_count{0};

    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    DrainOutcome on_ready(const ReadySources& sources, const WorkBudget&, SystemEventContext& context) override
    {
        ++ready_calls;
        last_source_count = sources.size();
        auto session = context.open_session();
        last_job_id = session.context().sample().job.JobID;
        return {CollectOutcome{}, sources.size(), false};
    }
    std::vector<OutputChannel> output_channels() const override { return {}; }
};

struct WrongScopeCollector : IPeriodicJobCollector {
    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    CollectOutcome collect(const Job&, OutputContext&) override { return {}; }
    std::vector<OutputChannel> output_channels() const override { return {}; }
};

struct MultiInterfaceCollector : IPeriodicJobCollector, IEventJobCollector {
    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    CollectOutcome collect(const Job&, OutputContext&) override { return {}; }
    DrainOutcome on_ready(const ReadySources&, const WorkBudget&, JobEventContext&) override { return {}; }
    std::vector<OutputChannel> output_channels() const override { return {}; }
};

struct FakeFinalJob : IPeriodicJobCollector {
    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    CollectOutcome collect(const Job&, OutputContext&) override { return {}; }
    std::vector<OutputChannel> output_channels() const override { return {}; }
    bool supports_final() const override { return true; }
};

AUTO_REGISTER_JOB_COLLECTOR(FakePeriodicJob, "fake periodic Job collector");
AUTO_REGISTER_SYSTEM_COLLECTOR(FakePeriodicSystem, "fake periodic System collector");
AUTO_REGISTER_JOB_EVENT_COLLECTOR(FakeEventJob, "fake Job event collector");
AUTO_REGISTER_SYSTEM_EVENT_COLLECTOR(FakeEventSystem, "fake System event collector");
AUTO_REGISTER_JOB_COLLECTOR(FakeFinalJob, "fake final-capable Job collector");

struct TestRegistrar final : SourceRegistrar {
    size_t next_id = 0;
    size_t active = 0;
    size_t cancelled = 0;

    std::optional<SourceRegistration> watch_fd(int, Interest) override
    {
        ++active;
        return SourceRegistration(this, SourceHandle{++next_id, 1});
    }
    std::optional<SourceRegistration> watch_manual(std::string) override
    {
        return watch_fd(-1, Interest::Readable);
    }
    void cancel(SourceHandle) override
    {
        --active;
        ++cancelled;
    }
};

struct TestResolver final : JobBindingResolver {
    std::optional<Job> resolve(uint64_t job_id, uint64_t generation) override
    {
        if (job_id != 101 || generation != 1) return std::nullopt;
        Job job;
        job.JobID = 101;
        job.jobtype = JobType::Job;
        job.subtype = JobSubType::Common;
        job.JobPIDs = {1010};
        return job;
    }
};

Job make_job(uint64_t id)
{
    Job job;
    job.JobID = id;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    job.JobPIDs = {static_cast<pid_t>(id + 1000)};
    return job;
}

void registration_matrix()
{
    auto& registry = CollectorRegistry::instance();
    auto job = registry.createCollector("FakePeriodicJob", "fake_job");
    auto system = registry.createCollector("FakePeriodicSystem", "fake_system");
    auto event_job = registry.createCollector("FakeEventJob", "fake_event_job");
    auto event_system = registry.createCollector("FakeEventSystem", "fake_event_system");

    check(job.valid() && job.scope == CollectorScope::Job && job.trigger == CollectorTrigger::Periodic,
          "M1 periodic Job registration");
    check(system.valid() && system.scope == CollectorScope::System &&
              system.trigger == CollectorTrigger::Periodic,
          "M1 periodic System registration");
    check(event_job.valid() && event_job.scope == CollectorScope::Job &&
              event_job.trigger == CollectorTrigger::Event,
          "M1 Job event registration");
    check(event_system.valid() && event_system.scope == CollectorScope::System &&
              event_system.trigger == CollectorTrigger::Event,
          "M1 System event registration");
    check(std::holds_alternative<PeriodicJobBinding>(job.binding), "M1 Job variant");
    check(std::holds_alternative<PeriodicSystemBinding>(system.binding), "M1 System variant");
    check(std::holds_alternative<EventJobBinding>(event_job.binding), "M1 event Job variant");
    check(std::holds_alternative<EventSystemBinding>(event_system.binding), "M1 event System variant");
    check(registry.getTrigger("FakePeriodicJob") == CollectorTrigger::Periodic,
          "M1 legacy macro keeps periodic trigger");

    NullSourceRegistrar registrar;
    CollectorInitContext context(job.scope, registrar);
    check(job.init(nlohmann::json::object(), context), "M1 init call");
    check(FakePeriodicJob::seen_scope == CollectorScope::Job, "M1 init context scope");
    CollectorInitContext system_context(system.scope, registrar);
    check(system.init(nlohmann::json::object(), system_context), "M1 system init call");
    check(FakePeriodicSystem::seen_scope == CollectorScope::System, "M1 system context scope");
}

void mismatch_rejection()
{
    auto& registry = CollectorRegistry::instance();
    registry.registerCollector<WrongScopeCollector>("WrongScope", CollectorScope::System, "wrong scope");
    auto wrong = registry.createCollector("WrongScope", "wrong");
    check(!wrong.valid(), "M2 wrong scope accepted");
    check(wrong.error.find("does not match") != std::string::npos, "M2 wrong-scope diagnostic");

    registry.registerCollector<MultiInterfaceCollector>("MultiInterface", CollectorScope::Job, "multi");
    auto multi = registry.createCollector("MultiInterface", "multi");
    check(!multi.valid(), "M2 multiple interfaces accepted");
    check(multi.error.find("exactly one") != std::string::npos, "M2 multiple-interface diagnostic");

    CollectorHandle empty;
    check(!empty.valid(), "M2 empty handle accepted");
}

void periodic_call_shape()
{
    auto& registry = CollectorRegistry::instance();
    auto job = registry.createCollector("FakePeriodicJob", "shape_job");
    auto system = registry.createCollector("FakePeriodicSystem", "shape_system");
    FakePeriodicJob::collect_calls = 0;
    FakePeriodicSystem::collect_calls = 0;

    auto sample = std::make_shared<SampleMetadata>();
    OutputContext output(nullptr, sample);
    const int before = FakePeriodicJob::collect_calls.load();
    std::get<PeriodicJobBinding>(job.binding).collect(make_job(7), output);
    check(FakePeriodicJob::collect_calls.load() == before + 1, "M3 Job collect shape");

    const int system_before = FakePeriodicSystem::collect_calls.load();
    std::get<PeriodicSystemBinding>(system.binding).collect(output);
    check(FakePeriodicSystem::collect_calls.load() == system_before + 1, "M3 System collect shape");

    bool threw = false;
    try {
        (void)std::get<PeriodicSystemBinding>(job.binding);
    } catch (const std::bad_variant_access&) {
        threw = true;
    }
    check(threw, "M3 binding type is not enforced");
}

void event_call_shape_and_context()
{
    auto& registry = CollectorRegistry::instance();
    auto event_job = registry.createCollector("FakeEventJob", "shape_event_job");
    auto event_system = registry.createCollector("FakeEventSystem", "shape_event_system");

    TestRegistrar registrar;
    auto first = registrar.watch_fd(-1, Interest::Readable);
    auto second = registrar.watch_manual("manual");
    check(first.has_value() && second.has_value(), "M7 source registration");
    check(registrar.active == 2, "M7 source count");

    TestResolver resolver;
    JobEventContext job_context("shape_event_job", "FakeEventJob", nullptr,
                                [] { return std::string("sample"); }, &resolver);
    ReadySources sources{{first->handle(), 1}, {second->handle(), 1}};
    WorkBudget budget{16, 4096, 4, std::chrono::steady_clock::now() + std::chrono::milliseconds(5)};
    const int before = FakeEventJob::ready_calls.load();
    auto outcome = std::get<EventJobBinding>(event_job.binding).on_ready(sources, budget, job_context);
    check(FakeEventJob::ready_calls.load() == before + 1, "M4 event handler call shape");
    check(FakeEventJob::last_source_count == 2, "M7 multiple sources");
    check(FakeEventJob::last_job_id == 101, "M4 Job event attribution");
    check(outcome.consumed == 2, "M4 drain outcome consumed count");
    check(!job_context.had_error(), "M4 unexpected Job event error");

    bool unknown_rejected = false;
    {
        JobEventContext unknown("shape_event_job", "FakeEventJob", nullptr, {}, &resolver);
        check(!unknown.open_session(999, 1).has_value(), "M4 unknown Job session");
        unknown_rejected = unknown.had_error();
    }
    check(unknown_rejected, "M4 unknown Job error not recorded");

    SystemEventContext system_context("shape_event_system", "FakeEventSystem", nullptr, {});
    const int system_before = FakeEventSystem::ready_calls.load();
    std::get<EventSystemBinding>(event_system.binding).on_ready({}, budget, system_context);
    check(FakeEventSystem::ready_calls.load() == system_before + 1,
          "M4 max-wait scan with empty readiness list");
    check(FakeEventSystem::last_job_id == 0, "M4 System envelope uses Job 0");
    check(FakeEventSystem::last_source_count == 0, "M4 empty source list passed through");

    {
        auto sample_context = JobEventContext("shape_event_job", "FakeEventJob", nullptr, {}, &resolver);
        auto session = sample_context.open_session(101, 1);
        check(session.has_value(), "M4 output session");
        (void)session->context().emit("text", Text{"not routed"});
        check(!sample_context.had_error(), "M4 error folded too early");
    }
    // The session was destroyed above; the context now owns the admission error.
    {
        JobEventContext error_context("shape_event_job", "FakeEventJob", nullptr, {}, &resolver);
        {
            auto session = error_context.open_session(101, 1);
            (void)session->context().emit("text", Text{"not routed"});
        }
        check(error_context.had_error(), "M4 output error was not folded");
    }

    first->cancel();
    first->cancel();
    check(registrar.cancelled == 1 && registrar.active == 1, "M4 registration cancellation idempotence");
}

void capability_and_policy()
{
    auto& registry = CollectorRegistry::instance();
    auto stream = registry.createCollector("FakePeriodicJob", "stream_capability");
    auto final = registry.createCollector("FakeFinalJob", "final_capability");
    check(!stream.supports_final, "M6 stream-only collector declared final support");
    check(final.supports_final, "M6 final-capable collector lost its declaration");

    OutputPolicy policy = OutputPolicy::Stream;
    std::string error;
    check(CollectorScheduler::parse_output_policy("stream", false, policy, error),
          "M6 stream policy rejected");
    check(policy == OutputPolicy::Stream, "M6 stream policy value");
    check(!CollectorScheduler::parse_output_policy("final", false, policy, error),
          "M6 unsupported final policy accepted");
    check(error.find("not supported") != std::string::npos, "M6 unsupported policy diagnostic");
    check(!CollectorScheduler::parse_output_policy("both", true, policy, error),
          "M6 unimplemented final policy accepted");
    check(error.find("not implemented") != std::string::npos, "M6 unimplemented policy diagnostic");
    check(!CollectorScheduler::parse_output_policy("bogus", true, policy, error),
          "M6 invalid policy accepted");
}

void error_bounded()
{
    JobEventContext context("c", "t", nullptr, {}, nullptr);
    for (int index = 0; index < 100; ++index)
        context.record_error("error-" + std::to_string(index));
    check(context.had_error(), "M4 error recording");
    check(context.error_summary().size() < 32 * 520, "M4 error list is unbounded");
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: collector_execution_matrix_test TEMP_BASE\n";
        return 2;
    }
    try {
        std::ofstream config(std::string(argv[1]) + ".yaml");
        config << "collectors_config:\n  enable_collector_perf: false\n";
        config.close();
        Config::instance(std::string(argv[1]) + ".yaml");

        registration_matrix();
        mismatch_rejection();
        periodic_call_shape();
        event_call_shape_and_context();
        capability_and_policy();
        error_bounded();

        std::cout << "PASS collector execution matrix: contracts, dispatch shape, "
                     "event contexts, capability and policy validation\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
