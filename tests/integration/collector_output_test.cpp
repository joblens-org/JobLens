#include "output_test_support.hpp"
#include "core/collector_registry.hpp"
#include "core/collector_scheduler.hpp"
#include "core/job_registry.hpp"
#include "core/writer_manager.hpp"
#include "common/local_rpc.hpp"

#include <atomic>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <unordered_map>

using namespace output_test;

namespace {

/// 与 routing 测试相同的接收 writer；按 collector 名称区分记录。
class CaptureWriter : public BaseWriter {
public:
    explicit CaptureWriter(std::string name, std::string type, std::string config_name)
        : BaseWriter(std::move(name), type, config_name) {}
    ~CaptureWriter() override { shutdown(); }
    bool accepts(OutputKind kind) const override { return kind == OutputKind::Text; }
protected:
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override {
        std::lock_guard lock(store().mutex);
        for (const auto& record : batch) store().records.push_back(record);
        store().cv.notify_all();
        return true;
    }
public:
    struct Store {
        std::mutex mutex;
        std::condition_variable cv;
        std::vector<OutputRecordPtr> records;
    };
    static Store& store() { static Store s; return s; }
    static std::vector<OutputRecordPtr> wait_for(const std::string& collector, size_t count) {
        std::unique_lock lock(store().mutex);
        const bool ok = store().cv.wait_for(lock, std::chrono::seconds(5), [&] {
            size_t matched = 0;
            for (const auto& record : store().records)
                if (record->sample->collector_name == collector) ++matched;
            return matched >= count;
        });
        check(ok, "collector '" + collector + "' did not deliver " + std::to_string(count) + " record(s)");
        std::vector<OutputRecordPtr> result;
        for (const auto& record : store().records)
            if (record->sample->collector_name == collector) result.push_back(record);
        return result;
    }
    static size_t count(const std::string& collector) {
        std::lock_guard lock(store().mutex);
        size_t matched = 0;
        for (const auto& record : store().records)
            if (record->sample->collector_name == collector) ++matched;
        return matched;
    }
};

enum class Behavior { Emit, NoEmit, Skipped, Failed, Throw, EmitThenSkipped, EmitThenFailed };

std::atomic<Behavior> behavior{Behavior::Emit};
std::atomic<int> native_calls{0};

struct NativeJobCollector : IPeriodicJobCollector {
    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    CollectOutcome collect(const Job& job, OutputContext& output) override {
        ++native_calls;
        (void)job;
        switch (behavior.load()) {
            case Behavior::Emit:
                output.emit("text", Text{"native\n"});
                return {};
            case Behavior::NoEmit:
                return {};
            case Behavior::Skipped:
                return {CollectState::Skipped, {}};
            case Behavior::Failed:
                return {CollectState::Failed, "synthetic failure"};
            case Behavior::Throw:
                throw std::runtime_error("synthetic exception");
            case Behavior::EmitThenSkipped:
                output.emit("text", Text{"partial\n"});
                return {CollectState::Skipped, {}};
            case Behavior::EmitThenFailed:
                output.emit("text", Text{"kept\n"});
                return {CollectState::Failed, "after emit"};
        }
        return {};
    }
    std::vector<OutputChannel> output_channels() const override {
        return {{"text", OutputKind::Text}};
    }
};

std::atomic<int> system_calls{0};

struct NativeSystemCollector : IPeriodicSystemCollector {
    bool init(const nlohmann::json&, CollectorInitContext&) override { return true; }
    void deinit() noexcept override {}
    CollectOutcome collect(OutputContext& output) override {
        ++system_calls;
        output.emit("text", Text{"system\n"});
        return {};
    }
    std::vector<OutputChannel> output_channels() const override {
        return {{"text", OutputKind::Text}};
    }
};

Job job_for(uint64_t id, const std::vector<std::string>& collectors) {
    Job job;
    job.JobID = id;
    job.NativeJobID = std::to_string(id);
    job.jobtype = JobType::Sys;
    job.subtype = JobSubType::Common;
    job.sub_attr = CommonJobAttr{.auto_update_child = false};
    job.JobPIDs = {getpid()};
    job.CollectorNames = collectors;
    return job;
}

nlohmann::json rpc(const std::string& path, const std::string& method) {
    const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    check(fd >= 0, "rpc socket");
    struct Close { int fd; ~Close() { close(fd); } } guard{fd};
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    check(path.size() < sizeof(address.sun_path), "rpc path too long");
    std::copy(path.begin(), path.end(), address.sun_path);
    timeval timeout{5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    check(connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "rpc connect");
    const std::string request = nlohmann::json{{"method", method}, {"params", nlohmann::json::object()}}.dump();
    check(send(fd, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size()), "rpc send");
    std::string response;
    char buffer[8192];
    ssize_t size;
    while ((size = recv(fd, buffer, sizeof(buffer), 0)) > 0) response.append(buffer, size);
    return nlohmann::json::parse(response);
}

nlohmann::json collector_status(const std::string& path, const std::string& name) {
    const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    check(fd >= 0, "rpc socket");
    struct Close { int fd; ~Close() { close(fd); } } guard{fd};
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::copy(path.begin(), path.end(), address.sun_path);
    timeval timeout{5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    check(connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "rpc connect");
    const std::string request = nlohmann::json{
        {"method", "CollectorScheduler/collector_status"},
        {"params", {{"name", name}}}}.dump();
    check(send(fd, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size()), "rpc send");
    std::string response;
    char buffer[8192];
    ssize_t size;
    while ((size = recv(fd, buffer, sizeof(buffer), 0)) > 0) response.append(buffer, size);
    return nlohmann::json::parse(response);
}

uint64_t write_success(const std::string& path, const std::string& name) {
    return collector_status(path, name)["collector"]["operations"]["write"]["success_count"].get<uint64_t>();
}

void wait_calls(int target) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (native_calls.load() < target && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    check(native_calls.load() >= target, "collector did not run");
}

void run(const std::string& base) {
    const std::string socket_path = base + ".sock";
    {
        std::ofstream config(base + ".yaml");
        config << "lens_config:\n  max_collector_threads: 2\n"
                  "collectors_config:\n"
                  "  enable_collector_perf: false\n"
                  "  default_freq: 20\n"
                  "  default_use_writers: []\n"
                  "  collectors:\n"
                  "    - {name: native_job, type: NativeJobCollector, config: native_job_cfg}\n"
                  "    - {name: native_system, type: NativeSystemCollector, config: native_system_cfg}\n"
                  "writers_config:\n"
                  "  buffer_capacity: 256\n"
                  "  writers:\n"
                  "    - {name: w_text, type: CaptureWriter, config: cap_text}\n"
                  "native_job_cfg:\n  freq: 20\n  outputs:\n    text: [w_text]\n"
                  "native_system_cfg:\n  freq: 20\n  auto_start: true\n  outputs:\n    text: [w_text]\n"
                  "cap_text: {}\n"
                  "job_registry_config:\n  job_db_path: " << base << ".db\n";
    }
    auto& server = RPCServer::instance(socket_path);
    server.start();
    Config::instance(base + ".yaml");
    spdlog::set_level(getenv("TEST_DEBUG") ? spdlog::level::debug : spdlog::level::off);

    auto& registry = CollectorRegistry::instance();
    registry.registerCollector<NativeJobCollector>("NativeJobCollector", CollectorScope::Job, "native job");
    registry.registerCollector<NativeSystemCollector>("NativeSystemCollector", CollectorScope::System, "native system");
    WriterManager::instance().registerWriter<CaptureWriter>("CaptureWriter");

    auto& scheduler = CollectorScheduler::instance();
    scheduler.start();

    // ---- System 作用域：自动启动，合成 Job 0，不走 Job 重载 ----
    const auto system_records = CaptureWriter::wait_for("native_system", 1);
    check(system_calls.load() >= 1, "system collector did not run");
    check(system_records[0]->sample->job.JobID == 0, "system collector did not receive synthetic Job 0 metadata");
    check(system_records[0]->sample->collector_type == "NativeSystemCollector", "collector type metadata");

    // ---- Job 作用域 + 一次 emit ----
    check(JobRegistry::instance().addJob(job_for(9001, {"native_job"})), "register native job");
    const auto first = CaptureWriter::wait_for("native_job", 1);
    check(first[0]->sample->job.JobID == 9001, "job collector received the wrong snapshot");
    check(first[0]->sample->collector_name == "native_job", "collector instance metadata");
    check(first[0]->sample->sample_id.rfind(
              WriterManager::instance().process_run_id() + ":", 0) == 0,
          "sample id does not derive from the process run id");

    const uint64_t base_writes = write_success(socket_path, "native_job");
    check(base_writes >= 1, "one emit must count as one write operation");

    // ---- Success without emission: no write operation ----
    behavior = Behavior::NoEmit;
    const int calls_before_empty = native_calls.load();
    wait_calls(calls_before_empty + 2);
    check(write_success(socket_path, "native_job") == base_writes,
          "zero emissions must not create write operations");

    // ---- Skipped: no output, no error ----
    behavior = Behavior::Skipped;
    const int calls_before_skipped = native_calls.load();
    wait_calls(calls_before_skipped + 2);
    check(write_success(socket_path, "native_job") == base_writes, "Skipped created a write operation");
    check(!collector_status(socket_path, "native_job")["collector"]["has_error"].get<bool>(),
          "Skipped must not mark an error");

    // ---- Failed: diagnostic becomes current error ----
    behavior = Behavior::Failed;
    const int calls_before_failed = native_calls.load();
    wait_calls(calls_before_failed + 2);
    auto failed = collector_status(socket_path, "native_job");
    check(failed["collector"]["has_error"].get<bool>(), "Failed did not set the error state");
    check(failed["collector"]["last_error"].get<std::string>().find("synthetic failure") != std::string::npos,
          "Failed diagnostic was not preserved");

    // ---- Skipped must not clear an earlier error ----
    behavior = Behavior::Skipped;
    const int calls_before_preserve = native_calls.load();
    wait_calls(calls_before_preserve + 2);
    check(collector_status(socket_path, "native_job")["collector"]["has_error"].get<bool>(),
          "Skipped cleared a previous error");

    // ---- Exception: reported as failure ----
    behavior = Behavior::Throw;
    const int calls_before_throw = native_calls.load();
    wait_calls(calls_before_throw + 2);
    auto thrown = collector_status(socket_path, "native_job");
    check(thrown["collector"]["has_error"].get<bool>(), "exception did not set the error state");
    check(thrown["collector"]["last_error"].get<std::string>().find("synthetic exception") != std::string::npos,
          "exception diagnostic was not preserved");

    // ---- Emit then Skipped: contract error, but record is delivered ----
    behavior = Behavior::EmitThenSkipped;
    const int calls_before_contract = native_calls.load();
    const size_t records_before = CaptureWriter::count("native_job");
    wait_calls(calls_before_contract + 2);
    CaptureWriter::wait_for("native_job", records_before + 2);
    auto contract = collector_status(socket_path, "native_job");
    check(contract["collector"]["has_error"].get<bool>(), "emit+Skipped was not a contract error");
    check(contract["collector"]["last_error"].get<std::string>().find("Skipped after emitting") != std::string::npos,
          "contract error diagnostic");

    // ---- Emit then Failed: accepted record is not rolled back ----
    behavior = Behavior::EmitThenFailed;
    const int calls_before_partial = native_calls.load();
    const size_t partial_before = CaptureWriter::count("native_job");
    wait_calls(calls_before_partial + 2);
    CaptureWriter::wait_for("native_job", partial_before + 2);
    check(collector_status(socket_path, "native_job")["collector"]["has_error"].get<bool>(),
          "emit+Failed did not report an error");

    // ---- A complete successful invocation clears the current error ----
    behavior = Behavior::Emit;
    const int calls_before_recover = native_calls.load();
    wait_calls(calls_before_recover + 2);
    check(!collector_status(socket_path, "native_job")["collector"]["has_error"].get<bool>(),
          "a successful invocation did not clear the current error");

    // ---- 状态语义版本 ----
    const auto status = rpc(socket_path, "CollectorScheduler/status");
    check(status["operation_semantics_version"] == 2, "missing operation_semantics_version 2");

    JobRegistry::instance().delJob(9001);
    JobRegistry::instance().delJob(9002);
    scheduler.shutdown();
    WriterManager::instance().shutdown();
    server.stop();
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    try {
        run(argv[1]);
        std::cout << "PASS: native collector output integration\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
