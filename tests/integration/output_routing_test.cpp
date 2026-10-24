#include "output_test_support.hpp"
#include "core/writer_manager.hpp"
#include "common/local_rpc.hpp"

#include <unordered_map>

using namespace output_test;

namespace {

/// 测试用 writer：按配置只接受一种 payload，并把收到的记录保存在进程内。
class CaptureWriter : public BaseWriter {
public:
    explicit CaptureWriter(std::string name, std::string type, std::string config_name)
        : BaseWriter(std::move(name), type, config_name) {
        auto& config = Config::instance();
        const auto kind = config.getString(config_name, "kind", "all");
        if (kind == "text") kind_ = OutputKind::Text;
        else if (kind == "document") kind_ = OutputKind::Document;
        else if (kind == "metrics") kind_ = OutputKind::Metrics;
        else kind_ = OutputKind::Document, all_ = true;
    }

    ~CaptureWriter() override { shutdown(); }

    bool accepts(OutputKind kind) const override { return all_ || kind_ == kind; }

protected:
    bool flush_impl(const std::vector<OutputRecordPtr>& batch) override {
        std::lock_guard lock(store().mutex);
        auto& list = store().records[get_name()];
        for (const auto& record : batch) list.push_back(record);
        store().cv.notify_all();
        return true;
    }

public:
    struct Store {
        std::mutex mutex;
        std::condition_variable cv;
        std::unordered_map<std::string, std::vector<OutputRecordPtr>> records;
    };
    static Store& store() {
        static Store instance;
        return instance;
    }
    static void clear() {
        std::lock_guard lock(store().mutex);
        store().records.clear();
    }
    static std::vector<OutputRecordPtr> wait_for(const std::string& name, size_t count) {
        std::unique_lock lock(store().mutex);
        const bool ok = store().cv.wait_for(lock, std::chrono::seconds(5), [&] {
            return store().records[name].size() >= count;
        });
        check(ok, "writer '" + name + "' did not receive " + std::to_string(count) + " record(s)");
        return store().records[name];
    }
    static size_t count(const std::string& name) {
        std::lock_guard lock(store().mutex);
        return store().records[name].size();
    }

private:
    OutputKind kind_ = OutputKind::Document;
    bool all_ = false;
};

std::vector<OutputChannel> channels() {
    return {{"document", OutputKind::Document},
            {"text", OutputKind::Text},
            {"metrics", OutputKind::Metrics}};
}

std::shared_ptr<OutputRouter> build(const std::string& config, std::string* error = nullptr) {
    auto result = OutputRouter::build("route_tester", config, channels());
    if (error) *error = result.error;
    return result.router;
}

void emit_all(OutputContext& context) {
    context.emit("document", Document{nlohmann::json{{"value", 1}}});
    context.emit("text", Text{"line\n"});
    context.emit("metrics", MetricBatch{});
}

void explicit_channels() {
    CaptureWriter::clear();
    auto router = build("rt_explicit");
    check(router != nullptr, "explicit route did not compile");
    OutputContext context(router, make_sample("route_tester", "TestCollector"));
    emit_all(context);
    check(context.errors().empty(), "valid explicit routes produced an error");
    check(CaptureWriter::wait_for("w_doc", 1).size() == 1, "document route");
    check(CaptureWriter::wait_for("w_text", 1).size() == 1, "text route");
    check(CaptureWriter::wait_for("w_metrics", 1).size() == 1, "metrics route");
    check(CaptureWriter::count("w_text") == 1, "text channel leaked a record to an unrelated payload");
}

void legacy_use_writers() {
    CaptureWriter::clear();
    auto router = build("rt_legacy");
    check(router != nullptr, "legacy use_writers route did not compile");
    OutputContext context(router, make_sample("route_tester", "TestCollector"));
    context.emit("text", Text{"legacy\n"});
    check(CaptureWriter::wait_for("w_text", 1).size() == 1, "legacy text route");
    const auto status = context.emit("document", Document{nlohmann::json::object()});
    check(status.code == EmitCode::Disabled, "undeclared-by-config channel must be disabled");
}

void explicit_disable() {
    auto empty_map = build("rt_disable_map");
    auto empty_list = build("rt_disable_list");
    check(empty_map && empty_list, "disable configs did not compile");
    OutputContext a(empty_map, make_sample("route_tester", "TestCollector"));
    OutputContext b(empty_list, make_sample("route_tester", "TestCollector"));
    check(a.emit("text", Text{"x"}).code == EmitCode::Disabled, "outputs: {} did not disable");
    check(b.emit("text", Text{"x"}).code == EmitCode::Disabled, "use_writers: [] did not disable");
    check(!a.enabled("text") && !b.enabled("text"), "disabled channel reported enabled");
    check(a.errors().empty() && b.errors().empty(), "disabled emit recorded an error");
}

void invalid_bindings() {
    std::string error;
    check(build("rt_unknown_writer", &error) == nullptr, "unknown writer accepted");
    check(error.find("unknown writer") != std::string::npos, "unknown writer diagnostic: " + error);
    check(build("rt_unknown_channel", &error) == nullptr, "unknown channel accepted");
    check(error.find("undeclared channel") != std::string::npos, "unknown channel diagnostic: " + error);
    check(build("rt_bad_kind", &error) == nullptr, "incompatible writer kind accepted");
    check(error.find("does not accept") != std::string::npos, "bad kind diagnostic: " + error);
    check(build("rt_both", &error) == nullptr, "outputs + use_writers accepted");
    check(error.find("mutually exclusive") != std::string::npos, "conflict diagnostic: " + error);
}

void duplicate_and_multi_channel() {
    CaptureWriter::clear();
    auto router = build("rt_duplicate");
    check(router != nullptr, "duplicate destination route did not compile");
    OutputContext context(router, make_sample("route_tester", "TestCollector"));
    context.emit("text", Text{"once\n"});
    check(CaptureWriter::wait_for("w_text", 1).size() == 1, "duplicate destination delivered more than once");

    auto multi = build("rt_two_channels");
    check(multi != nullptr, "multi-channel route did not compile");
    OutputContext second(multi, make_sample("route_tester", "TestCollector", 2, "run:2"));
    second.emit("document", Document{nlohmann::json{{"value", 2}}});
    second.emit("text", Text{"two\n"});
    check(CaptureWriter::wait_for("w_all", 2).size() == 2, "one writer bound to two channels must receive two records");
}

void target_restriction() {
    auto router = build("rt_explicit");
    check(router != nullptr, "explicit route did not compile");
    OutputContext context(router, make_sample("route_tester", "TestCollector"));
    const auto report = context.emit_to("text", "w_doc", Text{"x"});
    check(report.code == EmitCode::Invalid, "emit_to reached an unbound writer");
    check(context.had_error(), "restricted emit_to did not record an error");
}

void identity_and_accounting() {
    CaptureWriter::clear();
    auto router = build("rt_explicit");
    OutputContext context(router, make_sample("route_tester", "TestCollector"));
    context.emit("document", Document{nlohmann::json{{"value", 1}}});
    context.emit("document", Document{nlohmann::json{{"value", 2}}});
    const auto records = CaptureWriter::wait_for("w_doc", 2);
    check(records[0]->record_id != records[1]->record_id, "distinct records share an identity");
    check(records[0]->record_id.rfind("run:1:", 0) == 0, "record identity does not derive from sample id");
    check(records[0]->accounted_bytes > sizeof(OutputRecord), "accounting charged only the pointer size");
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    spdlog::set_level(spdlog::level::off);
    Config::instance(argv[1]);
    const std::string socket_path = std::string(argv[1]) + ".sock";
    auto& rpc = RPCServer::instance(socket_path);
    rpc.start();
    auto& manager = WriterManager::instance();
    manager.registerWriter<CaptureWriter>("CaptureWriter");
    manager.initialize();

    int failures = 0;
    for (const auto& test : std::vector<std::pair<const char*, void(*)()>>{
             {"explicit channels", explicit_channels},
             {"legacy use_writers", legacy_use_writers},
             {"explicit disable", explicit_disable},
             {"invalid bindings", invalid_bindings},
             {"duplicate and multi-channel", duplicate_and_multi_channel},
             {"target restriction", target_restriction},
             {"identity and accounting", identity_and_accounting}}) {
        try {
            test.second();
            std::cout << "PASS: " << test.first << '\n';
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "FAIL: " << test.first << ": " << error.what() << '\n';
        }
    }
    manager.shutdown();
    rpc.stop();
    return failures == 0 ? 0 : 1;
}
