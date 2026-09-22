#include "core/collector_registry.hpp"
#include <iostream>

namespace {
struct Payload {
    static int copies;
    int value;
    explicit Payload(int value) : value(value) {}
    Payload(const Payload& other) : value(other.value) { ++copies; }
    Payload(Payload&&) noexcept = default;
};
int Payload::copies = 0;

struct LegacyCollector : ICollector {
    bool init(const nlohmann::json&) override { return true; }
    void deinit() noexcept override {}
    CollectDataParseFunc get_writer_parser(const std::string&) override {
        return [](std::any payload) -> std::any { return payload; };
    }
};
struct DefensiveLegacyCollector : LegacyCollector {
    CollectDataParseFuncV2 get_writer_parser_v2(const std::string&) override { return nullptr; }
};

bool check_adapter(const char* name, const CollectDataParseFuncV2& parser) {
    std::any payload = Payload(42);
    WriterParseContext context{};
    Payload::copies = 0;
    const auto result = parser(context, std::move(payload));
    if (std::any_cast<const Payload&>(result).value != 42 || Payload::copies != 0) {
        std::cerr << "FAIL: " << name << " copied an owned payload " << Payload::copies << " times\n";
        return false;
    }
    std::cout << "PASS: " << name << " transfers the owned payload without copies\n";
    return true;
}
}

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    spdlog::set_level(spdlog::level::off);
    Config::instance(argv[1]);
    LegacyCollector collector;
    bool ok = check_adapter("ICollector V1 adapter", collector.get_writer_parser_v2("FileWriter"));
    auto& registry = CollectorRegistry::instance();
    registry.registerCollector<DefensiveLegacyCollector>("WriterForwardingCollector", CollectorScope::Job, "Writer parser forwarding");
    registry.createCollector("WriterForwardingCollector", "writer_forwarding");
    ok = check_adapter("registry defensive V1 adapter", registry.resolveBestParserV2("writer_forwarding", "FileWriter")) && ok;
    return ok ? 0 : 1;
}
