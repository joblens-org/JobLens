// Queue limits are configured as ints and must be validated in the signed
// domain before they become size_t: a negative value used to wrap into a huge
// number and silently bypass the limit, and an explicitly configured value
// that does not parse as an int used to fall back to the default. Both must
// be rejected at startup with invalid_argument (or the config layer's
// runtime_error for an unparseable scalar), for the global writers_config
// keys and for a writer's own override section alike.
#include "writer/base_writer.hpp"
#include "core/output_record.h"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

class LimitsWriter : public BaseWriter {
public:
    LimitsWriter(std::string name, std::string type, std::string config)
        : BaseWriter(std::move(name), std::move(type), std::move(config)) {}
    ~LimitsWriter() override { shutdown(); }
    bool accepts(OutputKind) const override { return false; }
protected:
    bool flush_impl(const std::vector<OutputRecordPtr>&) override { return true; }
};

// A start with the named bad key must be refused with the expected exception
// type and must name the offending key.
void expect_rejection(const std::string& name, const std::string& config_section,
                      const std::string& key, const std::type_info& expected) {
    try {
        LimitsWriter writer(name, "LimitsWriter", config_section);
        writer.shutdown();
    } catch (const std::invalid_argument& e) {
        check(&expected == &typeid(std::invalid_argument) && std::string(e.what()).find(key) != std::string::npos,
              name + " must be rejected as invalid_argument naming '" + key + "': " + e.what());
        return;
    } catch (const std::runtime_error& e) {
        check(&expected == &typeid(std::runtime_error),
              name + " must be rejected as runtime_error, got: " + e.what());
        return;
    } catch (const std::exception& e) {
        check(false, name + " must be rejected with a typed error, got: " + e.what());
    }
    check(false, name + " must not start with an invalid '" + key + "'");
}

void scenario_overrides() {
    // The control writer starts with the positive global limits.
    {
        LimitsWriter writer("ctl", "LimitsWriter", "ctl_cfg");
        check(writer.output_status().at("limits").at("buffer_capacity") == 256,
              "the control writer must start on the global limits");
        writer.shutdown();
    }
    // Negative and zero values, via a writer's own override section.
    expect_rejection("neg_records", "neg_records_cfg", "max_pending_records", typeid(std::invalid_argument));
    expect_rejection("zero_records", "zero_records_cfg", "max_pending_records", typeid(std::invalid_argument));
    expect_rejection("neg_bytes", "neg_bytes_cfg", "max_pending_bytes", typeid(std::invalid_argument));
    expect_rejection("zero_bytes", "zero_bytes_cfg", "max_pending_bytes", typeid(std::invalid_argument));
    expect_rejection("neg_record_bytes", "neg_record_bytes_cfg", "max_record_bytes", typeid(std::invalid_argument));
    expect_rejection("zero_record_bytes", "zero_record_bytes_cfg", "max_record_bytes", typeid(std::invalid_argument));
    // A value that does not fit an int is refused, not silently defaulted.
    expect_rejection("huge_records", "huge_records_cfg", "max_pending_records", typeid(std::runtime_error));
    expect_rejection("huge_bytes", "huge_bytes_cfg", "max_pending_bytes", typeid(std::runtime_error));
    std::cout << "PASS: writer override queue limits validate in the signed domain\n";
}

// The global writers_config keys go through the same validation: any writer
// construction must be refused while one of them is invalid.
void scenario_global(const std::string& key) {
    expect_rejection("global_" + key, "ctl_cfg", key, typeid(std::invalid_argument));
    std::cout << "PASS: writer global queue limits validate in the signed domain (" << key << ")\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) return 2;
    Config::instance(argv[1]);
    spdlog::set_level(spdlog::level::warn);
    const auto scenario = Config::instance().getString("limits_test", "scenario", "overrides");
    if (scenario == "overrides") scenario_overrides();
    else if (scenario == "global_buffer") scenario_global("buffer_capacity");
    else if (scenario == "global_records") scenario_global("max_pending_records");
    else throw std::runtime_error("unknown limits_test scenario: " + scenario);
}
