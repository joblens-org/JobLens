#include "collector/io_usage_collector.hpp"
#include <unistd.h>
#include <iostream>
#include <stdexcept>

extern unsigned metadata_calls;

static void require(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
struct LegacyTestFile {
    char path[40] = "/tmp/jl-io-file.XXXXXX";
    int fd = mkstemp(path);
    ~LegacyTestFile() { if (fd >= 0) { close(fd); unlink(path); } }
};

void run_legacy_without_metadata() {
    LegacyTestFile file;
    require(file.fd >= 0 && unlink(file.path) == 0, "prepare deleted open file failed");
    IOUsageCollector collector;
    require(collector.init(nlohmann::json::object()), "legacy collector init failed");
    Job job{}; job.JobID = 7; job.JobPIDs = {getpid()};

    metadata_calls = 0;
    const auto result = std::any_cast<std::vector<IOUsageInfo>>(collector.collect(job));

    require(metadata_calls == 0, "legacy IO attempted a blocking metadata lookup");
    require(result.size() == 1 && result.front().file_info.count(file.fd) == 1,
            "legacy collector discarded file details without metadata");
    const auto output = std::any_cast<nlohmann::json>(collector.get_writer_parser("ESWriter")(result));
    for (const auto& file_json : output.at("process_data").at(0).at("files")) {
        require(file_json.at("pos").is_null(), "unknown file size must be null");
        require(!file_json.contains("path"), "file path must not leak by default");
        require(!file_json.contains("mount_point"), "mount point must not leak by default");
    }
    collector.deinit();

    // 显式开启后才输出路径
    IOUsageCollector with_paths;
    require(with_paths.init(nlohmann::json{{"collect_file_paths", "true"}}),
            "collector with paths init failed");
    Job job2{}; job2.JobID = 8; job2.JobPIDs = {getpid()};
    const auto result2 = std::any_cast<std::vector<IOUsageInfo>>(with_paths.collect(job2));
    const auto output2 = std::any_cast<nlohmann::json>(
        with_paths.get_writer_parser("ESWriter")(result2));
    bool saw_path = false;
    for (const auto& file_json : output2.at("process_data").at(0).at("files")) {
        if (file_json.contains("path")) saw_path = true;
    }
    require(saw_path, "collect_file_paths=true must emit per-fd paths");
    with_paths.deinit();
    std::cout << "PASS legacy IO details require no target metadata lookup\n";
}

