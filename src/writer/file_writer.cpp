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
#include "writer/file_writer.hpp"
#include "core/writer_manager.hpp"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <iomanip>
#include "collector/collector_utils.hpp"
#include "core/collector_registry.hpp"

AUTO_REGISTER_WRITER(
    FileWriter,
    "Writer that appends line-delimited JSON to a specified file",
    ConfigParams{
        {"path", "Path to the output file where data will be appended"}
    }
)

using json = nlohmann::json;

static std::string format_timestamp(std::chrono::system_clock::time_point tp)
{
    std::time_t t = std::chrono::system_clock::to_time_t(tp);
    std::tm utc;
    gmtime_r(&t, &utc);
    utc.tm_hour += 8;
    std::mktime(&utc);
    std::ostringstream os;
    os << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S+0800");
    return os.str();
}

FileWriter::FileWriter(std::string name, std::string type, std::string config_name)
    : BaseWriter(name, type, config_name)
{
    auto file_path = Config::instance().getString(config_name, "path");
    ofs_.open(file_path, std::ios::app);
    if (!ofs_.is_open()) {
        spdlog::error("file_writer: failed to open file '{}'", file_path);
        throw std::runtime_error("file_writer: cannot open output file: " + file_path);
    }
    spdlog::info("file_writer: opened output file '{}'", file_path);
}

bool FileWriter::flush_impl(const std::vector<write_data>& batch)
{
    if (batch.empty()) {
        return true;
    }

    bool all_success = true;

    for (const auto& w : batch)
    {
        const auto& collect_name = std::get<0>(w);
        const auto& job          = std::get<1>(w);
        const auto& any_data     = std::get<2>(w);
        const auto& ts           = std::get<3>(w);

        json doc;
        doc["@timestamp"]    = format_timestamp(ts);
        doc["hostname"]      = collector_utils::get_hostname();
        doc["collector_name"] = collect_name;
        doc["job_info"]      = job_to_json(job);

        json parsed_data;
        auto parser_func = CollectorRegistry::instance().getCollectorParser(collect_name, type_);
        if (!parser_func) {
            // 如果 collector 没有为当前 writer 注册 parser，直接将数据转为 JSON 字符串
            spdlog::debug("file_writer: no parser registered for collector '{}', using raw data dump", collect_name);
            try {
                auto raw = std::any_cast<std::string>(any_data);
                parsed_data["raw"] = raw;
            } catch (const std::bad_any_cast&) {
                parsed_data["raw"] = "(non-string data)";
            }
        } else {
            try {
                auto parsed = parser_func(any_data);
                parsed_data = std::any_cast<json>(parsed);
            } catch (const std::exception& e) {
                spdlog::error("file_writer: failed to parse data for collector '{}': {}", collect_name, e.what());
                parsed_data["error"] = std::string(e.what());
                all_success = false;
            }
        }

        doc["data"] = parsed_data;

        ofs_ << doc.dump() << '\n';
        if (!ofs_.good()) {
            spdlog::error("file_writer: failed to write to file");
            all_success = false;
        }
    }

    ofs_.flush();
    spdlog::debug("file_writer: flushed {} records", batch.size());
    return all_success;
}
