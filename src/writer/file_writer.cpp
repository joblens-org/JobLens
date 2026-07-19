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
#include <map>
#include <unordered_set>
#include <filesystem>
#include <unistd.h>
#include "core/collector_registry.hpp"

AUTO_REGISTER_WRITER(
    FileWriter,
    "Writer that writes collector-formatted plain text to output files with configurable routing and rotation",
    ConfigParams{
        {"path", "Default output file path (required)"},
        {"write_mode", "Write mode: 'append' (default) or 'overwrite'"},
        {"flush_on_shutdown", "Flush remaining records on graceful shutdown (default: true)"},
        {"enable_rotation", "Enable size-based file rotation in append mode (default: false)"},
        {"max_file_size_bytes", "Rotation threshold in bytes (default: 104857600, 100MB)"},
        {"max_files", "Maximum rotated files to retain (default: 5)"},
        {"outputs", "List of collector→file routing entries [{collector_name, file_path}]"}
    }
)

FileWriterOptions FileWriter::parseOptions(const std::string& config_name)
{
    FileWriterOptions opts;
    auto& cfg = Config::instance();

    opts.path = cfg.getString(config_name, "path");
    opts.write_mode = cfg.getString(config_name, "write_mode", "append");
    opts.flush_on_shutdown = cfg.getBool(config_name, "flush_on_shutdown", true);
    opts.enable_rotation = cfg.getBool(config_name, "enable_rotation", false);
    auto raw_max_size = cfg.getInt(config_name, "max_file_size_bytes", 104857600);
    if (raw_max_size < 0) {
        throw std::runtime_error(
            "file_writer: max_file_size_bytes must be non-negative, got " +
            std::to_string(raw_max_size));
    }
    opts.max_file_size_bytes = static_cast<size_t>(raw_max_size);
    opts.max_files = cfg.getInt(config_name, "max_files", 5);

    if (cfg.has(config_name, "outputs")) {
        opts.outputs = cfg.getArray<OutputEntry>(config_name, "outputs",
            [](const YAML::Node& node) {
                OutputEntry entry;
                entry.collector_name = node["collector_name"].as<std::string>();
                entry.file_path = node["file_path"].as<std::string>();
                return entry;
            });
    }

    return opts;
}

void FileWriter::validateOptions(const FileWriterOptions& opts)
{
    if (opts.path.empty()) {
        throw std::runtime_error(
            "file_writer: 'path' is required and must be non-empty");
    }

    if (opts.write_mode != "append" && opts.write_mode != "overwrite") {
        throw std::runtime_error(
            "file_writer: invalid write_mode '" + opts.write_mode +
            "', expected 'append' or 'overwrite'");
    }

    // overwrite 模式不允许启用轮转
    if (opts.write_mode == "overwrite" && opts.enable_rotation) {
        throw std::runtime_error(
            "file_writer: write_mode 'overwrite' is incompatible with "
            "enable_rotation, rotation is only supported in append mode");
    }

    if (opts.enable_rotation) {
        if (opts.max_file_size_bytes == 0) {
            throw std::runtime_error(
                "file_writer: max_file_size_bytes must be positive when "
                "enable_rotation is true");
        }
        if (opts.max_files <= 0) {
            throw std::runtime_error(
                "file_writer: max_files must be positive when "
                "enable_rotation is true");
        }
    }

    // outputs 条目校验
    if (!opts.outputs.empty()) {
        std::unordered_set<std::string> collector_names;
        std::unordered_set<std::string> file_paths;
        bool is_overwrite = (opts.write_mode == "overwrite");

        for (const auto& entry : opts.outputs) {
            if (entry.collector_name.empty()) {
                throw std::runtime_error(
                    "file_writer: 'outputs' entry has empty collector_name");
            }
            if (entry.file_path.empty()) {
                throw std::runtime_error(
                    "file_writer: 'outputs' entry for collector '" +
                    entry.collector_name + "' has empty file_path");
            }

            // 不允许重复的 collector_name
            if (!collector_names.insert(entry.collector_name).second) {
                throw std::runtime_error(
                    "file_writer: duplicate collector_name '" +
                    entry.collector_name + "' in 'outputs'");
            }

            if (is_overwrite) {
                if (!file_paths.insert(entry.file_path).second) {
                    throw std::runtime_error(
                        "file_writer: duplicate file_path '" +
                        entry.file_path + "' in 'outputs' "
                        "(forbidden in overwrite mode)");
                }
            }
        }

        // outputs 中的 file_path 若等于默认 path，则未匹配的 collector 与
        // 已匹配的 collector 会解析到同一目标文件，overwrite 模式下产生歧义
        if (is_overwrite) {
            for (const auto& entry : opts.outputs) {
                if (entry.file_path == opts.path) {
                    throw std::runtime_error(
                        "file_writer: 'outputs' entry for collector '" +
                        entry.collector_name +
                        "' resolves to default path '" + opts.path +
                        "', which is forbidden in overwrite mode "
                        "(ambiguous destination)");
                }
            }
        }
    }
}

FileWriter::FileWriter(std::string name, std::string type, std::string config_name)
    : BaseWriter(name, type, config_name)
{
    options_ = parseOptions(config_name);
    validateOptions(options_);

    // 构建 collector_name → 目标文件路径的快速查找映射
    for (const auto& entry : options_.outputs) {
        collector_to_path_[entry.collector_name] = entry.file_path;
    }

    ofs_.open(options_.path, std::ios::app);
    if (!ofs_.is_open()) {
        spdlog::error("file_writer: failed to open file '{}'", options_.path);
        throw std::runtime_error("file_writer: cannot open output file: " + options_.path);
    }
    spdlog::info("file_writer: opened '{}' in {} mode", options_.path, options_.write_mode);
}

std::string FileWriter::resolveDestinationPath(const std::string& collector_name)
{
    auto it = collector_to_path_.find(collector_name);
    if (it != collector_to_path_.end()) {
        return it->second;
    }

    // 回退到默认路径，每个 collector 仅警告一次
    if (warned_fallback_collectors_.insert(collector_name).second) {
        spdlog::warn("file_writer: collector '{}' not found in outputs, "
                     "falling back to default path '{}'",
                     collector_name, options_.path);
    }

    return options_.path;
}

bool FileWriter::performRotation(const std::string& path)
{
    namespace fs = std::filesystem;

    // 1. 删除最旧的旋转文件 path.{max_files}
    std::string oldest = path + "." + std::to_string(options_.max_files);
    {
        std::error_code ec;
        if (fs::exists(oldest, ec)) {
            fs::remove(oldest, ec);
            if (ec) {
                spdlog::error("file_writer: failed to remove oldest rotated file '{}': {}",
                              oldest, ec.message());
                return false;
            }
        }
    }

    // 2. 级联重命名：path.(N-1) → path.N（从大到小降序，避免覆盖）
    for (int n = options_.max_files - 1; n >= 1; --n) {
        std::string old_name = path + "." + std::to_string(n);
        std::string new_name = path + "." + std::to_string(n + 1);

        {
            std::error_code ec;
            if (!fs::exists(old_name, ec)) {
                continue;
            }
        }

        // 安全处理：如果目标文件意外存在，先删除
        {
            std::error_code ec;
            if (fs::exists(new_name, ec)) {
                fs::remove(new_name, ec);
                if (ec) {
                    spdlog::error("file_writer: failed to remove unexpected target '{}' "
                                  "before cascade rename: {}",
                                  new_name, ec.message());
                    return false;
                }
            }
        }

        {
            std::error_code ec;
            fs::rename(old_name, new_name, ec);
            if (ec) {
                spdlog::error("file_writer: failed to rename '{}' to '{}': {}",
                             old_name, new_name, ec.message());
                return false;
            }
        }
    }

    // 3. 重命名活跃文件 path → path.1
    {
        std::error_code ec;
        if (fs::exists(path, ec) && !ec) {
            std::string first_rotated = path + ".1";

            // 移除可能残留的 path.1
            {
                std::error_code ec2;
                if (fs::exists(first_rotated, ec2)) {
                    fs::remove(first_rotated, ec2);
                    if (ec2) {
                        spdlog::error("file_writer: failed to remove residual '{}' "
                                      "before active rename: {}",
                                      first_rotated, ec2.message());
                        return false;
                    }
                }
            }

            {
                std::error_code ec3;
                fs::rename(path, first_rotated, ec3);
                if (ec3) {
                    spdlog::error("file_writer: failed to rename active file '{}' to '{}': {}",
                                 path, first_rotated, ec3.message());
                    return false;
                }
            }

            spdlog::info("file_writer: rotated '{}' to '{}' (max_files={})",
                         path, first_rotated, options_.max_files);
        }
    }
    // 如果活跃文件不存在（首次写入），无需旋转，直接返回成功

    return true;
}

bool FileWriter::flush_impl(const std::vector<write_data>& batch)
{
    if (batch.empty()) {
        return true;
    }

    bool all_success = true;

    // 第一遍：将每条记录解析为 FileWriter 纯文本块并按目标路径分组
    // 使用有序 map 以保证确定性输出顺序（文件路径为 key）
    std::map<std::string, std::vector<std::string>> path_to_payloads;

    for (const auto& w : batch)
    {
        const auto& collect_name = std::get<0>(w);
        const auto& any_data     = std::get<2>(w);

        // 构造 V2 parser 上下文，传递 writer 元信息、collector 名称、Job 和时间戳
        WriterParseContext ctx{name_, type_, config_name_, collect_name, std::get<1>(w), std::get<3>(w)};

        std::string payload;
        auto parser_func = CollectorRegistry::instance().resolveBestParserV2(collect_name, type_);
        if (!parser_func) {
            // 如果 collector 没有为当前 writer 注册 parser，仅支持原始 std::string 数据回退。
            spdlog::debug("file_writer: no parser for collector '{}', using raw string fallback", collect_name);
            try {
                payload = std::any_cast<std::string>(any_data);
            } catch (const std::bad_any_cast&) {
                spdlog::error("file_writer: collector '{}' has no FileWriter parser and raw data is not std::string",
                              collect_name);
                all_success = false;
                continue;
            }
        } else {
            spdlog::debug("file_writer: using parser for collector '{}'", collect_name);
            try {
                auto parsed = parser_func(ctx, any_data);
                payload = std::any_cast<std::string>(parsed);
            } catch (const std::exception& e) {
                spdlog::error("file_writer: failed to parse data for collector '{}': {}", collect_name, e.what());
                all_success = false;
                continue;
            }
        }

        // 解析目标路径并分组
        std::string dest_path = resolveDestinationPath(collect_name);
        path_to_payloads[dest_path].push_back(std::move(payload));
    }

    // 覆写模式：使用同目录临时文件 + 原子 rename，每个目标路径独立处理
    if (options_.write_mode == "overwrite") {
        namespace fs = std::filesystem;

        for (auto& [path, payloads] : path_to_payloads) {
            // 生成唯一临时文件名：target.tmp.<pid>.<timestamp>
            std::string tmp_path = path + ".tmp."
                + std::to_string(getpid()) + "."
                + std::to_string(
                    std::chrono::system_clock::now().time_since_epoch().count());

            std::ofstream tmp_ofs(tmp_path, std::ios::out | std::ios::trunc);
            if (!tmp_ofs.is_open()) {
                spdlog::error("file_writer: failed to create temp file '{}' for overwrite",
                              tmp_path);
                all_success = false;
                continue;
            }

            bool write_ok = true;
            for (const auto& payload : payloads) {
                tmp_ofs << payload;
                if (!tmp_ofs.good()) {
                    spdlog::error("file_writer: failed to write to temp file '{}'",
                                  tmp_path);
                    write_ok = false;
                    all_success = false;
                    break;
                }
            }

            tmp_ofs.flush();
            if (!tmp_ofs.good()) {
                spdlog::error("file_writer: flush failed for temp file '{}'", tmp_path);
                write_ok = false;
                all_success = false;
            }

            tmp_ofs.close();  // 必须在 rename 前关闭流

            if (write_ok) {
                std::error_code ec;
                fs::rename(tmp_path, path, ec);
                if (ec) {
                    spdlog::error("file_writer: rename failed from '{}' to '{}': {}",
                                  tmp_path, path, ec.message());
                    // 清理残留临时文件（最佳努力）
                    fs::remove(tmp_path, ec);
                    all_success = false;
                } else {
                    spdlog::debug("file_writer: overwrote '{}' with {} payload(s)",
                                  path, payloads.size());
                }
            } else {
                // 写入/刷新失败，清理临时文件，保持旧目标文件不变
                std::error_code ec;
                fs::remove(tmp_path, ec);
                if (ec) {
                    spdlog::warn("file_writer: failed to remove temp file '{}': {}",
                                 tmp_path, ec.message());
                }
            }
        }

        spdlog::debug("file_writer: overwrote {} records to {} destination(s)",
                      batch.size(), path_to_payloads.size());
        return all_success;
    }

    // 追加模式：按目标路径写入文件，旋转检查在写入前执行
    {
        namespace fs = std::filesystem;

        for (auto& [path, payloads] : path_to_payloads)
        {
            // 计算该目标路径的批次字节数（用于旋转阈值判断）
            size_t batch_bytes = 0;
            for (const auto& payload : payloads) {
                batch_bytes += payload.size();
            }

            // 旋转检查并执行（仅追加模式 + 启用旋转时）
            if (options_.enable_rotation) {
                size_t current_size = 0;
                std::error_code ec;
                if (fs::exists(path, ec) && !ec) {
                    current_size = fs::file_size(path, ec);
                    if (ec) {
                        spdlog::warn("file_writer: cannot get file size for '{}': {}",
                                     path, ec.message());
                        current_size = 0;
                    }
                }

                if (current_size + batch_bytes > options_.max_file_size_bytes) {
                    // 关闭当前持久化流，以便旋转重命名
                    if (path == options_.path) {
                        if (ofs_.is_open()) {
                            ofs_.close();
                        }
                    } else {
                        auto it = destination_streams_.find(path);
                        if (it != destination_streams_.end() && it->second.is_open()) {
                            it->second.close();
                        }
                    }

                    // 执行旋转级联
                    if (!performRotation(path)) {
                        spdlog::error("file_writer: rotation failed for '{}'", path);
                        all_success = false;
                        // 最佳努力重新打开流，供后续 flush 使用
                        if (path == options_.path) {
                            ofs_.open(path, std::ios::app);
                        } else {
                            destination_streams_[path].open(path, std::ios::app);
                        }
                        continue;  // 跳过当前目标批次，不写入
                    }

                    // 旋转成功：重新打开流，写入整个批次
                    if (path == options_.path) {
                        ofs_.open(path, std::ios::app);
                        if (!ofs_.is_open()) {
                            spdlog::error("file_writer: failed to reopen '{}' after rotation",
                                          path);
                            all_success = false;
                            continue;
                        }
                    } else {
                        auto& ofs = destination_streams_[path];
                        ofs.open(path, std::ios::app);
                        if (!ofs.is_open()) {
                            spdlog::error("file_writer: failed to reopen '{}' after rotation",
                                          path);
                            all_success = false;
                            continue;
                        }
                    }
                }
            }

            // 写入数据：FileWriter parser 返回完整文本块（包括所需换行），这里按原样写入。
            if (path == options_.path) {
                // 默认路径使用持久化 ofs_（向后兼容，保持单路径性能不变）
                for (const auto& payload : payloads) {
                    ofs_ << payload;
                    if (!ofs_.good()) {
                        spdlog::error("file_writer: failed to write to default file '{}'", path);
                        all_success = false;
                    }
                }
                ofs_.flush();
                if (!ofs_.good()) {
                    spdlog::error("file_writer: flush failed for default file '{}'", path);
                    all_success = false;
                }
            } else {
                // 非默认路径：使用持久化流（懒加载打开，完整关闭留给 Task 8）
                auto& ofs = destination_streams_[path];
                if (!ofs.is_open()) {
                    ofs.open(path, std::ios::app);
                    if (!ofs.is_open()) {
                        spdlog::error("file_writer: failed to open persistent stream for '{}'", path);
                        all_success = false;
                        continue;
                    }
                    spdlog::info("file_writer: opened persistent append stream for '{}'", path);
                }
                for (const auto& payload : payloads) {
                    ofs << payload;
                    if (!ofs.good()) {
                        spdlog::error("file_writer: failed to write to file '{}'", path);
                        all_success = false;
                    }
                }
                ofs.flush();
                if (!ofs.good()) {
                    spdlog::error("file_writer: flush failed for file '{}'", path);
                    all_success = false;
                }
                spdlog::debug("file_writer: wrote {} payload(s) to '{}'", payloads.size(), path);
            }
        }
    }

    spdlog::debug("file_writer: flushed {} records to {} destination(s)", batch.size(), path_to_payloads.size());
    return all_success;
}

void FileWriter::do_shutdown()
{
    // do_shutdown() 在 BaseWriter::shutdown() 的最后阶段被调用，
    // 此时 flush worker 已停止，最终缓冲区已通过 flush_buffer(*front_) 刷入 flush_impl。
    // 因此此处的职责仅为：关闭 FileWriter 管理的所有持久化 ofstream 句柄。
    //
    // flush_on_shutdown 语义：
    // - true（默认）：显式调用 ofstream::flush() 确保内核缓冲区数据落盘，
    //   然后关闭流。若 flush 后 good() 返回 false，记录错误日志但不阻止关闭。
    // - false：跳过显式 flush()，直接关闭流。
    //   close() 内部仍会隐式 flush，但不检查错误状态。
    //   注意：BaseWriter 的最终缓冲区刷新（flush_buffer）发生在 do_shutdown() 之前，
    //   flush_on_shutdown=false 不会绕过 BaseWriter 的安全保障。
    //   此选项仅影响 FileWriter 层面的 ofstream 显式刷新行为。

    spdlog::info("file_writer: do_shutdown() starting for '{}'", name_);

    // 1. 关闭默认持久化流 ofs_
    if (ofs_.is_open()) {
        if (options_.flush_on_shutdown) {
            ofs_.flush();
            if (!ofs_.good()) {
                spdlog::error("file_writer: flush failed for default stream '{}' during shutdown",
                              options_.path);
            }
        }
        ofs_.close();
        spdlog::info("file_writer: closed default stream '{}'", options_.path);
    }

    // 2. 关闭所有非默认路径持久化流 destination_streams_
    for (auto& [path, stream] : destination_streams_) {
        if (stream.is_open()) {
            if (options_.flush_on_shutdown) {
                stream.flush();
                if (!stream.good()) {
                    spdlog::error("file_writer: flush failed for stream '{}' during shutdown", path);
                }
            }
            stream.close();
            spdlog::info("file_writer: closed stream '{}'", path);
        }
    }

    // 3. 清除 destination_streams_ 映射，防止重复 shutdown 使用过时流
    destination_streams_.clear();

    spdlog::info("file_writer: do_shutdown() complete for '{}'", name_);
}
