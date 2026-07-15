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
#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "writer/base_writer.hpp"

struct OutputEntry {
    std::string collector_name;
    std::string file_path;
};

// 所有字段均有默认值，确保仅包含 path 的旧配置仍可正常工作
struct FileWriterOptions {
    std::string path;
    std::string write_mode = "append";
    bool flush_on_shutdown = true;
    bool enable_rotation = false;
    size_t max_file_size_bytes = 104857600;
    int max_files = 5;
    std::vector<OutputEntry> outputs;
};

class FileWriter : public BaseWriter {
public:
    explicit FileWriter(std::string name, std::string type, std::string config_name);

protected:
    bool flush_impl(const std::vector<write_data>& batch) override;
    void do_shutdown() override;

private:
    static FileWriterOptions parseOptions(const std::string& config_name);
    static void validateOptions(const FileWriterOptions& opts);

    // 根据 collector_name 解析目标文件路径：先查 outputs 映射，找不到则回退到 options_.path
    std::string resolveDestinationPath(const std::string& collector_name);

    // 对指定路径执行文件旋转级联（path.max_files 删除，path.(N-1)→path.N 降序，path→path.1）
    // 返回 true 表示旋转成功，false 表示部分或全部步骤失败（已记录错误日志）
    bool performRotation(const std::string& path);

    FileWriterOptions options_;
    std::ofstream ofs_;

    // collector_name → 目标文件路径的映射（从 options_.outputs 构建）
    std::unordered_map<std::string, std::string> collector_to_path_;
    // 已发出回退警告的 collector 名称集合（每条 collector 仅警告一次）
    std::unordered_set<std::string> warned_fallback_collectors_;

    // 追加模式下的非默认路径持久化流（懒加载，首次使用才打开）
    // 在 do_shutdown() 中逐一 flush/close 并最终 clear() 此映射
    std::unordered_map<std::string, std::ofstream> destination_streams_;
};