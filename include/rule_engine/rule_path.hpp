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

// 规则文件路径校验：规则名可来自 etcd / RPC，必须限制在规则目录内。

#include <cctype>
#include <filesystem>
#include <string>
#include <system_error>

namespace joblens {
namespace rules {

inline bool path_within(const std::filesystem::path& child,
                        const std::filesystem::path& parent) {
    std::error_code ec;
    const auto root = std::filesystem::weakly_canonical(parent, ec);
    if (ec) return false;
    const auto target = std::filesystem::weakly_canonical(child, ec);
    if (ec) return false;
    const auto root_str = root.string();
    const auto target_str = target.string();
    if (target_str == root_str) return true;
    if (target_str.size() <= root_str.size()) return false;
    return target_str.compare(0, root_str.size(), root_str) == 0 &&
           target_str[root_str.size()] == '/';
}

// 只允许 [A-Za-z0-9._-]，禁止空名、"."、".."、路径分隔符与控制字符。
inline bool filename_ok(const std::string& name) {
    if (name.empty() || name.size() > 128) return false;
    if (name == "." || name == "..") return false;
    for (const char c : name) {
        const auto u = static_cast<unsigned char>(c);
        if (std::isalnum(u) || c == '_' || c == '.' || c == '-') continue;
        return false;
    }
    return true;
}

} // namespace rules
} // namespace joblens
