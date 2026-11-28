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
#include <string>
#include <cctype>

// ClassAd 字符串值解码与组值校验。
// 现有 get_ad_field() 返回原始 token：字符串带引号与转义，缺字段/undefined 也原样返回。
// 组值（AcctGroup / PrimaryUnixGroup）入库前必须经过这里，否则会把
// `"JUNO.juno.default"`（带引号）或 `undefined` 当组名存进 LevelDB/ES。

namespace ClassAdValue {

// 组值与探测重试的全局上限（Condor/Slurm 共用）。
inline constexpr size_t kGroupValueMaxLen = 256;      // 字段最大长度；超长拒绝
inline constexpr uint8_t kMaxGroupProbeAttempts = 3;  // 读失败的有界重试次数

// 解码 ClassAd 字符串字面量：
// - 去首尾空白与成对双引号
// - 还原转义：\" \\ \n \t（其余 \x 保留 x）
// - `undefined` / 空串 → 空串（缺字段与显式 undefined 不区分，都按“无值”处理）
inline std::string decode_string(std::string v) {
    // 去首尾空白
    while (!v.empty() && std::isspace(static_cast<unsigned char>(v.front()))) v.erase(v.begin());
    while (!v.empty() && std::isspace(static_cast<unsigned char>(v.back()))) v.pop_back();

    if (v.size() >= 2 && v.front() == '"' && v.back() == '"') {
        const size_t last = v.size() - 1;  // 末尾引号下标；正文为 [1, last-1]
        std::string out;
        out.reserve(v.size() - 2);
        for (size_t i = 1; i < last; ++i) {
            char c = v[i];
            if (c == '\\' && i + 1 < last) {
                char n = v[i + 1];
                switch (n) {
                    case '"':  out += '"';  ++i; continue;
                    case '\\': out += '\\'; ++i; continue;
                    case 'n':  out += '\n'; ++i; continue;
                    case 't':  out += '\t'; ++i; continue;
                    default: break;
                }
            }
            out += c;
        }
        v = std::move(out);
    }

    if (v == "undefined" || v == "UNDEFINED") return "";
    return v;
}

// 组值合法性：长度 <= max_len，且不含控制字符（<0x20 / 0x7F）。
// 值来自作业可控的 ad/env，超长或含控制字符一律拒绝（计入 group_probe_stats.failures），
// 防止污染 ES keyword 字段与 Prometheus label。
inline bool valid_group_value(const std::string& v, size_t max_len) {
    if (v.size() > max_len) return false;
    for (unsigned char c : v) {
        if (c < 0x20 || c == 0x7F) return false;
    }
    return true;
}

} // namespace ClassAdValue
