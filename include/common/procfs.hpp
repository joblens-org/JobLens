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
// Root-explicit procfs / cgroup v2 readers (hoisted from core/job_discovery
// so every consumer — discovery adapters, the legacy CondorJob/SlurmJob
// helpers in common/utils.hpp and tests — shares one implementation).
//
// Every function takes the roots it reads explicitly, so unprivileged
// fixtures can present a fake procfs/cgroup tree instead of requiring root
// or live schedulers; production callers pass "/" and "/sys/fs/cgroup".
// The functions are pure reads: no caching except read_boot_id (see there).
#pragma once

#include "core/job_identity.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <sys/types.h>

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------
namespace procfs_detail {

// Whole-file read. Returns false with a message instead of throwing so a scan
// pass reports missing/unreadable entries instead of aborting.
inline bool read_file(const std::filesystem::path& path, std::string& out,
                      std::string& error)
{
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        error = "cannot read '" + path.string() + "'";
        return false;
    }
    out.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    if (!file.good() && !file.eof()) {
        error = "read error on '" + path.string() + "'";
        return false;
    }
    return true;
}

inline std::filesystem::path pid_path(const std::string& proc_root, pid_t pid,
                                      const char* leaf)
{
    return std::filesystem::path(proc_root) / std::to_string(pid) / leaf;
}

// Fields of /proc/<pid>/stat after the ")" of the command name: [0] is field 3
// (state), so field N lives at index N - 3.
inline std::optional<std::vector<std::string>> read_stat_fields(
    const std::string& proc_root, pid_t pid)
{
    std::string line;
    std::string error;
    if (!read_file(pid_path(proc_root, pid, "stat"), line, error)) return std::nullopt;

    const auto last_rparen = line.rfind(')');
    if (last_rparen == std::string::npos) return std::nullopt;

    std::istringstream stream(line.substr(last_rparen + 1));
    std::vector<std::string> fields;
    std::string field;
    while (stream >> field) fields.push_back(field);
    return fields;
}

} // namespace procfs_detail

// ---------------------------------------------------------------------------
// procfs / cgroup readers
// ---------------------------------------------------------------------------

// Numeric pid directories of `proc_root`, ascending. Unreadable roots yield an
// empty vector; callers that need the error call ensure_readable_directory().
inline std::vector<pid_t> list_proc_pids(const std::string& proc_root)
{
    std::vector<pid_t> pids;
    std::error_code ec;
    std::filesystem::directory_iterator iterator(proc_root, ec);
    if (ec) return pids;

    for (const auto& entry : iterator) {
        std::error_code entry_ec;
        if (!entry.is_directory(entry_ec) || entry_ec) continue;
        const auto name = entry.path().filename().string();
        if (name.empty()) continue;
        const bool numeric = std::all_of(name.begin(), name.end(), [](unsigned char c) {
            return std::isdigit(c) != 0;
        });
        if (!numeric) continue;
        try {
            pids.push_back(static_cast<pid_t>(std::stol(name)));
        } catch (const std::exception&) {
            continue;
        }
    }
    std::sort(pids.begin(), pids.end());
    return pids;
}

// Returns false with a message when `path` is missing or not a directory.
inline bool ensure_readable_directory(const std::string& path, std::string& error)
{
    std::error_code ec;
    if (!std::filesystem::is_directory(path, ec) || ec) {
        error = "'" + path + "' is not a readable directory";
        if (ec) error += ": " + ec.message();
        return false;
    }
    return true;
}

// NUL-separated cmdline turned into a space-separated string.
inline bool read_proc_cmdline(const std::string& proc_root, pid_t pid,
                              std::string& out, std::string& error)
{
    std::string raw;
    if (!procfs_detail::read_file(procfs_detail::pid_path(proc_root, pid, "cmdline"),
                                  raw, error))
        return false;
    for (char& c : raw) {
        if (c == '\0') c = ' ';
    }
    while (!raw.empty() && raw.back() == ' ') raw.pop_back();
    out = std::move(raw);
    return true;
}

// Reads one environment variable of a process. false = environ unreadable.
inline bool read_proc_env(const std::string& proc_root, pid_t pid,
                          const std::string& key, std::string& out,
                          std::string& error)
{
    std::string raw;
    if (!procfs_detail::read_file(procfs_detail::pid_path(proc_root, pid, "environ"),
                                  raw, error))
        return false;

    std::size_t pos = 0;
    while (pos < raw.size()) {
        std::size_t end = raw.find('\0', pos);
        if (end == std::string::npos) end = raw.size();
        const std::string entry = raw.substr(pos, end - pos);
        const auto eq = entry.find('=');
        if (eq != std::string::npos && entry.compare(0, eq, key) == 0) {
            out = entry.substr(eq + 1);
            return true;
        }
        pos = end + 1;
    }
    out.clear();
    return true;
}

// Reads <pid>/environ ONCE and extracts every requested key in a single pass
// (the discovery hot path asks for several variables of the same process).
// Absent keys are simply missing from the map; false = environ unreadable.
inline bool read_proc_env_values(const std::string& proc_root, pid_t pid,
                                 const std::vector<std::string>& keys,
                                 std::map<std::string, std::string>& out,
                                 std::string& error)
{
    out.clear();
    std::string raw;
    if (!procfs_detail::read_file(procfs_detail::pid_path(proc_root, pid, "environ"),
                                  raw, error))
        return false;

    std::size_t pos = 0;
    while (pos < raw.size()) {
        std::size_t end = raw.find('\0', pos);
        if (end == std::string::npos) end = raw.size();
        const std::size_t eq = raw.find('=', pos);
        if (eq != std::string::npos && eq < end) {
            // entry = key '=' value; a wanted key matches when the entry
            // starts with it and '=' follows immediately after.
            const std::size_t key_len = eq - pos;
            for (const auto& key : keys) {
                if (key.size() == key_len && raw.compare(pos, key_len, key) == 0) {
                    out[key] = raw.substr(eq + 1, end - eq - 1);
                    break;
                }
            }
        }
        pos = end + 1;
    }
    return true;
}

inline std::optional<pid_t> read_proc_ppid(const std::string& proc_root, pid_t pid)
{
    const auto fields = procfs_detail::read_stat_fields(proc_root, pid);
    if (!fields || fields->size() <= 1) return std::nullopt;
    try {
        return static_cast<pid_t>(std::stol(fields->at(1)));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

inline std::optional<std::uint64_t> read_proc_start_ticks(const std::string& proc_root,
                                                          pid_t pid)
{
    const auto fields = procfs_detail::read_stat_fields(proc_root, pid);
    if (!fields || fields->size() <= 19) return std::nullopt;
    try {
        return std::stoull(fields->at(19));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

// Boot identity for the host that owns `proc_root`. Empty when unavailable, in
// which case no ProcessIdentity/manager identity is complete.
inline std::string read_boot_id(const std::string& proc_root)
{
    // The boot id of one root never changes while the process runs; the
    // discovery hot path (every exec hint) reads it, so cache per root.
    // Production has a single root ("/proc"); fixtures get their own entry.
    static std::mutex cache_mutex;
    static std::map<std::string, std::string> cache;
    {
        std::lock_guard<std::mutex> lock(cache_mutex);
        const auto it = cache.find(proc_root);
        if (it != cache.end()) return it->second;
    }
    std::string content;
    std::string error;
    if (!procfs_detail::read_file(std::filesystem::path(proc_root) / "sys" / "kernel" /
                                      "random" / "boot_id",
                                  content, error))
        return {};
    while (!content.empty() && (content.back() == '\n' || content.back() == '\r'))
        content.pop_back();
    if (!content.empty()) {
        std::lock_guard<std::mutex> lock(cache_mutex);
        cache.emplace(proc_root, content);
    }
    return content;
}

// Absolute cgroup v2 directory of a process. Empty for the root cgroup (not
// execution-scoped evidence) or when the file is missing.
inline std::string read_proc_cgroup_path(const std::string& proc_root, pid_t pid,
                                         const std::string& cgroup_root)
{
    std::string content;
    std::string error;
    if (!procfs_detail::read_file(procfs_detail::pid_path(proc_root, pid, "cgroup"),
                                  content, error))
        return {};

    std::istringstream lines(content);
    std::string line;
    while (std::getline(lines, line)) {
        if (line.compare(0, 3, "0::") != 0) continue;
        std::string relative = line.substr(3);
        while (!relative.empty() && (relative.back() == '\r' || relative.back() == '\n'))
            relative.pop_back();
        if (relative.empty() || relative == "/") {
            // The root cgroup is not execution-scoped evidence: using its
            // cgroup.procs would claim every process on the host.
            return {};
        }
        std::filesystem::path path(relative);
        if (path.is_absolute()) path = path.relative_path();
        return (std::filesystem::path(cgroup_root) / path).lexically_normal().string();
    }
    return {};
}

// PIDs listed in <cgroup_dir>/cgroup.procs. Empty when unreadable.
inline std::vector<pid_t> read_cgroup_members(const std::string& cgroup_dir)
{
    std::vector<pid_t> members;
    if (cgroup_dir.empty()) return members;

    std::string content;
    std::string error;
    if (!procfs_detail::read_file(std::filesystem::path(cgroup_dir) / "cgroup.procs",
                                  content, error))
        return members;

    std::istringstream lines(content);
    pid_t pid = 0;
    while (lines >> pid) members.push_back(pid);
    return members;
}

// Error-reporting variant: false means the membership is unknown ("not empty"
// can never be concluded from it); true means the list is authoritative, an
// empty list included.
inline bool read_cgroup_members(const std::string& cgroup_dir,
                                std::vector<pid_t>& members, std::string& error)
{
    members.clear();
    error.clear();
    if (cgroup_dir.empty()) {
        error = "no cgroup path";
        return false;
    }

    std::string content;
    if (!procfs_detail::read_file(std::filesystem::path(cgroup_dir) / "cgroup.procs",
                                  content, error)) {
        error = "cannot read '" + cgroup_dir + "/cgroup.procs': " + error;
        return false;
    }

    std::istringstream lines(content);
    pid_t pid = 0;
    while (lines >> pid) members.push_back(pid);
    return true;
}

// Process identity from the fake or real procfs. Fields that cannot be read
// stay zero; ProcessIdentity::complete() reports whether the evidence is
// sufficient, and no caller may upgrade a partial identity.
inline ProcessIdentity read_process_identity(const std::string& proc_root,
                                             const std::string& boot_id, pid_t pid)
{
    ProcessIdentity identity;
    if (pid <= 0) return identity;
    identity.pid = pid;
    identity.boot_id = boot_id;
    if (const auto ticks = read_proc_start_ticks(proc_root, pid))
        identity.start_ticks = *ticks;
    return identity;
}
