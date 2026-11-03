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
#include "core/job_discovery.hpp"

#include <atomic>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>

namespace fs = std::filesystem;

namespace {
std::atomic<std::uint64_t> g_read_order{0};


// Whole-file read. Returns false with a message instead of throwing so a scan
// pass reports missing/unreadable entries instead of aborting.
bool read_file(const fs::path& path, std::string& out, std::string& error)
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

fs::path pid_path(const std::string& proc_root, pid_t pid, const char* leaf)
{
    return fs::path(proc_root) / std::to_string(pid) / leaf;
}

// Fields of /proc/<pid>/stat after the ")" of the command name: [0] is field 3
// (state), so field N lives at index N - 3.
std::optional<std::vector<std::string>> read_stat_fields(const std::string& proc_root, pid_t pid)
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

std::string join_reasons(const std::vector<std::string>& reasons)
{
    std::string joined;
    for (const auto& reason : reasons) {
        if (!joined.empty()) joined += "; ";
        joined += reason;
    }
    return joined;
}

void merge_members(std::vector<ProcessIdentity>& into, const std::vector<ProcessIdentity>& extra)
{
    for (const auto& member : extra) {
        const auto found = std::find_if(into.begin(), into.end(), [&member](const ProcessIdentity& existing) {
            return existing.pid == member.pid;
        });
        if (found == into.end()) into.push_back(member);
    }
}

// Disambiguation key of one local execution: two observations merge only when
// they agree on the manager identity (boot id, pid, start ticks).
std::string generation_key(const ExecutionGeneration& generation)
{
    return generation.boot_id + '|' + std::to_string(generation.manager_pid) + '|' +
           std::to_string(generation.manager_start_ticks);
}

} // namespace

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

std::int64_t now_epoch_ms()
{
    const auto since_epoch =
        std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(since_epoch).count();
}

// ---------------------------------------------------------------------------
// Source state
// ---------------------------------------------------------------------------

const char* to_string(SourceState state)
{
    switch (state) {
        case SourceState::Healthy: return "healthy";
        case SourceState::Degraded: return "degraded";
        case SourceState::Initializing: break;
    }
    return "initializing";
}

// ---------------------------------------------------------------------------
// Observation accumulation
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// procfs / cgroup readers
// ---------------------------------------------------------------------------

std::vector<pid_t> list_proc_pids(const std::string& proc_root)
{
    std::vector<pid_t> pids;
    std::error_code ec;
    fs::directory_iterator iterator(proc_root, ec);
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

bool ensure_readable_directory(const std::string& path, std::string& error)
{
    std::error_code ec;
    if (!fs::is_directory(path, ec) || ec) {
        error = "'" + path + "' is not a readable directory";
        if (ec) error += ": " + ec.message();
        return false;
    }
    return true;
}

bool read_proc_cmdline(const std::string& proc_root, pid_t pid, std::string& out,
                       std::string& error)
{
    std::string raw;
    if (!read_file(pid_path(proc_root, pid, "cmdline"), raw, error)) return false;
    for (char& c : raw) {
        if (c == '\0') c = ' ';
    }
    while (!raw.empty() && raw.back() == ' ') raw.pop_back();
    out = std::move(raw);
    return true;
}

bool read_proc_env(const std::string& proc_root, pid_t pid, const std::string& key,
                   std::string& out, std::string& error)
{
    std::string raw;
    if (!read_file(pid_path(proc_root, pid, "environ"), raw, error)) return false;

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

std::optional<pid_t> read_proc_ppid(const std::string& proc_root, pid_t pid)
{
    const auto fields = read_stat_fields(proc_root, pid);
    if (!fields || fields->size() <= 1) return std::nullopt;
    try {
        return static_cast<pid_t>(std::stol(fields->at(1)));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::optional<std::uint64_t> read_proc_start_ticks(const std::string& proc_root, pid_t pid)
{
    const auto fields = read_stat_fields(proc_root, pid);
    if (!fields || fields->size() <= 19) return std::nullopt;
    try {
        return std::stoull(fields->at(19));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::string read_boot_id(const std::string& proc_root)
{
    std::string content;
    std::string error;
    if (!read_file(fs::path(proc_root) / "sys" / "kernel" / "random" / "boot_id", content, error))
        return {};
    while (!content.empty() && (content.back() == '\n' || content.back() == '\r')) content.pop_back();
    return content;
}

std::string read_proc_cgroup_path(const std::string& proc_root, pid_t pid,
                                  const std::string& cgroup_root)
{
    std::string content;
    std::string error;
    if (!read_file(pid_path(proc_root, pid, "cgroup"), content, error)) return {};

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
        fs::path path(relative);
        if (path.is_absolute()) path = path.relative_path();
        return (fs::path(cgroup_root) / path).lexically_normal().string();
    }
    return {};
}

std::vector<pid_t> read_cgroup_members(const std::string& cgroup_dir)
{
    std::vector<pid_t> members;
    if (cgroup_dir.empty()) return members;

    std::string content;
    std::string error;
    if (!read_file(fs::path(cgroup_dir) / "cgroup.procs", content, error)) return members;

    std::istringstream lines(content);
    pid_t pid = 0;
    while (lines >> pid) members.push_back(pid);
    return members;
}

bool read_cgroup_members(const std::string& cgroup_dir, std::vector<pid_t>& members,
                         std::string& error)
{
    members.clear();
    error.clear();
    if (cgroup_dir.empty()) {
        error = "no cgroup path";
        return false;
    }

    std::string content;
    if (!read_file(fs::path(cgroup_dir) / "cgroup.procs", content, error)) {
        error = "cannot read '" + cgroup_dir + "/cgroup.procs': " + error;
        return false;
    }

    std::istringstream lines(content);
    pid_t pid = 0;
    while (lines >> pid) members.push_back(pid);
    return true;
}

ProcessIdentity read_process_identity(const std::string& proc_root,
                                      const std::string& boot_id, pid_t pid)
{
    ProcessIdentity identity;
    if (pid <= 0) return identity;
    identity.pid = pid;
    identity.boot_id = boot_id;
    if (const auto ticks = read_proc_start_ticks(proc_root, pid)) identity.start_ticks = *ticks;
    return identity;
}

// ---------------------------------------------------------------------------
// Discovered Job finalization
// ---------------------------------------------------------------------------

bool discovery_job_ready(const Job& job)
{
    if (!job.execution || !job.execution->generation.complete()) return false;
    return canonicalize(selector_of(job)).has_value();
}

void finalize_discovery_job(Job& job, std::vector<std::string> source_reasons)
{
    std::vector<std::string> reasons = std::move(source_reasons);

    if (!canonicalize(selector_of(job))) {
        reasons.push_back(
            "logical identity unresolved: scheduler type, cluster namespace or native job id missing");
    }

    if (!job.execution || !job.execution->generation.complete()) {
        reasons.push_back(
            "execution generation unresolved: manager pid, manager start ticks or boot id missing");
    }

    ensure_execution(job).incomplete_reason = join_reasons(reasons);
}

std::uint64_t next_discovery_read_order()
{
    return g_read_order.fetch_add(1, std::memory_order_relaxed) + 1;
}
