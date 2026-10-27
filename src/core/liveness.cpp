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
#include "core/liveness.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <unistd.h>

namespace {

// Reads field 22 (start time in clock ticks) of /proc/<pid>/stat, keeping errno
// so the caller can tell "gone" from "not allowed to look".
enum class StatRead {
    Ok = 0,
    Gone,
    Denied,
    Error,
};

StatRead read_start_ticks_raw(const std::string& proc_root, pid_t pid, std::uint64_t& ticks)
{
    const std::string path = proc_root + "/" + std::to_string(pid) + "/stat";
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT || errno == ESRCH) return StatRead::Gone;
        if (errno == EACCES || errno == EPERM) return StatRead::Denied;
        return StatRead::Error;
    }
    std::string content;
    char buffer[4096];
    while (true) {
        const ssize_t read_bytes = ::read(fd, buffer, sizeof(buffer));
        if (read_bytes < 0) {
            const int saved = errno;
            ::close(fd);
            if (saved == ENOENT || saved == ESRCH) return StatRead::Gone;
            if (saved == EACCES || saved == EPERM) return StatRead::Denied;
            return StatRead::Error;
        }
        if (read_bytes == 0) break;
        content.append(buffer, static_cast<std::size_t>(read_bytes));
    }
    ::close(fd);

    const auto last_rparen = content.rfind(')');
    if (last_rparen == std::string::npos) return StatRead::Error;

    std::istringstream fields(content.substr(last_rparen + 2));
    std::string field;
    // Field 3 (state) is first here; field 22 is index 19.
    for (int index = 3; index <= 22; ++index) {
        if (!(fields >> field)) return StatRead::Error;
        if (index == 22) {
            try {
                ticks = std::stoull(field);
            } catch (const std::exception&) {
                return StatRead::Error;
            }
            return StatRead::Ok;
        }
    }
    return StatRead::Error;
}

}  // namespace

const char* to_string(LivenessState state)
{
    switch (state) {
        case LivenessState::Alive: return "alive";
        case LivenessState::Dead: return "dead";
        case LivenessState::Unknown: break;
    }
    return "unknown";
}

ProcessLivenessProbe make_proc_liveness_probe(const std::string& proc_root)
{
    return [proc_root](const ProcessIdentity& identity) -> LivenessResult {
        if (identity.pid <= 0) {
            return LivenessResult{LivenessState::Dead, "no pid"};
        }
        std::uint64_t ticks = 0;
        switch (read_start_ticks_raw(proc_root, identity.pid, ticks)) {
            case StatRead::Gone:
                return LivenessResult{LivenessState::Dead, "process exited"};
            case StatRead::Denied:
                return LivenessResult{LivenessState::Unknown,
                                      "EPERM reading " + proc_root + "/" +
                                          std::to_string(identity.pid) + "/stat"};
            case StatRead::Error:
                return LivenessResult{LivenessState::Unknown,
                                      "unreadable " + proc_root + "/" +
                                          std::to_string(identity.pid) + "/stat"};
            case StatRead::Ok:
                break;
        }
        if (identity.start_ticks != 0 && ticks != identity.start_ticks) {
            // The pid was reused: this is a different process, so the observed
            // execution is gone even though the number is alive.
            return LivenessResult{LivenessState::Dead, "pid reused (start ticks differ)"};
        }
        return LivenessResult{LivenessState::Alive, "pid live"};
    };
}

CgroupMembershipProbe make_cgroup_membership_probe()
{
    return [](const std::string& cgroup_path) -> CgroupMembership {
        CgroupMembership result;
        if (cgroup_path.empty()) {
            result.error = "no cgroup path";
            return result;
        }
        const std::string procs = cgroup_path + "/cgroup.procs";
        const int fd = ::open(procs.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            result.error = std::string("cannot read ") + procs + ": " + std::strerror(errno);
            return result;
        }
        std::string content;
        char buffer[4096];
        while (true) {
            const ssize_t read_bytes = ::read(fd, buffer, sizeof(buffer));
            if (read_bytes < 0) {
                result.error = std::string("read error on ") + procs + ": " +
                               std::strerror(errno);
                ::close(fd);
                return result;
            }
            if (read_bytes == 0) break;
            content.append(buffer, static_cast<std::size_t>(read_bytes));
        }
        ::close(fd);
        std::istringstream lines(content);
        std::string line;
        while (std::getline(lines, line)) {
            if (line.empty()) continue;
            try {
                result.members.push_back(static_cast<pid_t>(std::stol(line)));
            } catch (const std::exception&) {
                result.error = "unparsable cgroup member '" + line + "'";
                result.members.clear();
                return result;
            }
        }
        std::sort(result.members.begin(), result.members.end());
        result.readable = true;
        return result;
    };
}

