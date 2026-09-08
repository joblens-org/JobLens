#pragma once

#include <optional>
#include <string>
#include <sys/types.h>
#include <vector>

namespace MountInfoUtils {

struct MountEntry {
    std::string mount_point;
    std::string fs_type;
};

using MountTable = std::vector<MountEntry>;

MountTable parse(const std::string& content);
std::optional<MountTable> read_for_pid(pid_t pid);
std::optional<MountEntry> resolve(const MountTable& table, const std::string& absolute_path);

}
