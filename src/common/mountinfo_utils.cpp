#include "common/mountinfo_utils.hpp"

#include <cstddef>
#include <fstream>
#include <sstream>
#include <string_view>

namespace {

bool is_octal(char ch)
{
    return ch >= '0' && ch <= '7';
}

std::string decode_mountinfo_field(std::string_view field)
{
    std::string decoded;
    decoded.reserve(field.size());

    for (std::size_t i = 0; i < field.size();) {
        if (field[i] == '\\' && i + 3 < field.size() &&
            is_octal(field[i + 1]) && is_octal(field[i + 2]) && is_octal(field[i + 3])) {
            int value = (field[i + 1] - '0') * 64 + (field[i + 2] - '0') * 8 + (field[i + 3] - '0');
            decoded.push_back(static_cast<char>(value));
            i += 4;
            continue;
        }

        decoded.push_back(field[i]);
        ++i;
    }

    return decoded;
}

bool mount_point_matches(const std::string& mount_point, const std::string& absolute_path)
{
    if (mount_point == "/") {
        return !absolute_path.empty() && absolute_path[0] == '/';
    }

    if (absolute_path.size() < mount_point.size()) {
        return false;
    }

    if (absolute_path.compare(0, mount_point.size(), mount_point) != 0) {
        return false;
    }

    return absolute_path.size() == mount_point.size() || absolute_path[mount_point.size()] == '/';
}

}

namespace MountInfoUtils {

MountTable parse(const std::string& content)
{
    MountTable table;
    std::istringstream lines(content);
    std::string line;

    while (std::getline(lines, line)) {
        std::istringstream fields(line);
        std::vector<std::string> tokens;
        std::string token;
        while (fields >> token) {
            tokens.push_back(token);
        }

        if (tokens.size() < 10) {
            continue;
        }

        std::size_t separator = 6;
        while (separator < tokens.size() && tokens[separator] != "-") {
            ++separator;
        }

        if (separator == tokens.size() || separator + 3 >= tokens.size()) {
            continue;
        }

        table.push_back(MountEntry{
            decode_mountinfo_field(tokens[4]),
            decode_mountinfo_field(tokens[separator + 1]),
        });
    }

    return table;
}

std::optional<MountTable> read_for_pid(pid_t pid)
{
    std::ifstream input("/proc/" + std::to_string(pid) + "/mountinfo");
    if (!input) {
        return std::nullopt;
    }

    std::ostringstream content;
    content << input.rdbuf();
    if (input.bad()) {
        return std::nullopt;
    }

    return parse(content.str());
}

std::optional<MountEntry> resolve(const MountTable& table, const std::string& absolute_path)
{
    if (absolute_path.empty() || absolute_path[0] != '/') {
        return std::nullopt;
    }

    const MountEntry* best = nullptr;
    for (const auto& entry : table) {
        if (!mount_point_matches(entry.mount_point, absolute_path)) {
            continue;
        }
        if (best == nullptr || entry.mount_point.size() > best->mount_point.size()) {
            best = &entry;
        }
    }

    if (best == nullptr) {
        return std::nullopt;
    }

    return *best;
}

}
