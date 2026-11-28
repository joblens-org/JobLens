#pragma once

#include <cstdint>
#include <string>

// Neutral read projection. Group facts themselves remain in adapter-owned
// sub_attr, including in the persisted and RPC representation.
struct JobGroupView {
    std::string acct_group;
    std::string unix_group;
    std::int64_t unix_gid{-1};

    bool empty() const {
        return acct_group.empty() && unix_group.empty() && unix_gid < 0;
    }
};

inline bool fill_missing_group_text(std::string& target, const std::string& source)
{
    if (!target.empty() || source.empty()) return false;
    target = source;
    return true;
}
