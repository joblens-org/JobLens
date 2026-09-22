#pragma once

#include "ebpf/job_io_new.h"
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

// 索引只在对应 FD 快照存活期间有效；刷新和失效必须同步更新。
class JobFdIndex {
public:
    void rebuild(const std::vector<job_pid_fd_key>& keys) {
        entries_.clear();
        for (size_t i = 0; i < keys.size(); ++i) {
            entries_[keys[i].job_id].push_back(i);
        }
    }

    const std::vector<size_t>& entries(uint64_t job_id) const {
        const auto it = entries_.find(job_id);
        static const std::vector<size_t> empty;
        return it == entries_.end() ? empty : it->second;
    }

    void clear() { entries_.clear(); }

private:
    std::unordered_map<uint64_t, std::vector<size_t>> entries_;
};
