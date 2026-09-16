#include "collector/job_fd_index.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>

static void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

int main() {
    JobFdIndex index;
    require(index.entries(7).empty(), "空缓存应无条目");
    const std::vector<job_pid_fd_key> keys{{7, 10, 3}, {9, 10, 4}, {7, 11, 5}, {9, 12, 6}};
    index.rebuild(keys);
    require(index.entries(7) == std::vector<size_t>{0, 2}, "同作业非连续条目应保持原序");
    require(index.entries(9) == std::vector<size_t>{1, 3}, "相同 PID 不应混淆作业归属");
    require(index.entries(99).empty(), "不存在的作业应返回空切片");
    index.rebuild({{9, 20, 7}});
    require(index.entries(7).empty(), "刷新应移除旧作业索引");
    require(index.entries(9) == std::vector<size_t>{0}, "刷新应重建位置索引");
    index.clear();
    require(index.entries(9).empty(), "清理失效应移除索引");
    index.rebuild({});
    require(index.entries(9).empty(), "空刷新不应保留旧索引");

    std::vector<job_pid_fd_key> large;
    for (uint64_t job = 1; job <= 146; ++job) {
        for (uint32_t fd = 0; fd < 1024; ++fd) large.push_back({job, 100, fd});
    }
    index.rebuild(large);
    size_t visited = 0;
    for (uint64_t job = 1; job <= 146; ++job) {
        require(index.entries(job).size() == 1024, "每个作业应只访问自身条目");
        for (const auto i : index.entries(job)) {
            require(large[i].job_id == job, "索引不能串作业");
            ++visited;
        }
    }
    require(visited == large.size(), "一轮所有作业的条目访问总数应等于快照大小");
    std::cout << "PASS job isolation, refresh, invalidation, empty snapshot\n"
              << "jobs=146 entries=" << large.size() << " indexed_visits=" << visited
              << " full_scan_visits=" << large.size() * 146 << '\n';
}
