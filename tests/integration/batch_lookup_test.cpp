#include "common/ebpf_common.hpp"
#include "ebpf/job_io_new.h"
#include "ebpf/fs_metadata.h"
#include <cstring>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>

template<typename Key, typename Value, typename MakeKey, typename MakeValue>
void check_map(bpf_object* obj, const char* name, unsigned total,
               MakeKey make_key, MakeValue make_value) {
    const int fd = bpf_object__find_map_fd_by_name(obj, name);
    if (fd < 0) throw std::runtime_error("测试 map 不存在");
    for (unsigned i = 0; i < total; ++i) {
        const Key key = make_key(i);
        const Value value = make_value(i);
        if (bpf_map_update_elem(fd, &key, &value, BPF_ANY))
            throw std::runtime_error("写入测试 map 失败");
    }
    std::vector<Key> keys;
    std::vector<Value> values;
    const auto count = EbpfCommon::lookup_hashmap_batch<Key, Value>(obj, name, keys, values);
    std::set<unsigned> seen;
    bool valid = count == total && keys.size() == total && values.size() == total;
    for (size_t i = 0; i < keys.size(); ++i) {
        const unsigned id = keys[i].pid;
        const Key expected_key = make_key(id);
        const Value expected_value = make_value(id);
        const bool unique = seen.insert(id).second;
        valid = id < total && unique &&
                std::memcmp(&keys[i], &expected_key, sizeof(Key)) == 0 &&
                std::memcmp(&values[i], &expected_value, sizeof(Value)) == 0 && valid;
    }
    std::cout << name << " expected=" << total << " actual=" << count
              << " unique=" << seen.size() << " valid=" << valid << '\n';
    if (!valid) throw std::runtime_error("批量读取丢失、重复或损坏条目");
}

int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "用法: batch_lookup_test batch_maps.bpf.o\n"; return 2; }
    spdlog::set_level(spdlog::level::warn);
    int failures = 0;
    for (unsigned total : {0U, 1U, 1023U, 1024U, 1025U, 2051U}) {
        for (bool io : {true, false}) {
            try {
                std::unique_ptr<bpf_object, decltype(&bpf_object__close)> obj(
                    bpf_object__open_file(argv[1], nullptr), bpf_object__close);
                if (!obj || libbpf_get_error(obj.get()) || bpf_object__load(obj.get()))
                    throw std::runtime_error("加载测试 map 失败，需要 BPF 权限");
                if (io) {
                    check_map<job_pid_fd_key, rw_stat>(obj.get(), "io_detail", total,
                        [](unsigned i) { return job_pid_fd_key{100 + i % 2, i, 3}; },
                        [](unsigned i) { rw_stat v{}; v.read_bytes = 4096 + i; v.read_count = i + 1; return v; });
                } else {
                    check_map<fs_meta_key, fs_meta_stat>(obj.get(), "fs_detail", total,
                        [](unsigned i) { return fs_meta_key{100 + i % 2, i, FS_META_GETATTR}; },
                        [](unsigned i) { fs_meta_stat v{}; v.calls = i + 1; v.success = i; v.errors = 1; return v; });
                }
            } catch (const std::exception& e) {
                std::cerr << "FAIL " << (io ? "io" : "fs") << " entries=" << total << ": " << e.what() << '\n';
                ++failures;
            }
        }
    }
    return failures == 0 ? 0 : 1;
}
