#pragma once

// Optional syscall-boundary fixture for environments without BPF capability.
// libbpf still opens the real ELF/map definitions; only map loading and kernel
// map operations are replaced. The default integration path uses the kernel.
#include <algorithm>
#include <cstring>
#include <map>

namespace BpfMapFixture {
inline bool enabled = false;
inline void (*after_delete)(int, const void*) = nullptr;
using Bytes = std::vector<unsigned char>;
using Entries = std::map<Bytes, Bytes>;
inline std::unordered_map<int, Entries> maps;

inline int fd(const char* name) {
    if (std::strcmp(name, "job_fd_stat") == 0) return 1;
    if (std::strcmp(name, "job_stat") == 0) return 2;
    if (std::strcmp(name, "latency_hist") == 0) return 3;
    return -1;
}
inline size_t key_size(int fd) {
    return fd == 1 ? sizeof(job_pid_fd_key) : fd == 2 ? sizeof(uint64_t) : sizeof(latency_key);
}
inline size_t value_size(int fd) { return fd == 3 ? sizeof(uint64_t) : sizeof(rw_stat); }
inline Bytes bytes(const void* value, size_t size) {
    const auto* begin = static_cast<const unsigned char*>(value);
    return Bytes(begin, begin + size);
}
inline int missing() { errno = ENOENT; return -ENOENT; }
inline int batch(int fd, void* in, void* out, void* keys, void* values, __u32* count) {
    auto& entries = maps[fd];
    const uint32_t offset = in ? *static_cast<uint32_t*>(in) : 0;
    auto entry = entries.begin();
    std::advance(entry, std::min<size_t>(offset, entries.size()));
    const auto limit = *count;
    *count = 0;
    while (entry != entries.end() && *count < limit) {
        std::memcpy(static_cast<unsigned char*>(keys) + *count * key_size(fd), entry->first.data(), key_size(fd));
        std::memcpy(static_cast<unsigned char*>(values) + *count * value_size(fd), entry->second.data(), value_size(fd));
        ++entry;
        ++*count;
    }
    *static_cast<uint32_t*>(out) = offset + *count;
    return entry == entries.end() ? missing() : 0;
}
}

extern "C" int __real_bpf_object__load(bpf_object*);
extern "C" int __wrap_bpf_object__load(bpf_object* object) {
    if (!BpfMapFixture::enabled) return __real_bpf_object__load(object);
    BpfMapFixture::maps.clear();
    return 0;
}
extern "C" int __real_bpf_object__find_map_fd_by_name(const bpf_object*, const char*);
extern "C" int __wrap_bpf_object__find_map_fd_by_name(const bpf_object* object, const char* name) {
    return BpfMapFixture::enabled ? BpfMapFixture::fd(name) : __real_bpf_object__find_map_fd_by_name(object, name);
}
extern "C" int __real_bpf_map__fd(const bpf_map*);
extern "C" int __wrap_bpf_map__fd(const bpf_map* map) {
    return BpfMapFixture::enabled ? BpfMapFixture::fd(bpf_map__name(map)) : __real_bpf_map__fd(map);
}
extern "C" int __real_bpf_map_update_elem(int, const void*, const void*, __u64);
extern "C" int __wrap_bpf_map_update_elem(int fd, const void* key, const void* value, __u64 flags) {
    if (!BpfMapFixture::enabled) return __real_bpf_map_update_elem(fd, key, value, flags);
    BpfMapFixture::maps[fd][BpfMapFixture::bytes(key, BpfMapFixture::key_size(fd))] =
        BpfMapFixture::bytes(value, BpfMapFixture::value_size(fd));
    return 0;
}
extern "C" int __real_bpf_map_lookup_elem(int, const void*, void*);
extern "C" int __wrap_bpf_map_lookup_elem(int fd, const void* key, void* value) {
    if (!BpfMapFixture::enabled) return __real_bpf_map_lookup_elem(fd, key, value);
    const auto& entries = BpfMapFixture::maps[fd];
    const auto found = entries.find(BpfMapFixture::bytes(key, BpfMapFixture::key_size(fd)));
    if (found == entries.end()) return BpfMapFixture::missing();
    std::memcpy(value, found->second.data(), found->second.size());
    return 0;
}
extern "C" int __real_bpf_map_delete_elem(int, const void*);
extern "C" int __wrap_bpf_map_delete_elem(int fd, const void* key) {
    const int result = BpfMapFixture::enabled
        ? (BpfMapFixture::maps[fd].erase(BpfMapFixture::bytes(key, BpfMapFixture::key_size(fd)))
            ? 0 : BpfMapFixture::missing())
        : __real_bpf_map_delete_elem(fd, key);
    if (!result && BpfMapFixture::after_delete) BpfMapFixture::after_delete(fd, key);
    return result;
}
extern "C" int __real_bpf_map__lookup_elem(const bpf_map*, const void*, size_t, void*, size_t, __u64);
extern "C" int __wrap_bpf_map__lookup_elem(const bpf_map* map, const void* key, size_t key_size,
                                           void* value, size_t value_size, __u64 flags) {
    if (!BpfMapFixture::enabled)
        return __real_bpf_map__lookup_elem(map, key, key_size, value, value_size, flags);
    return __wrap_bpf_map_lookup_elem(BpfMapFixture::fd(bpf_map__name(map)), key, value);
}
extern "C" int __real_bpf_map__lookup_and_delete_elem(const bpf_map*, const void*, size_t, void*, size_t, __u64);
extern "C" int __wrap_bpf_map__lookup_and_delete_elem(const bpf_map* map, const void* key, size_t key_size,
                                                      void* value, size_t value_size, __u64 flags) {
    if (!BpfMapFixture::enabled)
        return __real_bpf_map__lookup_and_delete_elem(map, key, key_size, value, value_size, flags);
    const int fd = BpfMapFixture::fd(bpf_map__name(map));
    const int result = __wrap_bpf_map_lookup_elem(fd, key, value);
    return result ? result : __wrap_bpf_map_delete_elem(fd, key);
}
