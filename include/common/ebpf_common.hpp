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
#pragma once

// 由 CMake target_compile_definitions 在构建时注入实际路径
// clangd / 非 CMake 环境使用默认值 "lib64"
#ifndef JOBLENS_INSTALL_LIBDIR
#define JOBLENS_INSTALL_LIBDIR "lib64"
#endif

#include <bpf/bpf.h>
#include <linux/bpf.h>
#include <string>
#include <bpf/libbpf.h>
#include <linux/bpf.h>
#include <spdlog/spdlog.h>
#include <optional>
#include <sys/stat.h>
#include <cerrno>
#include <cstdint>
#include <filesystem>


namespace EbpfCommon{
    namespace fs = std::filesystem;

    inline std::string normalize_cgroup_fs_path(const std::string& cgroup_path){
        const fs::path mount = "/sys/fs/cgroup";
        fs::path path = fs::path(cgroup_path).lexically_normal();

        const std::string mount_prefix = mount.string();
        const std::string path_string = path.string();
        if (path_string == mount_prefix || path_string.rfind(mount_prefix + "/", 0) == 0) {
            return path_string;
        }

        if (path.is_absolute()) {
            path = path.relative_path();
        }
        return (mount / path).lexically_normal().string();
    }

    // 确保 bpffs 已挂载且 pin 根目录存在。pin_root 形如 "/sys/fs/bpf/joblens"。
    // 常规系统 /sys/fs/bpf 已由 systemd 挂载, 这里仅兜底创建子目录。
    inline bool ensure_bpf_pin_dir(const std::string& pin_root) {
        const std::string bpffs = "/sys/fs/bpf";
        struct stat st{};
        if (::stat(bpffs.c_str(), &st) != 0) {
            spdlog::error("ensure_bpf_pin_dir: bpffs {} not available, errno={}", bpffs, errno);
            return false;
        }
        std::error_code ec;
        fs::create_directories(pin_root, ec);
        if (ec) {
            spdlog::error("ensure_bpf_pin_dir: create {} failed: {}", pin_root, ec.message());
            return false;
        }
        return true;
    }

    // 内部实现: open(可选 pin_root) -> load -> attach。
    inline bpf_object* load_bpf_obj_impl(const std::string& bpf_o_path,
                                         std::vector<struct bpf_link *>& links,
                                         const std::string& pin_root) {
        bpf_object* obj_ = nullptr;
        // 打开ELF。传入 pin_root_path 后, 标记了 LIBBPF_PIN_BY_NAME 的 map 会在
        // load 时按 <pin_root>/<map_name> 做 create-or-reuse, 实现跨 .bpf.o 共享。
        if (!pin_root.empty()) {
            if (!ensure_bpf_pin_dir(pin_root)) return nullptr;
            LIBBPF_OPTS(bpf_object_open_opts, opts);
            opts.pin_root_path = pin_root.c_str();
            obj_ = bpf_object__open_file(bpf_o_path.c_str(), &opts);
            spdlog::debug("load_bpf_obj: opening {} with pin_root={} (shared maps create-or-reuse)",
                          bpf_o_path, pin_root);
        } else {
            obj_ = bpf_object__open_file(bpf_o_path.c_str(), nullptr);
        }
        if (libbpf_get_error(obj_)) {
            spdlog::error("bpf_object__open_file {}", bpf_o_path);
            return nullptr;
        }
        /* 4. 加载进内核 */
        int err = bpf_object__load(obj_);
        if (err) {
            spdlog::error("load_bpf_obj: bpf_object__load {}", err);
            bpf_object__close(obj_);
            return nullptr;
        }

        /* 5. 自动 attach 所有 SEC("tp/...") */
        struct bpf_program *prog;
        bpf_object__for_each_program(prog, obj_) {
            struct bpf_link *link = bpf_program__attach(prog);
            if (!link) {
                spdlog::error("load_bpf_obj: bpf_program__attach failed for {}, errno: {}({})", bpf_program__name(prog), -errno, strerror(errno));
                return nullptr;
            }
            links.emplace_back(link);
        }
        spdlog::debug("load_bpf_obj: link count: {}",links.size());
        return obj_;
    }

    inline bpf_object* load_bpf_obj(const std::string& bpf_o_path, std::vector<struct bpf_link *>& links) {
        return load_bpf_obj_impl(bpf_o_path, links, "");
    }

    // 带 pin 根目录的加载: 标记 LIBBPF_PIN_BY_NAME 的 map 在 <pin_root>/<name>
    // 处做 create-or-reuse。首个加载者创建并 pin, 后续加载者复用同一份 map fd。
    inline bpf_object* load_bpf_obj_pinned(const std::string& bpf_o_path,
                                           std::vector<struct bpf_link *>& links,
                                           const std::string& pin_root) {
        return load_bpf_obj_impl(bpf_o_path, links, pin_root);
    }

    inline ring_buffer* new_rb(const bpf_object* obj, std::string name, ring_buffer_sample_fn callback, void* ctx){
        int rb_fd = bpf_object__find_map_fd_by_name(obj, name.c_str());
        if (rb_fd < 0) {
            spdlog::error("new_rb: find map rb failed");
            return nullptr;
        }
        auto rb = ring_buffer__new(rb_fd, callback, ctx, nullptr);
        if (!rb) {
            spdlog::error("new_rb: ring_buffer__new");
            return nullptr;
        }
        return rb;
    }

    template <typename Event>
    std::vector<Event> rb_fetch_data(ring_buffer* rb){
        ring_buffer__consume(rb);
    }

    template <typename Key, typename Value>
    bool update_hashmap_elem(const bpf_object* obj,
                            const std::string& map_name,
                            const Key& key,
                            const Value& value,
                            std::uint64_t flags = BPF_ANY)
    {
        int fd = bpf_object__find_map_fd_by_name(obj, map_name.c_str());
        if (fd < 0) {
            spdlog::error("update_hashmap_elem: bpf_object__find_map_fd_by_name({}) failed", map_name);
            return false;
        }

        int err = bpf_map_update_elem(fd, &key, &value, flags);
        if (err) {
            spdlog::error("update_hashmap_elem: bpf_map_update_elem({}) failed, errno={}", map_name, errno);
            return false;
        }
        return true;
    }

    template <typename Key, typename Value>
    bool update_hashmap_batch(const bpf_object* obj,
                            const std::string& map_name,
                            const std::vector<Key>& keys,
                            const std::vector<Value>& values,
                            std::uint64_t flags = BPF_ANY)
    {
        if (keys.size() != values.size()) {
            spdlog::error("update_hashmap_batch: keys.size({}) != values.size({})",
                        keys.size(), values.size());
            return false;
        }
        if (keys.empty()) return true;   // 空批次直接返回成功

        int fd = bpf_object__find_map_fd_by_name(obj, map_name.c_str());
        if (fd < 0) {
            spdlog::error("update_hashmap_batch: bpf_object__find_map_fd_by_name({}) failed",
                        map_name);
            return false;
        }

        std::uint32_t count = static_cast<std::uint32_t>(keys.size());

        /* 构造 opts */
        bpf_map_batch_opts opts{};
        opts.sz = sizeof(opts);
        opts.flags = flags;

        int err = bpf_map_update_batch(fd,
                                    keys.data(),
                                    values.data(),
                                    &count,
                                    &opts);

        if (err) {
            spdlog::error("update_hashmap_batch: bpf_map_update_batch returned {} "
                        "(updated={}/{}), errno={}",
                        err, count, keys.size(), errno);
            return false;
        }
        spdlog::debug("update_hashmap_batch: update map {}",map_name);
        return true;
    }

    template <typename Key, typename Value>
    std::optional<Value> lookup_hashmap_elem(const bpf_object* obj,
                                            const std::string& map_name,
                                            const Key& key)
    {
        /* 1. 找到 map */
        struct bpf_map* map = bpf_object__find_map_by_name(obj, map_name.c_str());
        if (!map) {
            spdlog::error("lookup_hashmap_elem: map '{}' not found", map_name);
            return std::nullopt;
        }

        /* 2. 尺寸校验 */
        size_t def_ksz = bpf_map__key_size(map);
        size_t def_vsz = bpf_map__value_size(map);
        if (def_ksz != sizeof(Key)) {
            spdlog::error("lookup_hashmap_elem: key size mismatch "
                        "(map {} vs template {})", def_ksz, sizeof(Key));
            return std::nullopt;
        }
        if (def_vsz != sizeof(Value)) {
            spdlog::error("lookup_hashmap_elem: value size mismatch "
                        "(map {} vs template {})", def_vsz, sizeof(Value));
            return std::nullopt;
        }

        /* 3. 查询 */
        Key key_cp = key;
        Value val{};
        int err = bpf_map__lookup_elem(map,
                                    &key_cp,  sizeof(Key),
                                    &val, sizeof(Value),
                                    0 /* flags */);
        
        if (err) {
            /* -ENOENT 表示 key 不存在，其余为真正错误 */
            if (err != -ENOENT) {
                spdlog::error("lookup_hashmap_elem: bpf_map__lookup_elem failed: {}",
                            strerror(-err));
            }
            return std::nullopt;
        }
        return val;   // 成功
    }

    template <typename Key, typename Value>
    std::optional<Value> delete_hashmap_elem(const bpf_object* obj,
                                            const std::string& map_name,
                                            const Key& key)
    {
        static_assert(std::is_standard_layout_v<Key> && std::is_trivially_copyable_v<Key>,
                    "Key must be a standard-layout trivially copyable type");
        static_assert(std::is_standard_layout_v<Value> && std::is_trivially_copyable_v<Value>,
                    "Value must be a standard-layout trivially copyable type");

        if (!obj) {
            fprintf(stderr, "delete_hashmap_elem: bpf_object is null\n");
            return std::nullopt;
        }

        /* 1. 找到 map */
        struct bpf_map* map = bpf_object__find_map_by_name(obj, map_name.c_str());
        if (!map) {
            fprintf(stderr, "delete_hashmap_elem: map '%s' not found\n", map_name.c_str());
            return std::nullopt;
        }

        /* 2. 校验 key/value 尺寸 */
        size_t map_key_sz   = bpf_map__key_size(map);
        size_t map_value_sz = bpf_map__value_size(map);
        if (sizeof(Key) != map_key_sz) {
            fprintf(stderr, "delete_hashmap_elem: key size mismatch "
                            "(expected %zu, got %zu)\n", map_key_sz, sizeof(Key));
            return std::nullopt;
        }

        /* 3. 计算真正的 value_size：
        对于 per-CPU map，需要按 CPU 数对齐到 8 字节。*/
        size_t value_sz = sizeof(Value);
        if (bpf_map__type(map) == BPF_MAP_TYPE_PERCPU_HASH ||
            bpf_map__type(map) == BPF_MAP_TYPE_PERCPU_ARRAY) {
            int cpus = libbpf_num_possible_cpus();
            if (cpus < 0) {
                fprintf(stderr, "delete_hashmap_elem: fail to get CPU count\n");
                return std::nullopt;
            }
            /* 对齐到 8 字节 */
            size_t slot = (sizeof(Value) + 7) / 8 * 8;
            value_sz = slot * static_cast<size_t>(cpus);
        }

        /* 4. 准备缓冲区并调用内核 API */
        std::vector<uint8_t> value_buf(value_sz, 0);
        int err = bpf_map__lookup_and_delete_elem(map,
                                                static_cast<const void*>(&key),
                                                sizeof(Key),
                                                value_buf.data(),
                                                value_sz,
                                                0 /* flags */);
        if (err) {
            fprintf(stderr, "delete_hashmap_elem: bpf_map__lookup_and_delete_elem failed: %s\n",
                    strerror(-err));
            return std::nullopt;
        }

        /* 5. 解析返回值（仅拷贝第一个 slot 即可） */
        Value ret{};
        std::memcpy(&ret, value_buf.data(), sizeof(Value));
        return ret;
    }

    inline bool update_pid_in_kernel(const bpf_object* obj, const std::string& map_name, uint64_t jobid, std::vector<pid_t> pids){
        std::vector<uint64_t> values;
        values.assign(pids.size(), jobid);
        return update_hashmap_batch<pid_t, uint64_t>(obj, map_name, pids, values);
    }

    // stat().st_ino 即 cgroup v2 目录的 kernfs inode，与内核侧
    // bpf_get_current_cgroup_id() 返回同值，故可直接作为 cgroup2job 的 key。
    inline std::optional<uint64_t> cgroup_path_to_id(const std::string& cgroup_path){
        if (cgroup_path.empty()) return std::nullopt;
        const std::string fs_path = normalize_cgroup_fs_path(cgroup_path);
        struct stat st{};
        if (::stat(fs_path.c_str(), &st) != 0){
            spdlog::warn("cgroup_path_to_id: stat({}) failed, original={}, errno={}", fs_path, cgroup_path, errno);
            return std::nullopt;
        }
        return static_cast<uint64_t>(st.st_ino);
    }

    inline bool update_cgroup_in_kernel(const bpf_object* obj, const std::string& map_name, uint64_t jobid, const std::vector<uint64_t>& cgroup_ids){
        std::vector<uint64_t> values;
        values.assign(cgroup_ids.size(), jobid);
        return update_hashmap_batch<uint64_t, uint64_t>(obj, map_name, cgroup_ids, values);
    }

    // 批量遍历 HASH map：一次拉取 batch 条 key/value，避免逐条 get_next_key 的 syscall 开销。
    // 返回拉取到的条目数；返回后 keys/values 大小一致。
    template <typename Key, typename Value>
    size_t lookup_hashmap_batch(const bpf_object* obj,
                                const std::string& map_name,
                                std::vector<Key>& keys,
                                std::vector<Value>& values,
                                size_t batch_size = 1024)
    {
        struct bpf_map* map = bpf_object__find_map_by_name(obj, map_name.c_str());
        if (!map) {
            spdlog::error("lookup_hashmap_batch: map '{}' not found", map_name);
            return 0;
        }

        keys.clear();
        values.clear();

        std::vector<Key> batch_keys(batch_size);
        std::vector<Value> batch_vals(batch_size);

        bpf_map_batch_opts opts{};
        opts.sz = sizeof(opts);

        void *in_batch = nullptr;
        // 遍历期间 map 可能被并发修改(如清理死进程条目), 导致 in_batch 指向的
        // key 被删除, 内核访问该失效 key 时 bpf_map_lookup_batch 返回 EFAULT。
        // 此时已收集的 keys/values 作废, 从头重启遍历; 限次避免死循环。
        constexpr int kMaxRestarts = 3;
        int restarts = 0;
        while (true) {
            void *out_batch = nullptr;
            uint32_t count = static_cast<uint32_t>(batch_size);
            int err = bpf_map_lookup_batch(bpf_map__fd(map), in_batch, &out_batch,
                                           batch_keys.data(), batch_vals.data(),
                                           &count, &opts);
            if (err) {
                if (err == -ENOENT) break;  // 遍历完成
                if (err == -EFAULT && in_batch != nullptr && restarts < kMaxRestarts) {
                    // in_batch 指向的 key 在遍历期间被并发删除, 从头重启
                    spdlog::warn("lookup_hashmap_batch: {} EFAULT (key removed during traversal), restarting from head",
                                 map_name);
                    keys.clear();
                    values.clear();
                    in_batch = nullptr;
                    ++restarts;
                    continue;
                }
                spdlog::error("lookup_hashmap_batch: bpf_map_lookup_batch({}) failed err={} errno={}",
                              map_name, err, errno);
                break;
            }
            keys.insert(keys.end(), batch_keys.begin(), batch_keys.begin() + count);
            values.insert(values.end(), batch_vals.begin(), batch_vals.begin() + count);
            if (count < batch_size) break;  // 已到尾部
            in_batch = out_batch;
        }
        return keys.size();
    }


    // 从 bpffs pin path 直接取回一个已 pin 的 map fd(不经由 bpf_object)。
    // 供 JobRegistry / JobPidTracker 访问由 job_pid_track.bpf.o 创建的共享 map。
    // 返回 <0 表示失败。调用方负责 close。
    inline int get_pinned_map_fd(const std::string& pin_path) {
        int fd = bpf_obj_get(pin_path.c_str());
        if (fd < 0) {
            spdlog::error("get_pinned_map_fd: bpf_obj_get({}) failed, errno={}", pin_path, errno);
        }
        return fd;
    }

    // 按 pid 逐条写入某个已取得 fd 的 hash map(key=u32 pid, value=u64 job_id)。
    inline bool update_map_by_fd(int fd, uint32_t key, uint64_t value, uint64_t flags = BPF_ANY) {
        if (fd < 0) return false;
        if (bpf_map_update_elem(fd, &key, &value, flags) != 0) {
            spdlog::error("update_map_by_fd: bpf_map_update_elem(fd={}) failed, errno={}", fd, errno);
            return false;
        }
        return true;
    }

    inline bool update_map_by_fd_u64(int fd, uint64_t key, uint64_t value, uint64_t flags = BPF_ANY) {
        if (fd < 0) return false;
        if (bpf_map_update_elem(fd, &key, &value, flags) != 0) {
            spdlog::error("update_map_by_fd_u64: bpf_map_update_elem(fd={}) failed, errno={}", fd, errno);
            return false;
        }
        return true;
    }

    inline bool delete_map_by_fd(int fd, uint32_t key) {
        if (fd < 0) return false;
        int err = bpf_map_delete_elem(fd, &key);
        if (err != 0 && errno != ENOENT) {
            spdlog::error("delete_map_by_fd: bpf_map_delete_elem(fd={}) failed, errno={}", fd, errno);
            return false;
        }
        return true;
    }

    inline bool delete_map_by_fd_u64(int fd, uint64_t key) {
        if (fd < 0) return false;
        int err = bpf_map_delete_elem(fd, &key);
        if (err != 0 && errno != ENOENT) {
            spdlog::error("delete_map_by_fd_u64: bpf_map_delete_elem(fd={}) failed, errno={}", fd, errno);
            return false;
        }
        return true;
    }

    inline void free_rb(struct ring_buffer* rb){
        if(rb) ring_buffer__free(rb);
    }

    inline void unload_bpf_obj(bpf_object* obj, std::vector<struct bpf_link*>& links){
        for(auto& link: links){
            bpf_link__detach(link);
            bpf_link__destroy(link);
        }
        links.clear();
        if(obj) bpf_object__close(obj);
    }
} // namespace EbpfCommon
