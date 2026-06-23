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

// 由 CMake target_compile_definitions 在构建时注入实际路径（lib 或 lib64）
// clangd / 非 CMake 环境使用默认值 "lib"
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


namespace EbpfCommon{
    inline bpf_object* load_bpf_obj(const std::string& bpf_o_path, std::vector<struct bpf_link *>& links) {
        // 打开ELF
        bpf_object* obj_ = bpf_object__open_file(bpf_o_path.c_str(), nullptr);
        if (libbpf_get_error(obj_)) {
            spdlog::error("bpf_object__open_file {}", bpf_o_path);
            return nullptr;
        }
        /* 4. 加载进内核 */
        int err = bpf_object__load(obj_);
        if (err) {
            spdlog::error("load_bpf_obj: bpf_object__load {}", err);
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
