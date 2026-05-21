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
// ibpf_collector.h
// #pragma once
// #include "icollector.h"
// #include "common/bpf_loader.hpp"          // 上一版的 BpfLoader
// #include <vector>
// #include <bpf/libbpf.h>

// // 统一抽象一个“attach 点”
// struct AttachLink {
//     std::string name;        // prog 名，方便调试
//     bpf_link*   link = nullptr; // libbpf 返回的 attach 句柄
// };

// class IBpfCollector : public ICollector {
// public:
//     ~IBpfCollector() override = default;

//     /*--------  生命周期  --------*/
//     // 派生类只需实现 load_and_attach() 与 fill_result()
//     bool init(const nlohmann::json& config) final;

//     void deinit() noexcept final;

//     /*--------  BPF 专有接口  --------*/
//     // 由派生类实现：把 prog attach 到内核
//     virtual bool load_and_attach() = 0;

//     // 由派生类实现：把采集结果填进 CollectResult
//     virtual CollectResult fill_result(const Job& job) = 0;
    
//     CollectResult collect(const Job& job);

// protected:
//     virtual bool do_init(const nlohmann::json&) { return true; }
//     virtual void do_deinit() noexcept {}
//     // 留给派生类直接用的句柄
//     bpf_object* bpf_obj() const { return bpf_loader_ ? bpf_loader_->obj() : nullptr; }

//     // 工具：attach 一个 prog 并记录到 links_，失败返回 false
//     bool attach_prog(const char* prog_name, enum bpf_prog_type type,
//                      const void* target, int target_fd = -1);
//     std::unique_ptr<BpfLoader> bpf_loader_;

// private:
    
//     std::vector<AttachLink>    links_;      // 保存所有 attach 点
//     bool                       attached_ = false;
// };

