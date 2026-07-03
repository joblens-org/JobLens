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

#include "job_watcher/job_trigger_event.hpp"
#include <functional>

// 所有自动发现触发源的抽象基类
class IJobTriggerSource {
public:
    using TriggerEventCallback = std::function<void(const TriggerEvent& event)>;

    virtual ~IJobTriggerSource() = default;

    // 启动 eBPF 程序、轮询线程、工作线程
    virtual bool start() = 0;

    // 停止所有线程，卸载 eBPF 程序
    virtual void stop() = 0;

    // 注册事件回调（线程安全，可在 start() 前后调用）
    virtual void register_callback(TriggerEventCallback cb) = 0;
};
