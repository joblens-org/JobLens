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
// job_lifecycle_event.h
#pragma once
#include <functional>
#include "core/collector_type.h"

struct Job;

enum class JobEvent {
    Added,
    Removed,
    Updated   // 预留，方便以后做属性热更新
};

// 回调签名：事件类型 + Job 常量引用
using JobLifecycleCb = std::function<void(JobEvent, const Job&)>;