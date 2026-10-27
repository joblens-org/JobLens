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
// SchedulerAdapterRegistry implementation (see scheduler_adapter.hpp).
#include "core/scheduler_adapter.hpp"

#include <spdlog/spdlog.h>

SchedulerAdapterRegistry& SchedulerAdapterRegistry::instance()
{
    static SchedulerAdapterRegistry registry;
    return registry;
}

void SchedulerAdapterRegistry::registerAdapter(const std::string& name, Factory factory)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto [it, inserted] = factories_.emplace(name, std::move(factory));
    if (!inserted) it->second = std::move(factory);
}

bool SchedulerAdapterRegistry::known(const std::string& name) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return factories_.count(name) != 0;
}

std::vector<std::string> SchedulerAdapterRegistry::names() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> out;
    out.reserve(factories_.size());
    for (const auto& [name, factory] : factories_) out.push_back(name);
    return out;
}

std::unique_ptr<ISchedulerAdapter> SchedulerAdapterRegistry::build(
    const std::string& name, const InventoryTypeSettings& settings) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = factories_.find(name);
    if (it == factories_.end()) return nullptr;
    return it->second(settings);
}
