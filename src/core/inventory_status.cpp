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
// Inventory status/refresh provider seam storage (see
// core/inventory_status_provider.hpp). The registry heartbeat publishes here.
#include "core/inventory_status_provider.hpp"

#include <mutex>

namespace {

std::mutex& provider_mutex()
{
    static std::mutex mutex;
    return mutex;
}

std::function<nlohmann::json()>& provider_slot()
{
    static std::function<nlohmann::json()> provider;
    return provider;
}

std::function<bool()>& refresh_slot()
{
    static std::function<bool()> provider;
    return provider;
}

}  // namespace

void set_inventory_status_provider(std::function<nlohmann::json()> provider)
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    provider_slot() = std::move(provider);
}

std::function<nlohmann::json()> get_inventory_status_provider()
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    return provider_slot();
}

void set_inventory_refresh_provider(std::function<bool()> provider)
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    refresh_slot() = std::move(provider);
}

std::function<bool()> get_inventory_refresh_provider()
{
    std::lock_guard<std::mutex> lock(provider_mutex());
    return refresh_slot();
}
