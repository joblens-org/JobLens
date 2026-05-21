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

#include <sol/sol.hpp>
#include <vector>
#include <variant>
#include <string>

// ========= 1. 类型特征检测 =========
template <typename T, typename = void>
struct has_reflection : std::false_type {};

template <typename T>
struct has_reflection<T, std::void_t<decltype(T::reflection())>> 
    : std::true_type {};

template <typename T>
constexpr bool has_reflection_v = has_reflection<T>::value;

// ========= 2. 前向声明（解决循环依赖） =========
template <typename T>
sol::object to_lua(sol::state_view lua, T&& value);

// ========= 3. 基本类型特化（必须最先） =========
// 处理所有非反射的基本类型（int, bool, string, 枚举等）
template <typename T>
inline std::enable_if_t<!has_reflection_v<std::decay_t<T>> && 
                        !std::is_same_v<std::decay_t<T>, std::string> &&
                        !std::is_same_v<std::decay_t<T>, bool> &&
                        !std::is_integral_v<std::decay_t<T>>, 
                        sol::object>
to_lua(sol::state_view lua, T&& value) {
    return sol::make_object(lua, std::forward<T>(value));
}

// 显式特化常见类型（避免歧义）
inline sol::object to_lua(sol::state_view lua, int value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, unsigned int value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, long value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, unsigned long value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, long long value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, unsigned long long value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, bool value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, double value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, const std::string& value) {
    return sol::make_object(lua, value);
}

inline sol::object to_lua(sol::state_view lua, std::string&& value) {
    return sol::make_object(lua, std::move(value));
}

// 处理枚举（如果有 to_string 函数）
template <typename T>
inline std::enable_if_t<std::is_enum_v<T>, sol::object>
to_lua(sol::state_view lua, T value) {
    return sol::make_object(lua, to_string(value));
}

// ========= 4. 容器特化 =========
template <typename T>
inline sol::object to_lua(sol::state_view lua, const std::vector<T>& vec) {
    sol::table tbl = lua.create_table();
    for (size_t i = 0; i < vec.size(); ++i) {
        tbl[i + 1] = to_lua(lua, vec[i]); // Lua 索引从1开始
    }
    return tbl;
}

// ========= 5. Variant 特化 =========
template <typename... Args>
inline sol::object to_lua(sol::state_view lua, const std::variant<Args...>& var) {
    return std::visit([&lua](const auto& value) -> sol::object {
        return to_lua(lua, value);
    }, var);
}

// ========= 6. 反射版本（最后定义，依赖前面所有特化） =========
template <typename T>
inline std::enable_if_t<has_reflection_v<T>, sol::object>
to_lua(sol::state_view lua, const T& obj) {
    sol::table tbl = lua.create_table();
    
    // 编译期展开所有字段
    constexpr auto fields = T::reflection();
    std::apply([&](const auto&... pairs) {
        // 折叠表达式：对每个 pair 执行赋值
        ((tbl[pairs.first] = to_lua(lua, obj.*(pairs.second))), ...);
    }, fields);
    
    return tbl;
}

// ========= 7. 辅助函数：直接转 table =========
template <typename T>
inline sol::table to_lua_table(sol::state_view lua, const T& obj) {
    return to_lua(lua, obj).template as<sol::table>();
}