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
// Aggregate-reflected scheduler facts.
//
// A facts struct is a plain aggregate plus one constexpr name list:
//
//   struct FooAttr {
//       int job_id{};
//       std::string owner{};
//       static constexpr std::array field_names{"job_id", "owner"};
//       static constexpr std::array<const char*, 0> optional_fields{};
//       static constexpr bool is_optional(std::string_view) { return false; }
//   };
//   static_assert(joblens::facts::valid_schema<FooAttr>());
//
// Everything else (JSON, merge, equality, Lua) is generated from the members
// with Boost.PFR. There is no second field list to keep in sync: the struct
// itself is the schema, and `valid_schema` fails the build when a name is
// missing or an optional name does not exist.
#pragma once

#include <boost/pfr.hpp>
#include <nlohmann/json.hpp>

#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace joblens::facts {

// A facts type is an aggregate with a name list, an optional-name list and the
// optional predicate derived from them.
template <typename T, typename = void>
struct is_facts : std::false_type {};
template <typename T>
struct is_facts<T, std::void_t<decltype(T::field_names),
                              decltype(T::optional_fields),
                              decltype(T::is_optional(std::string_view{}))>>
    : std::true_type {};
template <typename T>
constexpr bool is_facts_v = is_facts<T>::value;

template <typename T>
constexpr bool names_contain(std::string_view wanted)
{
    for (const char* candidate : T::field_names)
        if (wanted == candidate) return true;
    return false;
}

// The single schema invariant: one name per reflected member, and every
// optional name is a real field. A missing name is a compile error instead of
// a silently unpersisted fact.
template <typename T>
constexpr bool valid_schema()
{
    if (T::field_names.size() != boost::pfr::tuple_size_v<T>) return false;
    for (const char* optional : T::optional_fields)
        if (!names_contain<T>(optional)) return false;
    return true;
}

// Merge one field with the registry's non-default overwrite rule. A default
// observation never erases a known same-typed field:
//   bool        - a true observation arms it, false changes nothing
//   string      - a non-empty observation replaces, empty changes nothing
//   arithmetic  - a non-zero observation replaces, zero changes nothing
template <typename T>
bool merge_field(T& target, const T& source)
{
    if constexpr (std::is_same_v<T, bool>) {
        if (!source || target) return false;
        target = true;
        return true;
    } else if constexpr (std::is_same_v<T, std::string>) {
        if (source.empty() || target == source) return false;
        target = source;
        return true;
    } else {
        if (source == T{} || target == source) return false;
        target = source;
        return true;
    }
}

template <typename T, typename F, std::size_t... I>
constexpr void each_impl(T& value, F&& f, std::index_sequence<I...>)
{
    (f(T::field_names[I], boost::pfr::get<I>(value), I), ...);
}
template <typename T, typename F, std::size_t... I>
constexpr void each_impl(const T& value, F&& f, std::index_sequence<I...>)
{
    (f(T::field_names[I], boost::pfr::get<I>(value), I), ...);
}
template <typename T, typename F>
constexpr void each(T& value, F&& f)
{
    each_impl(value, std::forward<F>(f),
              std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
}
template <typename T, typename F>
constexpr void each(const T& value, F&& f)
{
    each_impl(value, std::forward<F>(f),
              std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
}

template <typename T, typename F, std::size_t... I>
constexpr void each_pair_impl(T& a, const T& b, F&& f, std::index_sequence<I...>)
{
    (f(T::field_names[I], boost::pfr::get<I>(a), boost::pfr::get<I>(b)), ...);
}
template <typename T, typename F, std::size_t... I>
constexpr void each_pair_impl(const T& a, const T& b, F&& f,
                              std::index_sequence<I...>)
{
    (f(T::field_names[I], boost::pfr::get<I>(a), boost::pfr::get<I>(b)), ...);
}
template <typename T, typename F>
constexpr void each_pair(T& a, const T& b, F&& f)
{
    each_pair_impl(a, b, std::forward<F>(f),
                   std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
}
template <typename T, typename F>
constexpr void each_pair(const T& a, const T& b, F&& f)
{
    each_pair_impl(a, b, std::forward<F>(f),
                   std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
}

template <typename T, std::enable_if_t<is_facts_v<T>, int> = 0>
inline bool merge_from(T& target, const T& source)
{
    bool changed = false;
    each_pair(target, source, [&](const char*, auto& current, const auto& observed) {
        changed |= merge_field(current, observed);
    });
    return changed;
}

template <typename T, std::enable_if_t<is_facts_v<T>, int> = 0>
inline bool equal(const T& a, const T& b)
{
    bool same = true;
    each_pair(a, b, [&](const char*, const auto& left, const auto& right) {
        same = same && (left == right);
    });
    return same;
}

}  // namespace joblens::facts

// nlohmann/json ADL entry points. Constrained to facts types so no other type
// is captured by accident.
template <typename T, std::enable_if_t<joblens::facts::is_facts_v<T>, int> = 0>
inline void to_json(nlohmann::json& j, const T& value)
{
    j = nlohmann::json::object();
    joblens::facts::each(value, [&](const char* name, const auto& field, std::size_t) {
        j[name] = field;
    });
}

template <typename T, std::enable_if_t<joblens::facts::is_facts_v<T>, int> = 0>
inline void from_json(const nlohmann::json& j, T& value)
{
    joblens::facts::each(value, [&](const char* name, auto& field, std::size_t) {
        if (T::is_optional(name)) {
            const auto it = j.find(name);
            if (it != j.end() && !it->is_null()) it->get_to(field);
        } else {
            j.at(name).get_to(field);
        }
    });
}
