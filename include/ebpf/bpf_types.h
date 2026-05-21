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

#include <cstdint>

// 整数类型
using i8   = std::int8_t;
using u8   = std::uint8_t;
using i16  = std::int16_t;
using u16  = std::uint16_t;
using i32  = std::int32_t;
using u32  = std::uint32_t;
using s64  = std::int64_t;
using u64  = std::uint64_t;

//  fastest integer with at least 32/64 bits
using u32f = std::uint_fast32_t;
using u64f = std::uint_fast64_t;
using i32f = std::int_fast32_t;
using i64f = std::int_fast64_t;

//  smallest integer with at least 32/64 bits
using u32l = std::uint_least32_t;
using u64l = std::uint_least64_t;

//  pointer difference / size
using isize = std::ptrdiff_t;
using usize = std::size_t;

//  byte
using byte = std::uint8_t;

//  floating
using f32 = float;
using f64 = double;