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
// One thread-local "mutation gate" shared by the registry and the collection
// manager.
//
// A lifecycle hook or collection applier runs inside the registry's apply gate.
// If it synchronously mutates intent (another `setCollectors`, another
// `reconcileCollectionIntent`), the nested attempt must be refused BEFORE it
// takes a lock or commits anything: the outer call would otherwise publish a
// runtime result that a newer durable binding already superseded. Refusing only
// at the runtime step is too late — the intent was already committed.
#pragma once

namespace joblens {

// True on a thread that is currently inside a gated mutation.
inline bool in_mutation_gate();

namespace detail {
inline thread_local bool g_in_mutation_gate = false;
}  // namespace detail

inline bool in_mutation_gate()
{
    return detail::g_in_mutation_gate;
}

// Marks the current scope as a gated mutation; refuses re-entry by reporting it
// through `in_mutation_gate()`.
class MutationGateScope {
public:
    MutationGateScope() : previous_(detail::g_in_mutation_gate)
    {
        detail::g_in_mutation_gate = true;
    }
    ~MutationGateScope() { detail::g_in_mutation_gate = previous_; }
    MutationGateScope(const MutationGateScope&) = delete;
    MutationGateScope& operator=(const MutationGateScope&) = delete;

private:
    bool previous_;
};

}  // namespace joblens
