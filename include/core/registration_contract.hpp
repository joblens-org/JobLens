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
// The registration result contract (plan §5.1). It lives on its own so a
// discovery source can consume results without pulling in LevelDB.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "core/registration_error.hpp"

enum class UpsertDisposition {
    Created,
    Updated,
    Unchanged,
    Rejected,
    Conflict,
};

struct UpsertResult {
    bool ok = false;
    UpsertDisposition disposition = UpsertDisposition::Rejected;
    uint64_t internal_id = 0;
    std::string instance_id;
    uint64_t facts_revision = 0;
    // True when this upsert completed the execution generation of a record
    // that was stored unverified: its first-complete-registration trigger.
    bool generation_became_complete = false;
    // Typed reason (plan §5.1). `error` is the stable wire string derived from
    // it; callers must branch on the code, never parse the string.
    RegistrationError code = RegistrationError::None;
    std::string error;                   // to_string(code), stable wire form
    std::string message;
    // Fixed projection of the code, not an independent decision: only a
    // source/metadata or facts-persistence failure asks the watcher to re-read
    // the source.
    bool retry_registration() const { return needs_source_retry(code); }
    // True when the initial automatic decision is durable: nullopt when no
    // decision was needed and no selection exists.
    std::optional<bool> intent_committed;

    void fail(RegistrationError why, std::string detail = {})
    {
        ok = false;
        code = why;
        error = to_string(why);
        message = std::move(detail);
    }
};

