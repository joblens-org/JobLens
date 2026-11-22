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
// Typed registration outcome contract.
//
// The store reports why a registration attempt did not fully succeed as an
// enum, not by asking every caller to parse a message string. `to_string()`
// reproduces the stable wire strings the HTTP/RPC surface already exposes, so
// this is an internal contract change only: no client-visible error code
// moves.
#pragma once

#include <string>

enum class RegistrationError {
    None,
    // The source did not supply enough information to build an identity or an
    // execution. Re-reading the source may complete it.
    IncompleteIdentity,
    IncompleteExecution,
    SourceMetadataUnavailable,
    // Durable-store outcomes.
    StoreUnavailable,
    StoreReadFailed,
    StoreWriteFailed,
    WriteOutcomeUnknown,
    IdExhausted,
    // Lifecycle/identity conflicts: retrying without new evidence cannot help.
    ManagerConflict,
    InstanceClosing,
    IdentityQuarantined,
    IdentityIndexMismatch,
    StaleRevision,
    // Manual-create conflicts (the addOrUpdateManualJob CREATE branch): the
    // primary key or the canonical identity index already owns a live row, or
    // the request carried no collector to select. They share the HTTP surface
    // the trigger already maps (already_exists -> 409, invalid_collectors ->
    // 400).
    DuplicateJobId,
    DuplicateIdentity,
    EmptyCollectors,
    // The admission gate (maintenance capture_all=false) refused the
    // observation before any registration work: no record was created and no
    // state changed. Re-reading the source cannot change the decision.
    NotAdmitted,
    // Post-commit obligations: facts are durable, the decision/application is
    // repaired by record-driven maintenance, never by re-reading the source.
    PolicyEvaluationFailed,
    InitialIntentWriteFailed,
    ObserverDeliveryFailed,
    // Threading contract violation, not a data outcome.
    ReentrantMutation,
};

// Stable wire/diagnostic string. Existing strings are preserved verbatim; the
// codes that share a wire string (WriteOutcomeUnknown, the store failures) stay
// distinguishable internally while the API surface is unchanged.
const char* to_string(RegistrationError code);

// True when re-reading the source (the watcher's bounded re-read) or a later
// scan may resolve the failure. False means either the facts are already
// durable and record-driven maintenance owns the repair, or retrying the same
// evidence cannot help.
bool needs_source_retry(RegistrationError code);

// Parses a wire string back to its code. Used by exhaustive-mapping tests and
// by adapters that only see the string form; an unknown string returns
// RegistrationError::None only after the caller checks `known`.
RegistrationError registration_error_from_string(const std::string& wire, bool& known);
