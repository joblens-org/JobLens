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
#include "core/registration_error.hpp"

#include <array>
#include <utility>

namespace {

// One table, one mapping: the wire string and the retry projection are fixed
// here so an exhaustive test can assert every enumerator instead of grepping
// for call sites.
struct Entry {
    RegistrationError code;
    const char* wire;
    bool source_retry;
};

constexpr std::array<Entry, 18> kEntries{{
    {RegistrationError::None, "", false},
    {RegistrationError::IncompleteIdentity, "incomplete_identity", true},
    {RegistrationError::IncompleteExecution, "incomplete_execution", true},
    {RegistrationError::SourceMetadataUnavailable, "source_metadata_unavailable", true},
    {RegistrationError::StoreUnavailable, "store_unavailable", true},
    {RegistrationError::StoreReadFailed, "store_error", true},
    // An ambiguous write may have landed: re-reading the source and looking the
    // identity up is exactly how it is resolved.
    {RegistrationError::StoreWriteFailed, "store_write_failed", true},
    {RegistrationError::WriteOutcomeUnknown, "store_write_failed", true},
    {RegistrationError::IdExhausted, "id_exhausted", false},
    {RegistrationError::GenerationConflict, "execution_generation_conflict", false},
    {RegistrationError::InstanceClosing, "instance_closing", false},
    {RegistrationError::IdentityQuarantined, "identity_quarantined", false},
    {RegistrationError::IdentityIndexMismatch, "identity_index_mismatch", false},
    {RegistrationError::StaleRevision, "stale_revision", false},
    {RegistrationError::PolicyEvaluationFailed, "policy_evaluation_failed", false},
    {RegistrationError::InitialIntentWriteFailed, "initial_intent_write_failed", false},
    {RegistrationError::ObserverDeliveryFailed, "observer_delivery_failed", false},
    {RegistrationError::ReentrantMutation, "reentrant_mutation", false},
}};

const Entry* lookup(RegistrationError code)
{
    for (const auto& entry : kEntries)
        if (entry.code == code) return &entry;
    return nullptr;
}

} // namespace

const char* to_string(RegistrationError code)
{
    const Entry* entry = lookup(code);
    if (entry == nullptr) return "unknown_registration_error";
    return entry->wire;
}

bool needs_source_retry(RegistrationError code)
{
    const Entry* entry = lookup(code);
    return entry != nullptr && entry->source_retry;
}

RegistrationError registration_error_from_string(const std::string& wire, bool& known)
{
    known = false;
    for (const auto& entry : kEntries) {
        if (entry.code == RegistrationError::None) continue;
        if (wire == entry.wire) {
            known = true;
            return entry.code;
        }
    }
    return RegistrationError::None;
}
