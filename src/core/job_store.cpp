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
// JobStore implementation — see include/core/job_store.hpp.
//
// This file is the persistence executor only: LevelDB layout, row codec,
// batch atomicity, tiered durability, recovery fences and the ID high-water.
// Every business decision (merge, restore validation, selection strategy,
// archive-versus-discard) lives in job_registry.cpp.

#include "core/job_store.hpp"

#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>

#include "common/config.hpp"
#include "common/common_job.hpp"

namespace {

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

const std::string KEY_PREFIX_JOB      = "j:";
const std::string KEY_PREFIX_HISTORY  = "h:";
const std::string KEY_PREFIX_COUNTER  = "c:";
const std::string KEY_PREFIX_META     = "m:";
const std::string KEY_PREFIX_IDENTITY = "i:";

constexpr uint32_t kJobStoreSchemaVersion = 5;

// The schema marker is a canonical decimal: ASCII digits only, no sign, no
// whitespace, no leading zero, no trailing junk, and it must fit the field.
// std::stoul would accept "3garbage" (prefix parse) and 4294967299 (fits
// unsigned long, truncates on narrowing), both of which would make an
// unsupported store look like schema 3.
std::optional<uint32_t> parse_schema_marker(const std::string& raw)
{
    if (raw.empty() || raw.size() > 10) return std::nullopt;
    if (raw.size() > 1 && raw.front() == '0') return std::nullopt;
    uint64_t value = 0;
    for (const char digit : raw) {
        if (digit < '0' || digit > '9') return std::nullopt;
        value = value * 10 + static_cast<uint64_t>(digit - '0');
        if (value > std::numeric_limits<uint32_t>::max()) return std::nullopt;
    }
    return static_cast<uint32_t>(value);
}

std::string hex64(uint64_t v) {
    return fmt::format("{:016x}", v);
}

uint64_t unhex64(const std::string& s) {
    return std::stoull(s, nullptr, 16);
}

// Strict hex-id parse for foreign input (the legacy layout): never throws,
// rejects short/long/non-hex keys instead of guessing an id.
bool parse_hex_id(const std::string& hex, uint64_t& value)
{
    if (hex.size() != 16) return false;
    for (const char c : hex) {
        if (std::isxdigit(static_cast<unsigned char>(c)) == 0) return false;
    }
    const auto* begin = hex.data();
    const auto* end = hex.data() + hex.size();
    const auto result = std::from_chars(begin, end, value, 16);
    return result.ec == std::errc() && result.ptr == end;
}

std::string key_job(uint64_t jobID) {
    return KEY_PREFIX_JOB + hex64(jobID);
}

std::string key_history_prefix(uint64_t jobID) {
    return KEY_PREFIX_HISTORY + hex64(jobID) + ":";
}

std::string key_history(uint64_t jobID, uint64_t end_time) {
    return key_history_prefix(jobID) + hex64(end_time);
}

std::string key_counter(const std::string& name) {
    return KEY_PREFIX_COUNTER + name;
}

// One scanner for the id keyspace, used for this store's own rows and for a
// quarantined sibling's foreign ones: the c:job_id counter plus the highest
// j:/h: row id. Malformed keys are skipped, never parsed into an exception;
// a scan failure is reported instead of guessed from.
struct IdScan {
    uint64_t counter = 0;
    uint64_t max_row_id = 0;
    bool counter_malformed = false;
    bool ok = true;
};
IdScan scan_id_keys(leveldb::DB* db)
{
    IdScan out;
    std::string counter_json;
    if (db->Get(leveldb::ReadOptions(), key_counter("job_id"), &counter_json).ok()) {
        try {
            out.counter = std::stoull(counter_json);
        } catch (const std::exception&) {
            out.counter_malformed = true;
        }
    }
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        std::string hex_part;
        if (key.rfind(KEY_PREFIX_JOB, 0) == 0) {
            hex_part = key.substr(KEY_PREFIX_JOB.size(), 16);
        } else if (key.rfind(KEY_PREFIX_HISTORY, 0) == 0) {
            // history key: "h:" + hex64(JobID) + ":" + hex64(end_time)
            hex_part = key.substr(KEY_PREFIX_HISTORY.size(), 16);
        } else {
            continue;
        }
        uint64_t id = 0;
        if (!parse_hex_id(hex_part, id)) continue;
        if (id > out.max_row_id) out.max_row_id = id;
    }
    out.ok = it->status().ok();
    delete it;
    return out;
}

std::string key_meta(const std::string& name) {
    return KEY_PREFIX_META + name;
}


std::string key_identity(const std::string& encoded) {
    return KEY_PREFIX_IDENTITY + encoded;
}

int64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string generate_db_uuid() {
    std::ifstream input("/proc/sys/kernel/random/uuid");
    std::string value;
    if (input && std::getline(input, value) && value.size() >= 32) {
        return value;
    }
    std::random_device device;
    return fmt::format("{:016x}{:016x}-{:016x}",
                       (static_cast<uint64_t>(device()) << 32) | device(),
                       (static_cast<uint64_t>(device()) << 32) | device(),
                       (static_cast<uint64_t>(device()) << 32) | device());
}

// ---------------------------------------------------------------------------
// Process liveness helpers (restore-time validation)
// ---------------------------------------------------------------------------

// /proc/<pid>/stat field 22 (starttime) comes from the shared helper so the
// store, the registry and the probes agree on what "the same process" means.

// read_boot_id() lives in core/liveness so the store, the probes and the
// admission boundary cannot disagree about the current boot.

// An execution identity is only replayable on POSITIVE evidence: the same boot and the
// same manager start time read back from the current process.  A populated
// execution identity proves nothing by itself (a numeric PID is recycled), and a
// readable-but-different start time is positive proof of reuse.
//
// The third answer matters: when /proc/<pid>/stat cannot be read the process
// may well be alive, so the record is neither stale (never delete evidence on
// "unknown") nor verified (never claim a complete execution we did not check).
//
// Provenance decides what "nothing to verify" means.  A manual/legacy row owns
// no manager identity and its bound pid identities are the evidence; a discovery row that
// never produced a complete manager identity has an UNVERIFIED execution, not a
// verified one — an incomplete discovery execution must never be replayed as

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

nlohmann::json manager_to_json(const ProcessIdentity& manager) {
    return nlohmann::json{
        {"boot_id", manager.boot_id},
        {"manager_pid", manager.pid},
        {"manager_start_ticks", manager.start_ticks},
    };
}

ProcessIdentity manager_from_json(const nlohmann::json& in) {
    ProcessIdentity manager;
    manager.boot_id = in.value("boot_id", std::string{});
    manager.pid = in.value("manager_pid", pid_t{0});
    manager.start_ticks = in.value("manager_start_ticks", uint64_t{0});
    return manager;
}

nlohmann::json pid_identity_to_json(const ProcessIdentity& process) {
    return nlohmann::json{
        {"pid", process.pid},
        {"start_ticks", process.start_ticks},
        {"boot_id", process.boot_id},
    };
}

ProcessIdentity pid_identity_from_json(const nlohmann::json& in) {
    ProcessIdentity process;
    process.pid = in.value("pid", pid_t{0});
    process.start_ticks = in.value("start_ticks", uint64_t{0});
    process.boot_id = in.value("boot_id", std::string{});
    return process;
}

void write_job_fields(nlohmann::json& out, const Job& job) {
    out["jobtype"]       = static_cast<int>(job.jobtype);
    out["subtype"]       = static_cast<int>(job.subtype);
    if (!job.subtype_name.empty()) out["subtype_name"] = job.subtype_name;
    out["pids"]          = pids_of(job);
    out["JobNativeID"]   = job.JobNativeID;
    out["clusterName"]   = job.clusterName;
    out["clusterTag"]    = job.clusterTag;
    std::visit([&out](const auto& obj) {
        out["sub_attr"] = nlohmann::json(obj);
    }, job.sub_attr);
}

nlohmann::json selection_to_json(const CollectionSelection& selection);
CollectionSelection selection_from_json(const nlohmann::json& in);

nlohmann::json record_json(const JobRecord& record) {
    nlohmann::json out;
    write_job_fields(out, record.job);
    out["schema_version"] = kJobStoreSchemaVersion;
    // The desired selection lives in the record that owns the instance and is
    // ordered by the record's single revision.
    out["selection"] = selection_to_json(record.selection);
    out["revision"]       = record.revision;
    out["state"]          = to_string(record.state);
    out["first_seen_us"]  = record.first_seen_us;
    out["last_seen_us"]   = record.last_seen_us;
    // Who owns the facts. The logical identity is not stored: it is derived
    // from the facts by the adapter's native-id rule.
    out["source"] = record.source == FactSource::Discovery ? "discovery" : "manual";
    if (!record.index_key.empty()) out["index_key"] = record.index_key;
    if (!record.job.subtype_name.empty()) out["adapter"] = record.job.subtype_name;
    // Execution evidence: the local run that produced the facts, rebuilt from
    // a read on the next observation.
    out["manager"]        = manager_to_json(record.job.execution.manager);
    out["pid_identities"] = nlohmann::json::array();
    for (const auto& pid_identity : pid_identities_of(record.job)) {
        out["pid_identities"].push_back(pid_identity_to_json(pid_identity));
    }
    return out;
}

Job decode_legacy_job(const nlohmann::json& j) {
    Job job;
    job.jobtype       = static_cast<JobType>(j.value("jobtype", 0));
    job.subtype       = static_cast<JobSubType>(j.value("subtype", 0));
    job.subtype_name  = j.value("subtype_name", "");
    set_pids(job, j.value("pids", nlohmann::json::array()).get<std::vector<pid_t>>());
    job.JobNativeID   = j.value("JobNativeID", "");
    job.clusterName   = j.value("clusterName", "");
    job.clusterTag    = j.value("clusterTag", "");

    if (!job.subtype_name.empty()) {
        // Open-world scheduler: the adapter owns the facts schema; the JSON
        // object is carried verbatim (the adapter's decodeFacts is the only
        // interpreter).
        job.sub_attr = j.contains("sub_attr") && j["sub_attr"].is_object()
                           ? j["sub_attr"]
                           : nlohmann::json::object();
    } else if (job.subtype == JobSubType::Common || job.subtype == JobSubType::Unknown) {
        job.sub_attr = CommonJobAttr{.auto_update_child = true};
    } else {
        // A typed row without an adapter decoder: keep its stored facts as the
        // open-world value instead of guessing a scheduler schema.
        job.sub_attr = j.contains("sub_attr") && j["sub_attr"].is_object()
                           ? j["sub_attr"]
                           : nlohmann::json::object();
    }

    return job;
}

JobRecord parse_record(const std::string& raw, uint64_t job_id,
                       const JobStore::Options::RowDecoder& decoder) {
    const auto j = nlohmann::json::parse(raw);
    JobRecord record;
    record.job = decoder ? decoder(j) : decode_legacy_job(j);
    record.job.JobID = job_id;
    ExecutionEvidence& execution = record.job.execution;

    record.revision       = j.value("revision", uint64_t{1});
    const auto state_text = j.value("state", std::string{"active"});
    const auto state = record_state_from_string(state_text);
    if (!state) {
        throw std::runtime_error("record has an unknown state '" + state_text + "'");
    }
    record.state = *state;
    record.first_seen_us  = j.value("first_seen_us", int64_t{0});
    record.last_seen_us   = j.value("last_seen_us", int64_t{0});
    if (record.revision == 0) record.revision = 1;

    // Schema 4 stores the desired selection in the same row, ordered by the
    // record revision. A record without a selection object is a corrupt row,
    // not an implicit auto selection: never silently invent intent for an
    // existing instance.
    if (!j.contains("selection") || !j["selection"].is_object()) {
        throw std::runtime_error("record has no selection object");
    }
    record.selection = selection_from_json(j["selection"]);

    if (record.job.subtype_name.empty())
        record.job.subtype_name = j.value("adapter", std::string{});
    record.index_key = j.value("index_key", std::string{});
    if (j.contains("manager") && j["manager"].is_object()) {
        execution.manager = manager_from_json(j["manager"]);
    }
    const auto pid_identities_it = j.find("pid_identities");
    if (pid_identities_it != j.end() && pid_identities_it->is_array()) {
        for (const auto& pid_identity_json : *pid_identities_it) {
            if (pid_identity_json.is_object())
                execution.pid_identities.push_back(pid_identity_from_json(pid_identity_json));
        }
        execution.pid_identities = normalized_pid_identities(std::move(execution.pid_identities));
    }
    // Provenance is stored explicitly. A row written before the field existed
    // is classified from its own evidence: a populated manager triple means a
    // discovery observation produced it, a completely empty one is the manual
    // shape (manual registration never invents a manager).
    const auto source = j.value("source", std::string{});
    if (source == "discovery") {
        record.source = FactSource::Discovery;
    } else if (source == "manual") {
        record.source = FactSource::Manual;
    } else {
        record.source = execution.manager.complete() ? FactSource::Discovery
                                                     : FactSource::Manual;
    }
    return record;
}

nlohmann::json selection_to_json(const CollectionSelection& selection) {
    nlohmann::json out;
    out["automatic"] = selection.automatic.has_value()
                           ? nlohmann::json(*selection.automatic)
                           : nlohmann::json(nullptr);
    out["automatic_source"] = selection.automatic_source;
    out["manual"] = selection.manual.has_value() ? nlohmann::json(*selection.manual)
                                                 : nlohmann::json(nullptr);
    return out;
}

CollectionSelection selection_from_json(const nlohmann::json& in) {
    CollectionSelection selection;
    if (in.contains("automatic") && in["automatic"].is_array()) {
        selection.automatic =
            in["automatic"].get<std::vector<std::string>>();
    }
    selection.automatic_source = in.value("automatic_source", std::string{});
    if (in.contains("manual") && in["manual"].is_array()) {
        selection.manual = in["manual"].get<std::vector<std::string>>();
    }
    return selection;
}

}  // namespace

// ---------------------------------------------------------------------------
// Row pid identity canonicalization (shared with the registry's merge path)
// ---------------------------------------------------------------------------

std::vector<ProcessIdentity> normalized_pid_identities(std::vector<ProcessIdentity> pid_identities) {
    std::sort(pid_identities.begin(), pid_identities.end(),
              [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                  if (lhs.pid != rhs.pid) return lhs.pid < rhs.pid;
                  return lhs.start_ticks < rhs.start_ticks;
              });
    pid_identities.erase(std::unique(pid_identities.begin(), pid_identities.end(),
                                [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                                    return lhs == rhs;
                                }),
                    pid_identities.end());
    return pid_identities;
}

// Unambiguous identity index key. Canonical components are validated
// (scheduler name, digits/dot native id, normalized step), so the only
// free-form part is the cluster namespace and it is length-prefixed.

// ---------------------------------------------------------------------------
// Selection value types
// ---------------------------------------------------------------------------

std::vector<std::string> CollectionSelection::effectiveCollectors() const
{
    if (manual.has_value()) return *manual;
    if (automatic.has_value()) return *automatic;
    return {};
}

// ---------------------------------------------------------------------------
// Construction / open / schema admission
// ---------------------------------------------------------------------------

JobStore::JobStore(const Options& options)
    : options_(options) {
    if (!init_db(options)) {
        db_running_ = false;
        spdlog::warn("JobStore: job db not running");
    }
}

JobStore::~JobStore()
{
    close_db();
}

bool JobStore::init_db(const Options& options) {
    std::string db_path = options.db_path;
    if (db_path.empty()) {
        db_path = Config::instance().getString("job_registry_config", "job_db_path");
    }
    // A trailing separator would push the quarantine sibling inside the store
    // directory; normalize once here so every downstream path agrees.
    while (db_path.size() > 1 && db_path.back() == '/') db_path.pop_back();

    std::filesystem::path db_dir(db_path);
    if (db_dir.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(db_dir.parent_path(), ec);
    }

    leveldb::Options level_options;
    level_options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(level_options, db_path, &db_);
    if (!status.ok()) {
        spdlog::error("JobStore: failed to open LevelDB at {}: {}", db_path, status.ToString());
        db_ = nullptr;
        return false;
    }

    spdlog::info("JobStore: opened {}", db_path);
    db_running_ = true;

    // The store format lifecycle seam owns every admission decision (open the
    // current schema, initialize a fresh store, quarantine a legacy layout,
    // or reject); init_db only executes its outcome.
    if (!admit_database(db_path)) return false;

    migration_complete_ = true;
    migration_state_ = "complete";
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        std::string value;
        if (db_->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value).ok()) {
            db_uuid_ = value;
        }
    }

    if (db_uuid_.empty()) {
        if (!load_or_create_db_uuid()) {
            spdlog::error("JobStore: could not persist the Agent DB UUID");
        }
    }

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        init_id_counter();
    }

    return true;
}

bool JobStore::db_is_empty() const {
    if (!db_) return true;
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    it->SeekToFirst();
    const bool empty = !it->Valid();
    const auto status = it->status();
    delete it;
    if (!status.ok()) {
        spdlog::error("JobStore: cannot inspect the database during schema admission: {}",
                      status.ToString());
        return false;   // fail closed: never treat an unreadable store as empty
    }
    return empty;
}

bool JobStore::initialize_schema(uint64_t watermark_floor) {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return false;
    if (db_uuid_.empty()) db_uuid_ = generate_db_uuid();
    leveldb::WriteBatch batch;
    batch.Put(key_meta("schema_version"), std::to_string(kJobStoreSchemaVersion));
    batch.Put(key_meta("db_uuid"), db_uuid_);
    // The watermark rides the bootstrap batch (synced): a crash leaves the
    // store either unmarked — the whole batch lost — or marked with the
    // watermark already durable, never marked without it.
    if (watermark_floor > 1)
        batch.Put(key_counter("job_id"), std::to_string(watermark_floor));
    const auto status = write_batch_locked(batch, "initialize schema");
    if (!status.ok()) {
        spdlog::error("JobStore: failed to initialize schema {}: {}", kJobStoreSchemaVersion,
                      status.ToString());
        return false;
    }
    schema_version_ = kJobStoreSchemaVersion;
    return true;
}

void JobStore::reject_unsupported_schema(uint32_t found) {
    reject_schema("unsupported_schema:" + std::to_string(found), found);
}

void JobStore::reject_schema(const std::string& state, uint32_t found) {
    schema_version_ = found;
    migration_complete_ = false;
    migration_state_ = state;
    // Rejection is read-only: the database handle is closed without a
    // checkpoint, so every original key and value stays exactly as it was.
    if (db_) {
        delete db_;
        db_ = nullptr;
    }
    db_running_ = false;
}

// ---------------------------------------------------------------------------
// Store format lifecycle: the one admission seam
// ---------------------------------------------------------------------------

bool JobStore::admit_database(const std::string& db_path) {
    // The marker names the layout this binary speaks. An unreadable marker is
    // not "no marker": quarantine decisions must never be made against an
    // I/O error.
    bool schema_present = false;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        std::string value;
        const auto s = db_->Get(leveldb::ReadOptions(), key_meta("schema_version"), &value);
        if (s.ok()) {
            schema_present = true;
            const auto parsed = parse_schema_marker(value);
            if (!parsed) {
                // Not a marker this build wrote: no UUID, id counter or record
                // is initialized from it, and no byte of the store is touched.
                spdlog::error("JobStore: schema marker '{}' at {} is not canonical; "
                              "refusing to open an unreadable layout", value, db_path);
                reject_schema("malformed_schema_marker", 0);
                return false;
            }
            schema_version_ = *parsed;
        } else if (!s.IsNotFound()) {
            spdlog::error("JobStore: could not read the schema marker at {}: {}",
                          db_path, s.ToString());
            reject_schema("schema_marker_unreadable", 0);
            return false;
        }
    }
    const std::string legacy_path = db_path + ".legacy";

    if (schema_present) {
        if (schema_version_ != kJobStoreSchemaVersion) {
            spdlog::error("JobStore: db schema {} is not supported by this binary (schema {}); "
                          "refusing to migrate or delete it", schema_version_,
                          kJobStoreSchemaVersion);
            reject_unsupported_schema(schema_version_);
            return false;
        }
        // Current schema. A sibling may still hold a watermark this store
        // never adopted — the crash window between the schema batch and the
        // import. It blocks readiness only while this store has no counter
        // of its own; with one, the sibling is a residual.
        const auto imported = import_legacy_watermark(legacy_path);
        if (imported == WatermarkImport::Blocked && !counter_present_locked()) {
            spdlog::error("JobStore: the legacy sibling at {} cannot be read and this "
                          "store carries no allocator watermark of its own; refusing "
                          "to start without it", legacy_path);
            reject_schema("legacy_watermark_unreadable", 0);
            return false;
        }
        return true;
    }

    // No marker. A store with data is a pre-schema legacy layout: quarantine it
    // by rename — every byte preserved, never migrated or deleted in place.
    // The slot is single-use: a second legacy store while a quarantined
    // sibling exists is an operator decision, never an automatic overwrite.
    if (!db_is_empty()) {
        std::error_code exists_error;
        if (std::filesystem::exists(legacy_path, exists_error) || exists_error) {
            spdlog::error("JobStore: database at {} has data but no schema marker and the "
                          "quarantine slot {} is already taken; refusing to chain "
                          "quarantines — resolve both stores explicitly",
                          db_path, legacy_path);
            reject_unsupported_schema(/*found=*/0);
            return false;
        }
        // Read-only close, the same discipline reject_schema uses: the legacy
        // store keeps every key and value exactly as the old build left them.
        {
            std::lock_guard<std::mutex> db_lock(db_mtx_);
            delete db_;
            db_ = nullptr;
        }
        std::error_code rename_error;
        std::filesystem::rename(db_path, legacy_path, rename_error);
        if (rename_error) {
            spdlog::error("JobStore: could not quarantine the legacy store at {} to {} "
                          "({}); rejecting", db_path, legacy_path,
                          rename_error.message());
            reject_unsupported_schema(/*found=*/0);
            return false;
        }
        spdlog::warn("JobStore: quarantined a legacy (pre-schema) store to {} — it was "
                     "neither migrated nor deleted; the allocator watermark is "
                     "imported, row data is not", legacy_path);
        leveldb::Options reopen_options;
        reopen_options.create_if_missing = true;
        const auto reopened = leveldb::DB::Open(reopen_options, db_path, &db_);
        if (!reopened.ok()) {
            spdlog::error("JobStore: could not open a fresh store at {} after "
                          "quarantining: {}", db_path, reopened.ToString());
            db_running_ = false;
            migration_complete_ = false;
            migration_state_ = "quarantine_reopen_failed";
            return false;
        }
    }

    // Fresh store: read the sibling's floor first so the watermark rides the
    // schema batch — an import failure here is a refusal to start, never a
    // silent restart of the id space.
    uint64_t sibling_floor = 1;
    std::error_code sibling_exists_error;
    const bool sibling_exists =
        std::filesystem::exists(legacy_path, sibling_exists_error);
    if (sibling_exists || sibling_exists_error) {
        if (!read_legacy_floor(legacy_path, sibling_floor)) {
            spdlog::error("JobStore: the legacy sibling at {} cannot be read for the "
                          "watermark import; refusing to initialize without it",
                          legacy_path);
            reject_schema("legacy_watermark_unreadable", 0);
            return false;
        }
    }
    if (!initialize_schema(sibling_floor)) {
        spdlog::error("JobStore: could not initialize a new schema-{} store",
                      kJobStoreSchemaVersion);
        reject_unsupported_schema(/*found=*/0);
        return false;
    }
    return true;
}

bool JobStore::read_legacy_floor(const std::string& legacy_path, uint64_t& floor)
{
    floor = 1;
    leveldb::Options options;
    options.create_if_missing = false;
    leveldb::DB* legacy = nullptr;
    const auto opened = leveldb::DB::Open(options, legacy_path, &legacy);
    if (!opened.ok() || legacy == nullptr) {
        spdlog::warn("JobStore: the legacy store at {} could not be opened: {}",
                     legacy_path, opened.ToString());
        return false;
    }
    const IdScan scan = scan_id_keys(legacy);
    delete legacy;
    if (!scan.ok) {
        spdlog::warn("JobStore: the legacy store at {} could not be scanned",
                     legacy_path);
        return false;
    }
    // The watermark rule init_id_counter applies to this store's own rows,
    // applied to the foreign ones: the counter plus every row id (active and
    // archived), saturated. A saturated floor makes the counter init read an
    // unmarked MAX back and conservatively stop allocating.
    if (scan.max_row_id == std::numeric_limits<uint64_t>::max()) {
        floor = scan.max_row_id;
        return true;
    }
    floor = std::max({scan.counter, scan.max_row_id + 1, uint64_t{1}});
    return true;
}

JobStore::WatermarkImport JobStore::import_legacy_watermark(const std::string& legacy_path)
{
    std::error_code exists_error;
    if (!std::filesystem::exists(legacy_path, exists_error) || exists_error)
        return WatermarkImport::NoSibling;

    uint64_t floor = 1;
    if (!read_legacy_floor(legacy_path, floor))
        return WatermarkImport::Blocked;
    if (floor <= 1) return WatermarkImport::AlreadyAhead;

    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return WatermarkImport::Blocked;
    uint64_t current = 0;
    {
        std::string value;
        if (db_->Get(leveldb::ReadOptions(), key_counter("job_id"), &value).ok()) {
            try {
                current = std::stoull(value);
            } catch (const std::exception&) {
            }
        }
    }
    if (floor <= current) return WatermarkImport::AlreadyAhead;
    leveldb::WriteOptions synced;
    synced.sync = true;
    if (!db_->Put(synced, key_counter("job_id"), std::to_string(floor)).ok()) {
        spdlog::error("JobStore: could not persist the imported allocator watermark");
        return WatermarkImport::Blocked;
    }
    spdlog::info("JobStore: imported the allocator watermark {} from the legacy "
                 "store at {}", floor, legacy_path);
    return WatermarkImport::Imported;
}

bool JobStore::counter_present_locked() const
{
    if (!db_) return false;
    std::string value;
    return db_->Get(leveldb::ReadOptions(), key_counter("job_id"), &value).ok();
}

void JobStore::close_db() {
    // Last chance: anything written without a fence becomes durable here, or it
    // is reported instead of silently dropped.
    if (!flushDirty("shutdown checkpoint")) {
        spdlog::error("JobStore: the shutdown checkpoint could not be written; the last "
                      "unsynced registration window is lost");
    }
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    delete db_;
    db_ = nullptr;
    db_running_ = false;
    spdlog::info("JobStore: closed");
}

bool JobStore::load_or_create_db_uuid() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return false;

    std::string value;
    auto s = db_->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value);
    if (s.ok() && !value.empty()) {
        db_uuid_ = value;
        return true;
    }

    db_uuid_ = generate_db_uuid();
    leveldb::WriteBatch batch;
    batch.Put(key_meta("db_uuid"), db_uuid_);
    auto status = write_batch_locked(batch, "create db uuid");
    if (!status.ok()) {
        spdlog::error("JobStore: failed to persist db uuid: {}", status.ToString());
        return false;
    }
    return true;
}

std::vector<JobRecord> JobStore::loadAll() {
    // Raw rows only: the registry validates restored executions with its own
    // probe and owns the record map. This loads what is durable, verbatim.
    std::vector<JobRecord> restored;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        if (!db_) return restored;
        leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
        for (it->Seek(KEY_PREFIX_JOB); it->Valid() && it->key().starts_with(KEY_PREFIX_JOB); it->Next()) {
            const std::string key = it->key().ToString();
            uint64_t jobID = 0;
            try {
                jobID = unhex64(key.substr(KEY_PREFIX_JOB.size()));
            } catch (const std::exception&) {
                ++corrupt_records_;
                continue;
            }
            if (jobID == kSystemSentinelJobId) continue;
            try {
                auto record = parse_record(it->value().ToString(), jobID, options_.row_decoder);
                restored.push_back(std::move(record));
            } catch (const std::exception& error) {
                ++corrupt_records_;
                spdlog::warn("JobStore: cannot restore job {}: {}", jobID, error.what());
            }
        }
        const auto status = it->status();
        delete it;
        if (!status.ok()) {
            spdlog::warn("JobStore: iterator error during restore: {}", status.ToString());
        }
    }

    size_t closing_restored = 0, incomplete = 0;
    for (const auto& record : restored) {
        if (record.state == RecordState::Closing) ++closing_restored;
        incomplete += record.index_key.empty() ? 1 : 0;
    }
    if (closing_restored > 0) {
        spdlog::warn("JobStore: restored {} interrupted closing record(s); they will be "
                     "archived by the closing cleanup", closing_restored);
    }
    spdlog::info("JobStore: loaded {} job records ({} incomplete)",
                 restored.size(), incomplete);
    return restored;
}

void JobStore::mark_record_dirty(uint64_t job_id) {
    // Callers hold db_mtx_ (dirty_records_ is persistence-side bookkeeping).
    dirty_records_.insert(job_id);
}

bool JobStore::flushDirty(const char* reason) {
    if (!db_running_ || db_ == nullptr) return true;
    if (!row_provider_) {
        spdlog::error("JobStore: no row provider installed; cannot checkpoint");
        return false;
    }

    // Snapshot phase 1: the dirty ids under db_mtx_.
    std::vector<uint64_t> ids;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        if (dirty_records_.empty()) return true;
        if (recovery_required_.load()) {
            // These rows may already carry a write memory never published:
            // rewriting the memory snapshot here would revert the durable
            // image. Shutdown and the destructor must never do that.
            for (const auto id : dirty_records_) {
                if (recovery_ids_.count(id) != 0) {
                    spdlog::error("JobStore: checkpoint '{}' refused: JobID {} has an "
                                  "unknown durable image (last operation '{}')",
                                  reason, id, recovery_operation_);
                    return false;
                }
            }
        }
        ids.assign(dirty_records_.begin(), dirty_records_.end());
    }

    // Snapshot phase 2: row content through the provider WITHOUT any store
    // lock, so the registry's lock order (registry lock -> store lock) can
    // never invert. Rows the registry no longer holds are skipped exactly like
    // a row the old in-memory map no longer had.
    std::vector<JobRecord> snapshots;
    for (const auto id : ids) {
        auto row = row_provider_(id);
        if (row) snapshots.push_back(std::move(*row));
    }

    leveldb::WriteBatch batch;
    for (const auto& snapshot : snapshots)
        batch.Put(key_job(snapshot.job.JobID), record_json(snapshot).dump());

    leveldb::Status status;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        status = write_batch_locked(batch, reason, /*durable=*/true);
        if (status.ok()) {
            for (const auto id : ids) dirty_records_.erase(id);
        }
    }
    if (!status.ok()) {
        // Memory already holds these rows: the recovery direction is "write
        // them again", never "read them back".
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        latch_recovery_locked(reason, RecoveryKind::RewriteMemory, {});
        return false;
    }
    return true;
}


bool JobStore::ready() const {
    return store_ready();
}


bool JobStore::store_ready() const {
    return db_running_ && db_ != nullptr && migration_complete_;
}


leveldb::Status JobStore::write_batch_locked(leveldb::WriteBatch& batch,
                                             const char* operation, bool durable,
                                             const std::vector<uint64_t>* touched,
                                             bool delete_only) {
    if (!db_) return leveldb::Status::IOError("job db is not open");

    StoreFaultMode injected = StoreFaultMode::None;
    if (fault_count_ > 0) {
        if (fault_skip_ > 0) {
            --fault_skip_;
        } else {
            --fault_count_;
            injected = fault_mode_;
            if (injected == StoreFaultMode::FailBeforeWrite) {
                record_store_failure(fmt::format("injected write failure before commit ({})",
                                                 operation));
                if (touched && !touched->empty())
                    latch_recovery_locked(operation, RecoveryKind::AdoptDurable, *touched,
                                          delete_only);
                return leveldb::Status::IOError("injected write failure before commit");
            }
        }
    }

    leveldb::WriteOptions write_options;
    write_options.sync = durable;
    auto status = db_->Write(write_options, &batch);

    if (injected == StoreFaultMode::FailAfterWrite) {
        record_store_failure(fmt::format("injected write failure after commit ({})", operation));
        if (touched && !touched->empty())
            latch_recovery_locked(operation, RecoveryKind::AdoptDurable, *touched, delete_only);
        return leveldb::Status::IOError("injected write failure after commit");
    }
    if (!status.ok()) {
        record_store_failure(fmt::format("LevelDB write failed ({}): {}",
                                         operation, status.ToString()));
        if (touched && !touched->empty())
            latch_recovery_locked(operation, RecoveryKind::AdoptDurable, *touched, delete_only);
        return status;
    }

    durable_commits_.fetch_add(1);
    if (durable) sync_commits_.fetch_add(1);
    record_store_success();
    return status;
}

void JobStore::latch_recovery_locked(const char* operation, RecoveryKind kind,
                                     const std::vector<uint64_t>& ids, bool delete_only)
{
    // A durable-row uncertainty is never downgraded to a checkpoint retry:
    // adopting disk is the safe direction and it covers every nameable row.
    if (recovery_kind_ != RecoveryKind::AdoptDurable) recovery_kind_ = kind;
    // A rewrite latch (the checkpoint failure path) names no ids and must not
    // weaken a delete-only proof already latched for earlier ids.
    if (kind == RecoveryKind::AdoptDurable) {
        const bool first = recovery_ids_.empty();
        recovery_delete_only_ = first ? delete_only
                                      : (recovery_delete_only_ && delete_only);
    }
    recovery_ids_.insert(ids.begin(), ids.end());
    recovery_operation_ = operation ? operation : "unknown write";
    recovery_required_.store(true);
    spdlog::error("JobStore: write outcome for '{}' is unknown; recovery required before "
                  "any mutation, runtime application or checkpoint", recovery_operation_);
}

bool JobStore::recoverPending(std::vector<JobRecord>& adopted,
                              std::vector<uint64_t>& removed, std::string& error) {
    error.clear();
    adopted.clear();
    removed.clear();
    if (!recovery_required_.load()) return true;
    // No whole-call db lock: while the latch is set every mutation and the
    // checkpoint refuse to run, so the phases below may release db_mtx_
    // between steps — required because the RowProvider must never be called
    // with a store lock held.
    return recover_pending_locked(adopted, removed, error);
}

bool JobStore::recover_pending_locked(std::vector<JobRecord>& adopted,
                                      std::vector<uint64_t>& removed, std::string& error) {
    if (!recovery_required_.load()) return true;
    if (!db_) {
        error = "store_unavailable";
        return false;
    }

    // Phase 1 (db_mtx_ held by the caller): adopt the durable image for every
    // latched id. No memory is touched here; adopted rows and proven removals
    // are collected for the caller (the registry owns the record map) and for
    // the fence.
    std::vector<uint64_t> ids;
    std::vector<ArchiveFence> archives;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        ids.assign(recovery_ids_.begin(), recovery_ids_.end());
        for (const auto id : ids) {
            if (adopt_locked_pass1(id, archives, adopted, removed, error)) continue;
            for (const auto pending : ids) dirty_records_.insert(pending);
            spdlog::error("JobStore: recovery of JobID {} failed ({}); the store stays "
                          "latched and no write is attempted", id, error);
            return false;
        }
    }

    // The recovered rows are once again authoritative, but the write that
    // landed may itself have been unsynced. Adopting the row and erasing its
    // dirty marker is not a durability fence: rewrite every adopted row
    // together with the remaining unsynced rows in ONE fenced batch. Only a
    // successful fence may clear the latch or the dirty obligation.
    std::set<uint64_t> fence_extra;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        for (const auto id : recovery_ids_) dirty_records_.insert(id);
        for (const auto id : dirty_records_) {
            // Rows already covered by an adopted image, an archive fence or a
            // confirmed delete must not ALSO be written from the pre-recovery
            // memory image, which would resurrect a removed instance.
            if (recovery_ids_.count(id) != 0) continue;
            fence_extra.insert(id);
        }
    }

    // Phase 2 (no store lock): the registry image of the plain dirty rows.
    if (!row_provider_) {
        error = "no_row_provider";
        return false;
    }
    std::vector<JobRecord> extra_rows;
    for (const auto id : fence_extra) {
        auto row = row_provider_(id);
        if (row) extra_rows.push_back(std::move(*row));
    }

    // Phase 3: one durable fence batch.
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        leveldb::WriteBatch batch;
        uint64_t fence_entries = 0;
        for (const auto& row : adopted) {
            batch.Put(key_job(row.job.JobID), record_json(row).dump());
            ++fence_entries;
        }
        for (const auto& row : extra_rows) {
            batch.Put(key_job(row.job.JobID), record_json(row).dump());
            ++fence_entries;
        }
        for (const auto& id : removed) {
            batch.Delete(key_job(id));
            ++fence_entries;
        }
        for (const auto& archive : archives) {
            // Verbatim: the key carries the archive timestamp and the value the
            // final summary. Rebuilding either from memory would write a second
            // history row under a different key.
            batch.Put(archive.history_key, archive.history_value);
            // The active row the archive deleted: re-deleting is the idempotent
            // half of the same image, and it makes the fence non-empty even
            // when the instance had no other active row left.
            batch.Delete(key_job(archive.job_id));
            fence_entries += 2;
            if (!archive.identity_key.empty()) {
                batch.Delete(archive.identity_key);
                ++fence_entries;
            }
        }
        recovery_fence_entries_ = fence_entries;
        const auto status = write_batch_locked(batch, "recovery checkpoint",
                                               /*durable=*/true);
        if (!status.ok()) {
            // The latch stays set and the dirty sets keep their obligation:
            // the next retry must fence these same rows again.
            error = "recovery_checkpoint_failed";
            return false;
        }
        for (const auto id : recovery_ids_) dirty_records_.erase(id);
        for (const auto id : fence_extra) dirty_records_.erase(id);
    }

    recovery_ids_.clear();
    recovery_kind_ = RecoveryKind::None;
    recovery_delete_only_ = false;
    recovery_operation_.clear();
    recovery_required_.store(false);
    record_store_success();
    spdlog::info("JobStore: recovery complete; {} latched instance(s) resolved into {} "
                  "adoption(s), {} archive image(s) re-asserted and {} deletion(s) "
                  "confirmed",
                  ids.size(), adopted.size(), archives.size(), removed.size());
    return true;
}

bool JobStore::adopt_locked_pass1(uint64_t id,
                                  std::vector<ArchiveFence>& archives,
                                  std::vector<JobRecord>& adopted,
                                  std::vector<uint64_t>& removed, std::string& error)
{
    std::optional<ArchiveFence> archive;
    bool delete_confirmed = false;
    JobRecord adopted_row;
    std::string failure;
    if (!adopt_durable_locked(id, archive, delete_confirmed, adopted_row, failure)) {
        // Adoption is partial: keep the fenced-write obligation visible. A
        // later flush skips rows the registry no longer holds, so re-marking
        // every latched id is the conservative direction.
        error = failure.empty() ? "recovery_read_failed" : failure;
        return false;
    }
    if (archive) {
        archives.push_back(std::move(*archive));
        // The archive fence proves the instance is gone from the active set;
        // report it as a removal so the registry drops its map row (the fence
        // itself re-asserts the durable history image).
        removed.push_back(id);
    } else if (delete_confirmed) {
        removed.push_back(id);
    } else {
        adopted.push_back(std::move(adopted_row));
    }
    return true;
}
bool JobStore::read_archive_fence_locked(uint64_t job_id, ArchiveFence& fence,
                                         std::string& error) {
    const std::string prefix = key_history_prefix(job_id);
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(leveldb::ReadOptions()));
    size_t found = 0;
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        if (++found > 1) {
            // Two history rows for one job id: no image can be called
            // authoritative, so nothing may be re-asserted or dropped.
            error = "recovery_history_ambiguous";
            spdlog::error("JobStore: JobID {} has more than one history row; recovery "
                          "cannot choose an archive image", job_id);
            return false;
        }
        fence.history_key = it->key().ToString();
        fence.history_value = it->value().ToString();
    }
    const auto status = it->status();
    if (!status.ok()) {
        error = "recovery_read_failed";
        record_store_failure("LevelDB read failed (recovery history): " + status.ToString());
        return false;
    }
    if (found == 0) {
        // The active row is gone but no archive exists: the landed batch is not
        // the archive this process assumed. Guessing either way would lose or
        // resurrect an instance.
        error = "recovery_history_missing";
        spdlog::error("JobStore: JobID {} has no active row and no history row; "
                      "recovery cannot prove what landed", job_id);
        return false;
    }

    fence.job_id = job_id;
    JobRecord archived;
    try {
        archived = parse_record(fence.history_value, job_id, options_.row_decoder);
    } catch (const std::exception& exc) {
        error = "recovery_parse_failed";
        spdlog::error("JobStore: cannot parse the history row of JobID {} during "
                      "recovery: {}", job_id, exc.what());
        return false;
    }
    if (archived.index_key.empty()) return true;

    // The archive released the identity index only while this instance owned
    // it. Read the owner back instead of deleting blind: a newer execution may
    // own the same identity by now.
    const std::string index_key = key_identity(archived.index_key);
    std::string value;
    const auto index_status = db_->Get(leveldb::ReadOptions(), index_key, &value);
    if (!index_status.ok() && !index_status.IsNotFound()) {
        error = "recovery_read_failed";
        record_store_failure("LevelDB read failed (recovery identity): " +
                             index_status.ToString());
        return false;
    }
    if (index_status.ok()) {
        uint64_t owner = 0;
        try {
            owner = nlohmann::json::parse(value).value("job_id", uint64_t{0});
        } catch (const std::exception&) {
            owner = 0;
        }
        if (owner == job_id) fence.identity_key = index_key;
    }
    return true;
}

bool JobStore::adopt_durable_locked(uint64_t job_id, std::optional<ArchiveFence>& archive,
                                    bool& delete_confirmed, JobRecord& adopted_record,
                                    std::string& error) {
    archive.reset();
    delete_confirmed = false;
    std::string raw;
    const auto status = db_->Get(leveldb::ReadOptions(), key_job(job_id), &raw);

    if (status.IsNotFound()) {
        if (recovery_delete_only_) {
            // The latched batch only deleted rows and the `j:` row is gone: a
            // delete batch is atomic, so the index entry cannot have survived
            // it either. The deletion is confirmed; the fence re-asserts it by
            // deleting the row again.
            delete_confirmed = true;
            return true;
        }
        // The write that lost certainty deleted the active row. Only the
        // archive deletes `j:`; the image it left in `h:` is read back as the
        // only authoritative image and must be fenced verbatim.
        ArchiveFence fence;
        if (!read_archive_fence_locked(job_id, fence, error)) return false;
        archive = std::move(fence);
        return true;
    }
    if (!status.ok()) {
        error = "recovery_read_failed";
        record_store_failure("LevelDB read failed (recovery): " + status.ToString());
        return false;
    }

    JobRecord record;
    try {
        record = parse_record(raw, job_id, options_.row_decoder);
    } catch (const std::exception& exc) {
        error = "recovery_parse_failed";
        spdlog::error("JobStore: cannot parse the durable row of JobID {} during "
                      "recovery: {}", job_id, exc.what());
        return false;
    }
    adopted_record = std::move(record);
    return true;
}

void JobStore::record_store_failure(const std::string& message) {
    store_write_failures_.fetch_add(1);
    store_degraded_.store(true);
    last_store_error_ = message;
    spdlog::error("JobStore: {}", message);
}

void JobStore::record_store_success() {
    // A successful write says nothing about the row whose outcome already lost
    // certainty: it must not release the recovery latch.
    store_degraded_.store(recovery_required_.load());
    last_store_error_.clear();
}

void JobStore::injectFaultForTest(StoreFaultMode mode, size_t skip, size_t count) {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    fault_mode_ = mode;
    fault_skip_ = skip;
    fault_count_ = count;
}

void JobStore::clearFaultsForTest() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    fault_mode_ = StoreFaultMode::None;
    fault_skip_ = 0;
    fault_count_ = 0;
}

void JobStore::init_id_counter() {
    if (!db_) {
        spdlog::warn("JobStore: db not running, using default next_id_ = 1");
        return;
    }

    try {
        const IdScan scan = scan_id_keys(db_);
        if (!scan.ok) {
            // Fail closed, the same rule db_is_empty applies: never guess an
            // id floor from an unreadable keyspace.
            spdlog::error("JobStore: could not scan the id keyspace; keeping the "
                          "allocator stopped");
            id_exhausted_ = true;
            next_id_ = 0;
            return;
        }
        const uint64_t counter_value = scan.counter;
        const uint64_t max_job_id = scan.max_row_id;
        if (scan.counter_malformed) {
            spdlog::error("JobStore: job_id counter is not a number; refusing to "
                          "reset the allocator");
        }

        uint64_t high_water = counter_value;
        if (max_job_id == std::numeric_limits<uint64_t>::max()) {
            high_water = max_job_id;
            id_exhausted_ = true;
        } else {
            high_water = std::max(counter_value, max_job_id + 1);
        }
        if (high_water == 0) high_water = 1;
        next_id_ = high_water;

        // The durable exhaustion marker (m:job_id_exhausted) decides whether
        // MAX is still available. The legacy layout stored only the counter,
        // where MAX is ambiguous: it may mean "MAX is the next free value" or
        // "MAX was already handed out". Without a marker the conservative
        // choice is to stop issuing IDs: reissuing a value that was already
        // published is worse than burning the last one.
        std::string marker_json;
        bool marker_present = false;
        bool marker_true = false;
        if (db_->Get(leveldb::ReadOptions(), key_meta("job_id_exhausted"), &marker_json).ok()) {
            marker_present = true;
            marker_true = marker_json == "true";
        }

        bool persist_marker = false;
        if (scan.counter_malformed) {
            id_exhausted_ = true;
            next_id_ = 0;
            persist_marker = true;
        } else if (marker_present && marker_true) {
            id_exhausted_ = true;
            next_id_ = 0;
        } else if (marker_present && max_job_id == std::numeric_limits<uint64_t>::max()) {
            // A false marker cannot override the evidence of an existing MAX
            // record: the value was issued, so the counter is exhausted.
            spdlog::error("JobStore: job_id_exhausted=false contradicts an existing "
                          "maximum JobID; keeping the allocator exhausted");
            id_exhausted_ = true;
            next_id_ = 0;
            persist_marker = true;
        } else if (!marker_present &&
                   (counter_value == std::numeric_limits<uint64_t>::max() || id_exhausted_)) {
            spdlog::warn("JobStore: legacy job_id counter {} carries no exhaustion marker; "
                         "treating the allocator as exhausted instead of reissuing MAX",
                         counter_value);
            id_exhausted_ = true;
            next_id_ = 0;
            persist_marker = true;
        }

        // 持久化当前值 (durable high-water reservation)
        leveldb::WriteBatch batch;
        batch.Put(key_counter("job_id"), std::to_string(next_id_));
        if (persist_marker) batch.Put(key_meta("job_id_exhausted"), "true");
        auto status = write_batch_locked(batch, "init job id counter");
        if (!status.ok()) {
            spdlog::error("JobStore: failed to persist job_id_counter: {}", status.ToString());
        }

        spdlog::info("JobStore: initialized job_id counter, next_id_ = {} (exhausted: {})",
                     next_id_, id_exhausted_);
    } catch (const std::exception& e) {
        spdlog::error("JobStore: failed to init job_id counter: {}", e.what());
        next_id_ = 1;
    }
}

uint64_t JobStore::allocateId(std::string& error) {
    // Every allocator consumer shares the same admission order as the registry
    // registration path: a standalone reservation can never land
    // between a batch's id reservation and that batch's commit, which would let
    // the batch write an older counter back and reissue a published id.
    std::lock_guard<std::mutex> id_lock(id_mtx_);
    if (id_exhausted_ || next_id_ == 0) {
        error = "id_exhausted";
        spdlog::error("JobStore: job id space is exhausted, refusing allocation");
        return 0;
    }

    const uint64_t id = next_id_;
    bool exhaust_now = false;
    if (id == std::numeric_limits<uint64_t>::max()) {
        id_exhausted_ = true;
        next_id_ = 0;
        exhaust_now = true;
    } else {
        next_id_ = id + 1;
    }

    // Reserve the id durably before it is handed out. An ambiguous outcome
    // still advances the in-memory high-water mark: ids are never reused. The
    // exhaustion marker rides in the same transaction so a restart cannot
    // conclude that MAX is still free.
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    leveldb::WriteBatch batch;
    batch.Put(key_counter("job_id"),
              std::to_string(next_id_ == 0 ? id : next_id_));
    if (exhaust_now) batch.Put(key_meta("job_id_exhausted"), "true");
    auto status = write_batch_locked(batch, "allocate job id");
    if (!status.ok()) {
        error = "store_write_failed";
        spdlog::error("JobStore: durable id allocation failed for id {}, id is burned", id);
        return 0;
    }
    return id;
}

bool JobStore::reserveId(uint64_t& id, uint64_t& durable_counter,
                         bool& durable_exhausted, std::string& error) {
    std::lock_guard<std::mutex> id_lock(id_mtx_);
    if (id_exhausted_ || next_id_ == 0) {
        error = "id_exhausted";
        spdlog::error("JobStore: job id space is exhausted, refusing allocation");
        return false;
    }
    durable_exhausted = false;
    id = next_id_;
    if (id == std::numeric_limits<uint64_t>::max()) {
        id_exhausted_ = true;
        next_id_ = 0;
        durable_counter = id;
        durable_exhausted = true;
    } else {
        next_id_ = id + 1;
        durable_counter = next_id_;
    }
    // The reservation rides the caller's create batch: the id, the record and
    // the advanced high-water mark commit together (or not at all). Whether
    // that batch is fsynced is the caller's choice — a create is written
    // without a fence and the maintenance checkpoint persists it later. An
    // ambiguous outcome still advances the in-memory high-water mark: a burned
    // id is never handed out twice.
    return true;
}


uint64_t JobStore::nextJobId() {
    if (!store_ready()) {
        spdlog::warn("JobStore: refusing JobID allocation before the job store is ready");
        return 0;
    }
    std::string error;
    const uint64_t job_id = allocateId(error);
    if (job_id == 0) {
        spdlog::error("JobStore: JobID allocation failed: {}", error);
        return 0;
    }
    return job_id;
}

// ---------------------------------------------------------------------------
// Durable identity resolution (the ambiguous-create repair read)
// ---------------------------------------------------------------------------

std::optional<JobRecord> JobStore::readRowByIdentity(const SchedulerKey& key) {
    if (!store_ready()) return std::nullopt;
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return std::nullopt;

    std::string value;
    const auto index_key = key_identity(key.index_key());
    auto status = db_->Get(leveldb::ReadOptions(), index_key, &value);
    if (status.IsNotFound()) return std::nullopt;
    if (!status.ok()) {
        record_store_failure("LevelDB read failed (identity lookup): " + status.ToString());
        return std::nullopt;
    }
    uint64_t job_id = 0;
    try {
        job_id = nlohmann::json::parse(value).value("job_id", uint64_t{0});
    } catch (const std::exception& error) {
        spdlog::warn("JobStore: unreadable identity index: {}", error.what());
        return std::nullopt;
    }
    if (job_id == 0) return std::nullopt;

    std::string raw;
    status = db_->Get(leveldb::ReadOptions(), key_job(job_id), &raw);
    if (status.IsNotFound()) {
        // A stale entry: its row is gone (batches delete row and index
        // atomically), so the next create's PutIdentity overwrites it.
        spdlog::warn("JobStore: identity index names JobID {} but its row is gone",
                     job_id);
        return std::nullopt;
    }
    if (!status.ok()) {
        record_store_failure("LevelDB read failed (record lookup): " + status.ToString());
        return std::nullopt;
    }
    try {
        auto record = parse_record(raw, job_id, options_.row_decoder);
        return record;
    } catch (const std::exception& error) {
        spdlog::warn("JobStore: unreadable record {}: {}", job_id, error.what());
        return std::nullopt;
    }
}

// ---------------------------------------------------------------------------
// The write primitive
// ---------------------------------------------------------------------------

bool JobStore::identity_owner_locked(const SchedulerKey& key, uint64_t& owner,
                                     std::string& error) {
    // Callers hold db_mtx_.
    owner = 0;
    std::string value;
    const auto status = db_->Get(leveldb::ReadOptions(),
                                 key_identity(key.index_key()), &value);
    if (status.IsNotFound()) return true;
    if (!status.ok()) {
        error = "store_error";
        record_store_failure("LevelDB read failed (identity index): " + status.ToString());
        return false;
    }
    try {
        owner = nlohmann::json::parse(value).value("job_id", uint64_t{0});
    } catch (const std::exception&) {
        owner = 0;   // unparsable entry: treat as absent, writes overwrite it
    }
    return true;
}

bool JobStore::apply_write_op_locked(const WriteOp& op, leveldb::WriteBatch& batch,
                                     bool& put_seen, std::string& error) {
    // Callers hold db_mtx_.
    switch (op.kind) {
        case WriteOp::PutRecord: {
            if (!op.record || op.job_id == 0) {
                error = "invalid_op";
                return false;
            }
            batch.Put(key_job(op.job_id), record_json(*op.record).dump());
            put_seen = true;
            return true;
        }
        case WriteOp::DeleteRecord: {
            batch.Delete(key_job(op.job_id));
            return true;
        }
        case WriteOp::PutIdentity: {
            if (!op.key) {
                error = "invalid_op";
                return false;
            }
            // Refuse a duplicate LIVE identity only: a different owner whose
            // row still exists. An entry whose row is gone (interrupted
            // archive, unparsable value) is stale and overwritten here.
            uint64_t owner = 0;
            if (!identity_owner_locked(*op.key, owner, error)) return false;
            if (owner != 0 && owner != op.job_id) {
                std::string raw;
                const auto row = db_->Get(leveldb::ReadOptions(), key_job(owner), &raw);
                if (!row.ok() && !row.IsNotFound()) {
                    error = "store_error";
                    record_store_failure("LevelDB read failed (identity owner row): " +
                                         row.ToString());
                    return false;
                }
                if (row.ok()) {
                    spdlog::error("JobStore: identity for JobID {} is already registered "
                                  "as JobID {}; refusing the write",
                                  op.job_id, owner);
                    error = "identity_conflict";
                    return false;
                }
                spdlog::warn("JobStore: overwriting a stale identity index entry that "
                             "named JobID {}", owner);
            }
            const SchedulerKey& identity = *op.key;
            batch.Put(key_identity(identity.index_key()), nlohmann::json{
                {"job_id", op.job_id},
                {"scheduler_type", identity.adapter},
                {"cluster_namespace", identity.cluster_namespace},
                {"job_native_id", identity.job_native_id},
                {"canonical_native", identity.canonical_native},
                {"step_id", identity.step_id},
            }.dump());
            put_seen = true;
            return true;
        }
        case WriteOp::DeleteIdentity: {
            if (!op.key) {
                error = "invalid_op";
                return false;
            }
            // Release only an entry this instance still owns on disk: a newer
            // execution may have claimed the identity by now.
            uint64_t owner = 0;
            if (!identity_owner_locked(*op.key, owner, error)) return false;
            if (owner == op.job_id) {
                batch.Delete(key_identity(op.key->index_key()));
            }
            return true;
        }
        case WriteOp::PutHistory: {
            if (!op.record || op.job_id == 0) {
                error = "invalid_op";
                return false;
            }
            int64_t end_time = now_us();
            if (!op.history_key_suffix.empty()) {
                end_time = static_cast<int64_t>(unhex64(op.history_key_suffix));
            }
            auto history = record_json(*op.record);
            history["state"] = to_string(RecordState::Closing);
            history["end_time_us"] = end_time;
            // The fact snapshot of what the collection runtime really had
            // attached when the registry accepted the removal, and how it
            // was established. The desired selection alone never decides this
            // row.
            history["applied_collectors"] = op.applied_collectors;
            history["applied_provenance"] = op.history_provenance;
            batch.Put(key_history(op.job_id, static_cast<uint64_t>(end_time)),
                      history.dump());
            put_seen = true;
            return true;
        }
        case WriteOp::ReserveCounter: {
            batch.Put(key_counter("job_id"), std::to_string(op.durable_counter));
            // MAX is handed out at most once, ever: the marker is committed
            // with the batch that consumes it.
            if (op.counter_exhausted) batch.Put(key_meta("job_id_exhausted"), "true");
            put_seen = true;
            return true;
        }
    }
    error = "invalid_op";
    return false;
}

bool JobStore::commit(std::vector<WriteOp> ops, bool durable,
                      std::vector<uint64_t>& touched, const char* reason,
                      std::string& error) {
    error.clear();
    if (ops.empty()) return true;   // nothing to make atomic
    if (!store_ready()) {
        error = "store_unavailable";
        return false;
    }
    if (recovery_required_.load()) {
        spdlog::error("JobStore: refusing '{}' while recovery is required", reason);
        error = "recovery_required";
        return false;
    }

    std::lock_guard<std::mutex> db_lock(db_mtx_);

    leveldb::WriteBatch batch;
    bool put_seen = false;
    std::vector<uint64_t> record_puts;
    for (const auto& op : ops) {
        if (!apply_write_op_locked(op, batch, put_seen, error)) return false;
        if (op.kind == WriteOp::PutRecord) record_puts.push_back(op.job_id);
    }
    // A batch of only deletes proves its own ambiguous outcome by the row's
    // absence; anything that writes a row needs the regular recovery fence.
    const bool delete_only = !put_seen;

    const auto status = write_batch_locked(batch, reason, durable,
                                           touched.empty() ? nullptr : &touched,
                                           delete_only);
    if (!status.ok()) {
        error = "store_write_failed";
        return false;
    }
    if (!durable) {
        // The row image lives in the registry: the dirty set records the
        // persistence-side obligation to fence it (mark_record_dirty runs
        // under the db_mtx_ this function still holds).
        for (const auto id : record_puts) mark_record_dirty(id);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

StoreHealth JobStore::health() const {
    StoreHealth store_health;
    store_health.db_running = db_running_ && db_ != nullptr;
    store_health.migration_complete = migration_complete_;
    store_health.migration_state = migration_state_;
    store_health.schema_version = schema_version_;
    store_health.db_uuid = db_uuid_;
    store_health.degraded = store_degraded_.load();
    store_health.recovery_required = recovery_required_.load();
    store_health.durable_commits = durable_commits_.load();
    store_health.sync_commits = sync_commits_.load();
    store_health.write_failures = store_write_failures_.load();
    store_health.corrupt_records = corrupt_records_;

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        store_health.last_write_error = last_store_error_;
        store_health.recovery_state = recovery_operation_;
        store_health.recovery_fence_entries = recovery_fence_entries_;
        store_health.dirty_records = dirty_records_.size();
    }
    // active_instances / incomplete_identities / quarantined_identities describe
    // the record map: the registry overlays them (it owns the records).
    return store_health;
}
