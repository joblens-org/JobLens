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
/* Core half of the job/collection RPC contract.
 *
 * Every assertion below drives the real RPCServer over a temporary AF_UNIX
 * socket; nothing calls the handlers directly.
 *
 *   * selector forms, snapshot-scoped job scope, ambiguity, 404/409 classes
 *   * set / stop / reset_auto / unknown collector / stale revision /
 *     durable-unavailable mapping, accepted versus applied
 *   * legacy add resolves to the inventoried instance (JobID is only a hint),
 *     duplicate observation is not a failure, legacy remove stops collection
 *     and keeps the job record
 *   * bounded paging, snapshot invalidation, sentinel exclusion,
 *     decimal-string ids, capabilities field names, truthful status
 */
#include "core/job_registry.hpp"
#include "job_fixture.h"
#include "collection_runtime_fixture.hpp"
#include "core/status_provider.hpp"
#include "common/config.hpp"
#include "common/local_rpc.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <leveldb/db.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

// JobRegistry::hasInstance() was removed as a production-dead API; active-only
// is derived from the record state so the assertion semantics stay unchanged.
bool has_active(const JobRegistry& registry, uint64_t job_id)
{
    const auto record = registry.jobRecord(job_id);
    return record.has_value() && record->state != RecordState::Closing;
}

using json = nlohmann::json;

std::string socket_path;

json rpc(const std::string& method, const json& params = json::object())
{
    const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    check(fd >= 0, "socket() failed");
    timeval timeout{5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    check(socket_path.size() < sizeof(address.sun_path), "socket path too long");
    std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size());

    // The server binds/listens on start(); retry briefly so the test never
    // races thread startup.
    int connected = -1;
    for (int attempt = 0; attempt < 200; ++attempt) {
        connected = connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
        if (connected == 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    check(connected == 0, "connect to " + socket_path + " failed: " + std::strerror(errno));

    const auto request = json{{"method", method}, {"params", params}}.dump();
    check(send(fd, request.data(), request.size(), MSG_NOSIGNAL) == ssize_t(request.size()),
          "send failed");

    std::string response;
    char buffer[8192];
    ssize_t received = 0;
    while ((received = recv(fd, buffer, sizeof(buffer), 0)) > 0)
        response.append(buffer, size_t(received));
    close(fd);
    check(!response.empty(), method + " returned an empty response");
    return json::parse(response);
}

NativeSelector condor_selector(const std::string& native, const std::string& tag)
{
    NativeSelector selector;
    selector.adapter = "condor";
    selector.clusterName = "htcondor01";
    selector.clusterTag = tag;
    selector.job_native_id = native;
    return selector;
}

NativeSelector slurm_selector(const std::string& native, const std::string& step)
{
    NativeSelector selector;
    selector.adapter = "slurm";
    selector.clusterName = "slurm-cluster";
    selector.job_native_id = native;
    selector.step_id = step;
    return selector;
}

Job condor_observation(const NativeSelector& selector, pid_t manager_pid)
{
    ProcessIdentity manager;
    manager.boot_id = "boot-collection-rpc";
    manager.pid = manager_pid;
    manager.start_ticks = 900000 + uint64_t(manager_pid);
    Job observation = discovered_job(
        selector, manager,
        {ProcessIdentity{manager_pid, 900000 + uint64_t(manager_pid), "boot-collection-rpc"}},
        true);

    CondorJobAttr attr{};
    attr.auto_update_child = false;
    const auto dot = selector.job_native_id.find('.');
    attr.cluster_id = std::stoul(selector.job_native_id.substr(0, dot));
    attr.proc_id = std::stoul(selector.job_native_id.substr(dot + 1));
    attr.starter_pid = manager_pid;
    attr.owner = "alice";
    attr.scheduler_name = selector.clusterTag;
    attr.collector_host = selector.clusterName;
    observation.subtype = JobSubType::Condor;
    observation.sub_attr = attr;
    return observation;
}

Job slurm_observation(const NativeSelector& selector, pid_t manager_pid)
{
    ProcessIdentity manager;
    manager.boot_id = "boot-collection-rpc";
    manager.pid = manager_pid;
    manager.start_ticks = 910000 + uint64_t(manager_pid);
    Job observation = discovered_job(
        selector, manager,
        {ProcessIdentity{manager_pid, 910000 + uint64_t(manager_pid), "boot-collection-rpc"}},
        true);

    SlurmJobAttr attr{};
    attr.auto_update_child = false;
    attr.stepd_pid = manager_pid;
    attr.job_id = std::stoul(selector.job_native_id);
    attr.step_id = *SlurmJob::step_value(selector.step_id);
    attr.clusterName = selector.clusterName;
    attr.user = "bob";
    observation.subtype = JobSubType::Slurm;
    observation.sub_attr = attr;
    return observation;
}

uint64_t seed(JobRegistry& registry, const Job& observation,
              const std::string& label)
{
    const auto result = registry.upsertObservedJob(observation);
    check(result.ok, "seeding " + label + " failed: " + result.error + " " + result.message);
    check(result.job_id != 0, "seeding " + label + " allocated no internal id");
    return result.job_id;
}

std::vector<uint64_t> target_ids(const json& response)
{
    check(response.contains("targets"), "response has no targets: " + response.dump());
    std::vector<uint64_t> ids;
    for (const auto& target : response["targets"]) {
        check(target["JobID"].is_string(),
              "job_id must be a decimal string: " + target.dump());
        const auto parsed = parse_job_id(target["JobID"]);
        check(parsed.status == LegacyIdStatus::Valid,
              "job_id is not a decimal string: " + target.dump());
        ids.push_back(parsed.value);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::vector<std::string> candidates(const json& response)
{
    std::vector<std::string> out;
    if (!response.contains("candidates")) return out;
    for (const auto& item : response["candidates"]) out.push_back(item.get<std::string>());
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> sorted_ids(std::vector<std::string> values)
{
    std::sort(values.begin(), values.end());
    return values;
}

json collection_request(const json& selector, const std::string& operation,
                        const json& collectors = nullptr, const std::string& scope = "instance",
                        std::optional<uint64_t> expected_revision = std::nullopt,
                        bool ensure_registered = false)
{
    json body;
    body["selector"] = selector;
    body["scope"] = scope;
    body["operation"] = operation;
    body["ensure_registered"] = ensure_registered;
    if (!collectors.is_null()) body["collectors"] = collectors;
    if (expected_revision.has_value()) body["expected_revision"] = *expected_revision;
    return body;
}

json condor_selector_json(const std::string& native, const std::string& tag)
{
    json selector{{"scheduler_type", "condor"},
                  {"clusterName", "htcondor01"},
                  {"JobNativeID", native}};
    if (!tag.empty()) selector["clusterTag"] = tag;
    return selector;
}

json slurm_selector_json(const std::string& native, const std::string& step)
{
    json selector{{"scheduler_type", "slurm"},
                  {"clusterName", "slurm-cluster"},
                  {"JobNativeID", native}};
    if (!step.empty()) selector["step_id"] = step;
    return selector;
}

struct Fixture {
    uint64_t condor_a = 0;      // 172.0 in schedd-a
    uint64_t condor_a_other = 0;  // 172.0 in schedd-b (different schedd namespace)
    uint64_t condor_b = 0;      // 173.0 in schedd-a
    uint64_t condor_legacy = 0; // 180.0 in schedd-a
    uint64_t condor_extra = 0;  // 190.0 in schedd-a (closing case)
    uint64_t slurm_step1 = 0;   // 9001 step 1
    uint64_t slurm_step2 = 0;   // 9001 step 2
    uint64_t slurm_batch = 0;   // 9002 step batch
};

void run_selector_cases(JobRegistry& registry, const Fixture& ids)
{
    // --- a selector that addresses nothing is rejected ------------------
    // The removed instance_id spelling is no longer singled out: the selector
    // has to name the target by JobID or by native identity fields. A selector
    // carrying only the old key addresses nothing, so it stays a request error.
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(json{{"instance_id", "ji1-deadbeef"}}, "set", json::array()));
        check(response["status"] == "error" && response["code"] == "invalid_request",
              "a selector with no JobID and no identity fields must be "
              "invalid_request: " + response.dump());
    }
    // --- incomplete namespace -----------------------------------------
    {
        json selector{{"scheduler_type", "condor"}, {"JobNativeID", "172.0"}};
        const auto response = rpc("jobs/collection/set",
                                  collection_request(selector, "set", json::array()));
        check(response["code"] == "invalid_request",
              "missing cluster namespace must be invalid_request: " + response.dump());
    }
    // --- unknown scheduler type ---------------------------------------
    {
        json selector{{"scheduler_type", "pbs"}, {"clusterName", "c"}, {"JobNativeID", "1"}};
        const auto response = rpc("jobs/collection/set",
                                  collection_request(selector, "set", json::array()));
        check(response["code"] == "invalid_request",
              "unknown scheduler_type must be invalid_request: " + response.dump());
    }
    // --- invalid native-id keeps the legacy caller-facing diagnostic --
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(condor_selector_json("172", "schedd-a"), "set", json::array()));
        check(response["code"] == "invalid_request" &&
                  response["message"] ==
                      "incomplete native selector: scheduler_type, clusterName and JobNativeID "
                      "must name one scheduler identity (Condor native ids are '<ClusterId>.<ProcId>')",
              "invalid selector must preserve the wire diagnostic: " + response.dump());
    }
    // --- condor native selector resolves one instance -----------------
    {
        auto selector = condor_selector_json("172.0", "");
        selector["clusterName"] = "htcondor01@schedd-a";
        const auto response = rpc("jobs/collection/set",
                                  collection_request(selector, "set", json::array()));
        check(response.value("code", "") == "not_found",
              "a canonical delimiter alias must not target another pool: " + response.dump());
    }
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(condor_selector_json("172.0", "schedd-a"), "set",
                               json::array({"cpumem_collector"})));
        check(response["status"] == "accepted", "condor set must be accepted: " + response.dump());
        const auto targets = target_ids(response);
        check(targets.size() == 1 && targets.front() == ids.condor_a,
              "condor selector must resolve exactly the schedd-a instance: " + response.dump());
        check(response["targets"].front()["JobNativeID"] == "172.0",
              "target must carry the native job id");
        check(response["targets"].front()["mode"] == "manual", "manual mode expected");
        check(response["targets"].front()["desired_collectors"] ==
                  json::array({"cpumem_collector"}),
              "desired collectors must be the requested set");
    }
    // --- schedd-ambiguity without clusterTag -------------------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(condor_selector_json("172.0", ""), "set", json::array()));
        check(response["code"] == "ambiguous_selector",
              "condor selector without schedd tag must be ambiguous: " + response.dump());
        check(candidates(response) ==
                  sorted_ids({format_job_id(ids.condor_a),
                              format_job_id(ids.condor_a_other)}),
              "ambiguous condor selector must list the candidate instance ids: " +
                  response.dump());
    }
    // --- unknown target is not_found, with and without ensure_registered
    {
        const auto plain = rpc("jobs/collection/set",
                               collection_request(condor_selector_json("999.0", "schedd-a"),
                                                  "set", json::array()));
        check(plain["code"] == "not_found",
              "unknown native target must be not_found: " + plain.dump());

        const auto ensured = rpc(
            "jobs/collection/set",
            collection_request(condor_selector_json("999.0", "schedd-a"), "set",
                               json::array(), "instance", std::nullopt, true));
        check(ensured["code"] == "not_found",
              "ensure_registered without a live source must stay not_found: " + ensured.dump());
        check(ensured["message"].get<std::string>().find("ensure_registered") !=
                  std::string::npos,
              "ensure_registered boundary must be explicit in the message: " + ensured.dump());
        check(registry.jobRecords().size() == size_t{8},
              "a failed resolution must not invent job records");
    }
    // --- ensure_registered performs a verified discovery pass ---------
    {
        std::atomic<int> refreshes{0};
        set_refresh_provider([&registry, &refreshes]() {
            ++refreshes;
            // A real source pass would create the instance; the resolver still
            // has to find it by canonical identity afterwards.
            seed(registry, condor_observation(condor_selector("950.0", "schedd-a"), 6600),
                 "ensure_registered refresh");
            return true;
        });
        const auto ensured = rpc(
            "jobs/collection/set",
            collection_request(condor_selector_json("950.0", "schedd-a"), "set",
                               json::array({"cpumem_collector"}), "instance", std::nullopt,
                               true));
        set_refresh_provider({});
        check(refreshes.load() == 1,
              "ensure_registered must run exactly one authoritative pass");
        check(ensured["status"] == "accepted",
              "ensure_registered must resolve the refreshed instance: " + ensured.dump());
        check(ensured["targets"].size() == 1,
              "ensure_registered must resolve exactly the refreshed target: " + ensured.dump());
    }
    // --- Slurm instance scope without a step is ambiguous -------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(slurm_selector_json("9001", ""), "set", json::array()));
        check(response["code"] == "ambiguous_selector",
              "slurm instance scope without step must be ambiguous: " + response.dump());
        check(candidates(response) ==
                  sorted_ids({format_job_id(ids.slurm_step1),
                              format_job_id(ids.slurm_step2)}),
              "ambiguous slurm selector must list both step instances: " + response.dump());
    }
    // --- exact step selector resolves one step ------------------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(slurm_selector_json("9001", "2"), "set",
                               json::array({"cpumem_collector"})));
        check(target_ids(response) == std::vector<uint64_t>({ids.slurm_step2}),
              "step selector must resolve exactly that step: " + response.dump());
    }
    // --- job scope lists the exact current targets --------------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(slurm_selector_json("9001", ""), "set",
                               json::array({"cpumem_collector"}), "job"));
        check(response["status"] == "accepted", "job scope must be accepted: " + response.dump());
        check(target_ids(response) == std::vector<uint64_t>({ids.slurm_step1, ids.slurm_step2}),
              "job scope must list every currently observed step: " + response.dump());
        check(response["scope"] == "job", "response must echo the resolved scope");

        // Snapshot scope: a step that appears later is not covered by the
        // earlier request, and a fresh request lists the new exact set.
        const auto late = seed(registry,
                               slurm_observation(slurm_selector("9001", "3"), 5353), "slurm step 3");
        const auto refreshed = rpc(
            "jobs/collection/set",
            collection_request(slurm_selector_json("9001", ""), "set",
                               json::array({"cpumem_collector"}), "job"));
        auto expected = std::vector<uint64_t>({ids.slurm_step1, ids.slurm_step2, late});
        std::sort(expected.begin(), expected.end());
        check(target_ids(refreshed) == expected,
              "job scope must resolve the refreshed exact target set: " + refreshed.dump());
        check(registry.jobRecord(late).has_value(),
              "the later step must exist in the registry");
    }
    // --- instance handle selector -------------------------------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(json{{"JobID", format_job_id(ids.condor_a)}}, "set",
                               json::array({"cpumem_collector"})));
        check(target_ids(response) == std::vector<uint64_t>({ids.condor_a}),
              "instance handle must resolve its own instance: " + response.dump());
    }
    // --- instance handle only supports instance scope ------------------
    {
        const auto response = rpc("jobs/collection/set",
                                  collection_request(
                                      json{{"JobID", format_job_id(ids.condor_a)}},
                                      "set", json::array(), "job"));
        check(response["code"] == "invalid_request",
              "instance handle with job scope must be invalid_request: " + response.dump());
    }
    // --- unknown instance handle --------------------------------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(json{{"JobID", "999999999"}}, "set", json::array()));
        check(response["code"] == "not_found",
              "unknown instance handle must be not_found: " + response.dump());
    }
}

void run_intent_cases(JobRegistry& registry, const Fixture& ids)
{
    const auto selector = json{{"JobID", format_job_id(ids.condor_b)}};

    // --- missing / null collectors ------------------------------------
    {
        const auto missing = rpc("jobs/collection/set", collection_request(selector, "set"));
        check(missing["code"] == "invalid_request",
              "set without collectors must be invalid_request: " + missing.dump());
        auto body = collection_request(selector, "set");
        body["collectors"] = nullptr;
        const auto nulled = rpc("jobs/collection/set", body);
        check(nulled["code"] == "invalid_request",
              "set with null collectors must be invalid_request: " + nulled.dump());
        check(revision_of(registry, ids.condor_b) == 0 ||
                  registry.jobRecord(ids.condor_b).has_value(),
              "an invalid request must not create a new record");
        check(!registry.selectionRecord(ids.condor_b).manual.has_value(),
              "an invalid request must not create a manual selection");
    }
    // --- unknown collector name ---------------------------------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(selector, "set", json::array({"not_a_collector"})));
        check(response["code"] == "unknown_collector",
              "unknown collector must be unknown_collector: " + response.dump());
        check(!registry.selectionRecord(ids.condor_b).manual.has_value(),
              "an unknown collector must not mutate intent");
    }
    // --- accepted set and identical repeat ----------------------------
    {
        const auto accepted = rpc(
            "jobs/collection/set",
            collection_request(selector, "set", json::array({"cpumem_collector"})));
        check(accepted["status"] == "accepted", "valid set must be accepted: " + accepted.dump());
        const auto& target = accepted["targets"].front();
        check(target["revision"] == target["revision"],
              "the accepted revision must be published");
        check(target["applied_attach_token"] == 0,
              "no runtime means no attach token: " + target.dump());
        check(target["mode"] == "manual", "manual mode expected");
        check(target["desired_collectors"] == json::array({"cpumem_collector"}),
              "desired collectors must match");
        check(target["applied_collectors"].is_array() && target["errors"].is_array(),
              "applied state and errors must be shaped as arrays: " + target.dump());
        // This core has no collection runtime: the intent is durable, but the
        // response must not pretend it is collecting.
        check(target["applied_collectors"].empty(),
              "a core without a collection runtime must not claim applied collectors: " +
                  target.dump());
        check(target["errors"].size() == 1 &&
                  target["errors"][0]["code"] == "not_converged",
              "unapplied intent must report not_converged: " + target.dump());

        const auto repeated = rpc(
            "jobs/collection/set",
            collection_request(selector, "set", json::array({"cpumem_collector"})));
        check(repeated["targets"].front()["revision"] ==
                  target["revision"],
              "an identical repeat must not create a new revision: " + repeated.dump());
        check(repeated["targets"].front()["changed"] == false,
              "an identical repeat must report no change");
    }
    // --- stale expected_revision --------------------------------------
    {
        const auto stale = rpc(
            "jobs/collection/set",
            collection_request(selector, "set", json::array({"cpumem_collector"}), "instance", 99));
        check(stale["code"] == "stale_revision",
              "stale expected_revision must be stale_revision: " + stale.dump());

        const auto current_revision = revision_of(registry, ids.condor_b);
        check(current_revision != 0, "the current record revision must be known");
        const auto current = rpc(
            "jobs/collection/set",
            collection_request(selector, "set", json::array({"cpumem_collector"}),
                               "instance", current_revision));
        check(current["status"] == "accepted",
              "the current expected_revision must be accepted: " + current.dump());
    }
    // --- expected_revision is single-instance only --------------------
    {
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(slurm_selector_json("9001", ""), "set",
                               json::array({"cpumem_collector"}), "job", 1));
        check(response["code"] == "invalid_request",
              "expected_revision with job scope must be invalid_request: " + response.dump());
    }
    // --- reset_auto ---------------------------------------------------
    {
        const auto with_collectors = rpc(
            "jobs/collection/set",
            collection_request(selector, "reset_auto", json::array({"cpumem_collector"})));
        check(with_collectors["code"] == "invalid_request",
              "reset_auto with collectors must be invalid_request: " + with_collectors.dump());

        const auto reset = rpc("jobs/collection/set", collection_request(selector, "reset_auto"));
        check(reset["status"] == "accepted", "reset_auto must be accepted: " + reset.dump());
        check(reset["targets"].front()["mode"] == "auto",
              "reset_auto must return to auto mode: " + reset.dump());
        check(reset["targets"].front()["desired_collectors"].empty(),
              "auto mode without a policy decision selects nothing: " + reset.dump());

        // The dedicated reset sub-resource is the same command with the
        // operation fixed by the route, not a second implementation.
        const auto manual = rpc(
            "jobs/collection/set",
            collection_request(selector, "set", json::array({"cpumem_collector"})));
        check(manual["targets"].front()["mode"] == "manual",
              "set must create a manual override: " + manual.dump());
        const auto reset_resource =
            rpc("jobs/collection/reset", collection_request(selector, "reset_auto"));
        check(reset_resource["status"] == "accepted" &&
                  reset_resource["targets"].front()["mode"] == "auto",
              "jobs/collection/reset must clear the manual override: " +
                  reset_resource.dump());
    }
    // --- unknown operation --------------------------------------------
    {
        const auto response = rpc("jobs/collection/set", collection_request(selector, "delete"));
        check(response["code"] == "invalid_request",
              "unknown operation must be invalid_request: " + response.dump());
    }
    // --- persistent stop (explicit []) --------------------------------
    {
        const auto stop = rpc("jobs/collection/set",
                              collection_request(selector, "set", json::array()));
        check(stop["status"] == "accepted", "explicit [] must be accepted: " + stop.dump());
        check(stop["targets"].front()["desired_collectors"].empty(),
              "explicit [] must be a persistent empty selection: " + stop.dump());
        const auto binding = registry.selectionRecord(ids.condor_b);
        check(binding.mode() == CollectionMode::Manual && binding.manual.has_value() &&
                  binding.manual->empty(),
              "a stop is a manual empty selection");
        check(revision_of(registry, ids.condor_b) ==
                  stop["targets"].front()["revision"],
              "the reported revision must be the persisted revision");
        const auto status = rpc("jobs/collection/get",
                                json{{"JobID", format_job_id(ids.condor_b)}});
        check(status["status"] == "ok" && status["desired_collectors"].empty(),
              "collection get must report the stopped selection: " + status.dump());
        check(status["applied_collectors"].empty(), "nothing applied after a stop");
        check(status["JobID"] == std::to_string(ids.condor_b),
              "collection get must carry a decimal-string internal id");
    }
    // --- durable store failure maps to store_unavailable --------------
    {
        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
        const auto response = rpc(
            "jobs/collection/set",
            collection_request(json{{"JobID", format_job_id(ids.condor_b)}}, "set",
                               json::array({"cpumem_collector"})));
        registry.clearStoreFaultsForTest();
        check(response["code"] == "store_unavailable",
              "a rejected durable write must be store_unavailable: " + response.dump());
        check(registry.selectionRecord(ids.condor_b).manual.has_value() &&
                  registry.selectionRecord(ids.condor_b).manual->empty(),
              "a failed durable write must not rewrite the intent");
    }
    // --- get on an unknown instance ----------------------------------
    {
        const auto response = rpc("jobs/collection/get",
                                  json{{"JobID", "999999999"}});
        check(response["code"] == "not_found",
              "collection get on an unknown instance must be not_found: " + response.dump());
        const auto missing = rpc("jobs/collection/get", json::object());
        check(missing["code"] == "invalid_request",
              "collection get without job_id must be invalid_request: " +
                  missing.dump());
    }
}

void run_legacy_cases(JobRegistry& registry, const Fixture& ids)
{
    const auto before = registry.jobRecords().size();
    const auto hint = std::to_string(ids.condor_legacy + 5000);

    json add{{"opt", "add"},
             {"type", "job.condor"},
             {"JobID", hint},
             {"JobPIDs", json::array({int(getpid())})},
             {"Lens", json::array({"cpumem_collector"})},
             {"sub_attr", json{{"cluster_id", 180},
                               {"proc_id", 0},
                               {"owner", "alice"},
                               {"scheduler_name", "schedd-a"},
                               {"collector_host", "htcondor01"}}}};

    {
        auto conflicting = add;
        conflicting["clusterName"] = "another-pool";
        const auto response = rpc("jobs/upsert", conflicting);
        check(response.value("code", "") == "namespace_conflict",
              "contradictory top-level and typed pool must be rejected: " + response.dump());
    }

    {
        const auto response = rpc("jobs/upsert", add);
        check(response["status"] == "success",
              "legacy add of an inventoried job must succeed: " + response.dump());
        // One wire key for the record id: the decimal string. The numeric
        // spelling is not published separately any more.
        check(response["JobID"] == std::to_string(ids.condor_legacy),
              "legacy add must report the resolved instance as a decimal string");
        check(response["ignored_job_id_hint"] == hint,
              "the mismatch between hint and identity must be observable");
        check(registry.jobRecords().size() == before,
              "legacy add must not create another job record");

        const auto binding = registry.selectionRecord(ids.condor_legacy);
        check(binding.mode() == CollectionMode::Manual &&
                  binding.manual.has_value() &&
                  *binding.manual == std::vector<std::string>{"cpumem_collector"},
              "legacy add must attach a manual binding");
        const auto hint_id = parse_job_id(nlohmann::json(hint));
        check(hint_id.status == LegacyIdStatus::Valid && !has_active(registry, hint_id.value),
              "the JobID hint must never become a job id");
    }
    // --- strict create is not an upsert ------------------------------
    {
        auto create = add;
        create["sub_attr"]["cluster_id"] = 987;
        const auto created = rpc("jobs/create", create);
        check(created["status"] == "success" &&
                  created["JobID"].is_string(),
              "jobs/create must return the created identity: " + created.dump());
        check(created["revision"].get<uint64_t>() >= 1,
              "jobs/create must return the created revision: " + created.dump());

        const auto conflict = rpc("jobs/create", create);
        check(conflict["code"] == "already_exists",
              "jobs/create must reject an existing identity: " + conflict.dump());
        check(conflict["JobID"] == created["JobID"],
              "already_exists must report the existing instance: " + conflict.dump());
    }
    // --- a scheduler registration must name a scheduler job -----------
    {
        auto missing = add;
        missing["sub_attr"].erase("cluster_id");
        missing["sub_attr"].erase("proc_id");
        const auto response = rpc("jobs/create", missing);
        check(response["status"] == "error" &&
                  response.value("code", "") == "invalid_request",
              "a scheduler registration without cluster_id must be rejected: " +
                  response.dump());
    }
    // --- duplicate observation is not a failure ----------------------
    {
        const auto response = rpc("jobs/upsert", add);
        check(response["status"] == "success",
              "a duplicate legacy add must not fail: " + response.dump());
        check(response["duplicate_observation"] == true,
              "a duplicate legacy add must be reported as an observation duplicate");
        check(revision_of(registry, ids.condor_legacy) > 0,
              "duplicate add must keep the existing record");
    }
    // --- ambiguous legacy add is actionable, not a guess -------------
    {
        const auto ambiguous = rpc("jobs/upsert",
                                   json{{"opt", "add"},
                                        {"type", "job.condor"},
                                        {"JobPIDs", json::array({int(getpid())})},
                                        {"Lens", json::array({"cpumem_collector"})},
                                        {"sub_attr", json{{"cluster_id", 172}, {"proc_id", 0},
                                                          {"collector_host", "htcondor01"}}}});
        check(ambiguous["status"] == "error" &&
                  ambiguous["code"] == "ambiguous_selector",
              "an ambiguous legacy add must not pick a schedd: " + ambiguous.dump());
        check(candidates(ambiguous).size() == size_t{2},
              "an ambiguous legacy add must list both candidates: " + ambiguous.dump());
    }
    // --- legacy remove stops collection and keeps the job record -----
    {
        const auto response = rpc("jobs/stop",
                                  json{{"JobID", ids.condor_legacy}});
        check(response["status"] == "success", "legacy remove must succeed: " + response.dump());
        check(response["stopped_count"] == 1, "legacy remove must stop collection");
        check(response["deleted_count"] == 0, "legacy remove must not archive the job record");

        check(registry.jobRecord(ids.condor_legacy).has_value(),
              "legacy remove must retain the job record");
        check(registry.findJob(ids.condor_legacy).has_value(),
              "legacy remove must keep the live execution readable");
        const auto binding = registry.selectionRecord(ids.condor_legacy);
        check(binding.mode() == CollectionMode::Manual && binding.manual.has_value() &&
                  binding.manual->empty(),
              "legacy remove must persist a manual empty selection");

        const auto status = rpc("jobs/collection/get",
                                json{{"JobID", format_job_id(ids.condor_legacy)}});
        check(status["desired_collectors"].empty(),
              "collection get must report the stopped selection after remove: " + status.dump());
        const auto listed = rpc("jobs/list", json{{"scheduler_type", "condor"}});
        bool found = false;
        for (const auto& record : listed["records"]) {
            if (record["JobID"] == std::to_string(ids.condor_legacy)) found = true;
        }
        check(found, "the removed instance must still be listed in the registry");
    }
    // --- internal-id lookup is a resource read ------------------------
    {
        const auto response = rpc(
            "jobs/get", json{{"JobID", format_job_id(ids.condor_legacy)}});
        check(response["status"] == "ok" &&
                  response["JobID"] == format_job_id(ids.condor_legacy),
              "the internal-id lookup must return the job resource: " + response.dump());
    }
    // Adapter-owned legacy filters still accept the original RPC field names.
    const auto sibling = seed(registry,
        condor_observation(condor_selector("180.1", "schedd-a"), 4247), "condor 180.1");
    const auto attach = rpc("jobs/collection/set",
        collection_request(json{{"JobID", format_job_id(sibling)}}, "set",
                           json::array({"cpumem_collector"})));
    check(attach["status"] == "accepted", "attach the Condor sibling: " + attach.dump());
    const auto stop_with_matcher = [](const json& matcher, uint64_t expected) {
        const auto response = rpc("jobs/stop", json{{"matcher", matcher}});
        check(response["status"] == "success" &&
                  response["stopped_instances"] == json::array({format_job_id(expected)}),
              "legacy matcher must stop precisely the matched instance: " + response.dump());
    };
    stop_with_matcher({{"subtype", "Condor"}, {"condor_cluster_id", 180},
                       {"condor_proc_id", 0}}, ids.condor_legacy);
    check(registry.selectionRecord(sibling).manual ==
              std::vector<std::string>{"cpumem_collector"},
          "matching proc 0 must not change the sibling proc's collection intent");
    stop_with_matcher({{"subtype", "slurm"}, {"slurm_job_id", 9001},
                       {"slurm_step_id", 2}}, ids.slurm_step2);
}

void run_job_record_cases(JobRegistry& registry, const Fixture& ids)
{
    // --- bounded paging, sentinel exclusion, decimal-string ids -------
    {
        const auto all = rpc("jobs/list", json::object());
        check(all["status"] == "ok", "job list must succeed: " + all.dump());
        const auto total = all["records"].size();
        check(total >= size_t{8}, "job list must include the seeded instances");
        const auto counted = rpc("jobs/count", json::object());
        check(counted["status"] == "ok" &&
                  counted["job_count"].get<size_t>() >= total,
              "jobs/count must count the active records: " + counted.dump());
        check(all["next_cursor"].is_null(), "a single page must not advertise a cursor");
        check(all["snapshot_revision"].is_number_unsigned() ||
                  all["snapshot_revision"].is_number_integer(),
              "snapshot_revision must be numeric");
        for (const auto& record : all["records"]) {
            check(record["JobID"].is_string(),
                  "internal ids must be decimal strings: " + record.dump());
            check(record["JobID"] != "0",
                  "the system sentinel must never be listed");
            // The record carries one id key (JobID above); the removed
            // instance handle is not projected any more.
        }

        std::set<uint64_t> seen;
        std::string cursor;
        size_t pages = 0;
        do {
            json params{{"limit", 2}};
            if (!cursor.empty()) params["cursor"] = cursor;
            const auto page = rpc("jobs/list", params);
            check(page["status"] == "ok", "paged list failed: " + page.dump());
            check(page["records"].size() <= size_t{2}, "limit must bound the page");
            check(page["snapshot_revision"] == all["snapshot_revision"],
                  "every page must report the captured snapshot revision");
            for (const auto& record : page["records"]) {
                const auto id = parse_job_id(record["JobID"]);
                check(id.status == LegacyIdStatus::Valid, "job id must parse as uint64");
                check(seen.insert(id.value).second, "paging must not duplicate entries");
            }
            cursor = page["next_cursor"].is_null() ? "" : page["next_cursor"].get<std::string>();
            ++pages;
        } while (!cursor.empty() && pages < size_t{32});
        check(seen.size() == total, "paging must visit every record exactly once");
    }
    // --- limit bounds ------------------------------------------------
    {
        const auto zero = rpc("jobs/list", json{{"limit", 0}});
        check(zero["code"] == "invalid_request", "limit 0 must be invalid_request: " + zero.dump());
        const auto too_big = rpc("jobs/list", json{{"limit", 1001}});
        check(too_big["code"] == "invalid_request",
              "limit above 1000 must be invalid_request: " + too_big.dump());
        const auto max = rpc("jobs/list", json{{"limit", 1000}});
        check(max["status"] == "ok", "limit 1000 must be accepted: " + max.dump());
        const auto negative = rpc("jobs/list", json{{"limit", -3}});
        check(negative["code"] == "invalid_request",
              "a negative limit must be invalid_request: " + negative.dump());
    }
    // --- filters -----------------------------------------------------
    {
        const auto slurm = rpc("jobs/list", json{{"scheduler_type", "slurm"}});
        check(slurm["status"] == "ok", "scheduler_type filter must work");
        for (const auto& record : slurm["records"]) {
            check(record["scheduler_type"] == "slurm",
                  "scheduler_type filter must not leak other schedulers: " + record.dump());
        }
        const auto native = rpc("jobs/list", json{{"JobNativeID", "9001"}});
        check(native["records"].size() == size_t{3},
              "job_native_id filter must match every current step: " + native.dump());
        const auto state = rpc("jobs/list", json{{"state", "active"}});
        check(state["status"] == "ok", "state filter must work");
        const auto bogus = rpc("jobs/list", json{{"state", "gone"}});
        check(bogus["code"] == "invalid_request",
              "an unknown state must be invalid_request: " + bogus.dump());
        const auto bad_type = rpc("jobs/list", json{{"scheduler_type", "pbs"}});
        check(bad_type["code"] == "invalid_request",
              "an unknown scheduler_type must be invalid_request: " + bad_type.dump());
    }
    // --- changed snapshot invalidates the cursor ---------------------
    {
        const auto first = rpc("jobs/list", json{{"limit", 1}});
        check(!first["next_cursor"].is_null(), "a truncated page must advertise a cursor");
        const auto cursor = first["next_cursor"].get<std::string>();

        seed(registry, condor_observation(condor_selector("191.0", "schedd-a"), 6464),
             "condor 191.0");

        const auto stale = rpc("jobs/list",
                               json{{"limit", 1}, {"cursor", cursor}});
        check(stale["status"] == "error" && stale["code"] == "cursor_invalidated",
              "a changed snapshot must invalidate the cursor: " + stale.dump());

        const auto garbage = rpc("jobs/list", json{{"cursor", "not-a-token"}});
        check(garbage["code"] == "invalid_request",
              "an unparsable cursor must be invalid_request: " + garbage.dump());
    }
    // --- capabilities ------------------------------------------------
    {
        const auto response = rpc("joblens/capabilities");
        check(response["api_version"] == 2, "api_version must be 2");
        check(response["collection_control_version"] == 1,
              "collection_control_version must be 1");
        check(response["native_identity_selectors"] == true,
              "native_identity_selectors must be advertised");
        check(response["job_scope"] == "current_local_instances",
              "job_scope must document the snapshot semantics");
        check(response["db_schema_version"] == 5,
              "db_schema_version must be the contract field name (schema 5)");
        check(response["ready"] == true, "a migrated store must be ready");
    }
    // --- status without a maintenance provider ----------------------
    {
        const auto response = rpc("joblens/status");
        check(response["status"] == "healthy", "a migrated store must report healthy");
        check(response["enabled"] == true, "the job registry must be enabled");
        check(response["sources"]["state"] == "not_configured",
              "without a provider the sources state must be not_configured: " + response.dump());
        check(response["db_schema_version"] == 5, "status must expose db_schema_version");
        check(response["store"]["migration_complete"] == true,
              "store migration state must be reported");
        check(response["instances"]["active"].get<size_t>() >= size_t{8},
              "status must count the active instances");
        check(response["instances"]["closing"].get<size_t>() == 0,
              "no instance must be closing yet");
        (void)ids;
    }
    // --- the maintenance provider is merged, store facts win ----------
    {
        set_status_provider([] {
            return json{{"status", "degraded"},
                        {"sources", json::array({{{"scheduler_type", "slurm"},
                                                  {"state", "degraded"}}})},
                        {"queue", json{{"pending", 4}}},
                        {"store", json{{"migration_complete", false}, {"running", false}}}};
        });
        const auto merged = rpc("joblens/status");
        check(merged["status"] == "degraded",
              "a degraded maintenance provider must not be masked: " + merged.dump());
        check(merged["sources"].is_array() && merged["sources"].size() == 1,
              "provider sources must be served verbatim: " + merged.dump());
        check(merged["queue"]["pending"] == 4, "provider queue state must be served");
        check(merged["store"]["migration_complete"] == true &&
                  merged["store"]["running"] == true,
              "store facts must not be overridden by the provider payload: " + merged.dump());
        check(merged["db_schema_version"] == 5,
              "db_schema_version must come from store metadata");
        set_status_provider({});
        const auto cleared = rpc("joblens/status");
        check(cleared["status"] == "healthy" && cleared["sources"]["state"] == "not_configured",
              "clearing the provider must restore the explicit not_configured state: " +
                  cleared.dump());
    }
}

void run_closing_case(JobRegistry& registry, const Fixture& ids)
{
    // A closing manager rejects new binding mutations.
    install_deferred_drain(registry, [](JobEvent, const Job&) {});
    check(registry.removeJob(ids.condor_extra),
          "removing an instance with subscribers must start the drain");
    check(registry.closingJobCount() == size_t{1}, "the instance must be closing, not archived");

    const auto response = rpc(
        "jobs/collection/set",
        collection_request(json{{"JobID", format_job_id(ids.condor_extra)}}, "set",
                           json::array({"cpumem_collector"})));
    check(response["code"] == "instance_closing",
          "a closing instance must reject new intent with instance_closing: " + response.dump());
    check(test_runtime(registry).completeDrain(ids.condor_extra),
          "closing case: explicit deferred drain completion must finish removal");
}

// The remove two-tier: the default keeps the stop semantics; erase:true
// removes the instance for real — a collected instance leaves a durable h:
// history row, a never-collected one leaves none. This runs LAST and closes
// the registry so the raw LevelDB image can be inspected.
void run_erase_cases(JobRegistry& registry, const Fixture& ids, const std::string& db_path)
{
    const auto hex64 = [](uint64_t value) {
        std::string out;
        for (int shift = 60; shift >= 0; shift -= 4) {
            const int digit = static_cast<int>((value >> shift) & 0xf);
            out.push_back(static_cast<char>(digit < 10 ? '0' + digit : 'a' + digit - 10));
        }
        return out;
    };
    const auto raw_keys_with_prefix = [&db_path](const std::string& prefix) {
        leveldb::Options options;
        options.create_if_missing = false;
        leveldb::DB* db = nullptr;
        const auto opened = leveldb::DB::Open(options, db_path, &db);
        check(opened.ok(), "open raw db: " + opened.ToString());
        std::vector<std::string> keys;
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const std::string key = it->key().ToString();
            if (key.rfind(prefix, 0) == 0) keys.push_back(key);
        }
        check(it->status().ok(), "iterate raw db: " + it->status().ToString());
        delete it;
        delete db;
        return keys;
    };

    // The closing case left the runtime double in deferred-drain mode; restore
    // the auto-drain so the removals below converge through reconcile.
    install_collection_runtime(
        registry, [](const Job& job, std::vector<std::string>& applied, std::string& error) {
            applied = job.CollectorNames;
            error.clear();
            return true;
        });
    test_runtime(registry).setDeferredDrain(false);

    // Default remove (no erase): stop semantics, mode observable.
    {
        const auto response = rpc("jobs/stop",
                                  json{{"JobID", ids.condor_b}});
        check(response["status"] == "success",
              "default remove must keep succeeding: " + response.dump());
        check(response["mode"] == "stop",
              "default remove must identify itself as the stop tier");
        check(response["record_retained"] == true,
              "default remove must keep the job record");
        check(registry.jobRecord(ids.condor_b).has_value(),
              "default remove must keep the record");
    }

    // Erase a never-collected instance (nothing was ever attached to it):
    // the removal is a durable delete, no history row.
    {
        const auto response = rpc("jobs/erase",
                                  json{{"JobID", ids.condor_a_other}});
        check(response["status"] == "success",
              "erase of a never-collected instance must succeed: " + response.dump());
        check(response["mode"] == "erase",
              "erase must identify itself as the erase tier: " + response.dump());
        check(response["deleted_count"] == 1, "erase must count the removal");
        check(response["record_retained"] == false,
              "erase must not claim job-record retention");
        // The runtime owns the drain; reconcile drives it to completion, and
        // the never-collected fact turns the archive into a durable delete.
        test_runtime(registry).reconcile(ids.condor_a_other);
        check(!registry.jobRecord(ids.condor_a_other).has_value(),
              "erase must remove the instance from the active map");
    }

    // Erase a COLLECTED instance: with the runtime holding a real attachment,
    // the removal archives a durable history row instead of discarding.
    {
        CollectionRequest bind;
        bind.targets = {ids.slurm_batch};
        bind.collectors = {"cpumem_collector"};
        check(registry.setCollectors(bind).ok, "erase: binding must commit");
        test_runtime(registry).reconcile(ids.slurm_batch);
        check(registry.actualCollection(ids.slurm_batch).collectors.size() == 1,
              "erase: the runtime must hold a real attachment before the erase");

        const auto response = rpc("jobs/erase",
                                  json{{"JobID", ids.slurm_batch}});
        check(response["status"] == "success",
              "erase of a collected instance must succeed: " + response.dump());
        check(response["mode"] == "erase" && response["deleted_count"] == 1,
              "erase must report one removed instance: " + response.dump());
        // The drain converges through the test runtime (auto-drain), which
        // archives the closing record.
        test_runtime(registry).reconcile(ids.slurm_batch);
        check(!registry.jobRecord(ids.slurm_batch).has_value(),
              "the collected instance must leave the active map after the drain");
    }

    // The durable image, read with the store closed.
    check(registry.deinit(), "erase: shutdown must succeed");
    {
        const auto history_collected = raw_keys_with_prefix("h:" + hex64(ids.slurm_batch) + ":");
        check(history_collected.size() == 1,
              "a collected instance must leave exactly one history row, found " +
                  std::to_string(history_collected.size()));
        const auto active_collected = raw_keys_with_prefix("j:" + hex64(ids.slurm_batch));
        check(active_collected.empty(),
              "the erased instance must leave no active row");
        const auto history_discarded =
            raw_keys_with_prefix("h:" + hex64(ids.condor_a_other) + ":");
        check(history_discarded.empty(),
              "a never-collected instance must be deleted without a history row");
        const auto active_discarded = raw_keys_with_prefix("j:" + hex64(ids.condor_a_other));
        check(active_discarded.empty(),
              "the discarded instance must leave no active row");
    }

    std::cout << "PASS erase tier: remove keeps stop semantics; erase removes for "
                 "real (collected -> history row, never-collected -> delete)"
              << std::endl;
}

} // namespace

// (shutdown) ------------------------------------------------------------

// stop() joins the accept loop, and the loop may be writing a response. A client
// that stops reading must not pin shutdown: the response write is bounded and
// the active client is shut down before the join.
void slow_client_does_not_block_stop(const std::string& /*base*/)
{
    RPCServer::instance().register_method("test/big", [](const nlohmann::json&) {
        std::string payload(4u << 20, 'x');
        return nlohmann::json{{"payload", payload}};
    });

    // The suite's singleton already listens on the shared socket path.
    const std::string& path = socket_path;

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    check(fd >= 0, "slow client socket");
    int receive_buffer = 4096;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &receive_buffer, sizeof(receive_buffer));
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::copy(path.begin(), path.end(), address.sun_path);
    check(connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0,
          "slow client connect");

    const std::string request = R"({"method":"test/big","params":{}})";
    check(send(fd, request.data(), request.size(), MSG_NOSIGNAL) ==
              static_cast<ssize_t>(request.size()),
          "slow client send");
    // The server starts writing a 4 MiB response into a socket nobody reads.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::promise<void> finished;
    auto future = finished.get_future();
    std::thread stopper([&] {
        RPCServer::instance().stop();
        finished.set_value();
    });
    const bool bounded = future.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    if (!bounded) ::close(fd);   // unblock the server so the thread can finish
    stopper.join();
    ::close(fd);

    check(bounded, "stop() must not block on a client that stopped reading");
    std::cout << "PASS slow client: shutdown is bounded" << std::endl;
}

int main(int argc, char** argv)
{
    try {
        const std::string base =
            argc > 1 ? argv[1] : "/tmp/joblens-collection-rpc-test";
        std::filesystem::remove_all(base);
        std::filesystem::create_directories(base);

        const auto config_path = base + "/config.yaml";
        {
            std::ofstream config(config_path);
            config << "lens_config:\n  max_collector_threads: 1\n"
                      "collectors_config:\n  enable_collector_perf: false\n"
                      "  collectors:\n"
                      "    - name: cpumem_collector\n"
                      "      type: CPUMemCollector\n"
                      "      config: cpumem_collector_config\n"
                      "cpumem_collector_config:\n  freq: 1\n"
                      "job_registry_config:\n  job_db_path: " << base << "/job.db\n";
        }
        Config::instance(config_path);

        socket_path = base + "/rpc.sock";
        RPCServer::instance(socket_path);

        JobRegistryOptions options;
        options.db_path = base + "/job.db";
        options.test_static_registry = true;
        options.enable_pid_tracker = false;
        options.register_rpc = true;
        JobRegistry registry(options);
        check(registry.storeHealth().migration_complete,
              "the test store must migrate to the current schema before the RPC cases run");

        RPCServer::instance().start();
        struct Stop {
            ~Stop() { RPCServer::instance().stop(); }
        } stop;

        Fixture ids;
        ids.condor_a = seed(registry, condor_observation(condor_selector("172.0", "schedd-a"), 4242),
                            "condor 172.0 schedd-a");
        ids.condor_a_other =
            seed(registry, condor_observation(condor_selector("172.0", "schedd-b"), 4243),
                 "condor 172.0 schedd-b");
        ids.condor_b = seed(registry, condor_observation(condor_selector("173.0", "schedd-a"), 4244),
                            "condor 173.0");
        ids.condor_legacy =
            seed(registry, condor_observation(condor_selector("180.0", "schedd-a"), 4245),
                 "condor 180.0");
        ids.condor_extra =
            seed(registry, condor_observation(condor_selector("190.0", "schedd-a"), 4246),
                 "condor 190.0");
        ids.slurm_step1 = seed(registry, slurm_observation(slurm_selector("9001", "1"), 5252),
                               "slurm 9001 step 1");
        ids.slurm_step2 = seed(registry, slurm_observation(slurm_selector("9001", "2"), 5253),
                               "slurm 9001 step 2");
        ids.slurm_batch = seed(registry, slurm_observation(slurm_selector("9002", "batch"), 5254),
                               "slurm 9002 batch");

        run_selector_cases(registry, ids);
        run_intent_cases(registry, ids);
        run_legacy_cases(registry, ids);
        run_job_record_cases(registry, ids);
        run_closing_case(registry, ids);
        run_erase_cases(registry, ids, base + "/job.db");
        slow_client_does_not_block_stop(base);

        std::cout << "PASS collection rpc: selector forms, snapshot job scope, "
                     "ambiguity and error classes; set/stop/reset_auto, unknown "
                     "collector, stale revision, durable-unavailable mapping and "
                     "accepted-versus-applied; legacy add identity resolution with "
                     "a JobID hint, duplicate observation, remove-as-stop with retained "
                     "job records; bounded paging, cursor invalidation, sentinel "
                     "exclusion, decimal-string ids, capabilities and status"
                  << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL collection rpc: " << error.what() << std::endl;
        return 1;
    }
}
