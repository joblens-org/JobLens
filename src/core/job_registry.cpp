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
// job_registry.cpp
#include "core/job_registry.hpp"
#include <spdlog/spdlog.h>
#include <date/date.h>
#include "common/config.hpp"
#include <signal.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <chrono>
#include <filesystem>

#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "common/common_job.hpp"
#include "common/local_rpc.hpp"

std::pair<std::string, std::string>
split_two(std::string s, char sep = '.') {
    auto p = s.find(sep);
    return (p == s.npos)
           ? std::pair{s, ""}
           : std::pair{s.substr(0, p), s.substr(p + 1)};
}

// hex64: uint64 → 16 字符零填充十六进制，保证字典序与数值序一致
static std::string hex64(uint64_t v) {
    return fmt::format("{:016x}", v);
}

static uint64_t unhex64(const std::string& s) {
    return std::stoull(s, nullptr, 16);
}

static const std::string KEY_PREFIX_JOB     = "j:";
static const std::string KEY_PREFIX_HISTORY = "h:";
static const std::string KEY_PREFIX_COUNTER = "c:";

static std::string key_job(uint64_t jobID) {
    return KEY_PREFIX_JOB + hex64(jobID);
}

static std::string key_history(uint64_t jobID, uint64_t end_time) {
    return KEY_PREFIX_HISTORY + hex64(jobID) + ":" + hex64(end_time);
}

static std::string key_counter(const std::string& name) {
    return KEY_PREFIX_COUNTER + name;
}

static std::string persist_job_to_json(const Job& job) {
    nlohmann::json j;
    j["jobtype"]       = static_cast<int>(job.jobtype);
    j["subtype"]       = static_cast<int>(job.subtype);
    j["pids"]          = job.JobPIDs;
    j["collectors"]    = job.CollectorNames;
    j["native_job_id"] = job.NativeJobID;
    std::visit([&j](const auto& obj) {
        j["sub_attr"] = nlohmann::json(obj);
    }, job.sub_attr);
    return j.dump();
}

static Job persist_json_to_job(const std::string& json_str) {
    auto j = nlohmann::json::parse(json_str);
    Job job;
    job.jobtype       = static_cast<JobType>(j.value("jobtype", 0));
    job.subtype       = static_cast<JobSubType>(j.value("subtype", 0));
    job.JobPIDs       = j.value("pids", nlohmann::json::array()).get<std::vector<pid_t>>();
    job.CollectorNames = j.value("collectors", nlohmann::json::array()).get<std::vector<std::string>>();
    job.NativeJobID   = j.value("native_job_id", "");

    if (job.subtype == JobSubType::Condor) {
        if (j.contains("sub_attr")) {
            job.sub_attr = j["sub_attr"].get<CondorJobAttr>();
        } else {
            job.sub_attr = CondorJobAttr{};
        }
    } else if (job.subtype == JobSubType::Slurm) {
        if (j.contains("sub_attr")) {
            job.sub_attr = j["sub_attr"].get<SlurmJobAttr>();
        } else {
            job.sub_attr = SlurmJobAttr{};
        }
    } else {
        job.sub_attr = CommonJobAttr{.auto_update_child = true};
    }

    return job;
}


std::string json2JobOpt(const nlohmann::json& obj, Job& job) {
    auto opt = obj["opt"].get<std::string>();

    job.JobID = obj.value("JobID", 0u);
    job.JobPIDs = obj.value("JobPIDs", nlohmann::json::array()).get<std::vector<pid_t>>();
    job.CollectorNames = obj.value("Lens", nlohmann::json::array()).get<std::vector<std::string>>();
    if (job.JobPIDs.size() == 0) {
        spdlog::warn("JobRegistry: job ID {} has empty PID list", job.JobID);
    }
    for (auto pid: job.JobPIDs) {
        if (pid < 0) {
            spdlog::warn("JobRegistry: invalid PID {} in job ID {}", pid, job.JobID);
        }
    }

    auto [type, subtype] = split_two(obj["type"].get<std::string>());
    if(type.compare("job") == 0){
        job.jobtype = JobType::Job;
    }else if (type.compare("sys") == 0){
        job.jobtype = JobType::Sys;
    }else {
        job.jobtype = JobType::Unknown;
        throw std::runtime_error("invalid job type: " + type);
    }
    
    if (subtype.compare("condor") == 0){
        job.subtype = JobSubType::Condor;
        auto sub = obj.value("sub_attr", nlohmann::json::object());
        job.sub_attr = CondorJobAttr{
            .auto_update_child = sub.value("auto_update_child", obj.value("auto_update_child", true)),
            .starter_pid = sub.value("starter_pid", nlohmann::json(0)).get<pid_t>(),
            .cluster_id = sub.value("cluster_id", nlohmann::json(0u)).get<size_t>(),
            .proc_id = sub.value("proc_id", nlohmann::json(0u)).get<size_t>(),
            .scheduler_host = sub.value("scheduler_host", ""),
            .scheduler_name = sub.value("scheduler_name", ""),
            .slots_cgroup_path = sub.value("slots_cgroup_path", ""),
            .owner = sub.value("owner", ""),
            .job_ad_path = sub.value("job_ad_path", ""),
            .collector_host = sub.value("collector_host", "")
        };
        auto& condor_attr = std::get<CondorJobAttr>(job.sub_attr);
        auto cid = condor_attr.cluster_id;
        auto proc = condor_attr.proc_id;
        job.NativeJobID = fmt::format("{}.{}", cid, proc);
    }else if (subtype.compare("slurm") == 0){
        job.subtype = JobSubType::Slurm;
        auto sub = obj.value("sub_attr", nlohmann::json::object());
        job.sub_attr = SlurmJobAttr{
            .auto_update_child = sub.value("auto_update_child", obj.value("auto_update_child", true)),
            .stepd_pid = sub.value("stepd_pid", nlohmann::json(0)).get<pid_t>(),
            .job_id = sub.value("job_id", nlohmann::json(0u)).get<size_t>(),
            .step_id = sub.value("step_id", nlohmann::json(0u)).get<uint32_t>(),
            .cluster_name = sub.value("cluster_name", ""),
            .cgroup_path = sub.value("cgroup_path", ""),
            .user = sub.value("user", ""),
            .job_name = sub.value("job_name", ""),
            .partition = sub.value("partition", "")
        };
        auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
        auto jid = slurm_attr.job_id;
        job.NativeJobID = fmt::format("{}", jid);
    }else if (subtype.compare("common") == 0){
        job.subtype = JobSubType::Common;
        job.sub_attr = CommonJobAttr{
            .auto_update_child = obj.value("auto_update_child", true)
        };
    }

    spdlog::info("JobRegistry: json2JobOpt parsed opt={}, type={}, subtype={}, JobID={}, "
                 "PIDs_count={}, Lens_count={}, nativeID={}",
                 opt, type, subtype, job.JobID, job.JobPIDs.size(),
                 job.CollectorNames.size(), job.NativeJobID);
    
    return opt;
}

JobRegistry::JobRegistry(){
    Job fake_job;
    fake_job.JobID = 0;
    fake_job.JobPIDs = {1};
    fake_job.jobtype = JobType::Sys;
    fake_job.subtype = JobSubType::Common;
    fake_job.sub_attr = CommonJobAttr{
        .auto_update_child = false
    };
    fake_job.CollectorNames = {"Easter egg"}; //实际上这里的添加方式不一样，所以无所谓这里的名字
    jobs_.emplace(0, fake_job); // 直接添加避免拉起回调

    if (init_job_db()){
        db_running = true;
        init_job_id_counter();  // 初始化JobID计数器
    } else {
        spdlog::warn("JobRegistry: job db not running");
    }

    init_job_watcher();

    regRPChandle();
};

void JobRegistry::init_job_watcher() {
    enable_auto_add_condor_job = Config::instance().getBool("job_registry_config", "auto_add_condorjob", false);
    enable_auto_add_slurm_job = Config::instance().getBool("job_registry_config", "auto_add_slurmjob", false);

    if (enable_auto_add_condor_job || enable_auto_add_slurm_job) {
        cgroup_mkdir_event_source_ = std::make_unique<CgroupMkdirEventSource>();
        if (!cgroup_mkdir_event_source_->start()) {
            spdlog::warn("JobRegistry: failed to start cgroup mkdir event source");
        } else {
            spdlog::info("JobRegistry: enabled cgroup mkdir event source");
        }
    }

    if (enable_auto_add_condor_job) {
        condor_job_watcher_ = std::make_unique<CondorJobWatcher>();
        condor_job_watcher_->register_add_if(
            [this](Job job){
                this->addJob(job);
            }
        );
        if (cgroup_mkdir_event_source_) {
            cgroup_mkdir_event_source_->register_callback(
                [this](const std::string& path) {
                    if (condor_job_watcher_) {
                        condor_job_watcher_->on_cgroup_mkdir(path);
                    }
                }
            );
            spdlog::debug("JobRegistry: registered condor cgroup mkdir callback");
        }
        spdlog::info("JobRegistry: enabled auto add condor job");
    }

    if (enable_auto_add_slurm_job) {
        slurm_job_watcher_ = std::make_unique<SlurmJobWatcher>();
        slurm_job_watcher_->register_add_if(
            [this](Job job){
                this->addJob(job);
            }
        );
        if (cgroup_mkdir_event_source_) {
            cgroup_mkdir_event_source_->register_callback(
                [this](const std::string& path) {
                    if (slurm_job_watcher_) {
                        slurm_job_watcher_->on_cgroup_mkdir(path);
                    }
                }
            );
            spdlog::debug("JobRegistry: registered slurm cgroup mkdir callback");
        }
        spdlog::info("JobRegistry: enabled auto add slurm job");
    }
}

void JobRegistry::regRPChandle() {
    // 注册 RPC 方法
    // 请求体格式示例：
    // {
    //     "opt": "add",
    //     "type": "job.condor",
    //     "JobID": 1,
    //     "JobPIDs": [1],
    //     "Lens": ["proc_collector"],
    //      "auto_update_child": true
    // }
    RPCServer::instance().register_method("JobRegistry/job_opt",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            try {
                Job job;
                std::string opt = json2JobOpt(req, job);
                if(opt.compare("add") == 0){
                    if (addJob(job)) {
                        j["status"] = "success";
                    } else {
                        j["status"] = "error";
                        j["msg"] = fmt::format("failed to add job {} (type={}): empty collectors, duplicate sub_attr, or duplicate JobID — check server logs",
                                               job.JobID, job.NativeJobID);
                    }
                }else if(opt.compare("remove") == 0){
                    size_t deleted = delJobByRequest(req);
                    j["status"] = "success";
                    j["deleted_count"] = deleted;
                }else {
                    j["status"] = "error";
                    j["msg"] = "invalid operation, expected 'add' or 'remove'";
                }
            } catch (const std::exception& e) {
                j["status"] = "error";
                j["msg"] = fmt::format("failed to add job: {}", e.what());
            }
            return j;
        }
    );

    RPCServer::instance().register_method("JobRegistry/get_job_count",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            j["status"] = "ok";
            j["job_count"] = jobs_.size();
            return j;
        }
    );

    // 列出所有 Job
    RPCServer::instance().register_method("JobRegistry/list_jobs",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json res = nlohmann::json::object();
            res["status"] = "ok";
            res["jobs"] = nlohmann::json::array();
            for (const auto& job : jobs_) {
                if(job.second.JobID == 0) continue; //跳过彩蛋任务
                res["jobs"].push_back(dump_job(job.second));
            }
            return res;
        }
    );
    // 获取指定 JobID 的 Job 信息
    RPCServer::instance().register_method("JobRegistry/get_job",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            if (!req.contains("JobID")) {
                j["error"] = "missing JobID in request";
                return j;
            }
            uint64_t jobID = req["JobID"].get<uint64_t>();
            auto job = this->findJob(jobID);
            if (!job.has_value()) {
                j["error"] = "job not found with JobID " + std::to_string(jobID);
                return j;
            }
            j = dump_job(job.value());
            return j;
        }
    );
}

JobRegistry& JobRegistry::instance() {
    static JobRegistry reg;
    return reg;
}


bool JobRegistry::addJob(const Job& job) {
    spdlog::info("JobRegistry: addJob entry JobID={}, type={}, collectors={}, PIDs={}",
                 job.JobID, job.NativeJobID, job.CollectorNames.size(), job.JobPIDs.size());
    // 错误检查
    if (job.CollectorNames.empty()) {
        spdlog::warn("JobRegistry: job with JobID {} has empty CollectorNames", job.JobID);
        return false;
    }
    // 自动生成JobID
    Job job_copy = job; // 先拷贝一份，避免修改外部对象
    if (job_copy.JobID == 0) {
        job_copy.JobID = getNextJobID();
        spdlog::info("JobRegistry: assigned JobID {} to new job", job_copy.JobID);
    } 
    return addJob(job_copy, false);
}

bool JobRegistry::addJob(const Job& job, bool from_db) {
    {
        std::lock_guard lg(mtx_);
        SubAttrMatcher matcher;
        matcher.subtype = job.subtype;
        if (std::holds_alternative<CondorJobAttr>(job.sub_attr)) {
            const auto& attr = std::get<CondorJobAttr>(job.sub_attr);
            matcher.subtype = JobSubType::Condor;
            matcher.condor_cluster_id = attr.cluster_id;
            matcher.condor_proc_id = attr.proc_id;
        } else if (std::holds_alternative<SlurmJobAttr>(job.sub_attr)) {
            const auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
            matcher.subtype = JobSubType::Slurm;
            matcher.slurm_job_id = attr.job_id;
            matcher.slurm_step_id = attr.step_id;
        }
        for (const auto& [id, existing_job] : jobs_) {
            if (matcher.matches(existing_job.sub_attr)) {
                spdlog::warn("JobRegistry: job with matching sub_attr already exists (JobID {}), ignored", existing_job.JobID);
                return false;
            }
        }
        
        if (jobs_.count(job.JobID)) {
            spdlog::warn("JobRegistry: duplicate jobID {}, ignored", job.JobID);
            return false;
        }
        jobs_.emplace(job.JobID, job);
    }
    if (!from_db) {
        // 在锁内取出job副本，避免释放锁后worker线程并发删除导致at()抛out_of_range
        Job job_for_cb;
        {
            std::shared_lock lg(mtx_);
            auto it = jobs_.find(job.JobID);
            if (it == jobs_.end()) return false;
            job_for_cb = it->second;
        }

        for (const auto& cb : cbs_) cb(JobEvent::Added, job_for_cb);
        spdlog::info("JobRegistry: add job with JobID {}", job.JobID);

        // 回调链中定时器可能已立即触发并删除job，用find安全访问
        {
            std::shared_lock lg(mtx_);
            auto it = jobs_.find(job.JobID);
            if (it == jobs_.end()) {
                spdlog::info("JobRegistry: job {} was removed during callback, skipping persist", job.JobID);
                return false;
            }
            persist_new_job(it->second);
        }
    } else {
        spdlog::info("JobRegistry: restored job with JobID {} from db", job.JobID);
    }
    return true;
}

void JobRegistry::delJob(uint64_t jobID) {
    Job removed;                // 先拷贝出来，再回调，避免锁内回调
    {
        std::lock_guard lg(mtx_);
        auto it = jobs_.find(jobID);
        if (it == jobs_.end()) return;
        removed = std::move(it->second);
        jobs_.erase(it);
    }
    for (const auto& cb : cbs_) cb(JobEvent::Removed, removed);
    spdlog::info("JobRegistry: remove job with JobID {}", removed.JobID);
    end_job_in_db(removed.JobID);
    return;
}

size_t JobRegistry::delJob(const SubAttrMatcher& matcher) {
    std::vector<Job> removed_jobs;
    
    {
        std::lock_guard lg(mtx_);
        for (auto it = jobs_.begin(); it != jobs_.end(); ) {
            if (matcher.matches(it->second.sub_attr)) {
                removed_jobs.push_back(std::move(it->second));
                it = jobs_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    // 在锁外进行回调和数据库操作
    for (const auto& job : removed_jobs) {
        for (const auto& cb : cbs_) cb(JobEvent::Removed, job);
        spdlog::info("JobRegistry: remove job with JobID {} by sub_attr matcher", job.JobID);
        end_job_in_db(job.JobID);
    }
    
    spdlog::info("JobRegistry: removed {} jobs by sub_attr matcher", removed_jobs.size());
    return removed_jobs.size();
}

size_t JobRegistry::delJobByRequest(const nlohmann::json& req) {
    // 优先检查 JobID，如果存在则按 JobID 删除
    if (req.contains("JobID")) {
        uint64_t jobID = req["JobID"].get<uint64_t>();
        if (findJob(jobID).has_value()) {
            delJob(jobID);
            return 1;
        }
        return 0;
    }
    
    // 检查 matcher 字段，解析 sub_attr 匹配条件
    if (req.contains("matcher")) {
        const auto& matcher_json = req["matcher"];
        SubAttrMatcher matcher;
        
        // 解析 subtype
        if (matcher_json.contains("subtype")) {
            std::string subtype_str = matcher_json["subtype"].get<std::string>();
            if (subtype_str == "Condor") {
                matcher.subtype = JobSubType::Condor;
            } else if (subtype_str == "Slurm") {
                matcher.subtype = JobSubType::Slurm;
            } else if (subtype_str == "Common") {
                matcher.subtype = JobSubType::Common;
            }
        }
        
        // 解析 Condor 匹配字段
        if (matcher_json.contains("condor_cluster_id")) {
            matcher.condor_cluster_id = matcher_json["condor_cluster_id"].get<size_t>();
        }
        if (matcher_json.contains("condor_proc_id")) {
            matcher.condor_proc_id = matcher_json["condor_proc_id"].get<size_t>();
        }
        if (matcher_json.contains("condor_owner")) {
            matcher.condor_owner = matcher_json["condor_owner"].get<std::string>();
        }
        if (matcher_json.contains("condor_starter_pid")) {
            matcher.condor_starter_pid = matcher_json["condor_starter_pid"].get<pid_t>();
        }
        
        // 解析 Slurm 匹配字段
        if (matcher_json.contains("slurm_job_id")) {
            matcher.slurm_job_id = matcher_json["slurm_job_id"].get<size_t>();
        }
        if (matcher_json.contains("slurm_step_id")) {
            matcher.slurm_step_id = matcher_json["slurm_step_id"].get<uint32_t>();
        }
        if (matcher_json.contains("slurm_user")) {
            matcher.slurm_user = matcher_json["slurm_user"].get<std::string>();
        }
        if (matcher_json.contains("slurm_job_name")) {
            matcher.slurm_job_name = matcher_json["slurm_job_name"].get<std::string>();
        }
        if (matcher_json.contains("slurm_stepd_pid")) {
            matcher.slurm_stepd_pid = matcher_json["slurm_stepd_pid"].get<pid_t>();
        }
        
        return delJob(matcher);
    }
    
    // 既没有 JobID 也没有 matcher，返回 0
    spdlog::warn("JobRegistry::delJobByRequest: request must contain 'JobID' or 'matcher' field");
    return 0;
}

inline bool is_process_running(pid_t pid) {
    return kill(pid, 0) == 0;
}

inline void JobRegistry::update_job(Job& job){
    if(job.subtype == JobSubType::Condor){
        CondorJob::update_job_info(job);
        CondorJob::update_job_pids(job);
    }
    if(job.subtype == JobSubType::Slurm){
        SlurmJob::update_job_info(job);
        SlurmJob::update_job_pids(job);
    }
    if(job.subtype == JobSubType::Common){
        if (std::get<CommonJobAttr>(job.sub_attr).auto_update_child){
            auto old_pids = job.JobPIDs; 
            CommonJob::update_job_child_process(job);
            if (old_pids != job.JobPIDs){
                update_job_info(job);
            }
        }
    }

}

std::optional<Job> JobRegistry::findJob(uint64_t jobID) {
    std::vector<uint64_t> toDelete;
    Job result;

    {
        std::shared_lock lg(mtx_);
        auto it = jobs_.find(jobID);
        if (it == jobs_.end()) return std::nullopt;
        result = it->second;  // 先拷贝
    }

    update_job(result);
    result.JobPIDs.erase(
        std::remove_if(result.JobPIDs.begin(), result.JobPIDs.end(),
                       [](pid_t pid){ return !is_process_running(pid); }),
        result.JobPIDs.end());

    {
        std::unique_lock lg(mtx_);
        // 使用find代替at，避免读锁与写锁间隙中job被其他线程删除导致out_of_range
        auto it = jobs_.find(jobID);
        if (it == jobs_.end()) return std::nullopt;
        auto& job = it->second;
        job = result;

        if (job.JobPIDs.empty()) {
            toDelete.push_back(jobID);
            spdlog::info("JobRegistry: job {} has no running process, delete it", job.JobID);
        }
        result = job;  // 更新拷贝以反映最新状态
    }

    for (size_t id : toDelete)
        const_cast<JobRegistry*>(this)->delJob(id);

    return toDelete.empty() ? std::optional<Job>(result) : std::nullopt;
}

std::vector<Job> JobRegistry::findJob(const SubAttrMatcher& matcher) const {
    std::vector<Job> matched_jobs;
    
    std::shared_lock lg(mtx_);
    for (const auto& [id, job] : jobs_) {
        if (matcher.matches(job.sub_attr)) {
            matched_jobs.push_back(job);
        }
    }
    
    spdlog::debug("JobRegistry: found {} jobs matching the criteria", matched_jobs.size());
    return matched_jobs;
}

bool JobRegistry::init_job_db(){
    auto db_path = Config::instance().getString("job_registry_config", "job_db_path");

    std::filesystem::path db_dir(db_path);
    if (db_dir.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(db_dir.parent_path(), ec);
    }

    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_path, &job_db);
    if (!status.ok()) {
        spdlog::error("JobRegistry: failed to open LevelDB at {}: {}", db_path, status.ToString());
        return false;
    }

    spdlog::info("JobRegistry: init_job_db at {}", db_path);
    
    try{
        restore_jobs_from_db();
    }
    catch(const std::exception& e){
        spdlog::warn("JobRegistry: failed to restore jobs from db: {}", e.what());
    }
    
    return true;
}

void JobRegistry::deinit_job_db(){
    delete job_db;
    job_db = nullptr;
    spdlog::info("JobRegistry: deinit_job_db");
}

void JobRegistry::restore_jobs_from_db(){
    leveldb::Iterator* it = job_db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(KEY_PREFIX_JOB); it->Valid() && it->key().starts_with(KEY_PREFIX_JOB); it->Next()) {
        std::string key_str = it->key().ToString();
        uint64_t jobID = unhex64(key_str.substr(KEY_PREFIX_JOB.size()));

        Job job = persist_json_to_job(it->value().ToString());
        job.JobID = jobID;

        restore_jobs_.push_back(std::move(job));
        spdlog::debug("JobRegistry: restored job with JobID {} from db", jobID);
    }
    if (!it->status().ok()) {
        spdlog::warn("JobRegistry: iterator error during restore: {}", it->status().ToString());
    }
    delete it;
    spdlog::info("JobRegistry: restored {} jobs from db", restore_jobs_.size());
}

void JobRegistry::persist_new_job(const Job& job)
{
    if (!db_running) return;

    std::lock_guard<std::mutex> db_lock(db_mtx_);

    std::string key = key_job(job.JobID);
    std::string json = persist_job_to_json(job);
    leveldb::Status s = job_db->Put(leveldb::WriteOptions(), key, json);
    if (s.ok()) {
        spdlog::info("JobRegistry: persist new job with JobID {} to db", job.JobID);
    } else {
        spdlog::error("JobRegistry: failed to persist JobID {} — LevelDB error: {}",
                      job.JobID, s.ToString());
    }
}


void JobRegistry::update_job_info(const Job& job){
    if(!db_running) return;
    std::lock_guard<std::mutex> db_lock(db_mtx_);

    std::string key = key_job(job.JobID);
    std::string json = persist_job_to_json(job);
    leveldb::Status s = job_db->Put(leveldb::WriteOptions(), key, json);
    if (s.ok()) {
        spdlog::info("JobRegistry: update job info with JobID {} in db", job.JobID);
    } else {
        spdlog::error("JobRegistry: failed to update JobID {} — LevelDB error: {}",
                      job.JobID, s.ToString());
    }
}
    

void JobRegistry::end_job_in_db(uint64_t jobID){
    if(!db_running) return;
    
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    
    std::string key = key_job(jobID);
    std::string json;
    leveldb::Status s = job_db->Get(leveldb::ReadOptions(), key, &json);
    if (s.IsNotFound()) {
        spdlog::warn("JobRegistry: job {} not found in db when ending", jobID);
        return;
    }
    if (!s.ok()) {
        spdlog::error("JobRegistry: failed to read job {} for archival: {}", jobID, s.ToString());
        return;
    }

    uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    leveldb::WriteBatch batch;
    batch.Delete(key);
    batch.Put(key_history(jobID, now), json);

    s = job_db->Write(leveldb::WriteOptions(), &batch);
    if (s.ok()) {
        spdlog::info("JobRegistry: moved ended job {} from jobs to jobs_history", jobID);
    } else {
        spdlog::error("JobRegistry: failed to end_job_in_db for JobID {} — LevelDB error: {}",
                      jobID, s.ToString());
    }
}


// 暂时弃用，等待加入bpf特性改为共享指针加作业粒度锁
// const Job* JobRegistry::findJob(int jobID)
// {
//     std::vector<int> toDelete;          
//     const Job* ret = nullptr;

//     {
//         std::shared_lock lg(mtx_);
//         auto it = jobs_.find(jobID);
//         if (it == jobs_.end()) return nullptr;
//         // 其实这里设计的目的是为了惰性加载，在符合实时性的前提下减少调用次数
//         // 更好的方法是用bpf监听相关内容，使用shared_point，使用更加细粒度的锁
//         // 目前这里选择
//         update_job(jobs_[jobID]);
//         // 过滤 PID
//         jobs_[jobID].JobPIDs.erase(
//             std::remove_if(jobs_[jobID].JobPIDs.begin(),
//                            jobs_[jobID].JobPIDs.end(),
//                            [](pid_t pid){ return !is_process_running(pid); }),
//             jobs_[jobID].JobPIDs.end());

//         if (jobs_[jobID].JobPIDs.empty()) {
//             toDelete.push_back(jobID);
//             spdlog::info("JobRegistry: job {} has no running process, delete it", jobs_[jobID].JobID);
//         }
        
//         ret = &jobs_[jobID];
//     }

//     for (int id : toDelete)
//         const_cast<JobRegistry*>(this)->delJob(id);   // 非 const 调用

    
//     return toDelete.empty() ? ret : nullptr;   // 如果删了，就返回空
// }


std::vector<Job> JobRegistry::snapshot() const {
    std::shared_lock lg(mtx_);
    std::vector<Job> out;
    out.reserve(jobs_.size());
    for (const auto& [id, job] : jobs_) out.push_back(job);
    return out;
}

void JobRegistry::addLifecycleCb(JobLifecycleCb cb) {
    {
        std::lock_guard lg(mtx_);
        cbs_.push_back(std::move(cb));
    }
    
    if (!restore_jobs_.empty()) {
        // 先调用回调添加恢复的任务
        // 这里其实设计不好，只能支持一个回调函数
        for (const auto& job : restore_jobs_) {
            addJob(job);
        }
        restore_jobs_.clear();
    }
}

void JobRegistry::init_job_id_counter() {
    if (!db_running) {
        spdlog::warn("JobRegistry: db not running, using default next_job_id_ = 1");
        return;
    }

    try {
        uint64_t counter_value = 1;

        std::string counter_json;
        leveldb::Status s = job_db->Get(leveldb::ReadOptions(), key_counter("job_id"), &counter_json);
        if (s.ok()) {
            counter_value = std::stoull(counter_json);
        }

        uint64_t max_job_id = 0;

        // 扫描 jobs 前缀取最大 JobID
        leveldb::Iterator* it = job_db->NewIterator(leveldb::ReadOptions());
        it->Seek(KEY_PREFIX_JOB);
        if (it->Valid() && it->key().starts_with(KEY_PREFIX_JOB)) {
            // 跳到最后一个 j: key
            it->SeekToLast();
            // 回退确保在 j: 范围内
            while (it->Valid() && !it->key().starts_with(KEY_PREFIX_JOB)) {
                it->Prev();
            }
            if (it->Valid()) {
                uint64_t id = unhex64(it->key().ToString().substr(2));
                if (id > max_job_id) max_job_id = id;
            }
        }

        // 扫描 history 前缀
        it->Seek(KEY_PREFIX_HISTORY);
        if (it->Valid() && it->key().starts_with(KEY_PREFIX_HISTORY)) {
            it->SeekToLast();
            while (it->Valid() && !it->key().starts_with(KEY_PREFIX_HISTORY)) {
                it->Prev();
            }
            if (it->Valid()) {
                // history key: "h:" + hex64(JobID) + ":" + hex64(timestamp)
                std::string hex_part = it->key().ToString().substr(2, 16);
                uint64_t id = unhex64(hex_part);
                if (id > max_job_id) max_job_id = id;
            }
        }
        delete it;

        next_job_id_ = std::max(counter_value, max_job_id + 1);

        // 持久化当前值
        std::string value = std::to_string(next_job_id_);
        job_db->Put(leveldb::WriteOptions(), key_counter("job_id"), value);

        spdlog::info("JobRegistry: initialized job_id_counter, next_job_id_ = {}", next_job_id_);
    } catch (const std::exception& e) {
        spdlog::error("JobRegistry: failed to init job_id_counter: {}", e.what());
        next_job_id_ = 1;
    }
}

void JobRegistry::persist_job_id_counter() {
    if (!db_running) return;

    std::lock_guard<std::mutex> db_lock(db_mtx_);
    std::string value = std::to_string(next_job_id_);
    leveldb::Status s = job_db->Put(leveldb::WriteOptions(), key_counter("job_id"), value);
    if (!s.ok()) {
        spdlog::error("JobRegistry: failed to persist job_id_counter: {}", s.ToString());
    }
}

uint64_t JobRegistry::getNextJobID() {
    std::lock_guard<std::mutex> lg(job_id_mtx_);
    uint64_t job_id = next_job_id_++;
    persist_job_id_counter();
    spdlog::debug("JobRegistry: allocated new JobID {}", job_id);
    return job_id;
}
