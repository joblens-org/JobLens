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
#include "common/streamer_watcher.hpp"
#include <date/date.h>
#include "common/config.hpp"
#include <signal.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <thread>

#include "common/utils.hpp"
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

// Helper functions to split/merge uint64 for SQLite storage
static inline void split_uint64(uint64_t value, uint32_t& high, uint32_t& low) {
    high = static_cast<uint32_t>(value >> 32);
    low = static_cast<uint32_t>(value & 0xFFFFFFFF);
}

static inline uint64_t combine_high_low(uint32_t high, uint32_t low) {
    return (static_cast<uint64_t>(high) << 32) | static_cast<uint64_t>(low);
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
        auto cid = sub.value("cluster_id", nlohmann::json(0u)).get<size_t>();
        auto proc = sub.value("proc_id", nlohmann::json(0u)).get<size_t>();
        if (!obj.contains("sub_attr")) {
            spdlog::info("JobRegistry: condor job sub_attr not provided, using defaults cluster_id={}, proc_id={}", cid, proc);
        }
        job.sub_attr = CondorJobAttr{
            .auto_update_child = obj.value("auto_update_child", true),
            .cluster_id = cid,
            .proc_id = proc,
        };
        job.NativeJobID = fmt::format("{}.{}", cid, proc);
    }else if (subtype.compare("slurm") == 0){
        job.subtype = JobSubType::Slurm;
        auto sub = obj.value("sub_attr", nlohmann::json::object());
        auto jid = sub.value("job_id", nlohmann::json(0u)).get<size_t>();
        auto sid = sub.value("step_id", nlohmann::json(0u)).get<uint32_t>();
        if (!obj.contains("sub_attr")) {
            spdlog::info("JobRegistry: slurm job sub_attr not provided, using defaults job_id={}, step_id={}", jid, sid);
        }
        job.sub_attr = SlurmJobAttr{
            .auto_update_child = obj.value("auto_update_child", true),
            .job_id = jid,
            .step_id = sid
        };
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
    if (enable_auto_add_condor_job) {
        condor_job_watcher_ = std::make_unique<CondorJobWatcher>();
        condor_job_watcher_->register_add_if(
            [this](Job job){
                this->addJob(job);
            }
        );
        spdlog::info("JobRegistry: enabled auto add condor job");
    }

    enable_auto_add_slurm_job = Config::instance().getBool("job_registry_config", "auto_add_slurmjob", false);
    if (enable_auto_add_slurm_job) {
        slurm_job_watcher_ = std::make_unique<SlurmJobWatcher>();
        slurm_job_watcher_->register_add_if(
            [this](Job job){
                this->addJob(job);
            }
        );
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
        CommonJob::update_job_child_process(job);   
    }
    if(job.subtype == JobSubType::Slurm){
        SlurmJob::update_job_info(job);
        CommonJob::update_job_child_process(job);
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

    {
        std::unique_lock lg(mtx_);
        // 使用find代替at，避免读锁与写锁间隙中job被其他线程删除导致out_of_range
        auto it = jobs_.find(jobID);
        if (it == jobs_.end()) return std::nullopt;
        auto& job = it->second;
        update_job(job);

        job.JobPIDs.erase(
            std::remove_if(job.JobPIDs.begin(), job.JobPIDs.end(),
                           [](pid_t pid){ return !is_process_running(pid); }),
            job.JobPIDs.end());

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
    job_db = std::make_unique<SQLite::Database>(db_path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE);
    if (!job_db) {
        spdlog::error("JobRegistry: failed to create job database");
        return false;
    }
    // 创建表
    try{
        job_db->exec("CREATE TABLE IF NOT EXISTS jobs \
                 (id INTEGER PRIMARY KEY, \
                  jobid_high INTEGER, \
                  jobid_low INTEGER, \
                  jobtype INTEGER, \
                  subtype INTEGER, \
                  pids TEXT, \
                  collectors TEXT, \
                  native_job_id TEXT DEFAULT '', \
                  status INTEGER DEFAULT 1, \
                  UNIQUE(jobid_high, jobid_low));");
        job_db->exec("CREATE INDEX IF NOT EXISTS idx_jobid ON jobs (jobid_high, jobid_low);");
        
        // 迁移已有表：添加 native_job_id 列（如果不存在）
        try {
            job_db->exec("ALTER TABLE jobs ADD COLUMN native_job_id TEXT DEFAULT ''");
        } catch (const SQLite::Exception& e) {
            // 列已存在则忽略
        }
        
        // 创建历史表
        job_db->exec("CREATE TABLE IF NOT EXISTS jobs_history \
                 (id INTEGER PRIMARY KEY, \
                  jobid_high INTEGER, \
                  jobid_low INTEGER, \
                  jobtype INTEGER, \
                  subtype INTEGER, \
                  pids TEXT, \
                  collectors TEXT, \
                  native_job_id TEXT DEFAULT '', \
                  status INTEGER DEFAULT 0, \
                  end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");
        job_db->exec("CREATE INDEX IF NOT EXISTS idx_history_jobid ON jobs_history (jobid_high, jobid_low);");
        job_db->exec("CREATE INDEX IF NOT EXISTS idx_history_endtime ON jobs_history (end_time);");

        // 迁移 jobs_history 表：添加 native_job_id 列（如果不存在）
        try {
            job_db->exec("ALTER TABLE jobs_history ADD COLUMN native_job_id TEXT DEFAULT ''");
        } catch (const SQLite::Exception& e) {
            // 列已存在则忽略
        }
    }catch(const SQLite::Exception& e){
        spdlog::error("JobRegistry: failed to create jobs table: {}", e.what());
        return false;
    }
    spdlog::info("JobRegistry: init_job_db at {}", db_path);
    
    // 迁移历史数据（一次性）
    // migrate_ended_jobs_to_history();
    
    try{       
        restore_jobs_from_db();
    }
    catch(const std::exception& e){
        // 不影响正常初始化结果
        spdlog::warn("JobRegistry: failed to restore jobs from db: {}", e.what());
    }
    
    return true;
}

void JobRegistry::deinit_job_db(){
    job_db.reset();
    spdlog::info("JobRegistry: deinit_job_db");
}

void JobRegistry::restore_jobs_from_db(){
    SQLite::Transaction transaction(*job_db);
    SQLite::Statement query(*job_db, "SELECT jobid_high, jobid_low, jobtype, subtype, pids, collectors, native_job_id, status FROM jobs;");
    while (query.executeStep()) {
        Job job;
        uint32_t high = static_cast<uint32_t>(query.getColumn(0).getInt());
        uint32_t low = static_cast<uint32_t>(query.getColumn(1).getInt());
        job.JobID = combine_high_low(high, low);
        job.jobtype = static_cast<JobType>(query.getColumn(2).getInt());
        job.subtype = static_cast<JobSubType>(query.getColumn(3).getInt());
        std::string pids_str = query.getColumn(4).getString();
        std::string collectors_str = query.getColumn(5).getString();
        // 解析 pids 和 collectors
        nlohmann::json pids_json = nlohmann::json::parse(pids_str);
        nlohmann::json collectors_json = nlohmann::json::parse(collectors_str);
        job.JobPIDs = pids_json.get<std::vector<int>>();
        job.CollectorNames = collectors_json.get<std::vector<std::string>>();
        job.NativeJobID = query.getColumn(6).getString();

        if (job.subtype == JobSubType::Condor) {
            job.sub_attr = CondorJobAttr{};
        } else if (job.subtype == JobSubType::Slurm) {
            job.sub_attr = SlurmJobAttr{};
        } else if (job.subtype == JobSubType::Common) {
            job.sub_attr = CommonJobAttr{
                .auto_update_child = true
            };
        }

        
        restore_jobs_.push_back(std::move(job));
        
        spdlog::debug("JobRegistry: restored job with JobID {} from db", job.JobID);
    }
    spdlog::info("JobRegistry: restored {} jobs from db", restore_jobs_.size());
    transaction.commit();
    return;
}

void JobRegistry::persist_new_job(const Job& job)
{
    if (!db_running) return;

    std::lock_guard<std::mutex> db_lock(db_mtx_);
    SQLite::Transaction transaction(*job_db);

    try
    {
        SQLite::Statement query(*job_db,
            "INSERT INTO jobs(jobid_high, jobid_low, jobtype, subtype, pids, collectors, native_job_id, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 1) "
            "ON CONFLICT(jobid_high, jobid_low) DO UPDATE "
            "SET status = excluded.status, native_job_id = excluded.native_job_id;");

        uint32_t high, low;
        split_uint64(job.JobID, high, low);
        query.bind(1, static_cast<int>(high));
        query.bind(2, static_cast<int>(low));
        query.bind(3, static_cast<int>(job.jobtype));
        query.bind(4, static_cast<int>(job.subtype));

        nlohmann::json pids_json        = job.JobPIDs;
        nlohmann::json collectors_json  = job.CollectorNames;
        query.bind(5, pids_json.dump());
        query.bind(6, collectors_json.dump());
        query.bind(7, job.NativeJobID);

        query.exec();                 // 可能抛出 SQLiteException
        transaction.commit();         // 只有 exec 成功才提交

        spdlog::info("JobRegistry: insert new job with JobID {} to db", job.JobID);
    }
    catch (const SQLite::Exception& e)
    {
        transaction.rollback();

        spdlog::error("JobRegistry: failed to persist JobID {} — SQLite error [{}] {}",
                      job.JobID, e.getErrorCode(), e.what());

        return;
    }
}


void JobRegistry::update_job_info(const Job& job){
    if(!db_running) return;
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    SQLite::Transaction transaction(*job_db);
    try
    {
        SQLite::Statement query(*job_db, "UPDATE jobs SET pids = ?, collectors = ?, native_job_id = ? WHERE jobid_high = ? AND jobid_low = ?;");
        nlohmann::json pids_json = job.JobPIDs;
        nlohmann::json collectors_json = job.CollectorNames;
        query.bind(1, pids_json.dump());
        query.bind(2, collectors_json.dump());
        query.bind(3, job.NativeJobID);
        
        uint32_t high, low;
        split_uint64(job.JobID, high, low);
        query.bind(4, static_cast<int>(high));
        query.bind(5, static_cast<int>(low));
        
        query.exec();
        transaction.commit();
        spdlog::info("JobRegistry: update job info with JobID {} in db", job.JobID);
    }
    catch(const SQLite::Exception& e)
    {
        transaction.rollback();

        spdlog::error("JobRegistry: failed to update JobID {} — SQLite error [{}] {}",
                      job.JobID, e.getErrorCode(), e.what());

        return;
    }
}
    

void JobRegistry::end_job_in_db(uint64_t jobID){
    if(!db_running) return;
    
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    SQLite::Transaction transaction(*job_db);
    
    try
    {
        // 1. 从 jobs 表查询任务数据
        SQLite::Statement select_query(*job_db, 
            "SELECT jobid_high, jobid_low, jobtype, subtype, pids, collectors, native_job_id, status FROM jobs WHERE jobid_high = ? AND jobid_low = ?;");
        
        uint32_t high, low;
        split_uint64(jobID, high, low);
        select_query.bind(1, static_cast<int>(high));
        select_query.bind(2, static_cast<int>(low));
        
        if (!select_query.executeStep()) {
            spdlog::warn("JobRegistry: job {} not found in db when ending", jobID);
            transaction.rollback();
            return;
        }
        
        // 2. 插入到 history 表
        SQLite::Statement insert_query(*job_db,
            "INSERT INTO jobs_history(jobid_high, jobid_low, jobtype, subtype, pids, collectors, native_job_id, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 0);");
        
        insert_query.bind(1, select_query.getColumn(0).getInt());
        insert_query.bind(2, select_query.getColumn(1).getInt());
        insert_query.bind(3, select_query.getColumn(2).getInt());
        insert_query.bind(4, select_query.getColumn(3).getInt());
        insert_query.bind(5, select_query.getColumn(4).getString());
        insert_query.bind(6, select_query.getColumn(5).getString());
        insert_query.bind(7, select_query.getColumn(6).getString());
        insert_query.exec();
        
        // 3. 从 jobs 表删除
        SQLite::Statement delete_query(*job_db, "DELETE FROM jobs WHERE jobid_high = ? AND jobid_low = ?;");
        delete_query.bind(1, static_cast<int>(high));
        delete_query.bind(2, static_cast<int>(low));
        delete_query.exec();
        
        transaction.commit();
        spdlog::info("JobRegistry: moved ended job {} from jobs to jobs_history", jobID);
    }
    catch(const SQLite::Exception& e)
    {
        transaction.rollback();
        spdlog::error("JobRegistry: failed to end_job_in_db for JobID {} — SQLite error [{}] {}",
                      jobID, e.getErrorCode(), e.what());
        return;
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
        // 创建计数器表（如果不存在）
        job_db->exec("CREATE TABLE IF NOT EXISTS job_id_counter ( \
                     id INTEGER PRIMARY KEY CHECK(id = 1), \
                     next_job_id INTEGER NOT NULL DEFAULT 1);");
        
        // 初始化计数器记录（如果不存在）
        job_db->exec("INSERT OR IGNORE INTO job_id_counter (id, next_job_id) VALUES (1, 1);");

        // 先从计数器表读取值
        SQLite::Statement query(*job_db, "SELECT next_job_id FROM job_id_counter WHERE id = 1;");
        uint64_t counter_value = 1;
        if (query.executeStep()) {
            counter_value = static_cast<uint64_t>(query.getColumn(0).getInt64());
        }

        // 从jobs表和jobs_history表中找出最大的JobID
        uint64_t max_job_id = 0;
        
        // 检查jobs表
        SQLite::Statement query_jobs(*job_db, 
            "SELECT MAX((CAST(jobid_high AS INTEGER) << 32) | CAST(jobid_low AS INTEGER)) as max_id FROM jobs;");
        if (query_jobs.executeStep() && !query_jobs.getColumn(0).isNull()) {
            max_job_id = static_cast<uint64_t>(query_jobs.getColumn(0).getInt64());
        }

        // 检查jobs_history表
        SQLite::Statement query_history(*job_db, 
            "SELECT MAX((CAST(jobid_high AS INTEGER) << 32) | CAST(jobid_low AS INTEGER)) as max_id FROM jobs_history;");
        if (query_history.executeStep() && !query_history.getColumn(0).isNull()) {
            uint64_t history_max = static_cast<uint64_t>(query_history.getColumn(0).getInt64());
            if (history_max > max_job_id) {
                max_job_id = history_max;
            }
        }

        // 取最大值 + 1 作为下一个可用的JobID
        next_job_id_ = std::max(counter_value, max_job_id + 1);
        
        // 更新计数器表
        SQLite::Statement update(*job_db, 
            "INSERT INTO job_id_counter (id, next_job_id) VALUES (1, ?) \
             ON CONFLICT(id) DO UPDATE SET next_job_id = excluded.next_job_id;");
        update.bind(1, static_cast<int64_t>(next_job_id_));
        update.exec();

        spdlog::info("JobRegistry: initialized job_id_counter, next_job_id_ = {}", next_job_id_);
    } catch (const SQLite::Exception& e) {
        spdlog::error("JobRegistry: failed to init job_id_counter: {}", e.what());
        next_job_id_ = 1;
    }
}

void JobRegistry::persist_job_id_counter() {
    if (!db_running) return;

    std::lock_guard<std::mutex> db_lock(db_mtx_);
    try {
        SQLite::Statement query(*job_db, 
            "INSERT INTO job_id_counter (id, next_job_id) VALUES (1, ?) \
             ON CONFLICT(id) DO UPDATE SET next_job_id = excluded.next_job_id;");
        query.bind(1, static_cast<int64_t>(next_job_id_));
        query.exec();
    } catch (const SQLite::Exception& e) {
        spdlog::error("JobRegistry: failed to persist job_id_counter: {}", e.what());
    }
}

uint64_t JobRegistry::getNextJobID() {
    std::lock_guard<std::mutex> lg(job_id_mtx_);
    uint64_t job_id = next_job_id_++;
    persist_job_id_counter();
    spdlog::debug("JobRegistry: allocated new JobID {}", job_id);
    return job_id;
}