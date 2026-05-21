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
#include "collector/gpu_usage_collector.hpp"
#include "core/collector_registry.hpp"
#include "writer/prometheus_exporter_writer.hpp"
#include "common/utils.hpp"
#include <fmt/format.h>
#include <algorithm>
#include <set>

AUTO_REGISTER_JOB_COLLECTOR(
    GPUUsageCollector,
    "Collect per-job GPU usage statistics via NVML (dynamically loaded)",
    ConfigParams{
        {"freq", "Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds"},
        {"summary", "Whether to summarize data across all processes (true/false), default false"}
    }
)

/* ---- NVML dynamic loader helpers ---- */

static void* resolve_sym(void* handle, const char* name) {
    void* sym = dlsym(handle, name);
    return sym;
}

#define RESOLVE_NVML(fn)                                       \
    do {                                                       \
        nvml_.fn = reinterpret_cast<decltype(nvml_.fn)>(resolve_sym(nvml_.handle, #fn)); \
        if (!nvml_.fn) {                                       \
            spdlog::warn("GPUUsageCollector: failed to resolve symbol: {}", #fn); \
        }                                                      \
    } while (0)

#define RESOLVE_NVML_FALLBACK(fn, fallback)                    \
    do {                                                       \
        nvml_.fn = reinterpret_cast<decltype(nvml_.fn)>(resolve_sym(nvml_.handle, #fn)); \
        if (!nvml_.fn) {                                       \
            nvml_.fn = reinterpret_cast<decltype(nvml_.fn)>(resolve_sym(nvml_.handle, fallback)); \
            if (!nvml_.fn) {                                   \
                spdlog::warn("GPUUsageCollector: failed to resolve symbol: {} / {}", #fn, fallback); \
            }                                                  \
        }                                                      \
    } while (0)

bool GPUUsageCollector::load_nvml() {
    nvml_.handle = dlopen("libnvidia-ml.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!nvml_.handle) {
        nvml_.handle = dlopen("libnvidia-ml.so", RTLD_NOW | RTLD_LOCAL);
    }
    if (!nvml_.handle) {
        spdlog::warn("GPUUsageCollector: dlopen libnvidia-ml.so.1 / libnvidia-ml.so failed: {}", dlerror());
        return false;
    }

    RESOLVE_NVML(nvmlInit);
    RESOLVE_NVML(nvmlShutdown);
    RESOLVE_NVML(nvmlErrorString);
    RESOLVE_NVML(nvmlDeviceGetCount);
    RESOLVE_NVML(nvmlDeviceGetHandleByIndex);
    RESOLVE_NVML(nvmlDeviceGetName);
    RESOLVE_NVML(nvmlDeviceGetUtilizationRates);
    RESOLVE_NVML(nvmlDeviceGetMemoryInfo);

    // Versioned macros may map these to _v3; try unversioned first, then versioned fallbacks.
    RESOLVE_NVML_FALLBACK(nvmlDeviceGetComputeRunningProcesses,  "nvmlDeviceGetComputeRunningProcesses_v3");
    RESOLVE_NVML_FALLBACK(nvmlDeviceGetGraphicsRunningProcesses, "nvmlDeviceGetGraphicsRunningProcesses_v3");
    RESOLVE_NVML(nvmlDeviceGetProcessUtilization);

    // Mandatory symbols for basic operation
    if (!nvml_.nvmlInit || !nvml_.nvmlShutdown || !nvml_.nvmlErrorString ||
        !nvml_.nvmlDeviceGetCount || !nvml_.nvmlDeviceGetHandleByIndex ||
        !nvml_.nvmlDeviceGetName || !nvml_.nvmlDeviceGetUtilizationRates ||
        !nvml_.nvmlDeviceGetMemoryInfo ||
        !nvml_.nvmlDeviceGetComputeRunningProcesses || !nvml_.nvmlDeviceGetGraphicsRunningProcesses) {
        spdlog::error("GPUUsageCollector: one or more mandatory NVML symbols missing");
        dlclose(nvml_.handle);
        nvml_.handle = nullptr;
        return false;
    }

    return true;
}

bool GPUUsageCollector::init_nvml() {
    if (!nvml_.nvmlInit) {
        return false;
    }
    nvmlReturn_t ret = nvml_.nvmlInit();
    if (ret != NVML_SUCCESS) {
        const char* err = nvml_.nvmlErrorString ? nvml_.nvmlErrorString(ret) : "unknown";
        spdlog::error("GPUUsageCollector: nvmlInit failed: {}", err);
        return false;
    }
    nvml_initialized = true;
    return true;
}

void GPUUsageCollector::shutdown_nvml() {
    if (nvml_initialized && nvml_.nvmlShutdown) {
        nvml_.nvmlShutdown();
        nvml_initialized = false;
    }
    gpu_handles_.clear();
    if (nvml_.handle) {
        dlclose(nvml_.handle);
        nvml_.handle = nullptr;
    }
    // zero out function pointers
    nvml_ = NvmlLoader{};
}

bool GPUUsageCollector::enum_gpus() {
    if (!nvml_.nvmlDeviceGetCount) {
        return false;
    }
    unsigned int count = 0;
    nvmlReturn_t ret = nvml_.nvmlDeviceGetCount(&count);
    if (ret != NVML_SUCCESS) {
        const char* err = nvml_.nvmlErrorString ? nvml_.nvmlErrorString(ret) : "unknown";
        spdlog::error("GPUUsageCollector: nvmlDeviceGetCount failed: {}", err);
        return false;
    }

    gpu_handles_.clear();
    for (unsigned int i = 0; i < count; ++i) {
        nvmlDevice_t device;
        ret = nvml_.nvmlDeviceGetHandleByIndex(i, &device);
        if (ret != NVML_SUCCESS) {
            const char* err = nvml_.nvmlErrorString ? nvml_.nvmlErrorString(ret) : "unknown";
            spdlog::warn("GPUUsageCollector: nvmlDeviceGetHandleByIndex({}) failed: {}", i, err);
            continue;
        }
        char name[NVML_DEVICE_NAME_BUFFER_SIZE] = {};
        ret = nvml_.nvmlDeviceGetName(device, name, sizeof(name));
        if (ret != NVML_SUCCESS) {
            const char* err = nvml_.nvmlErrorString ? nvml_.nvmlErrorString(ret) : "unknown";
            spdlog::warn("GPUUsageCollector: nvmlDeviceGetName({}) failed: {}", i, err);
        }
        gpu_handles_.push_back({device, std::string(name)});
    }
    return true;
}

bool GPUUsageCollector::init(const nlohmann::json& cfg) {
    if (inited) {
        spdlog::warn("GPUUsageCollector init twice with config: {}", cfg.dump());
        return false;
    }

    if (cfg.contains("summary") && cfg["summary"].get<std::string>() == "true") {
        summary = true;
    } else {
        summary = false;
    }

    // 根据采集频率计算全局 GPU 缓存的刷新间隔 = 1 / (freq * 1.5)
    double freq_val = 1.0;
    if (cfg.contains("freq")) {
        try {
            freq_val = std::stod(cfg["freq"].get<std::string>());
        } catch (...) {
            spdlog::warn("GPUUsageCollector: invalid freq value, using default 1.0 Hz");
        }
    }
    if (freq_val <= 0.0) freq_val = 1.0;
    double refresh_sec = 1.0 / (freq_val * 1.5);
    refresh_interval_ = std::chrono::milliseconds(static_cast<long long>(refresh_sec * 1000));
    spdlog::info("GPUUsageCollector: freq={} Hz, refresh_interval={} ms", freq_val, refresh_interval_.count());

    if (!load_nvml()) {
        return false;
    }
    if (!init_nvml()) {
        shutdown_nvml();
        return false;
    }
    if (!enum_gpus()) {
        shutdown_nvml();
        return false;
    }

    inited = true;
    spdlog::info("GPUUsageCollector init ok, found {} GPU(s)", gpu_handles_.size());
    return true;
}

void GPUUsageCollector::deinit() noexcept {
    if (!inited) {
        return;
    }
    shutdown_nvml();
    gpu_global_cache_.clear();
    inited = false;
    spdlog::info("GPUUsageCollector deinit");
}

GPUUsageCollector::ProcUtilMap GPUUsageCollector::query_process_utilization(nvmlDevice_t device) {
    ProcUtilMap result;
    if (!nvml_.nvmlDeviceGetProcessUtilization) {
        return result;
    }
    unsigned int sample_count = 0;
    nvmlReturn_t ret = nvml_.nvmlDeviceGetProcessUtilization(device, nullptr, &sample_count, 0);
    if (ret != NVML_SUCCESS && ret != ((nvmlReturn_t)7) /* NVML_ERROR_INSUFFICIENT_SIZE approx */) {
        return result;
    }
    if (sample_count == 0) {
        return result;
    }

    std::vector<nvmlProcessUtilizationSample_t> samples(sample_count);
    ret = nvml_.nvmlDeviceGetProcessUtilization(device, samples.data(), &sample_count, 0);
    if (ret != NVML_SUCCESS) {
        return result;
    }

    for (unsigned int i = 0; i < sample_count; ++i) {
        result[samples[i].pid] = samples[i];
    }
    return result;
}

void GPUUsageCollector::match_processes(
    const Job& job,
    nvmlDevice_t device,
    int gpu_index,
    const std::string& gpu_name,
    const GPUCachedInfo& cache,
    const ProcUtilMap& proc_util_map,
    bool compute,
    std::unordered_map<pid_t, std::vector<GPUDeviceUsage>>& pid_devices) {

    unsigned int count = 0;
    nvmlReturn_t ret;
    auto* fn = compute ? nvml_.nvmlDeviceGetComputeRunningProcesses
                       : nvml_.nvmlDeviceGetGraphicsRunningProcesses;
    if (!fn) {
        return;
    }

    ret = fn(device, &count, nullptr);
    if (ret != NVML_SUCCESS && ret != ((nvmlReturn_t)7)) {
        return;
    }
    if (count == 0) {
        return;
    }

    std::vector<nvmlProcessInfo_t> infos(count);
    ret = fn(device, &count, infos.data());
    if (ret != NVML_SUCCESS) {
        return;
    }

    const char* process_type_name = compute ? "Compute" : "Graphics";

    for (unsigned int i = 0; i < count; ++i) {
        pid_t pid = static_cast<pid_t>(infos[i].pid);
        if (std::find(job.JobPIDs.begin(), job.JobPIDs.end(), pid) == job.JobPIDs.end()) {
            continue;
        }

        // 构造此 PID 在此 GPU 上的新条目
        GPUDeviceUsage usage;
        usage.gpu_index = gpu_index;
        usage.gpu_name = gpu_name;
        usage.mem_used_bytes = infos[i].usedGpuMemory;
        usage.gpu_util = cache.gpu_util;
        usage.gpu_mem_bw_util = cache.gpu_mem_bw_util;
        usage.gpu_mem_total_bytes = cache.gpu_mem_total_bytes;
        usage.process_type = process_type_name;

        auto it = proc_util_map.find(pid);
        if (it != proc_util_map.end()) {
            usage.sm_util = it->second.smUtil;
            usage.mem_bw_util = it->second.memUtil;
        }

        // 检查是否已有同 PID + 同 GPU 的条目 (Compute 和 Graphics 合并为 "Both")
        auto& devices = pid_devices[pid];
        auto dup_it = std::find_if(devices.begin(), devices.end(),
            [gpu_index](const GPUDeviceUsage& d) { return d.gpu_index == gpu_index; });

        if (dup_it != devices.end()) {
            // 合并: 显存累加, 利用率取最大值
            dup_it->mem_used_bytes += usage.mem_used_bytes;
            dup_it->sm_util = std::max(dup_it->sm_util, usage.sm_util);
            dup_it->mem_bw_util = std::max(dup_it->mem_bw_util, usage.mem_bw_util);
            dup_it->gpu_util = std::max(dup_it->gpu_util, usage.gpu_util);
            dup_it->gpu_mem_bw_util = std::max(dup_it->gpu_mem_bw_util, usage.gpu_mem_bw_util);
            dup_it->process_type = "Both";
        } else {
            devices.push_back(usage);
        }
    }
}

GPUProcessUsage GPUUsageCollector::aggregate_process(pid_t pid, const std::vector<GPUDeviceUsage>& devices) {
    GPUProcessUsage pu;
    pu.pid = pid;

    std::set<int> seen_gpus;
    double sum_sm = 0.0, sum_mbw = 0.0, sum_gpu = 0.0, sum_gmbw = 0.0;
    size_t device_count = devices.size();

    for (const auto& d : devices) {
        pu.total_mem_used_bytes += d.mem_used_bytes;
        sum_sm += d.sm_util;
        sum_mbw += d.mem_bw_util;
        sum_gpu += d.gpu_util;
        sum_gmbw += d.gpu_mem_bw_util;

        if (seen_gpus.insert(d.gpu_index).second) {
            pu.gpu_count++;
            pu.total_gpu_mem_bytes += d.gpu_mem_total_bytes;
        }
    }

    if (device_count > 0) {
        pu.avg_sm_util = sum_sm / static_cast<double>(device_count);
        pu.avg_mem_bw_util = sum_mbw / static_cast<double>(device_count);
        pu.avg_gpu_util = sum_gpu / static_cast<double>(device_count);
        pu.avg_gpu_mem_bw_util = sum_gmbw / static_cast<double>(device_count);
    }

    pu.devices = devices;
    return pu;
}

GPUProcessUsage GPUUsageCollector::aggregate_job(const std::vector<GPUProcessUsage>& process_usages) {
    GPUProcessUsage job;
    job.pid = 0;
    job.name = "JOB-SUMMARY";

    // 按 GPU 聚合所有进程的设备详情 (利用率累加)
    std::unordered_map<int, GPUDeviceUsage> gpu_agg;
    std::set<int> seen_gpus;

    double total_sm = 0.0, total_mbw = 0.0, total_gpu = 0.0, total_gmbw = 0.0;
    size_t proc_count = process_usages.size();

    for (const auto& pu : process_usages) {
        job.total_mem_used_bytes += pu.total_mem_used_bytes;
        total_sm += pu.avg_sm_util;
        total_mbw += pu.avg_mem_bw_util;
        total_gpu += pu.avg_gpu_util;
        total_gmbw += pu.avg_gpu_mem_bw_util;

        for (const auto& d : pu.devices) {
            auto& gd = gpu_agg[d.gpu_index];
            gd.gpu_index = d.gpu_index;
            gd.gpu_name = d.gpu_name;
            gd.mem_used_bytes += d.mem_used_bytes;
            gd.sm_util += d.sm_util;                        // 跨进程累加
            gd.mem_bw_util += d.mem_bw_util;                // 跨进程累加
            gd.gpu_util += d.gpu_util;                      // 跨进程累加
            gd.gpu_mem_bw_util += d.gpu_mem_bw_util;        // 跨进程累加
            gd.gpu_mem_total_bytes = d.gpu_mem_total_bytes;  // 同卡一致, 覆盖无影响
            gd.process_type = "Both";

            if (seen_gpus.insert(d.gpu_index).second) {
                job.gpu_count++;
                job.total_gpu_mem_bytes += d.gpu_mem_total_bytes;
            }
        }
    }

    // 取累加和作为任务的使用量
    if (proc_count > 0) {
        job.avg_sm_util = total_sm;
        job.avg_mem_bw_util = total_mbw;
        job.avg_gpu_util = total_gpu;
        job.avg_gpu_mem_bw_util = total_gmbw;
    }

    // 将聚合结果按 gpu_index 排序后放入 devices 列表
    for (auto& [idx, gd] : gpu_agg) {
        job.devices.push_back(std::move(gd));
    }
    std::sort(job.devices.begin(), job.devices.end(),
        [](const GPUDeviceUsage& a, const GPUDeviceUsage& b) { return a.gpu_index < b.gpu_index; });

    return job;
}

CollectResult GPUUsageCollector::collect(const Job& job) {
    if (!inited || gpu_handles_.empty()) {
        spdlog::debug("GPUUsageCollector not initialized or no GPUs available");
        return std::vector<GPUProcessUsage>{};
    }

    // ---- 阶段0: 按需刷新全局 GPU 缓存 ----
    auto now = std::chrono::steady_clock::now();
    if (gpu_global_cache_.empty() || (now - last_refresh_) > refresh_interval_) {
        gpu_global_cache_.clear();
        gpu_global_cache_.reserve(gpu_handles_.size());
        for (size_t gi = 0; gi < gpu_handles_.size(); ++gi) {
            nvmlDevice_t device = gpu_handles_[gi].device;
            GPUCachedInfo cache;

            nvmlUtilization_t utilization{};
            nvmlReturn_t ret = nvml_.nvmlDeviceGetUtilizationRates(device, &utilization);
            if (ret == NVML_SUCCESS) {
                cache.gpu_util = utilization.gpu;
                cache.gpu_mem_bw_util = utilization.memory;
            } else {
                const char* err = nvml_.nvmlErrorString ? nvml_.nvmlErrorString(ret) : "unknown";
                spdlog::debug("GPUUsageCollector: nvmlDeviceGetUtilizationRates failed for GPU {}: {}",
                              gi, err);
            }

            nvmlMemory_t meminfo{};
            ret = nvml_.nvmlDeviceGetMemoryInfo(device, &meminfo);
            if (ret == NVML_SUCCESS) {
                cache.gpu_mem_total_bytes = meminfo.total;
            }

            gpu_global_cache_.push_back(cache);
        }
        last_refresh_ = now;
    }

    // ---- 阶段1: 按 GPU 遍历, 匹配进程并写入 PID → devices 分组 ----
    std::unordered_map<pid_t, std::vector<GPUDeviceUsage>> pid_devices;

    for (size_t gi = 0; gi < gpu_handles_.size(); ++gi) {
        nvmlDevice_t device = gpu_handles_[gi].device;
        const auto& cache = gpu_global_cache_[gi];

        ProcUtilMap proc_util_map = query_process_utilization(device);

        match_processes(job, device, static_cast<int>(gi), gpu_handles_[gi].name,
                        cache, proc_util_map, true, pid_devices);
        match_processes(job, device, static_cast<int>(gi), gpu_handles_[gi].name,
                        cache, proc_util_map, false, pid_devices);
    }

    // ---- 阶段2: 按 PID 聚合为 GPUProcessUsage ----
    std::vector<GPUProcessUsage> results;
    for (auto& [pid, devices] : pid_devices) {
        results.push_back(aggregate_process(pid, devices));
    }

    // ---- 阶段3: Job 汇总 (同格式, pid=0) ----
    if (summary && !results.empty()) {
        results.push_back(aggregate_job(results));
    }

    size_t proc_count = summary && !results.empty() ? results.size() - 1 : results.size();
    spdlog::debug("GPUUsageCollector: collected {} process(es) for job {} (summary={})",
                  proc_count, job.JobID, summary);

    return results;
}

CollectDataParseFunc GPUUsageCollector::get_writer_parser(const std::string& writer_type) {
    CollectDataParseFunc func = nullptr;
    spdlog::debug("GPUUsageCollector: get_writer_parser for writer_type: {}", writer_type);

    if (writer_type.compare("ESWriter") == 0) {
        func = [this](std::any data) -> std::any {
            nlohmann::json ret;
            if (!data.has_value()) {
                spdlog::warn("GPUUsageCollector: error writer parser, empty data");
                ret["error"] = "empty data";
                return ret;
            }
            ret["process_data"] = nlohmann::json::array();
            auto parsed = std::any_cast<std::vector<GPUProcessUsage>>(data);

            for (const auto& pu : parsed) {
                nlohmann::json j;

                j["pid"] = pu.pid;
                j["gpu_count"] = pu.gpu_count;
                j["total_mem_used_bytes"] = pu.total_mem_used_bytes;
                j["total_mem_used_mb"] = pu.total_mem_used_bytes / (1024.0 * 1024.0);
                j["avg_sm_util"] = pu.avg_sm_util;
                j["avg_mem_bw_util"] = pu.avg_mem_bw_util;
                j["avg_gpu_util"] = pu.avg_gpu_util;
                j["avg_gpu_mem_bw_util"] = pu.avg_gpu_mem_bw_util;
                j["total_gpu_mem_bytes"] = pu.total_gpu_mem_bytes;

                // 设备详情列表
                j["devices"] = nlohmann::json::array();
                for (const auto& d : pu.devices) {
                    nlohmann::json dj;
                    dj["gpu_index"] = d.gpu_index;
                    dj["gpu_name"] = d.gpu_name;
                    dj["mem_used_bytes"] = d.mem_used_bytes;
                    dj["mem_used_mb"] = d.mem_used_bytes / (1024.0 * 1024.0);
                    dj["sm_util"] = d.sm_util;
                    dj["mem_bw_util"] = d.mem_bw_util;
                    dj["gpu_util"] = d.gpu_util;
                    dj["gpu_mem_bw_util"] = d.gpu_mem_bw_util;
                    dj["gpu_mem_total_bytes"] = d.gpu_mem_total_bytes;
                    dj["process_type"] = d.process_type;
                    j["devices"].push_back(dj);
                }

                // Job 汇总与进程数据分离存放
                if (pu.pid == 0 && summary) {
                    ret["summary"] = j;
                } else {
                    ret["process_data"].push_back(j);
                }
            }
            return ret;
        };
    }

    if (writer_type.compare("PrometheusExporterWriter") == 0) {
        func = [this](std::any data) -> std::any {
            PrometheusExporterWriter::prometheus_job_state ret;
            if (!data.has_value()) {
                spdlog::warn("GPUUsageCollector: error writer parser, empty data");
                ret.JobID = 0;
                return ret;
            }
            auto parsed = std::any_cast<std::vector<GPUProcessUsage>>(data);
            for (const auto& pu : parsed) {
                for (const auto& d : pu.devices) {
                    PrometheusExporterWriter::prometheus_process_state state;
                    state.pid = pu.pid;
                    state.name = d.gpu_name;
                    state.mem_rss_kb = static_cast<int64_t>(d.mem_used_bytes / 1024);
                    state.mem_usage_percent = 0.0;
                    if (d.gpu_mem_total_bytes > 0) {
                        state.mem_usage_percent = 100.0 * static_cast<double>(d.mem_used_bytes)
                                                  / static_cast<double>(d.gpu_mem_total_bytes);
                    }
                    state.cpu_usage_percent = static_cast<double>(d.sm_util);
                    state.threads_cnt = static_cast<int32_t>(d.gpu_util);
                    ret.processes_state.push_back(state);
                }
            }
            return ret;
        };
    }

    return func;
}
