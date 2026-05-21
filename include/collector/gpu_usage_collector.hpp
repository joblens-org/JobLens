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
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE

#include "core/collector_type.h"
#include "icollector.h"
#include <spdlog/spdlog.h>
#include <dlfcn.h>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

/* ===================== Minimal inline NVML types (no nvml.h at compile time) ===================== */

typedef struct nvmlDevice_st* nvmlDevice_t;

typedef int nvmlReturn_t;
#define NVML_SUCCESS 0

#define NVML_DEVICE_NAME_BUFFER_SIZE 64

typedef struct nvmlUtilization_st {
    unsigned int gpu;
    unsigned int memory;
} nvmlUtilization_t;

typedef struct nvmlMemory_st {
    unsigned long long total;
    unsigned long long free;
    unsigned long long used;
} nvmlMemory_t;

typedef struct nvmlProcessInfo_st {
    unsigned int        pid;
    unsigned long long  usedGpuMemory;
    unsigned int        gpuInstanceId;
    unsigned int        computeInstanceId;
} nvmlProcessInfo_t;

typedef struct nvmlProcessUtilizationSample_st {
    unsigned int        pid;
    unsigned long long  timeStamp;
    unsigned int        smUtil;
    unsigned int        memUtil;
    unsigned int        encUtil;
    unsigned int        decUtil;
} nvmlProcessUtilizationSample_t;

/* ===================== Data structures ===================== */

// Layer 2: 进程在单张 GPU 上的使用详情
struct GPUDeviceUsage {
    int gpu_index = -1;
    std::string gpu_name;
    uint64_t mem_used_bytes = 0;           // 该进程在此卡上的显存用量
    uint32_t sm_util = 0;                  // 进程在此卡上的 SM 利用率 (%)
    uint32_t mem_bw_util = 0;              // 进程在此卡上的显存带宽利用率 (%)
    uint32_t gpu_util = 0;                 // 此卡整体 GPU 利用率 (%)
    uint32_t gpu_mem_bw_util = 0;          // 此卡整体显存控制器带宽利用率 (%)
    uint64_t gpu_mem_total_bytes = 0;      // 此卡总显存
    std::string process_type;              // "Compute" / "Graphics" / "Both"
};

// Layer 1: 进程跨多卡汇总 (或 Job 汇总, 两者格式一致)
struct GPUProcessUsage {
    pid_t pid = 0;                         // 进程 PID (Job 汇总时为 0)
    std::string name;                      // 标识名 (进程名或 "JOB-SUMMARY")

    uint32_t gpu_count = 0;                // 涉及多少张 GPU (去重)
    uint64_t total_mem_used_bytes = 0;     // 跨卡显存总占用
    double avg_sm_util = 0.0;              // 跨卡平均 SM 利用率 (%)
    double avg_mem_bw_util = 0.0;          // 跨卡平均显存带宽利用率 (%)
    double avg_gpu_util = 0.0;             // 跨卡平均 GPU 利用率 (%)
    double avg_gpu_mem_bw_util = 0.0;      // 跨卡平均显存控制器带宽利用率 (%)
    uint64_t total_gpu_mem_bytes = 0;      // 涉及 GPU 的去重总显存之和

    std::vector<GPUDeviceUsage> devices;   // 每张卡的明细列表
};

/* ===================== Collector ===================== */

class GPUUsageCollector : public ICollector {
public:
    bool init(const nlohmann::json& cfg) override;
    CollectResult collect(const Job& job) override;
    void deinit() noexcept override;
    CollectDataParseFunc get_writer_parser(const std::string& writer_type);

private:
    bool inited = false;
    bool summary = false;
    bool nvml_initialized = false;

    // 全局 GPU 缓存: 按 refresh_interval 频率刷新, 避免每次采集都拉取
    struct GPUCachedInfo {
        uint32_t gpu_util = 0;
        uint32_t gpu_mem_bw_util = 0;
        uint64_t gpu_mem_total_bytes = 0;
    };
    std::vector<GPUCachedInfo> gpu_global_cache_;
    std::chrono::milliseconds refresh_interval_{};
    std::chrono::steady_clock::time_point last_refresh_{};

    struct GPUHandleInfo {
        nvmlDevice_t device{};
        std::string name;
    };
    std::vector<GPUHandleInfo> gpu_handles_;

    /* ---- NVML dynamic loader ---- */
    struct NvmlLoader {
        void* handle = nullptr;

        nvmlReturn_t (*nvmlInit)(void) = nullptr;
        nvmlReturn_t (*nvmlShutdown)(void) = nullptr;
        const char*  (*nvmlErrorString)(nvmlReturn_t) = nullptr;

        nvmlReturn_t (*nvmlDeviceGetCount)(unsigned int*) = nullptr;
        nvmlReturn_t (*nvmlDeviceGetHandleByIndex)(unsigned int, nvmlDevice_t*) = nullptr;
        nvmlReturn_t (*nvmlDeviceGetName)(nvmlDevice_t, char*, unsigned int) = nullptr;
        nvmlReturn_t (*nvmlDeviceGetUtilizationRates)(nvmlDevice_t, nvmlUtilization_t*) = nullptr;
        nvmlReturn_t (*nvmlDeviceGetMemoryInfo)(nvmlDevice_t, nvmlMemory_t*) = nullptr;

        nvmlReturn_t (*nvmlDeviceGetComputeRunningProcesses)(nvmlDevice_t, unsigned int*, nvmlProcessInfo_t*) = nullptr;
        nvmlReturn_t (*nvmlDeviceGetGraphicsRunningProcesses)(nvmlDevice_t, unsigned int*, nvmlProcessInfo_t*) = nullptr;
        nvmlReturn_t (*nvmlDeviceGetProcessUtilization)(nvmlDevice_t, nvmlProcessUtilizationSample_t*, unsigned int*, unsigned long long) = nullptr;
    };
    NvmlLoader nvml_;

    bool load_nvml();
    bool init_nvml();
    void shutdown_nvml();
    bool enum_gpus();

    using ProcUtilMap = std::unordered_map<pid_t, nvmlProcessUtilizationSample_t>;
    ProcUtilMap query_process_utilization(nvmlDevice_t device);

    void match_processes(
        const Job& job,
        nvmlDevice_t device,
        int gpu_index,
        const std::string& gpu_name,
        const GPUCachedInfo& cache,
        const ProcUtilMap& proc_util_map,
        bool compute,
        std::unordered_map<pid_t, std::vector<GPUDeviceUsage>>& pid_devices);

    GPUProcessUsage aggregate_process(pid_t pid, const std::vector<GPUDeviceUsage>& devices);
    GPUProcessUsage aggregate_job(const std::vector<GPUProcessUsage>& process_usages);
};
