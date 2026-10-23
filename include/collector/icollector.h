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
// icollector.h
#pragma once
#include <string>
#include <utility>
#include <vector>
#include "core/collector_type.h"
#include <nlohmann/json.hpp>

class ICollector {
public:
    virtual ~ICollector() = default;
    const CollectorScope scope() const { return scope_; }
    // 生命周期
    virtual bool init(const nlohmann::json& config)      = 0;   // 返回 false 表示失败
    virtual void deinit() noexcept                       = 0;

    // ------------------------------------------------------------------
    // 原生输出接口（Collector-Controlled Output）
    //
    // Job 作用域绑定 collect(job, output)；System 作用域绑定 collect(output)，
    // 上下文内携带合成 Job 0。默认实现返回 Skipped（没有可用样本）。
    // ------------------------------------------------------------------
    virtual CollectOutcome collect(const Job& job, OutputContext& output) {
        (void)job; (void)output;
        return {CollectState::Skipped, {}};
    }
    virtual CollectOutcome collect(OutputContext& output) {
        (void)output;
        return {CollectState::Skipped, {}};
    }

    /// 采集器声明的输出通道。只有声明且被配置的通道会收到记录。
    virtual std::vector<OutputChannel> output_channels() const = 0;

    // ------------------------------------------------------------------
    // Job 生命周期钩子（同步调用，默认空实现，由采集器实例自行按需覆写）
    //
    // 语义：
    //   - Job 作用域采集器：只针对 CollectorNames 中包含本采集器的 Job。
    //       Added   = 首次挂载到该 Job（采集器运行中才会触发）
    //       Updated = 已挂载状态下 Job 属性 / PID 列表发生变化
    //       Removed = 从该 Job 摘除（Job 结束时，或 Job 的采集器配置变更时）
    //     Job 的采集器配置变更会同时产生 Removed（被移除的采集器）与 Added（新增的采集器），
    //     继续保留的采集器只收到 Updated。
    //   - System 作用域采集器：对全机所有 Job 的生命周期事件广播（采集器已 init 且在运行）。
    //   - 系统占位 Job(JobID == 0) 不触发任何钩子。
    //
    // 调用约束（实现必须遵守，否则会阻塞采集或自锁）：
    //   - 在调度线程 / Job 管理线程中同步执行，且调度器持有该采集器的状态锁，
    //     必须快速返回，禁止阻塞等待、禁止长时间 IO；
    //   - 必须幂等：Update 可能重复到达，Removed 可能被重复摘除，不能假设严格配对；
    //   - 禁止回调 JobRegistry / CollectorScheduler（同一线程取同一把锁会自锁），
    //     只能用传入的 job 快照；需要在钩子里做其它事情请转交给自己的线程/队列；
    //   - Removed 触发时 Job 已从 JobRegistry 移除，findJob 不会再返回它。
    // ------------------------------------------------------------------
    virtual void on_job_added(const Job& /*job*/) {}
    virtual void on_job_updated(const Job& /*job*/) {}
    virtual void on_job_removed(const Job& /*job*/) {}

    void set_type(std::string type){
        type_ = std::move(type);
    }
    void set_name(std::string name){
        name_ = std::move(name);
    }
    std::string get_name() const {
        return name_;
    }
    std::string get_type() const {
        return type_;
    }
protected:
    CollectorScope scope_;
private:
    std::string name_;
    std::string type_;
};
