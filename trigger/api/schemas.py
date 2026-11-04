#   Copyright 2026 - 2026 wzycc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
"""
JobLens Trigger API Schemas

使用 Pydantic 规范所有请求体和响应体
"""

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator


# ==================== 请求体模型 ====================

class JobRequest(BaseModel):
    """通用作业操作请求"""
    opt: str = Field(..., description="操作类型: add/remove")
    type: str = Field(..., description="作业类型: job.condor/job.common")
    JobID: Optional[int] = Field(None, description="作业ID")
    JobPIDs: Optional[List[int]] = Field(None, description="进程ID列表")
    Lens: Optional[List[str]] = Field(None, description="采集器列表，默认 ['cpumem_collector', 'io_collector', 'net_collector']")
    sub_attr: Optional[Dict[str, Any]] = Field(None, description="子属性，condor类型需cluster_id/proc_id，slurm类型需job_id/step_id")


class CondorJobRequest(BaseModel):
    """Condor作业专用请求"""
    opt: str = Field(..., description="操作类型，目前仅支持 'add'")
    JobID: Optional[int] = Field(None, description="作业ID")
    slot: str = Field(..., description="slot名称，必须以 'slot' 开头")
    Lens: Optional[List[str]] = Field(None, description="采集器列表，默认 ['cpumem_collector', 'io_collector', 'net_collector']")
    sub_attr: Optional[Dict[str, Any]] = Field(None, description="子属性，包含cluster_id/proc_id，不提供则默认0")


class SlurmJobRequest(BaseModel):
    """Slurm作业专用请求

    §9.4：JobID 是兼容提示，不是权威身份。原生身份以 sub_attr 的
    job_id/step_id + 调度器命名空间为准，因此 identity-only 的
    legacy add 可以不带 JobID。
    """
    opt: str = Field(..., description="操作类型，目前仅支持 'add'")
    JobID: Optional[int] = Field(None, description="作业ID（兼容提示，可省略）")
    Lens: Optional[List[str]] = Field(None, description="采集器列表，默认 ['cpumem_collector', 'io_collector', 'net_collector']")
    sub_attr: Optional[Dict[str, Any]] = Field(None, description="子属性，包含job_id/step_id，不提供则用JobID/0")


# ==================== 响应体模型 ====================

class ErrorResponse(BaseModel):
    """错误响应"""
    error: str = Field(..., description="错误信息")


class HealthResponse(BaseModel):
    """健康检查响应"""
    active: bool = Field(..., description="服务是否处于 active 状态")
    state: str = Field(..., description="服务状态字符串")
    sub: str = Field(..., description="子状态")
    healthy: bool = Field(..., description="整体健康状态")


class RPCHealthResponse(BaseModel):
    """RPC健康检查响应"""
    rpc_latency_ms: float = Field(..., description="RPC调用延迟(毫秒)")
    # 其他动态字段通过 model_extra 处理
    model_config = {"extra": "allow"}


class JobResponse(BaseModel):
    """作业操作响应"""
    status: str = Field(..., description="操作状态")
    received: Dict[str, Any] = Field(..., description="接收到的数据")


class JobCountResponse(BaseModel):
    """作业数量响应"""
    status: str = Field(..., description="操作状态")
    job_count: int = Field(..., description="作业总数")


class JobsListResponse(BaseModel):
    """作业列表响应"""
    status: str = Field(..., description="操作状态")
    jobs: List[Dict[str, Any]] = Field(..., description="作业列表")


class JobDetailResponse(BaseModel):
    """作业详情响应 - 动态字段"""
    model_config = {"extra": "allow"}


class RPCFunctionsResponse(BaseModel):
    """RPC函数列表响应"""
    status: str = Field(..., description="响应状态")
    functions: List[str] = Field(..., description="可用函数列表")
    count: int = Field(..., description="函数数量")


class PidTrackerStatsResponse(BaseModel):
    """pid 归属追踪状态与 job_event ringbuf 统计响应"""
    status: str = Field(..., description="响应状态")
    running: bool = Field(..., description="归属追踪线程是否运行")
    pushed_events: int = Field(..., description="ringbuf 累计成功入队事件数")
    dropped_events: int = Field(..., description="ringbuf 累计溢出丢弃事件数")


class CollectorsPerfResponse(BaseModel):
    """Collector性能统计响应 - 动态字段"""
    model_config = {"extra": "allow"}


class WritersPerfResponse(BaseModel):
    """Writer性能统计响应 - 动态字段"""
    model_config = {"extra": "allow"}


class WriterInfoResponse(BaseModel):
    """Writer信息响应 - 动态字段"""
    model_config = {"extra": "allow"}


class ConfigUpdateResponse(BaseModel):
    """配置更新响应"""
    status: str = Field(..., description="操作状态")
    message: str = Field(..., description="响应消息")


class ConfigStatusResponse(BaseModel):
    """配置管理器状态响应"""
    enabled: bool = Field(..., description="是否启用")
    running: Optional[bool] = Field(None, description="是否运行中")
    mode: Optional[str] = Field(None, description="当前配置模式")
    use_etcd: Optional[bool] = Field(None, description="是否使用 etcd")
    sync_status: Optional[Dict[str, Any]] = Field(None, description="同步状态")
    message: Optional[str] = Field(None, description="状态消息")


class RegistryStatusResponse(BaseModel):
    """服务注册状态响应"""
    enabled: bool = Field(..., description="是否启用")
    registered: Optional[bool] = Field(None, description="是否已注册")
    service_id: Optional[str] = Field(None, description="服务ID")
    service_host: Optional[str] = Field(None, description="服务主机")
    service_port: Optional[int] = Field(None, description="服务端口")
    registry_url: Optional[str] = Field(None, description="注册中心URL")
    version: Optional[str] = Field(None, description="版本信息")
    etcd_path: Optional[str] = Field(None, description="etcd路径")
    etcd_workdir: Optional[str] = Field(None, description="etcd工作目录")
    etcd_addr: Optional[str] = Field(None, description="etcd地址")
    etcd_port: Optional[int] = Field(None, description="etcd端口")
    message: Optional[str] = Field(None, description="状态消息")


class RegistryRegisterResponse(BaseModel):
    """服务注册响应"""
    status: str = Field(..., description="操作状态")
    message: str = Field(..., description="响应消息")


class RulesListResponse(BaseModel):
    """规则列表响应"""
    enabled: bool = Field(..., description="是否启用")
    count: int = Field(..., description="规则数量")
    rules: List[str] = Field(..., description="规则名称列表")
    message: Optional[str] = Field(None, description="状态消息")


class RulesSyncResponse(BaseModel):
    """规则同步响应"""
    status: str = Field(..., description="操作状态")
    message: str = Field(..., description="响应消息")


class RuleStatusResponse(BaseModel):
    """规则管理器状态响应 - 复用 RuleManagerStatus 结构"""
    enabled: bool = Field(..., description="是否启用")
    running: bool = Field(..., description="是否运行中")
    etcd_priority: bool = Field(..., description="etcd是否优先")
    etcd_role_path: str = Field(..., description="etcd角色路径")
    etcd_workdir_prefix: str = Field(..., description="etcd工作目录前缀")
    local_rules_dir: str = Field(..., description="本地规则目录")
    role_id: str = Field(..., description="角色ID")
    local_rules_count: int = Field(..., description="本地规则数量")
    remote_rules_count: int = Field(..., description="远程规则数量")
    callbacks_count: int = Field(..., description="回调函数数量")
    etcd_role_watch_id: Optional[str] = Field(None, description="etcd角色watch ID")
    etcd_role_info_watch_id: Optional[str] = Field(None, description="etcd角色信息watch ID")
    file_watcher_running: bool = Field(..., description="文件监控是否运行")
    syncing: bool = Field(..., description="是否正在同步")
    sync_lock_locked: bool = Field(..., description="同步锁是否锁定")


class VersionResponse(BaseModel):
    """版本信息响应"""
    trigger_version: str = Field(..., description="Trigger版本")
    joblens_version: str = Field(..., description="JobLens版本")
    build_id: str = Field(..., description="构建ID")
    build_time: str = Field(..., description="构建时间")


class UpgradeResponse(BaseModel):
    """升级响应"""
    status: str = Field(..., description="操作状态")
    message: str = Field(..., description="响应消息")
    pid: int = Field(..., description="升级进程PID")
    log_file: str = Field(..., description="日志文件路径")


class HardwareInfoResponse(BaseModel):
    """硬件信息响应 - 动态字段"""
    model_config = {"extra": "allow"}


# ==================== 作业清单 / 采集控制模型 ====================
# 契约见 doc/job_inventory_collection_decoupling_plan.md §9。
# Trigger 只做边界校验与透传：不分配、不推导内部 ID。

class CollectionSelector(BaseModel):
    """采集控制选择器，两种互斥形式之一

    形式 1（原生身份）: scheduler_type + cluster_name + native_job_id
                        [+ cluster_tag] [+ step_id]
    形式 2（实例句柄）: internal_job_id（推荐的持久内部 ID 十进制字符串）
                        或 instance_id（旧版不透明句柄），二者只能取其一
    """
    scheduler_type: Optional[str] = Field(None, description="调度器类型，如 condor/slurm")
    cluster_name: Optional[str] = Field(None, description="调度器命名空间；原生身份必填")
    cluster_tag: Optional[str] = Field(None, description="调度器命名空间标签，可选")
    native_job_id: Optional[str] = Field(None, description="调度器原生作业 ID 字符串，如 '172.0'")
    step_id: Optional[str] = Field(None, description="Slurm step 字符串，可选")
    internal_job_id: Optional[str] = Field(
        None,
        pattern=r"^[0-9]+$",
        description="内部作业 ID 十进制字符串；推荐的实例句柄形式，仅支持 instance scope",
    )
    instance_id: Optional[str] = Field(None, description="不透明实例句柄（旧版形式），仅支持 instance scope")

    @field_validator("*", mode="before")
    @classmethod
    def _normalize_identity_string(cls, value: Any, info: ValidationInfo) -> Any:
        """原生 ID/namespace 一律按契约以字符串过界；这里只做无损归一化。"""
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError(f"{info.field_name} must be a string")
        if isinstance(value, int):
            if info.field_name not in ("native_job_id", "step_id"):
                raise ValueError(f"{info.field_name} must be a string")
            value = str(value)
        if not isinstance(value, str):
            raise ValueError(f"{info.field_name} must be a string")
        value = value.strip()
        if not value:
            raise ValueError(f"{info.field_name} must not be empty")
        return value


class CollectionControlRequest(BaseModel):
    """POST /joblens/collection 请求体（§9.2）"""
    selector: CollectionSelector = Field(..., description="实例句柄或原生身份选择器，二者只能取其一")
    scope: Literal["instance", "job"] = Field("instance", description="instance 或 job；实例句柄只支持 instance")
    ensure_registered: bool = Field(False, description="为 true 时要求 Core 先刷新发现再匹配")
    operation: Literal["set", "reset_auto"] = Field(..., description="set 显式设置采集器；reset_auto 交回自动策略")
    collectors: Optional[List[str]] = Field(None, description="set 必填，[] 表示持久停止；reset_auto 必须省略")
    expected_revision: Optional[int] = Field(
        None, ge=0, description="绑定期望修订号，0 表示尚无绑定；仅单实例请求可携带"
    )

    @field_validator("collectors")
    @classmethod
    def _normalize_collector_names(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        names: List[str] = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("collectors must be a list of non-empty collector names")
            names.append(item.strip())
        return names

    @model_validator(mode="after")
    def _validate_contract(self) -> "CollectionControlRequest":
        selector = self.selector
        has_internal = selector.internal_job_id is not None
        has_instance = selector.instance_id is not None
        has_handle = has_internal or has_instance
        native_fields = [
            name for name in ("scheduler_type", "cluster_name", "cluster_tag", "native_job_id", "step_id")
            if getattr(selector, name) is not None
        ]

        if has_internal and has_instance:
            raise ValueError(
                "selector must use exactly one instance form: internal_job_id or instance_id, not both"
            )
        if has_handle and native_fields:
            raise ValueError(
                "selector must use exactly one form: an instance handle (internal_job_id, instance_id) "
                "or native identity fields, not both"
            )
        if not has_handle:
            missing = [
                name for name in ("scheduler_type", "cluster_name", "native_job_id")
                if getattr(selector, name) is None
            ]
            if missing:
                raise ValueError(
                    "incomplete native selector, missing: " + ", ".join(missing)
                )
        if has_handle and self.scope != "instance":
            raise ValueError("instance handle selectors only support scope=instance")
        if self.operation == "set" and self.collectors is None:
            raise ValueError(
                "operation=set requires an explicit collectors array; [] means persistent stop"
            )
        if self.operation == "reset_auto" and self.collectors is not None:
            raise ValueError("operation=reset_auto must omit collectors")
        if self.expected_revision is not None and self.scope != "instance":
            raise ValueError(
                "expected_revision is only accepted for a single-instance request (scope=instance)"
            )
        return self


class CollectionErrorResponse(BaseModel):
    """§9.2 结构化错误体"""
    code: str = Field(..., description="机器可读错误码")
    message: str = Field(..., description="人类可读错误信息")
    candidates: Optional[List[str]] = Field(None, description="歧义选择器的候选实例句柄")


class CollectionTarget(BaseModel):
    """§9.2 被精确解析到的单个实例"""
    instance_id: str = Field(..., description="不透明实例句柄")
    internal_job_id: str = Field(..., pattern=r"^[0-9]+$", description="内部作业 ID，十进制字符串")
    native_job_id: Optional[str] = Field(None, description="调度器原生作业 ID 字符串")
    record_revision: int = Field(..., description="已受理（accepted）的记录修订号")
    mode: str = Field(..., description="绑定模式，如 manual/auto")
    desired_collectors: List[str] = Field(..., description="已持久化的期望采集器")
    applied_collectors: List[str] = Field(..., description="调度器已实际生效的采集器")
    applied_attach_token: int = Field(0, description="实际生效的调度器附着令牌")
    draining: bool = Field(False, description="该实例的停止是否仍在排空")
    errors: List[Any] = Field(default_factory=list, description="单实例应用错误")
    model_config = {"extra": "allow"}


class CollectionControlResponse(BaseModel):
    """POST /joblens/collection 成功响应"""
    status: str = Field(..., description="受理状态，如 accepted")
    targets: List[CollectionTarget] = Field(..., description="精确解析到的目标实例")
    model_config = {"extra": "allow"}


class CollectionStatusResponse(BaseModel):
    """GET /joblens/collection 响应 - 透传 Core 的期望/生效选择与错误"""
    model_config = {"extra": "allow"}


class InventoryListResponse(BaseModel):
    """GET /joblens/inventory 响应（§9.3）"""
    snapshot_revision: int = Field(..., description="首页捕获的快照修订号")
    records: List[Dict[str, Any]] = Field(..., description="清单记录，内部 ID 为十进制字符串")
    next_cursor: Optional[str] = Field(None, description="下一页不透明游标；无更多数据为 null")
    model_config = {"extra": "allow"}


class InventoryStatusResponse(BaseModel):
    """GET /joblens/inventory/status 响应 - 透传 Core 的来源/持久化/队列/对账健康"""
    model_config = {"extra": "allow"}


class CapabilitiesResponse(BaseModel):
    """GET /joblens/capabilities 响应（§9.3）- 只透传，绝不代 Core 声明能力"""
    inventory_version: Optional[int] = Field(None, description="清单契约版本")
    collection_control_version: Optional[int] = Field(None, description="采集控制契约版本")
    native_identity_selectors: Optional[bool] = Field(None, description="是否支持原生身份选择器")
    job_scope: Optional[str] = Field(None, description="job scope 语义，如 current_local_instances")
    db_schema_version: Optional[int] = Field(None, description="DB schema 版本")
    model_config = {"extra": "allow"}
