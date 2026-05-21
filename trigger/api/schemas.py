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

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ==================== 请求体模型 ====================

class JobRequest(BaseModel):
    """通用作业操作请求"""
    opt: str = Field(..., description="操作类型: add/remove")
    type: str = Field(..., description="作业类型: job.condor/job.common")
    JobID: Optional[int] = Field(None, description="作业ID")
    JobPIDs: Optional[List[int]] = Field(None, description="进程ID列表")
    Lens: Optional[List[str]] = Field(None, description="采集器列表，默认 ['proc_collector']")
    sub_attr: Optional[Dict[str, Any]] = Field(None, description="子属性，condor类型需cluster_id/proc_id，slurm类型需job_id/step_id")


class CondorJobRequest(BaseModel):
    """Condor作业专用请求"""
    opt: str = Field(..., description="操作类型，目前仅支持 'add'")
    JobID: Optional[int] = Field(None, description="作业ID")
    slot: str = Field(..., description="slot名称，必须以 'slot' 开头")
    Lens: Optional[List[str]] = Field(None, description="采集器列表，默认 ['proc_collector']")
    sub_attr: Optional[Dict[str, Any]] = Field(None, description="子属性，包含cluster_id/proc_id，不提供则默认0")


class SlurmJobRequest(BaseModel):
    """Slurm作业专用请求"""
    opt: str = Field(..., description="操作类型，目前仅支持 'add'")
    JobID: int = Field(..., description="Slurm作业ID")
    Lens: Optional[List[str]] = Field(None, description="采集器列表，默认 ['proc_collector']")
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
    heartbeat_interval: Optional[int] = Field(None, description="心跳间隔(秒)")
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
