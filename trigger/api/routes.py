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
JobLens Trigger API Routes

所有Flask路由定义，接收依赖注入
"""

from flask import Flask, Response, request, jsonify, abort
import logging
import time
import subprocess
from typing import Optional
from werkzeug.exceptions import HTTPException
from pydantic import ValidationError

# 业务操作函数
from trigger.core.tools import joblens_format_metrics, systemd_status, job_opt, add_condorjob, add_slurmjob, restart_joblens
from trigger.core import tools as core_tools
from trigger.core.rpc_client import RPCError
from trigger.api.auth import install_auth
from trigger.api.errors import install_error_handlers
from trigger.utils.hardware_info import hardware_info

# 兼容两种运行方式：以项目根目录运行或以 trigger 目录运行
try:
    from trigger.api.schemas import (
        JobRequest, JobUpsertRequest, CondorJobRequest, SlurmJobRequest,
        ErrorResponse, HealthResponse, RPCHealthResponse,
        JobResponse, JobCountResponse, JobsListResponse, JobDetailResponse,
        RPCFunctionsResponse, CollectorsPerfResponse, WritersPerfResponse, WriterInfoResponse,
        ConfigUpdateResponse, ConfigStatusResponse,
        RegistryStatusResponse, RegistryRegisterResponse,
        RulesListResponse, RulesSyncResponse, RuleStatusResponse,
        VersionResponse, UpgradeResponse, HardwareInfoResponse, PidTrackerStatsResponse,
        CollectionControlRequest, JobCollectionIntentRequest, CollectionControlResponse, CollectionStatusResponse,
        CollectionErrorResponse, JobListPageResponse, JobLensStatusResponse,
        CapabilitiesResponse
    )
except ImportError:
    from trigger.api.schemas import (
        JobRequest, JobUpsertRequest, CondorJobRequest, SlurmJobRequest,
        ErrorResponse, HealthResponse, RPCHealthResponse,
        JobResponse, JobCountResponse, JobsListResponse, JobDetailResponse,
        RPCFunctionsResponse, CollectorsPerfResponse, WritersPerfResponse, WriterInfoResponse,
        ConfigUpdateResponse, ConfigStatusResponse,
        RegistryStatusResponse, RegistryRegisterResponse,
        RulesListResponse, RulesSyncResponse, RuleStatusResponse,
        VersionResponse, UpgradeResponse, HardwareInfoResponse, PidTrackerStatsResponse,
        CollectionControlRequest, JobCollectionIntentRequest, CollectionControlResponse, CollectionStatusResponse,
        CollectionErrorResponse, JobListPageResponse, JobLensStatusResponse,
        CapabilitiesResponse
    )

logger = logging.getLogger(__name__)


# 新接口的 Core 结构化错误码 -> HTTP 状态码。
# Flask 只按这张表映射，不对 Core 语义做额外猜测；未知码按 503 处理并告警，
# 保证任何 Core 错误都不会被吞成 200。
_COLLECTION_ERROR_STATUS = {
    # 400 invalid request
    'invalid_request': 400,
    'invalid_argument': 400,
    'invalid_selector': 400,
    'invalid_collectors': 400,
    'unknown_collector': 400,
    # 404 no active instance
    'not_found': 404,
    'no_verified_instance': 404,
    'instance_not_found': 404,
    'job_not_found': 404,
    # 409 ambiguous selector / stale revision / closing generation / changed snapshot
    'ambiguous_selector': 409,
    'ambiguous': 409,
    'already_exists': 409,
    'stale_revision': 409,
    'revision_conflict': 409,
    'generation_closing': 409,
    'closing_generation': 409,
    'snapshot_changed': 409,
    'cursor_invalidated': 409,
    # 503 unavailable identity verification / persistence / control service
    'unavailable': 503,
    'identity_unavailable': 503,
    'persistence_unavailable': 503,
    'store_unavailable': 503,
    'control_unavailable': 503,
    'service_unavailable': 503,
    'not_ready': 503,
}


def _error_response(status: int, code: str, message: str, candidates=None):
    """结构化错误响应：code + message [+ candidates]"""
    body = CollectionErrorResponse(code=code, message=message, candidates=candidates)
    return jsonify(body.model_dump(exclude_none=True)), status


def _invalid_request(message: str):
    return _error_response(400, 'invalid_request', message)


def _rpc_unavailable(method: str, exc: Exception):
    logger.error("Job/collection RPC unavailable: method=%s, error=%s", method, str(exc))
    return _error_response(
        503,
        'rpc_unavailable',
        f"JobLens core is unavailable while handling {method}: {str(exc)}",
    )


def _invalid_core_response(method: str, detail: str):
    logger.error("Job/collection RPC contract mismatch: method=%s, detail=%s", method, detail)
    return _error_response(
        502,
        'core_response_invalid',
        f"JobLens core returned an invalid response for {method}: {detail}",
    )


def _format_validation_error(exc: ValidationError) -> str:
    """把 Pydantic 校验错误压成一行，避免暴露内部 URL/文档链接。"""
    parts = []
    for err in exc.errors():
        location = '.'.join(str(item) for item in err.get('loc', ())) or 'body'
        message = str(err.get('msg', 'invalid value')).replace('Value error, ', '')
        parts.append(f"{location}: {message}")
    return '; '.join(parts) or 'invalid request body'


def _candidate_handle(candidate):
    """候选实例只暴露句柄，避免把内部结构当成契约泄漏出去。

    `JobID` 是规范键；`instance_id` / `instance` 作为旧信封的兼容别名保留。
    """
    if isinstance(candidate, dict):
        for key in ('JobID', 'instance_id', 'instance'):
            if candidate.get(key) is not None:
                return str(candidate[key])
        return str(candidate)
    return str(candidate)


def _structured_core_error(payload):
    """把 Core 结构化错误翻译成 HTTP 响应；非错误 payload 返回 None。

    约定的 Core 错误信封：{"status": "error", "code": ..., "message": ...,
    "candidates": [...]}。同时兼容嵌套 {"error": {...}}、候选人字段
    candidate_instance_ids 与旧字段 msg，避免因信封细节丢失错误码。
    """
    if not isinstance(payload, dict):
        return None

    nested = payload.get('error')
    if isinstance(nested, dict):
        source = nested
    elif payload.get('status') == 'error':
        source = payload
    else:
        return None

    code = str(
        source.get('code') or source.get('error_code')
        or payload.get('code') or payload.get('error_code')
        or 'core_error'
    )
    message = (
        source.get('message') or source.get('msg')
        or payload.get('message') or payload.get('msg')
        or 'core reported an error'
    )
    raw_candidates = (
        source.get('candidates') or source.get('candidate_instance_ids')
        or payload.get('candidates') or payload.get('candidate_instance_ids')
    )
    candidates = (
        [_candidate_handle(item) for item in raw_candidates]
        if isinstance(raw_candidates, list) else None
    )

    status = _COLLECTION_ERROR_STATUS.get(code)
    if status is None:
        logger.warning("Unknown core error code %r on job/collection API; replying 503", code)
        status = 503
    return _error_response(status, code, str(message), candidates)


def _with_legacy_field_aliases(payload):
    """v1 旧路径的字段别名；v2 只暴露新字段。"""
    if not isinstance(payload, dict):
        return payload
    for record in payload.get('records') or []:
        if isinstance(record, dict) and 'revision' in record:
            # `facts_revision` is the retired v1 spelling of `revision`.
            record.setdefault('facts_revision', record['revision'])
    return payload


def _v2_job_payload(data):
    """Validate a v2 job body and apply the v1-compatible defaults."""
    req = JobUpsertRequest.model_validate(data)
    payload = req.model_dump(exclude_none=True)
    if not payload.get('Lens'):
        payload['Lens'] = ['cpumem_collector', 'io_collector', 'net_collector']
    if 'sub_attr' not in payload:
        if payload['type'] == 'job.condor':
            payload['sub_attr'] = {'cluster_id': 0, 'proc_id': 0}
        elif payload['type'] == 'job.slurm':
            payload['sub_attr'] = {'job_id': payload.get('JobID', 0), 'step_id': 0}
    return payload


def _legacy_job_from_record(record):
    """把 Core 的 JobRecord 投影成旧版 `/joblens/jobs` 的 dump_job 形状。

    旧版 API 的字段集合保持不变；新增字段只在 v2 的资源表示里出现。
    """
    pid_identities = record.get('pid_identities') or []
    return {
        'JobID': int(record['JobID']),
        'JobNativeID': record.get('JobNativeID', ''),
        'clusterTag': record.get('clusterTag', ''),
        'clusterName': record.get('clusterName', ''),
        'jobtype': 'job',
        'subtype': record.get('scheduler_type') or 'common',
        'JobPIDs': [process.get('pid') for process in pid_identities
                    if process.get('pid') is not None],
        'CollectorNames': record.get('desired_collectors') or [],
        'sub_attr': record.get('attributes') or {},
    }


def register_routes(app: Flask, rpc_client, config_manager, service_registrar, rule_manager):
    """
    注册所有路由
    
    Args:
        app: Flask应用实例
        rpc_client: RPC客户端
        config_manager: 配置管理器（可选）
        service_registrar: 服务注册器（可选）
        rule_manager: 规则管理器（可选）
    """
    # 统一鉴权必须在业务路由之前安装；健康/指标端点由 auth 模块放行。
    install_auth(app)
    install_error_handlers(app)

    def rpc_call(method: str, params=None):
        """RPC 调用兜底：core 未连接时抛 RPCError，由各路由已有的 503 分支统一处理"""
        if rpc_client is None:
            raise RPCError("RPC client is not initialized (JobLens core may be down)")
        return rpc_client.call(method, params)

    def current_joblens_version() -> str:
        try:
            from trigger.core.tools import get_joblens_version
            return get_joblens_version().get('version', 'UNKNOWN')
        except Exception:
            return 'UNKNOWN'
    
    # ==================== 指标接口 ====================
    
    @app.route("/metrics")
    def prmxs_writer_metrics():
        """获取Prometheus格式的指标数据"""
        try:
            # The current core can pre-format the exposition text, avoiding a
            # JSON parse and a second formatting pass here.
            try:
                text = rpc_call("prmxs_writer/metrics_text")
                if isinstance(text, str):
                    return Response(text, mimetype="text/plain")
            except RPCError:
                pass
            metrics = rpc_call("prmxs_writer/metrics")
            # 防御检查: C++ 返回 JSON 数组表示方法不存在/错误
            if not isinstance(metrics, dict):
                return Response("", mimetype="text/plain")
            return Response(joblens_format_metrics(metrics), mimetype="text/plain")
        except HTTPException:
            raise
        except RPCError as e:
            return abort(503, description=f"RPC调用失败: {str(e)}")
        except Exception as e:
            return abort(500, description=f"获取指标失败: {str(e)}")
    
    # ==================== Trigger 自身状态 ====================
    
    @app.route('/')
    def index():
        """根路径，返回 Trigger 基本信息"""
        return jsonify({
            "service": "JobLens Trigger",
            "version": current_joblens_version(),
            "status": "running",
            "endpoints": {
                "trigger_health": "/trigger/health",
                "joblens_health": "/joblens/healthy",
                "joblens_rpc_health": "/joblens/rpc/health",
                "version": "/joblens/version",
                "metrics": "/metrics"
            }
        })
    
    @app.route('/trigger/health')
    def trigger_health():
        """返回 Trigger 自身各组件的运行状态（不依赖 JobLens RPC）"""
        from trigger.core.tools import get_joblens_version as _get_joblens_version
        joblens_info = {}
        try:
            joblens_info = _get_joblens_version()
        except Exception:
            pass
        return jsonify({
            "service": "JobLens Trigger",
            "version": joblens_info.get('version', 'UNKNOWN'),
            "status": "running",
            "components": {
                "rpc_client": rpc_client is not None,
                "service_registrar": service_registrar is not None,
                "config_manager": config_manager is not None,
                "rule_manager": rule_manager is not None,
            },
            "joblens": {
                "version": joblens_info.get('version', 'UNKNOWN'),
                "build_id": joblens_info.get('build_id', 'UNKNOWN'),
                "build_time": joblens_info.get('build_time_local', 'UNKNOWN')
            }
        })
    
    # ==================== 健康检查接口 ====================
    
    @app.route('/joblens/healthy', methods=['GET'])
    def joblens_health():
        """检查JobLens服务健康状态"""
        try:
            active, sub = systemd_status('joblens')
            healthy = (active == "active") and (sub == "running")
            response = HealthResponse(
                active=active == "active",
                state=active,
                sub=sub,
                healthy=healthy
            )
            return jsonify(response.model_dump())
        except RuntimeError as e:
            return abort(400, description=str(e))
        except Exception as e:
            return abort(500, description=f"健康检查失败: {str(e)}")
    
    @app.route('/joblens/rpc/health', methods=['GET'])
    def rpc_health():
        """
        通过RPC调用检查服务端健康状态
        返回: 包含详细服务状态的JSON
        """
        try:
            start_time = time.time()
            result = rpc_call("health")
            result['rpc_latency_ms'] = round((time.time() - start_time) * 1000, 2)
            response = RPCHealthResponse.model_validate(result)
            return jsonify(response.model_dump())
        except RPCError as e:
            return abort(503, description=f"RPC health check failed: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    # ==================== 作业管理接口 ====================
    
    VALID_OPT = {'add', 'remove'}
    VALID_TYPE = {'job.condor', 'job.slurm', 'job.common'}
    
    @app.route('/joblens/job', methods=['POST'])
    def job_handler():
        """
        通用作业操作接口
        
        请求示例:
        {
            "opt": "add",
            "type": "job.condor",
            "JobID": 1,
            "JobPIDs": [1],
            "Lens": ["cpumem_collector", "io_collector", "net_collector"],
            "sub_attr": {"cluster_id": 123456, "proc_id": 0}
        }
        """
        try:
            data = request.get_json(silent=True) or {}
            req = JobRequest.model_validate(data)
        except Exception as e:
            return abort(400, description=f'请求体验证失败: {str(e)}')
        
        if req.opt not in VALID_OPT:
            return abort(400, description=f'invalid opt: {req.opt}')
        if req.type not in VALID_TYPE:
            return abort(400, description=f'invalid type: {req.type}')
        
        # 构建操作数据
        opt_data = req.model_dump(exclude_none=True)
        # Lens 默认处理
        if not opt_data.get('Lens'):
            opt_data['Lens'] = ['cpumem_collector', 'io_collector', 'net_collector']
        
        # 对 condor/slurm 类型自动补全 sub_attr，避免C++端因缺失而抛异常
        if 'sub_attr' not in opt_data:
            if opt_data['type'] == 'job.condor':
                opt_data['sub_attr'] = {'cluster_id': 0, 'proc_id': 0}
            elif opt_data['type'] == 'job.slurm':
                job_id = opt_data.get('JobID', 0)
                opt_data['sub_attr'] = {'job_id': job_id, 'step_id': 0}
        
        logger.info("Job opt request: opt=%s, type=%s, JobID=%s, Lens=%s",
                     opt_data.get('opt'), opt_data.get('type'), opt_data.get('JobID'), opt_data.get('Lens'))
        try:
            ret = job_opt(opt_data)
            # 检查RPC返回值，如果C++端返回error则向上报错
            if isinstance(ret, dict) and ret.get('status') == 'error':
                logger.error("Job opt failed: opt=%s, type=%s, JobID=%s, msg=%s",
                             opt_data.get('opt'), opt_data.get('type'), opt_data.get('JobID'), ret.get('msg'))
                return abort(500, description=f"作业操作失败: {(ret.get('msg') or ret.get('message') or 'unknown error')}")
            logger.info("Job opt success: JobID=%s", opt_data.get('JobID'))
            response = JobResponse(status='ok', received=opt_data)
            return jsonify(response.model_dump())
        except Exception as e:
            logger.error("Job opt exception: JobID=%s, error=%s", opt_data.get('JobID'), str(e), exc_info=True)
            return abort(500, description=f"作业操作失败: {str(e)}")
    
    @app.route('/joblens/condor_job', methods=['POST'])
    def condorjob_handler():
        """
        Condor作业专用接口
        
        请求示例:
        {
            "opt": "add",
            "JobID": 1,
            "slot": "slotx",
            "Lens": ["cpumem_collector","io_collector"],
            "sub_attr": {"cluster_id": 123456, "proc_id": 0}
        }
        """
        try:
            data = request.get_json(silent=True) or {}
            req = CondorJobRequest.model_validate(data)
        except Exception as e:
            return abort(400, description=f'请求体验证失败: {str(e)}')
        
        if not data:
            return abort(400, description='no json data received')
        
        if req.opt not in ['add']:
            return abort(400, description=f'invalid opt: {req.opt}, only support "add" for condor_job')
        if not req.slot or not req.slot.startswith('slot'):
            return abort(400, description='slot is required and must start with "slot"')
        
        # 构建操作数据
        opt_data = req.model_dump(exclude_none=True)
        # Lens 默认处理
        if not opt_data.get('Lens'):
            opt_data['Lens'] = ['cpumem_collector', 'io_collector', 'net_collector']

        logger.info("Condor job add request: JobID=%s, slot=%s, Lens=%s, sub_attr=%s",
                     opt_data.get('JobID'), opt_data.get('slot'), opt_data.get('Lens'), opt_data.get('sub_attr'))
        
        try:
            ret = add_condorjob(opt_data)
            if isinstance(ret, dict) and ret.get('status') == 'error':
                logger.error("Condor job add failed: JobID=%s, slot=%s, msg=%s",
                             opt_data.get('JobID'), opt_data.get('slot'), ret.get('msg'))
                return abort(500, description=f"Condor作业添加失败: {(ret.get('msg') or ret.get('message') or 'unknown error')}")
            logger.info("Condor job add success: JobID=%s, slot=%s", opt_data.get('JobID'), opt_data.get('slot'))
            response = JobResponse(status='ok', received=opt_data)
            return jsonify(response.model_dump())
        except Exception as e:
            logger.error("Condor job add exception: JobID=%s, slot=%s, error=%s",
                         opt_data.get('JobID'), opt_data.get('slot'), str(e), exc_info=True)
            return abort(500, description=str(e))
    
    @app.route('/joblens/slurm_job', methods=['POST'])
    def slurmjob_handler():
        """
        Slurm作业专用接口
        
        请求示例:
        {
            "opt": "add",
            "JobID": 12345,
            "Lens": ["cpumem_collector", "io_collector", "net_collector"]
        }
        """
        try:
            data = request.get_json(silent=True) or {}
            req = SlurmJobRequest.model_validate(data)
        except Exception as e:
            return abort(400, description=f'请求体验证失败: {str(e)}')
        
        if not data:
            return abort(400, description='no json data received')
        
        if req.opt not in ['add']:
            return abort(400, description=f'invalid opt: {req.opt}, only support "add" for slurm_job')
        
        # 构建操作数据
        opt_data = req.model_dump(exclude_none=True)
        # Lens 默认处理
        if not opt_data.get('Lens'):
            opt_data['Lens'] = ['cpumem_collector', 'io_collector', 'net_collector']

        logger.info("Slurm job add request: JobID=%s, Lens=%s, sub_attr=%s",
                     opt_data.get('JobID'), opt_data.get('Lens'), opt_data.get('sub_attr'))
        
        try:
            ret = add_slurmjob(opt_data)
            if isinstance(ret, dict) and ret.get('status') == 'error':
                logger.error("Slurm job add failed: JobID=%s, msg=%s", opt_data.get('JobID'), ret.get('msg'))
                return abort(500, description=f"Slurm作业添加失败: {(ret.get('msg') or ret.get('message') or 'unknown error')}")
            logger.info("Slurm job add success: JobID=%s", opt_data.get('JobID'))
            response = JobResponse(status='ok', received=opt_data)
            return jsonify(response.model_dump())
        except Exception as e:
            logger.error("Slurm job add exception: JobID=%s, error=%s", opt_data.get('JobID'), str(e), exc_info=True)
            return abort(500, description=str(e))
    
    # ==================== 作业列表与采集控制接口 ====================
    # 契约：见 doc/api_documentation.md 第 12 节。
    # 与现有控制接口（/joblens/job 等）一致，请求鉴权沿用部署层/网关策略，
    # 路由内不再额外套 token。Trigger 只校验并转发，不分配内部 ID。

    JOB_LIST_DEFAULT_LIMIT = 100
    JOB_LIST_MAX_LIMIT = 1000

    def core_rpc(method: str, params=None):
        """调用 Core 的作业/采集控制 RPC（tools.call_core_rpc 为唯一接缝）。"""
        return core_tools.call_core_rpc(method, params, rpc_client)

    def forward_core_rpc(method: str, params, response_model, legacy=False):
        """转发一次 RPC：Core 错误按契约映射，成功则透传 Core payload。

        legacy=True 只在 v1 旧路径上补齐旧字段名；v2 始终返回新表示。
        """
        try:
            payload = core_rpc(method, params)
        except RPCError as e:
            return _rpc_unavailable(method, e)

        error = _structured_core_error(payload)
        if error is not None:
            return error
        if not isinstance(payload, dict):
            return _invalid_core_response(method, 'payload is not a JSON object')
        if legacy:
            payload = _with_legacy_field_aliases(payload)
        try:
            response_model.model_validate(payload)
        except ValidationError as e:
            return _invalid_core_response(method, _format_validation_error(e))
        return jsonify(payload)

    @app.route('/joblens/v2/jobs', methods=['GET'])
    @app.route('/joblens/jobs/list', methods=['GET'])
    def job_list_page():
        """分页查询作业列表，支持 scheduler_type/JobNativeID/state/limit/cursor"""
        raw_limit = request.args.get('limit')
        try:
            limit = JOB_LIST_DEFAULT_LIMIT if raw_limit in (None, '') else int(raw_limit)
        except (TypeError, ValueError):
            return _invalid_request(
                f'limit must be an integer between 1 and {JOB_LIST_MAX_LIMIT}'
            )
        if limit < 1 or limit > JOB_LIST_MAX_LIMIT:
            return _invalid_request(
                f'limit must be between 1 and {JOB_LIST_MAX_LIMIT}'
            )

        params = {'limit': limit}
        for name in ('scheduler_type', 'JobNativeID', 'state', 'cursor'):
            value = request.args.get(name)
            if value:
                params[name] = value

        return forward_core_rpc('jobs/list', params, JobListPageResponse)

    @app.route('/joblens/v2/status', methods=['GET'])
    @app.route('/joblens/jobs/status', methods=['GET'])
    def joblens_status():
        """透传 Core 的来源/持久化/队列/对账健康状态"""
        return forward_core_rpc('joblens/status', {}, JobLensStatusResponse)

    @app.route('/joblens/collection', methods=['POST'])
    def collection_set():
        """按实例句柄或原生身份设置/重置采集意图"""
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return _invalid_request('request body must be a JSON object')

        try:
            req = CollectionControlRequest.model_validate(data)
        except ValidationError as e:
            return _invalid_request(_format_validation_error(e))

        payload = req.model_dump(exclude_none=True)
        logger.info(
            "Collection control request: operation=%s, scope=%s, ensure_registered=%s, selector=%s",
            payload.get('operation'), payload.get('scope'),
            payload.get('ensure_registered'), payload.get('selector'),
        )
        return forward_core_rpc('jobs/collection/set', payload, CollectionControlResponse)

    @app.route('/joblens/collection', methods=['GET'])
    def collection_get():
        """查询单个实例的期望/已生效采集选择与错误

        `JobID` 是唯一的实例句柄参数（内部记录 ID 的十进制字符串）；
        旧参数名 `internal_job_id` / `instance_id` 已退役，Trigger 直接把
        JobID 交给 Core。
        """
        job_id = (request.args.get('JobID') or '').strip()
        if not job_id:
            return _invalid_request('JobID query parameter is required')

        return forward_core_rpc(
            'jobs/collection/get',
            {'JobID': job_id},
            CollectionStatusResponse,
        )

    @app.route('/joblens/v2/capabilities', methods=['GET'])
    @app.route('/joblens/capabilities', methods=['GET'])
    def capabilities():
        """只透传 Core 的能力声明；Core 不支持时返回 503，绝不代其声明能力"""
        return forward_core_rpc(
            'joblens/capabilities', {}, CapabilitiesResponse,
            legacy=(request.path == '/joblens/capabilities'),
        )

    def all_job_records():
        """分页拉取 Core 的 JobRecord，供旧版 dump_job 视图使用。"""
        records = []
        cursor = None
        while True:
            params = {'limit': 1000}
            if cursor:
                params['cursor'] = cursor
            payload = core_rpc("jobs/list", params)
            error = _structured_core_error(payload)
            if error is not None:
                return None, error
            if not isinstance(payload, dict):
                return None, _invalid_core_response('jobs/list', 'payload is not a JSON object')
            records.extend(payload.get('records') or [])
            cursor = payload.get('next_cursor')
            if not cursor:
                return records, None

    @app.route('/joblens/v2/jobs/count', methods=['GET'])
    @app.route('/joblens/jobs/count', methods=['GET'])
    def get_job_count():
        """获取当前注册的作业总数"""
        return forward_core_rpc("jobs/count", {}, JobCountResponse)
    
    @app.route('/joblens/jobs', methods=['GET'])
    def list_all_jobs():
        """列出所有作业（旧版 dump_job 形状；内部数据来自 jobs/list）"""
        try:
            records, error = all_job_records()
            if error is not None:
                return error
            # The old list_jobs surface exposed active jobs only; closing
            # records stay visible in the resource list and v2.
            jobs = [
                _legacy_job_from_record(record)
                for record in records
                if record.get('state') != 'closing'
            ]
            response = JobsListResponse(status="ok", jobs=jobs)
            return jsonify(response.model_dump())
        except RPCError as e:
            return abort(503, description=f"Failed to list jobs: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    @app.route('/joblens/jobs/<int:job_id>', methods=['GET'])
    def get_job_by_id(job_id: int):
        """获取指定 JobID 的详细信息（旧版 dump_job 形状）"""
        try:
            result = core_rpc("jobs/get", {"JobID": str(job_id)})
            error = _structured_core_error(result)
            if error is not None:
                return error
            if not isinstance(result, dict):
                return _invalid_core_response('jobs/get', 'payload is not a JSON object')
            # The old get_job surface treated a closing generation as absent.
            if result.get('state') == 'closing':
                return abort(404, description=f"job not found with JobID {job_id}")
            response = JobDetailResponse.model_validate(
                _legacy_job_from_record(result)
            )
            return jsonify(response.model_dump())
        except HTTPException:
            raise
        except RPCError as e:
            return abort(503, description=f"Failed to get job: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    # ==================== v2 资源 API ====================
    # v1 的 /joblens/job、/joblens/jobs 等继续保留；v2 直接暴露 JobRecord
    # 与 collection 子资源，响应即为 Core 的资源表示。

    @app.route('/joblens/v2/jobs/<int:job_id>', methods=['GET'])
    def get_job_v2(job_id: int):
        """获取单个作业资源。"""
        return forward_core_rpc(
            'jobs/get', {'JobID': str(job_id)}, JobDetailResponse
        )

    @app.route('/joblens/v2/jobs', methods=['POST'])
    def create_job_v2():
        """严格创建作业；同一身份或内部 ID 已存在时返回 409。"""
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return _invalid_request('request body must be a JSON object')
        try:
            payload = _v2_job_payload(data)
        except ValidationError as e:
            return _invalid_request(_format_validation_error(e))
        return forward_core_rpc('jobs/create', payload, JobDetailResponse)

    @app.route('/joblens/v2/jobs', methods=['PUT'])
    def upsert_job_v2():
        """按原生身份 upsert 作业（有则更新 selection，无则创建）。"""
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return _invalid_request('request body must be a JSON object')
        try:
            payload = _v2_job_payload(data)
        except ValidationError as e:
            return _invalid_request(_format_validation_error(e))
        return forward_core_rpc('jobs/upsert', payload, JobDetailResponse)

    @app.route('/joblens/v2/jobs/<int:job_id>', methods=['DELETE'])
    def erase_job_v2(job_id: int):
        """退役/删除作业资源（采集过的归档，未采集的删除）。"""
        return forward_core_rpc(
            'jobs/erase', {'JobID': job_id}, JobDetailResponse
        )

    @app.route('/joblens/v2/jobs/<int:job_id>/stop', methods=['POST'])
    def stop_job_v2(job_id: int):
        """停止采集，但保留作业记录。"""
        return forward_core_rpc(
            'jobs/stop', {'JobID': job_id}, JobDetailResponse
        )

    @app.route('/joblens/v2/jobs/<int:job_id>/collection', methods=['GET'])
    def get_job_collection_v2(job_id: int):
        """读取作业的期望/已生效采集选择。"""
        return forward_core_rpc(
            'jobs/collection/get',
            {'JobID': str(job_id)},
            CollectionStatusResponse,
        )

    @app.route('/joblens/v2/jobs/<int:job_id>/collection', methods=['PUT'])
    def set_job_collection_v2(job_id: int):
        """替换作业的期望采集器集合；[] 表示持久停止。"""
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return _invalid_request('request body must be a JSON object')
        try:
            req = JobCollectionIntentRequest.model_validate(data)
        except ValidationError as e:
            return _invalid_request(_format_validation_error(e))
        payload = {
            'selector': {'JobID': str(job_id)},
            'scope': 'instance',
            'operation': 'set',
            'collectors': req.collectors,
        }
        if req.expected_revision is not None:
            payload['expected_revision'] = req.expected_revision
        return forward_core_rpc(
            'jobs/collection/set', payload, CollectionControlResponse
        )

    @app.route('/joblens/v2/jobs/<int:job_id>/collection', methods=['DELETE'])
    def reset_job_collection_v2(job_id: int):
        """清除 manual override，交回自动采集策略。"""
        payload = {
            'selector': {'JobID': str(job_id)},
            'scope': 'instance',
            'operation': 'reset_auto',
        }
        return forward_core_rpc(
            'jobs/collection/reset', payload, CollectionControlResponse
        )

    # ==================== RPC函数接口 ====================
    
    @app.route('/joblens/rpc/functions', methods=['GET'])
    def rpc_functions():
        """获取 RPC 服务端注册的所有可用方法列表"""
        try:
            result = rpc_call("func_list")
            response = RPCFunctionsResponse(
                status="ok",
                functions=result if isinstance(result, list) else [],
                count=len(result) if isinstance(result, list) else 0
            )
            return jsonify(response.model_dump())
        except RPCError as e:
            return abort(503, description=f"Failed to get function list: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    # ==================== pid 归属追踪接口 ====================

    @app.route('/joblens/pid_tracker/stats', methods=['GET'])
    def pid_tracker_stats():
        """获取 pid 归属追踪状态与 job_event ringbuf 入队/丢弃计数"""
        return forward_core_rpc(
            "diagnostics/pid_tracker", {}, PidTrackerStatsResponse
        )

    # ==================== Collector接口 ====================

    @app.route('/joblens/collectors/status', methods=['GET'])
    def collectors_status():
        """返回核心 CollectorScheduler 的只读状态快照。"""
        try:
            result = rpc_call("CollectorScheduler/status", {})
        except RPCError as e:
            if "method not found" in str(e).lower():
                return jsonify({
                    "status": "error",
                    "code": "unsupported",
                    "supported": False,
                    "message": "CollectorScheduler/status is not supported by this JobLens core",
                }), 501
            return jsonify({
                "status": "error",
                "code": "rpc_unavailable",
                "message": "CollectorScheduler/status is unavailable",
            }), 503

        if not isinstance(result, dict) or result.get("status") != "ok":
            message = (
                result.get("msg", "CollectorScheduler/status returned an invalid response")
                if isinstance(result, dict)
                else "CollectorScheduler/status returned an invalid response"
            )
            return jsonify({
                "status": "error",
                "code": "core_error",
                "message": message,
            }), 502

        return jsonify(result)

    @app.route('/joblens/collectors/perf', methods=['GET'])
    def collectors_perf():
        """获取所有 Collector 的性能统计信息"""
        try:
            result = rpc_call("CollectorRegistry/CollectorsPerfCount")
            if result.get("status") == "error":
                return abort(500, description=result.get("msg", "Unknown error"))
            response = CollectorsPerfResponse.model_validate(result)
            return jsonify(response.model_dump())
        except HTTPException:
            raise
        except RPCError as e:
            return abort(503, description=f"Failed to get collector perf: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    # ==================== Writer接口 ====================
    
    @app.route('/joblens/writers/<string:writer_name>/info', methods=['GET'])
    def get_writer_info(writer_name: str):
        """获取指定 Writer 的基本信息"""
        try:
            method_name = f"{writer_name}/info"
            result = rpc_call(method_name)
            response = WriterInfoResponse.model_validate(result)
            return jsonify(response.model_dump())
        except RPCError as e:
            if "not found" in str(e).lower():
                return abort(404, description=f"Writer '{writer_name}' not found")
            return abort(503, description=f"Failed to get writer info: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    @app.route('/joblens/writers/perf', methods=['GET'])
    def writers_perf():
        """获取所有 Writer 的性能统计信息"""
        try:
            result = rpc_call("WriterManager/WriterPerfCount")
            if result.get("status") == "error":
                return abort(500, description=result.get("msg", "Unknown error"))
            response = WritersPerfResponse.model_validate(result)
            return jsonify(response.model_dump())
        except HTTPException:
            raise
        except RPCError as e:
            return abort(503, description=f"Failed to get writer perf: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    # ==================== 配置管理接口 ====================
    
    @app.route('/joblens/config/update', methods=['POST'])
    def config_update():
        """手动触发配置更新"""
        if not config_manager:
            return abort(503, description="ConfigManager未初始化")

        logger.warning("config_update requested by %s", request.remote_addr)
        try:
            # 强制重新加载配置
            config_manager._handle_config_change()
            response = ConfigUpdateResponse(status="ok", message="配置更新已触发")
            return jsonify(response.model_dump())
        except Exception as e:
            return abort(500, description=f"配置更新失败: {str(e)}")
    
    @app.route('/joblens/config/status', methods=['GET'])
    def config_status():
        """获取配置管理器状态"""
        if not config_manager:
            response = ConfigStatusResponse(
                enabled=False,
                message="ConfigManager未初始化"
            )
            return jsonify(response.model_dump())
        
        response = ConfigStatusResponse(
            enabled=True,
            running=config_manager.running,
            mode=config_manager.mode,
            use_etcd=config_manager.use_etcd,
            sync_status=config_manager.get_sync_status()
        )
        return jsonify(response.model_dump())
    
    # ==================== 服务注册接口 ====================
    
    @app.route('/joblens/registry/status', methods=['GET'])
    def registry_status():
        """获取服务注册状态"""
        if not service_registrar:
            response = RegistryStatusResponse(
                enabled=False,
                message="ServiceRegistrar未初始化"
            )
            return jsonify(response.model_dump())
        
        status = service_registrar.get_status()
        response = RegistryStatusResponse.model_validate(status)
        return jsonify(response.model_dump())
    
    @app.route('/joblens/registry/register', methods=['POST'])
    def registry_register():
        """手动触发服务注册"""
        if not service_registrar:
            return abort(503, description="ServiceRegistrar未初始化")
        
        try:
            success = service_registrar.register()
            if success:
                response = RegistryRegisterResponse(status="ok", message="注册成功")
                return jsonify(response.model_dump())
            else:
                response = RegistryRegisterResponse(status="warning", message="注册失败，但服务继续运行")
                return jsonify(response.model_dump()), 202
        except Exception as e:
            return abort(500, description=f"注册失败: {str(e)}")
    
    # ==================== 规则管理接口 ====================
    
    @app.route('/joblens/rules', methods=['GET'])
    def list_rules():
        """列出所有本地规则文件"""
        if not rule_manager:
            response = RulesListResponse(
                enabled=False,
                count=0,
                rules=[],
                message="RuleManager未初始化"
            )
            return jsonify(response.model_dump())
        
        try:
            rules = rule_manager.get_local_rules_list()
            rule_list = list(rules.keys())
            response = RulesListResponse(
                enabled=True,
                count=len(rule_list),
                rules=rule_list
            )
            return jsonify(response.model_dump())
        except Exception as e:
            return abort(500, description=f"获取规则列表失败: {str(e)}")
    
    @app.route('/joblens/rules/<string:filename>', methods=['GET'])
    def get_rule(filename: str):
        """获取特定规则文件内容"""
        if not rule_manager:
            return abort(503, description="RuleManager未初始化")
        
        # 安全性检查：确保文件名只包含安全字符
        if not filename.endswith('.lua') or '/' in filename or '\\' in filename:
            return abort(400, description="无效的文件名")
        
        try:
            rule_path = rule_manager.get_local_rule_path(filename)
            if not rule_path or not rule_path.exists():
                return abort(404, description="规则文件不存在")
            
            with open(rule_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            return Response(content, mimetype='text/plain')
        except Exception as e:
            return abort(500, description=f"读取规则文件失败: {str(e)}")
    
    @app.route('/joblens/rules/sync', methods=['POST'])
    def sync_rules():
        """手动触发规则同步"""
        if not rule_manager:
            return abort(503, description="RuleManager未初始化")
        
        try:
            # 触发全量同步
            rule_manager.sync_all_from_etcd_to_file()
            response = RulesSyncResponse(status="ok", message="规则同步已触发")
            return jsonify(response.model_dump())
        except Exception as e:
            return abort(500, description=f"规则同步失败: {str(e)}")
    
    @app.route('/joblens/rules/status', methods=['GET'])
    def rule_status():
        """获取规则管理器状态"""
        if not rule_manager:
            response = RuleStatusResponse(
                enabled=False,
                running=False,
                etcd_priority=False,
                etcd_role_path="",
                etcd_workdir_prefix="",
                local_rules_dir="",
                role_id="",
                local_rules_count=0,
                remote_rules_count=0,
                callbacks_count=0,
                file_watcher_running=False,
                syncing=False,
                sync_lock_locked=False
            )
            return jsonify(response.model_dump())
        
        # 使用 rule_manager.get_status() 获取状态模型
        status = rule_manager.get_status()
        
        # 将模型转换为字典并返回，添加 enabled=True
        result = status.model_dump()
        result["enabled"] = True
        return jsonify(result)
    
    # ==================== 版本和升级接口 ====================
    
    @app.route('/joblens/version')
    def version():
        """获取版本信息"""
        try:
            from trigger.core.tools import get_joblens_version
            joblens_info = get_joblens_version()
            response = VersionResponse(
                trigger_version=joblens_info.get('version', 'UNKNOWN'),
                joblens_version=joblens_info.get('version', 'UNKNOWN'),
                build_id=joblens_info.get('build_id', 'UNKNOWN'),
                build_time=joblens_info.get('build_time_local', 'UNKNOWN')
            )
            return jsonify(response.model_dump())
        except Exception as e:
            response = VersionResponse(
                trigger_version="UNKNOWN",
                joblens_version="UNKNOWN",
                build_id="UNKNOWN",
                build_time=str(e)
            )
            return jsonify(response.model_dump())
    
    # ==================== 一些奇妙的用户需求 ====================
    @app.route('/utils/hardware_info', methods=['GET'])
    def get_hardware_info():
        """获取硬件信息（CPU型号、内存大小、磁盘信息等）"""
        try:
            info = hardware_info()
            response = HardwareInfoResponse.model_validate(info)
            return jsonify(response.model_dump())
        except Exception as e:
            return abort(500, description=f"Failed to get hardware info: {str(e)}")
