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

# 业务操作函数
from trigger.core.tools import joblens_format_metrics, systemd_status, job_opt, add_condorjob, add_slurmjob, restart_joblens
from trigger.core.rpc_client import RPCError
from trigger.utils.hardware_info import hardware_info

# 兼容两种运行方式：以项目根目录运行或以 trigger 目录运行
try:
    from trigger.api.schemas import (
        JobRequest, CondorJobRequest, SlurmJobRequest,
        ErrorResponse, HealthResponse, RPCHealthResponse,
        JobResponse, JobCountResponse, JobsListResponse, JobDetailResponse,
        RPCFunctionsResponse, CollectorsPerfResponse, WritersPerfResponse, WriterInfoResponse,
        ConfigUpdateResponse, ConfigStatusResponse,
        RegistryStatusResponse, RegistryRegisterResponse,
        RulesListResponse, RulesSyncResponse, RuleStatusResponse,
        VersionResponse, UpgradeResponse, HardwareInfoResponse
    )
except ImportError:
    from trigger.api.schemas import (
        JobRequest, CondorJobRequest, SlurmJobRequest,
        ErrorResponse, HealthResponse, RPCHealthResponse,
        JobResponse, JobCountResponse, JobsListResponse, JobDetailResponse,
        RPCFunctionsResponse, CollectorsPerfResponse, WritersPerfResponse, WriterInfoResponse,
        ConfigUpdateResponse, ConfigStatusResponse,
        RegistryStatusResponse, RegistryRegisterResponse,
        RulesListResponse, RulesSyncResponse, RuleStatusResponse,
        VersionResponse, UpgradeResponse, HardwareInfoResponse
    )

# 升级接口的访问 token，建议从环境变量读取，默认为空表示不校验
UPGRADE_TOKEN = '3P843X9A5L4Vrt1s3mv9CH7yYt6ZcPUp0BZt2adDlCAUBLlsX8BPe3b7KzQyV6h5'

logger = logging.getLogger(__name__)


def require_upgrade_token(f):
    """升级接口权限校验装饰器"""
    from functools import wraps
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 如果未设置 token，则允许访问（开发环境）
        if not UPGRADE_TOKEN:
            return f(*args, **kwargs)
        
        # 从请求头获取 token
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Missing or invalid Authorization header'}), 401
        
        token = auth_header.replace('Bearer ', '').strip()
        if token != UPGRADE_TOKEN:
            return jsonify({'error': 'Invalid upgrade token'}), 403
        
        return f(*args, **kwargs)
    
    return decorated_function


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
            metrics = rpc_client.call("prmxs_writer/metrics")
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
            result = rpc_client.call("health")
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
            "Lens": ["proc_collector"],
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
                return abort(500, description=f"作业操作失败: {ret.get('msg', 'unknown error')}")
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
            opt_data['Lens'] = ['proc_collector']

        logger.info("Condor job add request: JobID=%s, slot=%s, Lens=%s, sub_attr=%s",
                     opt_data.get('JobID'), opt_data.get('slot'), opt_data.get('Lens'), opt_data.get('sub_attr'))
        
        try:
            ret = add_condorjob(opt_data)
            if isinstance(ret, dict) and ret.get('status') == 'error':
                logger.error("Condor job add failed: JobID=%s, slot=%s, msg=%s",
                             opt_data.get('JobID'), opt_data.get('slot'), ret.get('msg'))
                return abort(500, description=f"Condor作业添加失败: {ret.get('msg', 'unknown error')}")
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
            "Lens": ["proc_collector"]
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
            opt_data['Lens'] = ['proc_collector']

        logger.info("Slurm job add request: JobID=%s, Lens=%s, sub_attr=%s",
                     opt_data.get('JobID'), opt_data.get('Lens'), opt_data.get('sub_attr'))
        
        try:
            ret = add_slurmjob(opt_data)
            if isinstance(ret, dict) and ret.get('status') == 'error':
                logger.error("Slurm job add failed: JobID=%s, msg=%s", opt_data.get('JobID'), ret.get('msg'))
                return abort(500, description=f"Slurm作业添加失败: {ret.get('msg', 'unknown error')}")
            logger.info("Slurm job add success: JobID=%s", opt_data.get('JobID'))
            response = JobResponse(status='ok', received=opt_data)
            return jsonify(response.model_dump())
        except Exception as e:
            logger.error("Slurm job add exception: JobID=%s, error=%s", opt_data.get('JobID'), str(e), exc_info=True)
            return abort(500, description=str(e))
    
    @app.route('/joblens/jobs/count', methods=['GET'])
    def get_job_count():
        """获取当前注册的作业总数"""
        try:
            result = rpc_client.call("JobRegistry/get_job_count")
            response = JobCountResponse.model_validate(result)
            return jsonify(response.model_dump())
        except RPCError as e:
            return abort(503, description=f"Failed to get job count: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    @app.route('/joblens/jobs', methods=['GET'])
    def list_all_jobs():
        """列出所有作业（不包含彩蛋任务 JobID=0）"""
        try:
            result = rpc_client.call("JobRegistry/list_jobs")
            # RPC 返回 {"status": "ok", "jobs": [...]}，需提取 jobs 数组
            jobs = result.get("jobs", []) if isinstance(result, dict) else (result if isinstance(result, list) else [])
            response = JobsListResponse(status="ok", jobs=jobs)
            return jsonify(response.model_dump())
        except RPCError as e:
            return abort(503, description=f"Failed to list jobs: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    @app.route('/joblens/jobs/<int:job_id>', methods=['GET'])
    def get_job_by_id(job_id: int):
        """获取指定 JobID 的详细信息"""
        try:
            result = rpc_client.call("JobRegistry/get_job", {"JobID": job_id})
            if "error" in result:
                return abort(404, description=result["error"])
            response = JobDetailResponse.model_validate(result)
            return jsonify(response.model_dump())
        except HTTPException:
            raise
        except RPCError as e:
            return abort(503, description=f"Failed to get job: {str(e)}")
        except Exception as e:
            return abort(500, description=f"Unexpected error: {str(e)}")
    
    # ==================== RPC函数接口 ====================
    
    @app.route('/joblens/rpc/functions', methods=['GET'])
    def rpc_functions():
        """获取 RPC 服务端注册的所有可用方法列表"""
        try:
            result = rpc_client.call("func_list")
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
    
    # ==================== Collector接口 ====================
    
    @app.route('/joblens/collectors/perf', methods=['GET'])
    def collectors_perf():
        """获取所有 Collector 的性能统计信息"""
        try:
            result = rpc_client.call("CollectorRegistry/CollectorsPerfCount")
            if result.get("status") == "error":
                return abort(500, description=result.get("msg", "Unknown error"))
            response = CollectorsPerfResponse.model_validate(result)
            return jsonify(response.model_dump())
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
            result = rpc_client.call(method_name)
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
            result = rpc_client.call("WriterManager/WriterPerfCount")
            if result.get("status") == "error":
                return abort(500, description=result.get("msg", "Unknown error"))
            response = WritersPerfResponse.model_validate(result)
            return jsonify(response.model_dump())
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
