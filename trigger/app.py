#!/usr/bin/env python3
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
#!/usr/bin/env python3
"""
JobLens Trigger Entry Point

JobLens触发器入口文件

初始化流程：
1. 加载配置（环境变量 > 配置文件 > 默认值）
2. 初始化组件（RPC客户端、服务注册器、配置管理器）
3. 创建Flask应用并注册路由
4. 异步注册服务（不阻塞启动）

关闭流程（由gunicorn钩子触发）：
1. 停止ServiceRegistrar（心跳线程和注销服务）
2. 停止ConfigManager（监控线程）
3. 关闭RPC客户端连接
"""
import flask
from flask import has_request_context, request
from werkzeug.local import LocalProxy

# 保存原始方法
_original_log_exception = flask.Flask.log_exception
_original_handle_exception = flask.Flask.handle_exception

def safe_log_exception(self, exc_info):
    """安全的异常日志记录，处理上下文丢失"""
    if not has_request_context():
        # 无上下文时降级记录
        self.logger.error(
            f"Exception occurred (outside context): {exc_info[1] if exc_info else 'Unknown'}",
            exc_info=exc_info
        )
        return
    
    try:
        # 尝试安全地获取请求信息
        # 使用 try-except 而不是依赖 has_request_context()，因为上下文可能在检查后丢失
        try:
            path = request.path
            method = request.method
            self.logger.error(
                f"Exception on {path} [{method}]",
                exc_info=exc_info
            )
        except RuntimeError:
            # 上下文在访问时已丢失
            self.logger.error(
                f"Exception occurred (context lost during request access): {exc_info[1] if exc_info else 'Unknown'}",
                exc_info=exc_info
            )
    except Exception:
        # 如果仍然失败，最低限度记录
        self.logger.error(
            f"Exception occurred (context lost during logging): {exc_info[1] if exc_info else 'Unknown'}",
            exc_info=exc_info
        )

def safe_handle_exception(self, e):
    """安全的异常处理，确保上下文不会重复 pop"""
    # 如果上下文已经丢失，直接返回 500 响应，不尝试记录
    if not has_request_context():
        from werkzeug.exceptions import InternalServerError
        return InternalServerError()
    
    try:
        return _original_handle_exception(self, e)
    except RuntimeError as re:
        if "Working outside of request context" in str(re) or "ContextVar" in str(re):
            # 上下文丢失，降级处理
            self.logger.error(f"Context lost during exception handling: {e}")
            from werkzeug.exceptions import InternalServerError
            return InternalServerError()
        raise
    except LookupError as le:
        # ContextVar 查找失败，说明上下文已丢失
        self.logger.error(f"ContextVar lookup failed during exception handling: {e}")
        from werkzeug.exceptions import InternalServerError
        return InternalServerError()

# 应用补丁
flask.Flask.log_exception = safe_log_exception
flask.Flask.handle_exception = safe_handle_exception

from trigger.app_factory import create_application
from trigger.utils.email_notifier import simple_send

# 创建应用实例
# 使用工厂函数创建，所有初始化逻辑在工厂中完成
try:
    config_path = os.environ.get('JOBLENS_CONFIG_PATH', '/etc/JobLens/config.yaml')
    app = create_application(config_path)
except Exception as e:
    if __name__ != "__main__":
        # 开发环境不告警，生产环境告警
        simple_send('JobLens Trigger启动失败', f"应用初始化失败: {e}")
    raise RuntimeError(f"应用初始化失败: {e}")

if __name__ == "__main__":
    # 开发环境直接运行
    # 从_app_context获取配置
    port = app._app_context.config['service']['port']
    host = app._app_context.config['service']['host']
    
    print(f"启动开发服务器")
    print(f"地址: http://{host}:{port}")
    
    app.run(host=host, port=port, debug=True)