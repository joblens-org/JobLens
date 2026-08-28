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
Gunicorn configuration for JobLens Trigger

This file is loaded by gunicorn when starting the application.
Since preload_app=True, the app is initialized in the master process.
"""

import os
import yaml

# ---------------------------------------------------------------------------
# gRPC fork 支持（必须在任何 `import grpc` / etcd3 之前设置环境变量才生效）
#
# etcd3 底层依赖 grpcio 的 C 扩展（cygrpc），gRPC core 使用多线程，
# 官方明确声明不支持 fork()。在 gunicorn worker 被超时杀死后 master 重新
# fork worker 时，残留的 gRPC 后台线程状态会在子进程中损坏，导致 abort/段错误
# （core dump）。此处在 gunicorn 加载配置阶段（master 进程、任何 grpc import 之前）
# 提前注入官方 fork 支持开关，使所有后续 fork 出的 worker 都继承。
#
# 参考：grpc/grpc doc/fork_support.md —— 两个变量必须同时设置，
# 且 fork 支持仅在 'poll' / 'epoll1' 轮询策略下有效。
# systemd unit 已通过 Environment= 设置；此处为手动/开发直启 gunicorn 时的兜底。
# ---------------------------------------------------------------------------
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "true")
os.environ.setdefault("GRPC_POLL_STRATEGY", "poll")


def _resolve_loglevel() -> str:
    """从 trigger 配置文件读取 logging.level，供 gunicorn 自身日志复用；读取失败回退 info"""
    config_path = os.environ.get(
        "JOBLENS_TRIGGER_CONFIG", "/etc/JobLens/trigger/config.yaml"
    )
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return str(cfg.get("logging", {}).get("level", "info")).lower()
    except Exception:
        return "info"


# Network binding
bind = "0.0.0.0:7592"

# Worker configuration
workers = 1
worker_class = "sync"
timeout = 120
keepalive = 5

# Application preloading
# When True, the app is loaded in the master process before forking workers
preload_app = False

# Logging
accesslog = "-"
errorlog = "-"
loglevel = _resolve_loglevel()


def post_fork(server, worker):
    """
    Called after a worker process is forked.
    
    Args:
        server: The gunicorn server instance
        worker: The worker instance
    """
    server.log.info(f"Worker spawned (pid: {worker.pid})")
    # 记录 gRPC fork 支持环境变量的实际生效值，便于线上排查 core dump 问题。
    # etcd3(grpcio) 的 client 应在此 fork 之后、于 worker 进程内创建（use-after-fork）。
    server.log.info(
        "gRPC fork settings in worker: "
        f"GRPC_ENABLE_FORK_SUPPORT={os.environ.get('GRPC_ENABLE_FORK_SUPPORT')}, "
        f"GRPC_POLL_STRATEGY={os.environ.get('GRPC_POLL_STRATEGY')}"
    )


def worker_exit(server, worker):
    """
    Called when a worker process is exiting.
    
    This hook is used to cleanup resources in worker processes.
    
    Args:
        server: The gunicorn server instance
        worker: The worker instance
    """
    server.log.info(f"Worker exiting (pid: {worker.pid})")
    
    # try:
    #     # Get the Flask app instance from the server
    #     app = server.app
        
    #     # Cleanup resources via app_context
    #     if hasattr(app, '_app_context'):
    #         server.log.info("Cleaning up worker resources...")
    #         app._app_context.shutdown()
    #         server.log.info("Worker resources cleaned up")
    # except Exception as e:
    #     server.log.error(f"Error during worker cleanup: {e}")


def on_exit(server):
    """
    Called when the master process is exiting.
    
    This hook is used to cleanup resources in the master process.
    
    Args:
        server: The gunicorn server instance
    """
    server.log.info("Gunicorn master process is exiting...")
    
    try:
        # Get the Flask app instance from the server
        # server.app is the gunicorn WSGIApplication, wsgi() returns the actual Flask app
        app = server.app.wsgi()
        
        # Cleanup resources via app_context
        if hasattr(app, '_app_context'):
            server.log.info("Cleaning up master process resources...")
            app._app_context.shutdown()
            server.log.info("Master process resources cleaned up")
    except Exception as e:
        server.log.error(f"Error during master cleanup: {e}")
    
    server.log.info("Gunicorn master process has exited")