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
import requests
import socket
import threading
import time
import logging
from typing import Optional, Tuple

from trigger.utils import cluster_info

logger = logging.getLogger(__name__)


class ServiceRegistrar:
    """服务注册器，支持向注册中心注册服务，并提供心跳机制"""
    
    def __init__(self, registry_url: str, service_host: str, service_port: int,
                 retry_interval: int = 10, max_retries: int = 3,
                 heartbeat_interval: int = 1800):
        """
        初始化服务注册器
        
        Args:
            registry_url: 注册中心URL
            service_host: 服务主机名
            service_port: 服务端口
            retry_interval: 注册失败重试间隔（秒）
            max_retries: 最大重试次数
            heartbeat_interval: 心跳间隔（秒），默认30分钟
            enabled: 是否启用注册功能
        """
        self.registry_url = registry_url.rstrip('/')
        self.service_host = service_host
        self.service_port = service_port
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self.heartbeat_interval = heartbeat_interval
        self.enabled = True
        
        self.service_id = None
        self._heartbeat_thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()  # 保护service_id的并发访问
        
        
        
        # 新增：注册中心下发的etcd路径（完整字符串）
        self.etcd_path = None
        self.etcd_workdir = None
        self.etcd_addr = None
        self.etcd_port = None
        
        # 版本信息
        self.version = self._get_version()
    
    def _get_version(self) -> str:
        """获取版本信息"""
        try:
            from trigger.core.tools import get_joblens_version
            joblens_buildinfo = get_joblens_version()
            if joblens_buildinfo['version'] != 'UNKNOWN':
                return f"{joblens_buildinfo['version']} {joblens_buildinfo['build_id']} {joblens_buildinfo['build_time_local']}"
        except Exception as e:
            logger.warning(f"获取JobLens版本失败: {e}")
        
        # 回退到触发器版本
        return '0.0.8'
    
    def _fetch_etcd_path(self) -> Optional[str]:
        """
        从注册中心获取etcd路径（完整字符串）
        
        Returns:
            etcd路径（如: "joblens/prod/compute-node-01"），如果获取失败返回None
        """
        if not self.service_id:
            return None
        
        try:
            response = requests.get(
                f"{self.registry_url}/services/{self.service_id}/etcd-path",
                timeout=5
            )
            response.raise_for_status()
            # 直接返回完整路径字符串，不解析格式
            etcd_path = response.json().get("directory")
            if etcd_path:
                logger.info(f"从注册中心获取etcd路径: {etcd_path}")
            return etcd_path
        except Exception as e:
            logger.warning(f"从注册中心获取etcd路径失败: {e}")
            return None
    
    def _fetch_etcd_addr(self) -> Optional[Tuple[str, int]]:
        """
        从注册中心获取etcd地址和端口
        
        :return: 返回地址和端口
        :rtype: Tuple[str, int] | None
        """
        if not self.service_id:
            return None, None
        
        try:
            response = requests.get(
                f"{self.registry_url}/etcd-addr",
                timeout=5
            )
            response.raise_for_status()
            # 直接返回完整路径字符串，不解析格式
            addr = response.json()['host']
            port = response.json()['port']
            self.etcd_workdir = response.json().get("workdir")
            if addr and port:
                logger.info(f"从注册中心获取etcd地址: {addr}:{port}")
            return addr, port
        except Exception as e:
            logger.warning(f"从注册中心获取etcd地址失败: {e}")
            return None, None
        
    
    def register(self) -> bool:
        """
        向注册中心注册服务，支持重试机制
        
        Returns:
            是否注册成功（如果enabled=False，返回True）
        """
        if not self.enabled:
            logger.info("服务注册已禁用，跳过注册")
            return True
        
        for attempt in range(self.max_retries):
            try:
                # 防止并发注册
                time.sleep(1)
                
                data = {
                    "host": self.service_host,
                    "port": self.service_port,
                    "name": f"joblens-{self.service_host}:{self.service_port}",
                    "version": self.version,
                    "metadata": {
                        "cluster_discovery": cluster_info.discover_cluster_discovery()
                    }
                }
                
                response = requests.post(
                    f"{self.registry_url}/register",
                    json=data,
                    timeout=5
                )
                response.raise_for_status()
                result = response.json()
                
                with self._lock:
                    self.service_id = result.get("service_id")
                
                logger.info(f"✓ 已向注册中心注册 (ID: {self.service_id})")
                
                self.etcd_addr, self.etcd_port = self._fetch_etcd_addr()
                self.etcd_path = self._fetch_etcd_path()
                
                # 启动心跳线程
                self._start_heartbeat()
                
                return True
                
            except Exception as e:
                logger.warning(f"⚠ 注册失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    logger.info(f"{self.retry_interval}秒后重试...")
                    time.sleep(self.retry_interval)
        
        logger.error("服务注册最终失败，将继续运行但无法被注册中心发现")
        return False
    
    def _start_heartbeat(self):
        """启动心跳线程，定期重新注册"""
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            logger.warning("心跳线程已在运行，忽略重复启动")
            return
        
        def heartbeat_worker():
            logger.info(f"心跳线程已启动，每{self.heartbeat_interval}秒重新注册一次")
            
            while not self._stop_event.is_set():
                try:
                    # 等待心跳间隔
                    if self._stop_event.wait(self.heartbeat_interval):
                        break  # 收到停止信号
                    
                    # 重新注册
                    with self._lock:
                        current_service_id = self.service_id
                    
                    if current_service_id:
                        logger.info("心跳触发：重新注册服务...")
                        
                        # 再注册
                        self.register()
                    else:
                        logger.warning("服务未注册，心跳跳过")
                        
                except Exception as e:
                    logger.error(f"心跳线程异常: {e}", exc_info=True)
                    # 继续运行，不退出线程
        
        self._heartbeat_thread = threading.Thread(target=heartbeat_worker, daemon=True)
        self._heartbeat_thread.start()
    
    def unregister(self):
        """从注册中心注销服务"""
        if not self.enabled:
            logger.info("服务注册已禁用，跳过注销")
            return
        
        with self._lock:
            current_service_id = self.service_id
            if not current_service_id:
                logger.warning("未找到service_id，跳过注销")
                return
        
        try:
            response = requests.delete(
                f"{self.registry_url}/unregister/{current_service_id}",
                timeout=5
            )
            response.raise_for_status()
            logger.info(f"✓ 已从注册中心注销 (ID: {current_service_id})")
            
        except Exception as e:
            logger.warning(f"⚠ 注销失败: {e}")
        finally:
            with self._lock:
                self.service_id = None
    
    def shutdown(self):
        """优雅关闭，停止心跳线程并注销服务"""
        logger.info("正在停止ServiceRegistrar...")
        
        # 发送停止信号
        self._stop_event.set()
        
        # 等待心跳线程结束（最多5秒）
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=5)
            if self._heartbeat_thread.is_alive():
                logger.warning("心跳线程未在5秒内停止，强制继续")
        
        # 注销服务
        self.unregister()
        
        logger.info("ServiceRegistrar已停止")
    
    def get_status(self) -> dict:
        """获取注册状态"""
        with self._lock:
            return {
                "enabled": self.enabled,
                "registered": self.service_id is not None,
                "service_id": self.service_id,
                "service_host": self.service_host,
                "service_port": self.service_port,
                "registry_url": self.registry_url,
                "heartbeat_interval": self.heartbeat_interval,
                "version": self.version,
                "etcd_path": self.etcd_path,
                "etcd_workdir": self.etcd_workdir,
                "etcd_addr": self.etcd_addr,
                "etcd_port": self.etcd_port
            }
