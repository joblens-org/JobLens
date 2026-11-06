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
# etcd_client.py
import os
import threading
import logging
from pathlib import Path
from typing import Optional, Tuple, Any, Callable, Dict, List, Union
import etcd3

logger = logging.getLogger(__name__)

TLS_ENV = {
    "ca_cert": "JOBLENS_ETCD_CA_CERT",
    "cert_key": "JOBLENS_ETCD_CERT_KEY",
    "cert_cert": "JOBLENS_ETCD_CERT_CERT",
    "user": "JOBLENS_ETCD_USER",
    "password": "JOBLENS_ETCD_PASSWORD",
}
TLS_REQUIRED_ENV = "JOBLENS_ETCD_TLS_REQUIRED"

_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


def _env(name: str) -> Optional[str]:
    value = os.environ.get(name, "")
    value = value.strip()
    return value or None


def _tls_required() -> bool:
    """etcd 是否必须使用 TLS。

    默认要求；只有显式设置 JOBLENS_ETCD_TLS_REQUIRED=off 才允许明文，
    用于还没有 etcd TLS 的过渡环境。
    """
    raw = os.environ.get(TLS_REQUIRED_ENV)
    if raw is None:
        return True
    return raw.strip().lower() not in _FALSE_VALUES


class EtcdClient:
    """Etcd客户端封装类"""

    def __init__(
        self, 
        host: str = "localhost", 
        port: int = 2379,
        timeout: Optional[int] = None,
        ca_cert: Optional[str] = None,
        cert_key: Optional[str] = None,
        cert_cert: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ):
        """
        初始化etcd客户端

        Args:
            host: etcd主机地址
            port: etcd端口
            timeout: 超时时间（秒）
            ca_cert/cert_key/cert_cert: mTLS 证书路径；未显式传入时读环境变量
            user/password: etcd 认证；未显式传入时读环境变量
            **kwargs: 其他etcd连接参数
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ca_cert = ca_cert or _env(TLS_ENV["ca_cert"])
        self.cert_key = cert_key or _env(TLS_ENV["cert_key"])
        self.cert_cert = cert_cert or _env(TLS_ENV["cert_cert"])
        self.user = user or _env(TLS_ENV["user"])
        self.password = password or _env(TLS_ENV["password"])
        self.tls_required = _tls_required()
        self._client = None
        self._connected = False
        self._watch_threads: Dict[str, threading.Thread] = {}
        self._watch_stop_events: Dict[str, threading.Event] = {}
        self._watch_callbacks: Dict[str, List[Callable]] = {}
        
        # 尝试连接
        self.connect()

    def _validate_tls(self) -> Optional[str]:
        """校验 TLS 配置，返回错误原因；None 表示可以连接。"""
        pair = (self.cert_key, self.cert_cert)
        if bool(pair[0]) != bool(pair[1]):
            return "cert_key 与 cert_cert 必须同时提供"
        if self.tls_required and not self.ca_cert:
            return (
                f"etcd TLS 已启用但缺少 CA 证书；请设置 {TLS_ENV['ca_cert']}，"
                f"或显式设置 {TLS_REQUIRED_ENV}=off（不推荐）"
            )
        for label, path in (("CA", self.ca_cert), ("cert_key", self.cert_key), ("cert_cert", self.cert_cert)):
            if path and not Path(path).is_file():
                return f"etcd {label} 文件不存在: {path}"
        if self.tls_required and not (self.user and self.password):
            logger.warning("etcd TLS 已启用但未提供认证信息（%s/%s）", TLS_ENV["user"], TLS_ENV["password"])
        return None

    def connect(self) -> bool:
        """连接到etcd服务器；TLS 配置不完整时拒绝连接，不回退明文。"""
        self._connected = False
        self._client = None

        error = self._validate_tls()
        if error:
            logger.error("拒绝连接 etcd %s:%s: %s", self.host, self.port, error)
            return False

        if not self.ca_cert and not self.cert_cert:
            logger.warning(
                "etcd %s:%s 正在使用明文 gRPC（%s=off）；控制面内容可被网络读取/篡改",
                self.host, self.port, TLS_REQUIRED_ENV,
            )

        client_kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.timeout:
            client_kwargs["timeout"] = self.timeout
        if self.ca_cert:
            client_kwargs["ca_cert"] = self.ca_cert
        if self.cert_key:
            client_kwargs["cert_key"] = self.cert_key
        if self.cert_cert:
            client_kwargs["cert_cert"] = self.cert_cert
        if self.user:
            client_kwargs["user"] = self.user
        if self.password:
            client_kwargs["password"] = self.password

        try:
            self._client = etcd3.client(**client_kwargs)

            # 测试连接
            self._client.get('/')
            self._connected = True
            scheme = "https" if self.ca_cert else "http"
            logger.info("成功连接到etcd: %s://%s:%s", scheme, self.host, self.port)
            return True
            
        except Exception as e:
            logger.error("连接etcd失败: %s", e)
            self._connected = False
            self._client = None
            return False
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self._connected and self._client is not None
    
    def get(self, key: str) -> Tuple[Optional[bytes], Optional[Any]]:
        """
        获取键值
        
        Args:
            key: 键名
            
        Returns:
            (value, metadata) 元组
        """
        if not self.is_connected():
            logger.warning("etcd客户端未连接")
            return None, None
        
        try:
            return self._client.get(key)
        except Exception as e:
            logger.error(f"获取键值失败 {key}: {e}")
            return None, None
    
    def put(self, key: str, value: Any) -> bool:
        """
        设置键值
        
        Args:
            key: 键名
            value: 值
            
        Returns:
            是否成功
        """
        if not self.is_connected():
            logger.warning("etcd客户端未连接")
            return False
        
        try:
            self._client.put(key, value)
            logger.debug(f"设置键值: {key} = {value}")
            return True
        except Exception as e:
            logger.error(f"设置键值失败 {key}: {e}")
            return False
    
    def watch(
        self, 
        key: str, 
        callback: Optional[Callable] = None,
        start_revision: Optional[int] = None,
        watch_id: Optional[str] = None,
        **watch_kwargs
    ) -> Union[Tuple[Any, Callable], str]:
        """
        监听键值变化
        
        Args:
            key: 要监听的键名
            callback: 回调函数，接收事件作为参数
            start_revision: 开始监听修订版本号
            watch_id: 监听ID，用于标识和取消特定监听
            **watch_kwargs: 其他watch参数
            
        Returns:
            如果未提供callback: (events_iterator, cancel_func) 元组
            如果提供了callback: watch_id 字符串，可用于取消监听
        """
        if not self.is_connected():
            logger.warning("etcd客户端未连接")
            if callback:
                return ""
            return None, lambda: None
        
        try:
            # 如果没有回调函数，返回原始watch接口
            if callback is None:
                return self._client.watch(key, start_revision=start_revision, **watch_kwargs)
            
            # 生成watch_id
            if watch_id is None:
                watch_id = f"{key}_{threading.get_ident()}_{id(callback)}"
            
            # 创建停止事件
            stop_event = threading.Event()
            self._watch_stop_events[watch_id] = stop_event
            
            # 存储回调函数
            self._watch_callbacks.setdefault(watch_id, []).append(callback)
            
            def watch_worker():
                """监听工作线程"""
                try:
                    # 获取事件迭代器
                    events_iterator, cancel_func = self._client.watch(
                        key, 
                        start_revision=start_revision, 
                        **watch_kwargs
                    )
                    
                    # 存储取消函数
                    def enhanced_cancel():
                        cancel_func()
                        stop_event.set()
                    
                    # 更新停止事件的处理
                    def check_and_cancel():
                        if stop_event.is_set():
                            cancel_func()
                            return True
                        return False
                    
                    # 监听循环
                    for event in events_iterator:
                        # 检查是否应该停止
                        if stop_event.is_set():
                            cancel_func()
                            break
                        
                        # 调用所有注册的回调函数
                        for cb in self._watch_callbacks.get(watch_id, []):
                            try:
                                cb(event)
                            except Exception as e:
                                logger.error(f"回调函数执行失败 (watch_id: {watch_id}): {e}")
                
                except Exception as e:
                    if not stop_event.is_set():  # 非主动停止的错误
                        logger.error(f"监听线程异常 (watch_id: {watch_id}): {e}")
                
                finally:
                    # 清理资源
                    self._cleanup_watch_resources(watch_id)
            
            # 创建并启动监听线程
            watch_thread = threading.Thread(
                target=watch_worker,
                name=f"etcd_watch_{watch_id}",
                daemon=True
            )
            watch_thread.start()
            
            # 存储线程引用
            self._watch_threads[watch_id] = watch_thread
            
            logger.info(f"已启动监听: key={key}, watch_id={watch_id}")
            return watch_id
            
        except Exception as e:
            logger.error(f"监听键值失败 {key}: {e}")
            if callback:
                return ""
            return None, lambda: None
    
    def watch_prefix(
        self, 
        prefix: str, 
        callback: Optional[Callable] = None,
        start_revision: Optional[int] = None,
        watch_id: Optional[str] = None,
        **watch_kwargs
    ) -> Union[Tuple[Any, Callable], str]:
        """
        监听前缀键值变化
        
        Args:
            prefix: 键前缀
            callback: 回调函数，接收事件作为参数
            start_revision: 开始监听修订版本号
            watch_id: 监听ID，用于标识和取消特定监听
            **watch_kwargs: 其他watch参数
            
        Returns:
            如果未提供callback: (events_iterator, cancel_func) 元组
            如果提供了callback: watch_id 字符串，可用于取消监听
        """
        if not self.is_connected():
            logger.warning("etcd客户端未连接")
            if callback:
                return ""
            return None, lambda: None
        
        try:
            # 如果没有回调函数，返回原始watch_prefix接口
            if callback is None:
                return self._client.watch_prefix(prefix, start_revision=start_revision, **watch_kwargs)
            
            # 生成watch_id
            if watch_id is None:
                watch_id = f"prefix_{prefix}_{threading.get_ident()}_{id(callback)}"
            
            # 创建停止事件
            stop_event = threading.Event()
            self._watch_stop_events[watch_id] = stop_event
            
            # 存储回调函数
            self._watch_callbacks.setdefault(watch_id, []).append(callback)
            
            def watch_worker():
                """监听工作线程"""
                try:
                    # 获取事件迭代器
                    events_iterator, cancel_func = self._client.watch_prefix(
                        prefix, 
                        start_revision=start_revision, 
                        **watch_kwargs
                    )
                    
                    # 监听循环
                    for event in events_iterator:
                        # 检查是否应该停止
                        if stop_event.is_set():
                            cancel_func()
                            break
                        
                        # 调用所有注册的回调函数
                        for cb in self._watch_callbacks.get(watch_id, []):
                            try:
                                cb(event)
                            except Exception as e:
                                logger.error(f"回调函数执行失败 (watch_id: {watch_id}): {e}")
                
                except Exception as e:
                    if not stop_event.is_set():  # 非主动停止的错误
                        logger.error(f"监听线程异常 (watch_id: {watch_id}): {e}")
                
                finally:
                    # 清理资源
                    self._cleanup_watch_resources(watch_id)
            
            # 创建并启动监听线程
            watch_thread = threading.Thread(
                target=watch_worker,
                name=f"etcd_watch_prefix_{watch_id}",
                daemon=True
            )
            watch_thread.start()
            
            # 存储线程引用
            self._watch_threads[watch_id] = watch_thread
            
            logger.info(f"已启动前缀监听: prefix={prefix}, watch_id={watch_id}")
            return watch_id
            
        except Exception as e:
            logger.error(f"监听前缀键值失败 {prefix}: {e}")
            if callback:
                return ""
            return None, lambda: None
    
    def add_watch_callback(
        self, 
        watch_id: str, 
        callback: Callable
    ) -> bool:
        """
        为已存在的监听添加额外的回调函数
        
        Args:
            watch_id: 监听ID
            callback: 回调函数
            
        Returns:
            是否成功添加
        """
        if watch_id not in self._watch_callbacks:
            logger.warning(f"监听ID不存在: {watch_id}")
            return False
        
        self._watch_callbacks[watch_id].append(callback)
        logger.debug(f"已为监听 {watch_id} 添加回调函数")
        return True
    
    def remove_watch_callback(
        self, 
        watch_id: str, 
        callback: Optional[Callable] = None
    ) -> bool:
        """
        移除监听的回调函数
        
        Args:
            watch_id: 监听ID
            callback: 要移除的回调函数，如果为None则移除所有
            
        Returns:
            是否成功移除
        """
        if watch_id not in self._watch_callbacks:
            logger.warning(f"监听ID不存在: {watch_id}")
            return False
        
        if callback is None:
            self._watch_callbacks[watch_id].clear()
            logger.debug(f"已移除监听 {watch_id} 的所有回调函数")
        else:
            try:
                self._watch_callbacks[watch_id].remove(callback)
                logger.debug(f"已移除监听 {watch_id} 的指定回调函数")
            except ValueError:
                logger.warning(f"回调函数未找到: {watch_id}")
                return False
        
        return True
    
    def stop_watch(self, watch_id: str) -> bool:
        """
        停止指定的监听
        
        Args:
            watch_id: 监听ID
            
        Returns:
            是否成功停止
        """
        if watch_id not in self._watch_stop_events:
            logger.warning(f"监听ID不存在或已停止: {watch_id}")
            return False
        
        try:
            # 设置停止事件
            self._watch_stop_events[watch_id].set()
            
            # 等待线程结束
            if watch_id in self._watch_threads:
                thread = self._watch_threads[watch_id]
                if thread.is_alive():
                    thread.join(timeout=5.0)
            
            # 清理资源
            self._cleanup_watch_resources(watch_id)
            
            logger.info(f"已停止监听: watch_id={watch_id}")
            return True
            
        except Exception as e:
            logger.error(f"停止监听失败 {watch_id}: {e}")
            return False
    
    def stop_all_watches(self) -> None:
        """停止所有监听"""
        watch_ids = list(self._watch_stop_events.keys())
        for watch_id in watch_ids:
            self.stop_watch(watch_id)
        
        logger.info(f"已停止所有监听，共 {len(watch_ids)} 个")
    
    def _cleanup_watch_resources(self, watch_id: str) -> None:
        """清理监听资源"""
        if watch_id in self._watch_stop_events:
            del self._watch_stop_events[watch_id]
        
        if watch_id in self._watch_threads:
            del self._watch_threads[watch_id]
        
        if watch_id in self._watch_callbacks:
            del self._watch_callbacks[watch_id]
    
    def get_watch_status(self) -> Dict:
        """获取所有监听状态"""
        status = {
            "active_watches": len(self._watch_threads),
            "watch_details": {}
        }
        
        for watch_id, thread in self._watch_threads.items():
            status["watch_details"][watch_id] = {
                "thread_alive": thread.is_alive(),
                "callbacks_count": len(self._watch_callbacks.get(watch_id, [])),
                "stop_event_set": self._watch_stop_events.get(watch_id, threading.Event()).is_set()
            }
        
        return status
    
    def get_all(self, prefix: str = "") -> dict:
        """
        获取指定前缀的所有键值
        
        Args:
            prefix: 键前缀
            
        Returns:
            键值字典
        """
        if not self.is_connected():
            logger.warning("etcd客户端未连接")
            return {}
        
        try:
            result = {}
            for value, metadata in self._client.get_prefix(prefix):
                if metadata:
                    result[metadata.key.decode('utf-8')] = value.decode('utf-8')
            return result
        except Exception as e:
            logger.error(f"获取前缀键值失败 {prefix}: {e}")
            return {}
    
    def delete(self, key: str) -> bool:
        """
        删除键值
        
        Args:
            key: 要删除的键名
            
        Returns:
            是否成功
        """
        if not self.is_connected():
            logger.warning("etcd客户端未连接")
            return False
        
        try:
            self._client.delete(key)
            logger.debug(f"删除键值: {key}")
            return True
        except Exception as e:
            logger.error(f"删除键值失败 {key}: {e}")
            return False
    
    def close(self) -> None:
        """关闭连接"""
        # 停止所有监听
        self.stop_all_watches()
        
        # 关闭客户端连接
        if self._client:
            try:
                # etcd3客户端通常不需要显式关闭，但可以清理资源
                self._connected = False
                self._client = None
                logger.info("etcd客户端已关闭")
            except Exception as e:
                logger.error(f"关闭etcd客户端失败: {e}")
    
    def get_status(self) -> dict:
        """获取客户端状态"""
        return {
            "connected": self.is_connected(),
            "host": self.host,
            "port": self.port,
            "timeout": self.timeout,
            "active_watches": len(self._watch_threads)
        }
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
    
    def __del__(self):
        """析构函数"""
        try:
            self.close()
        except:
            pass
