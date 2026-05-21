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
import threading
import logging
from typing import Optional, Tuple, Any, Callable, Dict, List, Union
import etcd3

logger = logging.getLogger(__name__)

class EtcdClient:
    """Etcd客户端封装类"""
    
    def __init__(
        self, 
        host: str = "localhost", 
        port: int = 2379,
        timeout: Optional[int] = None,
        **kwargs
    ):
        """
        初始化etcd客户端
        
        Args:
            host: etcd主机地址
            port: etcd端口
            timeout: 超时时间（秒）
            **kwargs: 其他etcd连接参数
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._client = None
        self._connected = False
        self._watch_threads: Dict[str, threading.Thread] = {}
        self._watch_stop_events: Dict[str, threading.Event] = {}
        self._watch_callbacks: Dict[str, List[Callable]] = {}
        
        # 尝试连接
        self.connect()
    
    def connect(self) -> bool:
        """连接到etcd服务器"""
        try:
            if self.timeout:
                self._client = etcd3.client(
                    host=self.host,
                    port=self.port,
                    timeout=self.timeout
                )
            else:
                self._client = etcd3.client(
                    host=self.host,
                    port=self.port
                )
            
            # 测试连接
            self._client.get('/')
            self._connected = True
            logger.info(f"成功连接到etcd: {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"连接etcd失败: {e}")
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
