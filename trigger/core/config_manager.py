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
import os
import hashlib
import threading
from pathlib import Path
from typing import Dict, List, Callable, Optional, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
try:
    from .etcd_client import EtcdClient
except:
    from etcd_client import EtcdClient
import logging
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import json
import shutil
from pprint import pprint
import yaml
import time

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(
        self,
        etcd_client: EtcdClient,
        etcd_workdir: str,
        etcd_mode_path: str,
        local_config_path: str,
        use_etcd: bool = True,
        etcd_priority: bool = True,
        config_change_callback: Optional[Callable[[Dict], None]] = None
    ):
        self.etcd_client = etcd_client
        self.etcd_workdir = etcd_workdir
        self.etcd_mode_path = etcd_mode_path
        self.local_config_path = Path(local_config_path)
        self.use_etcd = use_etcd
        self.etcd_priority = etcd_priority
        self.config_cache: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        
        # 回调函数列表，配置更新时会调用这些函数
        self.callbacks: List[Callable[[Dict], None]] = []
        if config_change_callback:
            self.callbacks.append(config_change_callback)
        # etcd watch id
        self.etcd_mode_watch_id: Optional[str] = None
        self.etcd_config_watch_id: Optional[str] = None
        # local watch
        self.local_config_watch_thread: Optional[threading.Thread] = None
        
        self.running = False
        
        # etcd 原始内容
        self.etcd_raw_content: Optional[bytes] = None
        
        # 同步状态管理
        self.sync_status = {
            "local_synced": True,      # 本地配置是否已同步到 etcd
            "etcd_synced": True,       # etcd 配置是否已同步到本地
            "local_modified": False,   # 本地配置是否被外部修改
            "needs_restore": False,    # 是否需要恢复本地配置
            "last_sync_time": None     # 最后同步时间
        }
        
        self.raw_local_config = self._load_raw_local_config()
        self.local_config = self._load_local_config()
        if self.use_etcd:
            self.mode = self._fetch_current_mode()
            self.remote_config, self.etcd_raw_content = self._fetch_remote_config()
            is_diff = self._sync_config_from_etcd2local()
            if is_diff:
                self._notify_callbacks()
            
    def _sync_config_from_etcd2local(self):
        is_diff = self._is_diff_local_config(self.remote_config)
        if is_diff:
            logger.info("Detected config change in etcd, updating local config")
            with self.lock:
                self.local_config = self.remote_config
                # 设置未同步标志
                self.sync_status["etcd_synced"] = True
                self.sync_status["last_sync_time"] = time.time()
                
                self.save_local_config(use_raw_content=True)
        return is_diff

    def _load_raw_local_config(self) -> bytes:
        if not os.path.exists(self.local_config_path):
            logger.warning(f"Local config file {self.local_config_path} does not exist.")
            return b""
        with open(self.local_config_path, "rb") as f:
            return f.read()
    
    def _load_local_config(self) -> Dict[str, Any]:
        raw_data = self._load_raw_local_config()
        if not raw_data:
            return {}
        try:
            return yaml.safe_load(raw_data.decode("utf-8"))
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse local config: {e}")
            return {}
    
    def _is_diff_local_config(self, new_data: dict) -> bool:
        # 每一项挨个比较，如果有任何一项不同，就认为配置发生了变化
        current_data = self.local_config
        if set(current_data.keys()) != set(new_data.keys()):
            return True
        for key, value in new_data.items():
            if current_data[key] != value:
                return True
        return False
    
    def _fetch_current_mode(self) -> Optional[str]:
        if not self.use_etcd:
            return ''
        value, meta = self.etcd_client.get(self.etcd_mode_path)
        if value is not None:
            return value.decode("utf-8")
        return None
            
    def _fetch_remote_config(self) -> tuple[Dict[str, Any], bytes]:
        """从 etcd 加载配置，返回 (配置字典, 原始字节)"""
        if not self.use_etcd:
            return {}, b''
        try:
            full_path = f'{self.etcd_workdir}/modes/{self.mode}/config/config.yaml'.replace('//', '/')
            value, meta = self.etcd_client.get(full_path)
            if value:
                # 保存原始内容
                self.etcd_raw_content = value
                config_dict = yaml.safe_load(value.decode("utf-8")) or {}
                return config_dict, value
            return {}, b''
        except Exception as e:
            logger.error(f"Failed to fetch remote config: {e}")
            return {}, b''
    
    def _handle_etcd_mode_update(self, event):
        new_mode = event.value.decode("utf-8")
        logger.info(f"Detected mode change in etcd: {new_mode}")
        with self.lock:
            if new_mode != self.mode:
                self.mode = new_mode
                # 模式变化了，重新加载远程配置
                self.etcd_client.stop_watch(self.etcd_config_watch_id)
                self.remote_config, self.etcd_raw_content = self._fetch_remote_config()
                self._sync_config_from_etcd2local()
                self._start_etcd_config_watch()
                self._notify_callbacks()
    
    def _handle_etcd_config_update(self, event):
        self.remote_config = yaml.safe_load(event.value.decode("utf-8"))
        self.etcd_raw_content = event.value
        self._sync_config_from_etcd2local()
        self._notify_callbacks()
    
    def _handle_config_change(self):
        new_local_config = self._load_local_config()
        if self._is_diff_local_config(new_local_config):
            if self.etcd_priority:
                # etcd 优先级高，拒绝本地修改，恢复为 etcd 配置
                logger.warning("本地配置被外部修改，但 etcd_priority=True，恢复为 etcd 配置")
                with self.lock:
                    # 恢复为 etcd 配置
                    if self.etcd_raw_content:
                        self.local_config = yaml.safe_load(self.etcd_raw_content.decode("utf-8"))
                        # 使用原始内容保存到本地
                        self.save_local_config(use_raw_content=True)
                    # 设置本地修改标志
                    self.sync_status["local_modified"] = True
                    self.sync_status["needs_restore"] = True
                    self._notify_callbacks()
            else:
                # 允许本地修改，但不能修改 etcd 中内容
                logger.info("Local config has changed, updating...")
                with self.lock:
                    self.local_config = new_local_config
                    # 设置未同步标志
                    self.sync_status["local_synced"] = False
                    self.sync_status["local_modified"] = True
                    self._notify_callbacks()
                
                # 注意：etcd_priority=False 时，不自动保存到 etcd
    
    def _start_etcd_config_watch(self):
        if not self.use_etcd:
            return
        config_path = f'{self.etcd_workdir}/modes/{self.mode}/config/config.yaml'.replace('//','/')
        self.etcd_config_watch_id = self.etcd_client.watch(config_path, self._handle_etcd_config_update)
    
    def _start_etcd_watch(self):
        if not self.use_etcd:
            return
        # 监听模式变化
        self.etcd_mode_watch_id = self.etcd_client.watch(self.etcd_mode_path, self._handle_etcd_mode_update)
        # 监听配置变化
        self._start_etcd_config_watch()
    
    def _start_local_config_watch(self):
        def _watch_local_config(self) -> None:
            """监控本地配置文件变化"""
            class ConfigFileHandler(FileSystemEventHandler):
                def __init__(self, manager):
                    self.manager = manager
                
                def on_modified(self, event):
                    if event.src_path == str(self.manager.local_config_path):
                        logger.info("检测到本地配置文件变化")
                        self.manager._handle_config_change()
            
            try:
                event_handler = ConfigFileHandler(self)
                observer = Observer()
                observer.schedule(
                    event_handler, 
                    path=str(self.local_config_path.parent), 
                    recursive=False
                )
                observer.start()
                
                while self.running:
                    time.sleep(1)
                
                observer.stop()
                observer.join()
                
            except Exception as e:
                logger.error(f"文件监控错误: {e}")

        self.local_config_watch_thread=threading.Thread(target=_watch_local_config, args=(self,), daemon=True)
        self.local_config_watch_thread.start()    
        
    def start(self):
        self.running = True

        if self.use_etcd:
            self.mode = self._fetch_current_mode()
            self.remote_config, self.etcd_raw_content = self._fetch_remote_config()
        
        # 启动etcd监控
        self._start_etcd_watch()
        # 启动本地文件监控
        self._start_local_config_watch()
    
    def stop(self):
        if self.use_etcd:
            if self.etcd_mode_watch_id:
                self.etcd_client.stop_watch(self.etcd_mode_watch_id)
            if self.etcd_config_watch_id:
                self.etcd_client.stop_watch(self.etcd_config_watch_id)
        
        self.running = False
        if self.local_config_watch_thread:
            self.local_config_watch_thread.join()
    
    def register_callback(self, callback: Callable[[Dict], None]) -> None:
        self.callbacks.append(callback)
    
    def _notify_callbacks(self) -> None:
        """通知所有回调函数"""
        logger.info('正在执行回调')
        for callback in self.callbacks:
            try:
                callback(self.local_config)
            except Exception as e:
                logger.error(f"回调函数执行失败: {e}")
    
    def save_local_config(self, use_raw_content: bool = False) -> bool:
        """保存配置到本地 YAML 文件"""
        try:
            if use_raw_content and self.etcd_raw_content:
                # 使用原始字节内容写入
                with open(self.local_config_path, 'wb') as f:
                    f.write(self.etcd_raw_content)
            else:
                # 使用字典序列化写入
                with open(self.local_config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(self.local_config, f, default_flow_style=False, allow_unicode=True)
            
            logger.info(f"配置已保存到本地: {self.local_config_path}")
            return True
        except Exception as e:
            logger.error(f"保存本地配置失败: {e}")
            return False

    
    def _merge_configs(self, base: Dict, overlay: Dict) -> Dict:
        """合并两个配置字典，overlay 优先级更高"""
        result = base.copy()
        
        for key, value in overlay.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        获取配置项，支持点分路径
        
        Args:
            key_path: 配置项路径，如 "database.host" 或 "app.name"
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key_path.split('.')
        current = self.local_config
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        
        return current
    
    def get_all(self) -> Dict[str, Any]:
        """获取所有配置的副本"""
        return self.local_config.copy()
    
    def update_local_config(self, updates: Dict[str, Any]) -> bool:
        """
        更新本地配置并保存
        
        Args:
            updates: 要更新的配置字典
            
        Returns:
            是否成功
        """
        try:
            with self.lock:
                # 合并更新到当前配置
                self.local_config = self._merge_configs(self.local_config, updates)
                
                # 保存到本地文件
                if not self.save_local_config():
                    return False
                
                # 设置未同步标志
                self.sync_status["local_synced"] = False
                self.sync_status["last_sync_time"] = time.time()
                
                # 通知回调
                self._notify_callbacks()
                
            logger.info("本地配置更新成功")
            return True
            
        except Exception as e:
            logger.error(f"更新本地配置失败: {e}")
            return False
    
    def get_sync_status(self) -> Dict[str, Any]:
        """
        获取同步状态
        
        Returns:
            同步状态字典
        """
        with self.lock:
            return self.sync_status.copy()
    
    def get_etcd_raw_content(self) -> bytes:
        """获取 etcd 配置的原始内容"""
        return self.etcd_raw_content or b''
    
    def needs_restore(self) -> bool:
        """检查是否需要恢复本地配置"""
        return self.sync_status.get("needs_restore", False)