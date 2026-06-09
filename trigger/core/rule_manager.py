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
Rule Manager Module

规则文件管理器，负责从etcd同步lua规则文件到本地，并watch变化
"""

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



logger = logging.getLogger(__name__)


class RuleInfo(BaseModel):
    """规则信息模型"""
    rule_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="规则ID（UUID）")
    role_id: str = Field(..., description="所属角色ID（UUID）")
    name: str = Field(..., description="规则名称")
    lua_content: bytes = Field(..., description="Lua规则内容")
    created_at: datetime = Field(default_factory=datetime.now, description="创建时间")
    updated_at: datetime = Field(default_factory=datetime.now, description="更新时间")
    version: int = Field(1, description="版本号")
    metadata: Optional[Dict[str, Any]] = Field(None, description="规则元数据")


class LocalRuleInfo(BaseModel):
    """本地规则信息模型"""
    filename: str = Field(..., description="规则文件名")
    hash: str = Field(..., description="内容哈希")
    synced_from_etcd: bool = Field(False, description="是否从etcd同步")


class RuleManagerStatus(BaseModel):
    """规则管理器状态模型"""
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


class RuleManager:
    """
    规则文件管理器
    
    负责从etcd同步lua规则文件到本地，并watch变化
    """
    
    def __init__(
        self,
        etcd_client: EtcdClient,
        etcd_workdir: str,
        etcd_role_path: str,
        local_rules_dir: str,
        etcd_priority: bool = True,
    ):
        """
        初始化规则管理器
        
        Args:
            etcd_client: 增强版EtcdClient实例
            etcd_rules_path: etcd中的规则路径（完整路径）
            local_rules_dir: 本地规则文件存储目录
            etcd_priority: etcd是否优先（True: etcd修改覆盖本地；False: 本地修改有效）
        """
        # 参数
        self.etcd_client = etcd_client
        self.etcd_role_path = etcd_role_path
        self.local_rules_dir = Path(local_rules_dir)
        self.etcd_priority = etcd_priority
        self.etcd_workdir_prefix = etcd_workdir
        
        # 初始化本地文件夹
        self.local_rules = {}
        self._ensure_local_dir()
        self._load_local_rules()
        
        # 存储watch ID，便于后续管理
        self._etcd_role_watch_id = None
        self._etcd_role_info_watch_id = None
        self._file_watcher = None
        
        self.callbacks: List[Callable[[], None]] = []
        self.running = False
        self._sync_lock = threading.Lock()
        self._syncing = False
        
        # 拉取远端rules info
        self.role_info = self._fetch_role()
        logger.info(f'role_info: {self.role_info}')
        self.remote_rules: dict = self._fetch_rules_info(self.role_info.get("rule_ids", [])) if self.role_info else []
        logger.debug(f'remote_rules: {self.remote_rules}')
        
    def set_etcd_priority(self, priority:bool):
        self.etcd_priority = priority

    def get_local_rules_list(self) -> List[LocalRuleInfo]:
        return self.local_rules
    
    def get_local_rules_content(self, name:str) -> bytes:
        if not name.endswith('.lua'):
            name += '.lua'
        full_path = str(self.local_rules_dir / name)
        if not os.path.exists(full_path):
            logger.error(f'本地路径不存在: {full_path}')
            return b''

        content = b''
        with open(full_path, 'rb') as f:
            content = f.read()
            return content
    
    def update_local_rule(self, name:str, content:bytes):
        # etcd优先级高，禁止手动更新
        if self.etcd_priority:
            return False
        
        if not name.endswith('.lua'):
            name += '.lua'
        
        is_update = False
        full_path = str(self.local_rules_dir / name)
        if name not in self.local_rules.keys():
            new_info = LocalRuleInfo(
                filename=name,
                hash=self._calculate_hash(content),
                synced_from_etcd=False
            )
            
            self.local_rules[name] = new_info
        else:
            if not os.path.exists(full_path):
                logger.error(f'本地路径不存在: {full_path}')
                return False
            info = self.local_rules[name]
            info.hash = self._calculate_hash(content),
            info.synced_from_etcd=False
            is_update = True
            
        with open(full_path, 'wb') as f:
            f.write(content)
            logger.info(f'{"更新" if is_update else "新建"}路径: {full_path} 下规则')
        
        return True
    
    def delete_local_rule(self, name:str, content:bytes):
        # etcd优先级高，禁止手动更新
        if self.etcd_priority:
            return False
        
        if not name.endswith('.lua'):
            name += '.lua'
        
        full_path = str(self.local_rules_dir / name)
        if name not in self.local_rules.keys():
            logger.info()
            return False
        
        self.local_rules.pop(name)
        # 避免各种原因删除重要文件
        self.move2history(full_path)
        logger.info(f'删除路径: {full_path} 下规则')
        return True
        
    
    def _fetch_role(self):
        value, metadata = self.etcd_client.get(self.etcd_role_path)
        if not value:
            logger.warning(f"未找到角色ID: {self.etcd_role_path}")
            return None
        role_id = value.decode()
        role_info_path = self.etcd_workdir_prefix + '/roles/' + role_id + '/info'
        role_info_path = role_info_path.replace('//','/')
        print('role_info_path: ', role_info_path)
        value, metadata = self.etcd_client.get(role_info_path)
        if not value:
            logger.warning(f"未找到角色信息: {role_info_path}")
            return None
        return json.loads(value.decode())

    def _fetch_rules_info(self, rules_ids):
        rules_dict = {}
        for rule_id in rules_ids:
            rule_key = f"{self.etcd_workdir_prefix}/rules/{rule_id}".replace('//','/')
            value, metadata = self.etcd_client.get(rule_key)
            if value:
                rule_info = RuleInfo.model_validate_json(value.decode())
                rules_dict[rule_info.name] = rule_info
                logger.info(f"获取规则信息: {rule_info.name} (ID: {rule_info.rule_id})")
            else:
                logger.warning(f"未找到规则信息: {rule_id}")
        return rules_dict

    def _load_local_rules(self) -> None:
        """加载本地规则文件到内存"""
        try:
            for lua_file in self.local_rules_dir.glob('*.lua'):
                with open(lua_file, 'rb') as f:
                    content = f.read()
                    rule_info = LocalRuleInfo(
                        filename=lua_file.name.replace('.lua',''), # 只存名字
                        hash=self._calculate_hash(content),
                        synced_from_etcd=False
                    )
                    self.local_rules[rule_info.filename] = rule_info
                    logger.info(f"加载本地规则文件: {lua_file.name}")
        except Exception as e:
            logger.error(f"加载本地规则文件失败: {e}")

    def _ensure_local_dir(self) -> None:
        """确保本地规则目录存在"""
        try:
            self.local_rules_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"规则目录: {self.local_rules_dir}")
        except Exception as e:
            logger.error(f"创建规则目录失败: {e}")
    
    def _calculate_hash(self, content: bytes) -> str:
        """计算内容哈希"""
        return hashlib.md5(content).hexdigest()
    
    def sync_rule_to_file(self, filename: str) -> bool:
        """
        同步单个规则文件
        
        Args:
            filename: 规则文件名
            content: 规则文件内容
        
        Returns:
            是否成功
        """
        try:
            name = filename.replace('.lua','')
            local_rule:LocalRuleInfo = self.local_rules.get(name,None)
            if not local_rule:
                logger.info(f'没有这个文件{filename}，新建')
                local_rule = LocalRuleInfo(
                    filename=name,
                    hash='',
                    synced_from_etcd=False
                )
            
            
            remote_rule:RuleInfo = self.remote_rules.get(name, None)
            if not remote_rule:
                logger.info('etcd中没有这条规则，跳过')
                return False
        
            content_hash = local_rule.hash
            remote_content_hash = self._calculate_hash(remote_rule.lua_content)
            if  remote_content_hash == content_hash:
                logger.debug(f"文件未变化，跳过同步: {filename}")
                return False  # 文件未变化
            
            # 保存到本地
            local_path = self.local_rules_dir / (filename+'.lua')
            with open(local_path, 'wb') as f:
                f.write(remote_rule.lua_content)
                local_rule.hash = remote_content_hash
                local_rule.synced_from_etcd = True
            
            # 更新缓存
            self.local_rules[filename] = local_rule
            
            logger.info(f"同步规则文件: {filename} -> {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"同步规则文件失败 {filename}: {e}")
            return False
    
    def sync_rule_from_etcd(self, rule_id: str) -> bool:
        """
        从etcd同步指定规则id到内存
        
        Args:
            rule_id: 规则id
        
        Returns:
            是否成功
        """
        try:
            
            # 构建etcd key
            key = f"{self.etcd_workdir_prefix}/rules/{rule_id}".replace('//','/')
            
            # 从etcd获取值
            value, metadata = self.etcd_client.get(key)
            if value is None:
                logger.warning(f"etcd中不存在规则文件: {key}")
                return False
            
            rule_info = RuleInfo.model_validate_json(value.decode())
            self.remote_rules[rule_info.name] = rule_info

        except Exception as e:
            logger.error(f"从etcd同步规则失败 {rule_id}: {e}")
            return False
    
    def sync_all_from_etcd_to_file(self) -> bool:
        ret = True
        for name, rule_info in self.remote_rules.items():
            ret &= self.sync_rule_from_etcd(rule_info.rule_id)
            ret &= self.sync_rule_to_file(name)
            
        return ret
    
    # 注释掉未使用的函数
    # def register_callback(self, callback: Callable[[], None]) -> None:
    #     """注册规则变更回调"""
    #     self.callbacks.append(callback)
    #     logger.debug(f"已注册规则回调函数，当前总数: {len(self.callbacks)}")
    #
    # def unregister_callback(self, callback: Callable[[], None]) -> bool:
    #     """注销规则变更回调"""
    #     try:
    #         self.callbacks.remove(callback)
    #         logger.debug(f"已注销规则回调函数，剩余总数: {len(self.callbacks)}")
    #         return True
    #     except ValueError:
    #         logger.warning(f"未找到要注销的回调函数")
    #         return False
    
    def _notify_callbacks(self, reason: str = "规则变更") -> None:
        """通知回调函数"""
        if not self.callbacks:
            logger.debug(f"没有注册的回调函数，跳过通知")
            return
        
        logger.info(f"{reason}，通知 {len(self.callbacks)} 个回调函数")
        for callback in self.callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"规则回调执行失败: {e}")
    
    def start(self) -> None:
        """启动规则管理器"""
        if self.running:
            logger.warning("RuleManager已启动或未启用，跳过")
            return
        
        logger.info("正在启动RuleManager...")
        self.running = True
        
        # 首次全量同步
        self._sync_all_rules_from_etcd()
        self._sync_all_rules_to_file()
        
        # 启动etcd watch
        self._start_etcd_watch()
        
        # 始终启动本地文件监控（根据优先级设置恢复逻辑）
        self._start_file_watcher()
        
        logger.info("RuleManager已启动")

    
    @staticmethod
    def safe_remove(file_path):
        path = Path(file_path)
        try:
            if path.is_file():
                path.unlink()
                logger.debug(f"已删除: {path}")
            else:
                logger.info(f"文件不存在: {path}")
        except PermissionError:
            logger.error(f"权限不足，无法删除: {path}")
        except Exception as e:
            logger.error(f"删除失败: {e}")
    
    @staticmethod
    def move2history(file_path):
        path = Path(file_path)
    
        # 检查源文件
        if not path.exists():
            raise FileNotFoundError(f"源文件不存在: {file_path}")
        if not path.is_file():
            raise ValueError(f"路径必须是文件: {file_path}")
        
        # 构建目标目录 (.history)
        parent = path.parent
        history_dir = parent / ".history"
        history_dir.mkdir(exist_ok=True)  # 如果不存在则创建
        
        # 构建目标路径
        target = history_dir / path.name
        
        # 处理重名：添加时间戳
        if target.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"{path.stem}_{timestamp}{path.suffix}"
            target = history_dir / new_name
        
        # 移动文件（保留元数据）
        shutil.move(str(path), str(target))
        
        return target
        
    
    def _fetch_role_info_path(self):
        role_id = self.role_info['role_id']
        role_info_path = f'{self.etcd_workdir_prefix}/roles/{role_id}/info'.replace('//','/')
        return role_info_path
    
    def _sync_all_rules_to_file(self):
        """同步所有规则到本地文件（后台线程执行）"""
        if self._syncing:
            logger.debug("同步操作正在进行中，跳过")
            return
        
        def sync_impl():
            with self._sync_lock:
                self._syncing = True
                try:
                    to_delete_files = []
                    to_update_files = []
                    
                    local_names = self.local_rules.keys()
                    remote_names = self.remote_rules.keys()
                    for remote_name, remote_info in self.remote_rules.items():
                        if remote_name not in local_names:
                            to_update_files.append(remote_name)
                            continue
                        else:
                            if self._calculate_hash(remote_info.lua_content) != self.local_rules[remote_name].hash:
                                to_update_files.append(remote_name)
                    
                    for local_name in local_names:
                        if local_name not in remote_names:
                            to_delete_files.append(local_name)
                    
                    for file in to_delete_files:
                        self.local_rules.pop(file)
                        if not file.endswith('.lua'):
                            file += '.lua'
                        full_path = str(self.local_rules_dir / file)
                        self.safe_remove(full_path)
                    
                    for file in to_update_files:
                        remote_info = self.remote_rules[file]
                        self.local_rules.update({
                            file:
                                LocalRuleInfo(
                                    filename=file,
                                    hash=self._calculate_hash(remote_info.lua_content),
                                    synced_from_etcd=True
                                )
                        })
                        
                        content = remote_info.lua_content
                        if not file.endswith('.lua'):
                            file += '.lua'
                        full_path = str(self.local_rules_dir / file)
                        with open(full_path, 'wb') as f:
                            f.write(content)
                    
                    logger.info("后台线程完成规则文件同步")
                except Exception as e:
                    logger.error(f"后台线程同步规则文件失败: {e}")
                finally:
                    self._syncing = False
        
        # 启动后台线程执行同步
        thread = threading.Thread(target=sync_impl, daemon=True)
        thread.start()
        logger.debug("已启动后台线程执行规则文件同步")

    def _sync_all_rules_from_etcd(self):
        new_rule_ids = self.role_info['rule_ids']
        name_to_delete = []
        for name, info in self.remote_rules.items():
            if info.rule_id not in new_rule_ids:
                name_to_delete.append(name)
        for name in name_to_delete:
            self.remote_rules.pop(name)
        for id in new_rule_ids:
            self.sync_rule_from_etcd(id)
        
    def _handle_etcd_role_event(self, event: Any) -> None:
        # 更新role信息
        # 处理键被删除的情况（如服务注销时），此时 event.value 可能为 None 或空
        if not event.value:
            logger.debug("etcd role 事件值为空，可能是键被删除，忽略")
            return
        
        new_role_id = event.value.decode()
        if not new_role_id:
            logger.debug("etcd role ID为空，忽略")
            return
            
        if new_role_id != self.role_info['role_id']:
            self.etcd_client.stop_watch(self._etcd_role_info_watch_id)
            role_info_path = f'{self.etcd_workdir_prefix}/roles/{new_role_id}/info'.replace('//','/')
            value, metadata = self.etcd_client.get(role_info_path)
            if not value:
                logger.warning(f"未找到角色信息: {role_info_path}")
                raise Exception(f"未找到角色信息: {role_info_path}")
            self.role_info = json.loads(value.decode())
            # 手动更新规则
            self._sync_all_rules_from_etcd()
            if self.etcd_priority:
                self._sync_all_rules_to_file()
            # 重新启用watch
            self._start_etcd_role_info_watch()
    
    def _handle_etcd_role_info_event(self, event: Any) -> None:
        if not event.value:
            logger.debug("etcd role_info 事件值为空，可能是键被删除，忽略")
            return
        self.role_info = json.loads(event.value.decode())
        self._sync_all_rules_from_etcd()
        if self.etcd_priority:
            self._sync_all_rules_to_file()
    
    def _start_etcd_role_info_watch(self, event: Any) -> None:
        # watch role_info
        etcd_role_info_path = self._fetch_role_info_path()
        self._etcd_role_info_watch_id = self.etcd_client.watch(
            key=etcd_role_info_path,
            callback=self._handle_etcd_role_info_event,
            watch_id=f"rule_manager_{etcd_role_info_path}"
        )
        
        if self._etcd_role_watch_id:
            logger.info(f"已重新启动etcd role info watch，ID: {self._etcd_role_watch_id}")
        else:
            logger.error("重新启动etcd role info watch失败")
    
    def _start_etcd_watch(self) -> None:
        """
        启动etcd watch，使用增强版EtcdClient的回调功能
        """
        if not self.etcd_client:
            logger.error("无法启动watch：etcd客户端或规则路径未设置")
            return
        
        logger.info(f"启动etcd角色watch: {self.etcd_role_path}")
        try:
            # watch role 和 role_info ，这样实现更加灵活
            self._etcd_role_watch_id = self.etcd_client.watch(
                key=self.etcd_role_path,
                callback=self._handle_etcd_role_event,
                watch_id=f"rule_manager_{self.etcd_role_path}"
            )
            
            if self._etcd_role_watch_id:
                logger.info(f"已启动etcd role watch，ID: {self._etcd_role_watch_id}")
            else:
                logger.error("启动etcd role watch失败")
                raise Exception("启动etcd role watch失败")
                
            
            # watch role_info
            etcd_role_info_path = self._fetch_role_info_path()
            self._etcd_role_info_watch_id = self.etcd_client.watch(
                key=etcd_role_info_path,
                callback=self._handle_etcd_role_info_event,
                watch_id=f"rule_manager_{etcd_role_info_path}"
            )
            
            if self._etcd_role_watch_id:
                logger.info(f"已启动etcd role info watch，ID: {self._etcd_role_watch_id}")
            else:
                logger.error("启动etcd role info watch失败")
                raise Exception("启动etcd role info watch失败")

        except Exception as e:
            logger.error(f"启动etcd watch失败: {e}")
            raise Exception(f"启动etcd watch失败: {e}")
    
    def _start_file_watcher(self) -> None:
        """
        启动本地文件监控
        
        根据 etcd_priority 决定是否恢复本地修改
        """
        if not self.local_rules_dir.exists():
            logger.warning(f"本地规则目录不存在: {self.local_rules_dir}")
            return
        
        class RuleFileHandler(FileSystemEventHandler):
            def __init__(self, manager):
                self.manager = manager
            
            def on_modified(self, event):
                if event.is_directory or not event.src_path.endswith('.lua'):
                    return
                
                filename = os.path.basename(event.src_path)
                logger.info(f"检测到本地规则文件修改: {filename}")
                
                if self.manager.etcd_priority:
                    # etcd优先模式：恢复本地修改
                    self.manager._restore_file_from_etcd(filename)
                else:
                    # 本地优先模式：保留修改，更新LocalRuleInfo
                    self.manager._update_local_rule_info(filename, 'modified')
            
            def on_deleted(self, event):
                if event.is_directory or not event.src_path.endswith('.lua'):
                    return
                
                filename = os.path.basename(event.src_path)
                logger.info(f"检测到本地规则文件删除: {filename}")
                
                if self.manager.etcd_priority:
                    # etcd优先模式：从etcd重新下载文件
                    self.manager._restore_file_from_etcd(filename)
                else:
                    # 本地优先模式：移除LocalRuleInfo记录
                    self.manager._update_local_rule_info(filename, 'deleted')
            
            def on_created(self, event):
                if event.is_directory or not event.src_path.endswith('.lua'):
                    return
                
                filename = os.path.basename(event.src_path)
                logger.info(f"检测到本地规则文件创建: {filename}")
                
                if self.manager.etcd_priority:
                    # etcd优先模式：从etcd恢复文件（如果存在）
                    self.manager._restore_file_from_etcd(filename)
                else:
                    # 本地优先模式：创建新的LocalRuleInfo记录
                    self.manager._update_local_rule_info(filename, 'created')
        
        try:
            event_handler = RuleFileHandler(self)
            self._file_watcher = Observer()
            self._file_watcher.schedule(
                event_handler,
                path=str(self.local_rules_dir),
                recursive=False
            )
            self._file_watcher.start()
            logger.info(f"本地规则文件监控已启动: {self.local_rules_dir}")
        except Exception as e:
            logger.error(f"启动文件监控失败: {e}")
    
    def get_local_rule_path(self, filename: str) -> Optional[Path]:
        """获取本地规则文件路径"""
        path = self.local_rules_dir / filename
        return path if path.exists() else None
    
    def _restore_file_from_etcd(self, filename: str) -> None:
        """从etcd恢复文件"""
        name = filename.replace('.lua', '')
        remote_rule = self.remote_rules.get(name)
        
        if not remote_rule:
            logger.warning(f"etcd中没有找到规则: {filename}")
            return

        local_rule = self.local_rules.get(name)
        local_path = self.local_rules_dir / filename
        if os.path.exists(local_path):
            hash = None
            with open(local_path, 'rb') as f:
                hash= self._calculate_hash(f.read())
            if hash == self._calculate_hash(remote_rule.lua_content):
                return
        try:
            with open(local_path, 'wb') as f:
                f.write(remote_rule.lua_content)
            
            # 更新LocalRuleInfo
            self.local_rules[name] = LocalRuleInfo(
                filename=name,
                hash=self._calculate_hash(remote_rule.lua_content),
                synced_from_etcd=True
            )
            
            logger.info(f"已从etcd恢复文件: {filename}")
        except Exception as e:
            logger.error(f"恢复文件失败 {filename}: {e}")
    
    def _update_local_rule_info(self, filename: str, action: str) -> None:
        """根据修改类型更新或新建LocalRuleInfo"""
        name = filename.replace('.lua', '')
        local_path = self.local_rules_dir / filename
        
        if action == 'deleted':
            # 删除文件：移除LocalRuleInfo记录
            if name in self.local_rules:
                del self.local_rules[name]
                logger.info(f"已移除LocalRuleInfo记录: {filename}")
        elif action == 'created':
            # 新建文件：创建新的LocalRuleInfo记录
            if local_path.exists():
                try:
                    with open(local_path, 'rb') as f:
                        content = f.read()
                    
                    self.local_rules[name] = LocalRuleInfo(
                        filename=name,
                        hash=self._calculate_hash(content),
                        synced_from_etcd=False
                    )
                    logger.info(f"已创建LocalRuleInfo记录: {filename}")
                except Exception as e:
                    logger.error(f"创建LocalRuleInfo记录失败 {filename}: {e}")
        elif action == 'modified':
            # 修改文件：更新现有记录的hash
            if name in self.local_rules and local_path.exists():
                try:
                    with open(local_path, 'rb') as f:
                        content = f.read()
                    
                    self.local_rules[name].hash = self._calculate_hash(content)
                    self.local_rules[name].synced_from_etcd = False
                    logger.info(f"已更新LocalRuleInfo记录: {filename}")
                except Exception as e:
                    logger.error(f"更新LocalRuleInfo记录失败 {filename}: {e}")
        
    def stop(self) -> None:
        """停止规则管理器"""
        if not self.running:
            logger.warning("RuleManager未运行，无需停止")
            return
        
        logger.info("正在停止RuleManager...")
        self.running = False
        
        # 停止etcd watch
        try:
            self.etcd_client.stop_watch(self._etcd_role_watch_id)
            logger.debug(f"已停止etcd watch: {self._etcd_role_watch_id}")
        except Exception as e:
            logger.error(f"停止etcd watch失败 {self._etcd_role_watch_id}: {e}")
            
        try:
            self.etcd_client.stop_watch(self._etcd_role_info_watch_id)
            logger.debug(f"已停止etcd watch: {self._etcd_role_info_watch_id}")
        except Exception as e:
            logger.error(f"停止etcd watch失败 {self._etcd_role_info_watch_id}: {e}")
                
        # 停止文件监控
        if self._file_watcher:
            try:
                self._file_watcher.stop()
                self._file_watcher.join(timeout=3)
                logger.info("本地文件监控已停止")
            except Exception as e:
                logger.error(f"停止文件监控失败: {e}")
            finally:
                self._file_watcher = None
        
        logger.info("RuleManager已停止")
    
    def get_status(self) -> RuleManagerStatus:
        """获取管理器状态"""
        # 获取角色ID
        role_id = self.role_info.get('role_id', 'unknown') if self.role_info else 'unknown'
        
        return RuleManagerStatus(
            running=self.running,
            etcd_priority=self.etcd_priority,
            etcd_role_path=self.etcd_role_path,
            etcd_workdir_prefix=self.etcd_workdir_prefix,
            local_rules_dir=str(self.local_rules_dir),
            role_id=role_id,
            local_rules_count=len(self.local_rules),
            remote_rules_count=len(self.remote_rules),
            callbacks_count=len(self.callbacks),
            etcd_role_watch_id=self._etcd_role_watch_id,
            etcd_role_info_watch_id=self._etcd_role_info_watch_id,
            file_watcher_running=self._file_watcher is not None,
            syncing=self._syncing,
            sync_lock_locked=self._sync_lock.locked() if hasattr(self, '_sync_lock') else False
        )
    
    def __enter__(self):
        """上下文管理器入口"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.stop()
    
    def __del__(self):
        """析构函数"""
        try:
            self.stop()
        except:
            pass


# 使用示例
def example_usage():
    """使用示例"""
    
    # 导入增强版EtcdClient
    from etcd_client import EtcdClient
    
    # 创建etcd客户端
    etcd_client = EtcdClient(host="your-etcd-host", port=2379)
    
    # 创建规则管理器
    rule_manager = RuleManager(
        etcd_client=etcd_client,
        etcd_workdir='/joblens/config',
        etcd_role_path='/joblens/test/test_role',
        local_rules_dir='/tmp/rule_test/',
        etcd_priority=True
    )
        
    # 启动规则管理器
    rule_manager.start()
    
    try:
        # 获取状态
        status = rule_manager.get_status()
        pprint(f"规则管理器状态: {status}")
        
        # # 获取所有规则状态
        # rules_status = rule_manager.get_all_rule_status()
        # print(f"规则状态: {rules_status}")
        
        # 手动同步某个规则
        # rule_manager.sync_rule_from_etcd("test_rule.lua")
        
        # 运行一段时间...
        import time
        while True:
            time.sleep(10)
            pass
        
    finally:
        # 停止规则管理器
        rule_manager.stop()


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.DEBUG)
    
    # 运行示例
    example_usage()