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
JobLens Trigger Application Factory

应用工厂，负责初始化所有组件并创建Flask应用
"""

import os
import socket
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from flask import Flask
import yaml

from trigger.core.config_manager import ConfigManager
from trigger.core.service_registrar import ServiceRegistrar
from trigger.core.rule_manager import RuleManager
from trigger.core.etcd_client import EtcdClient
from trigger.core.rpc_client import RPCClient
from trigger.api.routes import register_routes
from trigger.utils.email_notifier import simple_send
from trigger.core.tools import restart_joblens

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AppContext:
    """
    应用上下文，管理所有组件生命周期
    
    初始化流程：
    1. load_configuration(): 加载基础配置
    2. initialize_core_components(): 初始化核心组件
    3. initialize_optional_components(): 初始化可选组件
    4. setup_component_dependencies(): 建立组件依赖
    5. create_app(): 创建Flask应用并注册路由
    """
    
    def __init__(self, config_path: str = None):
        """
        初始化应用上下文
        
        Args:
            config_path: 配置文件路径，如果为None则使用默认路径
        """
        self.config_path = config_path or self._get_default_config_path()
        self.config = {}
        
        # 核心组件（必须初始化）
        self.rpc_client: Optional[RPCClient] = None
        
        # 可选组件（根据配置决定是否初始化）
        self.service_registrar: Optional[ServiceRegistrar] = None
        self.etcd_client: Optional[EtcdClient] = None
        self.config_manager: Optional[ConfigManager] = None
        self.rule_manager: Optional[RuleManager] = None
        
        # 应用
        self.flask_app: Optional[Flask] = None
        
        # 状态标记
        self._components_initialized = False
        self._shutdown_called = False
    
    def _get_default_config_path(self) -> str:
        """获取默认配置文件路径
        
        查找顺序:
        /etc/JobLens/trigger/config.yaml（生产配置）

        """
        system_config = Path("/etc/JobLens/trigger/config.yaml")
        if system_config.exists():
            return str(system_config)

        raise FileNotFoundError("Cannot find configuration file. Please provide a config.yaml in /etc/JobLens/trigger/ or use the example config.")
    
    def _load_yaml_config(self) -> dict:
        """从YAML文件加载配置"""
        config = {}
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                logger.info(f"加载配置文件: {self.config_path}")
            except Exception as e:
                logger.warning(f"加载配置文件失败: {e}")
        else:
            logger.warning(f"配置文件不存在: {self.config_path}")
        return config
    
    def _get_config_value(self, path: str, default: Any = None) -> Any:
        """
        安全的获取配置值，支持嵌套路径
        
        Args:
            path: 配置路径，如 'service.port'
            default: 默认值
        """
        keys = path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def load_configuration(self) -> None:
        """
        阶段1：加载配置
        
        加载所有配置，设置默认值，不初始化任何组件
        """
        logger.info("阶段1: 加载配置...")
        
        # 加载基础配置
        self.config = self._load_yaml_config()
        
        # 设置服务配置
        self.config.setdefault('service', {})
        self.config['service']['port'] = int(os.environ.get(
            'JOBLENS_PORT', 
            self._get_config_value('service.port', 7592)
        ))
        self.config['service']['host'] = os.environ.get(
            'JOBLENS_HOST',
            self._get_config_value('service.host', '0.0.0.0')
        )
        
        # 设置服务注册配置
        self.config.setdefault('service_registry', {})
        self.config['service_registry']['enabled'] = self._get_config_value(
            'service_registry.enabled', True
        )
        self.config['service_registry']['url'] = os.environ.get(
            'REGISTRY_URL',
            self._get_config_value('service_registry.url', 'http://localhost:8080')
        )
        self.config['service_registry']['host'] = socket.gethostname()
        self.config['service_registry']['port'] = self.config['service']['port']
        self.config['service_registry']['retry_interval'] = int(
            self._get_config_value('service_registry.retry_interval', 10)
        )
        self.config['service_registry']['max_retries'] = int(
            self._get_config_value('service_registry.max_retries', 3)
        )
        self.config['service_registry']['heartbeat_interval'] = int(
            self._get_config_value('service_registry.heartbeat_interval', 1800)
        )
        
        # 加载JobLens配置用于获取其他配置项
        joblens_config = self._load_joblens_config()
        
        # RPC配置
        self.config.setdefault('lens_config', {})
        self.config['lens_config']['rpc_socket_path'] = joblens_config.get(
            'lens_config', {}
        ).get('rpc_socket_path', '/var/JobLens/rpc.sock')
        self.config['lens_config']['rpc_timeout'] = float(
            self._get_config_value('lens_config.rpc_timeout', 5.0)
        )
        
        # 规则管理器配置
        self.config.setdefault('rule_manager', {})
        self.config['rule_manager']['enabled'] = self._get_config_value(
            'rule_manager.enabled', True
        )
        self.config['rule_manager']['local_rules_dir'] = joblens_config.get(
            'job_registry_config', {}
        ).get('rules_path', str(Path(__file__).resolve().parent.parent.absolute()/'config'/'rules'))
        self.config['rule_manager']['etcd_priority'] = self._get_config_value(
            'rule_manager.etcd_priority', True
        )
        
        # 配置管理器设置
        self.config.setdefault('config_manager', {})
        self.config['config_manager']['enabled'] = self._get_config_value(
            'config_manager.enabled', True
        )
        self.config['config_manager']['config_file'] = self._get_config_value(
            'config_manager.config_file', '/etc/JobLens/config.yaml'
        )
        self.config['config_manager']['etcd_priority'] = self._get_config_value(
            'config_manager.etcd_priority', True
        )
        
        logger.info("阶段1完成\n")
    
    def _load_joblens_config(self) -> dict:
        """加载JobLens配置"""
        config_file = self._get_config_value('config_manager.config_file', 'config/config.yaml')
        abs_path = str(Path(__file__).resolve().parent.parent.absolute() / Path(config_file))
        
        try:
            with open(abs_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"加载JobLens配置失败: {e}")
            return {}
    
    def initialize_core_components(self) -> None:
        """
        阶段2：初始化核心组件
        
        初始化必须的组件，如RPC客户端
        """
        logger.info("阶段2: 初始化核心组件...")
        
        # 初始化RPC客户端（连接失败不影响启动）
        self._initialize_rpc_client()
        
        logger.info("阶段2完成\n")
    
    def _initialize_rpc_client(self) -> None:
        """初始化RPC客户端"""
        try:
            rpc_config = self.config.get('lens_config', {})            
            from trigger.core.rpc_client import RPCClient
            self.rpc_client = RPCClient(
                socket_path=rpc_config['rpc_socket_path'],
                timeout=rpc_config['rpc_timeout']
            )
        
            # 测试连接
            self.rpc_client.connect()
            self.rpc_client.close()
            logger.info("✓ RPCClient初始化成功")
        except Exception as e:
            logger.warning(f"RPCClient初始化失败，部分功能不可用（不影响Trigger启动）: {e}")
            self.rpc_client = None
    
    def initialize_optional_components(self) -> None:
        """
        阶段3：初始化可选组件
        
        根据配置初始化可选组件，各组件独立容错，任一失败不影响整体启动
        """
        logger.info("阶段3: 初始化可选组件...")
        
        # 初始化服务注册器（注册失败不影响启动）
        if self._get_config_value('service_registry.enabled'):
            self._initialize_service_registrar()
        
        # 初始化 Etcd 客户端（连接失败后 ConfigManager/RuleManager 降级为本地模式）
        self._initialize_etcd_client()
        
        # 初始化配置管理器
        if self._get_config_value('config_manager.enabled'):
            self._initialize_config_manager()
        
        # 初始化规则管理器
        if self._get_config_value('rule_manager.enabled'):
            self._initialize_rule_manager()
        
        self._components_initialized = True
        logger.info("阶段3完成\n")
    
    def _initialize_service_registrar(self) -> None:
        """初始化服务注册器"""
        try:
            registrar_config = self.config['service_registry']
            self.service_registrar = ServiceRegistrar(
                registry_url=registrar_config['url'],
                service_host=registrar_config['host'],
                service_port=registrar_config['port'],
                retry_interval=registrar_config['retry_interval'],
                max_retries=registrar_config['max_retries'],
                heartbeat_interval=registrar_config['heartbeat_interval']
            )
            
            # 尝试注册服务
            if self.service_registrar.register():
                logger.info("✓ 服务注册成功")
                logger.info(self.service_registrar.get_status())
            else:
                logger.warning("⚠ 服务注册失败，将使用本地模式")
        except Exception as e:
            logger.warning(f"⚠ ServiceRegistrar初始化失败: {e}")
            self.service_registrar = None
    
    def _initialize_etcd_client(self) -> None:
        """初始化Etcd客户端"""
        if not self.service_registrar:
            return
        try:
            status = self.service_registrar.get_status()
            self.etcd_client = EtcdClient(
                host=status['etcd_addr'],
                port=status['etcd_port']
            )
            logger.debug(f"EtcdClient连接信息: {status['etcd_addr']}:{status['etcd_port']}")
            logger.info("✓ EtcdClient初始化成功")
        except Exception as e:
            logger.warning(f"⚠ EtcdClient初始化失败，后续组件将使用本地模式: {e}")
            self.etcd_client = None
    
    def _initialize_config_manager(self) -> None:
        """初始化配置管理器"""
        try:
            def config_callback(new_config):
                logger.info('配置变更回调触发')
                restart_joblens()
            config = self.config['config_manager']
            
            # 根据是否有etcd_client决定使用远程还是本地配置
            if self.etcd_client:
                status = self.service_registrar.get_status()
                self.config_manager = ConfigManager(
                    etcd_client=self.etcd_client,
                    etcd_workdir=status['etcd_workdir'],
                    etcd_mode_path=status['etcd_path'] + '/mode',
                    local_config_path=config['config_file'],
                    etcd_priority=config['etcd_priority'],
                    config_change_callback=config_callback
                )
                self.config_manager.start()
                logger.info("✓ ConfigManager初始化成功（etcd模式）")
            else:
                # 跳过初始化，使用集群puppet进行配置管理
                logger.info("✓ ConfigManager初始化跳过（puppet模式）")
                
        except Exception as e:
            logger.warning(f"⚠ ConfigManager初始化失败: {e}")
            self.config_manager = None
    
    def _initialize_rule_manager(self) -> None:
        """初始化规则管理器"""
        try:
            config = self.config['rule_manager']
            
            # 根据是否启用远程配置决定初始化方式
            if self.etcd_client and self.config_manager:
                status = self.service_registrar.get_status()
                self.rule_manager = RuleManager(
                    etcd_client=self.etcd_client,
                    etcd_workdir=status['etcd_workdir'],
                    etcd_role_path=status['etcd_path'] + '/role',
                    local_rules_dir=config['local_rules_dir'],
                    etcd_priority=config['etcd_priority']
                )
                logger.info("✓ RuleManager初始化成功（远程模式）")
                self.rule_manager.start()
            else:
                logger.info("⚠ etcd不可用，跳过RuleManager初始化")
                
        except Exception as e:
            logger.warning(f"⚠ RuleManager初始化失败: {e}")
            self.rule_manager = None
    
        
        logger.info("阶段4完成\n")
    
    def create_app(self) -> Flask:
        """
        阶段5：创建Flask应用
        
        创建Flask应用并注册路由
        """
        logger.info("阶段5: 创建Flask应用...")
        
        self.flask_app = Flask(__name__)
        
        # 注册路由，注入依赖
        register_routes(
            app=self.flask_app,
            rpc_client=self.rpc_client,
            config_manager=self.config_manager,
            service_registrar=self.service_registrar,
            rule_manager=self.rule_manager
        )
        
        logger.info("✓ 路由注册完成")
        
        # 将应用上下文附加到app，使用自定义属性名避免覆盖Flask的app_context方法
        self.flask_app._app_context = self
        
        logger.info("阶段5完成")
        logger.info("应用初始化完成！")
        
        return self.flask_app
    
    def initialize_all(self) -> Flask:
        """
        完整初始化流程
        
        Returns:
            初始化完成的Flask应用
        """
        self.load_configuration()
        self.initialize_core_components()
        self.initialize_optional_components()
        return self.create_app()
    
    def shutdown(self):
        """
        优雅关闭
        
        线程安全，可重复调用
        """
        if self._shutdown_called:
            logger.warning("shutdown()已被调用，忽略重复调用")
            return
        
        logger.info("正在关闭应用...")
        
        self._shutdown_called = True
        
        # 定义关闭顺序
        shutdown_order = [
            ('ServiceRegistrar', self.service_registrar),
            ('ConfigManager', self.config_manager),
            ('RuleManager', self.rule_manager),
            ('RPCClient', self.rpc_client),
        ]
        
        for component_name, component in shutdown_order:
            if component:
                try:
                    logger.info(f"正在停止{component_name}...")
                    if hasattr(component, 'shutdown'):
                        component.shutdown()
                    elif hasattr(component, 'stop'):
                        component.stop()
                    elif hasattr(component, 'close'):
                        component.close()
                    logger.info(f"✓ {component_name}已停止")
                except Exception as e:
                    logger.error(f"停止{component_name}时发生错误: {e}")
        
        logger.info("应用关闭完成")


def create_application(config_path: str = None) -> Flask:
    """
    工厂函数，创建应用实例
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        Flask应用实例
    """
    # 创建应用上下文并执行完整初始化
    app_context = AppContext(config_path)
    return app_context.initialize_all()