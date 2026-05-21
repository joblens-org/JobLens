#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
# -*- coding: utf-8 -*-
"""
JobLens CLI v2.0 - 支持注册中心模式
支持直接连接 JobLens 服务或通过注册中心管理多个服务
"""
VER = '0.0.7'

import cmd
import requests
import json
import os
import sys
from enum import Enum
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin
import logging
import readline

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False
    print("提示: 安装 'tabulate' 库可获得更好的表格显示效果: pip install tabulate")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("JobLensCLI")

# ==================== 配置与颜色 ====================

class Colors:
    """终端颜色"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
    @staticmethod
    def disable():
        """禁用颜色输出"""
        Colors.HEADER = ''
        Colors.OKBLUE = ''
        Colors.OKCYAN = ''
        Colors.OKGREEN = ''
        Colors.WARNING = ''
        Colors.FAIL = ''
        Colors.ENDC = ''
        Colors.BOLD = ''
        Colors.UNDERLINE = ''

class Config:
    """配置管理"""
    DEFAULT_SERVICE_HOST = "localhost"
    DEFAULT_SERVICE_PORT = 7592
    DEFAULT_REGISTRY_HOST = "localhost"
    DEFAULT_REGISTRY_PORT = 8080

# ==================== 连接模式 ====================

class ConnectionMode(Enum):
    """连接模式枚举"""
    DIRECT = "direct"          # 直接连接 JobLens 服务
    REGISTRY = "registry"      # 通过注册中心管理

class ConnectionInfo:
    """连接信息"""
    def __init__(self):
        self.mode: ConnectionMode = ConnectionMode.DIRECT
        self.service_host: str = Config.DEFAULT_SERVICE_HOST
        self.service_port: int = Config.DEFAULT_SERVICE_PORT
        self.registry_host: Optional[str] = None
        self.registry_port: Optional[int] = None
        self.current_service_id: Optional[str] = None
        self.current_service_name: Optional[str] = None
    
    def get_service_base_url(self) -> str:
        """获取当前连接的 JobLens 服务 Base URL"""
        return f"http://{self.service_host}:{self.service_port}"
    
    def get_registry_base_url(self) -> Optional[str]:
        """获取注册中心 Base URL"""
        if self.registry_host and self.registry_port:
            return f"http://{self.registry_host}:{self.registry_port}"
        return None
    
    def is_connected(self) -> bool:
        """检查是否已连接（直接模式连服务，注册中心模式连注册中心）"""
        return (self.mode == ConnectionMode.DIRECT and bool(self.service_host)) or \
               (self.mode == ConnectionMode.REGISTRY and bool(self.registry_host))
    
    def get_prompt_context(self) -> str:
        """生成提示符上下文信息"""
        if self.mode == ConnectionMode.DIRECT:
            return f"(Direct: {self.service_host}:{self.service_port})"
        else:  # REGISTRY mode
            if self.current_service_id:
                service_label = self.current_service_name or f"svc-{self.current_service_id[:8]}"
                return f"(Registry: {self.registry_host}:{self.registry_port} -> {service_label})"
            else:
                return f"(Registry: {self.registry_host}:{self.registry_port} [No Service])"

# ==================== JobLens CLI Shell ====================

class JobLensShell(cmd.Cmd):
    """JobLens 交互式命令行工具"""
    
    intro = f"""
{Colors.HEADER}{Colors.BOLD}
╔═════════════════════════════════════════════════════════════════╗
║                   JobLens CLI Tool v{VER}                       ║
║                                                                 ║
║                                                                 ║
║  Quick Start:                                                   ║
║    • connect <host> <port>  - Connect to JobLens of a node      ║
║    • registry <host> <port> - Connect to the registry center    ║
║    • help                   - View detailed commands            ║
╚═════════════════════════════════════════════════════════════════╝
{Colors.ENDC}
"""
    
    # 初始化
    def __init__(self, default_host: str = None, default_port: int = None):
        super().__init__()
        self.conn_info = ConnectionInfo()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': f'JobLens-CLI/{VER}',
            'Content-Type': 'application/json'
        })
        
        # 层级管理：0层为基础层，连接服务或注册中心会增加层级
        self.level = 0
        
        # 如果提供默认参数，自动连接
        if default_host or default_port:
            if default_host:
                self.conn_info.service_host = default_host
            if default_port:
                self.conn_info.service_port = default_port
        
            # 初始连接检查
            self._check_connection()
        
        # 更新初始提示符
        self._update_prompt()
    
    def cmdloop(self, intro=None):
        """重载 cmdloop 以处理 ctrl-c 和 ctrl-d"""
        if intro is not None:
            self.intro = intro
        if self.intro:
            self.stdout.write(str(self.intro) + "\n")
        
        # 设置 readline 配置
        if hasattr(readline, "parse_and_bind"):
            # 启用 Tab 补全
            readline.parse_and_bind("tab: complete")
        
        # 主循环
        stop = None
        while not stop:
            try:
                # 读取用户输入
                line = input(self.prompt)
                
                # 处理输入
                line = self.precmd(line)
                stop = self.onecmd(line)
                stop = self.postcmd(stop, line)
            except KeyboardInterrupt:
                # Ctrl-C: 清空当前行并显示新提示符
                print("^C")
                print(f"{Colors.WARNING}已清空输入，继续...{Colors.ENDC}")
                continue
            except EOFError:
                # Ctrl-D: 退回一层或退出
                print("^D")
                if self.level > 0:
                    # 如果在注册中心服务连接状态，返回注册中心
                    if self.conn_info.mode == ConnectionMode.DIRECT and \
                       self.conn_info.registry_host and self.conn_info.current_service_id:
                        print(f"{Colors.OKCYAN}检测到 Ctrl-D，返回上一层...{Colors.ENDC}")
                        self.do_back("")
                        continue
                    # 如果在直连模式且有注册中心信息，返回注册中心
                    elif self.conn_info.mode == ConnectionMode.DIRECT and self.conn_info.registry_host:
                        print(f"{Colors.OKCYAN}检测到 Ctrl-D，返回注册中心...{Colors.ENDC}")
                        self.conn_info.mode = ConnectionMode.REGISTRY
                        self.level = max(0, self.level - 1)
                        self._update_prompt()
                        continue
                
                # 在0层时退出
                print(f"\n{Colors.OKCYAN}感谢使用 JobLens CLI，再见！{Colors.ENDC}\n")
                stop = True
            except SystemExit:
                # 处理 system exit
                stop = True
        
        return self.postloop()
    
    def _update_prompt(self):
        """根据当前连接状态更新提示符"""
        context = self.conn_info.get_prompt_context()
        self.prompt = f"{Colors.OKGREEN}[L{self.level}]{context} JobLens> {Colors.ENDC}"
    
    def _check_connection(self) -> bool:
        """检查当前连接状态"""
        if self.conn_info.mode == ConnectionMode.REGISTRY and self.conn_info.registry_host:
            # 检查注册中心连接
            return self._check_registry_connection()
        else:
            # 检查直接服务连接
            return self._check_service_connection()
    
    def _check_service_connection(self) -> bool:
        """检查 JobLens 服务连接"""
        try:
            url = urljoin(self.conn_info.get_service_base_url(), "/joblens/rpc/health")
            response = self.session.get(url, timeout=3)
            if response.status_code == 200:
                logger.info(f"✓ 已连接到 JobLens: {self.conn_info.service_host}:{self.conn_info.service_port}")
                return True
            else:
                logger.warning(f"⚠ 服务连接失败: HTTP {response.status_code}")
                return False
        except Exception as e:
            logger.warning(f"⚠ 无法连接服务 ({self.conn_info.service_host}:{self.conn_info.service_port}): {str(e)}")
            return False
    
    def _check_registry_connection(self) -> bool:
        """检查注册中心连接"""
        try:
            registry_url = self.conn_info.get_registry_base_url()
            response = self.session.get(f"{registry_url}/health", timeout=3)
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✓ 已连接注册中心: {self.conn_info.registry_host}:{self.conn_info.registry_port}")
                logger.info(f"  管理服务数: {data.get('details', {}).get('registered_services', 0)}")
                return True
            else:
                logger.warning(f"⚠ 注册中心连接失败: HTTP {response.status_code}")
                return False
        except Exception as e:
            logger.warning(f"⚠ 无法连接注册中心 ({self.conn_info.registry_host}:{self.conn_info.registry_port}): {str(e)}")
            return False
    
    def _make_request(self, endpoint: str, method: str = "GET", data: Dict = None, 
                     use_registry: bool = False) -> Optional[Dict]:
        """发送 HTTP 请求"""
        try:
            # 决定使用哪个 base_url
            if use_registry:
                base_url = self.conn_info.get_registry_base_url()
            else:
                base_url = self.conn_info.get_service_base_url()
            
            url = urljoin(base_url, endpoint)
            
            if method.upper() == "GET":
                response = self.session.get(url, timeout=5)
            elif method.upper() == "POST":
                response = self.session.post(url, json=data, timeout=5)
            else:
                raise ValueError(f"不支持的 HTTP 方法: {method}")
            
            if response.status_code in (200, 201):
                return response.json()
            else:
                logger.error(f"请求失败: HTTP {response.status_code}")
                try:
                    error_data = response.json()
                    logger.error(f"错误详情: {error_data.get('error', 'Unknown error')}")
                except:
                    logger.error(f"响应: {response.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"请求异常: {str(e)}")
            return None
    
    def _print_json(self, data: Any, title: str = None):
        """美化打印 JSON 数据"""
        if title:
            print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
            print(f"{Colors.HEADER}{Colors.BOLD}{title:^60s}{Colors.ENDC}")
            print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}\n")
        
        if isinstance(data, dict) and "error" in data:
            print(f"{Colors.FAIL}错误: {data['error']}{Colors.ENDC}")
        else:
            print(json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True))
    
    def _print_table(self, data: List[Dict], headers: List[str] = None, title: str = None):
        """表格形式打印数据"""
        if title:
            print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
            print(f"{Colors.HEADER}{Colors.BOLD}{title:^60s}{Colors.ENDC}")
            print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}\n")
        
        if not data:
            print(f"{Colors.WARNING}暂无数据{Colors.ENDC}")
            return
        
        if HAS_TABULATE:
            print(tabulate(data, headers=headers, tablefmt="grid", showindex=True))
        else:
            self._print_json(data)
    
    def _paginate(self, data: List[Any], page_size: int = 15):
        """分页显示数据"""
        total = len(data)
        pages = (total + page_size - 1) // page_size
        
        for page in range(pages):
            start = page * page_size
            end = min(start + page_size, total)
            
            print(f"\n{Colors.OKCYAN}-- 页 {page+1}/{pages} (显示 {start+1}-{end}/{total}) --{Colors.ENDC}\n")
            yield data[start:end]
            
            if page < pages - 1:
                try:
                    response = input(f"\n{Colors.WARNING}按回车继续，或输入 'q' 退出分页: {Colors.ENDC}")
                    if response.lower() == 'q':
                        break
                except KeyboardInterrupt:
                    print(f"\n{Colors.WARNING}已中断分页显示{Colors.ENDC}")
                    break
    
    # ==================== 核心命令 ====================
    
    def do_connect(self, args: str):
        """
        直接连接 JobLens 服务（直连模式）
        用法: connect [host] [port]
        示例:
            connect           # 连接到默认 localhost:7592
            connect 10.0.0.1 # 连接到 10.0.0.1:7592
            connect remote 7600  # 连接到 remote:7600
        """
        parts = args.split()
        if len(parts) >= 2:
            host, port = parts[0], int(parts[1])
        elif len(parts) == 1:
            host, port = parts[0], self.conn_info.service_port
        else:
            host, port = self.conn_info.service_host, self.conn_info.service_port
        
        # 切换到直连模式
        self.conn_info.mode = ConnectionMode.DIRECT
        self.conn_info.service_host = host
        self.conn_info.service_port = port
        self.conn_info.current_service_id = None
        self.conn_info.current_service_name = None
        
        # 增加层级
        self.level += 1
        
        print(f"{Colors.OKCYAN}正在切换到直连模式: {host}:{port}...{Colors.ENDC}")
        if self._check_connection():
            print(f"{Colors.OKGREEN}✓ 切换成功 (当前层级: {self.level}){Colors.ENDC}")
        else:
            print(f"{Colors.WARNING}⚠ 连接失败，请检查服务{Colors.ENDC}")
        
        self._update_prompt()
    
    def do_registry(self, args: str):
        """
        连接到服务注册中心（注册中心模式）
        用法: registry [host] [port]
        示例:
            registry               # 连接到默认 localhost:8080
            registry 10.0.0.1 # 连接到 10.0.0.1:8080
            registry remote 9000   # 连接到 remote:9000
        """
        parts = args.split()
        if len(parts) >= 2:
            host, port = parts[0], int(parts[1])
        elif len(parts) == 1:
            host, port = parts[0], Config.DEFAULT_REGISTRY_PORT
        else:
            host, port = Config.DEFAULT_REGISTRY_HOST, Config.DEFAULT_REGISTRY_PORT
        
        # 切换到注册中心模式
        self.conn_info.mode = ConnectionMode.REGISTRY
        self.conn_info.registry_host = host
        self.conn_info.registry_port = port
        self.conn_info.current_service_id = None
        self.conn_info.current_service_name = None
        
        # 增加层级
        self.level += 1
        
        print(f"{Colors.OKCYAN}正在连接到注册中心: {host}:{port}...{Colors.ENDC}")
        if self._check_connection():
            print(f"{Colors.OKGREEN}✓ 切换成功 (当前层级: {self.level}){Colors.ENDC}")
        else:
            print(f"{Colors.WARNING}⚠ 连接失败，请检查注册中心{Colors.ENDC}")
        
        self._update_prompt()
    
    def do_list_services(self, args: str):
        """
        列出注册中心管理的所有 JobLens 服务
        别名: ls
        用法: list_services [healthy_only]
        示例:
            list_services       # 列出所有服务
            list_services true  # 仅列出健康服务
        """
        if self.conn_info.mode != ConnectionMode.REGISTRY:
            print(f"{Colors.FAIL}错误: 此命令仅在注册中心模式下可用{Colors.ENDC}")
            print(f"请先使用 'registry' 命令连接到注册中心{Colors.ENDC}")
            return
        
        parts = args.split()
        healthy_only = parts[0].lower() == 'true' if parts else False
        
        endpoint = f"/services?healthy_only={healthy_only}" if healthy_only else "/services"
        result = self._make_request(endpoint, use_registry=True)
        
        if result:
            services = result
            if not services:
                print(f"{Colors.WARNING}暂无注册的服务{Colors.ENDC}")
                return
            
            # 格式化显示
            display_data = []
            for svc in services:
                status_color = Colors.OKGREEN if svc['status'] == 'healthy' else \
                              (Colors.WARNING if svc['status'] == 'unknown' else Colors.FAIL)
                display_data.append({
                    "ID": svc['service_id'][:8] + "...",
                    "名称": svc.get('name', 'N/A'),
                    "地址": f"{svc['host']}:{svc['port']}",
                    "状态": f"{status_color}{svc['status']}{Colors.ENDC}",
                    "版本": svc.get('version', 'N/A'),
                    "最后心跳": svc.get('last_heartbeat', 'N/A')[:19]  # 去掉毫秒
                })
            
            self._print_table(
                display_data,
                headers="keys",
                title=f"注册服务列表 ({'健康' if healthy_only else '全部'})"
            )
            
            # 显示统计
            status_counts = {}
            for svc in services:
                status_counts[svc['status']] = status_counts.get(svc['status'], 0) + 1
            
            print(f"\n{Colors.BOLD}统计信息:{Colors.ENDC}")
            for status, count in status_counts.items():
                print(f"  {status}: {count}")
    
    def do_cs(self, args: str):
        """connect-service 的快捷方式"""
        self.do_connect_service(args)
    
    def do_connect_service(self, args: str):
        """
        连接到注册中心中的某个 JobLens 服务
        别名: cs
        用法: connect_service <service_id|service_name>
        示例:
            connect_service a1b2c3d4-e5f6...    # 使用完整 ID
            cs a1b2c3d4                        # 使用 ID 前缀
            cs joblens-prod-01                 # 使用服务名称
        """
        if self.conn_info.mode != ConnectionMode.REGISTRY:
            print(f"{Colors.FAIL}错误: 此命令仅在注册中心模式下可用{Colors.ENDC}")
            return
        
        if not args:
            print(f"{Colors.FAIL}错误: 请指定服务 ID 或名称{Colors.ENDC}")
            print(f"用法: connect_service <service_id|service_name>{Colors.ENDC}")
            return
        
        search_term = args.strip()
        
        # 获取服务列表
        result = self._make_request("/services", use_registry=True)
        if not result:
            print(f"{Colors.FAIL}无法获取服务列表{Colors.ENDC}")
            return
        
        services = result
        target_service = None
        
        # 查找匹配的服务
        for svc in services:
            # 精确匹配 ID 或名称
            if svc['service_id'].startswith(search_term) or \
               (svc.get('name') and svc['name'] == search_term):
                target_service = svc
                break
        
        if not target_service:
            print(f"{Colors.FAIL}未找到匹配的服务: {search_term}{Colors.ENDC}")
            print(f"{Colors.WARNING}使用 'list_services' 查看可用服务{Colors.ENDC}")
            return
        
        # 检查服务状态
        if target_service['status'] != 'healthy':
            response = input(f"{Colors.WARNING}服务状态为 {target_service['status']}，继续连接吗? (y/N): {Colors.ENDC}")
            if response.lower() != 'y':
                return
        
        # 连接到该服务
        print(f"{Colors.OKCYAN}正在连接到服务: {target_service['host']}:{target_service['port']}...{Colors.ENDC}")
        
        # 保存注册中心信息
        reg_host = self.conn_info.registry_host
        reg_port = self.conn_info.registry_port
        
        # 切换到该服务
        self.conn_info.mode = ConnectionMode.DIRECT
        self.conn_info.service_host = target_service['host']
        self.conn_info.service_port = target_service['port']
        self.conn_info.current_service_id = target_service['service_id']
        self.conn_info.current_service_name = target_service.get('name')
        self.conn_info.registry_host = reg_host  # 保留注册中心信息
        self.conn_info.registry_port = reg_port
        
        # 增加层级
        self.level += 1
        
        if self._check_service_connection():
            print(f"{Colors.OKGREEN}✓ 连接成功 (当前层级: {self.level}){Colors.ENDC}")
            print(f"服务名称: {target_service.get('name', 'N/A')}")
            print(f"服务版本: {target_service.get('version', 'N/A')}")
        else:
            print(f"{Colors.FAIL}✗ 连接失败{Colors.ENDC}")
        self._update_prompt()
    
    def do_back(self, args: str):
        """
        返回注册中心视图（从直连模式切回注册中心模式）
        别名: b
        """
        if self.conn_info.mode != ConnectionMode.DIRECT or \
           not (self.conn_info.registry_host and self.conn_info.current_service_id):
            print(f"{Colors.WARNING}当前不在注册中心服务连接状态中{Colors.ENDC}")
            return
        
        # 保存当前服务信息用于显示
        last_service = self.conn_info.current_service_name or self.conn_info.current_service_id[:8]
        
        # 切换回注册中心
        self.conn_info.mode = ConnectionMode.REGISTRY
        self.conn_info.current_service_id = None
        self.conn_info.current_service_name = None
        
        # 减少层级
        self.level = max(0, self.level - 1)
        
        print(f"{Colors.OKCYAN}已返回注册中心视图{Colors.ENDC}")
        print(f"上一个服务: {last_service}")
        print(f"当前层级: {self.level}")
        
        if self._check_registry_connection():
            print(f"{Colors.OKGREEN}✓ 已重新连接注册中心{Colors.ENDC}")
        
        self._update_prompt()
    
    def do_disconnect(self, args: str):
        """断开当前连接"""
        self.conn_info.mode = ConnectionMode.DIRECT
        self.conn_info.service_host = Config.DEFAULT_SERVICE_HOST
        self.conn_info.service_port = Config.DEFAULT_SERVICE_PORT
        self.conn_info.current_service_id = None
        self.conn_info.current_service_name = None
        
        # 重置层级
        self.level = 0
        
        print(f"{Colors.WARNING}已断开连接 (层级已重置){Colors.ENDC}")
        self._update_prompt()
    
    # ==================== 原有功能命令 ====================
    
    def do_status(self, args: str):
        """显示当前连接状态"""
        print(f"\n{Colors.BOLD}连接状态:{Colors.ENDC}")
        print(f"  层级: {self.level}")
        print(f"  模式: {self.conn_info.mode.value}")
        
        if self.conn_info.mode == ConnectionMode.REGISTRY:
            print(f"  注册中心: {self.conn_info.registry_host}:{self.conn_info.registry_port}")
            if self.conn_info.current_service_id:
                print(f"  当前服务: {self.conn_info.current_service_name or self.conn_info.current_service_id[:8]}")
                print(f"  服务地址: {self.conn_info.service_host}:{self.conn_info.service_port}")
        else:
            print(f"  服务地址: {self.conn_info.service_host}:{self.conn_info.service_port}")
        
        print(f"  是否在线: {'是' if self._check_connection() else '否'}")
    
    def do_health(self, args: str):
        """检查服务健康状态"""
        if not self.conn_info.is_connected():
            print(f"{Colors.WARNING}未连接，请先使用 'connect' 或 'registry' 连接{Colors.ENDC}")
            return
        
        if self.conn_info.mode == ConnectionMode.REGISTRY and not self.conn_info.current_service_id:
            # 检查注册中心健康
            result = self._make_request("/health", use_registry=True)
            if result:
                self._print_json(result, "注册中心健康状态")
        else:
            # 检查具体服务健康
            result = self._make_request("/joblens/rpc/health")
            if result:
                self._print_json(result, "服务健康状态")
    
    def do_functions(self, args: str):
        """列出所有可用的 RPC 方法"""
        if not self._ensure_service_connected():
            return
        
        result = self._make_request("/joblens/rpc/functions")
        if result:
            functions = result.get("functions", [])
            self._print_table(
                [{"名称": f} for f in functions],
                title=f"可用 RPC 方法列表 ({result.get('count', 0)} 个)"
            )
    
    def do_collectors(self, args: str):
        """查看 Collector 性能统计"""
        if not self._ensure_service_connected():
            return
        
        result = self._make_request("/joblens/collectors/perf")
        if result and result.get("status") == "ok":
            perf_data = result.get("collectors_perf", [])
            self._print_table(
                perf_data,
                headers="keys",
                title="Collector 性能统计"
            )
    
    def do_job(self, args: str):
        """作业管理命令"""
        if not self._ensure_service_connected():
            return
        
        parts = args.split()
        cmd = parts[0] if parts else "list"
        
        if cmd in ("list", "ls"):
            result = self._make_request("/joblens/jobs")
            if result:
                jobs = result.get("jobs", [])
                if jobs:
                    self._print_table(jobs, headers="keys", title=f"作业列表 (共 {len(jobs)} 个)")
                else:
                    print(f"{Colors.WARNING}暂无作业{Colors.ENDC}")
        
        elif cmd == "count":
            result = self._make_request("/joblens/jobs/count")
            if result:
                count = result.get("job_count", 0)
                print(f"\n{Colors.BOLD}当前作业总数: {Colors.OKGREEN}{count}{Colors.ENDC}\n")
        
        elif cmd == "get":
            if len(parts) < 2:
                print(f"{Colors.FAIL}错误: 请指定 JobID{Colors.ENDC}")
                return
            try:
                job_id = int(parts[1])
                result = self._make_request(f"/joblens/jobs/{job_id}")
                if result:
                    self._print_json(result, f"作业详情 (JobID: {job_id})")
            except ValueError:
                print(f"{Colors.FAIL}错误: JobID 必须是整数{Colors.ENDC}")
        
        elif cmd in ("page", "pg"):
            result = self._make_request("/joblens/jobs")
            if result:
                jobs = result.get("jobs", [])
                if jobs:
                    for page_data in self._paginate(jobs):
                        self._print_table(page_data, headers="keys")
                else:
                    print(f"{Colors.WARNING}暂无作业{Colors.ENDC}")
        
        elif cmd == "add":
            job_type = ['common', 'condor', 'slurm']
            print(f"\n{Colors.BOLD}交互式添加作业ing，按下q退出...{Colors.ENDC}")
            print(f"{Colors.OKCYAN}支持的作业类型: {', '.join(job_type)}{Colors.ENDC}")
            input_data = input(f"{Colors.OKCYAN}请输入作业类型（default:common）: {Colors.ENDC}")
            job_type_input = input_data.strip() or 'common'
            
            if job_type_input == 'q':
                print(f"{Colors.WARNING}已取消添加作业{Colors.ENDC}")
                return
            if job_type_input not in job_type:
                print(f"{Colors.FAIL}错误: 不支持的作业类型{Colors.ENDC}")
                return
            
            jobid_input = input(f"{Colors.OKCYAN}请输入作业ID（整数）: {Colors.ENDC}")
            try:
                job_id = int(jobid_input.strip())
            except ValueError:
                print(f"{Colors.FAIL}错误: 作业ID必须是整数{Colors.ENDC}")
                return
            result = None
            match job_type_input:
                case 'common':
                    input_data = input(f"{Colors.OKCYAN}请输入作业PID，使用空格分隔: {Colors.ENDC}")
                    pids = input_data.strip()
                    if pids == 'q':
                        print(f"{Colors.WARNING}已取消添加作业{Colors.ENDC}")
                        return
                    pid_list = []
                    for pid_str in pids.split():
                        try:
                            pid_list.append(int(pid_str))
                        except ValueError:
                            print(f"{Colors.FAIL}错误: PID 必须是整数{Colors.ENDC}")
                            return
                    data = {
                        "job_id": job_id,
                        "type": "common",
                        "pids": pid_list
                    }
                    result = self._make_request("/joblens/jobs", method="POST", data=data)
                case 'condor':
                    input_data = input(f"{Colors.OKCYAN}请输入作业所在槽（格式: slot[num]）: {Colors.ENDC}")
                    slot_num = input_data.strip()
                    if slot_num == 'q':
                        print(f"{Colors.WARNING}已取消添加作业{Colors.ENDC}")
                        return
                    if not slot_num.startswith('slot') or not slot_num.split('slot')[1].isdigit():
                        print(f"{Colors.FAIL}错误: 槽格式错误{Colors.ENDC}")
                        return
                    data = {
                        "job_id": job_id,
                        "type": "condor",
                        "slot": slot_num
                    }
                    result = self._make_request("/joblens/jobs", method="POST", data=data)
                case 'slurm':
                    pass
            if result and result.get("status") == "ok":
                print(f"{Colors.OKGREEN}✓ 作业添加成功{Colors.ENDC}")
            else:
                print(f"{Colors.FAIL}✗ 作业添加失败{Colors.ENDC}")
                print(f"{Colors.FAIL}错误详情: {result.get('error', '未知错误')}{Colors.ENDC}")
                    
                
        
        else:
            print(f"{Colors.FAIL}未知子命令: {cmd}{Colors.ENDC}")
            print("可用: list, count, get <id>, page")
    
    def do_writers(self, args: str):
        """Writer 管理命令"""
        if not self._ensure_service_connected():
            return
        
        parts = args.split()
        if not parts:
            print(f"{Colors.WARNING}请指定子命令: list, perf, metrics <name>, info <name>{Colors.ENDC}")
            return
        
        cmd = parts[0]
        
        if cmd in ("list", "ls"):
            result = self._make_request("/joblens/rpc/functions")
            if result:
                functions = result.get("functions", [])
                writers = [f.replace("/info", "") for f in functions if f.endswith("/info")]
                self._print_table([{"Writer 名称": w} for w in writers], title="可用 Writer 列表")
        
        elif cmd == "perf":
            result = self._make_request("/joblens/writers/perf")
            if result and result.get("status") == "ok":
                perf_data = result.get("writers_perf", [])
                self._print_table(perf_data, headers="keys", title="Writer 性能统计")
        
        elif cmd == "metrics":
            if len(parts) < 2:
                print(f"{Colors.FAIL}错误: 请指定 Writer 名称{Colors.ENDC}")
                return
            writer_name = parts[1]
            result = self._make_request(f"/joblens/writers/{writer_name}/metrics")
            if result:
                self._print_json(result, f"Writer 指标 - {writer_name}")
        
        elif cmd == "info":
            if len(parts) < 2:
                print(f"{Colors.FAIL}错误: 请指定 Writer 名称{Colors.ENDC}")
                return
            writer_name = parts[1]
            result = self._make_request(f"/joblens/writers/{writer_name}/info")
            if result:
                self._print_json(result, f"Writer 信息 - {writer_name}")
        
        else:
            print(f"{Colors.FAIL}未知子命令: {cmd}{Colors.ENDC}")
    
    # ==================== 辅助方法 ====================
    
    def _ensure_service_connected(self) -> bool:
        """确保已连接到具体的 JobLens 服务"""
        if not self.conn_info.is_connected():
            print(f"{Colors.WARNING}未连接，请先使用以下方式连接:{Colors.ENDC}")
            print(f"  • {Colors.OKCYAN}connect <host> <port>{Colors.ENDC}    - 直接连接服务")
            print(f"  • {Colors.OKCYAN}registry <host> <port>{Colors.ENDC} - 连接注册中心")
            print(f"  • {Colors.OKCYAN}cs <service_id>{Colors.ENDC}        - 从注册中心选择服务")
            return False
        
        # 在注册中心模式下，必须已选择具体服务
        if self.conn_info.mode == ConnectionMode.REGISTRY and not self.conn_info.current_service_id:
            print(f"{Colors.WARNING}当前在注册中心视图，请先选择要操作的服务:{Colors.ENDC}")
            print(f"使用 {Colors.OKCYAN}list_services{Colors.ENDC} 查看服务列表")
            print(f"使用 {Colors.OKCYAN}connect_service <id>{Colors.ENDC} 连接服务")
            return False
        
        return True
    
    def do_clear(self, args: str):
        """清屏"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def do_exit(self, args: str):
        """退出程序"""
        print(f"\n{Colors.OKCYAN}感谢使用 JobLens CLI，再见！{Colors.ENDC}\n")
        return True
    
    def do_quit(self, args: str):
        """退出程序（同 exit）"""
        return self.do_exit(args)
    
    # ==================== 帮助系统 ====================
    
    def do_help(self, args: str):
        """
        显示帮助信息
        用法: help [command]
        """
        if args:
            try:
                doc = getattr(self, f"do_{args}").__doc__
                if doc:
                    print(f"\n{Colors.BOLD}命令: {args}{Colors.ENDC}")
                    print(doc)
                else:
                    print(f"\n{Colors.WARNING}暂无此命令的详细说明{Colors.ENDC}")
            except AttributeError:
                print(f"\n{Colors.FAIL}未知命令: {args}{Colors.ENDC}")
        else:
            self._show_main_help()
    
    def _show_main_help(self):
        """显示主帮助菜单"""
        print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
        print(f"{Colors.HEADER}{Colors.BOLD}{'JobLens CLI - 可用命令':^60s}{Colors.ENDC}")
        print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}\n")
        
        # 模式切换命令
        print(f"{Colors.HEADER}模式切换:{Colors.ENDC}")
        mode_commands = [
            ("connect [host] [port]", "直接连接服务 (直连模式)"),
            ("registry [host] [port]", "连接注册中心 (注册中心模式)"),
            ("disconnect", "断开连接")
        ]
        for cmd_name, desc in mode_commands:
            print(f"  {Colors.OKCYAN}{cmd_name:<25s}{Colors.ENDC} {desc}")
        
        # 注册中心命令
        print(f"\n{Colors.HEADER}注册中心操作:{Colors.ENDC}")
        registry_commands = [
            ("list_services [healthy_only]", "查看所有注册服务"),
            ("connect_service <id>", "连接到指定服务"),
            ("cs <id>", "connect_service 快捷方式"),
            ("back", "返回注册中心视图")
        ]
        for cmd_name, desc in registry_commands:
            print(f"  {Colors.OKCYAN}{cmd_name:<25s}{Colors.ENDC} {desc}")
        
        # 服务操作命令
        print(f"\n{Colors.HEADER}服务操作:{Colors.ENDC}")
        service_commands = [
            ("status", "查看连接状态"),
            ("health", "服务健康检查"),
            ("functions", "列出 RPC 方法"),
            ("collectors", "查看 Collector 性能"),
            ("job list|count|get|add", "作业管理"),
            ("writers list|perf|metrics|info", "Writer 管理")
        ]
        for cmd_name, desc in service_commands:
            print(f"  {Colors.OKCYAN}{cmd_name:<25s}{Colors.ENDC} {desc}")
        
        # 系统命令
        print(f"\n{Colors.HEADER}系统命令:{Colors.ENDC}")
        sys_commands = [
            ("clear", "清屏"),
            ("exit/quit", "退出程序"),
            ("help [cmd]", "显示帮助")
        ]
        for cmd_name, desc in sys_commands:
            print(f"  {Colors.OKCYAN}{cmd_name:<25s}{Colors.ENDC} {desc}")
        
        print(f"\n{Colors.WARNING}提示: 使用 'help <命令名>' 查看详细说明{Colors.ENDC}\n")
    
    # ==================== 自动补全 ====================
    
    def _complete_subcommand(self, text: str, options: List[str]) -> List[str]:
        """辅助方法：子命令自动补全"""
        if not text:
            return options
        return [opt for opt in options if opt.startswith(text)]
    
    def complete_connect(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        """connect 命令补全"""
        return ["localhost", "127.0.0.1"] if not text else []
    
    def complete_registry(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        """registry 命令补全"""
        return ["localhost", "127.0.0.1"] if not text else []
    
    def complete_list_services(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        """list_services 命令补全"""
        return ["true", "false"] if text else []
    
    def complete_connect_service(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        """connect_service 命令补全"""
        # 尝试从注册中心获取服务列表进行补全
        try:
            if self.conn_info.mode == ConnectionMode.REGISTRY and self.conn_info.registry_host:
                response = self.session.get(
                    f"http://{self.conn_info.registry_host}:{self.conn_info.registry_port}/services",
                    timeout=2
                )
                if response.status_code == 200:
                    services = response.json()
                    ids = [s['service_id'] for s in services]
                    names = [s.get('name') for s in services if s.get('name')]
                    return self._complete_subcommand(text, ids + names)
        except:
            pass
        return []
    
    def complete_jobs(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        """jobs 命令补全"""
        return self._complete_subcommand(text, ["list", "count", "get", "page", "ls", "pg"])
    
    def complete_writers(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        """writers 命令补全"""
        subcommands = ["list", "perf", "metrics", "info", "ls"]
        return self._complete_subcommand(text, subcommands)
    
    def complete_help(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        """help 命令补全"""
        commands = ["connect", "registry", "disconnect", "list_services", "cs",
                   "status", "health", "functions", "collectors", "jobs", "writers",
                   "clear", "exit", "quit", "back"]
        return self._complete_subcommand(text, commands)
    
    # ==================== 快捷别名 ====================
    
    def do_ls(self, args: str):
        """list_services 的快捷方式"""
        self.do_list_services(args)
    
    def do_cs(self, args: str):
        """connect_service 的快捷方式"""
        self.do_connect_service(args)
    
    def do_b(self, args: str):
        """back 的快捷方式"""
        self.do_back(args)
    
    # ==================== 空命令处理 ====================
    
    def emptyline(self):
        """回车不重复执行上条命令"""
        pass
    
    def default(self, line: str):
        """未知命令处理"""
        print(f"{Colors.FAIL}未知命令: {line}{Colors.ENDC}")
        print(f"输入 'help' 查看可用命令{Colors.ENDC}")

# ==================== 日志配置 ====================

import logging

def setup_logging():
    """配置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

# ==================== 主入口 ====================

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description=f"JobLens CLI v{VER} - 交互式 RPC 查询工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                           # 启动 CLI
  %(prog)s -H 10.0.0.1          # 启动并直接连接服务
  %(prog)s -H 10.0.0.1 -p 7592  # 启动并连接指定端口
  %(prog)s --no-color                # 禁用颜色输出
  %(prog)s --registry 192.168.1.200  # 启动并连接注册中心
        """
    )
    
    parser.add_argument("-H", "--host", default=None,
                       help=f"默认 JobLens 服务主机 (默认: {Config.DEFAULT_SERVICE_HOST})")
    parser.add_argument("-p", "--port", type=int, default=None,
                       help=f"默认 JobLens 服务端口 (默认: {Config.DEFAULT_SERVICE_PORT})")
    parser.add_argument("-r", "--registry", default=None,
                       help=f"默认注册中心主机:端口 (如: 10.0.0.2:8080)")
    parser.add_argument("--no-color", action="store_true",
                       help="禁用颜色输出")
    
    args = parser.parse_args()
    
    # 禁用颜色
    if args.no_color or not sys.stdout.isatty():
        Colors.disable()
    
    # 设置日志
    setup_logging()
    
    # 处理注册中心参数
    reg_host, reg_port = None, None
    if args.registry:
        if ':' in args.registry:
            reg_host, reg_port = args.registry.split(':')
            reg_port = int(reg_port)
        else:
            reg_host = args.registry
            reg_port = Config.DEFAULT_REGISTRY_PORT
    
    try:
        # 创建 CLI 实例
        shell = JobLensShell(default_host=args.host, default_port=args.port)
        
        # 如果指定了注册中心，自动连接
        if reg_host:
            shell.conn_info.mode = ConnectionMode.REGISTRY
            shell.conn_info.registry_host = reg_host
            shell.conn_info.registry_port = reg_port
            shell._check_connection()
            shell._update_prompt()
        
        # 启动交互循环
        shell.cmdloop()
    
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}用户中断，正在退出...{Colors.ENDC}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{Colors.FAIL}启动失败: {str(e)}{Colors.ENDC}")
        sys.exit(1)

if __name__ == "__main__":
    main()