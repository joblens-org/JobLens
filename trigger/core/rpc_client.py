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
import json
import logging
import socket
import os
import time
from typing import Any, Optional, Union, List

logger = logging.getLogger(__name__)

class RPCError(Exception):
    """RPC调用异常"""
    pass

class RPCClient:
    """
    与C++ RPC服务端通信的Python客户端
    """
    
    def __init__(self, socket_path: str = "/tmp/JobLens/rpc.sock", timeout: float = 5.0):
        self.socket_path = socket_path
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        
    def connect(self):
        """连接到RPC服务端"""
        if self._sock is not None:
            return
            
        if not os.path.exists(self.socket_path):
            raise RPCError(f"Socket file not found: {self.socket_path}")
            
        try:
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._sock.settimeout(self.timeout)
            self._sock.connect(self.socket_path)
        except socket.error as e:
            self._sock = None
            raise RPCError(f"Failed to connect to RPC server: {e}")
    
    def close(self):
        """关闭连接"""
        if self._sock:
            try:
                self._sock.close()
            except:
                pass
            finally:
                self._sock = None
    
    def _send_all(self, data: bytes):
        """发送所有数据"""
        if self._sock is None:
            raise RPCError("Not connected to server")
            
        total_sent = 0
        while total_sent < len(data):
            try:
                sent = self._sock.send(data[total_sent:])
                if sent == 0:
                    raise RPCError("Socket connection broken")
                total_sent += sent
            except socket.error as e:
                raise RPCError(f"Send failed: {e}")
    
    def _recv_all(self) -> str:
        chunks = []
        self._sock.settimeout(self.timeout)
        try:
            while True:
                chunk = self._sock.recv(4096)
                if not chunk:
                    break  # 对端关闭
                chunks.append(chunk)
        except socket.timeout:
            pass  # 超时也算读完
        return b''.join(chunks).decode('utf-8')
    
    def call(self, method: str, params: Any = None) -> Any:
        """
        调用远程方法
        """
        self.connect()
        
        request = {
            "method": method,
            "params": params if params is not None else {}
        }
        
        try:
            request_data = json.dumps(request)
            logger.debug("RPC request: method=%s, params_size=%d", method, len(request_data))
            self._send_all(request_data.encode('utf-8'))
            
            # 关闭连接的写端，让服务器知道请求结束
            self._sock.shutdown(socket.SHUT_WR)
            
            response_data = self._recv_all()
            if not response_data:
                raise RPCError("Empty response from server")
                
            response = json.loads(response_data)
            if isinstance(response, dict):
                logger.debug("RPC response: method=%s, status=%s, msg=%s",
                             method, response.get('status'), response.get('msg', ''))
            else:
                logger.debug("RPC response: method=%s, type=%s, len=%d",
                             method, type(response).__name__,
                             len(response) if hasattr(response, '__len__') else -1)
            
            self.close()
            return response
            
        except json.JSONDecodeError as e:
            logger.error("RPC JSON decode error: method=%s, error=%s", method, str(e))
            raise RPCError(f"Invalid JSON response: {e}")
        except Exception as e:
            self.close()
            if isinstance(e, RPCError):
                raise
            logger.error("RPC call exception: method=%s, error=%s", method, str(e))
            raise RPCError(f"RPC call failed: {e}")
        

    def health_check(self) -> bool:
        """检查服务健康状态"""
        try:
            start_time = time.time()
            result = self.call("health")
            print('call use time: ',time.time()-start_time)
            return result.get("status") == "healthy"
        except:
            return False
    
    def get_function_list(self) -> List[str]:
        """获取可用的函数列表"""
        try:
            result = self.call("func_list")
            # 从响应中提取函数列表
            if isinstance(result, dict) and "functions" in result:
                return result["functions"]
            else:
                return result
        except RPCError:
            raise
        except Exception as e:
            raise RPCError(f"Failed to get function list: {e}")


# 专门为C++服务端设计的客户端
class JoblensRPCClient:
    """
    专门为C++ RPC服务端设计的客户端封装
    """
    
    def __init__(self, socket_path: str = "/tmp/JobLens/rpc.sock"):
        self.client = RPCClient(socket_path)
        
    def health_check(self) -> bool:
        """健康检查"""
        return self.client.health_check()
    
    def get_functions(self) -> List[str]:
        """获取函数列表"""
        return self.client.get_function_list()
    
    def echo(self, message: str) -> str:
        """调用echo方法（如果服务端有的话）"""
        result = self.client.call("echo", message)

    def list_jobs(self) -> List[dict]:
        """列出所有Job"""
        return self.client.call("JobRegistry/list_jobs")
    
    def get_job(self, job_id: int) -> dict:
        """获取指定JobID的Job信息"""
        return self.client.call("JobRegistry/get_job", {"JobID": job_id})
    
    def metrics_report(self, metric_data: dict) -> bool:
        """上报指标数据"""
        result = self.client.call("prmxs_writer/metrics", metric_data)
        return result


if __name__ == "__main__":
    # 测试新功能
    try:
        client = JoblensRPCClient()
        
        # 健康检查
        if client.health_check():
            print("✓ Service is healthy")
        else:
            print("✗ Service is not healthy")
        # 健康检查
        if client.health_check():
            print("✓ Service is healthy")
        else:
            print("✗ Service is not healthy")
        
        # 获取函数列表
        functions = client.get_functions()
        print(f"Available functions: {functions}")
        client.client.call("CollectorRegistry/CollectorsPerfCount")
        client.client.call("WriterManager/WriterPerfCount")
        # client.client.call('JobRegistry/job_opt', {
        #     "opt": "add",
        #     "type": "job.common",
        #     "JobID": 12345,
        #     "JobPIDs": [1],
        #     "Lens": ["cpumem_collector"]
        # })

        # client.client.call('JobRegistry/job_opt', {
        #     "opt": "add",
        #     "type": "job.common",
        #     "JobID": 54321,
        #     "JobPIDs": [5],
        #     "Lens": ["cpumem_collector"]
        # })

        # print(client.client.call('JobRegistry/list_jobs'))
    except RPCError as e:
        print(f"RPC Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
