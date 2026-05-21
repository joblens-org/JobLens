# JobLens Trigger Service

JobLens Trigger Service 是一个基于 Flask 的 RESTful API 服务，为 JobLens 作业监控系统提供 HTTP 接口和配置管理功能。它充当 JobLens C++ 核心服务的前端网关，支持作业管理、监控指标查询、配置动态更新等服务。

## 📋 主要功能

### 1. **服务注册与发现**
- 自动向服务注册中心注册实例
- 支持多实例负载均衡与服务发现
- 健康检查与自动故障转移

### 2. **配置管理**
- 支持本地 YAML 配置文件和 etcd 分布式配置
- 实时配置变更监控与动态更新
- 配置变更回调机制，支持配置热更新

### 3. **作业管理 API**
- 作业的添加、删除操作
- 支持普通作业和 HTCondor 作业
- 批量作业查询与状态监控

### 4. **监控指标导出**
- Prometheus 格式的监控指标 (`/metrics` 端点)
- 实时作业性能数据（CPU、内存、I/O、网络等）
- 收集器和写入器的性能统计

### 5. **系统管理**
- 服务健康状态检查
- 远程版本升级（支持 token 认证）
- RPC 通信状态监控

### 6. **HTCondor 集成**
- 自动发现和监控 Condor 作业
- 通过作业槽动态添加作业
- Condor 作业状态同步

## 🚀 快速开始

### 环境要求
- Python 3.7+
- JobLens 核心服务已安装并运行
- 可选：etcd v3+（用于分布式配置管理）

### 安装依赖
```bash
cd trigger
pip install -r requirements.txt
```

### 启动服务
```bash
# 直接启动（开发环境）
python app.py

# 使用 Gunicorn 启动（生产环境）
gunicorn --config gunicorn.conf.py app:app

# 使用部署脚本
./deploy_trigger.sh
```

### 验证服务
```bash
# 检查服务健康状态
curl http://localhost:7592/joblens/healthy

# 获取 Prometheus 指标
curl http://localhost:7592/metrics
```

## ⚙️ 配置说明

### 主配置文件
Trigger 服务使用 JobLens 主配置文件 (`../config/config.yaml`)，主要配置项包括：

```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock  # RPC 通信 socket 路径
  rpc_timeout: 5                          # RPC 调用超时时间（秒）
```

### 环境变量配置
- `UPGRADE_TOKEN`: 升级接口的认证 token（从环境变量读取，默认不校验）

### 服务注册配置
- 注册中心地址: `http://your-registry-host:8080`（通过环境变量 `REGISTRY_URL` 配置）
- 服务端口: `7592`
- 自动注册: 启动时自动向注册中心注册

## 📡 API 接口

### 健康检查
```bash
GET /joblens/healthy
```
返回 JobLens 服务的健康状态。

### Prometheus 指标
```bash
GET /metrics
```
返回 Prometheus 格式的监控指标，包括作业状态、资源使用情况等。

### 作业管理

#### 添加/删除作业
```bash
POST /joblens/job
Content-Type: application/json

{
  "opt": "add",           # 操作类型："add" 或 "remove"
  "type": "job.common",   # 作业类型："job.common" 或 "job.condor"
  "JobID": 12345,         # 作业 ID
  "JobPIDs": [6789],      # 进程 PID 列表
  "Lens": ["proc_collector", "cpumem_collector"]  # 使用的收集器
}
```

#### 添加 Condor 作业
```bash
POST /joblens/condor_job
Content-Type: application/json

{
  "opt": "add",           # 目前仅支持 "add"
  "slot": "slot1",        # Condor 作业槽名称
  "JobID": 12345,         # 作业 ID
  "Lens": ["proc_collector"]  # 使用的收集器
}
```

#### 查询作业信息
```bash
GET /joblens/jobs                    # 列出所有作业
GET /joblens/jobs/count              # 获取作业总数
GET /joblens/jobs/{job_id}           # 获取指定作业详情
```

### RPC 功能

#### RPC 健康检查
```bash
GET /joblens/rpc/health
```
检查 RPC 服务端健康状态，返回详细的服务状态和延迟。

#### 获取可用函数列表
```bash
GET /joblens/rpc/functions
```
获取 RPC 服务端注册的所有可用方法列表。

### 性能统计

#### 收集器性能
```bash
GET /joblens/collectors/perf
```
获取所有 Collector 的性能统计信息。

#### 写入器性能
```bash
GET /joblens/writers/perf
```
获取所有 Writer 的性能统计信息。

#### 写入器信息
```bash
GET /joblens/writers/{writer_name}/info
```
获取指定 Writer 的基本信息和配置。

### 系统管理

#### 版本升级
```bash
POST /joblens/version/upgrade
Authorization: Bearer <upgrade_token>
```
触发远程版本升级，需要有效的升级 token。

#### 配置更新
```bash
GET /joblens/config/update
```
手动触发配置更新（调用配置管理器的回调函数）。

## 🔧 部署指南

### 使用部署脚本
```bash
# 运行部署脚本
./deploy_trigger.sh
```
部署脚本会：
1. 创建 Python 虚拟环境
2. 安装依赖包
3. 创建 systemd 服务文件
4. 启用并启动服务

### 手动部署

#### 1. 创建 systemd 服务文件
创建 `/etc/systemd/system/joblens-trigger.service`：
```ini
[Unit]
Description=JobLens Trigger Service
After=network.target

[Service]
User=<username>
WorkingDirectory=/path/to/JobLens/trigger
Environment="PATH=/path/to/JobLens/trigger/venv/bin"
ExecStart=/path/to/JobLens/trigger/venv/bin/gunicorn --config /path/to/JobLens/trigger/gunicorn.conf.py app:app
Restart=always
RestartSec=3
StartLimitInterval=0

[Install]
WantedBy=multi-user.target
```

#### 2. 启动服务
```bash
sudo systemctl daemon-reload
sudo systemctl enable joblens-trigger
sudo systemctl start joblens-trigger
```

### Gunicorn 配置
Gunicorn 配置文件 (`gunicorn.conf.py`) 主要设置：
- 绑定地址：`0.0.0.0:7592`
- Worker 数量：1（同步模式）
- 超时时间：120秒
- 日志级别：info

## 🛠️ 开发说明

### 项目结构
```
trigger/
├── app.py                 # Flask 主应用，包含所有 API 路由
├── config_manager.py      # 配置管理器，支持本地和 etcd 配置
├── rpc_cilent.py          # RPC 客户端，与 JobLens C++ 服务通信
├── tools.py               # 工具函数集（作业查询、指标格式化等）
├── user_config.py         # 用户配置类
├── deploy_trigger.sh      # 部署脚本
├── gunicorn.conf.py       # Gunicorn 配置文件
├── requirements.txt       # Python 依赖包列表
└── README.md              # 本文档
```

### 核心组件

#### ConfigManager
- 支持本地 YAML 配置和 etcd 分布式配置
- 实时监控配置变更
- 配置变更回调机制
- 配置历史记录

#### RPCClient
- 基于 Unix socket 的 RPC 通信
- 超时和错误处理
- 服务健康检查
- 函数列表查询

#### ServiceRegistrar
- 服务自动注册与注销
- 注册中心心跳保持
- 服务实例标识管理

### 扩展开发

#### 添加新 API 端点
在 `app.py` 中添加新的路由函数：
```python
@app.route('/joblens/new_endpoint', methods=['GET'])
def new_endpoint():
    # 实现逻辑
    return jsonify({'status': 'ok'})
```

#### 添加新工具函数
在 `tools.py` 中添加功能函数，支持异步或同步操作。

## 🔍 故障排除

### 常见问题

#### 1. 服务无法启动
- 检查 JobLens 核心服务是否运行
- 验证 RPC socket 路径是否正确
- 查看日志：`sudo journalctl -u joblens-trigger -f`

#### 2. API 请求返回错误
- 确认服务端口（默认 7592）是否监听
- 检查请求参数格式是否符合要求
- 验证 RPC 连接是否正常

#### 3. 配置更新不生效
- 检查 etcd 连接状态（如果使用 etcd）
- 确认配置文件路径权限
- 查看配置变更回调是否注册

#### 4. Prometheus 指标为空
- 确认有作业正在被监控
- 检查 RPC 调用是否返回数据
- 验证指标格式化函数

### 日志查看
```bash
# systemd 服务日志
sudo journalctl -u joblens-trigger -f

# Gunicorn 访问日志
tail -f /var/log/gunicorn-access.log

# Gunicorn 错误日志  
tail -f /var/log/gunicorn-error.log
```

## 📊 监控与指标

### 服务指标
- `joblens_trigger_requests_total`: 请求总数
- `joblens_trigger_request_duration_seconds`: 请求耗时
- `joblens_trigger_rpc_latency_ms`: RPC 调用延迟

### 集成监控
Trigger 服务本身可以通过以下方式监控：
1. **Prometheus**: 抓取 `/metrics` 端点
2. **健康检查**: 定期调用 `/joblens/healthy`
3. **日志聚合**: 收集 Gunicorn 和 systemd 日志

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request 来改进 Trigger 服务。

### 开发流程
1. Fork 项目仓库
2. 创建功能分支
3. 提交代码变更
4. 运行基础测试
5. 创建 Pull Request

### 代码规范
- 遵循 PEP 8 编码规范
- 添加必要的文档注释
- 确保向后兼容性
- 更新相关文档

## 📄 许可证

JobLens Trigger Service 遵循 JobLens 项目的整体许可证（MIT）。

## 🔗 相关链接

- [JobLens 主项目](https://github.com/nowzycc/JobLens)
- [JobLens 文档](../README.md)
- [API 接口文档](#api-接口)

---

**JobLens Trigger Service** - 为 JobLens 提供强大的 HTTP 接口和配置管理能力 🚀