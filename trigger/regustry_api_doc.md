# JobLens 服务注册中心 API 接口文档

## 概述

JobLens 服务注册中心是基于 FastAPI 构建的微服务注册与发现系统，提供完整的服务注册、健康检查、心跳监控和持久化存储功能。所有服务信息存储在 etcd 分布式键值存储中，支持高可用性和分布式部署场景。

## 基础信息

- **基准 URL**: `http://<host>:8080`（默认：`http://0.0.0.0:8080`）
- **内容类型**: `application/json`
- **认证**: 当前版本无需认证
- **API 文档**: 
  - Swagger UI: `http://<host>:8080/docs`
  - ReDoc: `http://<host>:8080/redoc`

## 通用响应格式

### 成功响应
成功响应遵循端点定义的响应模型，所有时间字段使用 ISO 8601 格式。

### 错误响应
```json
{
  "detail": "错误描述信息"
}
```

**常见 HTTP 状态码**:
- `200 OK`: 请求成功
- `201 Created`: 资源创建成功
- `204 No Content`: 请求成功，无返回内容
- `400 Bad Request`: 请求参数错误
- `404 Not Found`: 资源不存在
- `500 Internal Server Error`: 服务器内部错误

## 数据模型

### ServiceInfo（服务信息）
服务注册后的完整信息模型。

| 字段 | 类型 | 必填 | 描述 | 示例 |
|------|------|------|------|------|
| `service_id` | string | 是 | 服务唯一ID（UUID格式） | `"550e8400-e29b-41d4-a716-446655440000"` |
| `host` | string | 是 | 服务主机地址 | `"10.0.0.1"` |
| `port` | int | 是 | 服务端口号（1-65535） | `5000` |
| `base_url` | string | 是 | 服务基础URL | `"http://10.0.0.1:5000"` |
| `name` | string | 否 | 服务名称 | `"joblens-api"` |
| `version` | string | 否 | 服务版本 | `"1.0.0"` |
| `registered_at` | datetime | 是 | 注册时间（ISO格式） | `"2026-01-21T10:30:00.000000"` |
| `last_heartbeat` | datetime | 是 | 最后心跳时间（ISO格式） | `"2026-01-21T10:30:00.000000"` |
| `status` | string | 是 | 服务状态：`healthy`、`unhealthy`、`unknown` | `"healthy"` |
| `metadata` | object | 否 | 额外元数据（键值对） | `{"region": "us-west", "environment": "production"}` |

### RegisterRequest（注册请求）
注册新服务时使用的请求模型。

| 字段 | 类型 | 必填 | 描述 | 示例 |
|------|------|------|------|------|
| `host` | string | 是 | 服务主机地址 | `"10.0.0.1"` |
| `port` | int | 是 | 服务端口号（1-65535） | `5000` |
| `name` | string | 否 | 服务名称 | `"joblens-api"` |
| `version` | string | 否 | 服务版本 | `"1.0.0"` |
| `metadata` | object | 否 | 额外元数据（键值对） | `{"region": "us-west"}` |

### HealthResponse（健康响应）
注册中心自身健康状态的响应模型。

| 字段 | 类型 | 必填 | 描述 | 示例 |
|------|------|------|------|------|
| `status` | string | 是 | 注册中心状态 | `"healthy"` |
| `details` | object | 否 | 详细健康信息 | 见下文示例 |

## API 端点详细说明

### 1. 注册新服务

注册一个新的 JobLens 服务到注册中心。

**端点**: `POST /register`

**请求头**:
- `Content-Type: application/json`

**请求体**（RegisterRequest 模型）:
```json
{
  "host": "10.0.0.1",
  "port": 5000,
  "name": "joblens-api",
  "version": "1.0.0",
  "metadata": {
    "region": "us-west",
    "environment": "production"
  }
}
```

**成功响应**（`201 Created`）:
```json
{
  "service_id": "550e8400-e29b-41d4-a716-446655440000",
  "host": "10.0.0.1",
  "port": 5000,
  "base_url": "http://10.0.0.1:5000",
  "name": "joblens-api",
  "version": "1.0.0",
  "registered_at": "2026-01-21T10:30:00.000000",
  "last_heartbeat": "2026-01-21T10:30:00.000000",
  "status": "unknown",
  "metadata": {
    "region": "us-west",
    "environment": "production"
  }
}
```

**错误响应**:
- `400 Bad Request`: 请求参数验证失败
- `500 Internal Server Error`: 服务器内部错误

### 2. 注销服务

从注册中心注销一个已注册的服务。

**端点**: `DELETE /unregister/{service_id}`

**路径参数**:
- `service_id` (string, 必需): 服务唯一ID

**成功响应**（`204 No Content`）:
无响应体

**错误响应**:
- `404 Not Found`: 指定的服务ID不存在
- `500 Internal Server Error`: 服务器内部错误

### 3. 获取所有服务列表

获取所有已注册的服务列表，支持过滤仅返回健康服务。

**端点**: `GET /services`

**查询参数**:
- `healthy_only` (boolean, 可选): 仅返回健康的服务，默认 `false`

**成功响应**（`200 OK`）:
```json
[
  {
    "service_id": "550e8400-e29b-41d4-a716-446655440000",
    "host": "10.0.0.1",
    "port": 5000,
    "base_url": "http://10.0.0.1:5000",
    "name": "joblens-api",
    "version": "1.0.0",
    "registered_at": "2026-01-21T10:30:00.000000",
    "last_heartbeat": "2026-01-21T10:30:00.000000",
    "status": "healthy",
    "metadata": {
      "region": "us-west"
    }
  },
  {
    "service_id": "550e8400-e29b-41d4-a716-446655440001",
    "host": "10.0.0.2",
    "port": 5001,
    "base_url": "http://10.0.0.2:5001",
    "name": "joblens-worker",
    "version": "1.0.0",
    "registered_at": "2026-01-21T10:31:00.000000",
    "last_heartbeat": "2026-01-21T10:31:00.000000",
    "status": "unhealthy",
    "metadata": {
      "region": "us-west"
    }
  }
]
```

**错误响应**:
- `500 Internal Server Error`: 服务器内部错误

### 4. 获取指定服务信息

根据服务ID获取特定服务的详细信息。

**端点**: `GET /services/{service_id}`

**路径参数**:
- `service_id` (string, 必需): 服务唯一ID

**成功响应**（`200 OK`）:
```json
{
  "service_id": "550e8400-e29b-41d4-a716-446655440000",
  "host": "10.0.0.1",
  "port": 5000,
  "base_url": "http://10.0.0.1:5000",
  "name": "joblens-api",
  "version": "1.0.0",
  "registered_at": "2026-01-21T10:30:00.000000",
  "last_heartbeat": "2026-01-21T10:30:00.000000",
  "status": "healthy",
  "metadata": {
    "region": "us-west"
  }
}
```

**错误响应**:
- `404 Not Found`: 指定的服务ID不存在
- `500 Internal Server Error`: 服务器内部错误

### 5. 获取注册中心健康状态

返回注册中心自身的健康状态，包括已注册服务数量、健康服务数量等。

**端点**: `GET /health`

**成功响应**（`200 OK`）:
```json
{
  "status": "healthy",
  "details": {
    "registered_services": 5,
    "healthy_services": 4,
    "timestamp": "2026-01-21T10:30:00.000000",
    "persistence": {
      "enabled": true,
      "backend": "etcd",
      "etcd_host": "your-etcd-host",
      "etcd_port": 2379,
      "etcd_prefix": "/joblens_registry/",
      "loaded_from_db": true
    }
  }
}
```

**错误响应**:
- `500 Internal Server Error`: 服务器内部错误

### 6. 获取注册中心统计信息

获取注册中心的详细统计信息，包括服务状态分布、注册时间分布等。

**端点**: `GET /stats`

**成功响应**（`200 OK`）:
```json
{
  "total_services": 10,
  "status_distribution": {
    "healthy": 8,
    "unhealthy": 1,
    "unknown": 1
  },
  "age_distribution": {
    "less_than_1_hour": 5,
    "1_hour_to_1_day": 3,
    "more_than_1_day": 2
  },
  "heartbeat_interval": 10,
  "service_timeout": 30,
  "persistence": {
    "backend": "etcd",
    "etcd_host": "your-etcd-host",
    "etcd_port": 2379,
    "etcd_prefix": "/joblens_registry/",
    "backup_interval": 0,
    "backup_enabled": false
  },
  "timestamp": "2026-01-21T10:30:00.000000"
}
```

**错误响应**:
- `500 Internal Server Error`: 服务器内部错误

### 7. 手动触发数据库备份

手动触发数据库备份（etcd 快照）。注意：当前版本快照功能暂未完全实现。

**端点**: `POST /persistence/backup`

**成功响应**（`200 OK`）:
```json
{
  "status": "success",
  "message": "ETCD备份已触发（快照功能暂未实现）"
}
```

**错误响应**:
- `500 Internal Server Error`: 服务器内部错误

## 配置说明

注册中心支持通过环境变量配置以下参数：

| 环境变量 | 默认值 | 描述 |
|----------|--------|------|
| `HEARTBEAT_INTERVAL` | `10` | 心跳检查间隔（秒） |
| `SERVICE_TIMEOUT` | `30` | 服务超时时间（秒） |
| `DEFAULT_HOST` | `"0.0.0.0"` | 注册中心监听地址 |
| `DEFAULT_PORT` | `8080` | 注册中心监听端口 |
| `ETCD_HOST` | `"your-etcd-host"` | etcd 服务器地址 |
| `ETCD_PORT` | `2379` | etcd 服务器端口 |
| `ETCD_PREFIX` | `"/joblens_registry/"` | etcd 键前缀 |
| `BACKUP_INTERVAL` | `0` | 备份间隔（秒），0表示禁用自动备份 |

## 健康检查机制

注册中心会定期向已注册服务的 `/joblens/healthy` 端点发送健康检查请求：
- **检查间隔**: `HEARTBEAT_INTERVAL` 秒（默认10秒）
- **超时时间**: `SERVICE_TIMEOUT` 秒（默认30秒）
- **状态更新**: 根据健康检查响应更新服务状态为 `healthy` 或 `unhealthy`

## 示例调用

### 使用 curl 注册服务
```bash
curl -X POST http://localhost:8080/register \
  -H "Content-Type: application/json" \
  -d '{
    "host": "127.0.0.1",
    "port": 8000,
    "name": "test-service",
    "version": "1.0.0",
    "metadata": {"environment": "test"}
  }'
```

### 使用 curl 获取服务列表
```bash
# 获取所有服务
curl http://localhost:8080/services

# 仅获取健康服务
curl "http://localhost:8080/services?healthy_only=true"
```

### 使用 curl 检查注册中心健康状态
```bash
curl http://localhost:8080/health
```

### 使用 curl 获取统计信息
```bash
curl http://localhost:8080/stats
```

### 使用 curl 注销服务
```bash
curl -X DELETE http://localhost:8080/unregister/550e8400-e29b-41d4-a716-446655440000
```

## 注意事项

1. **服务健康端点**: 注册的服务需要实现 `/joblens/healthy` 端点，返回 HTTP 200 表示健康
2. **持久化**: 所有服务信息持久化存储在 etcd 中，重启后自动恢复
3. **状态更新**: 服务状态根据健康检查结果自动更新
4. **时间格式**: 所有时间字段使用 ISO 8601 格式
5. **端口范围**: 端口号必须在 1-65535 范围内
6. **服务状态**: 
   - `unknown`: 初始状态，尚未进行健康检查
   - `healthy`: 健康检查成功
   - `unhealthy`: 健康检查失败或超时

## 版本信息

- **当前版本**: v0.0.4
- **最后更新**: 2026-02-04
- **API 兼容性**: 向后兼容

---
*本文档基于 JobLens 服务注册中心代码自动生成，最后更新于 2026-02-04*