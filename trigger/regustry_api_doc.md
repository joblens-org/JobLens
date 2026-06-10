# JobLens Service Registry API Documentation

## Overview

The JobLens Service Registry is a microservice registration and discovery system built on FastAPI, providing complete service registration, health checking, heartbeat monitoring, and persistent storage functionality. All service information is stored in an etcd distributed key-value store, supporting high availability and distributed deployment scenarios.

## Basic Information

- **Base URL**: `http://<host>:8080` (default: `http://0.0.0.0:8080`)
- **Content Type**: `application/json`
- **Authentication**: No authentication required in current version
- **API Documentation**:
  - Swagger UI: `http://<host>:8080/docs`
  - ReDoc: `http://<host>:8080/redoc`

## Common Response Format

### Success Response
Success responses follow the response model defined by each endpoint. All time fields use ISO 8601 format.

### Error Response
```json
{
  "detail": "Error description"
}
```

**Common HTTP Status Codes**:
- `200 OK`: Request succeeded
- `201 Created`: Resource created successfully
- `204 No Content`: Request succeeded, no content returned
- `400 Bad Request`: Invalid request parameters
- `404 Not Found`: Resource not found
- `500 Internal Server Error`: Internal server error

## Data Models

### ServiceInfo
The complete information model after service registration.

| Field | Type | Required | Description | Example |
|------|------|------|------|------|
| `service_id` | string | Yes | Unique service ID (UUID format) | `"550e8400-e29b-41d4-a716-446655440000"` |
| `host` | string | Yes | Service host address | `"10.0.0.1"` |
| `port` | int | Yes | Service port number (1-65535) | `5000` |
| `base_url` | string | Yes | Service base URL | `"http://10.0.0.1:5000"` |
| `name` | string | No | Service name | `"joblens-api"` |
| `version` | string | No | Service version | `"1.0.0"` |
| `registered_at` | datetime | Yes | Registration time (ISO format) | `"2026-01-21T10:30:00.000000"` |
| `last_heartbeat` | datetime | Yes | Last heartbeat time (ISO format) | `"2026-01-21T10:30:00.000000"` |
| `status` | string | Yes | Service status: `healthy`, `unhealthy`, `unknown` | `"healthy"` |
| `metadata` | object | No | Additional metadata (key-value pairs) | `{"region": "us-west", "environment": "production"}` |

### RegisterRequest
The request model used when registering a new service.

| Field | Type | Required | Description | Example |
|------|------|------|------|------|
| `host` | string | Yes | Service host address | `"10.0.0.1"` |
| `port` | int | Yes | Service port number (1-65535) | `5000` |
| `name` | string | No | Service name | `"joblens-api"` |
| `version` | string | No | Service version | `"1.0.0"` |
| `metadata` | object | No | Additional metadata (key-value pairs) | `{"region": "us-west"}` |

### HealthResponse
Response model for the registry's own health status.

| Field | Type | Required | Description | Example |
|------|------|------|------|------|
| `status` | string | Yes | Registry status | `"healthy"` |
| `details` | object | No | Detailed health information | See example below |

## API Endpoint Details

### 1. Register a New Service

Register a new JobLens service with the registry.

**Endpoint**: `POST /register`

**Request Headers**:
- `Content-Type: application/json`

**Request Body** (RegisterRequest model):
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

**Success Response** (`201 Created`):
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

**Error Responses**:
- `400 Bad Request`: Request parameter validation failed
- `500 Internal Server Error`: Internal server error

### 2. Unregister a Service

Unregister a previously registered service from the registry.

**Endpoint**: `DELETE /unregister/{service_id}`

**Path Parameters**:
- `service_id` (string, required): Unique service ID

**Success Response** (`204 No Content`):
No response body

**Error Responses**:
- `404 Not Found`: Specified service ID does not exist
- `500 Internal Server Error`: Internal server error

### 3. Get All Services

Get a list of all registered services, with optional filtering for healthy services only.

**Endpoint**: `GET /services`

**Query Parameters**:
- `healthy_only` (boolean, optional): Return only healthy services, default `false`

**Success Response** (`200 OK`):
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

**Error Responses**:
- `500 Internal Server Error`: Internal server error

### 4. Get Specific Service Information

Get detailed information for a specific service by service ID.

**Endpoint**: `GET /services/{service_id}`

**Path Parameters**:
- `service_id` (string, required): Unique service ID

**Success Response** (`200 OK`):
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

**Error Responses**:
- `404 Not Found`: Specified service ID does not exist
- `500 Internal Server Error`: Internal server error

### 5. Get Registry Health Status

Return the registry's own health status, including the number of registered services, healthy services, etc.

**Endpoint**: `GET /health`

**Success Response** (`200 OK`):
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

**Error Responses**:
- `500 Internal Server Error`: Internal server error

### 6. Get Registry Statistics

Get detailed statistics for the registry, including service status distribution and registration time distribution.

**Endpoint**: `GET /stats`

**Success Response** (`200 OK`):
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

**Error Responses**:
- `500 Internal Server Error`: Internal server error

### 7. Manually Trigger Database Backup

Manually trigger a database backup (etcd snapshot). Note: snapshot functionality is not yet fully implemented in the current version.

**Endpoint**: `POST /persistence/backup`

**Success Response** (`200 OK`):
```json
{
  "status": "success",
  "message": "ETCD backup triggered (snapshot functionality not yet implemented)"
}
```

**Error Responses**:
- `500 Internal Server Error`: Internal server error

## Configuration

The registry supports configuration via the following environment variables:

| Environment Variable | Default | Description |
|----------|--------|------|
| `HEARTBEAT_INTERVAL` | `10` | Heartbeat check interval (seconds) |
| `SERVICE_TIMEOUT` | `30` | Service timeout duration (seconds) |
| `DEFAULT_HOST` | `"0.0.0.0"` | Registry listen address |
| `DEFAULT_PORT` | `8080` | Registry listen port |
| `ETCD_HOST` | `"your-etcd-host"` | etcd server address |
| `ETCD_PORT` | `2379` | etcd server port |
| `ETCD_PREFIX` | `"/joblens_registry/"` | etcd key prefix |
| `BACKUP_INTERVAL` | `0` | Backup interval (seconds), 0 disables automatic backup |

## Health Check Mechanism

The registry periodically sends health check requests to the `/joblens/healthy` endpoint of registered services:
- **Check Interval**: `HEARTBEAT_INTERVAL` seconds (default 10 seconds)
- **Timeout**: `SERVICE_TIMEOUT` seconds (default 30 seconds)
- **Status Updates**: Service status is updated to `healthy` or `unhealthy` based on health check responses

## Example Usage

### Register a Service Using curl
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

### Get Service List Using curl
```bash
# Get all services
curl http://localhost:8080/services

# Get only healthy services
curl "http://localhost:8080/services?healthy_only=true"
```

### Check Registry Health Using curl
```bash
curl http://localhost:8080/health
```

### Get Statistics Using curl
```bash
curl http://localhost:8080/stats
```

### Unregister a Service Using curl
```bash
curl -X DELETE http://localhost:8080/unregister/550e8400-e29b-41d4-a716-446655440000
```

## Notes

1. **Service Health Endpoint**: Registered services must implement the `/joblens/healthy` endpoint and return HTTP 200 to indicate health
2. **Persistence**: All service information is persistently stored in etcd and automatically restored after restart
3. **Status Updates**: Service status is automatically updated based on health check results
4. **Time Format**: All time fields use ISO 8601 format
5. **Port Range**: Port numbers must be in the range 1-65535
6. **Service Status**:
   - `unknown`: Initial state, health check not yet performed
   - `healthy`: Health check succeeded
   - `unhealthy`: Health check failed or timed out

## Version Information

- **Current Version**: v0.0.13
- **Last Updated**: 2026-06-10
- **API Compatibility**: Backward compatible

---
*This document is auto-generated based on the JobLens Service Registry code, last updated 2026-02-04*
