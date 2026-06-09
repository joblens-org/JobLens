# JobLens Trigger Service

The JobLens Trigger Service is a Flask-based RESTful API service that provides HTTP interface and configuration management capabilities for the JobLens job monitoring system. It acts as the frontend gateway for the JobLens C++ core service, supporting job management, metric queries, dynamic configuration updates, and more.

## Key Features

### 1. Service Registration & Discovery
- Automatic instance registration with the service registry
- Multi-instance load balancing and service discovery
- Health checks and automatic failover

### 2. Configuration Management
- Supports local YAML config files and etcd distributed configuration
- Real-time configuration change monitoring and dynamic updates
- Configuration change callback mechanism for hot reloading

### 3. Job Management API
- Add and remove jobs
- Supports both standard jobs and HTCondor jobs
- Batch job queries and status monitoring

### 4. Metrics Export
- Prometheus-format monitoring metrics (`/metrics` endpoint)
- Real-time job performance data (CPU, memory, I/O, network, etc.)
- Performance statistics for collectors and writers

### 5. System Administration
- Service health status checks
- Remote version upgrades (with token authentication)
- RPC communication status monitoring

### 6. HTCondor Integration
- Automatic discovery and monitoring of Condor jobs
- Dynamic job addition via job slots
- Condor job status synchronization

## Quick Start

### Requirements
- Python 3.7+
- JobLens core service installed and running
- Optional: etcd v3+ (for distributed configuration management)

### Install Dependencies
```bash
cd trigger
pip install -r requirements.txt
```

### Start the Service
```bash
# Direct launch (development)
python app.py

# Launch with Gunicorn (production)
gunicorn --config gunicorn.conf.py app:app

# Use deployment script
./deploy_trigger.sh
```

### Verify the Service
```bash
# Check service health
curl http://localhost:7592/joblens/healthy

# Get Prometheus metrics
curl http://localhost:7592/metrics
```

## Configuration

### Main Config File
The Trigger service uses the JobLens main configuration file (`../config/config.yaml`). Key configuration items include:

```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock  # RPC communication socket path
  rpc_timeout: 5                          # RPC call timeout (seconds)
```

### Environment Variables
- `UPGRADE_TOKEN`: Authentication token for the upgrade endpoint (read from environment; no validation by default)

### Service Registry Configuration
- Registry address: `http://your-registry-host:8080` (configured via `REGISTRY_URL` environment variable)
- Service port: `7592`
- Auto-registration: Automatically registers with the registry on startup

## API Reference

### Health Check
```bash
GET /joblens/healthy
```
Returns the health status of the JobLens service.

### Prometheus Metrics
```bash
GET /metrics
```
Returns Prometheus-format monitoring metrics, including job status, resource usage, etc.

### Job Management

#### Add/Remove Job
```bash
POST /joblens/job
Content-Type: application/json

{
  "opt": "add",           # Operation type: "add" or "remove"
  "type": "job.common",   # Job type: "job.common" or "job.condor"
  "JobID": 12345,         # Job ID
  "JobPIDs": [6789],      # Process PID list
  "Lens": ["proc_collector", "cpumem_collector"]  # Collectors to use
}
```

#### Add Condor Job
```bash
POST /joblens/condor_job
Content-Type: application/json

{
  "opt": "add",           # Currently only "add" is supported
  "slot": "slot1",        # Condor job slot name
  "JobID": 12345,         # Job ID
  "Lens": ["proc_collector"]  # Collectors to use
}
```

#### Query Job Information
```bash
GET /joblens/jobs                    # List all jobs
GET /joblens/jobs/count              # Get total job count
GET /joblens/jobs/{job_id}           # Get details for a specific job
```

### RPC Functions

#### RPC Health Check
```bash
GET /joblens/rpc/health
```
Checks RPC server health status, returning detailed service status and latency.

#### Get Available Functions
```bash
GET /joblens/rpc/functions
```
Retrieves the list of all registered available methods on the RPC server.

### Performance Statistics

#### Collector Performance
```bash
GET /joblens/collectors/perf
```
Gets performance statistics for all collectors.

#### Writer Performance
```bash
GET /joblens/writers/perf
```
Gets performance statistics for all writers.

#### Writer Information
```bash
GET /joblens/writers/{writer_name}/info
```
Gets basic information and configuration for a specific writer.

### System Administration

#### Version Upgrade
```bash
POST /joblens/version/upgrade
Authorization: Bearer <upgrade_token>
```
Triggers a remote version upgrade. Requires a valid upgrade token.

#### Configuration Update
```bash
GET /joblens/config/update
```
Manually triggers a configuration update (invokes the configuration manager's callbacks).

## Deployment Guide

### Using the Deployment Script
```bash
# Run deployment script
./deploy_trigger.sh
```
The deployment script will:
1. Create a Python virtual environment
2. Install dependencies
3. Create a systemd service file
4. Enable and start the service

### Manual Deployment

#### 1. Create systemd Service File
Create `/etc/systemd/system/joblens-trigger.service`:
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

#### 2. Start the Service
```bash
sudo systemctl daemon-reload
sudo systemctl enable joblens-trigger
sudo systemctl start joblens-trigger
```

### Gunicorn Configuration
Gunicorn config file (`gunicorn.conf.py`) key settings:
- Bind address: `0.0.0.0:7592`
- Worker count: 1 (sync mode)
- Timeout: 120 seconds
- Log level: info

## Development Notes

### Project Structure
```
trigger/
├── app.py                 # Flask main application, contains all API routes
├── config_manager.py      # Configuration manager, supports local and etcd config
├── rpc_cilent.py          # RPC client, communicates with JobLens C++ service
├── tools.py               # Utility functions (job queries, metric formatting, etc.)
├── user_config.py         # User configuration class
├── deploy_trigger.sh      # Deployment script
├── gunicorn.conf.py       # Gunicorn configuration file
├── requirements.txt       # Python dependency list
└── README.md              # This document
```

### Core Components

#### ConfigManager
- Supports local YAML config and etcd distributed configuration
- Real-time configuration change monitoring
- Configuration change callback mechanism
- Configuration history

#### RPCClient
- Unix socket-based RPC communication
- Timeout and error handling
- Service health checks
- Function list queries

#### ServiceRegistrar
- Automatic service registration and deregistration
- Registry heartbeat maintenance
- Service instance identity management

### Extending Development

#### Adding a New API Endpoint
Add a new route function in `app.py`:
```python
@app.route('/joblens/new_endpoint', methods=['GET'])
def new_endpoint():
    # Implementation logic
    return jsonify({'status': 'ok'})
```

#### Adding a New Utility Function
Add functions in `tools.py` supporting async or sync operations.

## Troubleshooting

### Common Issues

#### 1. Service Fails to Start
- Check if the JobLens core service is running
- Verify the RPC socket path is correct
- View logs: `sudo journalctl -u joblens-trigger -f`

#### 2. API Requests Return Errors
- Confirm the service port (default 7592) is listening
- Check that request parameter format meets requirements
- Verify RPC connection is working

#### 3. Configuration Updates Not Taking Effect
- Check etcd connection status (if using etcd)
- Confirm config file path permissions
- Verify configuration change callbacks are registered

#### 4. Prometheus Metrics Are Empty
- Confirm there are jobs being monitored
- Check whether RPC calls return data
- Verify metric formatting functions

### Viewing Logs
```bash
# systemd service logs
sudo journalctl -u joblens-trigger -f

# Gunicorn access logs
tail -f /var/log/gunicorn-access.log

# Gunicorn error logs
tail -f /var/log/gunicorn-error.log
```

## Monitoring & Metrics

### Service Metrics
- `joblens_trigger_requests_total`: Total request count
- `joblens_trigger_request_duration_seconds`: Request duration
- `joblens_trigger_rpc_latency_ms`: RPC call latency

### Integrated Monitoring
The Trigger service itself can be monitored via:
1. **Prometheus**: Scrape the `/metrics` endpoint
2. **Health Checks**: Periodically call `/joblens/healthy`
3. **Log Aggregation**: Collect Gunicorn and systemd logs

## Contributing

Issues and Pull Requests are welcome to improve the Trigger service.

### Development Workflow
1. Fork the project repository
2. Create a feature branch
3. Commit code changes
4. Run basic tests
5. Create a Pull Request

### Code Standards
- Follow PEP 8 coding conventions
- Add necessary documentation comments
- Ensure backward compatibility
- Update relevant documentation

## License

The JobLens Trigger Service follows the overall JobLens project license (MIT).

## Related Links

- [JobLens Main Project](https://github.com/nowzycc/JobLens)
- [JobLens Documentation](../README.md)
- [API Reference](#api-reference)

---

**JobLens Trigger Service** - Providing powerful HTTP interface and configuration management capabilities for JobLens
