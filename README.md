# JobLens

High-performance distributed job monitoring and performance analysis agent, designed for HTC/HPC environments.

[中文](README_zh.md)

## Quick Install

```bash
git clone https://github.com/joblens-org/JobLens.git
cd JobLens

mkdir build && cd build
cmake .. -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
ninja
```

## Usage

```bash
# Start in service mode
./JobLens -m service -c config/config.yaml

# Show version
./JobLens -v

# Show help
./JobLens -h

# Health check
curl http://localhost:7592/joblens/healthy
```

## Documentation

| Document | Description |
|----------|-------------|
| [API Documentation](doc/api_documentation.md) | Trigger RESTful API reference |
| [Configuration Manual](doc/configuration.md) | Complete configuration parameters |
| [Development Guide](doc/develop_guide.md) | Branch structure, commit conventions, etc. |
| [Minimal Deployment Kit](doc/joblens_simplest_deployable_suite.md) | Production deployment guide |

## Project Structure

```
JobLens/
├── config/                     # Configuration files
├── include/                    # Headers (collector/writer/core/ebpf)
├── src/                        # Source implementation
├── scripts/                    # Scripts (install/deploy/CLI)
├── trigger/                    # Trigger service (Flask)
├── doc/                        # Documentation
└── thirdparty/                 # Reserved for third-party sources (currently empty)
```

## License

[Apache-2.0](LICENSE)
