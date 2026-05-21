# JobLens

高性能分布式作业监控与性能分析采集端Agent，专为 HTC/HPC 环境设计。

[English](README_EN.md)

## 快速安装

```bash
git clone https://github.com/joblens-org/JobLens.git
cd JobLens

mkdir build && cd build
cmake .. && make -j$(nproc)
```

## 使用方法

```bash
# 服务模式启动
./JobLens -m service -c config/config.yaml

# 查看版本
./JobLens -v

# 查看帮助
./JobLens -h

# 健康检查
curl http://localhost:7592/joblens/healthy
```

## 文档

| 文档 | 说明 |
|------|------|
| [API 文档](doc/api_documentation_zh.md) | Trigger RESTful API 接口说明 |
| [配置手册](doc/configuration_zh.md) | 完整配置参数说明 |
| [开发指南](doc/develop_guide.md) | 分支结构、提交规范等协作约定 |
| [最简可部署套件](doc/joblens_simplest_deployable_suite.md) | 生产环境部署指南 |

## 项目结构

```
JobLens/
├── config/                     # 配置文件
├── include/                    # 头文件（collector/writer/core/ebpf）
├── src/                        # 源码实现
├── scripts/                    # 脚本工具（安装/部署/CLI）
├── test/                       # 测试文件
├── trigger/                    # 触发器服务（Flask）
├── doc/                        # 文档资源
└── thirdparty/                 # 第三方依赖
```

## 许可证

[Apache-2.0](LICENSE)
