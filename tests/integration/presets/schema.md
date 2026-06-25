# 预设 YAML Schema 定义

> 集成测试预设文件的 YAML schema 文档。`run_preset.sh` 使用此 schema 验证预设文件。

## 一、总体结构

```yaml
# ── 必填顶层字段 ──
name: <string>              # 预设名称，见 §2
topology: <map>             # 节点拓扑定义，见 §3
network: <map>              # 网络配置，见 §4
schedulers: <map>           # 调度器配置，见 §5
joblens: <map>              # JobLens 配置，见 §6
tests: <map>                # 测试配置，见 §7
```

未出现在上表中的任何顶层字段（如 `box_version`、`description` 等）为可选字段，**不做校验**。

## 二、`name` — 预设名称

| 属性 | 值 |
|------|-----|
| **类型** | `string` |
| **必填** | 是 |
| **格式** | `^[a-z][a-z0-9_-]*$`（小写字母开头，仅小写字母、数字、连字符、下划线） |

**校验规则**：
- 非空字符串
- 必须以小写字母开头
- 仅含小写字母 `[a-z]`、数字 `[0-9]`、连字符 `-`、下划线 `_`
- 不含大写字母、空格、中文、特殊符号

**示例**：
```yaml
name: alm9-default          # ✅ 合法
name: my_preset-v2          # ✅ 合法
name: Bad Preset!           # ❌ 含空格和特殊符号
name: 中文预设               # ❌ 含中文
```

## 三、`topology` — 节点拓扑

| 属性 | 值 |
|------|-----|
| **类型** | `map` |
| **必填** | 是 |

**校验规则**：
- 必须包含 `controller` 节点
- 必须包含 `worker` 节点
- 每个节点 key 必须匹配 `^[a-z0-9_-]+$`（纯小写）
- 每个节点为 object（可空或包含可选属性如 `box`/`hostname`/`ip`/`cpus`/`memory`）
- 节点属性不做校验（可选字段）

**示例**：
```yaml
topology:
  controller:               # ✅ 合法
    box: almalinux/9
    hostname: controller
    ip: 192.168.56.10
    cpus: 2
    memory: 2048
  worker:                   # ✅ 合法
    box: almalinux/9
    hostname: worker
    ip: 192.168.56.20
    cpus: 2
    memory: 3072
```

**错误示例**：
```yaml
topology:
  Controller: {}            # ❌ key 含大写 (必须是 controller)
  worker: {}
# ── 缺少 worker 节点 ──

topology:
  controller: {}
  worker@node: {}           # ❌ key 含 @ 非法字符
```

## 四、`network` — 网络配置

| 属性 | 值 |
|------|-----|
| **类型** | `map` |
| **必填** | 是 |

### 4.1 `network.subnet`

| 属性 | 值 |
|------|-----|
| **类型** | `string` |
| **必填** | 是 |
| **格式** | 合法 IPv4 CIDR（如 `192.168.56.0/24`） |

**校验规则**：
- 必须是 `x.x.x.x/n` 格式
- IP 部分每段为 0–255
- 子网掩码为 0–32

**示例**：
```yaml
network:
  subnet: 192.168.56.0/24   # ✅ 合法
```

**错误示例**：
```yaml
network:
  subnet: not-a-cidr        # ❌ 不是 CIDR 格式
  subnet: 192.168.56.0/33   # ❌ 子网掩码超出范围
  subnet: 999.999.999.0/24  # ❌ IP 段超出范围
```

### 4.2 其他 `network` 字段

`network` 下未列出的字段（如 `gateway`、`dns` 等）为可选字段，**不做校验**。

## 五、`schedulers` — 调度器配置

| 属性 | 值 |
|------|-----|
| **类型** | `map` |
| **必填** | 是 |

### 5.1 `schedulers.htcondor`

| 属性 | 值 |
|------|-----|
| **类型** | `map` |
| **必填** | 是 |

| 子字段 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `enabled` | `boolean` | 是 | 是否启用 HTCondor |

### 5.2 `schedulers.slurm`

| 属性 | 值 |
|------|-----|
| **类型** | `map` |
| **必填** | 是 |

| 子字段 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `enabled` | `boolean` | 是 | 是否启用 Slurm |

**校验规则**：
- `schedulers.htcondor.enabled` 必须是 `true` 或 `false`
- `schedulers.slurm.enabled` 必须是 `true` 或 `false`
- 任一 `enabled` 为 `true` 即表示启用该调度器

**示例**：
```yaml
schedulers:
  htcondor:
    enabled: true           # ✅ 合法
  slurm:
    enabled: false          # ✅ 合法
```

**错误示例**：
```yaml
schedulers:
  htcondor:
    enabled: "yes"          # ❌ 不是 boolean
  slurm:
    enabled: 1              # ❌ 不是 boolean
```

## 六、`joblens` — JobLens 配置

| 属性 | 值 |
|------|-----|
| **类型** | `map` |
| **必填** | 是 |

### 6.1 `joblens.core_config`

| 属性 | 值 |
|------|-----|
| **类型** | `string` |
| **必填** | 是 |
| **格式** | 有效文件路径（引用路径，不作存在性检查） |

### 6.2 `joblens.trigger_config`

| 属性 | 值 |
|------|-----|
| **类型** | `string` |
| **必填** | 是 |
| **格式** | 有效文件路径（引用路径，不作存在性检查） |

**校验规则**：
- 必须是字符串类型
- 非空字符串
- 路径格式不作强制要求（接受相对路径和绝对路径）

**示例**：
```yaml
joblens:
  core_config: /vagrant/rpms/joblens-core-config.yaml    # ✅ 合法
  trigger_config: /vagrant/rpms/joblens-trigger-config.yaml # ✅ 合法
```

**错误示例**：
```yaml
joblens:
  core_config:                                            # ❌ 空值
  core_config: 12345                                     # ❌ 不是字符串
```

## 七、`tests` — 测试配置

| 属性 | 值 |
|------|-----|
| **类型** | `map` |
| **必填** | 是 |

### 7.1 `tests.pytest_files`

| 属性 | 值 |
|------|-----|
| **类型** | `list[string]` |
| **必填** | 是 |
| **说明** | pytest 测试文件列表 |

**校验规则**：
- 必须是列表类型（YAML `- ` 项）
- 非空列表（至少 1 个元素）
- 每个元素为字符串类型

**示例**：
```yaml
tests:
  pytest_files:
    - test_cases/test_01_health.py
    - test_cases/test_02_htcondor_discovery.py
    - test_cases/test_03_slurm_discovery.py
```


**错误示例**：
```yaml
tests:
  pytest_files: []          # ❌ 空列表
  pytest_files:             # ❌ 缺失
```

## 八、错误消息格式

校验失败时，`run_preset.sh` 输出以下格式的错误消息到 stderr：

```
ERROR: <field_path>: <具体错误描述>
```

示例：
```
ERROR: name: invalid characters '!' at position 8 (allowed: ^[a-z][a-z0-9_-]*$)
ERROR: topology: missing required key 'controller'
ERROR: network.subnet: invalid CIDR format 'not-a-cidr'
ERROR: schedulers.htcondor.enabled: expected boolean, got string 'yes'
```

## 九、完整示例

最小有效预设文件：

```yaml
name: alm9-default
topology:
  controller:
    box: almalinux/9
    hostname: controller
    ip: 192.168.56.10
    cpus: 2
    memory: 2048
  worker:
    box: almalinux/9
    hostname: worker
    ip: 192.168.56.20
    cpus: 2
    memory: 3072
network:
  subnet: 192.168.56.0/24
schedulers:
  htcondor:
    enabled: true
  slurm:
    enabled: true
joblens:
  core_config: /vagrant/rpms/joblens-core-config.yaml
  trigger_config: /vagrant/rpms/joblens-trigger-config.yaml
tests:
  pytest_files:
    - test_cases/test_01_health.py
    - test_cases/test_02_htcondor_discovery.py
```

## 十、校验流程

```
run_preset.sh validate_preset <yaml_file>
    │
    ├─ 1. 读取 YAML 文件
    ├─ 2. 逐行解析为嵌套 dict（python3 stdlib，无外部库）
    ├─ 3. 对以下字段逐项校验：
    │   ├─ name: 字符串 + 字符白名单
    │   ├─ topology: map + controller/worker 节点 + key 格式
    │   ├─ network.subnet: CIDR 合法性
    │   ├─ schedulers.{htcondor,slurm}.enabled: boolean
    │   ├─ joblens.{core_config,trigger_config}: 非空字符串
    │   └─ tests.pytest_files: 非空列表
    ├─ 4. 任一校验失败 → stderr 输出具体字段 + exit 1
    └─ 5. 全部通过 → exit 0
```
