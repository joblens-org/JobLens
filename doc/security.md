# 安全模型与部署基线

本文写给部署和运维 JobLens 的人，说明它假设的信任边界、已经实现的安全控制、
上线前必须设置的项，以及仍然存在的已知边界。

适用版本：`develop` 0.3.7 之后的安全加固提交（`feature/security-hardening-*`）。

---

## 1. 威胁模型

JobLens 通常部署在**多用户登录节点**上，以 root 运行，并开启 eBPF 采集。假设：

- 任意能 SSH 登录的普通用户都是不可信输入源；
- 任意能访问服务端口的网络用户都是不可信输入源；
- 能更新制品但不是 root 的开发者，可能提供被篡改的 RPM/依赖；
- etcd、Lua 规则、配置、公共盘上的制品都可能被篡改。

**不假设**：普通用户能读取 root-only 文件、能写 `/etc/JobLens` 或 `/var/JobLens`。

---

## 2. 信任边界

| 区域 | 路径 | 要求 |
|---|---|---|
| root 执行区 | `/usr/bin/JobLens`、`/usr/lib64/joblens/bpf_obj/`、`/usr/lib/joblens/trigger-venv/` | root 属主，普通用户只读 |
| 配置 | `/etc/JobLens/`、`/etc/JobLens/trigger/` | 目录 0750，文件 0640，root 属主 |
| 凭据 | `/etc/JobLens/secrets.env` | 0600 root，由 systemd `EnvironmentFile` 加载 |
| 运行数据 | `/var/JobLens/`、`/var/lib/joblens/` | 0700/0750，root 属主 |
| 控制面 | Trigger `127.0.0.1:7592` | 默认只监听回环；除健康/指标外都要求 Bearer token |
| 数据面 | Elasticsearch / Kafka | TLS + 最小权限账号；凭据不落明文配置 |
| 本地 RPC | `/var/JobLens/rpc.sock` | `UMask=0077` 下创建，root-only |
| 规则目录 | `/var/JobLens/rules/`（或配置的 `rules_path`） | 0750 root，规则名白名单 |

---

## 3. 已实现的控制

### 3.1 Trigger 控制面

- 默认监听 `127.0.0.1:7592`（`JOBLENS_TRIGGER_BIND` 可覆盖）。
- `trigger/api/auth.py`：除 `/`、`/metrics`、`/trigger/health`、`/joblens/healthy`
  外全部要求 `Authorization: Bearer <token>` 或 `X-JobLens-Token`。
- 令牌来自 `JOBLENS_TRIGGER_TOKEN`；**未配置时保护端点返回 503**，不会放行。
- `JOBLENS_DEBUG=1` 只有在监听回环地址时才允许；否则拒绝启动。
- 配置变更触发的 core 重启带 10 秒去抖（`JOBLENS_RESTART_DEBOUNCE_SECONDS`），
  并记录来源地址。
- 5xx 响应统一为 `{"error":"internal error","request_id":...}`，细节只进日志。

### 3.2 Lua 规则沙箱

规则只能使用 `base`（去掉 `dofile/loadfile/load/loadstring/require/collectgarbage`）、
`string`、`table`、`math` 与自定义 `print`。

- **没有** `io`、`package`、`os`、`debug`，因此没有文件读写、原生库加载、进程执行。
- 单条规则单次执行有指令预算（200 万条）与内存增量预算（32 MB），
  加载期与执行期都受 `lua_sethook` 约束。
- 规则文件上限 256 KB，且必须位于规则目录内。
- 规则目录被收紧为 0750；世界可写时记录 ERROR。

### 3.3 etcd

- 连接默认要求 TLS：`JOBLENS_ETCD_CA_CERT` 等；证书对不完整或文件不存在直接拒绝。
- 认证：`JOBLENS_ETCD_USER` / `JOBLENS_ETCD_PASSWORD`。
- 只有 `rule_manager` 或 `config_manager` 启用时才建立 etcd 连接；连接失败降级为本地模式。
- 过渡期允许明文时需显式 `JOBLENS_ETCD_TLS_REQUIRED=off`，并记录 WARNING。

### 3.4 文件与权限

- unified RPM：配置 0640，`/etc/JobLens*` 0750，`/var/JobLens*` 0700。
- 两个 systemd unit 均设置 `UMask=0077`，因此 `rpc.sock`、FIFO、LevelDB、日志
  天然只允许 root 访问；`LimitCORE=0`、`NoNewPrivileges`、`PrivateTmp`、
  `RestrictSUIDSGID`。
- `%post` 会收紧旧版本遗留的宽松权限（`%config(noreplace)` 不会自动改权限）。
- `FileWriter` 拒绝符号链接目标；以 root 运行时拒绝世界可写父目录。
- IO 采集器默认不输出文件路径（`collect_file_paths: false`）。

### 3.5 依赖与构建

- `trigger/requirements.lock` 固定全部传递依赖与 sha256；
  打包使用 `pip install --require-hashes`。
- `scripts/install-deps.sh` 对 date/sol2/bpftool 下载物做 sha256 校验，
  未登记版本默认失败。

### 3.6 RPC

- 单请求上限 8 MiB；超过 4 KiB 的请求会继续排空已到达数据，不再静默截断。
- 发送失败不会把负值累加进 `size_t`；慢客户端会超时并断开。
- 作业清单读路径走注册表快照，不在无锁状态下遍历。

---

## 4. 上线前必做

1. 在 `/etc/JobLens/secrets.env` 写入至少：
   ```sh
   JOBLENS_TRIGGER_TOKEN=<随机 32+ 字节>
   # ES 凭据（如使用）
   ES_USER=...
   ES_PASSWORD=...
   # etcd（如启用）
   JOBLENS_ETCD_CA_CERT=/etc/JobLens/pki/ca.pem
   JOBLENS_ETCD_USER=...
   JOBLENS_ETCD_PASSWORD=...
   ```
   并 `chown root:root`、`chmod 0600`。
2. 核心配置里用占位符引用：
   ```yaml
   es_writer_config:
     user: "{{ES_USER}}"
     passwd: "{{ES_PASSWORD}}"
   ```
3. 确认默认监听与防火墙：
   ```sh
   ss -lntp | grep 7592          # 期望 127.0.0.1:7592
   ```
4. 轮换历史上出现在明文配置或仓库中的口令/令牌。
5. 需要远程访问 Trigger 时，用 SSH 隧道而不是改回 `0.0.0.0`：
   ```sh
   ssh -L 7592:127.0.0.1:7592 user@node
   ```

---

## 5. 验证清单

```sh
# 未授权写操作必须被拒
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:7592/joblens/job

# 普通用户不能读配置/数据
sudo -u nobody cat /etc/JobLens/config.yaml          # 期望 Permission denied
sudo -u nobody ls /var/JobLens                       # 期望 Permission denied

# RPC socket / FIFO 只允许 root
ss -xl | grep rpc.sock
find /var/JobLens -maxdepth 2 -perm -o+r -ls         # 期望无输出

# etcd 明文被拒（启用 etcd 时）
journalctl -u joblens-trigger | grep -i 'etcd'
```

---

## 6. 已知边界

- **SSH 会话追踪**不在当前开发分支；相关发现逻辑、ringbuf 丢弃计数、内核兼容性
  仍需要在 SSH 分支上单独评审。
- RPC 仍是“裸 JSON + 4 字节无长度前缀”的过渡协议；长度前缀/线程池属于后续改造，
  当前只保证大请求不截断、发送失败不静默。
- systemd `CapabilityBoundingSet` 尚未收窄：core 需要 BPF 与读他人 `/proc/<pid>/fd`
  的能力，必须在目标内核上逐项验证后再加。
- Elasticsearch 侧的保留期、索引生命周期与访问控制属于运维配置，不在本仓库。
- `%config(noreplace)` 会保留旧配置：升级后请在启动日志确认新开关是否生效。

---

## 7. 升级注意（0.3.7 → 安全加固版）

- **默认监听从 `0.0.0.0` 改为 `127.0.0.1`**：远程调用会断，需要隧道或显式覆盖。
- **鉴权默认开启**：旧脚本调用写接口会收到 401/403；先配置 token。
- **IO 文件路径默认关闭**：需要文件级排障时显式 `collect_file_paths: true`。
- **5xx 响应不再包含内部错误文本**：排障改用 `request_id` + 日志。
- 配置文件权限会被 `%post` 收敛为 0640；如果 Trigger 以后改为非 root 运行，
  需要把它加入 `joblens` 组并相应调整。
