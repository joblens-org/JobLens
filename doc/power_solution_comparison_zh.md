# IHEP 功耗方案与 WLCG 生态对比

## 方案定位

IHEP 功耗方案由两个互补组件构成：

| 组件 | 定位 | 运行模式 |
|------|------|---------|
| **power_bench** | 整机功耗基准验证 + RAPL/IPMI 双源校准 | 一次性 / 周期性离线 |
| **PowerCollector** (JobLens) | eBPF 纳秒级 per-job 能耗归因引擎 | 持续性在线采集 |

## 与 WLCG 现有方案的对比

```
                        WLCG systemd    WLCG Prom    Slurm Plugin    IHEP PowerCollector
                        ────────────    ────────    ────────────    ───────────────────
测量粒度                  整机           整机           per-job        per-job + per-PID
数据源                    RAPL+IPMI      RAPL+IPMI     RAPL(MSR)     RAPL + eBPF + cpufreq
归因算法                  无             无            E_end−E_start  频率加权CPU时间占比
短作业支持(<3min)          ✓             ✓             ✓             ✓✓✓ (ns精度)
频率感知                   ✗             ✗             ✗             ✓ (per-core cpufreq)
per-process可视化          ✗             ✗             ✗             ✓ (cpu_pct + energy)
多调度器                   N/A           N/A           Slurm only    HTCondor + Slurm
独立校准工具               ✗             ✗             ✗             ✓ (power_bench)
Prometheus输出             ✗             ✓             ✗             ✓
HS23/Watt                  ✓             ✓             ✗             ✓
PUE修正                    ✗             ✗             ✗             ✓
CO₂ 估算                   ✗             ✗             ✗             ✓
部署复杂度                 systemd timer Prometheus    Slurm编译      RPM一键安装
```

## 核心差异化优势

### 1. eBPF 纳秒级 per-CPU 追踪 (独有)

WLCG 现有方案全部依赖 RAPL 计数器的 start/end 差值，只能得到 job 级别的总能耗。

IHEP PowerCollector 通过 `tp_btf/sched_switch` 内核钩子，**实时追踪每个 (tgid, pid) 在每个 CPU 核心上的纳秒级运行时间**。这意味着：

- 知道 job 的每个 worker 进程在哪个核上跑了多久
- 能区分同一个 job 内部不同 PID 的能耗贡献
- 精确处理进程在核心间迁移（自动追踪，无需额外逻辑）
- 短作业精度不依赖测量窗口长度

### 2. 频率感知归因 (独有)

公式中引入 `freq(cpu)` 因子：

```
E_job = ΔE_pkg × Σ_job Σ_cpu (runtime_ns × freq_mhz) / (Δt × Σ_cpu freq_mhz)
```

同一种 CPU 利用率（如 100%），在不同频率下的实际功耗差异可达 50%+（2.0GHz vs 3.2GHz 约差 60% 动态功耗）。不使用频率感知的归因方案会产生系统性误差。

### 3. 独立校准验证工具链 (独有)

power_bench 在部署 PowerCollector 前先运行，提供：

- RAPL 读数稳定性验证 (σ<1W 确认)
- IPMI-RAPL 静态开销测定 (Kavanagh 2019 模型)
- Per-core 功耗增量基准
- 物理约束过滤 (IPMI≥PKG) 和离群检测 (PKG>500W)

PowerCollector 可加载 power_bench 的 `summary.json` 作为校准参考，运行时自动验证当前读数是否偏离基准 >30%，并在偏离时发出 WARN 日志。

### 4. 双调度器原生支持

同时支持 HTCondor (通过 eBPF `trace_condor_starter` 钩子自动发现) 和 Slurm (通过 `trace_slurm_stepd` 钩子)。WLCG 站点通常同时运行多种调度器，单一大仓方案降低运维复杂度。

## 推荐部署组合

```
┌──────────────────────────────────────────────────────────────────┐
│  推荐 Site 部署架构                                               │
│                                                                  │
│  Layer 1 (基础设施):                                              │
│    HEPscore + systemd/prometheus collector                       │
│    → HS23/Watt 整机指标 → WLCG 官方上报                          │
│                                                                  │
│  Layer 2 (可选增强):                                              │
│    IHEP power_bench                                              │
│    → 周期性 RAPL/IPMI 校准                                       │
│    → 验证 Layer 1 数据可信度                                      │
│    → 输出 summary.json 供 Layer 3 引用                            │
│                                                                  │
│  Layer 3 (精细归因):                                              │
│    IHEP PowerCollector (JobLens)                                 │
│    → per-job per-PID 能耗归因                                    │
│    → 频率感知精确分配                                             │
│    → VO 级别计费 / 碳排放报告                                      │
│    → Prometheus / ES / Kafka 多后端输出                            │
└──────────────────────────────────────────────────────────────────┘
```

## PowerCollector 配置参考

### 最小配置

```yaml
power_collector_config:
  freq: 0.5
  auto_start: true
  use_writers: [file_writer]
```

### 完整配置 (含 WLCG 指标 + 校准引用)

```yaml
power_collector_config:
  freq: 0.5
  auto_start: true
  use_writers: [file_writer, PrometheusExporterWriter]

  # ── WLCG 标准指标 ──
  pue: 1.5                          # Power Usage Effectiveness
  carbon_intensity_g_per_kwh: 500   # 电网碳强度 (g CO₂/kWh)
  hs23_score: 1234.5                # HS23 基准分数

  # ── power_bench 校准引用 ──
  calibration_ref:
    bench_summary: /path/to/power_bench/summary.json
```

### FileWriter 纯文本输出示例

```text
PowerCollector interval_s=1 delta_rapl_j=61.3 avg_power_w=61.3 system_overhead_j=16
PowerCollector job job_id=1 native_job_id=123.0 energy_j=45.3 power_watt=45.3 ipmi_power_watt=0
PowerCollector pid job_id=1 pid=12345 energy_j=27.6 cpu_pct=200
```

## 相关文件

| 文件 | 说明 |
|------|------|
| `power_collect/power_bench.sh` | 整机功耗基准测试脚本 |
| `power_collect/config.ini` | power_bench 配置 |
| `src/ebpf/power_collect.bpf.c` | eBPF sched_switch 内核钩子 |
| `src/collector/power_collector.cpp` | PowerCollector 用户态实现 |
| `include/collector/power_collector.hpp` | PowerCollector 类声明 |
| `include/ebpf/power_collect.h` | BPF 共享数据结构 |
