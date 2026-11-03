# Miku-ZSTD 实验室验证

作者：JunXieX | MikuMC系列插件交流群：1105054380 | 非开源项目，请勿二次分发

本目录是真实客户端验收用的 CI 组件，与生产插件无关：不进入任何发布物，也不改变三端压缩链路。

## 当前状态

仓库有两条真实客户端相关的工作流：

- **Real Client (Hosted)**：默认路径，GitHub 托管的 `ubuntu-24.04` 上真实启动 Minecraft 26.2 客户端（Fabric Loader + Krypton + 被测 Miku-ZSTD），经开启 `transport.useCompression` 的 FRP 进入隔离 Paper 26.2 并持续 PLAY。**不需要自托管 runner、仓库私有变量或任何账号会话。**
- **Performance Benchmark**：仍为自托管模式，用于 10/50 人配对 A/B。托管 runner 是共享虚拟机，不能当固定硬件基准。

Build 里的 Python 协议客户端只验证 Paper/Velocity 的线协议状态机，**不是游戏客户端**；两条链路互不替代。

## 托管冒烟怎么跑

1. Actions → Real Client (Hosted) → Run workflow。可选输入：`duration_seconds`（30–600，默认 120）、`compression_threshold`（默认 256）。
2. 工作流自己完成：构建本提交的 Fabric/Paper 成品 → 拉取固定版本 Krypton（sha512 校验）与 FRP（sha256 校验）→ 准备客户端运行时（首次下载约 600 MB，之后命中 Actions 缓存）→ 跑公共编排器与校验器。
3. 产物只有三个白名单文件：`sanitized/summary.json`、`sanitized/runner-preflight.json`、`sanitized/lifecycle.jsonl`。

失败时另有 `real-client-diagnostics` 产物，包含本次临时目录里的驱动/服务端/客户端日志。托管 runner 是本次运行专用的临时机器，这些日志不含账号会话、也不含在私有实验机上才需要保护的资料；它只用于定位启动问题，不作为验收证据。

## 组成

| 文件 | 职责 |
|---|---|
| [mc_runtime.py](<mc_runtime.py>) | 按 Mojang 官方 version json 抓取 26.2 客户端、规则过滤后的库、资产与 log4j 客户端配置；所有下载按 sha1 校验，离线身份由固定用户名推导 |
| [run_game_scenario.py](<run_game_scenario.py>) | 场景驱动：隔离 Paper + FRP + 真实客户端，等 PLAY 后计时、采样、写报告；缺任一前置即失败 |
| [cleanup_game_scenario.py](<cleanup_game_scenario.py>) | 幂等清理：按 PID/端口与每场景环境令牌双路查找并停止进程，仍有残留则失败 |
| [build_testmod.py](<build_testmod.py>) + [testmod/](<testmod>) | 用 JDK 直接编译仅 CI 使用的观测模组（记录实际加载的模组来源、PLAY 状态、Netty 入站字节） |
| [hosted_preflight.py](<hosted_preflight.py>) | 托管 runner 只读预检：可执行文件、Java 主版本、剩余磁盘；固定标记 `measurement_produced=false` |
| [lab_harness.py](<lab_harness.py>) | 公共编排：配置校验、work_dir 独占锁、超时/取消、轮次顺序、summary 生成；不含任何游戏动作 |
| [validate_real_client_result.py](<validate_real_client_result.py>) | 独立验收：人数、模组标志、流量与 RTT、时长、nonce/摘要，导出白名单摘要 |
| [runner_doctor.py](<runner_doctor.py>) | 自托管实验机只读预检（配置权限、Java/FRP、游戏与模组元数据、显示/NIC） |
| [test_*.py](<test_mc_runtime.py>) | 启动参数、规则匹配、字节格式化、编排与校验的模拟回归 |

## 场景驱动接口

公共编排器不实现游戏启动，它执行配置里的 `scenario_command`，并把下列环境变量交给驱动（前缀 `MZSTD_`，除 `MODE` 外同时提供无前缀别名）：

| 变量 | 含义 |
|---|---|
| MODE | smoke 或 benchmark-pair |
| CLIENTS | 必须维持的真实客户端人数 |
| DURATION_SECONDS、WARMUP_SECONDS | 测量与本变体预热秒数；预热不计入测量 |
| COMMIT_SHA | 该变体成功构建的提交 SHA |
| FABRIC_JAR、PAPER_JAR | 下载后的实际被测 JAR 绝对路径 |
| FABRIC_SHA256、PAPER_SHA256 | 被测文件摘要，驱动必须核对 |
| FRP_COMPRESSION | 固定为 true，驱动仍须核查实际配置 |
| RUN_NONCE、SESSION_ID | 本次 workflow 与本变体身份，报告必须原样回显 |
| REPORT_PATH | 仅供本次场景写入的私有 JSON 路径 |

托管驱动另外读取（工作流已设置，本地手动运行需自备）：

| 变量 | 含义 |
|---|---|
| KRYPTON_JAR、KRYPTON_SHA512 | 固定版本 Krypton 与期望摘要 |
| MC_CACHE | 客户端运行时缓存目录（客户端 jar、库、资产） |
| COMPRESSION_THRESHOLD | Paper `network-compression-threshold` |
| STARTUP_TIMEOUT_SECONDS | 服务端/客户端启动上限 |
| NIC | 计入 NIC 字节的网络接口名（默认 lo） |
| FRPS_EXECUTABLE、FRPC_EXECUTABLE、JAVA | 可执行文件；缺省走 PATH |

报告合同与自托管模式完全一致：`schema_version=1`、`status=passed`、`client_runtime=minecraft_client`、`frp.use_compression=true`、每个实例 `joined/play_traffic_received` 为真且 `active_seconds` 覆盖测量区间、`metrics` 七项基础指标为正。

## 指标口径与已知限制

- **raw / wire**：取插件 `/mikuzstd top` 在测量窗口前后的累计值差值。该输出是十进制格式化文本（`1.2 MB`），分辨率约 0.05 MB，**不是逐字节精确**；基准档需要阶段采样，见 [DEVELOPMENT.md](../DEVELOPMENT.md) 的优化候选。
- **observed_client_inbound_bytes**：CI 观测模组在 Netty 入站管线最前端挂了只读计数器，按字节累计服务端→客户端方向，不 retain/release、不改写消息。它与插件计数互为交叉校验，不混入报告的基础指标。
- **NIC**：游戏流量全程走 127.0.0.1（客户端 ↔ frps ↔ frpc ↔ Paper），因此默认统计 `lo`；接入真实网卡时用 `NIC` 指定。
- **分段耗时（pool_queue_wait / event_loop_wait 等）**：来自插件 `/mikuzstd latency` 的四段直方图——排队（提交→worker 开始）、执行（worker 压缩本身）、回调（worker 结束→event loop 回调执行）、写出（首帧写出→全部真实 write promise 完成）。驱动在测量窗口开始前先 `/mikuzstd latency reset`，所以分位点只覆盖本次窗口；桶宽约为下界的 12.5%，分位点取桶上界（只会偏保守）。
- **RTT**：游戏客户端自己发 `/mikuzstd ping <token>`，服务端只回一行 `pong <token>`，客户端用自己的单调时钟结算。它包含服务端命令处理时间，**不是** TCP 往返，也不是玩家列表里服务端算出的延迟；样本带 wall 毫秒戳，驱动按测量窗口筛选后用最近秩法取分位。
- 采样速率受服务端节流约束：每条聊天/命令按 +20 计数、每 tick −1，持续速率超过约 1 条/秒会在十几秒后被 `Kicked for spamming`。因此往返采样固定 0.5 条/秒（60 秒窗口约 30 个样本）。提高速率必须同时改服务端 `commands.spam-exclusions`，否则客户端会被踢，冒烟会如实失败。
- **CPU / RSS**：Paper 进程 `utime+stime` 窗口差值，与进程 `VmHWM` 峰值；不含客户端与 FRP 的开销。
- **active_seconds**：观测模组单调时钟累计的 PLAY 时长；期间采样间隔 250 ms。
- 缺 PLAY、缺 zstd 激活（客户端未打印下行接管日志）、加载的 Miku-ZSTD 摘要与被测成品不一致、窗口内进程退出或掉线，一律判失败，不补零、不缩窗口、不改人数。

## 自托管模式（保留）

自托管仍可用于固定硬件的 10/50 人基准：配置放在 checkout 之外的受控路径，`scenario_command` / `cleanup_command` 指向实验机已安装的绝对可执行文件，再由 `MZSTD_PERFORMANCE_RUNNER_READY` 与 `MZSTD_BENCH_CONFIG` 启用。两者共用同一 `work_dir` 时才共享物理实验室锁。驱动仍需自行启动图形会话、Paper 与 FRP，并按上表回显身份与实测指标。

## 验证边界

- 托管冒烟证明"真实游戏客户端能登录真实服务端并持续收包"，**不证明** 10/50 人容量、带宽节省百分比或尾延迟改善。
- 托管 runner 的 CPU 是共享的，LLVMPipe 软件渲染也只够把客户端跑起来；不能把其 FPS/CPU 数字当性能结论。
- 编排器的 stage 日志（`lifecycle.jsonl`）固定 `measurement_produced=false`：它是排障用的控制流记录，不是游戏或性能证据。
