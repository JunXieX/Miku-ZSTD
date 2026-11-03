# Miku-ZSTD 实验室验证

作者：JunXieX | MikuMC系列插件交流群：1105054380 | 非开源项目，请勿二次分发

## 功能与当前状态

仓库提供三条 Actions 工作流：普通 Build（含 PR）、Real Client Smoke、Performance Benchmark。后两条只在可信 main 手动触发，不参与 tag 发布依赖。

**当前已经交付公共编排脚本和报告校验器；尚未交付或验收具体实验机上的游戏启动/动作/采样驱动，也没有登记自托管 runner。** 因此现在可以验证 CI、编排单测和明确的 skip 路径，尚无本轮自动化真实 Fabric/Krypton 或 10/50 客户端性能数据。6.0.4 的真实 Fabric + Krypton + FRP 登录修复已由使用者确认，这与新增自动化实验室验收是两件事。

普通 Build 中的 Python 协议 E2E 不等于真实游戏客户端。校验器通过也只证明可信驱动报告满足合同，不独立证明游戏进程、拓扑或统计方法正确。

## 环境要求

| 项目 | 要求 |
|---|---|
| runner | 隔离 Linux x64，Python 3.10+，Java 25；不执行不受信任 PR |
| 冒烟标签 | self-hosted、linux、x64、miku-zstd-real-client |
| 性能标签 | self-hosted、linux、x64、miku-zstd-performance；固定 CPU/内存/内核/NIC/拓扑 |
| 游戏 | Minecraft Java 26.2、Fabric Loader 0.19.5、匹配 Fabric API、固定 Krypton、被测 Miku-ZSTD |
| 服务 | 隔离 Paper 26.2 + FRP，明确确认 transport.useCompression=true |
| 图形/会话 | 可运行真实游戏 JVM 的现有 launcher/profile；按部署需要 Xvfb/Mesa；账号与会话留在实验机 |
| 驱动 | 实验机已有的 scenario_command 和 cleanup_command；负责真实启动、固定动作、采样和清理 |

公共 Python 编排器不负责安装游戏文件、登录账号、生成 Fabric 游戏动作或实现阶段指标采样；这些仍需实验机驱动提供。缺失指标必须失败，不得填零或用协议机器人替代。

## 安装与配置

1. 准备固定实验室和真实游戏场景驱动。驱动不得只在登录后退出：每个实例均需进入 PLAY、持续收到流量并维持完整测量区间。
2. 将配置放在 checkout 和 artifact 目录以外的权限受控路径，例如 /srv/mzstd-lab/config/smoke.json。首次执行会在 work_dir 创建独占锁和权限为 0700 的本次私有目录，保存子进程原始日志与 report.json；不会自动安装游戏。
3. 让两种测试共用同一物理实验室时使用相同 work_dir。Actions 共用并发组，编排器还会取得该目录的 Linux flock。远端节点或其他工具共享同一资源时，驱动也必须取得节点侧资源锁。
4. 验收驱动的正常、失败、超时、取消和远端清理路径后，再设置仓库变量：
   - MZSTD_SMOKE_CONFIG、MZSTD_BENCH_CONFIG：各自实验机本地配置的绝对路径；
   - MZSTD_REAL_CLIENT_RUNNER_READY=true、MZSTD_PERFORMANCE_RUNNER_READY=true：分别启用相应测试。
   - 不再需要 MZSTD_LAB_HARNESS。代码从已审核 main checkout 执行，只有具体场景配置与凭据留在实验机。

配置示例中的路径仅为占位，不代表仓库已经包含这些驱动：

~~~json
{
  "schema_version": 1,
  "scenario_command": ["/srv/mzstd-lab/bin/run-game-scenario"],
  "cleanup_command": ["/srv/mzstd-lab/bin/cleanup-game-scenario"],
  "work_dir": "/srv/mzstd-lab/private",
  "startup_timeout_seconds": 120,
  "cleanup_timeout_seconds": 30,
  "terminate_grace_seconds": 5,
  "runtime": {
    "java_executable": "/opt/java25/bin/java",
    "frps_executable": "/srv/mzstd-lab/bin/frps",
    "frpc_executable": "/srv/mzstd-lab/bin/frpc",
    "minecraft_jar": "/srv/mzstd-lab/game/26.2.jar",
    "fabric_api_jar": "/srv/mzstd-lab/mods/fabric-api.jar",
    "krypton_jar": "/srv/mzstd-lab/mods/krypton.jar",
    "display_mode": "xvfb",
    "xvfb_executable": "/usr/bin/Xvfb",
    "network_interfaces": ["eth0"]
  }
}
~~~

命令以 argv 数组直接执行，不经过 shell，不打印命令参数。不要把令牌放在 argv；让驱动读取本地会话配置。cleanup_command 必须幂等，失败时整轮失败；它负责确认本地和远端服务、游戏实例都已经退出，不能只依赖杀死本地进程组。

## 实验机预检

在启用 READY 前，可直接运行只读前置检查：

~~~sh
install -d -m 700 /srv/mzstd-lab/reports
python3 scripts/ci/runner_doctor.py \
  --config /srv/mzstd-lab/config/smoke.json \
  --report-json /srv/mzstd-lab/reports/runner-preflight.json
~~~

预检检查 Linux x64/Python、配置权限与公共编排合同、Java 主版本 25、frps/frpc 版本一致、游戏 JAR 内 version.json 的 26.2 标识、Fabric API/Krypton 的模组标识和版本、显示模式及选定 NIC 计数。它不会启动游戏、访问账号缓存、登录服务器或跑负载，也不证明模组已加载、场景驱动可用或性能有效。

诊断文件固定标记 runner_preflight_only、measurement_produced=false，失败返回非零并列出固定问题代码；仅导出白名单硬件/版本/摘要/计数，不输出配置路径、原始命令输出和会话资料。runtime 中给出的路径必须指向实验机已经安装的文件，示例不是安装脚本。existing 显示模式要求 DISPLAY；xvfb 模式需要指定 Xvfb 可执行文件；二者都不是图形渲染验收。

两个自托管 job 在运行场景之前自动执行相同预检。运行配置、原始测量报告和白名单 artifact 使用不同子目录，避免预检输出干扰编排器的新报告检查。Actions 只按文件名白名单上传 summary.json、runner-preflight.json 和 lifecycle.jsonl；预检通过不增加任何真实测试通过计数。

## 场景驱动接口

每个变体都执行一次 scenario_command，然后无条件执行 cleanup_command。驱动通过环境读取：

| 变量 | 含义 |
|---|---|
| MZSTD_MODE | smoke 或 benchmark-pair |
| MZSTD_CLIENTS | 必须维持的真实客户端人数 |
| MZSTD_DURATION_SECONDS、MZSTD_WARMUP_SECONDS | 测量与本变体预热秒数；预热不计入测量 |
| MZSTD_COMMIT_SHA | 该变体成功 Build 的 SHA |
| MZSTD_FABRIC_JAR、MZSTD_PAPER_JAR | 下载后的实际被测 JAR 绝对路径 |
| MZSTD_FABRIC_SHA256、MZSTD_PAPER_SHA256 | 被测文件摘要，驱动需核对实际安装文件 |
| MZSTD_FRP_COMPRESSION | 固定为 true，仍须核查实际 FRP 配置与运行行为 |
| MZSTD_RUN_NONCE、MZSTD_SESSION_ID | 本次 workflow 与本变体身份；报告必须原样回显 |
| MZSTD_REPORT_PATH | 仅供本次场景写入的私有 JSON 文件路径 |

退出码为 0 仍不足以通过：必须产生本次新报告、回显本次 nonce/session/SHA/JAR 摘要，并满足下述合同。不得拿历史结果改字段回填。

每份场景 report.json 必须包含：
- schema_version=1、status=passed、client_runtime=minecraft_client；commit_sha、run_nonce、session_id；artifact_sha256 为 fabric/paper 摘要对象。
- frp.use_compression=true；client 中 actual_minecraft_client、fabric_loaded、krypton_loaded、miku_zstd_loaded、login_success 均为 true。
- 顶层 clients 为本变体的实例列表；每个 id 是唯一、不含账号信息的固定标签，joined/play_traffic_received=true、disconnected=false、active_seconds 不短于本变体测量时间。若还提交 client.clients，两者必须完全一致。
- metrics 含 duration_seconds、raw_bytes、compressed_wire_bytes、nic_rx_bytes、nic_tx_bytes、server_cpu_seconds、server_rss_bytes。raw/wire/RSS 为正；NIC 合计为正。计数须为同一测量区间的增量，RSS 须说明是峰值或何种统计口径。
- 基准另需 rtt_sample_count 正整数、rtt_p50_ms/rtt_p95_ms/rtt_p99_ms/rtt_p999_ms 单调递增、pool_queue_wait_p99_ms、event_loop_wait_p99_ms。RTT 需来自实际游戏端单调时钟的发出→收到回包时间；不可用服务端延迟列冒充。队列阶段尚未有可靠采样时保持实验未验收，勿填零冒充。

所有基准变体独立提交上述完整报告。编排器随机选择第一轮顺序、随后每轮严格反转；相同拓扑、相同负载轨迹、预热规则不可随版本变化。汇总的顶层延迟/计数取最后 candidate，不是所有轮合并分位；顶层 duration_seconds 是全部变体测量时间之和。分析应读取 paired_rounds 的各轮数据，不能直接把顶层分位当 A/B 总结。

## 运行与结果

在 GitHub Actions 页面选择 main：
- Real Client Smoke：指定同一 main SHA 的成功 Build run id、30–600 秒观测时间。
- Performance Benchmark：指定两个不同 SHA 的成功 main Build；candidate 必须等于 dispatch SHA；选择 10/50 人、每变体 5/10 分钟、5/7 轮，每变体每轮另预热 2 分钟。

现有 Build artifact 保留 1 天，需先生成新鲜 baseline/candidate 构建，再立即调度。最大配置测量 140 分钟、预热 28 分钟；240 分钟 job 还需容纳下载、启动、清理与上传，驱动超时需适配剩余预算。

READY/config 缺失时，托管 preflight 会给出 skip 提示，自托管测试 job 为 skipped；workflow 本身为绿色不表示实机通过。READY 已开但驱动、报告或清理失败时测试失败，不会回退为通过。

## 日志与凭据

每次 job 都创建带 run_id/run_attempt 的唯一临时目录和随机 nonce。校验器重新读取实际下载 JAR 的 SHA256，并校验每轮客户端人数、持续时间与指标。Actions 只上传校验器导出的 sanitized/summary.json、独立 sanitized/runner-preflight.json 与 sanitized/lifecycle.jsonl 白名单；原始游戏/launcher/FRP 日志和会话留在实验机，不整体上传。

发现 credential 字段时拒收整个报告；未知附加字段不输出到摘要或 stdout。此白名单不代替对驱动的人工验收：受信任驱动仍需确保 client id 与各数值字段不包含认证信息。私有原始日志的访问、留存与清理由实验机管理员控制。

## 编排阶段日志

公共编排器接受可选 --event-log，两个工作流会启用独立 sanitized/lifecycle.jsonl。每条记录仅含固定阶段代码、序号、相对单调时钟毫秒数、受限模式以及可选轮次/变体。内容由公共编排器构造，不复制场景驱动的 stdout、异常文本、命令参数、路径或配置。

它记录编排开始/结束/失败/取消、实验室锁、变体开始、收到报告与清理成败，失败时也保留已经产生的阶段记录。每行固定 measurement_produced=false：这是定位启动和清理问题的控制流程日志，不是客户端加载、真实登录或性能通过的证据；相对时间也不是游戏 RTT。游戏事实与性能判断仍需独立 report/summary 验收，原始游戏日志留在实验机。

日志采用0600独占新建，只允许写入指定证据根目录的 sanitized/lifecycle.jsonl；已有文件或链接目标拒绝写入，不会追加到历史日志。未指定 --event-log 的手动调用不创建阶段日志。

## 验证边界

本目录 Python 单测只使用模拟子进程/JSON 夹具验证编排与拒收规则，不计为真实游戏测试。普通三端 Java 构建与测试全部交给 Actions；本地不执行 Gradle。当前 CI 变更不修改生产 JAR、版本号或 6.0.4 发布物。
