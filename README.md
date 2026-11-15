# Miku-ZSTD

作者：JunXieX　|　MikuMC系列插件交流群：1105054380　|　非开源项目，请勿二次分发

面向 Velocity 代理与 Paper / Folia 服务端的下行压缩插件，配套 Fabric 客户端模组使用。上行保留 Minecraft 原版处理；原版客户端按原版方式连接。

## 功能特性

- 对服务端到客户端的下行流量使用连续 Zstandard 压缩，同一连接保留压缩历史，待发数据随上游 flush 输出。
- 支持代理入口与玩家直连两种部署形态；Paper 与 Folia 使用同一份 Paper 产物。
- 提供状态、连接流量排行与 BossBar 监控；客户端按 F8 查看下行速率与压缩率。
- 6.1.1 起调试日志**强制开启并写成文件**（服务端/代理为 `plugins/Miku-ZSTD/logs/zstd-debug.log`，客户端为 `<游戏目录>/Miku-ZSTD/logs/zstd-debug.log`），没有关闭开关，控制台口径不变；玩家进不去服时把该文件发出来即可定位。
- 6.1.1 起**卡住会话自动取证**：玩家"名字进去了、人没进来"时，服务端在判定卡住的当下把本端调试文件尾部写成现场，并主动向该客户端索取它自己的日志尾部，落成 `plugins/Miku-ZSTD/logs/stall/<玩家>-<时间>/`（`server.log` + `client.log.gz` + `session.txt`）。不需要玩家知道文件在哪，也不需要他放弃重连后才想起来取证。
- 代理上 `/mikuzstd bar` 对后端同名命令的遮蔽：具备代理权限时在代理执行，否则已连接后端的玩家交由后端鉴权。
- 关闭后端原版压缩时，仍保留 `/mikuzstd bar` 的执行与补全。
- 6.1.1 把协商应答等待从写死的 3 秒（Paper）/ 2 秒（Velocity）改为可配置的 `login.answer_timeout_ms`（默认 10 秒），超时改为<b>先回落原版</b>而不是直接断开连接；客户端补上压缩管线看门狗，未观察到 SetCompression 时按原版应答，不再无限期等待。协商与激活的逐连接诊断统一写进调试文件（强制开启、不可关闭，单文件 8 MiB、保留 2 份历史），控制台只留启用/停用、激活成功与真正的告警。
- 6.1.0 新增分段耗时（`latency`）、往返探测（`ping`）与精确字节计数（`traffic`）三个诊断命令；`batch_max_packets` 默认提高到 256，新增默认关闭的 `batch_max_bytes` 字节预算。
- 6.0.4 修复 Fabric + Krypton 在协商应答早于 SetCompression 时发出未封装登录包的问题；使用 FRP 转发仍可保持 `transport.useCompression=true`。
- 6.0.2 起不再设置小包直存阈值，所有接管后的下行包都进入连续压缩流；旧协商协议的客户端协商不匹配时保持原版。

压缩收益随区块、注册表、聊天与小包占比变化，请以本服监控数据判断效果。

## 环境要求

| 组件 | 当前基线 | 说明 |
|---|---|---|
| Java | 25 | 三端均以 Java 25 构建 |
| 客户端 | Minecraft 26.2 + Fabric | 当前构建使用 Fabric Loader 0.19.5、Fabric API 0.160.0+26.2 |
| 代理 | Velocity 4.x | 当前真实服务器登录回归使用 4.3.1 |
| 直连服务端 | Paper / Folia 26.2 | 当前构建基于 Paper 26.2；真实服务器登录回归使用 Paper 26.2 |
| 配套文件 | Miku-ZSTD 服务端插件与 Fabric 模组 | 建议服务端与 Fabric 客户端统一使用 6.1.1；旧协议客户端协商不匹配时保持原版 |

以上是当前构建与回归基线，不代表其他游戏或代理版本已经逐一验证。

## 安装

### 选择部署位置

| 场景 | 安装位置 | 监控范围 |
|---|---|---|
| 玩家通过 Velocity 进服 | 代理安装 `Miku-ZSTD-6.1.1-Velocity.jar`，玩家安装 Fabric 产物 | 代理实例处理的玩家下行连接 |
| 玩家直连 Paper / Folia | 服务端安装 `Miku-ZSTD-6.1.1-Paper.jar`，玩家安装 Fabric 产物 | 该服务端实例处理的下行连接 |
| 代理与后端均安装插件 | 按各自用途安装，分别配置与授权 | 两端统计独立，后端面板不会自动汇总代理的玩家出口 |

代理架构只需代理插件与客户端模组即可覆盖玩家侧链路；后端 Paper 插件属于独立安装项。

### 服务端与代理

1. 将对应产物放进运行实例的 `plugins/`，同一实例只保留一份本插件。
2. Paper / Folia 将 `server.properties` 的 `network-compression-threshold` 设为非负值，建议从 `256` 开始。Velocity 使用其自身的压缩阈值配置。
3. 重启对应实例。首次启动生成 `plugins/Miku-ZSTD/config.yml`；代理与后端各有自己的配置文件。经 FRP 转发时无需关闭 `transport.useCompression`，请让面向玩家的服务端/代理与客户端统一使用 6.1.1。
4. 使用配套客户端登录，并运行 `/mikuzstd status` 核对当前实例状态。需要图形监控时，再按权限说明执行 `/mikuzstd bar`。

Paper / Folia 将原版压缩阈值设为 `-1` 时，只关闭本端压缩接管，命令与 BossBar 监控仍可使用，玩家连接继续按原版处理。后端监控不会自动包含代理出口流量。

### 客户端

1. 安装与 Minecraft 26.2 对应的 Fabric Loader、Fabric API。
2. 将 `Miku-ZSTD-6.1.1-Fabric.jar` 放进该游戏实例的 `mods/`。
3. 首次启动生成 `<游戏目录>/Miku-ZSTD/config.yml`，同时开始写调试日志 `<游戏目录>/Miku-ZSTD/logs/zstd-debug.log`。进入游戏后按 **F8** 切换下行统计 HUD，按键切换只在本次运行期间生效。
4. 连不上服务器（一直停在连接中）时，退出游戏后把上面那个 `zstd-debug.log` 发出来即可——协商、看门狗、切换确认的每一步都在里面，不需要再去翻 `latest.log`。

旧版配置按既有规则尝试迁移，已有文件不会被默认模板覆盖。Velocity 通常使用上文的大写目录；旧注入目录迁移失败时会记日志并继续使用旧目录，请以迁移日志确认实际配置位置。

## 配置说明

### 服务端与代理配置

两端使用以下相同键名，路径为各自实例的 `plugins/Miku-ZSTD/config.yml`：

```yaml
compression:
  level: 3
  window_log: 20
  threads: 0
  batch_window_ms: 50
  batch_max_packets: 256
  batch_max_bytes: 0

login:
  answer_timeout_ms: 10000
  late_answer_window_ms: 120000

bossbar:
  format: "&a%players% &7人在用 &8| &f%raw% &7→ &a%wire% &8| 压缩率 &e%ratio%"

logging:
  file: "logs/zstd-debug.log"
  max_file_mb: 8
  keep_files: 2
  console_debug: false
  profiler: false

diagnostics:
  enabled: true
  stall_detect_ms: 15000
  pull_client_log: true
  preflight: true
  max_kb: 256
  keep_bundles: 20
```

服务端与代理的调试文件都在各自实例的 `plugins/Miku-ZSTD/logs/` 下；客户端在 `<游戏目录>/Miku-ZSTD/logs/` 下。

| 配置项 | 默认值 | 作用 |
|---|---|---|
| `compression.level` | `3` | 压缩等级，范围 1–22；更高等级会增加压缩端计算量 |
| `compression.window_log` | `20` | 压缩窗口指数，范围 10–27；调大同时增加每连接内存开销 |
| `compression.threads` | `0` | 压缩池线程数；0 为可用核心数减 1，至少 1 个线程 |
| `compression.batch_window_ms` | `50` | 上游只写不 flush 时的兜底时间，单位毫秒；正常批次随上游 flush 发出 |
| `compression.batch_max_packets` | `256` | 每个压缩批次最多合并的包数；批次输出可能跨多个传输帧 |
| `compression.batch_max_bytes` | `0` | 每个压缩批次的原始字节预算，范围 0–16777216；`0` 为不启用。用于大小差异极大的流量：包数上限对聊天小包与区块大包代表的字节量差两个数量级 |
| `login.answer_timeout_ms` | `10000` | 等客户端协商应答的预算，范围 1000–120000 毫秒。超时后本端回落原版并照常放行登录（只影响这一轮是否走 zstd）；必须明显大于客户端模组的 2000ms 看门狗 |
| `login.late_answer_window_ms` | `120000` | 协商定案后继续吞掉迟到应答的窗口，范围 5000–600000 毫秒（Velocity 端无此项）。窗口要盖住整个登录 + 配置阶段，否则玩家在资源包下载期间停留过久会被判为意外的查询应答而断连 |
| `bossbar.format` | 示例所示 | 当前实例监控文本，支持 `&` 颜色代码 |
| `logging.file` | `logs/zstd-debug.log` | 调试文件路径，相对各自数据目录；客户端相对游戏目录，默认 `Miku-ZSTD/logs/zstd-debug.log`。6.1.1 起日志强制开启，**没有关闭开关** |
| `logging.max_file_mb` | `8` | 单文件上限，范围 1–128 MiB；超过即滚动为 `.1`、`.2` |
| `logging.keep_files` | `2` | 保留的历史份数，范围 0–10；`0` 表示只保留当前文件（写满即截断） |
| `logging.console_debug` | `false` | 同时把同一批调试行进控制台。默认关闭：控制台留给真正的告警 |
| `logging.profiler` | `false` | Velocity 带宽剖析（每 60 秒一段，写进调试文件）；Paper 端无此项 |
| `diagnostics.enabled` | `true` | 卡住会话自动取证总开关。关掉它，玩家再卡住时只有各端调试文件可查，抓不到卡住那一侧的现场 |
| `diagnostics.stall_detect_ms` | `15000` | 判定"卡住"的阈值，范围 5000–120000 毫秒；计时从协商查询**发出**那一刻算起。调小会更灵敏，也会把慢机器当卡住 |
| `diagnostics.pull_client_log` | `true` | 卡住时向客户端索取它自己的调试日志尾部。客户端 6.1.1+ 才会应答 |
| `diagnostics.preflight` | `true` | 协商成功后 3 秒先取一次"上一轮尾巴"。客户端已经卡死时本次查询读不到东西，靠它把上一次的痕迹带上来 |
| `diagnostics.max_kb` | `256` | 单次取尾部的上限（KiB，16–1024），同时是本端接受上限；解压另有 4 MiB 硬顶 |
| `diagnostics.keep_bundles` | `20` | 保留的现场组数（1–100），超出按时间删最旧 |

BossBar 占位符：`%players%` 为使用 zstd 通道的人数，`%raw%` 为原始下行带宽，`%wire%` 为压缩后带宽，`%ratio%` 为压缩后 / 原始的百分比；带宽值自带单位，空闲时压缩率显示 `--`。原始带宽包含包长度前缀与包体，线路带宽包含 Minecraft 外层长度；不包含 TCP、加密等额外开销。

**升级已有配置：** 旧项 `compression.skip_compress_below_bytes` 已不生效，无需手动删除也不会改变连续压缩行为。已有文件保留原文、注释及未知旧键，不会被重写；新模板不再生成该项。

`logging.debug` 在 6.1.1 已**移除**：调试日志改为强制开启，不再提供关闭开关——定位"卡在连接协商中"那种现场，靠的就是用户把那个文件发过来，能关掉等于把唯一的证据关掉。旧配置里留着 `debug: false` 不会报错、不会被重写、也不生效，启动时会有一行 `调试日志已强制开启（6.1.1 起不再可关闭），配置里的 logging.debug 已忽略`。想连控制台一起看，改加 `console_debug: true`。唯一写不出文件的情况是磁盘不可写，此时会打一条 WARN 并降级为不写。

缺少 `login.answer_timeout_ms` / `login.late_answer_window_ms` 时取代码内默认值（10 秒 / 120 秒），不需要手动补键。`logging` 段的 `file` / `max_file_mb` / `keep_files` 缺省即用上表默认值，同样不需要补键。

### 修改后如何生效

- **Velocity：** 有代理权限时运行 `/mikuzstd reload`。整个 `logging` 段（开关、路径、上限、份数、控制台镜像、剖析）与监控文本即时读取——换文件路径会立刻改写到新文件；等级、窗口和批处理参数需新连接；线程数需重启代理。
- **Paper / Folia：** 修改配置后重启服务端；本端命令仅有 `status`、`top`、`bar`（`status` 会打印当前调试文件路径与大小）。
- 不要通过调大兜底时间追求更高压缩率：它主要改变未主动 flush 的调用方等待时间。

### 客户端显示配置

```yaml
display:
  hud_enabled: false

logging:
  file: "Miku-ZSTD/logs/zstd-debug.log"
  max_file_mb: 8
  keep_files: 2

diagnostics:
  send_client_log: true
  max_kb: 256
```

`display.hud_enabled` 决定启动后的 HUD 默认状态。F8 仅临时切换显示，不写回配置；HUD 只显示下行，不统计原版上行。

`logging` 段决定客户端调试日志：强制开启、路径相对游戏目录，超过 `max_file_mb` 会滚动并保留 `keep_files` 份历史；没有开关可以关掉它（旧版的 `debug` 键已移除，写了不生效）。客户端没有 `console_debug` / `profiler` 两项——游戏里没有需要清屏的控制台；`latest.log` 只会多出一行指路信息。

`diagnostics.send_client_log` 决定要不要应答服务端的取证查询：服务端发现你卡在"连接协商中"时，会向你索取上面那个调试文件的**尾部**（最多 `max_kb`），只发这一份文件的内容，不含聊天记录、不含账号信息。默认开着——真卡住的时候，玩家没法自己去翻文件、更不知道该发什么，而这份尾巴正是唯一能说明问题的东西。

## 命令

| 命令 | 可用端 | 作用 |
|---|---|---|
| `/mikuzstd` 或 `/mikuzstd status` | Paper / Velocity | 查看处理该命令的实例状态 |
| `/mikuzstd top` | Paper / Velocity | 查看该实例的下行连接流量排行 |
| `/mikuzstd bar` | Paper / Velocity | 切换实例级 BossBar 监控；经过代理时按下述规则选择执行端 |
| `/mikuzstd reload` | Velocity | 重读代理配置，保持代理权限检查 |

### 经过 Velocity 时的 bar 路由

1. **具备代理节点：** 由代理本地执行，查看或切换代理监控；即使后端也安装插件，仍优先代理。
2. **没有代理节点，且玩家已连接后端：** 命令交给当前后端，由后端自己的 OP / 权限规则决定是否执行。后端需要安装对应插件。
3. **没有代理节点，且来源是控制台或玩家尚未连接后端：** 返回明确提示，不转发、不改变代理监控。控制台可用 `status` 查看文本状态。

这次转发只针对 `bar`。`status`、`top` 仍查询代理实例；`reload` 仍是代理命令，不转发为 Paper 配置重载。

每个实例保留一个实例级 BossBar 监控。再次执行 `bar` 可关闭现有监控，当前观察玩家离线后自动停止；它不是每位玩家独立的一份开关。

## 权限节点

| 执行位置 | 节点 / 身份 | 管理范围 |
|---|---|---|
| Paper / Folia 后端 | `mikuzstd.command`，默认授予 OP | 后端 `bar`；后端 `status`、`top` 无额外权限 |
| Velocity 代理 | `mikuzstd.command` 或 `zstd.command` | 代理本地 `bar` 与 `reload`，在代理权限系统中授权 |
| 客户端 | 无额外权限 | F8 下行 HUD 开关 |

**后端 OP 与代理权限彼此独立。后端 OP 不会自动获得 Velocity 的权限节点。** 想查看代理监控，应在代理侧授权；想把 `bar` 交给后端，应让该玩家不具备代理节点，由后端自行鉴权。代理权限不足时的转发并不代表直接授予后端权限。

## 常见问题

### 在子服是 OP，为什么 bar 没有显示代理监控？

OP 属于后端权限。没有代理节点且已连接后端的玩家会把 `bar` 交给后端处理；需要代理出口监控时，请授予代理节点。若后端没有安装插件或后端权限不足，请按后端提示处理。

### 代理和后端都装了，数字为什么不同？

它们统计各自实例实际处理的连接与流量，观察的链路和使用 zstd 的连接集合不同。后端面板不会自动包含代理出口的数据；经过代理执行 `status`、`top` 也仍是在查询代理。

### 调试日志在哪里？出问题要发什么？

三端各有一份，强制开启、不可关闭，都在自己的目录里：

| 端 | 路径 |
|---|---|
| Paper / Folia | `plugins/Miku-ZSTD/logs/zstd-debug.log` |
| Velocity | `plugins/Miku-ZSTD/logs/zstd-debug.log` |
| Fabric 客户端 | `<游戏目录>/Miku-ZSTD/logs/zstd-debug.log` |

玩家进不去服时，让他把客户端那一份发出来；服主再把服务端/代理的同名文件一起给出，两边的协商时间线一对就能定位是"查询没发出去""应答没回来"还是"客户端拒绝/版本不匹配"。

如果玩家卡在"连接协商中"（名字进去了、人没进来），服务端会自己动手，见下一节：多数情况下不需要再找玩家要文件。文件带硬上界（默认 8 MiB × 3），不会写爆磁盘；写过的内容不会进控制台，所以控制台不会因此变吵。

路径与上限由 `logging.file`、`logging.max_file_mb`、`logging.keep_files` 控制；`/mikuzstd status` 会显示当前实际路径与大小。没有关闭开关——只有磁盘不可写时才会降级为不写并打一条 WARN。想连控制台一起看，把 `logging.console_debug` 设为 `true`（Velocity 用 `/mikuzstd reload` 即时生效，Paper 需重启）。

### 卡住的时候，插件会自动收上来什么？

服务端（Paper 或 Velocity）发现某条连接"协商查询发出去了、人却一直没进来"，会在判定卡住的当下做两件事：

1. 把**本端**调试文件尾部与连接事实（远端地址、协商 txId、应答是否收到、是否已激活）写成一组现场，放在 `plugins/Miku-ZSTD/logs/stall/<玩家>-<时间>/`；
2. 向那条连接发一条 `zstd:diag` 登录查询，把**客户端**调试日志的尾部取回来，写进同一组的 `client.log.gz`。

一组现场里是三件套：

| 文件 | 内容 |
|---|---|
| `server.log` | 判定卡住时本端调试文件的尾部（默认 256 KiB） |
| `client.log.gz` | 客户端回传的调试日志尾部（gzip 原样，`gunzip` 后是纯文本） |
| `session.txt` | 判定模式（卡住探针 / 预取）、远端地址、协商各阶段状态、回传大小与解压后大小 |

判定阈值默认 15 秒，从协商查询**发出**那一刻算起——不是从连接建立算起，否则慢机器进服时握手多花几秒就会被误判。已经进服的连接立即退出判定，所以正常玩家不会产生目录。

**预取**是另一条路：如果客户端已经卡在"该解 zstd 却解不开"里，它连入站包都读不到，本次查询必然空手而归；但客户端日志是跨连接追加写的，所以协商成功后 3 秒会先取一次"上一轮的尾巴"。上一次卡死留下的最后几百行，就这样被带上来。

这条链路的边界要清楚：

- 只有服务端能发起（登录阶段客户端不能主动发消息），所以必须有一端装着 6.1.1+ 的插件；
- 客户端 6.1.0 及更早版本不认识这个通道，回的是"未回传"——现场里照样有 `server.log`，只是没有 `client.log.gz`；
- 客户端把 `diagnostics.send_client_log` 关掉时同理；
- 一条连接最多被问两次（预取一次、判定卡住一次），查询本身也走登录通道，不新增端口或常驻连接。

### 玩家一直卡在"连接协商中"，先查什么？

先分两类，判据在客户端日志尾部，不在服务端：

1. **客户端本地原因**（实际遇到过）：本地存档/地图被删掉后客户端仍卡在加载或保存它，主线程不还，网络处理也没跑起来。特征是客户端日志在 `Connecting to ...` 之后**再无任何一行**，模组连一行 `negotiate rx` 都写不出来。这类跟插件无关，恢复办法是清掉那份存档/整合包缓存再进服。
2. **传输层原因**：客户端在处理网络，但查询/应答在某处丢了。特征是客户端日志里有 `negotiate rx`，之后中断；服务端侧同时能看到 `Sent zstd:negotiate` 而没有 `协商应答`。

服务端**无法**自己区分这两类——它看到的都只是"没有进展"。所以判定卡住时会自动留一组现场（见上一节）：`client.log.gz` 为空且 `session.txt` 写着 `协商应答=未收到` 属第 1 类；`client.log.gz` 里有 `negotiate rx` 属第 2 类。

顺带排除两个常见误判：模组版本不一致并不构成协商失败（6.0.4 与 6.1.0 的协商代码一行未改）；插件自身的登录等待预算是 2–10 秒量级，物理上不可能把连接挂住几分钟。

### 为什么没有压缩效果？

先确认玩家安装配套 Fabric 模组、入口装的是正确产物、Paper 压缩阈值非负，再用 `status` 与 F8 检查。协议不匹配或安装条件不满足时会保持原版；接管后的小包也进入连续压缩流，空闲流量为零属于正常情况。

### 如何查看带宽剖析？

把代理配置里的 `logging.profiler` 设为 `true` 再 `/mikuzstd reload`（默认关闭：它是分析工具，不是故障诊断）。输出在调试文件里，每 60 秒一段，按压缩批次的原始流字节大小分档，不是单个游戏包长度分布。排查结束后再关掉；持续积压时先降低等级或按实际核心余量调整线程数。

### 如何停用？

停止对应实例，移除该实例的插件产物后再启动。客户端模组可保留，在没有配套协商时继续使用原版连接。
