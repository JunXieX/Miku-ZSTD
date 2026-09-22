# Miku-ZSTD

Replaces Minecraft's vanilla zlib with Zstandard: player-facing traffic drops to about 1/8, with no server-side gameplay changes. A client mod is required.

用 Zstandard 替代 Minecraft 原版 zlib：玩家流量可降至约 1/8，且不改动服务端游戏逻辑。需要客户端安装配套模组。

---

## Declaration
**声明**

This is an original plugin for the MikuMC server. It is released free of charge for public use. The MikuMC server and the author JunXieX hold the copyright. This project is NOT open source — please note.

本项目为 MikuMC 服务器原创插件，公开给大众免费使用，MikuMC 服务器与作者 JunXieX 享有项目著作权，本项目非开源项目，请注意。

MikuMC plugin community group: **1105054380**

MikuMC 系列插件交流群：1105054380

---

## Why it matters
**为什么值得用**

Vanilla Minecraft compresses with zlib and frames every packet separately — frame headers, entropy tables and window information get re-sent over and over. Small packets, which are the majority of real traffic, end up larger than the raw data after compression, so they can only be stored uncompressed, wasting both CPU and bandwidth.

原版 Minecraft 用 zlib 压缩，且每个包单独成帧——帧头、熵表、窗口信息被反复发送。小包（占真实流量的大多数）压缩后甚至比原始数据更大，只能全部直存，白白浪费 CPU 与带宽。

This project does three things:

本方案做三件事：

| Optimization | Measured gain | What it does |
|---|---|---|
| Multi-packet framing (batching) | **-33.7%** | Merges several packets into one frame, amortizing the overhead |
| Slimmer frame header | **-10.7%** | Drops zstd's 4-byte magic plus contentSize / dictID |
| Dictionary training | small packets **-23.6%** | Trained automatically from real traffic; gains concentrate on small packets |
| **Total** | **-38.3%** | Against a "one frame per packet + full header" baseline |

| 优化 | 实测收益 | 说明 |
|---|---|---|
| 一帧多包（批处理） | **-33.7%** | 时间窗内的多个包合成一帧再压缩，开销被摊薄 |
| 帧头精简 | **-10.7%** | 去掉 zstd 的 4 字节 magic 与帧头里的 contentSize / dictID |
| 字典训练 | 小包 **-23.6%** | 从真实流量采样自动训练，收益集中在小包（正是原版最吃亏的地方） |
| **合计** | **-38.3%** | 相对「每包一帧 + 完整帧头」的基线 |

### Measured on a live server
**真实服务器实测**

A Velocity-CTD production instance, with the client mod and the server plugin both installed:

某 Velocity-CTD 生产实例（客户端 mod + 服务端插件）：

```text
Server outbound   31.85 MB  ->  3.92 MB               -87.7%
Client inbound    join burst 15.1 MB/s -> 1.8 MB/s     12.2%
                  steady     16.7 KB/s -> 10.8 KB/s    64.4%
```

```text
服务端出站   31.85 MB  ->  3.92 MB              省 87.7%
客户端接收   进服突发 15.1 MB/s -> 1.8 MB/s     12.2%
             稳态     16.7 KB/s -> 10.8 KB/s    64.4%
```

The join phase (chunks and registry data) compresses extremely well. In steady state small packets dominate, and that is where the dictionary pays off.

进服阶段（区块与注册表数据）压缩率极高；稳态以小包为主，是字典收益的主战场。

---

## Components and deployment
**组件与部署形态**

The project ships three deployable artifacts. The client mod is mandatory; pick one server-side artifact depending on your setup, or install both.

本项目包含三个可部署产物，客户端模组是必需的，服务端按你的架构二选一（或两处都装）：

| Artifact | Where it goes | When you need it |
|---|---|---|
| `Miku-ZSTD-<version>-Velocity.jar` | `plugins/` on a Velocity proxy | Players join through a Velocity proxy (the main production path) |
| `Miku-ZSTD-<version>-Paper.jar` | `plugins/` on a Paper server | Players connect directly to a Paper server |
| `Miku-ZSTD-<version>-Fabric.jar` | `.minecraft/mods/` on the client | Required in every setup |

| 产物 | 部署位置 | 何时需要 |
|---|---|---|
| `Miku-ZSTD-<版本>-Velocity.jar` | Velocity 代理的 `plugins/` | 玩家经 Velocity 代理进服（生产主路径） |
| `Miku-ZSTD-<版本>-Paper.jar` | Paper 子服的 `plugins/` | 玩家直连子服、不经过代理的场景 |
| `Miku-ZSTD-<版本>-Fabric.jar` | 客户端 `.minecraft/mods/` | 所有场景都需要 |

Typical combinations:

典型组合：

- Proxy setup: Velocity plugin + client mod. Optimization happens on the proxy-to-client leg
    - 代理架构：Velocity 插件 + 客户端模组。带宽优化发生在「代理 ↔ 客户端」这一段
- Direct setup: Paper plugin + client mod
    - 直连架构：Paper 插件 + 客户端模组
- Mixed: installing on both Velocity and Paper works too — each optimizes its own leg
    - 混合：Velocity + Paper 都装也可以，各自只优化自己那一段

> All three parts must be the same version. They handshake on a protocol version; on mismatch they fall back to vanilla zlib automatically, so you can still join, just without compression. If only one side activates, the connection will drop.

> 三端的版本必须一致。它们通过协议版本号握手，版本不一致时会自动回落原版 zlib（能正常进服，只是没有压缩优化）；若两端一侧生效、另一侧未生效则会断连。

---

## Requirements
**运行要求**

| | Requirement |
|---|---|
| Proxy | Velocity 4.x |
| Server | Paper 26.2+ |
| Client | Fabric, Minecraft 26.2+ |
| Java | 25+ |

| | 要求 |
|---|---|
| 代理 | Velocity 4.x |
| 服务端 | Paper 26.2+ |
| 客户端 | Fabric，Minecraft 26.2+ |
| Java | 25+ |

Paper extra requirement: `network-compression-threshold` in `server.properties` must be 0 or higher, and 256 is recommended. This plugin replaces rather than stacks on top of vanilla compression, so there is no double CPU cost. If that value is set to -1 (vanilla compression disabled), the plugin disables itself and logs a warning; joining is not affected.

Paper 端额外要求：`server.properties` 的 `network-compression-threshold` 必须 ≥ 0（推荐 256）。本插件是替换而非叠加原版压缩，因此不会带来双份 CPU 开销。若该值设为 -1（关闭原版压缩），插件会因缺少切换信号而自动停用并打 WARN —— 不影响正常进服。

---

## Installation
**安装**

### Server side
**服务端**

1. Put the matching jar into `plugins/`
    - 把对应的 jar 放入 `plugins/` 目录
2. Restart the server. A config file with full comments is generated on first run:
    - 重启服务端。首次运行会自动生成配置文件（自带中文注释）：
    - Velocity: `plugins/Miku-ZSTD/config.yml`
        - Velocity：`plugins/Miku-ZSTD/config.yml`
    - Paper: `plugins/Miku-ZSTD/config.yml`
        - Paper：`plugins/Miku-ZSTD/config.yml`
3. Confirm this line in the log, which means everything works:
    - 确认日志出现这一行，说明已正常工作：

```text
[Zstd] Zstd transport activated (dict=true)          <- Velocity
[Zstd] zstd transport activated on Paper             <- Paper
```

> Only one jar of this plugin may exist in `plugins/`. With several jars sharing the same id, only one of them is loaded, which produces "I changed the config but nothing happened" — a very time-consuming issue to track down.

> `plugins/` 下只能有一个本插件的 jar。存在多个同 id 的 jar 时只会加载其中一个，会造成「改了配置却没生效」——这个坑排查起来非常费时。

### Client side
**客户端**

Drop `Miku-ZSTD-<version>-Fabric.jar` into `.minecraft/mods/`. Press F8 in game to toggle the stats HUD, which shows uplink and downlink rates and compression ratio in real time.

把 `Miku-ZSTD-<版本>-Fabric.jar` 放入 `.minecraft/mods/`。进入游戏后按 F8 可切换统计 HUD，实时显示上下行的压缩前后速率与压缩率。

First launch creates `Miku-ZSTD/config.yml` in your game directory.

首次启动会生成 `Miku-ZSTD/config.yml`（游戏目录下）。

---

## How it works
**工作原理**

Protocol version v4, identical across Velocity, Paper and Fabric.

协议版本 v4（Velocity / Paper / Fabric 三端的协议版本常量一致）。

```text
Client --handshake (marker "\0ZSTD\0" appended to hostname)--> Server
       <-- zstd:negotiate (protocol version + 2x dictId)--
       -- answer (0=ready / 1=need dict / 2=stay vanilla)--> 
       <-- zstd:dict (only when the client answered 1)--
       -- answer (0 / 2) -->
       <-- SetCompression (must be uncompressed)--
       === both sides swap pipelines; everything goes zstd from here ===
```

1. The client appends a `\0ZSTD\0` marker to the hostname in the handshake packet
    - 客户端在握手包主机名末尾附加 `\0ZSTD\0` 标记
2. The server sends `zstd:negotiate` before the first outbound packet, carrying only the protocol version and two dictionary ids (a few dozen bytes)
    - 服务端在首个出站包前发出 `zstd:negotiate`，只带协议版本与两个字典 id（几十字节）
3. The client answers from its local cache: `0 = ready` / `1 = need dictionary` / `2 = stay vanilla`. Only on `1` does the server push the dictionary bytes via `zstd:dict`; the client verifies the CRC and answers `0` or `2`
    - 客户端按本地缓存回 `0 = 就绪` / `1 = 需要字典` / `2 = 保持原版`；仅当回 1 时服务端才单独推一次 `zstd:dict` 下发字典字节，客户端校验 CRC 后回 `0` 或 `2`
4. Version mismatch or a failed negotiation means both sides fall back to vanilla zlib together; the connection is never dropped
    - 版本不匹配或协商失败，双方一致回落原版 zlib，不会断连
5. After vanilla emits `SetCompression` (it must go out uncompressed, otherwise the client has not switched yet and parsing fails), the server enters a hold window and withholds pending packets
    - 原版发出 `SetCompression`（必须未压缩发出，否则客户端尚未切换就会解析失败）后，服务端进入 hold 窗口，扣住后续待发包
6. Both sides replace `compress` / `decompress` with the zstd implementation, then release the held packets over the now-frozen pipeline
    - 双端各自把 `compress` / `decompress` 替换为 zstd 实现，再按定型后的编码路径释放扣住的包

> Why the dictionary became on-demand in v4: v3 inlined the dictionary bytes into `negotiate`, so every connection ate those hundreds of KB, whether or not the client supported zstd or already had the dictionary cached. Splitting it in two makes reconnects and non-zstd clients zero-dictionary-traffic.

> 为什么字典在 v4 改成按需推送：v3 把字典字节内联进 negotiate，于是所有连接都要吃这几百 KB，无论客户端是否支持 zstd、本地是否已有缓存。拆成两段后，重连（缓存命中）与非 zstd 客户端都是零字典流量。

### Frame format (protocol v4)
**帧格式（协议 v4）**

```text
Server -> Client   [varint bodyLen?][varint rawSize][zstd(payload) | payload]
Client -> Server   [varint rawSize][zstd(payload) | payload]
payload = [varint pktLen][pkt] [varint pktLen][pkt] ...   <- one or more packets
```

- `rawSize == 0` means stored uncompressed, and the payload is then the raw content. Otherwise it is a magicless zstd frame
    - `rawSize == 0` 表示未压缩直存，此时 payload 就是原始内容；否则是 magicless zstd 帧
- Multi-packet framing: packets inside the batching window share one frame, amortizing header and entropy-table cost
    - 一帧多包：批处理窗口内的包合帧，帧头与熵表开销被摊薄
- Magicless zstd frame: saves the 4-byte magic and omits contentSize and dictID, since the size comes from `rawSize` and the dictionary is loaded explicitly by both sides
    - magicless zstd 帧：省 4 字节 magic，且不写 contentSize 与 dictID（解压尺寸由 `rawSize` 提供，字典由双方显式加载）
- `bodyLen` is written by Velocity only, because its pipeline has no standalone frame-encoder. On the client side the prepender handles it. This is deliberate: a missing or duplicated length field breaks decoding
    - `bodyLen` 只由 Velocity 写（4.x 管线无独立 frame-encoder，编码器自带外层长度）；客户端侧由 prepender 负责。这是刻意设计，缺层或双层都会导致对端解码失败
- Small batches get stored instead of compressed, because compression would inflate them: under 48 B without a dictionary, under 24 B with one
    - 小包（无字典 <48B、有字典 <24B）压缩后反而膨胀，直接走直存路径

### Negotiation payloads
**协商载荷**

| Message | Payload |
|---|---|
| `zstd:negotiate` | `[int protocolVersion][long encDictId][long decDictId][byte flags]` |
| `zstd:dict` | `[byte flags]` + up to two `[int crc32][int len][bytes]` |
| `answer` | `[varint encStatus][varint decStatus]` |

| 报文 | 载荷 |
|---|---|
| `zstd:negotiate` | `[int 协议版本][long encDictId][long decDictId][byte flags]` |
| `zstd:dict` | `[byte flags]` + 最多两组 `[int crc32][int len][bytes]` |
| `answer` | `[varint encStatus][varint decStatus]` |

The flags byte uses bit0 for "has an outbound dictionary" and bit1 for "has an inbound dictionary".

flags 字节：bit0 表示有压缩方向字典，bit1 表示有解压方向字典。

> Status codes must be identical on all three sides: `0 = ready` / `1 = need dictionary` / `2 = stay vanilla`. The server falls back to vanilla zlib strictly on `2`, so any client branch returning `2` must also abandon activation. Otherwise you get a "server zlib, client zstd" combination that is guaranteed to disconnect.

> 状态码语义三端必须完全一致：`0 = 就绪` / `1 = 需要字典` / `2 = 保持原版`。服务端严格按 `2` 回落原版 zlib，因此客户端任何返回 `2` 的分支都必须同时放弃激活 zstd，否则会形成「服务端 zlib、客户端 zstd」的必断连组合。

The dictionary is trained automatically from real traffic, with no manual configuration. Once trained it is pushed to clients and persisted across restarts.

字典是从真实流量自动训练的，无需手工配置；训练完成后自动推送给客户端，并跨重启持久化。

---

## Configuration
**配置**

The generated config file carries full comments. These are the options people touch most often:

配置文件自带完整中文注释，这里只列最常动的几项：

| Key | Default | Notes |
|---|---|---|
| `compression.level` | 3 | zstd level 1-22. 3 is enough: on small packets higher levels are actually worse (a 64 B packet compresses to 45.3% at L3 but only 57.8% at L9), and L9 costs 8 to 12 times more CPU. Raise to 6 only if large packets dominate and you have CPU to spare |
| `compression.window_log` | 20 | Sliding window as 2^N bytes. Do not raise it blindly: 17, 20 and 23 give identical ratios, but memory is per-connection and grows exponentially (23 is about 16 MB per connection, 20 is about 2 MB) |
| `compression.batch_window_ms` | 1 | Batching window. Packets written in the same tick are tens of microseconds apart, so 1 ms is enough to merge them. Never set 0, which disables batching and loses the bandwidth gain |
| `compression.skip_compress_below_bytes` | 48 | Without a dictionary, batches below this are stored, since 32 B and under compresses to 102-113% |
| `compression.skip_compress_below_bytes_with_dict` | 24 | With a dictionary. Do not share one value with the line above, or you lose the small-packet gain |
| `trainer.adoption_threshold` | 0.01 | Improvement required to replace a dictionary. Do not raise above 0.03: dictionary gains are only 1-3% when weighted by total bytes, and a higher threshold means the dictionary is never adopted |
| `logging.debug` | false | Verbose per-packet diagnostics plus a bandwidth profile by packet-size band, one line per 60 s. Enable only when troubleshooting |
| `display.hud_enabled` | false | Client only: the HUD default state. F8 toggles it at runtime, and that toggle is not persisted |

| 键 | 默认 | 说明 |
|---|---|---|
| `compression.level` | 3 | 压缩等级 1-22。默认 3 就够：实测小包上高等级反而更差（64B 单包 L3 压到 45.3%，L9 只有 57.8%），而 L9 耗时是 L3 的 8~12 倍。大包占比高且 CPU 富余时可调到 6 |
| `compression.window_log` | 20 | 滑动窗口 2^N 字节。不要盲目调大：实测 17、20、23 压缩率完全相同，但内存是每连接一份且指数增长（23 约 16MB/连接，20 约 2MB） |
| `compression.batch_window_ms` | 1 | 批处理窗口。同一 tick 内的包间隔只有几十微秒，1ms 足够合并。不要设 0，那等于关闭批处理，带宽收益消失 |
| `compression.skip_compress_below_bytes` | 48 | 无字典时低于此值的批次直接直存（实测 32B 及以下压缩后膨胀到 102~113%） |
| `compression.skip_compress_below_bytes_with_dict` | 24 | 有字典时的阈值。不要与上一项共用同一个值，否则会白丢小包收益 |
| `trainer.adoption_threshold` | 0.01 | 替换字典所需的改进率。不要调到 0.03 以上：字典收益按总字节加权通常只有 1~3%，阈值过高会让字典永远无法被采纳 |
| `logging.debug` | false | 开启后输出逐包诊断与按包长分档的带宽剖析（每 60s 一行），仅在排查时开启 |
| `display.hud_enabled` | false | 仅客户端：HUD 默认开关。游戏内 F8 可临时切换，该切换不落盘 |

The config file is written once on first run and never overwritten. Delete it to regenerate the defaults.

配置文件只在首次运行时生成，之后不会被覆盖；删除即可重新生成默认值。

### When does the dictionary take effect?
**字典什么时候会生效？**

The dictionary needs enough samples to train: 2000 samples by default, or a forced round after the 10-minute fallback timeout. Until then compression still works, just without the dictionary gain. Once it trains, the log shows that a new dictionary was adopted.

字典需要累积样本才能训练出来：默认 2000 个样本，或 10 分钟兜底强制触发一轮。在此之前压缩照常工作，只是没有字典增益。训练成功后日志会出现「采纳新字典」。

Use `/mikuzstd status` to check the current sample count and dictionary id.

可用 `/mikuzstd status` 查看当前样本数与字典 ID。

---

## Commands
**命令**

Availability differs by module: the Velocity side has the full set; the Paper side has only `/mikuzstd`, `status`, `top` and `bar`; the client mod has no command at all (press F8 to toggle the HUD).

| Command | Description |
|---|---|
| `/mikuzstd` | Same as `/mikuzstd status` |
| `/mikuzstd status` | Compression settings, player count, dictionary state, compression thread pool, decompress buffer |
| `/mikuzstd top` | Per-connection compression ranking by traffic, to find out who is dragging |
| `/mikuzstd bar` | Toggle the live BossBar, which refreshes every second |
| `/mikuzstd reload` | Hot-reload the config file (Velocity only) |
| `/mikuzstd train` | Trigger a training round once enough samples have been collected (Velocity only) |
| `/mikuzstd train force` | Trigger a training round regardless of the sample threshold (Velocity only) |

各端可用的子命令不同：Velocity 端是全集；Paper 端只有 `/mikuzstd`、`status`、`top`、`bar`；客户端模组没有任何命令（按 F8 开关 HUD）。

| 命令 | 功能 |
|---|---|
| `/mikuzstd` | 等同 `/mikuzstd status` |
| `/mikuzstd status` | 压缩参数、在线人数、字典训练状态、压缩线程池、解压缓冲 |
| `/mikuzstd top` | 各连接的压缩统计排行（按流量降序，用来定位是谁在拖后腿） |
| `/mikuzstd bar` | 开关 BossBar 实时监控（每秒刷新，显示使用人数与压缩率） |
| `/mikuzstd reload` | 热重载配置文件（仅 Velocity 端） |
| `/mikuzstd train` | 样本足够时触发一轮字典训练（仅 Velocity 端） |
| `/mikuzstd train force` | 忽略样本门槛，强制触发一轮训练（仅 Velocity 端） |

> `reload` only swaps the config object: `level`, `window_log` and `threads` apply only to connections created afterwards (the compression thread pool is sized at startup, so `threads` needs a proxy restart). `logging.debug` and `bossbar.format` take effect immediately.

> `reload` 只替换配置：`level`、`window_log`、`threads` 只对之后新建立的连接生效（压缩线程池在启动时就定下来了，改 `threads` 需要重启代理）；`logging.debug` 与 `bossbar.format` 立即生效。

Read-only subcommands (`status`, `top`) are open to everyone, because Velocity has no built-in permission system. `bar`, `reload` and `train` require `mikuzstd.command` — the Velocity side also accepts `zstd.command`. Note that `bar` is not merely a view: the monitor is single-owner and toggling it also starts or stops the traffic counters, so it is gated as well. On the Paper side the same node is declared in `paper-plugin.yml` and defaults to op.

只读子命令（`status`、`top`）对所有人开放，因为 Velocity 自身没有内置权限系统。`bar`、`reload`、`train` 需要 `mikuzstd.command`——Velocity 端同时兼容 `zstd.command`。注意 `bar` 不只是一个视图：监控是单人持有的，且开关它会同时启停流量统计，所以同样加了权限门槛。Paper 端在 `paper-plugin.yml` 里声明了同一权限节点，默认仅 OP。

---

## Compatibility
**兼容性**

- Vanilla clients: safe. A client without the `\0ZSTD\0` marker uses vanilla zlib and is never kicked
    - 原版客户端：安全。未带 `\0ZSTD\0` 标记的客户端完全走原版 zlib，不受影响、不会踢人
- ViaVersion: compatible; handlers are injected at the right positions around ViaVersion's
    - ViaVersion：兼容；关键操作在 ViaVersion 处理器前后正确注入
- Other plugins that append hostname markers, such as ServerSwitcher: compatible, the markers do not interfere with each other
    - 其他追加主机名标记的插件（如 ServerSwitcher）：兼容，各自标记互不干扰
- Krypton, the client-side compression optimization mod: NOT compatible, do not install both. The Fabric side already includes client-side compression optimization, and the two would fight over the same compression pipeline, overwriting each other's behavior
    - Krypton（客户端侧压缩优化模组）：不兼容，请勿同时安装。本模组的 Fabric 端已内置客户端压缩优化，两者会争抢同一条压缩管线，同时安装会导致压缩行为互相覆盖
- Multiple plugin jars with the same id: not supported, see the installation notes
    - 多个同 id 插件 jar：不兼容，见上方安装说明

---

## Troubleshooting
**排障**

### Check that both sides activated
**先确认双端都生效**

Activation on one side only always disconnects. Check both logs:

只有单侧生效必然断连。两端都要看日志：

```text
Server: [Zstd] Zstd transport activated (dict=true)
Client: [Zstd] Client zstd transport activated
```

### Reading the logs
**日志解读**

With `logging.debug: true`, a normal sequence looks like this:

开启 `logging.debug: true` 后，正常流程的日志顺序是：

```text
[Zstd] Sniffer detected Zstd client on <ip>          <- modded client detected
[Zstd] Sent zstd:negotiate txId=... encId=... decId=...
[Zstd] Negotiate response received: enc=0 dec=0      <- client answered
[Zstd] SetCompression detected - deferring ...       <- entering the activation window
[Zstd] Zstd transport activated (dict=true)          <- activation succeeded
```

| Log line | Meaning |
|---|---|
| `releasing N packet(s) held during activation window` | Normal. Packets held during the activation window were re-sent over the correct encoding path |
| `skipping zstd (vanilla fallback)` | Negotiation failed; both sides fall back to vanilla zlib, no disconnect |
| `Protocol mismatch reported by client` | The two sides run different protocol versions; upgrade both |
| Bandwidth profile line | The 60-second bandwidth report, split by packet-size band |

| 日志 | 含义 |
|---|---|
| `releasing N packet(s) held during activation window` | 正常。激活窗口内扣住的包已按正确编码路径补发 |
| `skipping zstd (vanilla fallback)` | 协商失败，双方一致回落原版 zlib，不会断连 |
| `Protocol mismatch reported by client` | 两端协议版本不一致，需同时升级两端 |
| 带宽剖析行 | 每 60s 一行的带宽报告，按包长分档 |

### Common issues
**常见问题**

Q: Cannot join, or the connection drops immediately.

Q：进不去服务器，或一进就断连？

Almost always activation on one side only. Verify that both logs show `activated`, and that both sides run the same version.

绝大多数是只有单侧生效。检查两端日志是否都有 `activated`，以及两端版本是否一致。

Q: Paper says the anchors never appeared.

Q：Paper 端提示「anchors 始终未出现」？

Check that `network-compression-threshold` in `server.properties` is 0 or higher. With -1 this plugin cannot work.

检查 `server.properties` 的 `network-compression-threshold` 是否 ≥ 0。设为 -1 时本插件不可用。

Q: The compression ratio looks bad.

Q：压缩率看起来很差？

First check `/mikuzstd status` to see whether a dictionary has been trained, indicated by a non-zero dictId. A small-packet ratio near 100% means the dictionary is not ready yet. Then look at the bandwidth profile to see which packet-size band dominates your traffic.

先看 `/mikuzstd status` 里字典是否已训练出来（dictId 非 0）。小包压缩率接近 100% 说明字典还没出来。再看带宽剖析，确认字节主要花在哪个包长区间。

Q: CPU usage went up.

Q：CPU 占用变高？

Look at the compression thread pool line in `/mikuzstd status`. If the queue depth stays above 0, compression cannot keep up, so lower `level` or add threads instead of tuning the batching window.

看 `/mikuzstd status` 的压缩线程池一行。若「队列」持续大于 0，说明压缩跟不上流量，此时应降低 `level` 或增加线程，而不是继续调批处理窗口。
