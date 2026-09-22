# Miku-ZSTD

## 声明

本项目为 MikuMC 服务器原创插件，公开给大众免费使用，MikuMC 服务器与作者 JunXieX 享有项目著作权，本项目非开源项目，请注意。

MikuMC 系列插件交流群：1105054380

---

用 **Zstandard** 替代 Minecraft 原版 zlib 的网络压缩方案。在不改动服务端任何游戏逻辑的前提下，
把玩家-facing 的网络流量压到原来的 **1/8 左右**。

> ⚠️ **需要客户端安装配套模组。** 这不是单纯的传输优化——它替换的是压缩协议本身，
> 所以必须服务端与客户端同时使用。**未安装模组的客户端不受任何影响**，照常走原版 zlib。

---

## 效果

原版 Minecraft 用 zlib 压缩，且**每个包单独成帧**——帧头、熵表、窗口信息被反复发送。
小包（占真实流量的大多数）压缩后甚至**比原始数据更大**，只能全部直存，白白浪费 CPU 与带宽。

本方案做三件事：

| 优化 | 实测收益 | 说明 |
|---|---|---|
| **一帧多包**（批处理） | **-33.7%** | 时间窗内的多个包合成一帧再压缩，开销被摊薄 |
| **帧头精简** | **-10.7%** | 去掉 zstd 的 4 字节 magic 与帧头里的 contentSize / dictID |
| **字典训练** | 小包 **-23.6%** | 从真实流量采样自动训练，收益集中在小包（正是原版最吃亏的地方） |
| **合计** | **-38.3%** | 相对「每包一帧 + 完整帧头」的基线 |

### 真实服务器实测

某 Velocity-CTD 生产实例（客户端 mod + 服务端插件）：

```
服务端出站   31.85 MB  →  3.92 MB              省 87.7%
客户端接收   进服突发 15.1 MB/s → 1.8 MB/s     12.2%
             稳态     16.7 KB/s → 10.8 KB/s    64.4%
```

进服阶段（区块与注册表数据）压缩率极高；稳态以小包为主，是字典收益的主战场。

---

## 组件与部署形态

本项目包含三个可部署产物，**客户端模组是必需的**，服务端按你的架构二选一（或两处都装）：

| 产物 | 部署位置 | 何时需要 |
|---|---|---|
| `Miku-ZSTD-<版本>-Velocity.jar` | Velocity 代理的 `plugins/` | 玩家经 **Velocity 代理**进服（生产主路径） |
| `Miku-ZSTD-<版本>-Paper.jar` | Paper 子服的 `plugins/` | 玩家**直连子服**、不经过代理的场景 |
| `Miku-ZSTD-<版本>-Fabric.jar` | 客户端 `.minecraft/mods/` | **所有场景都需要** |

典型组合：

- **代理架构**：Velocity 插件 + 客户端模组 。带宽优化发生在**代理 ↔ 客户端**这一段
- **直连架构**：Paper 插件 + 客户端模组
- **混合**：Velocity + Paper 都装也可以，各自只优化自己那一段

> ⚠️ **三端的版本必须一致。** 它们通过协议版本号握手，版本不一致时会**自动回落原版 zlib**
> （能正常进服，只是没有压缩优化）；若两端一侧生效、另一侧未生效则会断连。

---

## 运行要求

| | 要求 |
|---|---|
| 代理 | Velocity **4.x** |
| 服务端 | Paper **26.2+** |
| 客户端 | Fabric，Minecraft **26.2+** |
| Java | **25+** |

**Paper 端额外要求**：`server.properties` 的 `network-compression-threshold` 必须 **≥ 0**（推荐 `256`）。
本插件是**替换**而非叠加原版压缩，因此不会带来双份 CPU 开销。若该值设为 `-1`（关闭原版压缩），
插件会因缺少切换信号而**自动停用**并打 WARN——不影响正常进服。

---

## 安装

### 服务端

1. 把对应的 jar 放入 `plugins/` 目录
2. 重启服务端 —— 首次运行会自动生成配置文件（自带中文注释）
   - Velocity：`plugins/Miku-ZSTD/config.yml`
   - Paper：`plugins/Miku-ZSTD/config.yml`
3. 确认日志出现这一行，说明已正常工作：

```
[Zstd] Zstd transport activated (dict=true)          ← Velocity
[Zstd] zstd transport activated on Paper             ← Paper
```

> ⚠️ **`plugins/` 下只能有一个本插件的 jar。** 存在多个同 id 的 jar 时只会加载其中一个，
> 会造成「改了配置却没生效」——这个坑排查起来非常费时。

### 客户端

把 `Miku-ZSTD-<版本>-Fabric.jar` 放入 `.minecraft/mods/`。
进入游戏后按 **F8** 可切换统计 HUD，实时显示上下行的压缩前后速率与压缩率。

首次启动会生成 `Miku-ZSTD/config.yml`（游戏目录下）。

---

## 工作原理

```
客户端 ──握手(主机名末尾附 \0ZSTD\0 标记)──▶ 服务端
        ◀── zstd:negotiate(协议版本 + 字典 id)──
        ── answer(0=就绪 / 1=需字典 / 2=保持原版)──▶
        ◀── zstd:dict(仅当回 1 时，按需下发字典)──
        ── answer(0 / 2) ──▶
        ◀── SetCompression ──        ← 双方在此刻切换到 zstd
        ═══ 之后全部走 zstd 帧 ═══
```

1. 客户端在握手包主机名末尾附加 `\0ZSTD\0` 标记
2. 服务端识别标记，在登录阶段发起协商（只带协议版本与字典 id，几十字节）
3. 客户端按本地缓存应答；**仅当缺字典时**服务端才单独下发字典字节
4. 双方把压缩处理器替换为 zstd（服务端等协商应答到达后才激活，期间扣住待发包避免错帧）
5. 版本不匹配 / 协商失败 → **双方一致回落原版 zlib**，不会断连

字典是**从真实流量自动训练**的，无需手工配置；训练完成后自动推送给客户端，并跨重启持久化。

---

## 配置

配置文件自带完整中文注释，这里只列最常动的几项：

| 键 | 默认 | 说明 |
|---|---|---|
| `compression.level` | `3` | 压缩等级 1-22。**默认 3 就够**——实测小包上高等级反而更差（64B 单包 L3 压到 45.3%，L9 只有 57.8%），而 L9 耗时是 L3 的 8~12 倍。大包占比高且 CPU 富余时可调到 6 |
| `compression.window_log` | `20` | 滑动窗口 2^N 字节。**不要盲目调大**——实测 17/20/23 压缩率完全相同，但内存是每连接一份且指数增长（23 ≈ 16MB/连接，20 ≈ 2MB） |
| `compression.batch_window_ms` | `1` | 批处理窗口。同一 tick 内的包间隔只有几十微秒，1ms 足够合并。**不要设 0**——那等于关闭批处理，带宽收益消失 |
| `compression.skip_compress_below_bytes` | `48` | 无字典时低于此值的批次直接直存（实测 ≤32B 压缩后膨胀到 102~113%） |
| `compression.skip_compress_below_bytes_with_dict` | `24` | 有字典时的阈值。**不要与上一项共用同一个值**，否则会白丢小包收益 |
| `trainer.adoption_threshold` | `0.01` | 替换字典所需的改进率。**不要调到 0.03 以上**——字典收益按总字节加权通常只有 1~3%，阈值过高会让字典永远无法被采纳 |
| `logging.debug` | `false` | 开启后输出逐包诊断与按包长分档的带宽剖析（每 60s 一行），仅在排查时开启 |
| `display.hud_enabled` | `false` | **仅客户端**：HUD 默认开关（游戏内 F8 可临时切换，不落盘） |

配置文件**只在首次运行时生成**，之后不会被覆盖；删除即可重新生成默认值。

### 字典什么时候会生效？

字典需要累积样本才能训练出来（默认 2000 个样本，或 10 分钟兜底强制触发一轮）。
在此之前压缩照常工作，只是没有字典增益。训练成功后日志会出现「采纳新字典」。
可用 `/mikuzstd status` 查看当前样本数与字典 ID。

---

## 命令

服务端执行（Velocity 与 Paper 通用）：

| 命令 | 功能 |
|---|---|
| `/mikuzstd` | 等同 `/mikuzstd status` |
| `/mikuzstd status` | 压缩参数、在线人数、字典训练状态、压缩线程池、解压缓冲 |
| `/mikuzstd top` | 各连接的压缩统计排行（按流量降序，用来定位「谁在拖后腿」） |
| `/mikuzstd bar` | 开关 BossBar 实时监控（每秒刷新，显示使用人数与压缩率） |
| `/mikuzstd reload` | 热重载配置文件 |
| `/mikuzstd train` | 立即触发一轮字典训练 |

> `reload` 只替换配置：**`level` / `window_log` 等参数只对之后新建立的连接生效**，
> 已有连接仍在用旧值，需要重启服务端才会全部更新。

权限节点：`mikuzstd.command`（Velocity 端同时兼容 `zstd.command`）。
只读子命令对所有人开放——因为 Velocity 自身没有内置权限系统，装了权限插件后可对 `reload` / `train` 加限制。

---

## 兼容性

- **原版客户端**：安全。未带 `\0ZSTD\0` 标记的客户端完全走原版 zlib，不受影响、不会踢人
- **ViaVersion**：兼容（关键操作在 ViaVersion 处理器前后正确注入）
- **其他追加主机名标记的插件**（如 ServerSwitcher）：兼容，各自标记互不干扰
- **客户端侧压缩优化类模组**：Fabric 端已内置客户端压缩优化，**无需再额外安装同类模组**
- **多个同 id 插件 jar**：**不兼容**，见上方安装说明

---

## 排障

### 先确认双端都生效

**只有单侧生效必然断连。** 两端都要看日志：

```
服务端：[Zstd] Zstd transport activated (dict=true)
客户端：[Zstd] Client zstd transport activated
```

### 日志解读

开启 `logging.debug: true` 后，正常流程的日志顺序是：

```
[Zstd] Sniffer detected Zstd client on <ip>          ← 识别到 mod 客户端
[Zstd] Sent zstd:negotiate txId=... encId=... decId=...
[Zstd] Negotiate response received: enc=0 dec=0      ← 客户端应答
[Zstd] SetCompression detected — deferring ...       ← 进入激活窗口
[Zstd] Zstd transport activated (dict=true)          ← 激活成功
```

| 日志 | 含义 |
|---|---|
| `releasing N packet(s) held during activation window` | 正常。激活窗口内扣住的包已按正确编码路径补发 |
| `skipping zstd (vanilla fallback)` | 协商失败，**双方一致回落原版 zlib**，不会断连 |
| `Protocol mismatch reported by client` | 两端协议版本不一致，需同时升级两端 |
| `带宽剖析（本周期）：出站 ... → ...` | 每 60s 一行的带宽报告，按包长分档 |

### 常见问题

**Q：进不去服务器 / 一进就断连？**
绝大多数是**只有单侧生效**。检查两端日志是否都有 `activated`，以及两端版本是否一致。

**Q：Paper 端提示「anchors 始终未出现」？**
检查 `server.properties` 的 `network-compression-threshold` 是否 ≥ 0。设为 `-1` 时本插件不可用。

**Q：压缩率看起来很差？**
先看 `/mikuzstd status` 里字典是否已训练出来（dictId 非 0）。小包压缩率接近 100% 说明字典还没出来。
再看带宽剖析，确认字节主要花在哪个包长区间。

**Q：CPU 占用变高？**
看 `/mikuzstd status` 的压缩线程池一行。**若「队列」持续大于 0，说明压缩跟不上流量**——
此时应降低 `level` 或增加线程，而不是继续调批处理窗口。
