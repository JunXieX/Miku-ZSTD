# Miku-ZSTD

用 **Zstandard** 替代 Minecraft 原版 zlib 的 Velocity 代理插件。在不改动服务端与游戏逻辑的前提下，
把玩家-facing 的出口带宽压到原来的 **1/8 左右**（实测数据见下）。

> ⚠️ **需要客户端安装配套模组**（`miku_zstd`）。这不是单纯的传输优化——它替换的是压缩协议本身，
> 所以必须两端同时使用。未安装模组的客户端**不受任何影响**，走原版 zlib。

---

## 为什么值得用

原版 Minecraft 用 zlib 压缩，且每个包**单独成帧**——帧头、熵表、窗口信息被反复发送。
小包（占真实流量的大多数）压缩后甚至**比原始数据更大**，只能全部直存，白白浪费 CPU 与带宽。

本插件做三件事：

| 优化 | 实测收益 | 说明 |
|---|---|---|
| **一帧多包**（批处理） | **-33.7%** | 1ms 窗口内的多个包合成一帧再压缩，开销被摊薄，直存帧降到 0 |
| **帧头精简** | **-10.7%** | 去掉 zstd 的 4 字节 magic 与帧头里的 contentSize/dictID（尺寸由协议自带） |
| **字典训练** | 小包 **-23.6%** | 从真实流量采样自动训练，收益集中在小包（这正是原版最吃亏的地方） |
| **合计** | **-38.3%** | 相对"每包一帧 + 完整帧头"的基线 |

### 真实服务器实测

某 Velocity-CTD 生产实例（客户端 mod + 服务端插件）：

```
服务端出站   31.85 MB  →  3.92 MB              省 87.7%
客户端接收   进服突发 15.1 MB/s → 1.8 MB/s     12.2%
             稳态     16.7 KB/s → 10.8 KB/s    64.4%
```

进服阶段（区块与注册表数据）压缩率极高；稳态以小包为主，是字典收益的主战场。

---

## 运行要求

| | 要求 |
|---|---|
| 代理 | Velocity **4.x**（以 4.2.0 API 构建，兼容 4.x 全系列） |
| Java | **25+** |
| 客户端 | 需安装 `miku_zstd` 模组（Fabric，MC 26.2+） |

---

## 安装

### 服务端

1. 把 `Miku-ZSTD-<version>-Velocity.jar` 放入 Velocity 的 `plugins/` 目录
2. 重启代理 —— 首次运行会自动生成 `plugins/Miku-ZSTD/config.yml`
3. 确认日志出现这一行，说明已正常工作：

```
[Zstd] Zstd transport activated (dict=true)
```

> **⚠️ `plugins/` 下只能有一个本插件的 jar。** 存在多个同 id 的 jar 时 Velocity 只会加载其中一个，
> 会造成"改了代码却没生效"——这个坑排查起来非常费时。

### 客户端

把 `Miku-ZSTD-<version>-Fabric.jar` 放入 `.minecraft/mods/`。
按 **F8** 可切换统计 HUD，实时显示压缩前后速率与压缩率。

---

## 工作原理

协议版本 **v4**（Velocity / Paper / Fabric 三端的 `PROTOCOL_VERSION` 常量一致）。

```
客户端 ──握手(主机名末尾附 \0ZSTD\0 标记)──▶ 服务端
        ◀── zstd:negotiate(协议版本 + 2×dictId)──
        ── answer(0=就绪 / 1=需字典 / 2=保持原版)──▶
        ◀── zstd:dict(仅当回 1 时，按需推字典字节)──
        ── answer(0 / 2) ──▶
        ◀── SetCompression(必须未压缩)──
        ═══ 双端替换管线，之后全部走 zstd 帧 ═══
```

1. 客户端在握手包主机名末尾附加 `\0ZSTD\0` 标记；服务端嗅探器只认登录连接（`nextState == 2`），
   服务器列表 ping 完全不受影响
2. 服务端在首个出站包前发出 `zstd:negotiate`，**只带协议版本与两个字典 id**（几十字节）
3. 客户端按本地缓存回 `0 = 就绪` / `1 = 需要字典` / `2 = 保持原版`；**仅当回 1 时**服务端才
   单独推一次 `zstd:dict` 下发字典字节，客户端校验 CRC 后回 `0` / `2`
4. 版本不匹配 / 协商失败 → **双方一致回落原版 zlib**，不会断连
5. 原版发出 `SetCompression`（**必须未压缩**——否则客户端尚未切换就会解析失败）后，
   服务端进入 hold 窗口，扣住后续待发包
6. 双端各自把 `compress` / `decompress` 替换为 zstd 实现，再按定型后的编码路径释放扣住的包

> **为什么字典改成按需推送（v3 → v4）**：v3 把字典字节内联进 negotiate，于是**所有连接都要吃这几百 KB**，
> 无论客户端是否支持 zstd、本地是否已有缓存。拆成两段后，重连（缓存命中）与非 zstd 客户端都是零字典流量。

### 帧格式（协议 v4）

```
服务端 → 客户端   [varint bodyLen?][varint rawSize][zstd(payload) | payload]
客户端 → 服务端   [varint rawSize][zstd(payload) | payload]
payload = [varint pktLen][pkt] [varint pktLen][pkt] ...   ← 一个或多个包
```

- **`rawSize == 0` 表示未压缩直存**，此时 payload 就是原始内容；否则是 magicless zstd 帧
- **一帧多包**：批处理窗口内的包合帧，帧头与熵表开销被摊薄
- **magicless zstd 帧**：省 4 字节 magic，且不写 contentSize / dictID
  （解压尺寸由 `rawSize` 提供，字典由双方显式加载，帧内不必重复写）
- **`bodyLen` 只由 Velocity 写**（4.x 管线无独立 frame-encoder，编码器自带外层长度）；
  客户端侧由 prepender 负责——这是刻意设计，缺层或双层都会导致对端解码失败
- 小包（无字典 <48B / 有字典 <24B）压缩后反而膨胀，直接走直存路径

### 协商载荷

| 报文 | 载荷 |
|---|---|
| `zstd:negotiate` | `[int 协议版本][long encDictId][long decDictId][byte flags]` |
| `zstd:dict` | `[byte flags]` + 最多两组 `[int crc32][int len][bytes]` |
| `answer` | `[varint encStatus][varint decStatus]` |

`flags`：bit0 = 有压缩方向字典，bit1 = 有解压方向字典。

> ⚠️ **状态码语义三端必须完全一致**：`0 = 就绪` / `1 = 需要字典` / `2 = 保持原版`。
> 服务端严格按 `2` 回落原版 zlib，因此客户端任何返回 `2` 的分支都必须同时放弃激活 zstd——
> 否则会形成「服务端 zlib / 客户端 zstd」的**必断连组合**。

---

## 配置文件

`plugins/Miku-ZSTD/config.yml`（首次运行生成，**之后不会被覆盖**；删除可重新生成）

> 目录名与插件显示名一致（Velocity 的 plugin id 只允许小写，故取注入路径的父目录再拼 `Miku-ZSTD`）。
> 从旧版本升级时，原有的 `plugins/miku-zstd/`（含 `config.yml` 与已训练的 `zstd_dicts/`）
> 会在首次启动时自动迁移到新目录，不会丢配置与字典。

```yaml
compression:
  # zstd 压缩等级 (1-22)。
  # 默认 3。实测（带 128KB 字典、按批处理形态）：
  #   小包是字典收益的主战场，而高等级在这里反而更差——
  #   单包 64B：L3 压到 45.3%，L9 只能压到 57.8%，L3 好 12.5 个百分点；
  #   数据越长高等级才越有优势（8KB 批次上 L9 好约 4 个百分点），
  #   但代价悬殊：L9 的压缩耗时是 L3 的 8~12 倍，而解压耗时与等级无关。
  level: 3

  # 滑动窗口 2^N 字节（20 = 1MB）
  # 实测 window_log 17 / 20 / 23 的压缩率完全相同，但它决定 zstd 哈希表规模
  # （2^(windowLog-1) × 4B，指数增长）且是每连接一份：23 ≈ 16MB，20 ≈ 2MB。
  window_log: 20

  # ── 协议 v4 批处理 ──
  # 把同一时间窗内发往同一玩家的多个包合成一帧。代价是等量延迟。
  batch_window_ms: 1
  batch_max_packets: 64

  # ── 小包跳过压缩（实测阈值，别凭感觉改）──
  # 无字典时 ≤32B 压缩后反而膨胀到 102~113%，48B 起才有收益
  skip_compress_below_bytes: 48
  # 有字典时 24B 起才有收益（不要与上面共用同一个值）
  skip_compress_below_bytes_with_dict: 24

trainer:
  max_samples: 10000           # 采样环上限
  min_samples: 2000            # 触发训练所需最少样本
  cooldown_ms: 300000          # 训练冷却（5 分钟）
  fallback_timeout_ms: 600000  # 兜底强制训练（10 分钟）
  dict_max_bytes: 131072       # 字典大小上限（128KB）
  sample_target_bytes: 1048576 # 单样本目标大小
  max_history_samples: 20000   # 跨重启持久化样本上限
  adoption_threshold: 0.01     # 替换字典所需的改进率（首部字典只要不劣化就采纳）
  prune_min_payload: 16        # 丢弃载荷小于此值的包

logging:
  debug: false                 # 开启后每 60s 输出按包长分档的带宽剖析
```

### 调参建议

- **`level`**：默认 3。若大包（区块数据）占比高、CPU 也富余，可调到 6 捡回几个百分点；
  再往上只会让压缩线程池排队，那是延迟抖动的真正来源。
- **`batch_window_ms`**：默认 1ms。同一 tick 内写的包间隔通常只有几十微秒，1ms 足够把它们合并，
  所以取 1 几乎不损失带宽收益。**不要设 0**——那等于关闭批处理（每包一帧），带宽收益会消失。
- **`adoption_threshold`**：字典收益按总字节加权通常只有 1~3%，**不要调到 0.03 以上**，否则字典永远不会被采纳。

---

## 命令

| 命令 | 功能 | 权限 |
|---|---|---|
| `/mikuzstd` | 等同 `/mikuzstd status` | 无 |
| `/mikuzstd status` | 压缩参数、在线人数、训练器状态、压缩线程池、解压缓冲 | 无 |
| `/mikuzstd top` | 各连接的压缩统计排行（按原始字节降序，定位"谁在拖后腿"） | 无 |
| `/mikuzstd bar` | 开关 BossBar 实时监控（每秒刷新） | `mikuzstd.command` / `zstd.command` |
| `/mikuzstd reload` | 热重载 `config.yml` | `mikuzstd.command` / `zstd.command` |
| `/mikuzstd train` | 样本达标时触发一轮训练（绕过冷却与「环满 / 兜底超时」两个门槛） | `mikuzstd.command` / `zstd.command` |
| `/mikuzstd train force` | 忽略样本门槛，强制触发一轮训练 | 同 `train` |

> **权限说明**：Velocity 自身没有内置权限系统，未装权限插件时玩家的 `hasPermission` 恒为 false；
> 而 Brigadier 会把 `requires=false` 的节点**连同子树整个从命令树里剔除**（不只是禁用）。
> 因此只读的 `status` / `top` 对所有人开放；`bar` 虽然看起来只是个显示开关，实际会**启停全局流量统计**
> （且监控是单人持有的），所以与 `reload` / `train` 一样保留权限门槛。

> ⚠️ `reload` 只会替换配置单例：**`level` / `window_log` 等参数只对之后新建立的连接生效**，
> 已有连接仍在用旧值（这些参数在连接建立时才应用到该连接的上下文），需要重启代理才会全部更新。

---

## 兼容性

- **原版客户端**：安全。未带 `\0ZSTD\0` 标记的客户端完全走原版 zlib，不受影响、不会踢人
- **ViaVersion**：兼容（关键操作在 ViaVersion 处理器前后正确注入）
- **其他追加主机名标记的插件**（如 ServerSwitcher）：兼容，各自标记互不干扰
- **多个同 id 插件 jar**：**不兼容**，见上方安装说明

---

## 排障

开启 `logging.debug: true` 后可以看到详细过程。正常流程的日志顺序：

```
[Zstd] Sniffer detected Zstd client on <ip>          ← 识别到 mod 客户端
[Zstd] Sent zstd:negotiate txId=... encId=... decId=...
[Zstd] Negotiate 应答：enc=0 dec=0 dictConfirmed=true  ← 客户端应答
[Zstd] SetCompression detected — deferring ...       ← 进入激活窗口
[Zstd] Zstd transport activated (dict=true)          ← 激活成功
```

常见情况：

| 日志 | 含义 |
|---|---|
| `releasing N packet(s) held during activation window` | 正常。激活窗口内扣住的包已按正确编码路径补发 |
| `skipping zstd (vanilla fallback)` | 协商失败，**双方一致回落原版 zlib**，不会断连 |
| `Protocol mismatch reported by client` | 两端协议版本不一致，需同时升级两端 |
| `带宽剖析（本周期）：出站 ... → ...` | 每 60s 一行的带宽报告，按包长分档 |

**带宽剖析**会按包长分档输出原始字节、线路字节、压缩率与直存帧数，用来判断优化方向：
若"巨包"占比极高说明收益主要来自大包；若"小包"压缩率接近 100% 说明字典还没训练出来。

---

## 声明

本项目为 MikuMC 服务器原创插件，公开给大众免费使用，MikuMC 服务器与作者 JunXieX 享有项目著作权，本项目非开源项目，请注意。

MikuMC 系列插件交流群：1105054380
