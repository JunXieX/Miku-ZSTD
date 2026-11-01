# Miku-ZSTD 开发文档

面向维护者。部署与配置见 [README](README.md)。

---

## 1. 模块划分

| 模块 | 作用 | 依赖 |
|---|---|---|
| `miku-zstd-protocol` | 三端共享：Netty 编解码器基类、zstd 上下文池、压缩线程池、切换闸门、流量统计 | zstd-jni |
| `miku-zstd-paper` | Paper / Folia 服务端 | protocol |
| `miku-zstd-velocity` | Velocity 代理端 | protocol |
| `miku-zstd-fabric` | Fabric 客户端 | protocol |

三端都是对协议层抽象类（`ZstdBatchEncoderBase` / `ZstdBatchDecoderBase`）的薄封装，各自只需提供配置钩子、锚点名、锁对象。**任何改动优先落在 protocol，三端同步受益**。

---

## 2. 协议规格

### 2.1 版本常量

`ZstdProtocolVersion.NEGOTIATE = 6`，帧格式 v5。三端必须一致，不一致时正常回落原版（能进服，只是不压缩）。

### 2.2 帧格式

```text
Server -> Client   [varint bodyLen?][varint rawSize][varint blockCount][block]...
Client -> Server   [varint rawSize][varint blockCount][block]...
block              [varint blockRawLen][varint chunkLen][chunk]

rawSize == 0       直存帧：帧体恰好一个包 [0][packet]，与原版未压缩帧逐字节同形
```

要点：

- **`bodyLen` 只由 Velocity 写**。它 4.x 管线的 `frame-encoder` 不做长度前缀，编码器自带；Paper 与客户端由 `prepender` 负责。缺层或双层都会让对端解码失败。
- **直存帧不带任何内层长度前缀**。多写一层 `[varint pktLen]` 会被包解码器当作包 id，报 `Unknown packet`，玩家进不去。
- **分块按字节切，不按包切**。原始负载是 `[varint pktLen][pkt]...` 的连续字节，解压后按序拼回再切包，一个包可以横跨两块。
- **`rawSize` 是解压后的总字节数**，不是单块。
- **magicless zstd**：省 4 字节 magic，不写 contentSize / dictID。解压尺寸由 `rawSize` 提供，字典由双方显式加载。

### 2.3 协商载荷

| 报文 | 载荷 |
|---|---|
| `zstd:negotiate` | `[int protocolVersion][long encDictId][long decDictId][byte flags]` |
| `zstd:dict` | `[byte flags]` + 最多两组 `[int crc32][int len][bytes]` |
| `zstd:switch` | `[varint txId][int MAGIC]`，`MAGIC = 0x5A535743`（"ZSWC"） |
| 应答 | `[varint encStatus][varint decStatus]` |

状态码：`0 = 就绪` / `1 = 需要字典` / `2 = 保持原版`。**三端语义必须完全一致**（`ZstdNegotiateStatus` 统一归一化，非 0/1/2 一律按 2）。

---

## 3. 时序

```text
Client --handshake(host 末尾附加 "\0ZSTD\0")--> Server
       <-- zstd:negotiate --
       -- answer(0/1/2) -->
       <-- zstd:dict（仅当回 1）--
       -- answer(0/2) -->
       <-- SetCompression（必须未压缩发出）--
       === 服务端进入 hold 窗口，扣住后续出站包 ===
       <-- zstd:switch --
       -- 确认（带魔数）-->  server 放行扣住的包
       === 两端管线已替换，此后全部走 zstd ===
```

**关键约束：`negotiate` 只能在收到 `SetCompression` 时发出**，不能对"任意首个出站包"发。原因见 §7 事故 6。

---

## 4. 管线替换

### 4.1 锚点

| 端 | 出站锚点 | 入站锚点 | 注入位置 |
|---|---|---|---|
| Paper | `compress` / `compression-encoder` | `decompress` / `compression-decoder` | `packet_handler` 之前 |
| Velocity | `compression-encoder` / `compress` | `compression-decoder` / `decompress` | `handler` 之前 |
| Fabric | `compress` | `decompress` | `splitter` 之后 |

**两侧必须同时替换成功才能激活**。只装上一侧会造成"本端 zstd、对端原生"的必断连组合。

### 4.2 顺序约束

1. 闸门先于编码器（闸门要看到 zstd 帧才能判断切换状态）
2. 闸门先于解码器（解码器靠闸门状态判断"入站是否还可能是原版帧"）
3. `SetCompression` 必须在窗口内**原样透传**（它自己不能压缩，客户端此刻还没切）

### 4.3 扣包窗口

进入窗口后，本 handler 的所有出站写都**必须排队到 event loop**，与 `releaseHeld` 串行。否则主线程就地写出的 `LoginSuccess` 会排到仍在排队的 `SetCompression` 前面 —— 客户端在 LOGIN 态先收 `LoginSuccess`、再收 `0x03`，直接断连（见 §7 事故 8）。

---

## 5. 线程模型

| 线程 | 职责 | 约束 |
|---|---|---|
| Netty event loop | 管线读写、扣包/放行、组帧 | 不可阻塞；`handlerRemoved` 里的锁等待要有上界 |
| 服务端主线程 / Folia 区域线程 | 写包（`channel.writeAndFlush`）、协商器注入 | 必须投递到 event loop，不能就地操作管线 |
| `ZstdCompressPool` | 压缩 | 每连接同时只有一个在途任务（`compressing` 门），积压上限 ≈ 连接数 |

**每连接的 `compressCtx` / `rawBuf` 是独占资源**，跨线程访问必须持有 `ZstdCompressLock`。

---

## 6. 关键不变式

1. 三端 `ZstdNegotiateStatus` 语义一致，任何返回 2 的客户端分支都必须同时放弃激活。
2. 帧内不写 dictID，字典由双方显式加载；字典 id 与内容必须匹配。
3. 分块压缩的 `srcOffset` 是**缓冲内绝对偏移**（`readerIndex + 块起点`），不能传 0 —— zstd-jni 的 native 用 `GetDirectBufferAddress + srcOffset`，不看 ByteBuffer 的 position（见 §7 事故 7）。
4. `ZstdCtxPool.borrowCompress` 必须**每次**设 level —— `returnCompress` 里的 `reset()` 会把参数一起清回默认值（见 §7 事故 5）。
5. 池线程可能仍在读 `rawBuf`，`handlerRemoved` 释放前必须等压缩退出。
6. `CancelPacketException`（CraftEngine 之类的插件用它取消出站包）是**控制流**，不是连接错误。
7. 连接层 `IOException`（对端 RST）不是解码失败，不要打 WARN + 堆栈。

---

## 7. 历史事故（按时间顺序，每条都是真实线上问题）

### ① Folia 进不了服：协商器注入位置错
`EncoderException: Pipeline has no outbound protocol configured … unexpected_query_response`。协商器没在 `packet_handler` 之前，且 txId 重复。修法：等 `packet_handler` + 原子 claim + 10s linger。

### ② 重复 `zstd:switch`
`installHandlers` 重试绕过 `isReplaced()` 检查，客户端收到两个不同 txId 的切换请求。修法：入口加 `if (mgr.isReplaced()) return;`。

### ③ 客户端切换排队窗口
`prepareForSwitch` 无条件 `eventLoop().execute`，导致切换晚于服务端首个 zstd 帧。修法：`inEventLoop()` 时同步执行。

### ④ 空应答把确认嗅探器提前移除
客户端对未注册 channel 会自动回 `success=false`，旧实现收到后立刻 `pipeline().remove(this)`，真确认到达时无人识别。修法：只在 accepted 时移除。

### ⑤ 池化上下文丢失 level
`ZstdCtxPool.borrowCompress` 只在"池空新建"分支 `setLevel`，池命中直接返回。而 `reset()` 的语义是"清空全部状态**与参数**"。结果：配置的 level 只在池恰好为空的头几条连接上生效，之后全部按 3 跑。**修法：两个分支都设 level。**

### ⑥ 向 limbo 登录服发 negotiate 导致断连
`ZstdHijacker` 对"任意首个出站包"都发 `LoginPluginRequest`。limbo（PicoLimbo 那类假服务端）只实现 handshake + login start，收到 `0x04` 直接断开 —— 表现为玩家进 limbo 两秒后被踢、两端日志都没有异常。**修法：只在收到 `SetCompression` 时发。**

### ⑦ 分块压缩源偏移错位
`compressBlock` 给 `compressDirectByteBuffer` 传 `srcOffset = 0`，以为 `internalNioBuffer(index, len)` 是独立视图。实际 zstd-jni 的 native 用 `GetDirectBufferAddress + srcOffset`，**忽略 position**。后果：每块都压"缓冲开头那段"，解压后拼起来是第一块重复 N 次。线上表现：`illegal inner packet length in compressed frame at offset 523930`（512KB 恰好是块边界）。**修法：`srcOffset = readerIndex + 块起点`。** 回归用例：`largeFrameMultiBlockRoundTrip`（1MB 往返）。

### ⑧ 登录期出站顺序倒置（改出来的新 P0）
修 ⑥ 时把"窗口判定"放进 `startHold` 分支，导致只有 `SetCompression` 自己排队，紧随的 `LoginSuccess` 被透传分支直接送出，排到 `SetCompression` 前面。**修法：窗口开启后所有出站写一律排队，窗口开启本身在锁内提前判定。** 教训：这条路径的 bug 用 `EmbeddedChannel` 测不出来（模拟不了"写是否来自非 event loop 线程"），必须真实登录验证。

### ⑨ 取消发包被当成连接错误
CraftEngine 用 `throw CancelPacketException` 短路 pipeline 取消出站包。该异常的 `FIRE_EXCEPTION_ON_FAILURE` 从 pipeline 头广播，撞上我们的 decoder，被按"解码异常"关连接 —— 玩家执行 `/spawn` 就被踢。**修法：按类名识别 `CancelPacketException`，只记 debug。** 回归用例：`packetCancellationMustNotCloseChannel`。

### ⑩ 对端重置被记成解码错误
`SocketException: Connection reset` 的栈起点是 `NioSocketChannel.doReadBytes`，和帧格式无关。但触发时 `isActive()` 往往仍是 true，于是打 WARN + 整段堆栈，看上去像插件把人踢了。**修法：先识别 `IOException`。**

---

## 8. 调试手段

### 日志开关

三端 `config.yml` 的 `logging.debug: true`。关键日志：

| 日志 | 含义 |
|---|---|
| `Sent zstd:negotiate txId=…` | 协商已发出 |
| `协商应答：enc=… dec=… confirmed=…` | 客户端答复已解析 |
| `正在安装 zstd 编解码器 (pipeline=[…])` | 开始替换管线，方括号里是当时的完整 handler 名单 |
| `zstd transport activated on Paper` | 激活成功 |
| `anchors 未出现（compress/decompress 缺失）` | 该连接没有压缩层，回落原版（**不是错误**） |
| `客户端保持原版 zlib，本端不激活` | 客户端明确拒绝，回落原版 |
| `switch confirmation received` | 切换确认到达，闸门放行 |

### 排查顺序

拿到"玩家进不去"的反馈时：

1. 先看有没有 `activated`。没有 → 协商阶段就失败了，看 `negotiate` / `应答` / `anchors` 三种日志。
2. 有 `activated` 但玩家掉线 → 看客户端日志有没有 `frame rejected` / `illegal inner packet length`，那是帧格式问题。
3. 两侧日志都干净、只有一句"连接中断" → 大概率是网络层（`IOException`），看服务端有没有 `connection I/O error`。

### 统计

`/mikuzstd status`（Paper / Velocity）看压缩率与线程池状态；客户端 F8 打开 HUD。

---

## 9. 构建与发版

- **本地不跑 Gradle**。验证编译一律推分支交给 GitHub Actions。
- 版本号 `MAJOR.MINOR.PATCH`，每次改代码第三位 +1，写在提交正文最后一行 `- 版本号升至 x.y.z`。
- 发版：打 `v<版本>` tag → CI 构建三端 → 创建 Release（更新日志取自发版提交正文）。
- **CI 之后必须实测**：三端各跑四条登录路径（新客户端 / 竞态 / 旧客户端 / 原版），见 §10。

---

## 10. 测试

### 单元测试（`miku-zstd-protocol/src/test`）

`FrameLayoutTest` 用真实 encoder / decoder（`TestEncoder` / `TestDecoder` 只覆盖配置钩子），覆盖帧布局、往返、拒绝路径。三个必须保留的回归用例：

| 用例 | 守的是什么 |
|---|---|
| `largeFrameMultiBlockRoundTrip` | 分块压缩的源偏移（事故 ⑦） |
| `packetCancellationMustNotCloseChannel` | 取消发包不被当连接错误（事故 ⑨） |
| `compressedFrameMultiPacketRoundTrip` | 一帧多包的切包还原 |

### 实测脚本

四个 Python 模拟客户端，分别模拟：

| 脚本 | 模拟对象 |
|---|---|
| `client_normal.py` | 新客户端：立即切 zstd + 回带魔数的真确认 |
| `client_race.py` | MC 真实时序：自动空应答 + 100ms 后才切 |
| `client_old.py` | 旧版模组：只回空应答 |
| `client_vanilla.py` | 未装模组 |

判定标准：`normal` 必须收到 `zstd:switch` 并回确认；`race` 的 `delta` 应在 100ms 量级（真确认被识别）；另两条必须回落到 `LOGIN_SUCCESS`。

**单测覆盖不到的**：真实 NIO 写线程模型、真实客户端解码、第三方插件交互（CraftEngine / ProtocolLib / PacketEvents 会重排管线）。这些只能实测。

---

## 11. 已知的取舍

| 取舍 | 原因 |
|---|---|
| 不对"任意首包"发 negotiate | 会打崩 limbo 类极简服务端 |
| 阈值反射失败时按"未知"继续协商，不按"禁用"处理 | 否则一次字段改名就让全体客户端静默失效 |
| 分块并行阈值 256 KB、最多 4 块 | 更小的帧分块收益不抵块间失配的压缩率损失 |
| 不为"并发改管线"加锁 | 概率极低，代价是热路径加锁 |
