# Miku-ZSTD Paper 端排查手册

> 这份文件是三轮真实环境回归（"无法进入服务器"）的总结。动协议/管线相关的代码前，请先读完第一节。

## 一、三条铁律：任何「提前」动作都会破坏 MC 的登录时序

| 提前做什么 | 后果 |
|---|---|
| 提前发 negotiate（客户端还没挂登录查询接收器） | 查询被直接丢弃，协商静默失败 |
| 提前插入 zstd 编码器（原版还没发 SetCompression） | **SetCompression 包被 zstd 压缩**发出，客户端尚未切换 → 解析失败 → 断连 |
| 伪造 SetCompression（打乱双端切换时序） | 包落进 LOGIN→CONFIGURATION 过渡窗 → `Pipeline has no inbound protocol configured` → 断连 |

**结论：zstd 必须严格寄生在原版压缩流程的时间点上。** 等原版发完 SetCompression、装好
`compress`/`decompress`，再替换它们——一步都不能抢。

## 二、正确激活链路

1. **连接建立时**注入协商器（反射包装 netty `ServerBootstrapAcceptor.childHandler`）
2. 收到 **login start 包之后**发 negotiate（更早会被丢弃；服务器列表 ping 连接不发）
3. 客户端应答 → 门控确认（`0 = 就绪 / 1 = 需要字典 / 2 = 保持原版`）
4. 原版发 **SetCompression**（必须以**未压缩**形式发出）
5. 原版装上 `compress`/`decompress` → 我们的重试发现后**替换**为 zstd 编解码器
6. 释放 hold 期间扣住的包（此时走 zstd）

## 三、硬约束清单（违反任意一条都会出问题）

- **注入点**：连接建立时。**不能用 `PlayerLoginEvent`** —— Paper 检测到有插件监听它就会走
  `HorriblePlayerLoginEventHack` 改变登录时序（且该事件已 deprecated）。
- **协商器位置**：`addBefore("packet_handler", ...)`。放到末尾会**收不到入站包**
  （包已被 packet_handler 消费），表现为"negotiate 发了、客户端也回了，服务端等不到应答"。
- **应答负载位置**：`ServerboundCustomQueryAnswerPacket.payload.buffer`（`FriendlyByteBuf`），
  不是 `byte[]`；按 byte[] 直读会拿到默认值 2 → 被误判成"客户端保持原版"。
- **禁止当作替换锚点**：`outbound_config` / `inbound_config` —— 它们是协议阶段切换处理器，
  替换会阻塞 LOGIN→CONFIGURATION 切换（连接静默断开且无异常日志）。
- **首次激活必然 `anchors missing`**：SetCompression 写出时原版还没装 compress（MC 先发包再
  `setupCompression`）。靠 100ms × 20 重试兜住即可，**但绝不能不重试、也绝不能提前插入**。
- **`network-compression-threshold` 必须 ≥ 0**（推荐 256）。设为 `-1`（关闭原版压缩）时本插件
  **不可用**：没有 SetCompression 就没有双端切换信号，也没有替换锚点。此时插件会安全保持原版
  并打 WARN，不影响进服。
- **本插件会替换而非叠加原版压缩**，因此 `threshold=256` 不会带来双份 CPU 开销；
  只有客户端未装模组时才会退回原版 zlib。

## 四、排查清单（"进不去"按序查）

1. `logs/latest.log` 出现 `Failed to start the minecraft server` +
   `另一个程序已锁定文件的一部分` → **世界目录被残留进程锁住**，新进程根本没起来
   （此时你连的是旧进程、旧 jar）。先 `tasklist` 查 java 进程并全部结束。
2. 出现 `anchors 始终未出现` → 检查 `server.properties` 的 `network-compression-threshold` 是否 ≥ 0。
3. 出现 `zstd transport activated on Paper` → 服务端侧成功。
4. 客户端日志出现 `Client zstd transport activated` → 客户端侧成功。
5. **只有单侧 activated 必然断连** → 检查双端协议版本是否一致（当前 v4）。
6. 客户端日志噪音大 → 关掉 `config/miku_zstd.yml` 的 `logging.debug`（逐包 INFO 有性能开销）。

## 五、协议 v4 要点（与另两端一致）

- `negotiate` 只带协议版本 + 两个 dictId（几十字节），**不再内联字典**
- 客户端按缓存回 `0 就绪 / 1 需要字典`；仅当回 1 时服务端才推 `zstd:dict`
- 帧格式 `[bodyLen?][varint rawSize][zstd(payload) | payload]`，payload = `[varint pktLen][pkt]...`
- 服务端（本端）**不写** bodyLen（有 prepender）；Velocity 端才写
