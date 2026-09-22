# miku-zstd-protocol

三个模块**共享的协议层源码**（不是独立 Gradle 工程，见下）。

## 为什么存在

`miku-zstd-velocity/`、`miku-zstd-paper/` 与 `miku-zstd-fabric/` 是**三个独立 Gradle 工程**，
历史上同一套协议逻辑各实现一份，约 600 行近乎重复：

| 文件 | 服务端 | 客户端 | 差异 | 相似度 |
|---|---|---|---|---|
| `ZstdVarInts` | 115 | 108 | 17 | ~85% |
| `ZstdBatchEncoder` | 265 | 250 | 46 | ~82% |
| `ZstdBatchDecoder` | 252 | 234 | 40 | ~84% |

其中帧格式代码要求三端**逐字节对称**，而"改一边忘另一边"的症状是
**握手能成、之后静默错乱**（2.0.0 就因客户端多写一层长度前缀造成过断连回归），
排查成本极高。共享单一实现后，这类漂移在结构上不再可能发生。

## 术语：本项目的 "v几" 有三个含义

同一份代码同时属于"协商版本 v4"与"帧格式 v3"，两者是不同维度的编号：

- **协商版本**：`PROTOCOL_VERSION = 4`，登录期在 negotiate 载荷里交换，三端必须一致；
  不一致即回落原版 zlib。v4 的改动是"字典改为按需推送"。
- **帧格式 v3**：帧头精简（magicless / 无 contentSize / 无 dictID）+ 一帧多包批处理。
  **v4 没有再改帧格式。**
- **v1 / v2**：历史版本，只应出现在"我们为什么这么改"的说明里。

完整说明见 `ZstdVarInts` 的类注释。

## 共享边界

共享（本目录）：

| 文件 | 内容 |
|---|---|
| `ZstdVarInts` | VarInt 编解码 + byte[]/ByteBuf 两种输入的读取入口 |
| `ZstdBatchEncoderBase` | 批处理窗口、建帧、promise 编排、flush 吞并、生命周期清理 |
| `ZstdBatchDecoderBase` | 帧解析、内层切包、压缩比上限、scratch 上限、fail-fast |
| `ZstdNegotiateStatus` | 协商状态码白名单（越界一律归一为"保持原版"） |
| `ZstdDictId` | 字典 id 的 CRC32 契约（服务端声明 / 客户端缓存键必须同源） |
| `ZstdSampleFilter` / `ZstdDictAdoption` | 训练采样过滤与字典采纳判定 |
| `ZstdTrafficCounter` / `ZstdBossBarFormat` | 流量计数与采样、BossBar 文本与字节单位 |

**不共享**（各端职责差异大，强行合并会互相拖累）：

| 类 | 原因 |
|---|---|
| `ZstdChannelManager` | 相似度仅 ~32%：服务端是"门控 + 字典装配"，客户端是"字典加载 + 状态" |
| `ZstdDictRegistry` | 相似度仅 ~45%：服务端单代 + 换代延迟释放；客户端多代 LRU（可能连过多个服务器） |
| `Zstd*Config` / `Trainer` / BossBar 展示层 | 纯单端职责（展示层分别是 Adventure 与 Bukkit） |

跨端差异通过基类的抽象钩子收敛，各端只写 30~50 行：

| 钩子 | Velocity | Paper | Fabric（客户端） |
|---|---|---|---|
| `writeBodyLen()` | `true`（无 frame-encoder，编码器自带外层长度） | `false`（prepender 负责） | `false`（prepender 负责） |
| `onFrame(...)` | 带宽剖析 + 全局/连接统计 | 监控 + 连接统计 | HUD 统计 |
| `skipCompressThreshold(...)` | `ZstdVelocityConfig` | `ZstdPaperConfig` | `ZstdConfig` |
| `hasCompressDict()` | 查 `encoderDictEntry` | 查 `encoderDictEntry` | 查 `decoderDictEntry`（语义相反，见该类注释） |
| `wantsStoredPayload()` | `true`（训练用） | `true` | `false`（不训练，零开销） |

## 接入方式

三端 `build.gradle` 各有一处声明：

```groovy
sourceSets {
    main {
        java {
            srcDir(new File(rootDir.parentFile, 'miku-zstd-protocol/src/main/java'))
        }
    }
}
```

即**同一份源码被编译进各自的 jar**，无需 shadow / Loom `include` / 发布仓库。

本目录的测试（`src/test/java`）**由 Velocity 模块的 test 源集负责执行**——
三个模块里只有它是纯 `java` 插件（Paper 要 paper-api、Fabric 要 Loom），
跑纯 JVM 测试的代价最小，而这些测试只依赖 netty + zstd，不需要任何 Minecraft 类。

### 为什么不建成独立 Gradle 工程

三个模块各自独立构建（各自的 wrapper、各自 `gradlew build`），跨工程共享 jar 需要
composite build 或 `publishToMavenLocal`，并额外解决"classpath 依赖如何进入
fabric 的 Loom 产物"（必须 `include` 成嵌套 jar）——引入三重构建风险，而收益
（消除重复）与 `srcDir` 方案完全相同。因此当前取 `srcDir`：

- ✅ 源码单份：改一处、三端同时生效（这是本次要解决的核心问题）
- ✅ 零额外构建配置与打包风险
- ⚠️ 共享代码在各端各编译一次（编译产物重复，源码不重复）

### 升级为独立模块的路径（若将来需要）

1. 给本目录加 `build.gradle`（`java-library`）+ `settings.gradle`，`group = 'mikumc.zstd'`；
2. 三端 `settings.gradle` 加 `includeBuild('../miku-zstd-protocol')`；
3. 三端依赖改 `implementation("mikumc.zstd:miku-zstd-protocol")`；
4. Velocity 与 Paper 端 `shadowJar` 会自动打包；**Fabric 端需把该依赖加进 Loom 的 `include`**
   （否则运行期 `NoClassDefFoundError`）——这一步是主要风险点。

## 约束

改动本目录后**必须**跑回归测试：

```bash
cd miku-zstd-velocity && ./gradlew test
```

（在 CI 上由 Velocity job 的 `test shadowJar` 执行；测试不过就不会产出产物。）

测试位于本目录的 `src/test/java`：

| 测试 | 覆盖 |
|---|---|
| `FrameLayoutTest` | 帧格式的逐字段布局（两种外层长度形态）、批处理合帧、压缩帧、往返还原；畸形输入与放大攻击**必须 fail-fast 关连接** |
| `ProtocolUnitsTest` | VarInt 边界、字典 id 契约、协商状态码白名单、采样过滤、BossBar 模板与单位、流量统计、字典采纳判定 |

`FrameLayoutTest` 用两个只差 `writeBodyLen` 一个开关的测试专用编解码器，
覆盖 Velocity 形态与 Paper/Fabric 形态——这是整套共享代码的黄金不变量：
帧格式漂移不会编译失败，只会让两端静默错位，只有这类字节级断言能拦住它。
