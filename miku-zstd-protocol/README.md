# miku-zstd-protocol

两端**共享的协议层源码**（不是独立 Gradle 工程，见下）。

## 为什么存在

`miku-zstd-velocity/` 与 `miku-zstd-fabric/` 是**两个独立 Gradle 工程**，历史上
同一套协议逻辑各实现一份，约 600 行近乎重复：

| 文件 | 服务端 | 客户端 | 差异 | 相似度 |
|---|---|---|---|---|
| `ZstdVarInts` | 115 | 108 | 17 | ~85% |
| `ZstdBatchEncoder` | 265 | 250 | 46 | ~82% |
| `ZstdBatchDecoder` | 252 | 234 | 40 | ~84% |

其中帧格式代码要求两端**逐字节对称**，而"改一边忘另一边"的症状是
**握手能成、之后静默错乱**（2.0.0 就因客户端多写一层长度前缀造成过断连回归），
排查成本极高。共享单一实现后，这类漂移在结构上不再可能发生。

## 共享边界

共享（本目录）：

| 文件 | 内容 |
|---|---|
| `ZstdVarInts` | VarInt 编解码（读写快路径、writeTo 拼接） |
| `ZstdBatchEncoderBase` | 批处理窗口、建帧、promise 编排、flush 吞并、生命周期清理 |
| `ZstdBatchDecoderBase` | 帧解析、内层切包、压缩比上限、scratch 上限、fail-fast |

**不共享**（两端职责差异大，强行合并会互相拖累）：

| 类 | 原因 |
|---|---|
| `ZstdChannelManager` | 相似度仅 ~32%：服务端是"门控 + 字典装配"，客户端是"字典加载 + 状态" |
| `ZstdDictRegistry` | 相似度仅 ~45%：服务端单代 + 换代延迟释放；客户端多代 LRU（可能连过多个服务器） |
| `Zstd*Config` / 统计 / Trainer | 纯单端职责 |

跨端差异通过基类的抽象钩子收敛，两端各自只写 30~50 行：

| 钩子 | 服务端 | 客户端 |
|---|---|---|
| `writeBodyLen()` | `true`（Velocity 无 frame-encoder，编码器自带外层长度） | `false`（prepender 负责） |
| `onFrame(...)` | 上报 `ZstdBandwidthProfiler` | 上报 `ZstdStatsData`（HUD） |
| `skipCompressThreshold(...)` | 读 `ZstdVelocityConfig` | 读 `ZstdConfig` |
| `hasCompressDict()` | 查 `encoderDictEntry` | 查 `decoderDictEntry` |
| `resolveCompressContext/DecompressContext` | 取服务端 `ZstdChannelManager` | 取客户端 `ZstdChannelManager` |

## 接入方式

两端 `build.gradle` 末尾各有一处声明：

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

### 为什么不建成独立 Gradle 工程

两个模块各自独立构建（各自的 wrapper、各自 `gradlew build`），跨工程共享 jar 需要
composite build 或 `publishToMavenLocal`，并额外解决"classpath 依赖如何进入
fabric 的 Loom 产物"（必须 `include` 成嵌套 jar）——引入三重构建风险，而收益
（消除重复）与 `srcDir` 方案完全相同。因此当前取 `srcDir`：

- ✅ 源码单份：改一处、两端同时生效（这是本次要解决的核心问题）
- ✅ 零额外构建配置与打包风险
- ⚠️ 共享代码在两端各编译一次（编译产物重复，源码不重复）

### 升级为独立模块的路径（若将来需要）

1. 给本目录加 `build.gradle`（`java-library`）+ `settings.gradle`，`group = 'mikumc.zstd'`；
2. 两端 `settings.gradle` 加 `includeBuild('../miku-zstd-protocol')`；
3. 两端依赖改 `implementation("mikumc.zstd:miku-zstd-protocol")`；
4. Velocity 端 `shadowJar` 会自动打包；**Fabric 端需把该依赖加进 Loom 的 `include`**
   （否则运行期 `NoClassDefFoundError`）——这一步是主要风险点。

## 约束

改动本目录后**必须**跑跨端回归：

```bash
bash tools/prototest/run.sh
```

`FrameLayoutTest` 会同时加载两端产物做逐字节比对（含协议常量一致性、8 种批形状、
双向互解、直存/压缩、畸形输入与放大攻击拒绝），是这套共享代码的黄金不变量。
