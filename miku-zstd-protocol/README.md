# miku-zstd-protocol

作者：JunXieX　|　MikuMC系列插件交流群：1105054380　|　非开源项目，请勿二次分发

三个部署模块共享的下行协议源码，不是独立 Gradle 工程。用户部署、配置与命令见根目录 README。

## 当前协议

- 协商版本：`ZstdProtocolVersion.NEGOTIATE = 11`。三端必须一致，版本不匹配时保持原版。
- 下行帧格式：`ZstdProtocolVersion.FRAME = 7`。压缩帧带原始长度、滚动历史代数、分块数量及各块长度；直存帧为一包一帧 `[0][packet]`。
- 上行从 5.0.45 起完全由原版压缩、分帧与解压处理。客户端不安装批处理编码器，服务端不安装 zstd 解码器，不存在上行存储批帧。
- `negotiate` 应答用于能力校验，`switch` 确认与闸门用于确保客户端下行解码器就绪；确认包也遵循原版上行压缩规则。

## 共享边界

| 组件 | 职责 |
|---|---|
| `ZstdProtocolVersion` | 协商与帧格式版本的唯一来源 |
| `ZstdBatchEncoderBase` | 服务端下行攒批、压缩、直存、flush 边界、promise 与屏障保序 |
| `ZstdBatchDecoderBase` | 客户端下行帧解析、历史代数校验、解压与内层切包 |
| `ZstdRollingDict` | 确定性重建 64KB 滚动历史，不传输字典 |
| `ZstdSwitchGate` / `ZstdStepFrames` | 下行就绪屏障、控制报文编码与原版帧识别 |
| `ZstdCompressPool` / `ZstdCtxPool` / `ZstdCompressLock` | 下行压缩线程池、上下文池与 native 资源生命周期 |
| `ZstdVarInts` | 有界 VarInt 编解码与帧长度限制 |
| `ZstdConnStats` / `ZstdTrafficCounter` / `ZstdBossBarFormat` | 下行统计与展示口径 |

Velocity 与 Paper 持有下行压缩上下文；Fabric 持有下行解压上下文。各端只处理原版管线锚点、配置与展示差异。

## 接入方式

三个模块的 `build.gradle` 通过 `sourceSets.main.java.srcDir` 引用本目录 `src/main/java`。共享源码编译进各自产物，不额外发布共享 jar。

- Velocity 负责下行外层长度，Paper 继续使用原版 prepender。
- 服务端下行编码器从原版 compress 的 context 写帧，客户端下行解码器从原版 decompress 的 context 交付已解出的包。原版 handler 始终保留。
- 压缩帧按整帧原始负载喂入历史；直存帧按 `[pktLen][packet]` 一帧一次喂入，两端必须完全对称。

## 回归验证

本地仅做静态审查，禁止执行任何 Gradle 任务。协议回归测试通过 GitHub Actions 的 Velocity job 执行，三端构建与 Paper / Velocity E2E 完成后仍需真实客户端验证。

| 测试 | 覆盖 |
|---|---|
| `FrameLayoutTest` | 下行直存/压缩/分块往返、历史代数、flush 边界、屏障保序、非法帧拒绝 |
| `ProtocolUnitsTest` | VarInt、状态码、流量统计与文本格式 |
| `ZstdSwitchGateTest` | 下行扣包、确认/超时、连接关闭与控制帧识别 |
| `DictAblationTest` | 滚动历史对照实验，只出报告 |

真机验证至少包括新客户端、并发登录、旧版本回落与纯原版客户端；阈值 0 与 256 下的上行确认均须通过原版压缩层。
