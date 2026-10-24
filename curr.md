# 当前状态

- 项目：分布式消息总线（Go 中台，收口 RocketMQ），蓝本见 [docs/分布式消息总线架构设计.md](docs/分布式消息总线架构设计.md)。
- 进行中：（无）。D03 分发与路由（配置消费侧窄竖切：dispatcher 路由消费契约 + config 热装配函数 RunWatch）已验收通过并归档，设计文档见 [docs/history/分发与路由-design.md](history/分发与路由-design.md)。
- 骨架已就绪：配置加载、zap 日志（pkg/logger）、心跳指标、优雅关闭。
- 已完成：D01 开发流程规范、D02 配置抽象与热更新（pkg/mask 脱敏随之落地）、D03 分发与路由窄竖切，均已归档 docs/history/（D02 见 [docs/history/配置抽象与热更新-design.md](history/配置抽象与热更新-design.md)）。
- 剩余业务模块（生产 SDK、消费分发全量、BinLog 订阅）未开工；fileSource 仅为占位，配置终态经配置中心 gRPC 下发（已录入 [plan.md](plan.md)）；dispatcher 已落路由消费一角（D03），熔断/去重/重试/DLQ 属后续 D0n。

# 已知问题

- （无）统一脱敏工具函数已随 D02 落地于 pkg/mask，config 为首个使用方。

# 下一步

- 蓝本 M1 生产链路（envelope→publishpb→broker Producer→gateway→sdk）启动时另立 D04 设计文档（先对齐期望），届时 dispatcher 消费侧接真实消息源。
