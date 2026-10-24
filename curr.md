# 当前状态

- 项目：分布式消息总线（Go 中台，收口 RocketMQ），蓝本见 [docs/分布式消息总线架构设计.md](docs/分布式消息总线架构设计.md)。
- 进行中：D03 分发与路由（配置消费侧窄竖切：dispatcher 路由消费契约 + config 热装配函数），设计文档 [docs/分发与路由-design.md](docs/分发与路由-design.md)。方案已对齐、**待开发**。
- 骨架已就绪：配置加载、zap 日志（pkg/logger）、心跳指标、优雅关闭。
- 已完成：D01 开发流程规范、D02 配置抽象与热更新（pkg/mask 脱敏随之落地），均已归档 docs/history/（D02 见 [docs/history/配置抽象与热更新-design.md](history/配置抽象与热更新-design.md)）。
- 剩余业务模块（生产 SDK、消费分发、BinLog 订阅）未开工；fileSource 仅为占位，配置终态经配置中心 gRPC 下发（已录入 [plan.md](plan.md)）。

# 已知问题

- （无）统一脱敏工具函数已随 D02 落地于 pkg/mask，config 为首个使用方。

# 下一步

- 开发 D03（按 [docs/分发与路由-design.md](docs/分发与路由-design.md) 勾点）：新建 `internal/dispatcher`（ResolveMQ/ResolveCanal 消费契约）+ `internal/config/reload.go`（RunWatch 热装配），纯内存/临时文件驱动验收。**不接入 cmd/main.go**（接线待真实消费方落地）、**不引入 envelope**（用原始维度参数）、**不含**熔断/去重/重试/DLQ（留后续 D0n）。
- 蓝本 M1 生产链路（envelope→publishpb→broker Producer→gateway→sdk）后续另立设计文档；D03 为提前立的接口先行窄竖切（偏离口径见 D03-1）。
