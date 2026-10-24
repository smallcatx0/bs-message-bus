<!-- D02 | 状态：已完成 -->
# internal/config 配置抽象与热更新（阶段 0 首模块）

## 问题

- V1 蓝本（[docs/分布式消息总线架构设计.md](../分布式消息总线架构设计.md)）G3 要求「监听、暂停、回调路由均由配置中心动态下发、无需发版」，其总线机制 = 配置变更 → 校验 → 重建路由表 → 原子替换。dispatcher / broker / callback 三个下游模块都以 `config` 的数据结构（Route/Subscriber/Pause）为契约依赖，但 `internal/config` 尚未开工。
- 按 [plan.md](../../plan.md) 已对齐结论：config 为阶段 0 首模块，**自身无 internal 依赖、零外部基础设施依赖**（纯内存可测，不需要 MQ/MySQL/Redis 环境），适合作第一刀；M4 时仅以 grpcSource 无缝替换实现（蓝本 7.3）。
- 本模块同时是首个业务模块：项目规范第 2 节要求的**统一脱敏工具函数**（[curr.md](../../curr.md) 已知问题）随本模块落地，供后续所有模块收敛复用。

## 规则与约束

- 工程规则一律以 [项目规范.md](../../项目规范.md) 为准（不复述）：日志经 `logger.L()`（第 1 节）、脱敏（第 2 节）、goroutine 必须 panic recover + 明确终止（第 5 节）、热点 benchmark（第 6 节）、覆盖率 ≥ 85% 含并发与边界（第 7 节）、`internal/` 领域分包禁循环依赖（第 9 节）。
- 架构决策以蓝本为准，本模块不得偏离：蓝本 6.8（接口草图）、7.1–7.3（配置模型与热更新机制）、7.5（canal 路由结构）、第 14 章 TODO 1（V1 仅 fileSource，gRPC 契约不对齐）与 TODO 4（Pause 语义本设计拍板）。
- 依赖方向（蓝本 6.10）：`internal/config` 不依赖任何 internal 包；`envelope` 未开工，故 config 不得引用 envelope 类型（数据结构完全自含）。
- 总线**不持久化配置**（蓝本 2.3/6.8）：本模块只维护内存态，不落库；配置唯一真源未来是配置中心，V1 阶段以本地 yaml 文件代替。

## 方案与决策记录

### D02-1 包结构

```text
internal/config/
├── types.go        # Config/Listener/Route/Subscriber/Pause/BreakerConfig 等数据结构（蓝本 7.1）
├── source.go       # ConfigSource 接口 + ConfigEvent
├── static.go       # staticSource：内存静态，测试与本地起盘用
├── file.go         # fileSource：yaml + fsnotify（占位实现，终态为配置中心 gRPC 下发）
├── table.go        # RoutingTable：预索引 + atomic.Pointer 原子替换 + Snapshot
├── validate.go     # 校验：结构/引用完整性/endpoint 格式
└── *_test.go
```

脱敏工具不在本包，落 `pkg/mask`（理由见 D02-7）。

### D02-2 ConfigSource 接口与 ConfigEvent

```go
// ConfigSource 配置来源抽象：占位实现与未来 grpcSource 可无缝替换（蓝本 6.8）。
type ConfigSource interface {
    Load(ctx context.Context) (*Config, error)
    Watch(ctx context.Context) (<-chan ConfigEvent, error)
}

// ConfigEvent 一次配置变更通知。V1 只使用全量语义：
// 事件仅携带来源标识与原始内容，由 Reload 侧重读→校验→整体重建，不做增量 diff
// （增量协议属 gRPC 契约范畴，蓝本 TODO 1 本轮不对齐）。
type ConfigEvent struct {
    Source string        // "file" | "static" | 未来 "grpc"
    Op     ConfigOpKind  // Upsert/Delete/Reload；V1 fileSource 一律发 Reload
}
```

- `staticSource.Watch` 返回立即关闭的 channel（无变更能力），消费端按 channel 关闭退出，不特判。
- `Load` 与 Reload 内部复用同一「解析→校验→构建」管线，保证首载与热载行为一致。

### D02-3 fileSource：yaml + fsnotify

- **定位拍板（验收时确认）**：fileSource 仅为**占位实现**——后期配置由配置中心以 **gRPC 方式下发**，ConfigSource 抽象即为该替换预留（Load/Watch 接口不变，仅新增 grpcSource 实现 + 装配层切换）。

- **解析**：直接用 `gopkg.in/yaml.v3`（已是间接依赖，提升为直接）反序列化 `Config`，不用 viper——viper 继续只承担本地启动参数（蓝本 2.3「viper 仅本地启动参数」边界）。
- **监听**：`fsnotify`（同为间接依赖提升）监听**路由配置文件所在目录**而非文件本身——编辑器保存常采用「写临时文件 + rename」使被监听 inode 消失，监目录才可靠；事件按文件名过滤，其余忽略。
- **防抖**：fsnotify 事件先经 debounce（默认 200ms，可配）合并，保存动作产生的多事件只触发一次 Reload。
- **重读超时**：Reload 含文件 IO，显式 `context.WithTimeout`（默认 3s，规范第 5 节对「所有外部调用」要求延伸到文件系统读取，避免异常挂载点永久阻塞）。
- **Watch goroutine 生命周期**（规范第 5 节）：
  - 启动条件：`Load` 成功后由装配层调 `Watch` 启动；
  - panic recover：goroutine 入口 defer recover，经 `logger.L()` 记录堆栈与上下文（文件路径、事件），恢复后**关闭输出 channel** 交由装配层决定重启或退出（不静默复活，避免半死状态）；
  - 终止逻辑：`ctx.Done()` → 停 debounce ticker → 关闭 watcher → 关闭输出 channel，无孤儿 goroutine。

### D02-4 RoutingTable 与原子替换（蓝本 7.2）

```go
// TableStore 内存路由表（蓝本 6.8 草图，Snapshot/Reload 语义不变）。
type TableStore interface {
    Snapshot() *RoutingTable
    Reload(cfg *Config) error
}
```

- `RoutingTable` 为不可变快照，构建时预索引：
  - mq 维度：`topic+tag → []RouteIndex`（tag 空串匹配任意）；
  - canal 维度：`db.table+event → []RouteIndex`（event 空串匹配任意）；
  - 订阅方：`subscriber_id → *Subscriber`（Route 只存 ID，扇出时解引用，引用完整性在 Reload 校验）；
  - 暂停：`pause 列表按 scope 预分组`，`Pause.Expire` 在**读取时判定**（`now > Expire` 即视为不生效），不依赖定时器。
- 承载方式 `atomic.Pointer[RoutingTable]`，`Snapshot()` 无锁读；Reload = 校验 → 重建新表 → `Store`，旧快照由在途读者持有至完成、GC 回收。
- **Reload 失败策略：拒绝新配置、保留旧表**（返回 error、记 ERROR 含脱敏上下文、`config_reload_error_total` 语义留 M5 埋点）——错误配置宁可用旧的正确路由，绝不清空表导致全量丢消息。

### D02-5 校验规则（Reload 前置，蓝本 7.2）

1. 结构合法：yaml 可解析、必填字段非空、枚举值合法（Source/Scope/EventType/Kind）。
2. 引用完整性：`Route.Subscribers` 全部存在于 `Subscriber` 表；`Listener.Topic/Group` 非空且不重复；ID 全局唯一。
3. endpoint 格式：`Subscriber.Endpoint` 必须为 http/https 绝对 URL（有 scheme + host）。
4. 时间语义：`Pause.Expire` ≥ 0；`RetryPolicy.Multiplier` ≥ 1；超时/退避 > 0。
5. 敏感字段：`CallbackAuth.Token/Secret` 只存内存、任何日志输出经脱敏（D02-7）。

### D02-6 Pause 语义拍板（蓝本 TODO 4）

**V1 按「丢弃」实现**：dispatcher 命中 Pause（topic / tag / subscriber 任一 scope）时直接 ack 源消息并跳过投递，不暂存、不补投。

- 理由：与「总线不持久化配置/消息」边界一致（暂停是运维止损手段，补投需引入暂存存储，超出 V1 范围）；RocketMQ 侧如确需保留现场，运维可用「暂停 consumer group」替代，总线 Pause 只管快速掐断。
- 字段留扩展位：`Pause.Reason` 已存在；未来若升级为暂存补投，扩展 `Pause.Action` 字段（`drop|buffer`），yaml schema 与校验规则均不产生破坏性变更。
- `Expire` 过期自动失效（读取时判定，见 D02-4），永久暂停 = `Expire: 0`。

### D02-7 统一脱敏工具函数（规范第 2 节，plan.md 既有约定）

- 落 `internal/config/mask.go`？——**否**，脱敏是横切能力，放 config 包会造成后续模块反向依赖 config 的不良耦合。决策：落 **`pkg/mask`**（无业务依赖的通用代码，符合规范第 9 节 pkg 定位），config 为首个使用方。
- 覆盖字段：手机号、邮箱、密码、token、证件号；提供 `MaskString`（通用打码）+ 按 zap field 的 `Token/Secret` 便捷函数，error 构造与日志打点统一走此包，后续模块收敛到该实现（规范第 2 节「不得长期存在多套」）。

### D02-8 yaml schema（一次设计到位，含 canal 路由）

`conf/routing.yaml.example`（模板入库，`.gitignore` 放行 example）：

```yaml
listeners:
  - id: order-main
    source: mq            # mq | canal
    topic: order-topic
    group: bus-consumer
    tag: ""               # 可空，空串匹配任意
    enabled: true
routes:
  - id: order-to-erp
    match:
      topic: order-topic  # mq 维度
      tag: created
      db: ""              # canal 维度（source=canal 路由必填，供白名单派生，蓝本 7.5）
      table: ""
      event: ""           # INSERT | UPDATE | DELETE | 空=任意
    subscribers: [erp-callback]
subscribers:
  - id: erp-callback
    endpoint: http://erp.internal/hook/msg
    timeout: 5s
    retry: { max_attempts: 5, initial_backoff: 1s, max_backoff: 2m, multiplier: 2.0 }
    rate_limit: 0          # 0=不限（预留）
    headers: {}
    auth: { mode: static_header, header: X-Callback-Token, token: "..." }  # mode: none|static_header|hmac（仅前两者 V1 生效）
    breaker:               # 蓝本 7.1 BreakerConfig，V1 仅存配置、消费方 M2 接线
      enabled: true
      error_rate_threshold: 0.5
      slow_call_rate: 0.5
      slow_call_duration: 3s
      window_size: 20
      open_timeout: 500ms
      backoff_factor: 2.0
      max_open_timeout: 30s
      half_open_max_probe: 1
      max_dwell: 10m
      consecutive_success: 1
pauses:
  - id: pause-pay-refund
    scope: tag             # topic | tag | subscriber
    target: pay-topic:refunded   # scope=topic 时填 topic；scope=tag 时填 "topic:tag"；scope=subscriber 时填 subscriber id
    reason: 下游发布止损
    expire: 0              # Unix 秒；0=永久
```

- **target 编码规则拍板**：`scope=tag` 用 `topic:tag` 组合串（topic 名不允许含 `:`，校验规则强制），避免为两个字段另立嵌套结构。
- 路由配置**文件路径**经启动参数传入（`conf/app.yaml` 增加 `routing_file` 键，走现有 viper 机制，符合 12-Factor：路径来自配置/环境变量，路由**内容**不落库）。
- canal 路由的 `db/table/event` 结构 M3/M4 直接复用：M3 的 db.table 路由匹配、M4 的白名单派生（`filter.regex` = 所有 canal 路由 db.table 集合）均在既有字段上计算，**schema 无需变更**。

### D02-9 依赖与装配边界

- `go.mod` 变化：`fsnotify/fsnotify`、`gopkg.in/yaml.v3` 由 indirect 提升 direct；无新增第三方库。
- 本模块**不改 `cmd/main.go` 装配**——config 的消费方（dispatcher/broker）在 M1/M2 才出现，提前接线只能空转；交付自带 `example_test.go` 演示装配用法（Load → 初始 Reload → Watch 循环），供 M1 迁移参考。

## 状态勾点（关键里程碑）

- [x] `internal/config` 数据结构 + ConfigSource 接口 + staticSource（含单测）
- [x] RoutingTable：预索引构建、atomic.Pointer 替换、Snapshot 并发读（含 -race 并发测试 + Snapshot/Reload benchmark）
- [x] 校验规则 5 类全覆盖（含边界：空配置、引用断裂、坏 endpoint、非法枚举）
- [x] fileSource：yaml 解析 + fsnotify 目录监听 + debounce + Reload 超时（含改文件即时生效演练、rename 保存场景测试）
- [x] Watch goroutine：panic recover 记堆栈 + ctx 取消退出 + channel 关闭无泄漏（显式断言 channel 关闭）
- [x] `pkg/mask` 脱敏函数落地（手机/邮箱/密码/token/证件号），config 内日志与 error 统一接入
- [x] `conf/routing.yaml.example` 模板入库，字段口径注释随文件（含 TestShippedRoutingTemplate 实测可加载）
- [x] 覆盖率 ≥ 85%（`go test -race ./internal/... ./pkg/...`）+ `go vet` + `staticcheck` 通过
- [x] 用户验收确认（2026-10-24：含两项异常分支实现无定向测试的披露，均接受）

## 验收结论

验收通过（2026-10-24）。交付：`internal/config`（types/source/static/file/table/validate + 6 个测试文件，覆盖率 90.3%）+ `pkg/mask`（覆盖率 93.3%）+ `conf/routing.yaml.example` + `routing_file` 启动参数键；`-race` 全绿、vet/staticcheck 零告警、MatchMQ 基准 51ns/0 alloc。

两项披露均被接受：Reload 读取超时与 Watch panic recover 实现已就位，但无确定性定向测试（分别因本地文件读取极快、未注入 panic）。

后续事项（已录入 [plan.md](../../plan.md)）：配置中心 gRPC 来源接入替换 fileSource 占位；cmd 装配与 dispatcher/broker 接线归 M1。
