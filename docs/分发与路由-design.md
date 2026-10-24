<!-- D03 | 状态：进行中 -->
# dispatcher/broker 路由消费与热更新装配（配置消费侧窄竖切）

## 问题

- D02 已交付控制面（[history/配置抽象与热更新-design.md](history/配置抽象与热更新-design.md)，已完成）：`ConfigSource` 抽象、`RoutingTable`（`MatchMQ/MatchCanal`/`Paused*`/`Subscriber` 解引用）、`fileSource`、`staticSource`。但这些是**冷数据**——尚无消费方按蓝本机制「配置变更 → 校验 → 重建路由表 → 原子替换」把它跑起来。
- 蓝本（[分布式消息总线架构设计.md](分布式消息总线架构设计.md) 3.2/4/6.10）把「消费消息 → 路由匹配 → 暂停判定 → 扇出订阅方」归 `internal/dispatcher`，其匹配依赖 `config` 的 `MatchMQ/MatchCanal`。dispatcher 的消费契约若不定型，后续 M2 全量（熔断/去重/重试/DLQ）会各自臆造调用形态。
- D02 交付时**未改 `cmd/main.go` 装配**（D02-9），只留了装配参考形态 [example_test.go](../internal/config/example_test.go)。fileSource + Watch 循环这条热更新链路当前无人驱动，需要一处可复用装配函数承接（未来 grpcSource 复用同一函数）。

## 规则与约束

- 工程规则一律以 [项目规范.md](../项目规范.md) 为准（不复述）：日志经 `logger.L()`（第 1 节）、脱敏（第 2 节）、goroutine 必须 panic recover + 明确终止（第 5 节）、热点 benchmark（第 6 节）、覆盖率 ≥ 85% 含并发与边界（第 7 节）、`internal/` 领域分包禁循环依赖（第 9 节）、`cmd/` 不写业务实现（第 9 节）。
- 架构决策以蓝本为准：蓝本 3.2（dispatcher 职责=路由匹配+暂停判定+扇出，M2 才补熔断/去重/重试/DLQ）、6.10（依赖方向：dispatcher → config，config 不反向依赖）、7.2（Reload 失败保留旧表，D02-4 已拍板）、6（Pause 语义 V1=丢弃，D02-6 已拍板）。
- 范围边界（本次对齐结论）：
  1. **窄竖切**——只交付「路由消费调用契约 + 可复用热装配函数」，不实现熔断/去重/重试/DLQ/callback/store（留后续 D0n / M2 全量）。
  2. **不引入 envelope**——消费契约入参用原始维度参数（`topic/tag`、`db/table/event`），与 `MatchMQ/MatchCanal` 既有签名天然对齐；envelope 落地时加薄适配层映射，不改本契约。
  3. **不接入 `cmd/main.go`**——装配只交付为可测函数，实际接线待 dispatcher 有真实消费方（M1/M2 集成期）时由 `bootstrap` 调用，避免过早接线空转。

## 方案与决策记录

### D03-1 编号与里程碑口径

Dnn 按里程碑递增（蓝本 13.1：D02=阶段 0）。本切片编号 **D03**，但内容是把蓝本原定 M2 的「路由消费契约」**接口先行 + mock 驱动**提前立住（正合 13.1「接口先行 + mock 驱动、底层无依赖模块先落」原则），**不是** M1 生产链路（envelope→publishpb→broker Producer→gateway→sdk）的全量实现。M1 生产链路后续另立设计文档。此处偏离蓝本「D03≈M1」的粗粒度标注，以本切片实际范围为准。

### D03-2 包结构

```text
internal/dispatcher/        # 新建领域包（蓝本核心层，本次只落路由消费一角）
├── router.go               # 路由消费契约：ResolveMQ/ResolveCanal → 投递目标扇出
└── router_test.go          # 含临时 yaml 驱动的 Reload→重解析演练 + 暂停判定
internal/config/
└── reload.go               # 新增可复用热装配：RunWatch(ctx, src, store)
   + reload_test.go
```

依赖方向：`internal/dispatcher → internal/config`（合规，config 不反向依赖）；`reload.go` 属 config 自身，不引 dispatcher。二者互不引用，无循环。

### D03-3 路由消费契约（dispatcher）

把 `Snapshot().MatchMQ/MatchCanal` + `Paused*` + `Subscriber` 解引用收敛成「一条消息 → 就绪投递目标列表」的**单一纯函数**，屏蔽消费方对表内部结构的分散访问。

```go
package dispatcher

import "message-bus/internal/config"

// Target 一个就绪投递目标：路由 + 解引用后的订阅方。投递/重试/去重由后续 D0n 承接。
type Target struct {
    Route *config.Route
    Sub   *config.Subscriber
}

// ResolveMQ 按 mq 维度解析投递目标：
//  1. topic 暂停（PausedTopic）或 topic+tag 暂停（PausedTag）命中 → 返回 nil（整消息不扇出，D02-6 丢弃语义由调用方 ack）；
//  2. Snapshot().MatchMQ(topic, tag) 取命中路由（通配桶在前+精确桶在后，顺序沿用 D02-4）；
//  3. 逐路由解引用 Subscribers，命中 PausedSubscriber 的目标跳过，其余入列。
func (r *Router) ResolveMQ(topic, tag string) []Target

// ResolveCanal 同构，canal 维度入参 db/table/event，走 MatchCanal；
// topic/tag 暂停语义不适用，仅 PausedSubscriber 生效。
func (r *Router) ResolveCanal(db, table, event string) []Target
```

- `Router` 只持 `config.TableStore` 引用（构造注入），每次解析**按 `Snapshot()` 取当下不可变快照**——与在途 Reload 无锁并发安全（D02-4）。
- **暂停判定顺序**：源级（topic/tag）短路在前、订阅方级过滤在后——先判整条消息是否停发，再逐目标掐，避免对已暂停消息做无谓解引用。
- **返回语义**：空列表 = 无路由或全被暂停；调用方（未来 dispatcher 主循环）据此 ack 源消息、不投递。本契约不 ack、不落库、不记录，纯计算，便于单测。

### D03-4 热更新装配函数（config）

```go
package config

// RunWatch 承接标准装配管线（对齐 example_test.go 形态，来源无关、未来 grpcSource 复用）：
//  1. src.Load → 首次 store.Reload 建表；任一步失败 fail-fast 返回 error（装配层决定退出或重试）；
//  2. src.Watch 启动事件流；
//  3. 独立 goroutine 消费事件：每事件 src.Load→store.Reload（Reload 内部已含校验+失败保留旧表+脱敏日志，D02-4）；
//     该 goroutine 入口 defer recover 记堆栈后关闭并返回（规范第 5 节），ctx 取消 → src 关闭事件 channel → range 结束自然退出，无孤儿。
// RunWatch 本身同步完成首载后启动后台消费并立即返回；停止即取消 ctx。
func RunWatch(ctx context.Context, src ConfigSource, store TableStore) error
```

- **首载 fail-fast**：启动期建不起表属硬伤，直接返回 error；运行期热载失败由 `Reload` 保留旧表并记 ERROR，消费循环仅日志、不退出（错误配置不弄崩在跑的服务，D02-4）。
- **不在 RunWatch 内重复校验/构建**：复用 `src.Load` + `store.Reload` 既有管线，首载与热载行为一致（D02-2）。
- 幂等：事件可能因 debounce/背压冗余或丢失，`Reload` 全量幂等，多算一次无害（fileSource 已做指纹去重，D02-3）。

### D03-5 验收策略（纯内存 + 临时文件，零外部基础设施）

1. **消费契约单测**（`internal/dispatcher`）：给定快照，覆盖 mq 通配/精确命中、topic/tag/subscriber 三级暂停短路、canal 维度、订阅方解引用、空路由。`-race` 并发解析 + 后台 Reload 交叠。
2. **热更新链路演练**（`internal/config` + `internal/dispatcher`）：临时目录写 `routing.yaml` → `NewFileSource` → `RunWatch` → 断言首载命中；改写文件（含 rename 保存形态）增加一条路由 + 暂停一个 subscriber → 沉降 → 断言 `Resolve*` 输出随之变化、全程无重启。
3. **异常分支**：首载坏配置 → `RunWatch` 返回 error；运行期改坏配置 → 保留旧路由表、`Resolve*` 仍命中旧值（校验 D02-4 失败策略在消费侧的体现）。
4. **benchmark**（规范第 6 节）：`ResolveMQ` 热点路径（每条消息触发）建基线，目标零/极低分配（沿用 D02 `MatchMQ` 51ns/0 alloc 量级）。
5. **覆盖率**：新增包 `internal/dispatcher` 与 `internal/config/reload.go` 纳入 `go test -race ./internal/... ./pkg/...`，核心逻辑 ≥ 85%（规范第 7 节）。

## 状态勾点（关键里程碑）

- [ ] `internal/dispatcher/router.go`：`Router` + `ResolveMQ`/`ResolveCanal` + `Target`，暂停三级判定 + 订阅方解引用
- [ ] 消费契约单测全绿（mq 通配/精确、三级暂停、canal、空路由、解引用失败）+ `-race` 并发解析×Reload 交叠
- [ ] `internal/config/reload.go`：`RunWatch(ctx, src, store)`——首载 fail-fast、后台消费 goroutine（panic recover + ctx 退出 + channel 关闭无泄漏）、复用 Load/Reload 管线
- [ ] `RunWatch` 单测：正常首载 + 首载坏配置报错 + 运行期坏配置保留旧表（断言旧路由仍命中）
- [ ] 端到端热更新演练：临时 `routing.yaml` 驱动 `NewFileSource→RunWatch→Resolve*`，改文件（含 rename）即时改变解析结果，无重启
- [ ] `ResolveMQ` benchmark 建立（-benchmem），分配贴合 MatchMQ 量级
- [ ] 全套验证通过：`go build ./...`、`go vet ./...`、`staticcheck ./...`、`go test -race ./internal/... ./pkg/...`、覆盖率 ≥ 85%
- [ ] `docs/分布式消息总线架构设计.md` 6.10/13.1 与本切片口径核对（依赖方向、里程碑偏离注记）
- [ ] 用户验收确认

## 验收结论

（待验收完成后填写）
