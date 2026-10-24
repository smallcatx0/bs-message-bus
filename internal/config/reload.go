package config

import (
	"context"

	"go.uber.org/zap"

	"message-bus/pkg/logger"
)

// RunWatch 承接标准装配管线（D03-4，对齐 example_test.go 形态）：
// 把任意 ConfigSource 的首载与热更新接到 TableStore 上，来源无关——
// fileSource / staticSource 与未来 grpcSource 复用同一函数（蓝本 6.8、D02-9）。
//
// 流程：
//  1. src.Load → 首次 store.Reload 建表；任一步失败 fail-fast 返回 error，
//     不启动后台监听（启动期建不起表属硬伤，交由装配层决定退出或重试）；
//  2. src.Watch 启动事件流；
//  3. 独立 goroutine 消费事件：每事件重走 src.Load → store.Reload 全量管线
//     （校验、失败保留旧表、脱敏日志均在 Reload 内，D02-4）。
//
// 生命周期（规范第 5 节）：RunWatch 同步完成首载后启动后台消费 goroutine 并立即返回；
// 取消 ctx → src.Watch 内部 goroutine 关闭事件 channel → 消费循环 range 自然退出，无孤儿。
// 停止的唯一信号即事件 channel 关闭（含来源 panic 终止，D02-3），本函数不特判来源类型。
func RunWatch(ctx context.Context, src ConfigSource, store TableStore) error {
	// 步骤1：首载 fail-fast
	cfg, err := src.Load(ctx)
	if err != nil {
		return err
	}
	if err = store.Reload(cfg); err != nil {
		return err
	}
	// 步骤2：启动事件流
	ch, err := src.Watch(ctx)
	if err != nil {
		return err
	}
	// 步骤3：后台消费，热载失败保留旧表不退出
	go consumeWatch(ctx, src, store, ch)
	return nil
}

// consumeWatch 消费 ConfigEvent 驱动全量 Reload。
// 独立成函数便于 panic recover 边界清晰（规范第 5 节）。
func consumeWatch(ctx context.Context, src ConfigSource, store TableStore, ch <-chan ConfigEvent) {
	defer func() {
		if r := recover(); r != nil {
			logger.L().Error("config: RunWatch 消费 goroutine panic",
				zap.Any("panic", r), zap.Stack("stack"))
		}
		logger.L().Info("config: RunWatch 消费循环退出（来源事件 channel 已关闭）")
	}()
	for range ch {
		// Reload 幂等且全量：debounce/背压导致的冗余或丢失事件不影响最终一致（D02-3）
		cfg, err := src.Load(ctx)
		if err != nil {
			// Load 自身不记日志，此处补记；保留当前表，不退出
			logger.L().Error("config: 热载读取失败，保留当前路由表", zap.Error(err))
			continue
		}
		// Reload 内部已对失败记 ERROR 并保留旧表，成功记 INFO，此处不再重复记录
		if err = store.Reload(cfg); err != nil {
			continue
		}
	}
}
