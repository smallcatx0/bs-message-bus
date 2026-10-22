// Package bus 消息总线核心服务。
//
// 当前为骨架实现：Run 阻塞运行 worker 直至 context 取消，
// Shutdown 等待活跃 worker 退出，支撑入口的优雅关闭流程。
// 后续消息通道、存储等实现按领域继续在本包内展开。
package bus

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"message-bus/pkg/safego"
)

// Bus 消息总线服务实例。
//
// workerDone 用于 Run 与 Shutdown 之间同步 worker goroutine 生命周期，
// 保证关闭时不会出现孤儿 goroutine（规范第 5 节）。
type Bus struct {
	logger     *zap.Logger
	workerDone chan struct{}
	closeOnce  sync.Once
}

// New 创建一个消息总线服务实例。
//
// logger 必须为项目统一的 zap 结构化日志（规范第 1 节）。
func New(logger *zap.Logger) *Bus {
	return &Bus{
		logger:     logger,
		workerDone: make(chan struct{}),
	}
}

// Run 启动 worker 并阻塞运行，直到 ctx 取消后返回。
//
// worker 通过 safego.Go 启动，满足每个 goroutine 均需 panic recover 的要求。
func (b *Bus) Run(ctx context.Context) error {
	// 步骤1：以 panic 保护方式启动 worker，绑定 ctx 支持取消
	safego.Go(b.logger, "bus-worker", func() {
		defer close(b.workerDone)
		b.worker(ctx)
	})

	// 步骤2：等待取消信号，Run 生命周期与主 context 一致
	<-ctx.Done()
	return nil
}

// worker 是骨架态的消息处理循环，占位消费逻辑。
//
// 严格监听 ctx 退出，禁止空转泄漏 goroutine。
func (b *Bus) worker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("worker 收到取消信号，退出")
			return
		case <-ticker.C:
			// 步骤1：占位轮询，后续在此接入消息拉取与分发
			b.logger.Debug("worker 心跳")
		}
	}
}

// Shutdown 等待活跃 worker 退出，实现优雅关闭。
//
// ctx 必须携带显式超时（规范第 5 节）；超时返回错误而非无限等待。
func (b *Bus) Shutdown(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.logger.Info("等待 worker 退出")
	})

	select {
	case <-b.workerDone:
		b.logger.Info("worker 已退出")
		return nil
	case <-ctx.Done():
		return errors.New("优雅关闭超时，worker 未在期限内退出")
	}
}
