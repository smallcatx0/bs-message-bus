// Command message-bus 消息总线服务入口。
//
// 本包只做依赖装配与启动（规范第 9 节：cmd/ 不写业务实现），
// 负责 zap 日志初始化、SIGINT/SIGTERM 优雅关闭编排与 panic 保护的服务启动。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"message-bus/internal/bus"
	"message-bus/pkg/safego"
)

// shutdownTimeout 优雅关闭最长等待时间（规范第 5 节：关闭行为必须显式超时）。
const shutdownTimeout = 15 * time.Second

// main 服务入口：run 返回错误时以非零码退出，错误信息已包含调试上下文。
func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "服务异常退出: %v\n", err)
		os.Exit(1)
	}
}

// run 完成服务生命周期编排：初始化日志 → 启动核心服务 → 等待信号 → 优雅关闭。
func run() error {
	// 步骤1：初始化 zap 结构化日志（规范第 1 节强制选型）
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("初始化 logger 失败: %w", err)
	}
	// 进程退出前刷盘；退出阶段 Sync 错误已无处置意义，显式丢弃并注明原因
	defer func() { _ = logger.Sync() }()

	// 步骤2：监听 SIGINT/SIGTERM，收到信号即取消根 ctx，驱动全链路退出
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	b := bus.New(logger)

	// 步骤3：以 panic 保护方式启动核心服务（规范第 5 节：每个 goroutine 均须 recover）
	errCh := make(chan error, 1)
	safego.Go(logger, "bus-run", func() {
		errCh <- b.Run(ctx)
	})
	logger.Info("message-bus 服务已启动")

	// 步骤4：等待退出信号或服务异常提前退出
	select {
	case <-ctx.Done():
		logger.Info("收到退出信号，开始优雅关闭")
	case err := <-errCh:
		// 服务提前返回时主动取消 ctx，保证 worker 随之退出，不留孤儿 goroutine
		stop()
		logger.Warn("服务提前退出", zap.Error(err))
	}

	// 步骤5：使用独立超时 ctx 等待活跃请求/协程完成并释放资源
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := b.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("优雅关闭失败: %w", err)
	}

	logger.Info("message-bus 已安全退出")
	return nil
}
