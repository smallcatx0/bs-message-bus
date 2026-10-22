package bus

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestRunAndShutdown 验证 Run 启动 worker 后，取消 ctx 并 Shutdown 能等到 worker 退出。
func TestRunAndShutdown(t *testing.T) {
	b := New(zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	// 步骤1：触发取消，模拟收到退出信号
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run 期望返回 nil，实际: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未在 ctx 取消后返回")
	}

	// 步骤2：Shutdown 应在超时期限内等到 worker 退出
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := b.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown 失败: %v", err)
	}
}

// TestShutdownTimeout 验证 worker 未退出且关闭期限到达时，Shutdown 返回超时而非阻塞。
func TestShutdownTimeout(t *testing.T) {
	b := New(zap.NewNop())
	// workerDone 未关闭，模拟 worker 仍在运行的场景
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := b.Shutdown(shutdownCtx); err == nil {
		t.Fatal("期望 Shutdown 返回超时错误，实际为 nil")
	}
}
