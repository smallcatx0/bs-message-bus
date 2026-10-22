package safego

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestGoPanicRecovered 验证任务 panic 时被恢复并记录日志，进程不崩溃。
func TestGoPanicRecovered(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	logger := zap.New(core)

	var wg sync.WaitGroup
	wg.Add(1)
	Go(logger, "panic-task", func() {
		defer wg.Done()
		panic("boom")
	})
	wg.Wait()
	// 步骤1：等待 recover 日志写入（observer 同步写入，短暂让出调度即可）
	time.Sleep(10 * time.Millisecond)

	if logs.Len() != 1 {
		t.Fatalf("期望记录 1 条 panic 日志，实际 %d 条", logs.Len())
	}
	entry := logs.All()[0]
	if entry.ContextMap()["task"] != "panic-task" {
		t.Errorf("日志缺少任务上下文: %v", entry.ContextMap())
	}
}

// TestGoNormalFinish 验证正常任务体不受 recover 封装影响。
func TestGoNormalFinish(t *testing.T) {
	logger := zap.NewNop()

	done := make(chan struct{})
	Go(logger, "normal-task", func() { close(done) })

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("正常任务未在预期时间内完成")
	}
}
