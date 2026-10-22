package bootstrap

import (
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_runCleanup_recoversPanic(t *testing.T) {
	// panic 的清理函数应被隔离：runCleanup 返回 false，且不让测试进程崩溃
	ok := runCleanup(0, func() { panic("boom") })
	assert.False(t, ok, "panic 的清理应判为失败")

	// 正常清理返回 true
	ok = runCleanup(1, func() {})
	assert.True(t, ok)
}

func Test_cleanupWithTimeout_continuesAfterPanic(t *testing.T) {
	var ran int32
	code := cleanupWithTimeout(time.Second,
		func() { panic("first fails") },
		nil, // nil 应被跳过
		func() { atomic.AddInt32(&ran, 1) },
	)
	assert.Equal(t, 1, code, "存在失败清理时整体退出码应为 1")
	assert.Equal(t, int32(1), atomic.LoadInt32(&ran), "panic 不应中断后续清理执行")
}

func Test_cleanupWithTimeout_allSuccess(t *testing.T) {
	code := cleanupWithTimeout(time.Second, func() {}, func() {})
	assert.Equal(t, 0, code)
}

func Test_cleanupWithTimeout_returnsQuicklyOnSuccess(t *testing.T) {
	var doneFlag int32
	code := cleanupWithTimeout(time.Second, func() { atomic.StoreInt32(&doneFlag, 1) })
	assert.Equal(t, 0, code)
	assert.Equal(t, int32(1), atomic.LoadInt32(&doneFlag))
}

func Test_cleanupWithTimeout_forcesExitOnHang(t *testing.T) {
	// 清理函数阻塞远超时限，应在超时后返回 1，而不是永久等待
	blocked := make(chan struct{})
	defer close(blocked) // 放行 hang 住的 goroutine，避免测试结束后泄漏
	hanging := func() { <-blocked }

	start := time.Now()
	code := cleanupWithTimeout(50*time.Millisecond, hanging)
	elapsed := time.Since(start)

	assert.Equal(t, 1, code, "超时应返回非 0 退出码")
	assert.Less(t, elapsed, 1*time.Second, "应在时限附近返回，而非等待清理完成")
}

func Test_waitForSignal_gracefulPath(t *testing.T) {
	quit := make(chan os.Signal, 1)
	var exitCalled int32
	forceExit := func(int) { atomic.StoreInt32(&exitCalled, 1) }

	// 预投首个信号（正常关闭），二次信号不触发
	quit <- syscall.SIGTERM

	var cleaned int32
	code := waitForSignal(quit, time.Second, forceExit, func() { atomic.StoreInt32(&cleaned, 1) })

	assert.Equal(t, 0, code, "清理成功应为退出码 0")
	assert.Equal(t, int32(1), atomic.LoadInt32(&cleaned), "清理函数应被执行")
	assert.Equal(t, int32(0), atomic.LoadInt32(&exitCalled), "正常路径不应触发强制退出")
}

func Test_waitForSignal_secondSignalForcesExit(t *testing.T) {
	quit := make(chan os.Signal, 1)
	forced := make(chan int, 1)
	forceExit := func(code int) { forced <- code }

	blocked := make(chan struct{})
	quit <- syscall.SIGINT // 首个信号（正常投递，触发清理编排）

	// 清理 hang 住期间，由另一 goroutine 并发投递二次信号，看门狗应抢先强退
	go func() {
		time.Sleep(50 * time.Millisecond)
		quit <- syscall.SIGINT
	}()

	done := make(chan int, 1)
	go func() {
		done <- waitForSignal(quit, 5*time.Second, forceExit, func() { <-blocked })
	}()

	select {
	case got := <-forced:
		assert.Equal(t, 1, got, "二次信号应触发 forceExit(1)")
	case <-time.After(2 * time.Second):
		t.Fatal("二次信号未触发强制退出")
	}

	close(blocked) // 释放 hang 住的清理，使 waitForSignal 正常返回，避免 goroutine 泄漏
	select {
	case code := <-done:
		require.True(t, code == 0 || code == 1)
	case <-time.After(2 * time.Second):
		t.Fatal("waitForSignal 未在清理放行后返回")
	}
}
