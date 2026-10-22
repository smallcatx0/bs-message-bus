// Package safego 提供带 panic 恢复的 goroutine 启动封装。
//
// 存在原因：项目规范第 5 节要求每启动一个 goroutine 均须做 panic recover，
// 避免单个 goroutine 的 panic 崩掉整个进程；errgroup 不会拦截任务内部的 panic，
// 因此统一封装并在 recover 后用 zap 记录堆栈与任务上下文。
package safego

import (
	"runtime/debug"

	"go.uber.org/zap"
)

// Go 以 panic 保护方式启动一个 goroutine。
//
// name 为任务名（必须是有辨识度的静态字符串，便于日志检索），
// fn 为任务体。若 fn panic，错误与堆栈会被记录，进程不受影响。
func Go(logger *zap.Logger, name string, fn func()) {
	go func() {
		// 步骤1：defer recover，将 panic 转化为错误日志，保证进程存活
		defer func() {
			if r := recover(); r != nil {
				logger.Error("goroutine panic 已恢复",
					zap.String("task", name),
					zap.Any("panic", r),
					zap.ByteString("stack", debug.Stack()),
				)
			}
		}()
		// 步骤2：执行任务体
		fn()
	}()
}
