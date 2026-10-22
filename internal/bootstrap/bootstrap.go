package bootstrap

/**
初始化文件
*/
import (
	"fmt"
	"message-bus/internal/global"
	"message-bus/pkg/logger"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

// 日志写入介质的配置取值（配置语义属于装配层，不放入 pkg/logger）
const mediumFile = "file"

// shutdownSignals 需要触发优雅关闭的信号集合（跨平台编译安全：
// Windows 下 SIGHUP/SIGQUIT 有定义但不投递，仅作预留不构错）。
var shutdownSignals = []os.Signal{
	syscall.SIGHUP,
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGQUIT,
}

// shutdownTimeout 优雅关闭总时限（秒），读配置 shutdown_timeout，
// 缺省 30s；非正值回退缺省（规范第 8 节：等待必须有显式期限）。
func shutdownTimeout() time.Duration {
	sec := global.AppConf.GetInt("shutdown_timeout")
	if sec <= 0 {
		sec = 30
	}
	return time.Duration(sec) * time.Second
}

// MustInitLog 初始化日志
func MustInitLog() func() {
	conf := global.AppConf
	var err error

	// 从配置文件中获取日志参数
	filename := conf.GetString("log.path")
	if filename == "" {
		panic("log.path 未配置")
	}
	level := conf.GetString("log.level")
	logType := conf.GetString("log.type")

	var h *logger.Holder
	if logType == mediumFile {
		// rotatelogs 不会自动建目录，先确保日志目录存在（否则首次启动 panic）
		if dir := filepath.Dir(filename); dir != "" && dir != "." {
			if err = os.MkdirAll(dir, 0o755); err != nil {
				panic(err)
			}
		}
		h, err = logger.NewFileLogger(filename, level)
	} else {
		h, err = logger.NewStdLogger(level)
	}
	if err != nil {
		panic(err)
	}
	// 装载全局实例并登记等级句柄（并发安全由 zap.ReplaceGlobals 保证）
	logger.SetGlobal(h)
	return func() {
		// Holder.Close 内部已先 Sync 再关 writer
		_ = h.Close()
	}
}

// Heartbeat 心跳状态检测
func Heartbeat() {
	defer func() {
		if r := recover(); r != nil {
			logger.L().Error("Heartbeat goroutine run panic " + fmt.Sprint(r))
		}
	}()
	if !global.AppConf.GetBool("metrics") {
		return
	}
	dt := global.AppConf.GetInt("metrics_dt")
	if dt == 0 {
		dt = 10
	}
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(dt))
		defer ticker.Stop()
		for range ticker.C {
			logger.SysStatInfo(logger.L())
		}
	}()
}

// WaitingExit 阻塞直到收到终止信号，随后带 panic 隔离与超时兜底地执行清理函数，
// 返回进程退出码（由调用方交给 os.Exit）：
//   - 清理全部成功且在时限内 → 0
//   - 任一清理 panic 或整体超时 → 1
//   - 清理期间收到二次信号 → 看门狗直接 forceExit(1)
func WaitingExit(funs ...func()) int {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, shutdownSignals...)
	return waitForSignal(quit, shutdownTimeout(), os.Exit, funs...)
}

// waitForSignal 可测核心：从 quit 收取首个信号后编排清理与强制退出。
// forceExit 注入以便单测不真正终止进程；真实装配传 os.Exit。
func waitForSignal(quit chan os.Signal, timeout time.Duration, forceExit func(int), funs ...func()) int {
	sig := <-quit
	// 步骤1：记录触发信号，将优雅/快杀的语义判读交给运维，不再硬编码文案
	logger.L().Info("shutdown signal received: " + sig.String())

	// 步骤2：二次信号看门狗——清理期间再收到信号立即强退，不等 hang 住的清理
	// done 用于清理正常完成后退出监听，避免 goroutine 泄漏（规范第 5 节）
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.L().Error("shutdown watcher panic " + fmt.Sprint(r))
			}
		}()
		select {
		case s := <-quit:
			logger.L().Error("second shutdown signal received, forcing exit: " + s.String())
			_ = logger.L().Sync()
			forceExit(1)
		case <-done:
		}
	}()

	// 步骤3：带超时与 panic 隔离地执行清理，得到退出码
	code := cleanupWithTimeout(timeout, funs...)

	// 步骤4：os.Exit 会绕过 main 的 defer 链，退出前显式 Sync 落盘
	_ = logger.L().Sync()
	signal.Stop(quit)
	return code
}

// cleanupWithTimeout 在独立 goroutine 中顺序执行清理（逐函数 panic 隔离），
// 超时则放弃等待并返回 1。goroutine 边界是超时机制所需：主流程不等 hang 住的清理。
func cleanupWithTimeout(timeout time.Duration, funs ...func()) int {
	done := make(chan int, 1)
	go func() {
		code := 0
		for i, f := range funs {
			if f == nil {
				continue
			}
			// runCleanup 内部已逐函数 recover，无需外层再兑一层
			if !runCleanup(i, f) {
				code = 1
			}
		}
		done <- code
	}()
	select {
	case code := <-done:
		return code
	case <-time.After(timeout):
		logger.L().Error("shutdown cleanup timed out, forcing exit")
		return 1
	}
}

// runCleanup 执行单个清理函数并捕获其 panic，返回是否成功。
// 命名返回值 + defer recover：panic 时 ok 置 false，不影响后续清理。
func runCleanup(i int, f func()) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			logger.L().Error(fmt.Sprintf("cleanup[%d] panic: %v", i, r))
		}
	}()
	f()
	return true
}
