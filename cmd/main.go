// Command message-bus 消息总线服务入口。
//
// 本包只做依赖装配与启动（规范第 9 节：cmd/ 不写业务实现），
// 负责 zap 日志初始化、SIGINT/SIGTERM 优雅关闭编排与 panic 保护的服务启动。
package main

import (
	"os"

	"message-bus/internal/bootstrap"
	"message-bus/internal/global"
)

func main() {
	// 解析启动参数
	global.InitFlag()
	if !global.Flag() {
		return
	}
	// 初始化配置
	global.MustInitAppConf(global.Param.C)

	// 初始化全局日志（关闭函数交由 WaitingExit 作为清理步骤执行，
	// 因 os.Exit 会绕过 defer，不能用 defer logSync()）
	logSync := bootstrap.MustInitLog()

	// 心跳状态检测
	bootstrap.Heartbeat()

	// 等待退出：收到信号后按 panic 隔离 + 超时兜底执行清理，再按退出码结束
	os.Exit(bootstrap.WaitingExit(logSync))
}
