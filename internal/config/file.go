package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"message-bus/pkg/logger"
)

// fileSource 默认参数（D02-3）。
const (
	defaultDebounce    = 200 * time.Millisecond
	defaultLoadTimeout = 3 * time.Second
)

// FileOption 定制 fileSource 参数；不提供即取默认值（规范第 4 节：构造函数 + 合理默认）。
type FileOption func(*fileSource)

// WithDebounce 设置事件防抖窗口：合并一次保存产生的多事件，只触发一次 Reload。
func WithDebounce(d time.Duration) FileOption {
	return func(f *fileSource) {
		if d > 0 {
			f.debounce = d
		}
	}
}

// WithLoadTimeout 设置单次读取（含文件 IO）的显式超时，防异常挂载点永久阻塞（规范第 5 节）。
func WithLoadTimeout(d time.Duration) FileOption {
	return func(f *fileSource) {
		if d > 0 {
			f.loadTimeout = d
		}
	}
}

// fileSource 本地 yaml + fsnotify 来源：V1 唯一落地的 ConfigSource（蓝本 7.3）。
type fileSource struct {
	path        string
	debounce    time.Duration
	loadTimeout time.Duration
}

// NewFileSource 构造文件来源。path 为路由配置文件绝对/相对路径；
// 构造期不读文件（读取发生在 Load/Watch），仅规范化路径。
func NewFileSource(path string, opts ...FileOption) ConfigSource {
	f := &fileSource{
		path:        filepath.Clean(path),
		debounce:    defaultDebounce,
		loadTimeout: defaultLoadTimeout,
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

// Load 读取并校验当前完整配置：readFile（带超时）→ yaml 解析 → Validate。
// 与热载复用同一管线（D02-2），首载与热载行为一致。
func (f *fileSource) Load(ctx context.Context) (*Config, error) {
	raw, err := f.read(ctx)
	if err != nil {
		return nil, err
	}
	return parseAndValidate(raw)
}

// Watch 监听配置文件所在**目录**（而非文件本身）——编辑器「写临时文件 + rename」
// 会使被监听 inode 消失，监目录才可靠（D02-3）；事件按目标文件名过滤。
//
// goroutine 生命周期（规范第 5 节）：
//   - ctx 取消 → 停 debounce ticker → 关 watcher → 关输出 channel，无孤儿 goroutine；
//   - panic → defer recover 记堆栈与上下文后关闭 channel（不静默复活，交装配层决定重启/退出）。
func (f *fileSource) Watch(ctx context.Context) (<-chan ConfigEvent, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("config: 创建 fsnotify watcher 失败: %w", err)
	}
	// 步骤1：监听目录（提前在 Watch 内 Add，保证启动失败即时返回 error 而非静默进 goroutine）
	dir := filepath.Dir(f.path)
	if err = watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("config: 监听目录 %q 失败: %w", dir, err)
	}

	out := make(chan ConfigEvent, 1)
	go f.watchLoop(ctx, watcher, out)
	return out, nil
}

// watchLoop 监听主循环：防抖合并 + 指纹去重 + 显式退出。
func (f *fileSource) watchLoop(ctx context.Context, watcher *fsnotify.Watcher, out chan<- ConfigEvent) {
	// 步骤1：panic 隔离——恢复后关闭 out，交由装配层感知来源终止
	defer func() {
		if r := recover(); r != nil {
			logger.L().Error("config: fileSource watch goroutine panic",
				zap.String("path", f.path), zap.Any("panic", r), zap.Stack("stack"))
		}
		// 统一收尾：停 watcher、关 channel（消费端 range 退出，不特判来源类型）
		_ = watcher.Close()
		close(out)
	}()

	target := filepath.Base(f.path)
	debounce := time.NewTimer(f.debounce)
	debounce.Stop() // 初始不计时，收到首个相关事件再启动
	pending := false

	// lastFP 起始指纹：与防抖到期后重读的内容比对，跳过无实质变化（编辑器 touch 等）
	lastFP, _ := f.fingerprint(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case ev, ok := <-watcher.Events:
			if !ok {
				return
			}
			// 过滤非目标文件；rename 保存产生的临时文件/目标重命名均按 base 匹配
			if filepath.Base(ev.Name) != target {
				continue
			}
			// 重启防抖计时：窗口内的连续事件合并为一次 Reload
			if !pending {
				debounce.Reset(f.debounce)
				pending = true
			}

		case err, ok := <-watcher.Errors:
			if ok && err != nil {
				logger.L().Warn("config: fsnotify 错误", zap.String("path", f.path), zap.Error(err))
			}

		case <-debounce.C:
			pending = false
			// 单读：一次 Load 同时用于指纹比对与变更判定，避免重复文件 IO
			fp, err := f.fingerprint(ctx)
			if err != nil {
				// 读取/解析失败：保留旧配置（不发事件），仅告警；下次保存再尝试
				logger.L().Error("config: 热载解析失败，保留当前配置",
					zap.String("path", f.path), zap.Error(err))
				continue
			}
			if fp == lastFP {
				continue // 内容未实质变化
			}
			lastFP = fp
			f.notify(out)
		}
	}
}

// notify 向 out 发 Reload 事件。发送带 default 兜底：缓冲满说明确有未消费变更，
// 丢弃本次冗余通知不影响正确性（Reload 幂等且全量）。
func (f *fileSource) notify(out chan<- ConfigEvent) {
	select {
	case out <- ConfigEvent{Source: "file", Op: ConfigOpReload}:
	default:
		logger.L().Warn("config: 变更通知积压，丢弃冗余事件（Reload 幂等）", zap.String("path", f.path))
	}
}

// fingerprint 读取并计算当前配置指纹，供 watchLoop 起始与变更后比对。
func (f *fileSource) fingerprint(ctx context.Context) (uint64, error) {
	cfg, err := f.Load(ctx)
	if err != nil {
		return 0, err
	}
	return Fingerprint(cfg)
}

// read 在 loadTimeout 内读取文件；超时返回明确 error 而非阻塞（规范第 5 节）。
//
// 决策：os.ReadFile 本身不接受 context，用独立 goroutine + select 实现可取消读取；
// goroutine 有明确终止（读取系统调用返回即结束），不违反第 5 节孤儿约束。
func (f *fileSource) read(ctx context.Context) ([]byte, error) {
	type res struct {
		b   []byte
		err error
	}
	cctx, cancel := context.WithTimeout(ctx, f.loadTimeout)
	defer cancel()

	ch := make(chan res, 1)
	go func() {
		b, err := os.ReadFile(f.path)
		ch <- res{b, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("config: 读取 %q 失败: %w", f.path, r.err)
		}
		return r.b, nil
	case <-cctx.Done():
		return nil, fmt.Errorf("config: 读取 %q 超时: %w", f.path, cctx.Err())
	}
}

// ErrEmptyConfigFile 空文件（无任何键）视为非法，避免误清空路由表。
var ErrEmptyConfigFile = errors.New("config: 配置文件为空")

// parseAndValidate yaml 解析 + 结构校验，Load 与热载共用。
func parseAndValidate(raw []byte) (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("config: yaml 解析失败: %w", err)
	}
	if len(cfg.Listeners) == 0 && len(cfg.Routes) == 0 && len(cfg.Subscribers) == 0 && len(cfg.Pauses) == 0 {
		return nil, ErrEmptyConfigFile
	}
	// 敏感值已在结构体内，解析/校验错误文本只含 ID、键名与 endpoint，不含 token/secret 明文
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
