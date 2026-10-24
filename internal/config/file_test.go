package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// writeRoutingFile 在临时目录写入一份由 newValidConfig 序列化的 yaml，返回文件路径。
func writeRoutingFile(t *testing.T, dir, name string, cfg *Config) string {
	t.Helper()
	b, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, b, 0o644))
	return p
}

// nextEvent 在超时内等待一个事件，返回是否收到。
func nextEvent(ch <-chan ConfigEvent, within time.Duration) (ConfigEvent, bool) {
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(within):
		return ConfigEvent{}, false
	}
}

func TestFileSource_Load(t *testing.T) {
	dir := t.TempDir()
	p := writeRoutingFile(t, dir, "routing.yaml", newValidConfig())

	src := NewFileSource(p)
	cfg, err := src.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, cfg.Routes, 3)
	assert.Equal(t, "s-erp", cfg.Routes[0].Subscribers[0])
}

func TestFileSource_LoadErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("文件不存在", func(t *testing.T) {
		_, err := NewFileSource(filepath.Join(t.TempDir(), "missing.yaml")).Load(ctx)
		assert.Error(t, err)
	})
	t.Run("空文件视为非法", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "empty.yaml")
		require.NoError(t, os.WriteFile(p, []byte(""), 0o644))
		_, err := NewFileSource(p).Load(ctx)
		assert.ErrorIs(t, err, ErrEmptyConfigFile)
	})
	t.Run("坏 yaml", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "bad.yaml")
		require.NoError(t, os.WriteFile(p, []byte("listeners: [ this is: : not: yaml"), 0o644))
		_, err := NewFileSource(p).Load(ctx)
		assert.Error(t, err)
	})
	t.Run("校验失败的内容", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "ghost.yaml")
		require.NoError(t, os.WriteFile(p, []byte("routes:\n- id: r\n  match: {topic: t}\n  subscribers: [ghost]\n"), 0o644))
		_, err := NewFileSource(p).Load(ctx)
		assert.Error(t, err)
	})
}

// TestFileSource_Watch_ReloadOnChange 修改文件后在防抖到期收到 Reload 事件。
func TestFileSource_Watch_ReloadOnChange(t *testing.T) {
	dir := t.TempDir()
	p := writeRoutingFile(t, dir, "routing.yaml", newValidConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := NewFileSource(p, WithDebounce(30*time.Millisecond))
	ch, err := src.Watch(ctx)
	require.NoError(t, err)

	// 变更内容（新增一个订阅方并改 endpoint），触发一次 Reload
	mod := newValidConfig()
	mod.Subscribers = append(mod.Subscribers, newSubscriber("s-new", "http://new.internal/hook"))
	mod.Routes = append(mod.Routes, &Route{ID: "r-new", Match: MatchRule{Topic: "order-topic"}, Subscribers: []string{"s-new"}})
	require.NoError(t, os.WriteFile(p, mustYaml(t, mod), 0o644))

	ev, ok := nextEvent(ch, 2*time.Second)
	require.True(t, ok, "文件变更应在超时内产生事件")
	assert.Equal(t, "file", ev.Source)
	assert.Equal(t, ConfigOpReload, ev.Op)
}

// TestFileSource_Watch_RenameSave 覆盖编辑器「写临时文件 + rename 替换」场景（D02-3 监听目录的动因）。
func TestFileSource_Watch_RenameSave(t *testing.T) {
	dir := t.TempDir()
	p := writeRoutingFile(t, dir, "routing.yaml", newValidConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := NewFileSource(p, WithDebounce(30*time.Millisecond))
	ch, err := src.Watch(ctx)
	require.NoError(t, err)

	// 原子替换：临时文件 rename 覆盖目标
	mod := newValidConfig()
	mod.Listeners[0].Group = "g-order-v2"
	tmp := filepath.Join(dir, "routing.yaml.tmp")
	require.NoError(t, os.WriteFile(tmp, mustYaml(t, mod), 0o644))
	require.NoError(t, os.Rename(tmp, p))

	_, ok := nextEvent(ch, 2*time.Second)
	assert.True(t, ok, "rename 保存应被目录监听捕获并产生事件")
}

// TestFileSource_Watch_DedupSameContent 指纹去重：写入内容无实质变化不产生事件。
func TestFileSource_Watch_DedupSameContent(t *testing.T) {
	dir := t.TempDir()
	data := mustYaml(t, newValidConfig()) // 同一份字节写两次，避免因时间字段产生指纹假差异
	p := filepath.Join(dir, "routing.yaml")
	require.NoError(t, os.WriteFile(p, data, 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := NewFileSource(p, WithDebounce(20*time.Millisecond))
	ch, err := src.Watch(ctx)
	require.NoError(t, err)
	// 沉降：等 watchLoop 完成起始指纹读取，避免与紧随的写入在 truncate 瞬间竞态
	time.Sleep(100 * time.Millisecond)

	// 用相同内容再写一次（touch 语义）
	require.NoError(t, os.WriteFile(p, data, 0o644))

	_, ok := nextEvent(ch, 300*time.Millisecond)
	assert.False(t, ok, "内容未变化不得产生 Reload 事件")
}

// TestFileSource_Watch_IgnoresOtherFiles 目录内其它文件的变更被按名过滤忽略。
func TestFileSource_Watch_IgnoresOtherFiles(t *testing.T) {
	dir := t.TempDir()
	p := writeRoutingFile(t, dir, "routing.yaml", newValidConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := NewFileSource(p, WithDebounce(20*time.Millisecond))
	ch, err := src.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "unrelated.txt"), []byte("hello"), 0o644))
	_, ok := nextEvent(ch, 300*time.Millisecond)
	assert.False(t, ok, "非目标文件不得触发事件")
}

// TestFileSource_Watch_CtxCancelClosesChannel 取消 ctx → watchLoop 退出并关闭 channel，无孤儿 goroutine。
func TestFileSource_Watch_CtxCancelClosesChannel(t *testing.T) {
	dir := t.TempDir()
	p := writeRoutingFile(t, dir, "routing.yaml", newValidConfig())

	ctx, cancel := context.WithCancel(context.Background())
	src := NewFileSource(p, WithDebounce(20*time.Millisecond))
	ch, err := src.Watch(ctx)
	require.NoError(t, err)

	cancel()

	// channel 应在取消后关闭（range 收敛到 !ok）
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // 已关闭，watchLoop 确认退出
			}
			// 收到残留事件则继续等待关闭
		case <-deadline:
			t.Fatal("ctx 取消后 channel 未在规定时限内关闭")
		}
	}
}

// TestFileSource_Options 选项构造与零/负值回退默认（覆盖 With* 分支）。
func TestFileSource_Options(t *testing.T) {
	f := NewFileSource("x.yaml",
		WithDebounce(99*time.Millisecond),
		WithLoadTimeout(7*time.Second)).(*fileSource)
	assert.Equal(t, 99*time.Millisecond, f.debounce)
	assert.Equal(t, 7*time.Second, f.loadTimeout)

	// 非正值忽略，保持默认
	f2 := NewFileSource("y.yaml", WithDebounce(0), WithLoadTimeout(-1)).(*fileSource)
	assert.Equal(t, defaultDebounce, f2.debounce)
	assert.Equal(t, defaultLoadTimeout, f2.loadTimeout)
}

// TestFileSource_Watch_BadDir Watch 监听不存在的目录应即时返回 error（不静默进 goroutine）。
func TestFileSource_Watch_BadDir(t *testing.T) {
	src := NewFileSource(filepath.Join(t.TempDir(), "nope", "routing.yaml"))
	_, err := src.Watch(context.Background())
	assert.Error(t, err)
}

func mustYaml(t *testing.T, cfg *Config) []byte {
	t.Helper()
	b, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	return b
}

// TestShippedRoutingTemplate 验证入库模板 conf/routing.yaml.example 复制即用（能通过解析+校验）。
func TestShippedRoutingTemplate(t *testing.T) {
	src := NewFileSource(filepath.Join("..", "..", "conf", "routing.yaml.example"))
	cfg, err := src.Load(context.Background())
	require.NoError(t, err, "入库模板必须自洽可加载")
	assert.NotEmpty(t, cfg.Routes)
}
