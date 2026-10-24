package config

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSource 可控 ConfigSource：精确驱动 RunWatch 的首载/事件时序，不依赖文件系统。
// 字段经 mu 保护——RunWatch 后台消费 goroutine 会读，测试主 goroutine 会写（-race 必需）。
type mockSource struct {
	mu      sync.Mutex
	cfg     *Config
	loadErr error
	ch      chan ConfigEvent
}

func newMockSource(cfg *Config) *mockSource {
	return &mockSource{cfg: cfg, ch: make(chan ConfigEvent, 4)}
}

func (m *mockSource) set(cfg *Config, loadErr error) {
	m.mu.Lock()
	m.cfg, m.loadErr = cfg, loadErr
	m.mu.Unlock()
}

func (m *mockSource) fire() { m.ch <- ConfigEvent{Source: "mock", Op: ConfigOpReload} }

func (m *mockSource) Load(context.Context) (*Config, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return cloneConfig(m.cfg)
}

// Watch 返回测试持有的 channel，由测试 close 收尾（不响应 ctx，专测消费循环逻辑）。
func (m *mockSource) Watch(context.Context) (<-chan ConfigEvent, error) { return m.ch, nil }

// danglingConfig 构造 Load 可通过（mock 不校验）但 Reload 校验失败的配置：引用不存在的订阅方。
func danglingConfig() *Config {
	cfg := newValidConfig()
	cfg.Routes[0].Subscribers = []string{"ghost-subs"}
	return cfg
}

func TestRunWatch_InitialBuildsTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := newMockSource(newValidConfig())
	store := NewTableStore()
	require.NoError(t, RunWatch(ctx, m, store))

	// 首载同步建表：RunWatch 返回即可读到路由
	assert.NotEmpty(t, store.Snapshot().MatchMQ("order-topic", "created"))
	close(m.ch)
}

func TestRunWatch_InitialLoadErrorFailFast(t *testing.T) {
	m := newMockSource(nil)
	m.set(nil, errors.New("boom"))
	store := NewTableStore()
	// 首载读取失败 → 立即返回 error，不启动 Watch（不建后台 goroutine）
	require.Error(t, RunWatch(context.Background(), m, store))
}

func TestRunWatch_InitialReloadErrorFailFast(t *testing.T) {
	m := newMockSource(danglingConfig()) // Load 返回结构错配置 → 首次 Reload 校验失败
	store := NewTableStore()
	require.Error(t, RunWatch(context.Background(), m, store))
	// fail-fast 前不产生半成品表：仍为空表
	assert.Empty(t, store.Snapshot().Listeners())
}

func TestRunWatch_RuntimeBadConfigKeepsOldTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := newMockSource(newValidConfig())
	store := NewTableStore()
	require.NoError(t, RunWatch(ctx, m, store))
	before := routeIDs(store.Snapshot().MatchMQ("order-topic", "created"))
	require.NotEmpty(t, before)

	m.set(danglingConfig(), nil) // 运行期改坏配置
	m.fire()

	time.Sleep(50 * time.Millisecond) // 等事件被消费
	// Reload 校验失败 → 保留旧表 → 旧路由仍原样命中（D02-4）
	assert.Equal(t, before, routeIDs(store.Snapshot().MatchMQ("order-topic", "created")))
	close(m.ch)
}

func TestRunWatch_LoadErrorKeepsConsumerAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := newMockSource(newValidConfig())
	store := NewTableStore()
	require.NoError(t, RunWatch(ctx, m, store))

	// 1) 瞬时读取错误：消费循环应记日志后 continue，不退出
	m.set(nil, errors.New("transient read error"))
	m.fire()
	time.Sleep(30 * time.Millisecond)

	// 2) 恢复并追加一条精确 tag 路由：若消费循环仍存活，新路由应生效
	updated := newValidConfig()
	updated.Routes = append(updated.Routes, &Route{
		ID:          "r-extra",
		Match:       MatchRule{Topic: "order-topic", Tag: "shipped"},
		Subscribers: []string{"s-erp"},
	})
	m.set(updated, nil)
	m.fire()

	require.Eventually(t, func() bool {
		for _, r := range store.Snapshot().MatchMQ("order-topic", "shipped") {
			if r.ID == "r-extra" {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond, "瞬时 Load 错误后消费循环应仍处理后续事件")

	close(m.ch)
}
