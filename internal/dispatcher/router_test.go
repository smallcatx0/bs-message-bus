package dispatcher_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"message-bus/internal/config"
	"message-bus/internal/dispatcher"
)

// sub 构造一个可通过 Validate 的订阅方。
func sub(id, endpoint string) *config.Subscriber {
	return &config.Subscriber{
		ID:       id,
		Endpoint: endpoint,
		Timeout:  config.Duration(5 * time.Second),
		Retry: config.RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: config.Duration(time.Second),
			MaxBackoff:     config.Duration(time.Minute),
			Multiplier:     2.0,
		},
		Auth: config.CallbackAuth{Mode: config.AuthModeNone},
	}
}

// baseConfig 一份无暂停的基准配置：mq 通配 r-all、mq 精确 r-created、canal 精确 r-canal。
// listeners 声明 order topic，供后续 topic/tag 暂停校验引用（Pause target 须命中 listener）。
func baseConfig() *config.Config {
	return &config.Config{
		Listeners: []*config.Listener{
			{ID: "l-order", Source: config.ListenerSourceMQ, Topic: "order", Group: "g1", Enabled: true},
		},
		Routes: []*config.Route{
			{ID: "r-all", Match: config.MatchRule{Topic: "order"}, Subscribers: []string{"s-erp"}},
			{ID: "r-created", Match: config.MatchRule{Topic: "order", Tag: "created"}, Subscribers: []string{"s-erp", "s-wms"}},
			{ID: "r-canal", Match: config.MatchRule{DB: "shop", Table: "refund", Event: config.EventInsert}, Subscribers: []string{"s-wms"}},
		},
		Subscribers: []*config.Subscriber{
			sub("s-erp", "http://erp.internal/hook"),
			sub("s-wms", "https://wms.internal/hook"),
		},
	}
}

// newTestRouter 建表并注入 Router；Reload 内含校验，测试配置写错会在此即失败。
func newTestRouter(t *testing.T, cfg *config.Config) *dispatcher.Router {
	t.Helper()
	st := config.NewTableStore()
	require.NoError(t, st.Reload(cfg))
	return dispatcher.NewRouter(st)
}

// subIDs 提取目标列表中的订阅方 ID，便于顺序断言。
func subIDs(targets []dispatcher.Target) []string {
	out := make([]string, 0, len(targets))
	for _, tg := range targets {
		out = append(out, tg.Sub.ID)
	}
	return out
}

func TestResolveMQ_FanOutOrder(t *testing.T) {
	r := newTestRouter(t, baseConfig())

	t.Run("topic+命中tag → 通配r-all在前 + 精确r-created在后", func(t *testing.T) {
		targets := r.ResolveMQ("order", "created")
		// r-all:[s-erp] + r-created:[s-erp, s-wms]
		assert.Equal(t, []string{"s-erp", "s-erp", "s-wms"}, subIDs(targets))
		assert.Equal(t, "r-all", targets[0].Route.ID)
		assert.Equal(t, "r-created", targets[1].Route.ID)
	})
	t.Run("topic+未命中tag → 仅通配", func(t *testing.T) {
		assert.Equal(t, []string{"s-erp"}, subIDs(r.ResolveMQ("order", "deleted")))
	})
	t.Run("空tag → 仅通配", func(t *testing.T) {
		assert.Equal(t, []string{"s-erp"}, subIDs(r.ResolveMQ("order", "")))
	})
	t.Run("未知topic → 空", func(t *testing.T) {
		assert.Empty(t, r.ResolveMQ("nope", "x"))
		assert.Empty(t, r.ResolveMQ("", "x"))
	})
}

func TestResolveMQ_PauseScopes(t *testing.T) {
	t.Run("topic 暂停短路整条消息", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Pauses = []*config.Pause{
			{ID: "p-topic", Scope: config.PauseScopeTopic, Target: "order", Expire: 0},
		}
		assert.Empty(t, newTestRouter(t, cfg).ResolveMQ("order", "created"))
	})
	t.Run("tag 暂停短路命中该 tag 的消息", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Pauses = []*config.Pause{
			{ID: "p-tag", Scope: config.PauseScopeTag, Target: "order:created", Expire: 0},
		}
		r := newTestRouter(t, cfg)
		assert.Empty(t, r.ResolveMQ("order", "created"))                          // 命中暂停 tag
		assert.Equal(t, []string{"s-erp"}, subIDs(r.ResolveMQ("order", "other"))) // 其他 tag 不受影响
	})
	t.Run("subscriber 暂停仅剔除该目标", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Pauses = []*config.Pause{
			{ID: "p-sub", Scope: config.PauseScopeSubscriber, Target: "s-wms", Expire: 0},
		}
		// s-wms 被剔，r-created 只剩 s-erp；r-all 的 s-erp 不受影响
		assert.Equal(t, []string{"s-erp", "s-erp"}, subIDs(newTestRouter(t, cfg).ResolveMQ("order", "created")))
	})
	t.Run("暂停已过期（now>Expire）视为不生效", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Pauses = []*config.Pause{
			{ID: "p-topic", Scope: config.PauseScopeTopic, Target: "order", Expire: time.Now().Unix() - 10},
		}
		assert.NotEmpty(t, newTestRouter(t, cfg).ResolveMQ("order", "created"))
	})
}

func TestResolveCanal(t *testing.T) {
	t.Run("canal 精确 event 命中", func(t *testing.T) {
		assert.Equal(t, []string{"s-wms"}, subIDs(newTestRouter(t, baseConfig()).ResolveCanal("shop", "refund", config.EventInsert)))
	})
	t.Run("canal 通配（event 空）命中", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Routes[2].Match.Event = "" // r-canal 转通配
		assert.Equal(t, []string{"s-wms"}, subIDs(newTestRouter(t, cfg).ResolveCanal("shop", "refund", config.EventDelete)))
	})
	t.Run("canal 维度 subscriber 暂停生效", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Pauses = []*config.Pause{
			{ID: "p-sub", Scope: config.PauseScopeSubscriber, Target: "s-wms", Expire: 0},
		}
		assert.Empty(t, newTestRouter(t, cfg).ResolveCanal("shop", "refund", config.EventInsert))
	})
	t.Run("未命中 db.table → 空", func(t *testing.T) {
		r := newTestRouter(t, baseConfig())
		assert.Empty(t, r.ResolveCanal("shop", "other", config.EventInsert))
		assert.Empty(t, r.ResolveCanal("", "refund", config.EventInsert))
	})
}

// TestResolveConcurrentWithReload 并发解析 × 后台热替换交叠，-race 下验证快照读的无锁安全（D02-4）。
func TestResolveConcurrentWithReload(t *testing.T) {
	st := config.NewTableStore()
	require.NoError(t, st.Reload(baseConfig()))
	r := dispatcher.NewRouter(st)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// 读者：高频解析
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = r.ResolveMQ("order", "created")
				_ = r.ResolveCanal("shop", "refund", config.EventInsert)
			}
		}
	}()
	// 写者：反复热替换
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			cfg := baseConfig()
			cfg.Pauses = append(cfg.Pauses, &config.Pause{
				ID: "p-sub", Scope: config.PauseScopeSubscriber, Target: "s-wms", Expire: int64(i),
			})
			require.NoError(t, st.Reload(cfg))
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func BenchmarkResolveMQ(b *testing.B) {
	st := config.NewTableStore()
	if err := st.Reload(baseConfig()); err != nil {
		b.Fatal(err)
	}
	r := dispatcher.NewRouter(st)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.ResolveMQ("order", "created")
	}
}
