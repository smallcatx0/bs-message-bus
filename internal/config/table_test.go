package config

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustReload(t *testing.T, cfg *Config) TableStore {
	t.Helper()
	st := NewTableStore()
	require.NoError(t, st.Reload(cfg))
	return st
}

// TestMatchMQ 预索引匹配：通配桶 + 精确桶，通配在前、精确在后。
func TestMatchMQ(t *testing.T) {
	nt, err := buildTable(newValidConfig())
	require.NoError(t, err)

	t.Run("topic+命中tag → 通配r-all + 精确r-created", func(t *testing.T) {
		got := routeIDs(nt.MatchMQ("order-topic", "created"))
		assert.Equal(t, []string{"r-all", "r-created"}, got)
	})
	t.Run("topic+未命中tag → 仅通配", func(t *testing.T) {
		got := routeIDs(nt.MatchMQ("order-topic", "deleted"))
		assert.Equal(t, []string{"r-all"}, got)
	})
	t.Run("空tag查询 → 仅通配", func(t *testing.T) {
		got := routeIDs(nt.MatchMQ("order-topic", ""))
		assert.Equal(t, []string{"r-all"}, got)
	})
	t.Run("未知topic → 空", func(t *testing.T) {
		assert.Empty(t, nt.MatchMQ("nope", "x"))
		assert.Empty(t, nt.MatchMQ("", "x"))
	})
}

// TestMatchCanal canal 维度：db.table 通配 + event 精确分桶。
func TestMatchCanal(t *testing.T) {
	cfg := newValidConfig()
	// 增加一条 canal 通配路由（event 空），验证与精确路由分桶后正确合并
	cfg.Subscribers = append(cfg.Subscribers, newSubscriber("s-audit", "http://audit.internal/hook"))
	cfg.Routes = append(cfg.Routes, &Route{
		ID:          "r-canal-all",
		Match:       MatchRule{DB: "shop", Table: "refund"},
		Subscribers: []string{"s-audit"},
	})
	nt, err := buildTable(cfg)
	require.NoError(t, err)

	// INSERT：通配 r-canal-all 在前 + 精确 r-canal 在后
	assert.Equal(t, []string{"r-canal-all", "r-canal"}, routeIDs(nt.MatchCanal("shop", "refund", EventInsert)))
	// UPDATE：仅通配命中
	assert.Equal(t, []string{"r-canal-all"}, routeIDs(nt.MatchCanal("shop", "refund", EventUpdate)))
	// 空 event 查询：仅通配
	assert.Equal(t, []string{"r-canal-all"}, routeIDs(nt.MatchCanal("shop", "refund", "")))
	assert.Empty(t, nt.MatchCanal("shop", "other", EventInsert))
	assert.Empty(t, nt.MatchCanal("", "refund", EventInsert))

	// Listeners 访问器返回快照内监听定义
	assert.Len(t, nt.Listeners(), 2)
}

// TestPause_Expiry Pause 生效在读取时判定：过期即不生效，Expire=0 永久。
func TestPause_Expiry(t *testing.T) {
	cfg := newValidConfig()
	// p-tag 设为已过期
	cfg.Pauses[1].Expire = time.Now().Add(-time.Minute).Unix()
	nt, err := buildTable(cfg)
	require.NoError(t, err)

	assert.True(t, nt.PausedTopic("order-topic"), "Expire=0 永久生效")
	assert.False(t, nt.PausedTag("order-topic", "refunded"), "已过期的暂停不得生效")
	assert.True(t, nt.PausedSubscriber("s-wms"))
	assert.False(t, nt.PausedTopic("other-topic"), "未命中 scope 不生效")
}

func TestPause_ActiveTag(t *testing.T) {
	cfg := newValidConfig()
	cfg.Pauses[1].Expire = time.Now().Add(time.Hour).Unix()
	nt, err := buildTable(cfg)
	require.NoError(t, err)
	assert.True(t, nt.PausedTag("order-topic", "refunded"))
}

// TestSubscriberDeref Route 只存 ID，扇出时经表解引用；未命中返回 error。
func TestSubscriberDeref(t *testing.T) {
	nt, err := buildTable(newValidConfig())
	require.NoError(t, err)
	s, err := nt.Subscriber("s-erp")
	require.NoError(t, err)
	assert.Equal(t, "http://erp.internal/hook", s.Endpoint)
	_, err = nt.Subscriber("ghost")
	assert.Error(t, err)
}

// TestReload_FailureKeepsOldTable 失败策略：坏配置被拒，旧表继续服务（D02-4）。
func TestReload_FailureKeepsOldTable(t *testing.T) {
	st := mustReload(t, newValidConfig())
	before := st.Snapshot()

	bad := newValidConfig()
	bad.Routes[0].Subscribers = []string{"ghost"}
	require.Error(t, st.Reload(bad), "坏配置 Reload 必须返回 error")

	after := st.Snapshot()
	assert.Same(t, before, after, "失败必须保留旧表指针，不得原子替换")
	assert.Equal(t, []string{"r-all"}, routeIDs(after.MatchMQ("order-topic", "deleted")))
}

func TestReload_RejectsNil(t *testing.T) {
	st := NewTableStore()
	assert.Error(t, st.Reload(nil))
}

// TestConfigFingerprint_OnTable 表级指纹：空表返回 0，装载后与源配置一致。
func TestConfigFingerprint_OnTable(t *testing.T) {
	st := NewTableStore()
	fp, err := st.Snapshot().ConfigFingerprint()
	require.NoError(t, err)
	assert.EqualValues(t, 0, fp, "空表指纹为 0")

	cfg := newValidConfig()
	require.NoError(t, st.Reload(cfg))
	fp2, err := st.Snapshot().ConfigFingerprint()
	require.NoError(t, err)
	want, _ := Fingerprint(cfg)
	assert.Equal(t, want, fp2)
}

// TestConcurrentSnapshotRead 并发读快照 + 交错热更新：-race 下不得数据竞争，读到的表恒为完整快照。
func TestConcurrentSnapshotRead(t *testing.T) {
	st := mustReload(t, newValidConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	// 读者：持续 Snapshot + 匹配，校验命中集合法（只可能是已知路由 ID）
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				routes := st.Snapshot().MatchMQ("order-topic", "created")
				for _, r := range routes {
					assert.Contains(t, []string{"r-all", "r-created"}, r.ID)
				}
			}
		}()
	}
	// 写者：反复 Reload 等价配置，触发 atomic.Pointer 替换
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200 && ctx.Err() == nil; i++ {
			require.NoError(t, st.Reload(newValidConfig()))
		}
		cancel() // 写够轮次后停读者
	}()

	wg.Wait()
}

func routeIDs(rs []*Route) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.ID)
	}
	return out
}

// ---- 热点路径 benchmark（规范第 6 节：Snapshot/MatchMQ/Reload）----

func BenchmarkSnapshot(b *testing.B) {
	st := NewTableStore()
	_ = st.Reload(newValidConfig())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = st.Snapshot()
	}
}

func BenchmarkMatchMQ(b *testing.B) {
	nt, _ := buildTable(newValidConfig())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nt.MatchMQ("order-topic", "created")
	}
}

func BenchmarkReload(b *testing.B) {
	st := NewTableStore()
	cfg := newValidConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = st.Reload(cfg)
	}
}
