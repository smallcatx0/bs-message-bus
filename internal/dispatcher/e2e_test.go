package dispatcher_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"message-bus/internal/config"
	"message-bus/internal/dispatcher"
)

// 三份内容一致的 yaml 变体：基础（无暂停）、加 subscriber 暂停、去掉暂停还原。
// 覆盖 D03-5 端到端演练：改文件（含 rename 之外的原地写）即时改变解析结果，全程无重启。
const (
	yamlBase = `listeners:
  - id: l-order
    source: mq
    topic: order
    group: g1
    enabled: true
routes:
  - id: r-all
    match:
      topic: order
    subscribers: [s-erp]
subscribers:
  - id: s-erp
    endpoint: http://erp.internal/hook
    timeout: 5s
    retry:
      max_attempts: 3
      initial_backoff: 1s
      max_backoff: 1m
      multiplier: 2.0
`
)

// pausedYAML 在 base 上追加对订阅方 s-erp 的 subscriber 暂停；reason 携 nonce 保证每次轮询内容指纹变化，
// 免疫 watchLoop 起始基线与首次写入竞态导致的事件被去重吞掉（自愈）。
func pausedYAML(nonce int64) string {
	return yamlBase + "pauses:\n  - id: p-erp\n    scope: subscriber\n    target: s-erp\n    reason: \"e2e 演练止损-" +
		strconv.FormatInt(nonce, 10) + "\"\n    expire: 0\n"
}

// TestE2E_HotReloadDrivesResolve 端到端：临时 routing.yaml 经 fileSource→RunWatch→Router，
// 改文件暂停/恢复订阅方，断言 ResolveMQ 输出随之热切换。
func TestE2E_HotReloadDrivesResolve(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "routing.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlBase), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := config.NewTableStore()
	src := config.NewFileSource(path, config.WithDebounce(30*time.Millisecond))
	require.NoError(t, config.RunWatch(ctx, src, store))

	r := dispatcher.NewRouter(store)
	// 首载同步建表：立即命中 s-erp
	assert.Equal(t, []string{"s-erp"}, subIDs(r.ResolveMQ("order", "")))

	// ① 追加 subscriber 暂停 → 目标剔空。自愈式：每 tick 重写带新 nonce 的内容，
	// 保证无论 watchLoop 起始基线如何都会发出一次事件并被应用（抵御满载下的调度抽风）。
	require.Eventually(t, func() bool {
		_ = os.WriteFile(path, []byte(pausedYAML(time.Now().UnixNano())), 0o644)
		return len(r.ResolveMQ("order", "")) == 0
	}, 3*time.Second, 50*time.Millisecond, "暂停订阅方后 ResolveMQ 应热生效为空")

	// ② 中途写入坏配置（非法 flow 序列）→ fileSource 指纹读取失败不发事件，表保持稳定
	require.NoError(t, os.WriteFile(path, []byte(yamlBase+"broken: [[[\n"), 0o644))
	time.Sleep(150 * time.Millisecond)
	assert.Len(t, r.ResolveMQ("order", ""), 0, "坏配置不得清空或篡改当前表（保留旧表，D02-4）")

	// ③ 去掉暂停还原 → 目标恢复（此时基线为暂停变体，写 base 必产生差异事件）
	require.Eventually(t, func() bool {
		_ = os.WriteFile(path, []byte(yamlBase), 0o644)
		return assert.ObjectsAreEqual([]string{"s-erp"}, subIDs(r.ResolveMQ("order", "")))
	}, 3*time.Second, 50*time.Millisecond, "解除暂停后 ResolveMQ 应热恢复命中 s-erp")

	cancel() // 触发 fileSource watchLoop 退出并关闭事件 channel，RunWatch 消费 goroutine 随之收束
}
