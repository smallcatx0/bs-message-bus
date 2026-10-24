package config_test

import (
	"context"
	"fmt"
	"time"

	"message-bus/internal/config"
)

// Example 演示标准装配管线：来源 Load → 初始 Reload 建表 → Snapshot 无锁读匹配。
// 这是 M1 dispatcher/broker 接入 config 的参考形态（D02-9）。
func Example() {
	ctx := context.Background()

	// 装配期构造一份合法配置并用静态源承接（生产环境换成 config.NewFileSource/未来 grpcSource）
	cfg := &config.Config{
		Listeners: []*config.Listener{
			{ID: "l1", Source: config.ListenerSourceMQ, Topic: "order", Group: "g1", Enabled: true},
		},
		Routes: []*config.Route{
			{ID: "r1", Match: config.MatchRule{Topic: "order"}, Subscribers: []string{"s1"}},
		},
		Subscribers: []*config.Subscriber{
			{
				ID:       "s1",
				Endpoint: "http://svc/hook",
				Timeout:  config.Duration(5 * time.Second),
				Retry: config.RetryPolicy{
					MaxAttempts:    3,
					InitialBackoff: config.Duration(time.Second),
					MaxBackoff:     config.Duration(time.Minute),
					Multiplier:     2.0,
				},
			},
		},
	}

	src, err := config.NewStaticSource(cfg)
	if err != nil {
		panic(err)
	}
	store := config.NewTableStore()

	// 首载：Load（解析+校验）→ Reload（建表+原子替换）
	loaded, err := src.Load(ctx)
	if err != nil {
		panic(err)
	}
	if err = store.Reload(loaded); err != nil {
		panic(err)
	}

	// 消费方每次处理按 Snapshot 取不可变快照匹配
	hits := store.Snapshot().MatchMQ("order", "")
	fmt.Println("matched routes:", len(hits))
	// Output: matched routes: 1
}
