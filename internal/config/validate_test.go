package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_Base(t *testing.T) {
	require.NoError(t, Validate(newValidConfig()))
	require.Error(t, Validate(nil), "空配置指针必须报错")
}

// TestValidate_Rule1_Struct 规则1：结构合法、必填、枚举。
func TestValidate_Rule1_Struct(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"listener 空条目", func(c *Config) { c.Listeners = append(c.Listeners, nil) }},
		{"listener 非法 source", func(c *Config) { c.Listeners[0].Source = "kafka" }},
		{"route 非法 event", func(c *Config) { c.Routes[2].Match.Event = "DROP" }},
		{"pause 非法 scope", func(c *Config) { c.Pauses[0].Scope = "global" }},
		{"auth 非法 mode", func(c *Config) { c.Subscribers[0].Auth.Mode = "oauth" }},
		{"id 为空", func(c *Config) { c.Subscribers[0].ID = "" }},
		{"canal db/table 缺一", func(c *Config) { c.Routes[2].Match.Table = "" }},
		{"match 两维度都空", func(c *Config) { c.Routes[0].Match = MatchRule{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newValidConfig()
			tc.mutate(c)
			assert.Error(t, Validate(c))
		})
	}
}

// TestValidate_Rule2_Reference 规则2：引用完整性、ID 唯一、topic 不含冒号、tag 编码。
func TestValidate_Rule2_Reference(t *testing.T) {
	t.Run("引用不存在的 subscriber", func(t *testing.T) {
		c := newValidConfig()
		c.Routes[0].Subscribers = []string{"ghost"}
		assert.Error(t, Validate(c))
	})
	t.Run("subscriber ID 重复", func(t *testing.T) {
		c := newValidConfig()
		c.Subscribers = append(c.Subscribers, newSubscriber("s-erp", "http://x.internal/h"))
		assert.Error(t, Validate(c))
	})
	t.Run("ID 跨类全局重复", func(t *testing.T) {
		c := newValidConfig()
		c.Routes[0].ID = "s-erp"
		assert.Error(t, Validate(c))
	})
	t.Run("topic 含冒号被拒", func(t *testing.T) {
		c := newValidConfig()
		c.Listeners[0].Topic = "bad:name"
		assert.Error(t, Validate(c))
	})
	t.Run("pause topic 未在 listener 定义", func(t *testing.T) {
		c := newValidConfig()
		c.Pauses[0].Target = "no-such-topic"
		assert.Error(t, Validate(c))
	})
	t.Run("pause tag 编码缺分隔符", func(t *testing.T) {
		c := newValidConfig()
		c.Pauses[1].Target = "order-topic" // 缺 ":tag"
		assert.Error(t, Validate(c))
	})
	t.Run("pause tag 段为空", func(t *testing.T) {
		c := newValidConfig()
		c.Pauses[1].Target = "order-topic:"
		assert.Error(t, Validate(c))
	})
	t.Run("pause subscriber 未定义", func(t *testing.T) {
		c := newValidConfig()
		c.Pauses[2].Target = "ghost"
		assert.Error(t, Validate(c))
	})
}

// TestValidate_Rule3_Endpoint 规则3：endpoint 必须 http/https 绝对 URL。
func TestValidate_Rule3_Endpoint(t *testing.T) {
	cases := []string{"", "ftp://x/h", "://bad", "erp.internal/hook", "http://", "not a url"}
	for _, ep := range cases {
		t.Run("endpoint="+ep, func(t *testing.T) {
			c := newValidConfig()
			c.Subscribers[0].Endpoint = ep
			assert.Error(t, Validate(c), "非法 endpoint 必须被拒: %q", ep)
		})
	}
}

// TestValidate_Rule4_Time 规则4：时间/退避语义边界。
func TestValidate_Rule4_Time(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"timeout<=0", func(c *Config) { c.Subscribers[0].Timeout = 0 }},
		{"max_attempts<=0", func(c *Config) { c.Subscribers[0].Retry.MaxAttempts = 0 }},
		{"initial_backoff<=0", func(c *Config) { c.Subscribers[0].Retry.InitialBackoff = 0 }},
		{"max_backoff<initial", func(c *Config) { c.Subscribers[0].Retry.MaxBackoff = Duration(time.Millisecond) }},
		{"multiplier<1", func(c *Config) { c.Subscribers[0].Retry.Multiplier = 0.9 }},
		{"rate_limit<0", func(c *Config) { c.Subscribers[0].RateLimit = -1 }},
		{"pause expire<0", func(c *Config) { c.Pauses[0].Expire = -1 }},
		{"breaker 阈值越界", func(c *Config) {
			c.Subscribers[0].Breaker = BreakerConfig{Enabled: true, ErrorRateThreshold: 1.5}
		}},
		{"breaker max_open<open", func(c *Config) {
			c.Subscribers[0].Breaker = BreakerConfig{
				Enabled: true, ErrorRateThreshold: 0.5, SlowCallRate: 0.5,
				SlowCallDuration: Duration(time.Second), WindowSize: 10,
				OpenTimeout: Duration(time.Second), BackoffFactor: 2,
				MaxOpenTimeout: Duration(500 * time.Millisecond), HalfOpenMaxProbe: 1,
				MaxDwell: Duration(time.Minute), ConsecutiveSuccess: 1,
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newValidConfig()
			tc.mutate(c)
			assert.Error(t, Validate(c))
		})
	}
}

// TestValidate_Rule5_NoSecretLeak 规则5：error 文本不得含 token/secret 明文。
func TestValidate_Rule5_NoSecretLeak(t *testing.T) {
	c := newValidConfig()
	c.Subscribers[0].Auth = CallbackAuth{Mode: AuthModeStaticHeader, Header: "X-Token", Token: "SUP3R-SECRE7"}
	// 合法：static_header 有 header + token，应通过
	require.NoError(t, Validate(c))

	// 触发 static_header 缺 token 的校验错误，确认错误文本不含其它订阅方的 secret 明文
	c.Subscribers[1].Auth = CallbackAuth{Mode: AuthModeHMAC, Secret: "HMAC-T0TAL-LEAK"}
	c.Subscribers[0].Auth.Token = "" // 制造错误
	err := Validate(c)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "HMAC-T0TAL-LEAK", "校验错误不得泄露 secret 明文")
}

// TestValidate_BreakerDisabledSkipped 未启用熔断时不校验其字段（避免过度约束预留配置）。
func TestValidate_BreakerDisabledSkipped(t *testing.T) {
	c := newValidConfig()
	c.Subscribers[0].Breaker = BreakerConfig{Enabled: false, ErrorRateThreshold: 99}
	assert.NoError(t, Validate(c))
}
