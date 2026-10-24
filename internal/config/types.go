// Package config 提供配置中心抽象与内存态路由表热更新（蓝本 6.8、7.1–7.3）。
//
// 边界决策：
//   - 不依赖任何 internal 包（蓝本 6.10 依赖方向），数据结构完全自含；
//   - 不持久化配置（蓝本 2.3/6.8），只维护内存态，配置唯一真源是配置中心，
//     V1 阶段以本地 yaml 文件代替；
//   - 本包不向消费方（dispatcher/broker）暴露解析细节，消费方只经
//     ConfigSource.Load/Watch 与 TableStore.Snapshot/Reload 交互。
package config

import (
	"fmt"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Listener 来源枚举（蓝本 7.1）。
const (
	ListenerSourceMQ    = "mq"
	ListenerSourceCanal = "canal"
)

// Pause 作用域枚举（蓝本 7.1，D02-6 拍板 V1 语义为丢弃）。
const (
	PauseScopeTopic      = "topic"
	PauseScopeTag        = "tag"
	PauseScopeSubscriber = "subscriber"
)

// CallbackAuth 方案枚举；hmac 为预留位，V1 仅 none/static_header 生效（D02-8）。
const (
	AuthModeNone         = "none"
	AuthModeStaticHeader = "static_header"
	AuthModeHMAC         = "hmac"
)

// Canal 事件类型枚举（空串=任意）。
const (
	EventInsert = "INSERT"
	EventUpdate = "UPDATE"
	EventDelete = "DELETE"
)

// Duration 可序列化时长：yaml 既接受 Go 时长字符串（"5s"、"2m"），也接受数字（按秒）。
//
// 存在理由：标准 yaml.v3 不支持 time.Duration 字符串解析；统一在此类型处理，
// 全 schema 时长字段口径一致（项目规范第 1 节「一次设计到位」，D02-8）。
type Duration time.Duration

// UnmarshalYAML 解析时长（yaml.v3 接口：入参为 *yaml.Node）：
// 字符串走 time.ParseDuration，整数按秒。
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	// 优先按字符串解（"5s"/"2m"）；!!int 节点解成 string 会失败，转而按数字路径
	var s string
	if err := value.Decode(&s); err == nil {
		if parsed, derr := time.ParseDuration(s); derr == nil {
			*d = Duration(parsed)
			return nil
		}
		// 数字以字符串形式书写（如 "30"）同样按秒
		if n, serr := strconv.ParseInt(s, 10, 64); serr == nil {
			*d = Duration(time.Duration(n) * time.Second)
			return nil
		}
		return fmt.Errorf("时长字段 %q 解析失败", s)
	}
	var n int64
	if err := value.Decode(&n); err == nil {
		*d = Duration(time.Duration(n) * time.Second)
		return nil
	}
	return fmt.Errorf("时长字段不支持该类型: tag=%s", value.Tag)
}

// MarshalYAML 以时长字符串回显（供未来配置中心回显/导出，不落敏感值由调用方负责）。
func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// Config 一次完整配置快照（四类配置，蓝本 7.1）。
type Config struct {
	Listeners   []*Listener   `yaml:"listeners"`
	Routes      []*Route      `yaml:"routes"`
	Subscribers []*Subscriber `yaml:"subscribers"`
	Pauses      []*Pause      `yaml:"pauses"`
}

// Listener 监听定义：从哪里消费。
type Listener struct {
	ID      string `yaml:"id"`
	Source  string `yaml:"source"` // mq | canal
	Topic   string `yaml:"topic"`  // MQ topic 或 canal topic
	Group   string `yaml:"group"`  // consumer group
	Tag     string `yaml:"tag"`    // 过滤标签（空=任意）
	Enabled bool   `yaml:"enabled"`
}

// Route 路由定义：什么消息投给谁。Subscribers 只存 ID，扇出时经表解引用（D02-4）。
type Route struct {
	ID          string    `yaml:"id"`
	Match       MatchRule `yaml:"match"`
	Subscribers []string  `yaml:"subscribers"`
}

// MatchRule 匹配规则：mq 维度用 Topic/Tag，canal 维度用 DB/Table/Event。
type MatchRule struct {
	Topic string `yaml:"topic"`
	Tag   string `yaml:"tag"`   // 空串匹配任意
	DB    string `yaml:"db"`    // canal（source=canal 路由必填，白名单派生用，蓝本 7.5）
	Table string `yaml:"table"` // canal
	Event string `yaml:"event"` // canal：INSERT/UPDATE/DELETE，空=任意
}

// Subscriber 订阅方：回调目标与投递策略。
type Subscriber struct {
	ID        string            `yaml:"id"`
	Endpoint  string            `yaml:"endpoint"`
	Timeout   Duration          `yaml:"timeout"`
	Retry     RetryPolicy       `yaml:"retry"`
	RateLimit int               `yaml:"rate_limit"` // 并发上限，0=不限（预留）
	Headers   map[string]string `yaml:"headers"`
	Auth      CallbackAuth      `yaml:"auth"`
	Breaker   BreakerConfig     `yaml:"breaker"` // V1 仅存配置，消费方 M2 接线（D02-8）
}

// CallbackAuth 回调签名配置：方案可插拔；Token/Secret 只存内存，日志输出必须经 pkg/mask 脱敏（D02-5 规则 5）。
type CallbackAuth struct {
	Mode   string `yaml:"mode"`   // none | static_header | hmac
	Header string `yaml:"header"` // static_header：头名
	Token  string `yaml:"token"`  // static_header：固定 token
	Secret string `yaml:"secret"` // hmac：签名密钥
}

// RetryPolicy 投递重试策略：Multiplier 必须 ≥ 1（校验规则 4）。
type RetryPolicy struct {
	MaxAttempts    int      `yaml:"max_attempts"`
	InitialBackoff Duration `yaml:"initial_backoff"`
	MaxBackoff     Duration `yaml:"max_backoff"`
	Multiplier     float64  `yaml:"multiplier"`
}

// BreakerConfig 订阅方熔断治理配置（per-subscriber，蓝本 6.6.1/7.1）。
type BreakerConfig struct {
	Enabled            bool     `yaml:"enabled"`
	ErrorRateThreshold float64  `yaml:"error_rate_threshold"` // 滑动窗口错误率阈值
	SlowCallRate       float64  `yaml:"slow_call_rate"`       // 慢调用率阈值
	SlowCallDuration   Duration `yaml:"slow_call_duration"`   // 判定"慢"的延迟阈值
	WindowSize         int      `yaml:"window_size"`          // 滑动窗口最小样本数
	OpenTimeout        Duration `yaml:"open_timeout"`         // open→half-open 基础间隔
	BackoffFactor      float64  `yaml:"backoff_factor"`       // 半开间隔递增系数
	MaxOpenTimeout     Duration `yaml:"max_open_timeout"`     // 半开间隔上限
	HalfOpenMaxProbe   int      `yaml:"half_open_max_probe"`  // half-open 并发探针数
	MaxDwell           Duration `yaml:"max_dwell"`            // 熔断 park 最大滞留，超则入 DLQ
	ConsecutiveSuccess int      `yaml:"consecutive_success"`  // 连续成功多少次后转 closed
}

// Pause 暂停规则：热插拔核心。V1 语义 = 丢弃（D02-6）：命中即 ack 源并跳过投递，不暂存不补投。
type Pause struct {
	ID     string `yaml:"id"`
	Scope  string `yaml:"scope"`  // topic | tag | subscriber
	Target string `yaml:"target"` // scope=tag 时编码 "topic:tag"（topic 名禁含 ':'，校验规则 2）
	Reason string `yaml:"reason"`
	Expire int64  `yaml:"expire"` // Unix 秒；0=永久；过期读取时判定（D02-4），不依赖定时器
}
