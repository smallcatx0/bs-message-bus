package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Validate 按 D02-5 五类规则校验配置：结构合法、引用完整性、endpoint 格式、
// 时间语义、敏感字段约束（仅静态检查，值本身不进 error 文本）。
//
// 决策：一次收集全部错误再返回（errorJoin 聚合），便于运维改配置时一轮看全问题，
// 而不是修一条报一条。
func Validate(cfg *Config) error {
	if cfg == nil {
		return errors.New("config: 配置为空")
	}
	j := errJoin{}
	ids := map[string]struct{}{} // ID 全局唯一（规则 2）

	// 步骤1：Listener 结构、枚举与 Topic/Group 非空且不重复（规则 1/2）
	lTopic, lGroup := map[string]struct{}{}, map[string]struct{}{}
	for i, l := range cfg.Listeners {
		if l == nil {
			j.add(fmt.Errorf("listeners[%d]: 空条目", i))
			continue
		}
		tag := fmt.Sprintf("listener %q", orUnamed(i, l.ID))
		checkUniqueID(&j, ids, tag, l.ID)
		if l.Source != ListenerSourceMQ && l.Source != ListenerSourceCanal {
			j.add(fmt.Errorf("%s: source %q 非法（mq|canal）", tag, l.Source))
		}
		if l.Topic == "" {
			j.add(fmt.Errorf("%s: topic 不能为空", tag))
		} else if strings.Contains(l.Topic, ":") {
			// ':' 为 Pause tag 作用域的目标分隔符（D02-8 拍板），名字不得含冒号
			j.add(fmt.Errorf("%s: topic %q 含非法字符 ':'", tag, l.Topic))
		}
		if l.Group == "" {
			j.add(fmt.Errorf("%s: group 不能为空", tag))
		}
		if _, dup := lTopic[l.Topic]; dup {
			j.add(fmt.Errorf("%s: topic %q 重复定义", tag, l.Topic))
		}
		lTopic[l.Topic] = struct{}{}
		if _, dup := lGroup[l.Group]; dup {
			j.add(fmt.Errorf("%s: group %q 重复定义", tag, l.Group))
		}
		lGroup[l.Group] = struct{}{}
	}

	// 步骤2：Subscriber 结构、endpoint 格式与时间语义（规则 1/3/4/5）
	subIDs := map[string]struct{}{}
	for i, s := range cfg.Subscribers {
		if s == nil {
			j.add(fmt.Errorf("subscribers[%d]: 空条目", i))
			continue
		}
		tag := fmt.Sprintf("subscriber %q", orUnamed(i, s.ID))
		checkUniqueID(&j, ids, tag, s.ID)
		if _, dup := subIDs[s.ID]; dup {
			j.add(fmt.Errorf("%s: ID 重复定义", tag))
		}
		subIDs[s.ID] = struct{}{}
		checkEndpoint(&j, tag, s.Endpoint)
		if s.Timeout <= 0 {
			j.add(fmt.Errorf("%s: timeout 必须 > 0", tag))
		}
		if s.RateLimit < 0 {
			j.add(fmt.Errorf("%s: rate_limit 不能为负", tag))
		}
		checkRetry(&j, tag, s.Retry)
		checkAuth(&j, tag, s.Auth)
		if s.Breaker.Enabled {
			checkBreaker(&j, tag, s.Breaker)
		}
	}

	// 步骤3：Route 结构与订阅方引用完整性（规则 1/2）
	for i, r := range cfg.Routes {
		if r == nil {
			j.add(fmt.Errorf("routes[%d]: 空条目", i))
			continue
		}
		tag := fmt.Sprintf("route %q", orUnamed(i, r.ID))
		checkUniqueID(&j, ids, tag, r.ID)
		if len(r.Subscribers) == 0 {
			j.add(fmt.Errorf("%s: subscribers 不能为空", tag))
		}
		for _, sub := range r.Subscribers {
			if _, ok := subIDs[sub]; !ok {
				j.add(fmt.Errorf("%s: 引用不存在的 subscriber %q", tag, sub))
			}
		}
		mqSide := r.Match.Topic != ""
		canalSide := r.Match.DB != "" || r.Match.Table != ""
		if !mqSide && !canalSide {
			j.add(fmt.Errorf("%s: match 至少给出 mq（topic）或 canal（db.table）维度之一", tag))
		}
		if canalSide && (r.Match.DB == "" || r.Match.Table == "") {
			j.add(fmt.Errorf("%s: canal 维度 db/table 必须同时提供", tag))
		}
		if mqSide && strings.Contains(r.Match.Topic, ":") {
			j.add(fmt.Errorf("%s: topic %q 含非法字符 ':'", tag, r.Match.Topic))
		}
		switch r.Match.Event {
		case "", EventInsert, EventUpdate, EventDelete:
		default:
			j.add(fmt.Errorf("%s: canal event %q 非法", tag, r.Match.Event))
		}
	}

	// 步骤4：Pause 结构、scope 枚举与 target 编码（规则 1/2/4）
	for i, p := range cfg.Pauses {
		if p == nil {
			j.add(fmt.Errorf("pauses[%d]: 空条目", i))
			continue
		}
		tag := fmt.Sprintf("pause %q", orUnamed(i, p.ID))
		checkUniqueID(&j, ids, tag, p.ID)
		if p.Expire < 0 {
			j.add(fmt.Errorf("%s: expire 必须 ≥ 0（0=永久）", tag))
		}
		switch p.Scope {
		case PauseScopeTopic:
			if _, ok := lTopic[p.Target]; !ok {
				j.add(fmt.Errorf("%s: target topic %q 未在 listeners 中定义", tag, p.Target))
			}
		case PauseScopeTag:
			topic, sub, ok := cutTagTarget(p.Target)
			if !ok {
				j.add(fmt.Errorf("%s: scope=tag 的 target 必须编码为 \"topic:tag\"", tag))
			} else if sub == "" {
				j.add(fmt.Errorf("%s: scope=tag 的 tag 部分不能为空", tag))
			} else if _, exists := lTopic[topic]; !exists {
				j.add(fmt.Errorf("%s: target topic %q 未在 listeners 中定义", tag, topic))
			}
		case PauseScopeSubscriber:
			if _, ok := subIDs[p.Target]; !ok {
				j.add(fmt.Errorf("%s: target subscriber %q 未定义", tag, p.Target))
			}
		default:
			j.add(fmt.Errorf("%s: scope %q 非法（topic|tag|subscriber）", tag, p.Scope))
		}
	}

	return j.err()
}

// errJoin 轻量多错误收集器（不引外部包，errors.Join 语义 + 统一前缀）。
type errJoin struct {
	errs []error
}

func (j *errJoin) add(err error) { j.errs = append(j.errs, err) }

func (j *errJoin) err() error {
	if len(j.errs) == 0 {
		return nil
	}
	return errors.Join(append([]error{errors.New("config: 校验失败")}, j.errs...)...)
}

func orUnamed(i int, id string) string {
	if id == "" {
		return fmt.Sprintf("#%d(未命名)", i)
	}
	return id
}

func checkUniqueID(j *errJoin, ids map[string]struct{}, tag, id string) {
	if id == "" {
		j.add(fmt.Errorf("%s: id 不能为空", tag))
		return
	}
	if _, dup := ids[id]; dup {
		j.add(fmt.Errorf("%s: ID 全局重复", tag))
	}
	ids[id] = struct{}{}
}

// checkEndpoint 规则 3：http/https 绝对 URL（有 scheme + host）。
func checkEndpoint(j *errJoin, tag, endpoint string) {
	if endpoint == "" {
		j.add(fmt.Errorf("%s: endpoint 不能为空", tag))
		return
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		j.add(fmt.Errorf("%s: endpoint %q 必须为 http/https 绝对 URL", tag, endpoint))
	}
}

// checkAuth 规则 5 静态部分：mode 枚举与必填配套；Token/Secret 值绝不进 error 文本。
func checkAuth(j *errJoin, tag string, a CallbackAuth) {
	switch a.Mode {
	case "", AuthModeNone:
	case AuthModeStaticHeader:
		if a.Header == "" || a.Token == "" {
			j.add(fmt.Errorf("%s: static_header 鉴权要求 header 与 token 非空", tag))
		}
	case AuthModeHMAC:
		if a.Secret == "" {
			j.add(fmt.Errorf("%s: hmac 鉴权要求 secret 非空", tag))
		}
	default:
		j.add(fmt.Errorf("%s: 鉴权 mode %q 非法", tag, a.Mode))
	}
}

// checkRetry 规则 4：超时/退避 > 0，Multiplier ≥ 1。
func checkRetry(j *errJoin, tag string, r RetryPolicy) {
	if r.MaxAttempts <= 0 {
		j.add(fmt.Errorf("%s: retry.max_attempts 必须 > 0", tag))
	}
	if r.InitialBackoff <= 0 {
		j.add(fmt.Errorf("%s: retry.initial_backoff 必须 > 0", tag))
	}
	if r.MaxBackoff < r.InitialBackoff {
		j.add(fmt.Errorf("%s: retry.max_backoff 不得小于 initial_backoff", tag))
	}
	if r.Multiplier < 1 {
		j.add(fmt.Errorf("%s: retry.multiplier 必须 ≥ 1", tag))
	}
}

// checkBreaker 规则 1/4 在熔断配置上的展开：阈值区间与时长语义。
func checkBreaker(j *errJoin, tag string, b BreakerConfig) {
	if b.ErrorRateThreshold <= 0 || b.ErrorRateThreshold > 1 {
		j.add(fmt.Errorf("%s: breaker.error_rate_threshold 须在 (0,1]", tag))
	}
	if b.SlowCallRate <= 0 || b.SlowCallRate > 1 {
		j.add(fmt.Errorf("%s: breaker.slow_call_rate 须在 (0,1]", tag))
	}
	if b.SlowCallDuration <= 0 {
		j.add(fmt.Errorf("%s: breaker.slow_call_duration 必须 > 0", tag))
	}
	if b.WindowSize <= 0 {
		j.add(fmt.Errorf("%s: breaker.window_size 必须 > 0", tag))
	}
	if b.OpenTimeout <= 0 {
		j.add(fmt.Errorf("%s: breaker.open_timeout 必须 > 0", tag))
	}
	if b.BackoffFactor < 1 {
		j.add(fmt.Errorf("%s: breaker.backoff_factor 必须 ≥ 1", tag))
	}
	if b.MaxOpenTimeout < b.OpenTimeout {
		j.add(fmt.Errorf("%s: breaker.max_open_timeout 不得小于 open_timeout", tag))
	}
	if b.HalfOpenMaxProbe <= 0 {
		j.add(fmt.Errorf("%s: breaker.half_open_max_probe 必须 > 0", tag))
	}
	if b.MaxDwell <= 0 {
		j.add(fmt.Errorf("%s: breaker.max_dwell 必须 > 0", tag))
	}
	if b.ConsecutiveSuccess <= 0 {
		j.add(fmt.Errorf("%s: breaker.consecutive_success 必须 > 0", tag))
	}
}

// cutTagTarget 解析 scope=tag 的 "topic:tag" 编码（D02-8 拍板）。
func cutTagTarget(target string) (topic, tag string, ok bool) {
	i := strings.Index(target, ":")
	if i < 0 {
		return "", "", false
	}
	return target[:i], target[i+1:], true
}
