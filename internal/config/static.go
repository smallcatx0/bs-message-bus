package config

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
)

// staticSource 内存静态来源：无变更能力，用于测试与本地起盘（蓝本 7.3）。
type staticSource struct {
	cfg *Config
}

// NewStaticSource 以给定配置构造静态来源。内部先深拷贝并校验：
// 调用方拿到成功返回的 Source 后，其 Load 必然可复用，避免装配期才暴露配置错误。
func NewStaticSource(cfg *Config) (ConfigSource, error) {
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	cp, err := cloneConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &staticSource{cfg: cp}, nil
}

// Load 返回深拷贝，防止消费方就地改写内部状态（快照不可变约定，D02-4）。
func (s *staticSource) Load(_ context.Context) (*Config, error) {
	return cloneConfig(s.cfg)
}

// Watch 返回立即关闭的 channel：无变更能力，消费端按 channel 关闭退出，不特判（D02-2）。
func (s *staticSource) Watch(_ context.Context) (<-chan ConfigEvent, error) {
	ch := make(chan ConfigEvent)
	close(ch)
	return ch, nil
}

// cloneConfig 经 JSON 往返做深拷贝。
//
// 决策：结构均为导出数据字段，json round-trip 成本可接受（仅测试/装配路径使用，
// 不在热点），换取免写逐字段 copy 的维护成本。
func cloneConfig(cfg *Config) (*Config, error) {
	if cfg == nil {
		return nil, fmt.Errorf("配置为空")
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("拷贝配置失败: %w", err)
	}
	out := &Config{}
	if err = json.Unmarshal(b, out); err != nil {
		return nil, fmt.Errorf("拷贝配置失败: %w", err)
	}
	return out, nil
}

// Fingerprint 配置内容指纹（FNV-1a），供 Watch 侧跳过无实质变化的写入（如编辑器 touch）。
//
// 决策：对规范化 JSON 做哈希——字段遍历顺序由结构体定义固定，map 由 encoding/json
// 按键排序输出，同一内容必然得到同一指纹。
func Fingerprint(cfg *Config) (uint64, error) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return 0, fmt.Errorf("计算配置指纹失败: %w", err)
	}
	h := fnv.New64a()
	// fnv.Hash 的 Write 永不返回 error，忽略并显式说明
	_, _ = h.Write(b)
	return h.Sum64(), nil
}
