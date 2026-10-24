package config

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestDuration_UnmarshalYAML 时长字段双形态：Go 时长字符串与按秒数字。
func TestDuration_UnmarshalYAML(t *testing.T) {
	cases := []struct {
		yaml string
		want time.Duration
	}{
		{"timeout: 5s", 5 * time.Second},
		{"timeout: 2m", 2 * time.Minute},
		{"timeout: 500ms", 500 * time.Millisecond},
		{"timeout: 30", 30 * time.Second},     // 数字按秒
		{"timeout: \"30\"", 30 * time.Second}, // 带引号的数字字符串也按秒
		{"timeout: 0", 0},
	}
	for _, tc := range cases {
		t.Run(tc.yaml, func(t *testing.T) {
			var m struct {
				Timeout Duration `yaml:"timeout"`
			}
			require.NoError(t, yaml.Unmarshal([]byte(tc.yaml), &m))
			assert.Equal(t, Duration(tc.want), m.Timeout)
		})
	}
}

func TestDuration_BadInput(t *testing.T) {
	var m struct {
		Timeout Duration `yaml:"timeout"`
	}
	assert.Error(t, yaml.Unmarshal([]byte("timeout: abc"), &m), "非法时长字符串必须报错")
	assert.Error(t, yaml.Unmarshal([]byte("timeout: true"), &m), "布尔类型不受支持")
}

func TestDuration_MarshalYAML(t *testing.T) {
	b, err := yaml.Marshal(struct {
		Timeout Duration `yaml:"timeout"`
	}{Timeout: Duration(90 * time.Second)})
	require.NoError(t, err)
	assert.Contains(t, string(b), "1m30s")
}

// TestStaticSource_LoadDeepCopy 验证 Load 返回深拷贝：改副本不影响源（快照不可变约定）。
func TestStaticSource_LoadDeepCopy(t *testing.T) {
	base := newValidConfig()
	src, err := NewStaticSource(base)
	require.NoError(t, err)

	got, err := src.Load(context.Background())
	require.NoError(t, err)
	got.Listeners[0].Topic = "MUTATED"
	got.Subscribers[0].Retry.MaxAttempts = 999

	again, err := src.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "order-topic", again.Listeners[0].Topic, "内部状态不得被外部改写污染")
	assert.Equal(t, 5, again.Subscribers[0].Retry.MaxAttempts)
}

func TestStaticSource_RejectsInvalid(t *testing.T) {
	bad := newValidConfig()
	bad.Routes[0].Subscribers = []string{"ghost"} // 破坏引用完整性
	_, err := NewStaticSource(bad)
	assert.Error(t, err)
}

// TestStaticSource_WatchClosedImmediately 静态源 Watch 返回立即关闭的空 channel，消费端 range 直接退出。
func TestStaticSource_WatchClosedImmediately(t *testing.T) {
	src, err := NewStaticSource(newValidConfig())
	require.NoError(t, err)
	ch, err := src.Watch(context.Background())
	require.NoError(t, err)
	_, ok := <-ch
	assert.False(t, ok, "channel 必须已关闭")
}

// TestFingerprint 内容指纹：同内容稳定、异内容不同。
func TestFingerprint(t *testing.T) {
	a, err := Fingerprint(newValidConfig())
	require.NoError(t, err)
	b, err := Fingerprint(newValidConfig())
	require.NoError(t, err)
	assert.Equal(t, a, b, "相同内容指纹必须一致")

	mod := newValidConfig()
	mod.Subscribers[0].Endpoint = "http://other.internal/hook"
	fp, err := Fingerprint(mod)
	require.NoError(t, err)
	assert.NotEqual(t, a, fp, "内容变化指纹必须改变")
}

// TestCloneConfig_Nil clone 对空指针的边界处理。
func TestCloneConfig_Nil(t *testing.T) {
	_, err := cloneConfig(nil)
	assert.Error(t, err)
}
