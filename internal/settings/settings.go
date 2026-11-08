// Package settings 是动态配置中心的内存侧。
//
// 动态配置：
//   - 一切业务可变项由超级管理员在后台改，改完即生效，不重启、不发版。
//   - 变更经 pg_notify 广播，本包监听后原子替换内存快照。
//   - 解析失败必须保留旧快照，禁止把配置清空。
//
// 结构上刻意保持简单：一个不可变快照指针 + 一次 atomic 交换。
// 读路径无锁，写路径极少发生。这符合服务发现的「不要堆并发架构」。
package settings

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// Snapshot 是某一时刻全部配置的不可变视图。
// 一旦发布就不再修改，热更新时整个换掉，读侧无需加锁。
type Snapshot struct {
	values map[string]json.RawMessage
	at     time.Time
}

// Raw 返回原始 JSON，不存在时返回 nil。
func (s *Snapshot) Raw(key string) json.RawMessage {
	if s == nil {
		return nil
	}
	return s.values[key]
}

// Bool 读取布尔配置，缺失或解析失败时返回 def。
func (s *Snapshot) Bool(key string, def bool) bool {
	raw := s.Raw(key)
	if raw == nil {
		return def
	}
	var v bool
	if err := json.Unmarshal(raw, &v); err != nil {
		return def
	}
	return v
}

// Int64 读取整数配置，缺失或解析失败时返回 def。
func (s *Snapshot) Int64(key string, def int64) int64 {
	raw := s.Raw(key)
	if raw == nil {
		return def
	}
	var v int64
	if err := json.Unmarshal(raw, &v); err != nil {
		return def
	}
	return v
}

// String 读取字符串配置，缺失或解析失败时返回 def。
func (s *Snapshot) String(key string, def string) string {
	raw := s.Raw(key)
	if raw == nil {
		return def
	}
	var v string
	if err := json.Unmarshal(raw, &v); err != nil {
		// 兼容写入时未加引号的裸标量
		var any interface{}
		if err2 := json.Unmarshal(raw, &any); err2 == nil {
			if sv, ok := any.(string); ok {
				return sv
			}
		}
		return def
	}
	return v
}

// Duration 读取以秒为单位的整数配置并转 Duration。
func (s *Snapshot) Duration(key string, def time.Duration) time.Duration {
	sec := s.Int64(key, int64(def/time.Second))
	if sec <= 0 {
		return def
	}
	return time.Duration(sec) * time.Second
}

// All 返回全部键值，供管理端与 /admin/v1/settings 使用。
func (s *Snapshot) All() map[string]json.RawMessage {
	if s == nil {
		return nil
	}
	out := make(map[string]json.RawMessage, len(s.values))
	for k, v := range s.values {
		out[k] = v
	}
	return out
}

// At 返回快照生成时间。
func (s *Snapshot) At() time.Time {
	if s == nil {
		return time.Time{}
	}
	return s.at
}

// Manager 负责加载、发布与订阅变更。
type Manager struct {
	st   *store.Store
	snap atomic.Pointer[Snapshot]

	stopCh   chan struct{}
	stopOnce sync.Once

	// ReloadCount / ReloadFailed 供 /healthz 展示
	reloadCount  atomic.Int64
	reloadFailed atomic.Int64
}

// New 创建管理器并做首次加载。
//
// 首次加载失败直接返回错误：网关没有配置就无法正确工作，
// 这种情况应该在启动阶段暴露，而不是带着空配置跑起来。
func New(ctx context.Context, st *store.Store) (*Manager, error) {
	m := &Manager{st: st, stopCh: make(chan struct{})}
	if err := m.Reload(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// Current 返回当前快照。永不为 nil。
func (m *Manager) Current() *Snapshot {
	if s := m.snap.Load(); s != nil {
		return s
	}
	// 极端兜底：返回空快照，让所有读取走默认值
	return &Snapshot{values: map[string]json.RawMessage{}}
}

// Reload 从数据库全量重载。
//
// 关键点：只有全部解析成功才替换快照。
// 任何一步失败都保留旧快照并记一次失败计数（动态配置规则 4）。
func (m *Manager) Reload(ctx context.Context) error {
	rows, err := m.st.ListSettings(ctx)
	if err != nil {
		m.reloadFailed.Add(1)
		return err
	}

	values := make(map[string]json.RawMessage, len(rows))
	for _, r := range rows {
		// 逐条校验可解析性。保留原始 JSON 供 Snapshot 按类型读取。
		var probe interface{}
		if err := json.Unmarshal(r.Value, &probe); err != nil {
			m.reloadFailed.Add(1)
			return fmt.Errorf("配置项 %q 的值不是合法 JSON: %w", r.Key, err)
		}
		values[r.Key] = r.Value
	}

	m.snap.Store(&Snapshot{values: values, at: time.Now()})
	m.reloadCount.Add(1)
	return nil
}

// Watch 起一个后台 goroutine 监听 pg_notify('system_settings_changed')。
//
// 设计取舍：
//   - 不用轮询兜底。LISTEN 断线时 pgxpool 会报错，Receive 返回后我们重连并全量重载，
//     这比定时全量查库更省资源，也不会产生配置漂移。
//   - 重连失败时退避重试，但绝不阻塞网关主流程。
func (m *Manager) Watch(ctx context.Context) {
	go func() {
		backoff := time.Second
		for {
			select {
			case <-m.stopCh:
				return
			case <-ctx.Done():
				return
			default:
			}

			if err := m.listenOnce(ctx); err != nil {
				log.Printf("[settings] 监听配置变更失败: %v，%s 后重试", err, backoff)
				select {
				case <-time.After(backoff):
				case <-m.stopCh:
					return
				case <-ctx.Done():
					return
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second
		}
	}()
}

func (m *Manager) listenOnce(ctx context.Context) error {
	// LISTEN 必须独占一条连接，不能用池里随取随用的连接：
	// 池里的连接随时可能被归还或被别人借走，通知会被悄悄丢掉。
	// 因此这里用 pgx.Connect 建一条独立的长连接（不是连接池）。
	connString := m.st.Pool().Config().ConnConfig.ConnString()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("建立监听连接失败: %w", err)
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN system_settings_changed"); err != nil {
		return fmt.Errorf("执行 LISTEN 失败: %w", err)
	}

	// 重连后先全量对齐，弥补断线期间的变更
	if err := m.Reload(ctx); err != nil {
		log.Printf("[settings] 重连后全量重载失败: %v", err)
	}

	for {
		select {
		case <-m.stopCh:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}

		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		notification, err := conn.WaitForNotification(waitCtx)
		cancel()

		if err != nil {
			// 超时属于正常空闲，继续等
			if ctx.Err() == nil && waitCtx.Err() != nil {
				continue
			}
			return fmt.Errorf("等待通知失败: %w", err)
		}

		log.Printf("[settings] 收到配置变更通知: %s", notification.Payload)
		if err := m.Reload(ctx); err != nil {
			log.Printf("[settings] 变更后重载失败，保留旧快照: %v", err)
		}
	}
}

// Close 停止监听。
func (m *Manager) Close() {
	m.stopOnce.Do(func() { close(m.stopCh) })
}

// Stats 供健康检查展示。
func (m *Manager) Stats() (reloads, failures int64) {
	return m.reloadCount.Load(), m.reloadFailed.Load()
}
