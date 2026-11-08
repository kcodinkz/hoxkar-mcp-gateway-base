// Package recorder 负责会话录制：把用户原话与上游返回原样留下来，
// 供管理员审计与后期训练语料使用（会话录制）。
//
// 录制是彻底的旁路：它绝不能让任何一个 MCP 请求变慢或失败。
// 因此本包的公开方法一律不返回 error，队列满了就丢弃并计数，
// 由 /healthz 与日志暴露异常。
package recorder

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/settings"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// Options 是录制器的构造参数，与 config.SessionRecordingConfig 一一对应。
type Options struct {
	// Enabled 是静态兜底开关。动态配置中心存在时以 recording.enabled 为准。
	Enabled bool
	// QueueSize 是旁路队列容量，满了丢弃。
	QueueSize int
	// BatchSize 单批写入条数。
	BatchSize int
	// FlushInterval 强制刷盘间隔。
	FlushInterval time.Duration
	// MaxPayloadBytes 单条 payload 上限，超出转截断信封。
	MaxPayloadBytes int64
	// RedactSensitive 是否做深度脱敏。训练语料默认也必须开。
	RedactSensitive bool
	// CaptureToolPayloads 为 false 时只记方法名与工具名，不记 payload。
	CaptureToolPayloads bool
}

// Recorder 是会话录制器。零值不可用，必须经 New 构造。
type Recorder struct {
	st       *store.Store
	settings *settings.Manager
	opts     Options

	queue chan store.SessionMessage
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once

	// seq 按会话维护单调递增序号。session_messages 上有
	// UNIQUE(session_id, seq)，重试写入时靠 ON CONFLICT DO NOTHING 兜底。
	seqMu sync.Mutex
	seqs  map[idgen.ID]int64

	// 统计
	written   atomic.Int64
	dropped   atomic.Int64
	writeFail atomic.Int64
	redacted  atomic.Int64
	truncated atomic.Int64
}

// Stats 是录制器的运行计数快照。
type Stats struct {
	Written   int64
	Dropped   int64
	WriteFail int64
	Redacted  int64
	Truncated int64
}

// New 构造录制器并启动唯一的写协程。
func New(st *store.Store, sm *settings.Manager, opts Options) *Recorder {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 8192
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 200
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 2 * time.Second
	}
	if opts.MaxPayloadBytes <= 0 {
		opts.MaxPayloadBytes = 1 << 20
	}

	r := &Recorder{
		st:       st,
		settings: sm,
		opts:     opts,
		queue:    make(chan store.SessionMessage, opts.QueueSize),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		seqs:     make(map[idgen.ID]int64),
	}
	go r.run(opts.BatchSize, opts.FlushInterval)
	return r
}

// Enabled 判断当前是否录制。动态配置优先，读不到时退回静态开关。
func (r *Recorder) Enabled() bool {
	if r == nil {
		return false
	}
	if r.settings != nil {
		return r.settings.Current().Bool("recording.enabled", r.opts.Enabled)
	}
	return r.opts.Enabled
}

// NextSeq 分配某个会话的下一个消息序号，从 1 开始。
func (r *Recorder) NextSeq(sessionID idgen.ID) int64 {
	r.seqMu.Lock()
	defer r.seqMu.Unlock()
	r.seqs[sessionID]++
	return r.seqs[sessionID]
}

// ForgetSession 释放序号表条目，避免长期运行后 map 无限增长。
// 在会话结束（EndSession）时调用。
func (r *Recorder) ForgetSession(sessionID idgen.ID) {
	r.seqMu.Lock()
	delete(r.seqs, sessionID)
	r.seqMu.Unlock()
}

// Record 投递一条已构造好的消息。序号、ID、时间缺失时自动补齐。
//
// 永不阻塞：队列满时丢弃并计数。
func (r *Recorder) Record(m store.SessionMessage) {
	if r == nil || !r.Enabled() {
		return
	}
	if m.SessionID.IsZero() {
		return
	}
	if m.ID.IsZero() {
		// 消息 ID 属形态 B（分片字符串）：分片位取自会话 ID，不另行查表。
		id, err := idgen.NewSharded(idgen.KindSessionMessage, idgen.ShardOf(m.SessionID))
		if err != nil {
			r.dropped.Add(1)
			return
		}
		m.ID = id
	}
	if m.Seq == 0 {
		m.Seq = r.NextSeq(m.SessionID)
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
	if m.PayloadBytes == 0 && len(m.Payload) > 0 {
		m.PayloadBytes = int64(len(m.Payload))
	}

	select {
	case r.queue <- m:
	default:
		r.dropped.Add(1)
	}
}

// Payload 是一次录制的载荷描述，避免 Record 的参数列表过长。
type Payload struct {
	SessionID idgen.ID
	Direction store.Direction
	RPCID     string
	Method    string
	ToolName  string
	Raw       []byte
	IsError   bool
	// ErrorCode 是上游返回的错误码原样：JSON-RPC 是单个整数，
	// 业务错误可能是字符串或负数区间，故一律按字符串存放。
	ErrorCode  string
	DurationMS int
}

// RecordPayload 是可读性更好的入口：自动脱敏、按上限截断、补齐元信息。
//
// CaptureToolPayloads 为 false 且方向不属于「用户原话」时只记元信息，
// 这样即便关掉工具载荷也能保留对话骨架。
func (r *Recorder) RecordPayload(p Payload) {
	if r == nil || !r.Enabled() {
		return
	}

	msg := store.SessionMessage{
		SessionID:  p.SessionID,
		Direction:  p.Direction,
		RPCID:      p.RPCID,
		Method:     p.Method,
		ToolName:   p.ToolName,
		IsError:    p.IsError,
		ErrorCode:  p.ErrorCode,
		DurationMS: p.DurationMS,
	}

	if len(p.Raw) > 0 {
		if r.captureAllowed(p.Direction) {
			out, redacted, truncated := r.prepare(p.Raw)
			msg.Payload = out
			msg.PayloadBytes = int64(len(out))
			msg.Redacted = redacted
			msg.Truncated = truncated
			if redacted {
				r.redacted.Add(1)
			}
			if truncated {
				r.truncated.Add(1)
			}
		} else {
			// 只留骨架：标明本可以录但被策略关掉
			msg.Payload = json.RawMessage(`{"_capture":"disabled"}`)
			msg.PayloadBytes = int64(len(msg.Payload))
		}
	}

	r.Record(msg)
}

// captureAllowed 决定某个方向是否记录完整 payload。
func (r *Recorder) captureAllowed(dir store.Direction) bool {
	if r.opts.CaptureToolPayloads {
		return true
	}
	// 即便关闭工具载荷，用户输入与最终输出仍然要留（训练语料的主体）
	switch dir {
	case store.DirClientToGateway, store.DirGatewayToClient:
		return true
	default:
		return false
	}
}

// prepare 做脱敏 + 截断。脱敏开关关闭时仅截断。
func (r *Recorder) prepare(raw []byte) ([]byte, bool, bool) {
	maxBytes := r.maxPayloadBytes()
	if r.opts.RedactSensitive {
		return RedactJSON(raw, maxBytes)
	}
	if int64(len(raw)) <= maxBytes {
		return raw, false, false
	}
	preview := int64(len(raw))
	if preview > maxBytes {
		preview = maxBytes
	}
	envelope := map[string]any{
		"_truncated":     true,
		"_original_size": len(raw),
		"_preview":       string(raw[:minInt64(int64(len(raw)), preview)]),
	}
	out, err := json.Marshal(envelope)
	if err != nil {
		return []byte(`{"_truncated":true}`), false, true
	}
	return out, false, true
}

// maxPayloadBytes 允许管理员在运行期调整上限（动态配置）。
func (r *Recorder) maxPayloadBytes() int64 {
	if r.settings != nil {
		if v := r.settings.Current().Int64("recording.max_payload_bytes", r.opts.MaxPayloadBytes); v > 0 {
			return v
		}
	}
	return r.opts.MaxPayloadBytes
}

// ---------------------------------------------------------------------------
// 写协程
// ---------------------------------------------------------------------------

func (r *Recorder) run(batchSize int, flushEvery time.Duration) {
	defer close(r.done)

	timer := time.NewTimer(flushEvery)
	defer timer.Stop()

	buf := make([]store.SessionMessage, 0, batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		// 用独立 context，避免调用方请求结束时把写入一起取消
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := r.st.InsertSessionMessages(ctx, buf)
		cancel()
		if err != nil {
			r.writeFail.Add(1)
			log.Printf("[recorder] 批量写入 %d 条会话消息失败: %v", len(buf), err)
		} else {
			r.written.Add(int64(len(buf)))
		}
		buf = buf[:0]
	}

	for {
		select {
		case m := <-r.queue:
			buf = append(buf, m)
			if len(buf) >= batchSize {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(flushEvery)
			}
		case <-timer.C:
			flush()
			timer.Reset(flushEvery)
		case <-r.stop:
			// 尽量把队列里剩下的刷干净，但不无限等
			for {
				select {
				case m := <-r.queue:
					buf = append(buf, m)
					if len(buf) >= batchSize {
						flush()
					}
					continue
				default:
				}
				break
			}
			flush()
			return
		}
	}
}

// Close 停止写协程并尽力刷完队列。可重复调用。
func (r *Recorder) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		close(r.stop)
		<-r.done
	})
}

// Stats 返回运行计数。
func (r *Recorder) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	return Stats{
		Written:   r.written.Load(),
		Dropped:   r.dropped.Load(),
		WriteFail: r.writeFail.Load(),
		Redacted:  r.redacted.Load(),
		Truncated: r.truncated.Load(),
	}
}

// ---------------------------------------------------------------------------
// context 传递
// ---------------------------------------------------------------------------

type ctxKey struct{}

// WithRecorder 把录制器放进 context，供中间件与 handler 取用。
func WithRecorder(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, ctxKey{}, r)
}

// FromContext 取出录制器。不存在时返回 nil，调用方需判空。
func FromContext(ctx context.Context) *Recorder {
	r, _ := ctx.Value(ctxKey{}).(*Recorder)
	return r
}

// RecordFromContext 是调用点的便捷写法，录制器缺失时静默跳过。
func RecordFromContext(ctx context.Context, p Payload) {
	if r := FromContext(ctx); r != nil {
		r.RecordPayload(p)
	}
}
