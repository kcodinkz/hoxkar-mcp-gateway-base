package routing

// 本文件是 transport=http：transport=http 的远端后端，也就是**集群主路径**。
//
// 数据流（用户的原始意图）：
//
//	Rust 后端启动 → 向 r-nacos 注册并心跳
//	网关        → 从 r-nacos 发现「服务名 → 健康实例」
//	客户端      → 带 API Key 连 /sse/{ns}，经统一鉴权
//	网关        → 把 JSON-RPC 消息转发给该命名空间下的某个健康实例，取回结果
//
// 这里刻意只做「一条消息进、一条消息出」的反代，不解析 MCP 语义：
// 工具列表、握手、能力协商全部原样透传给上游。网关越薄，后端升级越自由。
//
// 三条硬性决策，都是为了避免生产事故：
//
//  1. 会话中途不换实例。实例是在 SSE 建连时选定的（Router.Dial 固化），
//     之后整个会话都发给它。MCP 的工具调用有状态（先 open 再 render），
//     中途漂移到另一个实例会得到「没这个 handle」。因此这里不做
//     「失败就换一个试试」——那会把可用性问题变成正确性问题。
//     探测到实例连不上时只记冷却期，让**新**会话避开它。
//
//  2. 不重试非幂等请求。只有「连 TCP 都没连上」才换实例，因为那种情况下
//     请求肯定没到达上游。拿到响应之后的任何 5xx 都原样上报，
//     否则一次「创建文件」可能被执行两遍。
//
//  3. 上游会话（Mcp-Session-Id）逐会话保存。有状态的 streamable-HTTP 后端
//     靠这个 id 关联上下文，丢了它等于丢了整段对话。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// ErrUpstreamSessionLost 表示上游说「你给我的会话 id 已经不存在了」。
//
// 单独一个哨兵值是因为它的处置方式和别的错误完全不同：
// 客户端应当重新 initialize，而不是重试同一条消息。映射成 404
// 也正是 MCP streamable-HTTP 规范里「会话需重建」的信号。
var ErrUpstreamSessionLost = errors.New("上游会话已失效，需要重新握手")

const (
	// maxHTTPResponseBytes 是单次上游响应的上限，防止一个坏实例把网关内存吃光。
	maxHTTPResponseBytes = 32 << 20

	// defaultUpstreamCooldown 是实例连接失败后的本地避让窗口。
	// 取得比 r-nacos 心跳周期长一些：心跳没来得及把它标不健康时，
	// 这段时间足够让新会话绕开它。
	defaultUpstreamCooldown = 15 * time.Second

	// defaultHTTPTimeout 与 store 对 timeout_ms 的兜底值一致（120000）。
	defaultHTTPTimeout = 120 * time.Second
)

// HTTPFactoryOptions 是反代工厂的装配参数。
type HTTPFactoryOptions struct {
	// Scheme 是上游协议，默认 http。集群内网通常不开 TLS。
	Scheme string
	// Client 允许注入带自定义 Transport 的客户端（测试用 httptest 时必需）。
	Client *http.Client
	// Logf 为 nil 时写标准日志。
	Logf func(string, ...any)
	// Cooldown 为实例连接失败后的避让时长，默认 15s。
	Cooldown time.Duration
	// Now 便于测试控制冷却期。
	Now func() time.Time
}

// HTTPFactory 是 transport=http 的 BackendFactory 实现。
type HTTPFactory struct {
	resolver discovery.Resolver
	client   *http.Client
	scheme   string
	logf     func(string, ...any)
	cooldown time.Duration
	now      func() time.Time

	mu sync.Mutex
	// current 是平滑加权轮询的中间状态，键为 "service|addr"。
	current map[string]int
	// down 记录实例的避让截止时间，键为 "service|addr"。
	down   map[string]time.Time
	closed bool
}

var _ BackendFactory = (*HTTPFactory)(nil)

// NewHTTPFactory 构造反代工厂。
func NewHTTPFactory(resolver discovery.Resolver, opts HTTPFactoryOptions) *HTTPFactory {
	if resolver == nil {
		panic("routing.NewHTTPFactory: resolver 不能为 nil")
	}
	if opts.Scheme == "" {
		opts.Scheme = "http"
	}
	if opts.Client == nil {
		// 不设 Timeout：单次转发用 context 控制，避免与命名空间级
		// timeout_ms 叠加成两个互相打架的上限。
		opts.Client = &http.Client{}
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = defaultUpstreamCooldown
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &HTTPFactory{
		resolver: resolver,
		client:   opts.Client,
		scheme:   opts.Scheme,
		logf:     opts.Logf,
		cooldown: opts.Cooldown,
		now:      opts.Now,
		current:  make(map[string]int),
		down:     make(map[string]time.Time),
	}
}

// Open 选出该命名空间的一个健康实例并固定下来。
//
// 选不到实例时返回 ErrNoBackend（上层 503 + Retry-After），这是可重试的
// 上游暂时不可用，而不是网关自身故障。
func (f *HTTPFactory) Open(ctx context.Context, ns *store.Namespace) (Backend, error) {
	if ns == nil {
		return nil, fmt.Errorf("%w: 命名空间为空", ErrNoBackend)
	}
	if ns.Transport != store.TransportHTTP {
		return nil, fmt.Errorf("%w: 命名空间 %q 的传输方式是 %q，HTTP 工厂只能处理 %q",
			ErrNoBackend, ns.Slug, ns.Transport, store.TransportHTTP)
	}

	service := strings.TrimSpace(ns.DiscoveryService)
	if service == "" {
		return nil, fmt.Errorf("%w: http 命名空间 %q 未配置 discovery_service",
			ErrNoBackend, ns.Slug)
	}

	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("%w: HTTP 工厂已关闭", ErrNoBackend)
	}

	insts, err := f.resolve(ctx, service, ns.Slug)
	if err != nil {
		return nil, err
	}

	inst := f.pick(service, insts)

	timeout := time.Duration(ns.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}

	endpoint := ns.EndpointPath
	if endpoint == "" {
		endpoint = "/mcp"
	}

	return &httpBackend{
		slug:    ns.Slug,
		service: service,
		addr:    inst.Addr(),
		url:     inst.URL(f.scheme, endpoint),
		client:  f.client,
		timeout: timeout,
		logf:    f.logf,
		onDialFail: func(addr string) {
			f.markDown(service, addr)
		},
	}, nil
}

// resolve 调用发现层，并**正确处理 DroppedError**。
//
// 这是最容易写错的一处：rnacos 在丢弃了部分实例（不健康/公网地址）时，
// 会同时返回「清洗后的可用实例」和一个 DroppedError。若把 err != nil
// 当成失败直接返回，一个投毒实例就能让整个命名空间 503 —— 而实际上
// 剩下的实例都是好的。因此：有实例就用实例，err 只用来记日志。
func (f *HTTPFactory) resolve(ctx context.Context, service, slug string) ([]discovery.Instance, error) {
	insts, err := f.resolver.Resolve(ctx, service)

	var dropped *discovery.DroppedError
	switch {
	case err == nil:
		// 正常路径
	case errors.As(err, &dropped) && len(insts) > 0:
		f.logf("命名空间 %q 的上游服务 %q 丢弃了 %d 个不可用实例，剩余 %d 个可用",
			slug, service, dropped.Count, len(insts))
	case errors.Is(err, discovery.ErrNoInstances):
		return nil, fmt.Errorf("%w: 服务 %q 当前没有健康实例", ErrNoBackend, service)
	default:
		// 发现层自身故障（注册中心 5xx/超时）。fail_open 由发现层内部决定，
		// 走到这里说明连兜底地址都没有，只能如实上报为上游客不可用。
		return nil, fmt.Errorf("%w: 解析服务 %q 失败: %v", ErrNoBackend, service, err)
	}

	if len(insts) == 0 {
		return nil, fmt.Errorf("%w: 服务 %q 当前没有健康实例", ErrNoBackend, service)
	}
	return insts, nil
}

// pick 先剔除处于冷却期的实例，再做平滑加权轮询。
//
// 全部实例都在冷却期时仍然从中选一个：宁可试一个刚失败的，
// 也不要因为「全部被标记」而凭空制造 503。
func (f *HTTPFactory) pick(service string, insts []discovery.Instance) discovery.Instance {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.now()
	f.pruneLocked(service, insts)

	usable := make([]discovery.Instance, 0, len(insts))
	for _, it := range insts {
		if until, bad := f.down[service+"|"+it.Addr()]; bad && now.Before(until) {
			continue
		}
		usable = append(usable, it)
	}
	if len(usable) == 0 {
		usable = insts
	}

	best := 0
	total := 0
	bests := make([]int, len(usable))
	for i, it := range usable {
		w := weightOf(it)
		key := service + "|" + it.Addr()
		f.current[key] += w
		bests[i] = f.current[key]
		total += w
		if bests[i] > bests[best] {
			best = i
		}
	}
	f.current[service+"|"+usable[best].Addr()] -= total
	return usable[best]
}

// pruneLocked 清掉已不在实例列表里的轮询状态与冷却记录，避免 map 无界增长。
func (f *HTTPFactory) pruneLocked(service string, insts []discovery.Instance) {
	alive := make(map[string]struct{}, len(insts))
	for _, it := range insts {
		alive[service+"|"+it.Addr()] = struct{}{}
	}
	prefix := service + "|"
	for k := range f.current {
		if strings.HasPrefix(k, prefix) {
			if _, ok := alive[k]; !ok {
				delete(f.current, k)
			}
		}
	}
	for k := range f.down {
		if strings.HasPrefix(k, prefix) {
			if _, ok := alive[k]; !ok {
				delete(f.down, k)
			}
		}
	}
}

func (f *HTTPFactory) markDown(service, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down[service+"|"+addr] = f.now().Add(f.cooldown)
}

// Close 让工厂满足 io.Closer。反代没有需要回收的子进程，
// 共享的 http.Client 由调用方持有，这里不清空连接池。
func (f *HTTPFactory) Close() error {
	f.mu.Lock()
	f.closed = true
	f.current = make(map[string]int)
	f.down = make(map[string]time.Time)
	f.mu.Unlock()
	return nil
}

// DownInstances 返回当前处于冷却期的实例数，供测试与运维观察。
func (f *HTTPFactory) DownInstances() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	n := 0
	for _, until := range f.down {
		if now.Before(until) {
			n++
		}
	}
	return n
}

// weightOf 取实例权重，非法值按 1 处理。
//
// 权重为 0 在平滑加权轮询里等价于「永不选中」，但 r-nacos 允许 weight=0
// 表示「权重待定」而不是「下线」，所以这里兜底成 1，下线只认 healthy/enabled。
func weightOf(it discovery.Instance) int {
	if it.Weight <= 0 {
		return 1
	}
	w := int(it.Weight + 0.5)
	if w < 1 {
		return 1
	}
	return w
}

// ---------------------------------------------------------------------------
// 反代后端
// ---------------------------------------------------------------------------

// httpBackend 是一个已固定到具体实例的远端后端。
//
// 并发语义：Forward 串行（b.mu）。串行不是为了排队省资源，而是因为
// 同一个上游会话（Mcp-Session-Id）上的请求可能隐含着顺序依赖，
// 并发写入会让「先 open 再 render」在极端情况下乱序。
type httpBackend struct {
	slug    string
	service string
	addr    string
	url     string

	client  *http.Client
	timeout time.Duration
	logf    func(string, ...any)

	onDialFail func(addr string)

	mu sync.Mutex
	// sessionID 是上游下发的 Mcp-Session-Id，逐会话保存。
	sessionID string
	closed    bool
}

// Forward 把一条 JSON-RPC 消息 POST 给上游并取回响应。
func (b *httpBackend) Forward(ctx context.Context, message json.RawMessage) (json.RawMessage, error) {
	compact, err := compactMessage(message)
	if err != nil {
		return nil, fmt.Errorf("HTTP 后端 %q 收到的消息不是合法 JSON: %w", b.slug, err)
	}
	wantID, isRequest := messageID(compact)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errors.New("HTTP 后端已关闭")
	}
	sessionID := b.sessionID
	b.mu.Unlock()

	status, body, respSession, err := b.post(ctx, compact, sessionID)
	if err != nil {
		return nil, err
	}

	// 上游说「这个会话 id 不认识」：清掉它并如实上报，
	// 让客户端重新握手，而不是拿着一个死 id 反复撞墙。
	if status == http.StatusNotFound && sessionID != "" {
		b.mu.Lock()
		if b.sessionID == sessionID {
			b.sessionID = ""
		}
		b.mu.Unlock()
		return nil, ErrUpstreamSessionLost
	}
	if status >= 400 {
		return nil, fmt.Errorf("HTTP 后端 %q 返回 %d: %s",
			b.slug, status, truncate(body, 300))
	}

	if respSession != "" && respSession != sessionID {
		b.mu.Lock()
		b.sessionID = respSession
		b.mu.Unlock()
	}

	// 通知没有响应体：202 + 空 body 是正常结局。
	if !isRequest {
		return nil, nil
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}

	return b.extract(body, wantID)
}

// post 发一次请求，返回状态码、响应体、上游会话 id。
//
// 只有「TCP 都没连上」才记冷却期并给出可排查的错误；其余情况原样返回，
// 由调用方按状态码处理——5xx 属于上游故障，重试可能造成重复副作用。
func (b *httpBackend) post(ctx context.Context, payload []byte, sessionID string) (int, []byte, string, error) {
	reqCtx := ctx
	if b.timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, b.timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, b.url, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, "", fmt.Errorf("构造上游请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// 同时接受两种响应形态：streamable-HTTP 允许服务端用 SSE 回一条消息。
	req.Header.Set("Accept", "application/json, text/event-stream")
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		if isDialError(err) {
			b.onDialFail(b.addr)
			return 0, nil, "", fmt.Errorf("上游实例 %s 连不上（已进入避让期）: %w", b.addr, err)
		}
		return 0, nil, "", fmt.Errorf("请求上游 %s 失败: %w", b.addr, err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxHTTPResponseBytes))
	if readErr != nil {
		return resp.StatusCode, nil, "", fmt.Errorf("读取上游响应失败: %w", readErr)
	}

	return resp.StatusCode, body, resp.Header.Get("Mcp-Session-Id"), nil
}

// extract 从响应体里挑出与请求 id 对应的那一条消息。
//
// 需要同时支持两种形态，否则「能用」和「不能用」之间只差一个后端实现：
//   - application/json：单条对象，或批量数组；
//   - text/event-stream：每条消息一行 data:，可能夹杂上游主动推送的通知。
func (b *httpBackend) extract(body []byte, wantID json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(body)

	if len(trimmed) > 0 && trimmed[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(trimmed, &batch); err != nil {
			return nil, fmt.Errorf("上游返回的批量响应无法解析: %w", err)
		}
		for _, item := range batch {
			if matched, ok := matchMessage(item, wantID); ok {
				return matched, nil
			}
		}
		return nil, fmt.Errorf("上游批量响应里没有 id 匹配的消息: %s", truncate(trimmed, 300))
	}

	if len(trimmed) > 0 && trimmed[0] == '{' {
		if matched, ok := matchMessage(trimmed, wantID); ok {
			return matched, nil
		}
		// 结构合法但不是我们要的那条：可能是上游推的通知，交给 SSE 分支再判一次。
		if bytes.Contains(trimmed, []byte("data:")) {
			return b.extractSSE(trimmed, wantID)
		}
		return nil, fmt.Errorf("上游响应不是本请求的结果: %s", truncate(trimmed, 300))
	}

	return b.extractSSE(trimmed, wantID)
}

// extractSSE 在 SSE 文本里找 id 匹配的 data 行。
func (b *httpBackend) extractSSE(body []byte, wantID json.RawMessage) (json.RawMessage, error) {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64<<10), maxHTTPResponseBytes)

	var data []byte
	flush := func() (json.RawMessage, bool) {
		if len(data) == 0 {
			return nil, false
		}
		candidate := bytes.TrimSpace(data)
		data = nil
		if matched, ok := matchMessage(candidate, wantID); ok {
			return matched, true
		}
		return nil, false
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			// 空行是 SSE 的事件边界。
			if msg, ok := flush(); ok {
				return msg, nil
			}
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			// event:/id:/retry:/注释行都与消息体无关。
			continue
		}
		payload := bytes.TrimPrefix(line, []byte("data:"))
		data = append(data, bytes.TrimSpace(payload)...)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("解析上游 SSE 响应失败: %w", err)
	}
	if msg, ok := flush(); ok {
		return msg, nil
	}
	return nil, fmt.Errorf("上游 SSE 响应里没有 id 匹配的消息: %s", truncate(body, 300))
}

// matchMessage 判断一条消息是否就是本次请求的响应。
func matchMessage(raw json.RawMessage, wantID json.RawMessage) (json.RawMessage, bool) {
	var probe struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, false
	}
	if len(bytes.TrimSpace(probe.ID)) == 0 {
		return nil, false
	}
	if string(bytes.TrimSpace(probe.ID)) == "null" {
		// 服务端主动通知：当前 Backend 接口只支持请求-响应，丢弃。
		return nil, false
	}
	if !idEqual(probe.ID, wantID) {
		return nil, false
	}
	return json.RawMessage(bytes.TrimSpace(raw)), true
}

// Close 标记后端不可用。反代不持有独占资源，无需归还任何东西。
func (b *httpBackend) Close() error {
	b.mu.Lock()
	b.closed = true
	b.sessionID = ""
	b.mu.Unlock()
	return nil
}

// isDialError 判断错误是否发生在「建立连接」阶段。
//
// 这个区分是重试策略的安全前提：dial 失败意味着请求字节一个都没发出去，
// 换实例重试不会造成重复副作用；而读写阶段的错误无法判断上游是否已经执行，
// 贸然重试可能把「创建」执行两遍。
func isDialError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	return false
}
