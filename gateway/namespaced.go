package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/routing"
)

// NamespacedMCP 是多租户模式下的 MCP 入口：/sse/{ns} 与 /messages/{ns}。
//
// 它与旧 SSEServer 的区别在于「后端不是一个本地子进程」：
// 建连时由 routing.Router 选出一个上游（r-nacos 实例或 stdio 池），
// 之后这个会话的所有消息都投给同一个上游，直到客户端断开。
//
// 为什么不复用 SSEServer.handleSSE：
//   - 那个实现把会话绑在 mcp-go 的 MCPServer 上，只能服务进程内注册的工具；
//   - 这里工具在后端，网关只做鉴权与转发，不需要 MCPServer 参与协议。
//
// 也就是说，本类型是纯反向代理 + 鉴权闸门，不持有任何工具定义。
type NamespacedMCP struct {
	router *routing.Router

	// messageBase 用于在 endpoint 事件里告诉客户端「消息往哪投」。
	// 形如 /messages，实际下发时会补上 /{ns}?sessionId=xxx。
	messageBase string

	keepAlive time.Duration

	// maxBodyBytes 限制单条 JSON-RPC 消息的体积，避免恶意客户端
	// 用超大 body 把网关内存吃掉。0 表示用默认 4 MiB。
	maxBodyBytes int64

	mu      sync.RWMutex
	streams map[string]*mcpStream
}

// mcpStream 是一条 SSE 长连接的写出侧状态。
//
// 只存「怎么写回去」，不存权限与后端：那两样由 routing.Router 持有，
// 避免同一个事实存在两份、然后慢慢对不上。
type mcpStream struct {
	writer  http.ResponseWriter
	flusher http.Flusher
	queue   chan string
	done    chan struct{}
	once    sync.Once
}

func (s *mcpStream) close() {
	s.once.Do(func() { close(s.done) })
}

// send 把一段 SSE 文本排进出队队列。
//
// 队列满时丢弃而不是阻塞：SSE 客户端读得慢，阻塞会把 /messages 的
// HTTP 请求一起卡住，最终拖垮所有会话。丢弃的代价只是客户端少看一条
// 事件，而它可以重连；卡死的代价是整个网关。
func (s *mcpStream) send(event string) {
	select {
	case s.queue <- event:
	case <-s.done:
	default:
	}
}

// NamespacedMCPOptions 是构造参数。
type NamespacedMCPOptions struct {
	// MessageBase 是消息端点路径（来自 config.Server.MessagePath）。
	MessageBase string
	// KeepAlive 为 0 时不发心跳；建议 20s 左右，防止中间代理掐断空闲连接。
	KeepAlive time.Duration
	// MaxBodyBytes 为 0 时取 4 MiB。
	MaxBodyBytes int64
}

const defaultMaxBodyBytes int64 = 4 << 20

// NewNamespacedMCP 构造多命名空间入口。
func NewNamespacedMCP(router *routing.Router, opts NamespacedMCPOptions) *NamespacedMCP {
	if router == nil {
		panic("gateway.NewNamespacedMCP: router 不能为 nil")
	}
	if opts.MessageBase == "" {
		opts.MessageBase = "/messages"
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = defaultMaxBodyBytes
	}
	return &NamespacedMCP{
		router:       router,
		messageBase:  strings.TrimSuffix(opts.MessageBase, "/"),
		keepAlive:    opts.KeepAlive,
		maxBodyBytes: opts.MaxBodyBytes,
		streams:      make(map[string]*mcpStream),
	}
}

// Handles 报告某条路径是否属于 MCP 端点（含无前缀写法）。
//
// 路由装配处靠它决定「交给本入口还是交给旧逻辑」：
// 多租户模式下 /sse 与 /messages 必须全部由这里接管，
// 否则会落到只认本地子进程的旧 handler 上，得到一个莫名其妙的连接错误。
func (n *NamespacedMCP) Handles(path string) bool {
	return n.router.Parse(path).Endpoint != routing.EndpointNone
}

// ServeHTTP 实现 http.Handler。必须挂在 auth.Middleware 之后。
func (n *NamespacedMCP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := n.router.Parse(r.URL.Path)

	switch target.Endpoint {
	case routing.EndpointSSE:
		n.handleSSE(w, r, target)
	case routing.EndpointMessage:
		n.handleMessage(w, r, target)
	default:
		http.NotFound(w, r)
	}
}

func (n *NamespacedMCP) handleSSE(w http.ResponseWriter, r *http.Request, target routing.Target) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writePlainError(w, http.StatusMethodNotAllowed, "建连端点只接受 GET")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writePlainError(w, http.StatusInternalServerError, "当前 ResponseWriter 不支持流式输出")
		return
	}

	p := auth.PrincipalFrom(r.Context())

	route, err := n.router.Open(r.Context(), p, target)
	if err != nil {
		routing.WriteError(w, r, err)
		return
	}

	sessionID := uuid.New().String()
	stream := &mcpStream{
		writer:  w,
		flusher: flusher,
		queue:   make(chan string, 100),
		done:    make(chan struct{}),
	}

	n.mu.Lock()
	n.streams[sessionID] = stream
	n.mu.Unlock()

	// 组装顺序必须是「先绑会话，再发 endpoint」：
	// 客户端收到 endpoint 后会立刻 POST 第一条消息，若这时绑定还没建立，
	// 那条消息会拿到 404 —— 而且只在慢客户端上偶发，极难排查。
	n.router.Dial(sessionID, p, route)

	defer func() {
		n.mu.Lock()
		delete(n.streams, sessionID)
		n.mu.Unlock()
		stream.close()
		n.router.Release(sessionID)
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// 反向代理常见配置会缓冲响应，SSE 必须逐条送达才可用。
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "event: endpoint\ndata: %s\r\n\r\n", n.messageEndpoint(target.Slug, sessionID))
	flusher.Flush()

	var ticker *time.Ticker
	if n.keepAlive > 0 {
		ticker = time.NewTicker(n.keepAlive)
		defer ticker.Stop()
	}

	for {
		select {
		case event := <-stream.queue:
			fmt.Fprint(w, event)
			flusher.Flush()
		case <-tickerC(ticker):
			// SSE 注释行即心跳，客户端会忽略它，但能让中间设备认为连接活跃。
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case <-stream.done:
			return
		case <-r.Context().Done():
			return
		}
	}
}

// tickerC 让「可能为 nil 的 ticker」可以安全地出现在 select 里。
// nil channel 永远阻塞，正好表达「没开心跳」。
func tickerC(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

func (n *NamespacedMCP) handleMessage(w http.ResponseWriter, r *http.Request, target routing.Target) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writePlainError(w, http.StatusMethodNotAllowed, "消息端点只接受 POST")
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		writePlainError(w, http.StatusBadRequest, "缺少 sessionId")
		return
	}

	p := auth.PrincipalFrom(r.Context())

	// Lookup 同时完成三件事：会话存在、属于这个 Key、slug 与建连时一致。
	// 早于读取 body 就做完，避免为一条注定被拒的消息解析几 MB 的 JSON。
	route, err := n.router.Lookup(sessionID, p, target.Slug)
	if err != nil {
		routing.WriteError(w, r, err)
		return
	}

	n.mu.RLock()
	stream := n.streams[sessionID]
	n.mu.RUnlock()
	if stream == nil {
		// 绑定还在但流已断：客户端可能刚断线又重发。按会话未知处理，
		// 让它重连而不是把消息送给一个没人读的队列。
		routing.WriteError(w, r, fmt.Errorf("%w: 会话不存在或已过期", routing.ErrSessionUnknown))
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, n.maxBodyBytes))
	if err != nil {
		writePlainError(w, http.StatusRequestEntityTooLarge, "消息体过大或读取失败")
		return
	}

	if err := n.router.AuthorizeTool(r.Context(), p, route, body); err != nil {
		routing.WriteError(w, r, err)
		return
	}

	response, err := route.Backend.Forward(r.Context(), body)
	if err != nil {
		// 上游故障用 502：这是「网关活着但后端挂了」，与网关自身 500 必须分开，
		// 否则告警会指错方向。
		writePlainError(w, http.StatusBadGateway, fmt.Sprintf("上游调用失败: %v", err))
		return
	}

	if response == nil {
		// 通知（无 id）没有响应体，按 JSON-RPC 传输约定回 202 空 body。
		w.WriteHeader(http.StatusAccepted)
		return
	}

	stream.send(fmt.Sprintf("event: message\ndata: %s\n\n", response))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(response)
}

// messageEndpoint 拼出投递地址。
//
// 始终返回路径形式（不带 scheme/host）：网关不知道自己是挂在
// 域名根下还是某个子路径下，返回绝对地址在反代后经常是错的。
func (n *NamespacedMCP) messageEndpoint(slug, sessionID string) string {
	base := n.messageBase
	if slug != "" {
		base = base + "/" + slug
	}
	return base + "?sessionId=" + sessionID
}

// CloseAll 关闭全部 SSE 流，供优雅退出使用。
//
// 只是关闭写出循环；后端由 routing.Router 的 Release 回收，
// 这里不越权去关别人的东西。
func (n *NamespacedMCP) CloseAll() int {
	n.mu.Lock()
	streams := make([]*mcpStream, 0, len(n.streams))
	for id, s := range n.streams {
		streams = append(streams, s)
		delete(n.streams, id)
	}
	n.mu.Unlock()

	for _, s := range streams {
		s.close()
	}
	return len(streams)
}

// StreamCount 返回当前活跃流数量，供测试与健康检查观察。
func (n *NamespacedMCP) StreamCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.streams)
}

func writePlainError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	payload, err := json.Marshal(map[string]any{
		"error": map[string]any{"code": http.StatusText(status), "message": message},
	})
	if err != nil {
		return
	}
	_, _ = w.Write(payload)
}

// 确保 NamespacedMCP 满足 http.Handler 与「可被 context 取消」的用法。
var _ http.Handler = (*NamespacedMCP)(nil)

// 保留 context 引用，避免将来加入上游超时控制时忘记该依赖。
var _ = context.Canceled
