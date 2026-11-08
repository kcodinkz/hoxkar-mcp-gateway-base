// Package routing 负责「这次请求该落到哪个命名空间的哪个后端」。
//
// 与 internal/auth 的分工（两者正交，不要互相渗透）：
//
//	auth    ：这把 Key 是谁、还能不能用、被允许碰哪些命名空间与工具
//	routing ：路径里的 slug 指向哪个命名空间、那个命名空间的实例现在在哪
//
// 因此路由层本身不做任何凭据解析：Principal 由 auth 中间件放进 context，
// 路由层只消费它。这样「换一套凭据体系」与「换一套后端选址策略」
// 可以各自演进，不必同时改动。
//
// 后端调用被收束在 Backend 接口后面：
//   - stdio 进程池实现它（本地调试命名空间）
//   - HTTP 反代实现它（集群主路径）
//
// 本包只依赖接口，不认识 exec.Command，也不认识 r-nacos。
package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// 路由层自己的错误。鉴权类错误继续沿用 auth 包的哨兵值，
// 这样调用方只需要一个 errors.Is 就能决定 401/403，不必同时认识两套。
var (
	// ErrNamespaceRequired 表示既没写命名空间前缀，也没有可回落的默认命名空间。
	ErrNamespaceRequired = errors.New("未指定命名空间")
	// ErrNoBackend 表示命名空间合法、权限也够，但上游一个可用实例都没有。
	// 这是 503 而不是 500：故障在上游，重试通常有效。
	ErrNoBackend = errors.New("命名空间没有可用后端")
)

// Backend 是一个已经连上的上游，可收发 JSON-RPC 消息。
//
// Forward 的约定与 MCP 传输层一致：
//   - 请求（带 id）返回响应体；
//   - 通知（无 id）返回 nil, nil，调用方据此写 202 且不写 body。
//
// 实现必须对同一个 Backend 的并发调用安全：一个 SSE 会话里的多条消息
// 可能同时被投递，进程池/连接池会把它们排到各自的通道上。
type Backend interface {
	Forward(ctx context.Context, message json.RawMessage) (json.RawMessage, error)
	// Close 归还连接或回收子进程；重复调用必须安全。
	Close() error
}

// BackendFactory 按命名空间开出一个可用的 Backend。
//
// 返回的 error 应当是 ErrNoBackend 的包装（例如「该服务无健康实例」），
// 这样上层不必为不同实现分别写状态码。
type BackendFactory interface {
	Open(ctx context.Context, ns *store.Namespace) (Backend, error)
}

// Options 是路由层的装配参数。
type Options struct {
	// SSEPath / MessagePath 来自 config，用于解析路径前缀。
	// 路由层不硬编码 /sse、/messages，改配置不需要改代码。
	SSEPath     string
	MessagePath string

	// DefaultNamespace 是无前缀请求（/sse）回落的 slug。
	// 空值按 "default" 处理：绝大多数客户端不解命名空间概念，
	// 让它们至少能连上默认后端，比直接 404 有用。
	DefaultNamespace string

	// SessionTTL 是会话绑定的存活上限，默认 12 小时。
	// 超过后视为客户端早已断开（进程可能已被杀掉而没有发 close）。
	SessionTTL time.Duration

	Now func() time.Time
}

func (o *Options) applyDefaults() {
	if o.SSEPath == "" {
		o.SSEPath = "/sse"
	}
	if o.MessagePath == "" {
		o.MessagePath = "/messages"
	}
	if o.DefaultNamespace == "" {
		o.DefaultNamespace = "default"
	}
	if o.SessionTTL <= 0 {
		o.SessionTTL = 12 * time.Hour
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Route 是一次成功裁决的结果：命名空间 + 已连上的后端。
type Route struct {
	Namespace *store.Namespace
	Backend   Backend
}

// Slug 返回规整后的命名空间 slug。
func (r *Route) Slug() string {
	if r == nil || r.Namespace == nil {
		return ""
	}
	return r.Namespace.Slug
}

// Router 把「路径 + 身份」裁决成 Route，并记住会话与后端的绑定关系。
//
// 之所以要有会话绑定表，而不是在 /messages 时重新裁决一次：
//
//	SSE 建连时选定的后端不能在中途漂移。若每条消息都重新做一次 round-robin，
//	同一个会话的消息会被打到不同实例上，而 MCP 的工具调用是有状态的
//	（例如先 open 再 render）——漂移的后果是「工具突然说没这个 handle」。
//
// 同时这张表也是越权防线：sessionID 与它建连时的 Key 绑定，
// 别的 Key 即使猜到 sessionID 也投递不进去。
type Router struct {
	authz   *auth.Authorizer
	factory BackendFactory
	opts    Options

	mu       sync.RWMutex
	sessions map[string]*binding
}

// binding 是一条会话绑定。
type binding struct {
	slug      string
	namespace *store.Namespace
	backend   Backend
	keyID     string
	createdAt time.Time
	lastUsed  time.Time
}

// NewRouter 构造路由表。
//
// authz 与 factory 都不能为空：前者缺失意味着可以在不裁决权限的情况下
// 打通上游，后者缺失意味着永远选不到后端。两者都属于装配错误，
// 与其让它表现为运行时的空指针，不如在启动时就炸掉。
func NewRouter(authz *auth.Authorizer, factory BackendFactory, opts Options) *Router {
	if authz == nil {
		panic("routing.NewRouter: authz 不能为 nil")
	}
	if factory == nil {
		panic("routing.NewRouter: factory 不能为 nil")
	}
	opts.applyDefaults()
	return &Router{
		authz:    authz,
		factory:  factory,
		opts:     opts,
		sessions: make(map[string]*binding),
	}
}

// Parse 是 ParsePath 的便捷入口，用构造时配置的端点路径解析。
func (r *Router) Parse(path string) Target {
	return ParsePath(r.opts.SSEPath, r.opts.MessagePath, path)
}

// Open 为一次建连裁决出 Route。
//
// target 必须来自 Parse，且 Endpoint 为 EndpointSSE：消息端点不接受
// 建连语义，它只能挂在已有会话上（见 Dial）。
//
// 裁决顺序刻意固定为「解析命名空间 → 查权限 → 开后端」：
// 权限必须在开后端之前，否则越权请求也会在上游排一个进程或占一条连接，
// 探测者可以用免费的错误请求把上游资源耗光。
func (r *Router) Open(ctx context.Context, p *auth.Principal, target Target) (*Route, error) {
	ns, err := r.resolveNamespace(ctx, p, target)
	if err != nil {
		return nil, err
	}

	backend, err := r.factory.Open(ctx, ns)
	if err != nil {
		return nil, err
	}
	return &Route{Namespace: ns, Backend: backend}, nil
}

// resolveNamespace 完成「slug → 命名空间行 → 该 Key 是否有权」两段裁决。
func (r *Router) resolveNamespace(ctx context.Context, p *auth.Principal, target Target) (*store.Namespace, error) {
	if p == nil {
		// 装配顺序错误：路由必须挂在 auth.Middleware 之后。
		// 这里返回 ErrForbidden 而不是 panic，避免一个漏挂的路由把进程打崩。
		return nil, fmt.Errorf("%w: 缺少调用者身份", auth.ErrForbidden)
	}

	slug := target.Slug
	if slug == "" {
		if target.Scoped {
			// 写了前缀但段内为空（如 /sse/%20）：这是路径错误，不能回落到默认命名空间。
			return nil, fmt.Errorf("%w: 命名空间为空", auth.ErrForbidden)
		}
		slug = r.opts.DefaultNamespace
		if slug == "" {
			return nil, ErrNamespaceRequired
		}
	}

	// ResolveNamespace 已经把「不存在」与「被禁用」统一成 ErrForbidden，
	// 路由层不再重复区分，避免两处口径漂移。
	ns, err := r.authz.ResolveNamespace(ctx, slug)
	if err != nil {
		return nil, err
	}

	ok, err := r.authz.AllowsNamespace(ctx, p, ns)
	if err != nil {
		return nil, fmt.Errorf("裁决命名空间权限失败: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: API Key 无权使用命名空间 %q", auth.ErrForbidden, ns.Slug)
	}
	return ns, nil
}

// Dial 给一个新建的会话绑定后端。
//
// 与 Open 分开是因为建连端点与消息端点走的是两条路径：
// 建连时才有「选后端」的自由度，之后只能是「找回原来那个」。
func (r *Router) Dial(sessionID string, p *auth.Principal, route *Route) {
	if sessionID == "" || route == nil || p == nil {
		return
	}
	now := r.opts.Now()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[sessionID] = &binding{
		slug:      route.Slug(),
		namespace: route.Namespace,
		backend:   route.Backend,
		keyID:     string(p.KeyID),
		createdAt: now,
		lastUsed:  now,
	}
}

// Lookup 取回会话绑定的 Route。
//
// 三种情况都必须拒绝，且对外表现一致（上层统一回 404 / 403）：
//   - 会话不存在（过期回收或客户端伪造）
//   - 会话属于另一个 Key（sessionID 泄露）
//   - 会话已超过 TTL
//
// slug 不为空时还要求与建连时的 slug 一致：/messages/blender?sessionId=<a 的会话>
// 不能借另一条路径把消息送进别的命名空间。
func (r *Router) Lookup(sessionID string, p *auth.Principal, slug string) (*Route, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("%w: 缺少 sessionId", ErrNamespaceRequired)
	}
	if p == nil {
		return nil, fmt.Errorf("%w: 缺少调用者身份", auth.ErrForbidden)
	}

	now := r.opts.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	b, ok := r.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: 会话不存在或已过期", ErrSessionUnknown)
	}
	if now.Sub(b.createdAt) > r.opts.SessionTTL {
		delete(r.sessions, sessionID)
		return nil, fmt.Errorf("%w: 会话不存在或已过期", ErrSessionUnknown)
	}
	if b.keyID != string(p.KeyID) {
		// 不复用 401/403 的措辞：调用方凭据本身没问题，问题在这个会话不属于他。
		return nil, fmt.Errorf("%w: 会话不属于当前 API Key", auth.ErrForbidden)
	}
	if slug != "" && slug != b.slug {
		return nil, fmt.Errorf("%w: 会话与命名空间 %q 不匹配", auth.ErrForbidden, slug)
	}

	b.lastUsed = now
	return &Route{Namespace: b.namespace, Backend: b.backend}, nil
}

// Release 关闭并移除一条会话绑定。返回的后端由调用方负责关闭语义——
// 这里直接关掉，因为路由层是按会话独占后端的。
func (r *Router) Release(sessionID string) {
	if sessionID == "" {
		return
	}
	r.mu.Lock()
	b, ok := r.sessions[sessionID]
	if ok {
		delete(r.sessions, sessionID)
	}
	r.mu.Unlock()

	if ok && b.backend != nil {
		_ = b.backend.Close()
	}
}

// Sweep 回收超时会话，供后台维护任务调用。
//
// 不能只靠客户端断开时调用 Release：进程被 kill -9、网络静默断链
// 都不会走到 defer，绑定会一直留着，连带把后端句柄也一起占住。
func (r *Router) Sweep() int {
	now := r.opts.Now()
	deadline := r.opts.SessionTTL

	r.mu.Lock()
	var expired []Backend
	for id, b := range r.sessions {
		if now.Sub(b.createdAt) > deadline {
			if b.backend != nil {
				expired = append(expired, b.backend)
			}
			delete(r.sessions, id)
		}
	}
	r.mu.Unlock()

	for _, b := range expired {
		_ = b.Close()
	}
	return len(expired)
}

// SessionCount 返回当前绑定数，供管理端与测试观察。
func (r *Router) SessionCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}

// ToolCall 从一条 JSON-RPC 消息里取出要调用的工具名。
//
// 返回 ok=false 表示这不是一次工具调用（initialize / tools/list / ping…），
// 此时只需命名空间级权限即可放行。
//
// 解析失败也不报错：消息体是否合法由上游判断，路由层只负责「能判断就判断」。
// 在这里抢先回 400 会把「协议错误」和「越权」混成同一个现象，反而更难排查。
func ToolCall(message json.RawMessage) (tool string, ok bool) {
	var probe struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	if err := json.Unmarshal(message, &probe); err != nil {
		return "", false
	}
	if !auth.ToolsCallMethod(probe.Method) {
		return "", false
	}
	return strings.TrimSpace(probe.Params.Name), true
}

// AuthorizeTool 在转发前做工具级裁决。
//
// 工具名为空（协议上不合法，但客户端会这么发）按「不许」处理：
// 让它在网关这里 403，而不是把一条残缺消息送给上游再拿一个语义不明的错误。
func (r *Router) AuthorizeTool(ctx context.Context, p *auth.Principal, route *Route, message json.RawMessage) error {
	tool, ok := ToolCall(message)
	if !ok {
		return nil
	}
	if tool == "" {
		return fmt.Errorf("%w: tools/call 缺少工具名", auth.ErrForbidden)
	}

	allowed, err := r.authz.AllowsTool(ctx, p, route.Namespace, tool)
	if err != nil {
		return fmt.Errorf("裁决工具权限失败: %w", err)
	}
	if !allowed {
		return fmt.Errorf("%w: API Key 无权调用工具 %q", auth.ErrForbidden, tool)
	}
	return nil
}

// ErrSessionUnknown 表示 sessionID 没有对应的活跃绑定。
var ErrSessionUnknown = errors.New("会话不存在或已过期")

// WriteError 把路由错误写成 HTTP 响应。
//
// 鉴权类错误直接交给 auth.WriteAuthError，保证同一个错误在
// 不同端点上有一致的响应体（客户端只会解析一次 error.code）。
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, auth.ErrMissingToken), errors.Is(err, auth.ErrInvalidToken),
		errors.Is(err, auth.ErrKeyDisabled), errors.Is(err, auth.ErrKeyExpired),
		errors.Is(err, auth.ErrUserNotActive), errors.Is(err, auth.ErrForbidden):
		auth.WriteAuthError(w, r, err)
	case errors.Is(err, ErrSessionUnknown):
		writeJSONError(w, http.StatusNotFound, "session_unknown", err.Error())
	case errors.Is(err, ErrNamespaceRequired):
		writeJSONError(w, http.StatusBadRequest, "namespace_required", err.Error())
	case errors.Is(err, ErrNoBackend):
		// 503 + Retry-After：上游暂时没有实例，客户端稍后重试即可恢复，
		// 用 500 会让监控把它算成网关自身的故障。
		w.Header().Set("Retry-After", "5")
		writeJSONError(w, http.StatusServiceUnavailable, "no_backend", err.Error())
	default:
		writeJSONError(w, http.StatusInternalServerError, "internal_error", "网关内部错误")
	}
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":{"code":"` + code + `","message":` + quoteJSON(message) + `}}`))
}

// quoteJSON 是最小化的 JSON 字符串转义，只用于错误消息。
func quoteJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(b)
}
