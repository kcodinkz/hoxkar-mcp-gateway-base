package auth

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ctxKey 是本包在 context 中放值的私有键类型。
//
// 用自定义类型而不是字符串常量，避免与其它包（尤其是第三方中间件）
// 恰好用同名 key 互相覆盖 —— 那种 bug 表现为「偶尔鉴权信息为 nil」，
// 极难复现。
type ctxKey int

const (
	ctxKeyPrincipal ctxKey = iota
	ctxKeyNamespace
)

// WithPrincipal 把鉴权结果放进 context。
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, ctxKeyPrincipal, p)
}

// PrincipalFrom 取出 context 中的调用者身份；没有则返回 nil。
func PrincipalFrom(ctx context.Context) *Principal {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(ctxKeyPrincipal).(*Principal)
	return p
}

// WithNamespaceSlug 把本次请求已解析出的命名空间 slug 放进 context，
// 供计费/录制层在写流水时使用。
func WithNamespaceSlug(ctx context.Context, slug string) context.Context {
	return context.WithValue(ctx, ctxKeyNamespace, slug)
}

// NamespaceSlugFrom 取出 context 中的命名空间 slug。
func NamespaceSlugFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	s, _ := ctx.Value(ctxKeyNamespace).(string)
	return s
}

// Middleware 用 Authenticator 保护一个 http.Handler。
//
// 之所以做成标准 middleware 而不是在 SSE handler 内部判断：
// 鉴权是横切关注点，未来 /messages/{ns}、/admin/v1/*、甚至健康检查之外的
// 任何端点都要各自决定「要不要鉴权、用哪套规则」，middleware 让这个决定
// 停留在路由装配处，而不是散落在每个 handler 里。
func Middleware(a *Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, err := ExtractToken(r)
			if err != nil {
				WriteAuthError(w, r, ErrMissingToken)
				return
			}

			p, err := a.Authenticate(r.Context(), token)
			if err != nil {
				WriteAuthError(w, r, err)
				return
			}

			// last_used_at 尽力而为，不阻塞本次请求
			a.MarkUsed(r.Context(), p.KeyID)

			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
		})
	}
}

// statusOf 把内部错误映射成 HTTP 状态码。
//
// 关键区分：401 表示「凭据本身不成立」（换一把 Key 就能解决），
// 403 表示「凭据成立但这个人不许做这件事」（换 Key 没用，要找管理员）。
// 客户端据此决定重试策略，所以这个区分不是学术性的。
func statusOf(err error) int {
	switch {
	case errors.Is(err, ErrMissingToken), errors.Is(err, ErrInvalidToken):
		return http.StatusUnauthorized
	case errors.Is(err, ErrKeyDisabled), errors.Is(err, ErrKeyExpired),
		errors.Is(err, ErrUserNotActive), errors.Is(err, ErrForbidden):
		return http.StatusForbidden
	default:
		// 数据库故障等：按服务端错误处理，绝不能伪装成 401，
		// 否则运维会以为「用户配错了 Key」而错过真实的数据库事故。
		return http.StatusInternalServerError
	}
}

// WriteAuthError 以 MCP/JSON 友好的形式输出鉴权失败。
//
// 响应体同时给出机器可读的 code 与人可读的 message，并把
// WWW-Authenticate 头带上，符合 RFC 6750 —— 客户端库据此知道该用哪种认证。
func WriteAuthError(w http.ResponseWriter, r *http.Request, err error) {
	status := statusOf(err)

	code := "unauthorized"
	msg := "认证失败"
	switch {
	case errors.Is(err, ErrMissingToken):
		code, msg = "missing_token", "请求未携带 API Key：请使用 Authorization: Bearer <key> 或 X-API-Key: <key>"
	case errors.Is(err, ErrInvalidToken):
		code, msg = "invalid_token", "API Key 无效"
	case errors.Is(err, ErrKeyDisabled):
		code, msg = "key_disabled", "API Key 已被禁用，请联系管理员"
	case errors.Is(err, ErrKeyExpired):
		code, msg = "key_expired", "API Key 已过期，请重新签发"
	case errors.Is(err, ErrUserNotActive):
		code, msg = "user_inactive", "账号状态不可用，请联系管理员"
	case errors.Is(err, ErrForbidden):
		code, msg = "forbidden", err.Error()
	default:
		code, msg = "auth_unavailable", "鉴权服务暂时不可用，请稍后重试"
		log.Printf("鉴权内部错误: %v", err)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="mcp-gateway"`)
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}

// -----------------------------------------------------------------------------
// Key 级限流
// -----------------------------------------------------------------------------

// RateLimiter 是 Key 级别的固定窗口计数器。
//
// 之所以放在这一层而不是复用 settings 里的全局开关：配额是「按 Key」的，
// 一个租户名下可能有几十把 Key，各自有独立上限；用全局计数器会在
// 多客户端场景下互相挤占，表现为「我明明没超量却被限流」。
//
// 固定窗口而非滑动窗口是刻意的：MCP 场景的限流目的是防止单客户端把
// 后端打满，不是精确计费（计费由 billing 包按 usage_events 结算）。
// 固定窗口的内存与 CPU 成本低一个数量级，代价是窗口边界允许双倍突刺
// —— 这个突刺量级远小于后端能承受的峰值，属于可接受的取舍。
type RateLimiter struct {
	limit  int
	window time.Duration

	mu      sync.Mutex
	entries map[string]*rateWindow
	lastGC  time.Time
	gcEvery time.Duration
	now     func() time.Time
}

type rateWindow struct {
	start time.Time
	count int64
}

// NewRateLimiter 构造限流器。limit <= 0 表示不限流。
func NewRateLimiter(limit int, per time.Duration) *RateLimiter {
	if per <= 0 {
		per = time.Minute
	}
	return &RateLimiter{
		limit:   limit,
		window:  per,
		entries: make(map[string]*rateWindow, 64),
		gcEvery: 10 * per,
		now:     time.Now,
	}
}

// Allow 用构造时设定的上限判断某个键本次是否放行。
// 第二个返回值是建议的 Retry-After。
func (l *RateLimiter) Allow(key string) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	return l.allowWith(key, l.limit)
}

// AllowN 用调用方指定的上限判断，而不是构造时的那一个。
//
// 存在这个方法的理由：上限是按 Key 存在数据库里的（api_keys.rate_limit），
// 而 RateLimiter 是进程级单例。若只让构造时那一个全局默认值生效，
// 管理员给某把 Key 单独调高或调低配额就完全不起作用 —— 那是静默失效，
// 线上排查起来比直接报错难受得多。所以「默认值」与「本次生效值」分开传。
func (l *RateLimiter) AllowN(key string, limit int) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	return l.allowWith(key, limit)
}

// allowWith 是真正的计数器实现。limit <= 0 表示不限流。
func (l *RateLimiter) allowWith(key string, limit int) (bool, time.Duration) {
	if limit <= 0 {
		return true, 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	w, ok := l.entries[key]
	if !ok || now.Sub(w.start) >= l.window {
		w = &rateWindow{start: now}
		l.entries[key] = w
	}

	w.count++
	l.gcLocked(now)

	if w.count > int64(limit) {
		remain := l.window - now.Sub(w.start)
		if remain < 0 {
			remain = 0
		}
		return false, remain
	}
	return true, 0
}

// gcLocked 周期性清理过期窗口，防止 map 随 Key 数量无限增长。
// 调用方必须已持有 l.mu。
func (l *RateLimiter) gcLocked(now time.Time) {
	if now.Sub(l.lastGC) < l.gcEvery {
		return
	}
	l.lastGC = now
	for k, w := range l.entries {
		if now.Sub(w.start) > 2*l.window {
			delete(l.entries, k)
		}
	}
}

// WriteRateLimited 输出 429，并带上 Retry-After。
func WriteRateLimited(w http.ResponseWriter, retryAfter time.Duration) {
	secs := int(retryAfter.Seconds())
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    "rate_limited",
			"message": "请求过于频繁，请稍后重试",
		},
	})
}

// LimitMiddleware 在读得出 Principal 的前提下做 Key 级限流。
//
// 必须排在 Authenticate 中间件之后：限流粒度是 Key，没有 Principal 就无法计数。
func LimitMiddleware(l *RateLimiter, defaultLimit int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := PrincipalFrom(r.Context())
			if p == nil {
				// 没有 Principal 说明装配顺序错了。这里选择放行而不是报错：
				// 鉴权缺失本身已被上一个中间件拦住，此处只是防御性分支，
				// 不该因为装配失误让所有请求 500。
				next.ServeHTTP(w, r)
				return
			}

			// Key 自己配了配额就用它，没配才回落到默认值。
			limit := defaultLimit
			if p.RateLimit != nil && *p.RateLimit > 0 {
				limit = *p.RateLimit
			}
			if limit <= 0 {
				next.ServeHTTP(w, r)
				return
			}

			// 注意用 AllowN 而不是 Allow：必须把算出来的 limit 传进去，
			// 否则 limiter 会一直用构造时的默认值，Key 级配额形同虚设。
			// limiter 内部按 KeyID 分桶，共用单例不会互相干扰。
			if ok, retry := l.AllowN(string(p.KeyID), limit); !ok {
				WriteRateLimited(w, retry)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// NormalizeSlug 把路径里的命名空间段做最小规范化。
//
// 只做去空白与大小写归一，不做任何"聪明"的别名猜测：
// 命名空间 slug 是管理员在后台设定、且会写进用户配置文件的稳定标识，
// 让网关去猜它可能是什么，只会在排查线上问题时制造谜题。
func NormalizeSlug(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}
