// Package admin 实现网关的超级管理员控制面（/admin/v1/*）。
//
// # 定位
//
// 网关是「MCP 服务端集群的前沿」：客户端拿着一把 API Key 连进来，由网关
// 统一鉴权，再按命名空间把请求转发给 r-nacos 发现到的后端实例。因此管理端
// 管的不是「某个本地进程」，而是三件互不重叠的事：
//
//  1. 身份与凭据：用户、租户、Key（含 Key 能访问哪些命名空间 / 工具）
//  2. 目录与容量：套餐额度、命名空间上下线
//  3. 审计与语料：用量汇总、会话录制记录
//
// # 为什么单独成包，而不是塞进 internal/auth
//
// auth 是每请求都在跑的守卫，必须极快、可离线单测；admin 是低频的写入面，
// 关心的是参数校验、幂等、缓存失效与审计语义。两者的失败方式完全不同
// （前者错 = 误放行/误拦截，后者错 = 数据被改坏），混在一个包里必然要么
// 让 auth 变慢，要么让 admin 变松。这里只依赖 auth 的公开能力。
//
// # 缓存失效是硬约束
//
// 鉴权结果缓存 30 秒、RBAC 裁决缓存 60 秒。管理员点「禁用这个 Key」之后
// 若只是改了库，用户还能拿它继续跑最多一分钟——这是不可接受的。所以本包
// 每一条会改变「可用性」的写操作，都必须同时调用 Authenticator.Invalidate
// 与 Authorizer.InvalidateKey。代码里统一收在 invalidateKey 一处，
// 不让各个 handler 各自记得。
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// MinAdminTokenLen 是管理 token 的最短长度。
//
// 与 config 里 auth.admin_token 的校验保持一致：管理 token 能改全库数据，
// 短口令在暴露到内网之后基本等于没有口令。两处都要拦，因为 admin 包
// 也可能被直接 New 出来（测试、未来嵌入式用法），不能只靠配置校验。
const MinAdminTokenLen = 16

// KeyStore 是管理端对 api_keys / key_permissions 的写入口。
//
// 刻意只声明「管理端真的会用」的方法：接口越窄，store 的演进越自由，
// 测试替身也越小。
type KeyStore interface {
	// CreateKeyWithQuotaCheck 在同一事务里校验配额并写入，避免并发超额。
	CreateKeyWithQuotaCheck(
		ctx context.Context, keyID, userID idgen.ID,
		name, prefix string, hash []byte,
		rateLimit *int, expiresAt *time.Time, limit int,
	) (*store.APIKey, error)

	GetKeyByID(ctx context.Context, id idgen.ID) (*store.APIKey, error)
	ListKeysByUser(ctx context.Context, userID idgen.ID) ([]store.APIKey, error)
	UpdateKey(ctx context.Context, id idgen.ID, name *string, rateLimit *int, expiresAt *time.Time, clearExpiry bool) (*store.APIKey, error)
	EnableKey(ctx context.Context, id idgen.ID) (*store.APIKey, error)
	DisableKeyWithReason(ctx context.Context, id idgen.ID, reason string) error

	ReplacePermissions(ctx context.Context, keyID idgen.ID, perms map[string][]string) error
	ListPermissions(ctx context.Context, keyID idgen.ID) ([]store.Permission, error)
}

// DirectoryStore 是目录类只读（与命名空间上下线）能力。
type DirectoryStore interface {
	GetUserByID(ctx context.Context, id idgen.ID) (*store.User, error)
	ListUsers(ctx context.Context, page, pageSize int) ([]store.User, int64, error)

	GetTenantByID(ctx context.Context, id idgen.ID) (*store.Tenant, error)
	ListTenants(ctx context.Context, page, pageSize int) ([]store.Tenant, int64, error)

	GetPlanByID(ctx context.Context, id idgen.ID) (*store.Plan, error)
	ListPlans(ctx context.Context) ([]store.Plan, error)

	ListNamespaces(ctx context.Context) ([]store.Namespace, error)
	GetNamespaceBySlug(ctx context.Context, slug string) (*store.Namespace, error)
	DisableNamespace(ctx context.Context, id idgen.ID, reason string) error
	EnableNamespace(ctx context.Context, id idgen.ID) error
}

// ReportStore 是用量与会话录制（训练语料）的查询面。
type ReportStore interface {
	SumUsage(ctx context.Context, userID idgen.ID, from, to time.Time) ([]store.UsageSummary, error)
	ListSessions(ctx context.Context, f store.SessionFilter, page, pageSize int) ([]store.Session, int64, error)
	ListSessionMessages(ctx context.Context, sessionID idgen.ID, page, pageSize int) ([]store.SessionMessage, int64, error)
	LabelSession(ctx context.Context, id idgen.ID, label, notes string) error
}

// Options 是管理端的可调参数。
type Options struct {
	// AdminToken 是管理接口唯一凭据，必填。
	AdminToken string

	// Now 可在测试里注入固定时钟，影响用量默认时间窗。
	Now func() time.Time

	// Logf 用于记录内部错误。默认丢弃：管理端不打印日志也不影响正确性，
	// 但线上排查 500 全靠它，所以 main.go 应当接上。
	Logf func(format string, args ...any)
}

// Server 是管理端 HTTP 处理器。
type Server struct {
	keys    KeyStore
	dir     DirectoryStore
	reports ReportStore

	// auth / rbac 允许为 nil：管理端只读部署（报表服务）不需要失效能力。
	// 写操作会因此失去即时失效，所以 main.go 必须传真实的实例。
	auth *auth.Authenticator
	rbac *auth.Authorizer

	token string
	now   func() time.Time
	logf  func(format string, args ...any)

	handler http.Handler
}

// New 构造管理端。
//
// 依赖必须是真实的 Store（或等价实现），nil 一律 panic 级别的编程错误，
// 这里返回 error 而不是 panic，是为了让 main.go 能在启动阶段体面退出。
func New(
	keys KeyStore,
	dir DirectoryStore,
	reports ReportStore,
	a *auth.Authenticator,
	az *auth.Authorizer,
	opts Options,
) (*Server, error) {
	if keys == nil || dir == nil || reports == nil {
		return nil, errors.New("admin: 数据源不能为空（keys / directory / reports）")
	}

	token := strings.TrimSpace(opts.AdminToken)
	if token == "" {
		return nil, errors.New("admin: 管理 token 不能为空；多租户模式必须在配置里设置 auth.admin_token")
	}
	if len(token) < MinAdminTokenLen {
		return nil, fmt.Errorf("admin: 管理 token 至少 %d 个字符，当前 %d 个", MinAdminTokenLen, len(token))
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	s := &Server{
		keys:    keys,
		dir:     dir,
		reports: reports,
		auth:    a,
		rbac:    az,
		token:   token,
		now:     now,
		logf:    logf,
	}
	s.handler = s.routes()
	return s, nil
}

// Handler 返回挂了管理 token 校验的处理器。
//
// 路由表用的是完整路径（/admin/v1/...），因此 main.go 既可以整体挂
// mux.Handle("/admin/v1/", s.Handler())，也可以直接把它当作 Server 使用。
func (s *Server) Handler() http.Handler { return s.handler }

// routes 注册全部管理端点。
//
// 用 Go 1.22+ 的方法+路径模式，让「GET 与 POST 走同一路径」不再需要手工
// 判断 r.Method；未知方法会自动落到 405，省掉一堆 if。
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /admin/v1/health", s.handleHealth)

	// 凭据
	mux.HandleFunc("POST /admin/v1/keys", s.handleCreateKey)
	mux.HandleFunc("GET /admin/v1/keys/{id}", s.handleGetKey)
	mux.HandleFunc("PATCH /admin/v1/keys/{id}", s.handleUpdateKey)
	mux.HandleFunc("POST /admin/v1/keys/{id}/enable", s.handleEnableKey)
	mux.HandleFunc("POST /admin/v1/keys/{id}/disable", s.handleDisableKey)
	mux.HandleFunc("GET /admin/v1/keys/{id}/permissions", s.handleGetPermissions)
	mux.HandleFunc("PUT /admin/v1/keys/{id}/permissions", s.handleReplacePermissions)

	// 目录
	mux.HandleFunc("GET /admin/v1/users", s.handleListUsers)
	mux.HandleFunc("GET /admin/v1/users/{id}", s.handleGetUser)
	mux.HandleFunc("GET /admin/v1/users/{id}/keys", s.handleListUserKeys)
	mux.HandleFunc("GET /admin/v1/users/{id}/usage", s.handleUserUsage)

	mux.HandleFunc("GET /admin/v1/tenants", s.handleListTenants)
	mux.HandleFunc("GET /admin/v1/tenants/{id}", s.handleGetTenant)

	mux.HandleFunc("GET /admin/v1/plans", s.handleListPlans)

	mux.HandleFunc("GET /admin/v1/namespaces", s.handleListNamespaces)
	mux.HandleFunc("POST /admin/v1/namespaces/{slug}/enable", s.handleEnableNamespace)
	mux.HandleFunc("POST /admin/v1/namespaces/{slug}/disable", s.handleDisableNamespace)

	// 审计与语料
	mux.HandleFunc("GET /admin/v1/sessions", s.handleListSessions)
	mux.HandleFunc("GET /admin/v1/sessions/{id}/messages", s.handleListSessionMessages)
	mux.HandleFunc("POST /admin/v1/sessions/{id}/label", s.handleLabelSession)

	return s.requireAdminToken(mux)
}

// requireAdminToken 是管理端唯一的门禁。
//
// 与业务 API Key 的鉴权完全分开：
//   - 不查库、不缓存（管理 token 只有一个，查库毫无意义）；
//   - 用 auth.SecureCompare 做定长比较，避免 == 的短路时序泄漏；
//   - 失败一律 401 + WWW-Authenticate，不给「token 存在但权限不足」的信息。
func (s *Server) requireAdminToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := auth.ExtractBearer(r)
		if tok == "" {
			// 兼容命令行/脚本：curl -H 'X-Admin-Token: ...'
			tok = strings.TrimSpace(r.Header.Get("X-Admin-Token"))
		}

		if tok == "" || !auth.SecureCompare(tok, s.token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="admin"`)
			writeError(w, http.StatusUnauthorized, "unauthorized", "管理 token 缺失或不正确")
			return
		}

		// 管理端响应里含 Key 前缀、邮箱、会话原文，任何中间层都不该缓存。
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

// invalidateKey 让「对这个 Key 的改动」立刻生效，而不是等 TTL 到期。
//
// 两层缓存都要清：
//   - auth 缓存按 token 哈希存 Principal，只能按 KeyID 值扫描删除
//     （Authenticator.Invalidate 内部就是这么做的）；
//   - RBAC 缓存按 keyID|nsID[|tool] 存，可以按前缀精确删。
//
// 少清任何一层，都会出现「Key 已禁用但仍能连」「权限已收回但仍能调工具」。
func (s *Server) invalidateKey(keyID idgen.ID) {
	if s.auth != nil {
		s.auth.Invalidate(keyID)
	}
	if s.rbac != nil {
		s.rbac.InvalidateKey(keyID)
	}
}

// -----------------------------------------------------------------------------
// 响应体
// -----------------------------------------------------------------------------

// errorDetail 与 internal/auth 的错误体保持一致，前端只需要一套解析逻辑。
type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// Details 用于配额超限这类需要携带数值的场景，其余情况省略。
	Details any `json:"details,omitempty"`
}

type errorPayload struct {
	Error errorDetail `json:"error"`
}

// writeJSON 输出 JSON 响应。
//
// 先编码到内存再写头：编码失败时还能改成 500，而不是已经写了 200
// 才发现 body 只写了一半。
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"encode_failed","message":"响应序列化失败"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorPayload{Error: errorDetail{Code: code, Message: msg}})
}

func writeErrorDetails(w http.ResponseWriter, status int, code, msg string, details any) {
	writeJSON(w, status, errorPayload{Error: errorDetail{Code: code, Message: msg, Details: details}})
}

// fail 把 store 层的错误翻译成 HTTP 语义。
//
// 映射规则（与 auth 包一致：不要把所有失败都伪装成 401/404）：
//   - ErrNotFound          → 404，调用方给的 ID 不存在
//   - ErrUserNotActive     → 409，用户被停用/注销，属于状态冲突而不是参数错
//   - KeyQuotaExceededError→ 409，带上 limit / current 便于前端提示
//   - 其它                 → 500，并把原始错误写进日志
//
// 500 的响应体只说「服务端处理失败」：管理端虽然只对管理员开放，
// 但把 SQL 错误原样吐回客户端，等于把表结构写进运维工单。
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var quotaErr *store.KeyQuotaExceededError

	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "记录不存在")

	case errors.Is(err, store.ErrUserNotActive):
		writeError(w, http.StatusConflict, "user_not_active", "该用户当前不可用，无法签发 API Key")

	case errors.As(err, &quotaErr):
		writeErrorDetails(w, http.StatusConflict, "key_quota_exceeded", quotaErr.Error(),
			map[string]any{"limit": quotaErr.Limit, "current": quotaErr.Current})

	default:
		s.logf("admin: %s %s 失败: %v", r.Method, r.URL.Path, err)
		writeError(w, http.StatusInternalServerError, "internal_error", "服务端处理失败，请查看网关日志")
	}
}

// decodeJSON 解析请求体，失败时已经写好 400，调用方直接 return。
//
// DisallowUnknownFields 是刻意的：管理端少一个字段名写错（rate_limit vs
// rateLimit），静默忽略会让管理员以为改成功、实际没改。宁可直接报错。
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	// 管理端请求体最大 1 MiB：权限表再长也不该超过这个量级。
	body := http.MaxBytesReader(w, r.Body, 1<<20)

	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "请求体不是合法 JSON: "+err.Error())
		return false
	}
	return true
}

// -----------------------------------------------------------------------------
// 参数与分页
// -----------------------------------------------------------------------------

// parseID 把路径/请求体里的字符串解析成指定类型的 ID。
//
// 一定要 ValidateOf 而不是只看非空：一个 usr_ 开头的 ID 传进租户接口，
// 后面按租户分片的推导会静默跑偏到默认池，那种 bug 在生产上极难复现。
func parseID(kind idgen.Kind, raw, field string) (idgen.ID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%s 不能为空", field)
	}
	id, err := idgen.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("%s 不是合法 ID: %w", field, err)
	}
	if err := idgen.ValidateOf(kind, string(id)); err != nil {
		return "", fmt.Errorf("%s 类型不符: %w", field, err)
	}
	return id, nil
}

// pageParams 读取 page / page_size，非法值交给 store 归一化。
func pageParams(r *http.Request) (int, int) {
	q := r.URL.Query()
	return atoiDefault(q.Get("page"), 1), atoiDefault(q.Get("page_size"), store.DefaultPageSize)
}

func atoiDefault(raw string, def int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

// clampPage 与 store.normalizePage 的收敛规则保持一致。
//
// 之所以在 admin 里复制一份：有些数据源（ListNamespaces）本身不分页，
// 需要在内存里切片，切片范围必须和返回给前端的 pagination 元信息同源，
// 否则会出现「说 20 条却返回 40 条」这种翻页错乱。
func clampPage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = store.DefaultPageSize
	}
	if pageSize > store.MaxPageSize {
		pageSize = store.MaxPageSize
	}
	return page, pageSize
}

// slicePage 对已在内存里的切片做分页。
func slicePage[T any](items []T, page, pageSize int) []T {
	page, pageSize = clampPage(page, pageSize)

	start := (page - 1) * pageSize
	if start >= len(items) {
		return []T{}
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	return items[start:end]
}

// parseTime 解析 RFC3339 时间参数；空串表示「不设边界」。
func parseTime(raw, field string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s 需要 RFC3339 格式（如 2026-01-02T15:04:05Z）: %w", field, err)
	}
	return t, nil
}
