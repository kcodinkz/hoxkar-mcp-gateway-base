package admin

import (
	"net/http"
	"sort"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// handleHealth 是最轻量的存活探针，可以用来验证管理 token 是否正确。
//
// 刻意不查库：健康检查一旦依赖数据库，数据库抖动时 k8s 会把网关一起重启，
// 而网关其实还能正常转发已经建好连接的请求。真正的就绪探测另有入口。
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"time":   s.now().UTC().Format(time.RFC3339),
	})
}

// -----------------------------------------------------------------------------
// 用户
// -----------------------------------------------------------------------------

// userDTO 是用户的对外表示。
//
// 不含任何凭据字段：users 表本来也没有密码列（Key 才是凭据），
// 但 quota_overrides 原样透出——管理员需要看清「这个人为什么是这个额度」。
type userDTO struct {
	ID             string         `json:"id"`
	Email          string         `json:"email"`
	DisplayName    string         `json:"display_name"`
	TenantID       string         `json:"tenant_id,omitempty"`
	PlanID         string         `json:"plan_id"`
	Status         string         `json:"status"`
	QuotaOverrides map[string]any `json:"quota_overrides,omitempty"`
	ExpiresAt      *time.Time     `json:"expires_at,omitempty"`
	Expired        bool           `json:"expired"`
	CreatedAt      time.Time      `json:"created_at"`
}

func toUserDTO(u *store.User, now time.Time) userDTO {
	dto := userDTO{
		ID:             string(u.ID),
		Email:          u.Email,
		DisplayName:    u.DisplayName,
		PlanID:         string(u.PlanID),
		Status:         string(u.Status),
		QuotaOverrides: u.QuotaOverrides,
		ExpiresAt:      u.ExpiresAt,
		CreatedAt:      u.CreatedAt,
	}
	if !u.TenantID.IsZero() {
		dto.TenantID = string(u.TenantID)
	}
	if u.ExpiresAt != nil && !u.ExpiresAt.After(now) {
		dto.Expired = true
	}
	return dto
}

func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
	page, pageSize := pageParams(r)

	users, total, err := s.dir.ListUsers(r.Context(), page, pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	now := s.now()
	items := make([]userDTO, 0, len(users))
	for i := range users {
		items = append(items, toUserDTO(&users[i], now))
	}

	writeJSON(w, http.StatusOK, store.NewPageResult(items, page, pageSize, total))
}

func (s *Server) handleGetUser(w http.ResponseWriter, r *http.Request) {
	userID, err := parseID(idgen.KindUser, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_id", err.Error())
		return
	}

	u, err := s.dir.GetUserByID(r.Context(), userID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toUserDTO(u, s.now()))
}

// handleListUserKeys 列出某用户名下全部 Key。
//
// 不做分页：ListKeysByUser 不分页，因为单用户 Key 数受套餐 max_api_keys
// 约束（量级在十位以内）。但仍按分页契约的形状返回，前端只写一套表格组件。
func (s *Server) handleListUserKeys(w http.ResponseWriter, r *http.Request) {
	userID, err := parseID(idgen.KindUser, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_id", err.Error())
		return
	}

	// 先确认用户存在，否则「不存在的用户」会返回空列表，看起来像「这个人还没有 Key」。
	if _, err := s.dir.GetUserByID(r.Context(), userID); err != nil {
		s.fail(w, r, err)
		return
	}

	keys, err := s.keys.ListKeysByUser(r.Context(), userID)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	now := s.now()
	items := make([]keyDTO, 0, len(keys))
	for i := range keys {
		items = append(items, toKeyDTO(&keys[i], now))
	}

	// 已在内存里，直接按分页契约的形状返回，total 就是实际条数。
	page, pageSize := pageParams(r)
	writeJSON(w, http.StatusOK,
		store.NewPageResult(slicePage(items, page, pageSize), page, pageSize, int64(len(items))))
}

// -----------------------------------------------------------------------------
// 租户
// -----------------------------------------------------------------------------

// tenantDTO 是租户的对外表示。
//
// Shard 会透出：它是排障时第一个要看的东西（「这个租户的流水落在哪个库」），
// 而且它不是秘密——分片号只是路由标签，不构成凭据。
type tenantDTO struct {
	ID        string     `json:"id"`
	Slug      string     `json:"slug"`
	Name      string     `json:"name"`
	Kind      string     `json:"kind"`
	Status    string     `json:"status"`
	PlanID    string     `json:"plan_id,omitempty"`
	Shard     string     `json:"shard"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Expired   bool       `json:"expired"`
	Usable    bool       `json:"usable"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

func toTenantDTO(t *store.Tenant, now time.Time) tenantDTO {
	dto := tenantDTO{
		ID:        string(t.ID),
		Slug:      t.Slug,
		Name:      t.Name,
		Kind:      string(t.Kind),
		Status:    string(t.Status),
		Shard:     t.Shard,
		ExpiresAt: t.ExpiresAt,
		Usable:    t.IsUsable(),
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
	}
	if !t.PlanID.IsZero() {
		dto.PlanID = string(t.PlanID)
	}
	if t.ExpiresAt != nil && !t.ExpiresAt.After(now) {
		dto.Expired = true
	}
	return dto
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	page, pageSize := pageParams(r)

	tenants, total, err := s.dir.ListTenants(r.Context(), page, pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	now := s.now()
	items := make([]tenantDTO, 0, len(tenants))
	for i := range tenants {
		items = append(items, toTenantDTO(&tenants[i], now))
	}

	writeJSON(w, http.StatusOK, store.NewPageResult(items, page, pageSize, total))
}

func (s *Server) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	tenantID, err := parseID(idgen.KindTenant, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_tenant_id", err.Error())
		return
	}

	t, err := s.dir.GetTenantByID(r.Context(), tenantID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTenantDTO(t, s.now()))
}

// -----------------------------------------------------------------------------
// 套餐
// -----------------------------------------------------------------------------

type planDTO struct {
	ID                 string `json:"id"`
	Code               string `json:"code"`
	Name               string `json:"name"`
	Description        string `json:"description,omitempty"`
	MaxAPIKeys         int    `json:"max_api_keys"`
	RateLimitPerMinute int    `json:"rate_limit_per_minute"`
	MaxUploadBytes     int64  `json:"max_upload_bytes"`
	MaxConcurrentJobs  int    `json:"max_concurrent_jobs"`
	MaxNamespaces      int    `json:"max_namespaces"`
	MaxPoolSize        int    `json:"max_pool_size"`
	Active             bool   `json:"active"`
}

func toPlanDTO(p *store.Plan) planDTO {
	return planDTO{
		ID:                 string(p.ID),
		Code:               p.Code,
		Name:               p.Name,
		Description:        p.Description,
		MaxAPIKeys:         p.MaxAPIKeys,
		RateLimitPerMinute: p.RateLimitPerMinute,
		MaxUploadBytes:     p.MaxUploadBytes,
		MaxConcurrentJobs:  p.MaxConcurrentJobs,
		MaxNamespaces:      p.MaxNamespaces,
		MaxPoolSize:        p.MaxPoolSize,
		Active:             p.Active,
	}
}

// handleListPlans 列出启用套餐。
//
// 套餐数量是个位数量级（free / pro / enterprise），分页没有意义。
// 但仍返回分页契约的形状，前端不必为这一个接口写特例。
func (s *Server) handleListPlans(w http.ResponseWriter, r *http.Request) {
	plans, err := s.dir.ListPlans(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}

	items := make([]planDTO, 0, len(plans))
	for i := range plans {
		items = append(items, toPlanDTO(&plans[i]))
	}

	page, pageSize := pageParams(r)
	writeJSON(w, http.StatusOK,
		store.NewPageResult(slicePage(items, page, pageSize), page, pageSize, int64(len(items))))
}

// -----------------------------------------------------------------------------
// 命名空间
// -----------------------------------------------------------------------------

// namespaceDTO 是命名空间的对外表示。
//
// Env 会脱敏：namespaces.env 里可能有后端服务的连接串与令牌（例如
// 某个 MCP 后端需要的第三方 API Key）。管理端需要知道「有没有配」，
// 但不需要把值回显出来——回显等于给任何能进管理端的人发一份后端凭据。
type namespaceDTO struct {
	ID          string  `json:"id"`
	Slug        string  `json:"slug"`
	Description string  `json:"description,omitempty"`
	Transport   string  `json:"transport"`
	Enabled     bool    `json:"enabled"`
	DisabledBy  *string `json:"disabled_by,omitempty"`
	PoolSize    int     `json:"pool_size"`
	TimeoutMS   int     `json:"timeout_ms"`

	// EnvKeys 只给键名，不给值。
	EnvKeys []string `json:"env_keys,omitempty"`

	DiscoveryProvider string     `json:"discovery_provider,omitempty"`
	DiscoveryService  string     `json:"discovery_service,omitempty"`
	DiscoveryGroup    string     `json:"discovery_group,omitempty"`
	EndpointPath      string     `json:"endpoint_path,omitempty"`
	LBStrategy        string     `json:"lb_strategy,omitempty"`
	Tools             []string   `json:"tools,omitempty"`
	Source            string     `json:"source"`
	LastSeenAt        *time.Time `json:"last_seen_at,omitempty"`
}

func toNamespaceDTO(ns *store.Namespace) namespaceDTO {
	dto := namespaceDTO{
		ID:                string(ns.ID),
		Slug:              ns.Slug,
		Description:       ns.Description,
		Transport:         ns.Transport,
		Enabled:           ns.Enabled,
		DisabledBy:        ns.DisabledBy,
		PoolSize:          ns.PoolSize,
		TimeoutMS:         ns.TimeoutMS,
		DiscoveryProvider: ns.DiscoveryProvider,
		DiscoveryService:  ns.DiscoveryService,
		DiscoveryGroup:    ns.DiscoveryGroup,
		EndpointPath:      ns.EndpointPath,
		LBStrategy:        ns.LBStrategy,
		Tools:             ns.Tools,
		Source:            ns.Source,
		LastSeenAt:        ns.LastSeenAt,
	}
	if len(ns.Env) > 0 {
		keys := make([]string, 0, len(ns.Env))
		for k := range ns.Env {
			keys = append(keys, k)
		}
		// map 遍历顺序随机，排一下让响应可比对、可写进快照测试。
		sort.Strings(keys)
		dto.EnvKeys = keys
	}
	return dto
}

// handleListNamespaces 列出全部命名空间。
//
// 数据源不分页（全表就是服务目录，量级在百位），这里在内存里切页，
// 保证 pagination 元信息与实际返回条数一致。
func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	list, err := s.dir.ListNamespaces(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}

	items := make([]namespaceDTO, 0, len(list))
	for i := range list {
		items = append(items, toNamespaceDTO(&list[i]))
	}

	page, pageSize := pageParams(r)
	writeJSON(w, http.StatusOK,
		store.NewPageResult(slicePage(items, page, pageSize), page, pageSize, int64(len(items))))
}

// handleDisableNamespace 管理员手工下线一个命名空间。
//
// 必须显式指定 slug 而不是 ID：下线是运维动作，运维看的是「blender 挂了」，
// 从日志里也只会看到 slug。用 slug 换来的是少一次「ID 对不上」的误操作。
func (s *Server) handleDisableNamespace(w http.ResponseWriter, r *http.Request) {
	ns, err := s.resolveNamespace(r)
	if err != nil {
		writeError(w, http.StatusNotFound, "namespace_not_found", err.Error())
		return
	}

	// 只能记 admin：registry_gone 由同步器专用，手填会让同步器下次
	// 把「人工下线」误判成「注册中心里没了」而重复处理。
	if err := s.dir.DisableNamespace(r.Context(), ns.ID, store.DisabledByAdmin); err != nil {
		s.fail(w, r, err)
		return
	}

	// 命名空间级别的可用性在两个地方被缓存：RBAC 的 nsCache 里存的是
	// 「这个 Key 能不能用这个 ns」，与 ns 行是否 enabled 无关（enabled 在
	// ResolveNamespace 里查库）。但工具级缓存仍可能带着旧结论，且
	// Authenticator 不缓存 ns 状态，所以这里最稳妥的做法是整体清空 RBAC。
	s.resetRBAC()

	writeJSON(w, http.StatusOK, map[string]any{
		"slug":    ns.Slug,
		"id":      string(ns.ID),
		"enabled": false,
		"reason":  store.DisabledByAdmin,
	})
}

// handleEnableNamespace 重新上线。
func (s *Server) handleEnableNamespace(w http.ResponseWriter, r *http.Request) {
	ns, err := s.resolveNamespace(r)
	if err != nil {
		writeError(w, http.StatusNotFound, "namespace_not_found", err.Error())
		return
	}

	if err := s.dir.EnableNamespace(r.Context(), ns.ID); err != nil {
		s.fail(w, r, err)
		return
	}
	s.resetRBAC()

	writeJSON(w, http.StatusOK, map[string]any{
		"slug":    ns.Slug,
		"id":      string(ns.ID),
		"enabled": true,
	})
}

// resolveNamespace 按 slug 取命名空间，是命名空间端点的唯一入口。
func (s *Server) resolveNamespace(r *http.Request) (*store.Namespace, error) {
	slug := auth.NormalizeSlug(r.PathValue("slug"))
	if slug == "" {
		return nil, errNamespaceSlugEmpty
	}
	return s.dir.GetNamespaceBySlug(r.Context(), slug)
}

var errNamespaceSlugEmpty = namespaceError("命名空间 slug 不能为空")

type namespaceError string

func (e namespaceError) Error() string { return string(e) }

// resetRBAC 清空权限裁决缓存。
//
// 命名空间上下线影响的是「所有 Key 对它的可用性」，按单 Key 前缀清不干净，
// 只能整体 Reset。这个操作代价极低（缓存是可重建的），而漏清的代价是
// 下线一个出事服务后，部分客户端还能继续打进去。
func (s *Server) resetRBAC() {
	if s.rbac != nil {
		s.rbac.Reset()
	}
}
