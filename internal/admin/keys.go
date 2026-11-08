package admin

import (
	"net/http"
	"strings"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/quota"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// keyDTO 是 API Key 的对外表示。
//
// 这里**没有** KeyHash 字段，而且将来也不允许加。key_hash 是「能直接换算出
// 有效凭据的等价物」——拿到它就能自己写一个 FindKeyByHash 的假 Store 或
// 直接比对，等于把全库凭据泄露给任何能读管理端响应的人（日志、代理、
// 浏览器开发者工具）。哈希只允许存在于 store 与 auth 内部。
//
// 明文 token 只在创建响应里出现一次（见 createKeyResponse），列表与详情
// 一律只给 KeyPrefix 用于辨认。
type keyDTO struct {
	ID         string     `json:"id"`
	UserID     string     `json:"user_id"`
	Name       string     `json:"name"`
	KeyPrefix  string     `json:"key_prefix"`
	Enabled    bool       `json:"enabled"`
	DisabledBy *string    `json:"disabled_by,omitempty"`
	RateLimit  *int       `json:"rate_limit,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	Expired    bool       `json:"expired"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

func toKeyDTO(k *store.APIKey, now time.Time) keyDTO {
	return keyDTO{
		ID:         string(k.ID),
		UserID:     string(k.UserID),
		Name:       k.Name,
		KeyPrefix:  k.KeyPrefix,
		Enabled:    k.Enabled,
		DisabledBy: k.DisabledBy,
		RateLimit:  k.RateLimit,
		ExpiresAt:  k.ExpiresAt,
		// Expired 是「按当前时间算已过期」，与 enabled 分开表达：
		// enabled=false 是被人关掉的，expired=true 是自己到期的，
		// 前者要管理员去查为什么被关，后者只需要续期，前端提示完全不同。
		Expired:    k.IsExpired(now),
		CreatedAt:  k.CreatedAt,
		LastUsedAt: k.LastUsedAt,
	}
}

// createKeyRequest 是签发 Key 的请求体。
type createKeyRequest struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`

	// RateLimit 可选：不传表示按用户套餐的 rate_limit_per_minute 走。
	// 传了则覆盖套餐值（见 quota.EffectiveRateLimit）。
	RateLimit *int `json:"rate_limit"`

	// ExpiresAt 可选，RFC3339。不传表示永不过期（由管理员手动禁用）。
	ExpiresAt *string `json:"expires_at"`

	// Permissions 可选：{命名空间 slug: [工具名...]}，空数组表示该命名空间
	// 下全部工具。签发时一并授权，避免「建了 Key 但忘了授权限」的半成品状态。
	Permissions map[string][]string `json:"permissions"`
}

// createKeyResponse 是唯一会携带明文 token 的响应。
type createKeyResponse struct {
	Key keyDTO `json:"key"`

	// Token 只在本次响应里出现，服务端只存 sha256。丢失只能重新签发。
	Token string `json:"token"`

	// Warning 是给前端/运维的明确提示，避免有人以为之后还能再查回来。
	Warning string `json:"warning"`
}

// handleCreateKey 签发一把新 Key。
//
// 顺序刻意如此：先读用户 → 解析额度 → 生成 token → 事务内写库（含配额校验）
// → 写权限 → 失效缓存。任何一步失败都不会留下有效凭据，因为 token 没有
// 泄露到用户手里（响应未发出），而库里未提交的行会被回滚。
func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	var req createKeyRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	userID, err := parseID(idgen.KindUser, req.UserID, "user_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_id", err.Error())
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid_name", "name 不能为空")
		return
	}
	if len(name) > 120 {
		writeError(w, http.StatusBadRequest, "invalid_name", "name 最长 120 个字符")
		return
	}

	var expiresAt *time.Time
	if req.ExpiresAt != nil {
		t, err := parseTime(*req.ExpiresAt, "expires_at")
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_expires_at", err.Error())
			return
		}
		if !t.IsZero() {
			if !t.After(s.now()) {
				// 建出来就是过期的 Key 只会让用户困惑地反复尝试，
				// 在入口拦掉比让他拿着 401 来问更省事。
				writeError(w, http.StatusBadRequest, "invalid_expires_at", "expires_at 必须是未来时间")
				return
			}
			expiresAt = &t
		}
	}

	if req.RateLimit != nil && *req.RateLimit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_rate_limit", "rate_limit 不能为负数")
		return
	}

	// 先确认用户存在且可用。虽然 CreateKeyWithQuotaCheck 事务里会再校验一次
	// （那是防并发的权威判断），但在这里先读一次能把「用户不存在」「用户被停用」
	// 变成清晰的 404/409，而不是让事务里抛出的错误难以区分。
	user, err := s.dir.GetUserByID(r.Context(), userID)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	limit, err := s.maxAPIKeys(r, user)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	plaintext, prefix, hash, err := auth.Generate()
	if err != nil {
		s.fail(w, r, err)
		return
	}

	keyID, err := idgen.NewRandom(idgen.KindAPIKey)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	key, err := s.keys.CreateKeyWithQuotaCheck(
		r.Context(), keyID, userID, name, prefix, hash, req.RateLimit, expiresAt, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	// 权限与 Key 不在同一事务里（ReplacePermissions 自带事务，且需要
	// namespaces 表反查 ID）。失败时删掉刚建的 Key 不划算——更好的做法是
	// 如实返回错误并让管理员重试授权；这把 Key 因为没有任何 key_permissions
	// 行而实际无法访问任何命名空间，等于「安全的半成品」，不会越权。
	if len(req.Permissions) > 0 {
		if err := s.keys.ReplacePermissions(r.Context(), keyID, req.Permissions); err != nil {
			s.logf("admin: Key %s 已创建但授权失败，需重试授权: %v", keyID, err)
			s.invalidateKey(keyID)
			writeErrorDetails(w, http.StatusConflict, "permissions_failed",
				"Key 已创建但授权失败，请对该 Key 重新提交权限："+err.Error(),
				map[string]any{"key_id": string(keyID), "token": plaintext})
			return
		}
	}

	s.invalidateKey(keyID)

	writeJSON(w, http.StatusCreated, createKeyResponse{
		Key:     toKeyDTO(key, s.now()),
		Token:   plaintext,
		Warning: "该 token 只显示这一次，服务端仅保存其哈希；丢失后只能重新签发。",
	})
}

// maxAPIKeys 解析该用户实际可持有的 Key 上限。
//
// 走的是与运行时完全相同的额度解析链（quota.ResolveScope → quota.Resolve），
// 而不是直接读 users.plan_id。原因：企业租户的成员额度由租户套餐决定，
// 若管理端只看用户自己的套餐，就会出现「页面显示还能建 2 把、实际建到
// 第 3 把被事务拒掉」——两个数字必须同源。
func (s *Server) maxAPIKeys(r *http.Request, user *store.User) (int, error) {
	ctx := r.Context()
	now := s.now()

	var userPlan *store.Plan
	if !user.PlanID.IsZero() {
		p, err := s.dir.GetPlanByID(ctx, user.PlanID)
		if err != nil {
			return 0, err
		}
		userPlan = p
	}

	var (
		tenant     *store.Tenant
		tenantPlan *store.Plan
	)
	if !user.TenantID.IsZero() {
		t, err := s.dir.GetTenantByID(ctx, user.TenantID)
		if err != nil {
			return 0, err
		}
		tenant = t

		if !t.PlanID.IsZero() {
			p, err := s.dir.GetPlanByID(ctx, t.PlanID)
			if err != nil {
				return 0, err
			}
			tenantPlan = p
		}
	}

	scope := quota.ResolveScope(user, tenant, tenantPlan, userPlan, now)
	limits := quota.Resolve(scope.Plan, user.QuotaOverrides)
	return limits.MaxAPIKeys, nil
}

// handleGetKey 查单个 Key。
func (s *Server) handleGetKey(w http.ResponseWriter, r *http.Request) {
	keyID, err := parseID(idgen.KindAPIKey, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_key_id", err.Error())
		return
	}

	key, err := s.keys.GetKeyByID(r.Context(), keyID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toKeyDTO(key, s.now()))
}

// updateKeyRequest 是局部更新请求体。字段用指针区分「不改」与「改成零值」。
type updateKeyRequest struct {
	Name      *string `json:"name"`
	RateLimit *int    `json:"rate_limit"`
	ExpiresAt *string `json:"expires_at"`

	// ClearExpiry 显式要求把到期时间置空（改为永不过期）。
	// 不能靠 expires_at 传 null 表达：JSON 里「省略」与「null」在 Go 的
	// *string 上无法区分，所以给一个独立布尔位，语义无歧义。
	ClearExpiry bool `json:"clear_expiry"`
}

// handleUpdateKey 更新 Key 元数据。
//
// 刻意不支持在这里改 enabled / 用户归属 / 权限：
//   - enabled 必须走 enable / disable 端点，因为 db 要求 enabled 与
//     disabled_by 成对写，留后门等于把 constraint violation 留到运行期；
//   - 换 user_id 等于把 Key 转给另一个人，历史用量归属会错乱，应当作废重建；
//   - 权限是覆盖式语义（ReplacePermissions），单独一个 PUT 更安全。
func (s *Server) handleUpdateKey(w http.ResponseWriter, r *http.Request) {
	keyID, err := parseID(idgen.KindAPIKey, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_key_id", err.Error())
		return
	}

	var req updateKeyRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		if trimmed == "" {
			writeError(w, http.StatusBadRequest, "invalid_name", "name 不能改成空字符串")
			return
		}
		if len(trimmed) > 120 {
			writeError(w, http.StatusBadRequest, "invalid_name", "name 最长 120 个字符")
			return
		}
		req.Name = &trimmed
	}

	if req.RateLimit != nil && *req.RateLimit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_rate_limit", "rate_limit 不能为负数")
		return
	}

	var expiresAt *time.Time
	if req.ExpiresAt != nil {
		t, err := parseTime(*req.ExpiresAt, "expires_at")
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_expires_at", err.Error())
			return
		}
		if !t.IsZero() {
			if !t.After(s.now()) {
				writeError(w, http.StatusBadRequest, "invalid_expires_at", "expires_at 必须是未来时间")
				return
			}
			expiresAt = &t
		}
	}

	if req.ClearExpiry && expiresAt != nil {
		writeError(w, http.StatusBadRequest, "conflicting_expiry",
			"clear_expiry 与 expires_at 不能同时给出")
		return
	}

	key, err := s.keys.UpdateKey(r.Context(), keyID, req.Name, req.RateLimit, expiresAt, req.ClearExpiry)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	// 改限流值同样必须失效：auth 缓存里存着 Principal.RateLimit，
	// 那正是限流中间件用的值。不清缓存的话，新额度要等 30 秒才生效。
	s.invalidateKey(keyID)

	writeJSON(w, http.StatusOK, toKeyDTO(key, s.now()))
}

// handleDisableKey 软禁用。
func (s *Server) handleDisableKey(w http.ResponseWriter, r *http.Request) {
	keyID, err := parseID(idgen.KindAPIKey, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_key_id", err.Error())
		return
	}

	// 管理端禁用一律记 admin。plan_expiry / tenant_closed 是系统联动的
	// 原因，让管理员手工填会造成「自动续费恢复时把人工封禁一起解开」。
	if err := s.keys.DisableKeyWithReason(r.Context(), keyID, store.DisabledByAdmin); err != nil {
		s.fail(w, r, err)
		return
	}
	s.invalidateKey(keyID)

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      string(keyID),
		"enabled": false,
		"reason":  store.DisabledByAdmin,
	})
}

// handleEnableKey 重新启用并清空禁用原因。
func (s *Server) handleEnableKey(w http.ResponseWriter, r *http.Request) {
	keyID, err := parseID(idgen.KindAPIKey, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_key_id", err.Error())
		return
	}

	key, err := s.keys.EnableKey(r.Context(), keyID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.invalidateKey(keyID)

	writeJSON(w, http.StatusOK, toKeyDTO(key, s.now()))
}

// permissionDTO 把 key_permissions 的行聚合成前端好用的形状。
//
// 数据库里一个工具一行，直接吐出去前端还得自己 group by。管理端的
// 使用者关心的是「这把 Key 能进哪些命名空间、每个里面能用什么」，
// 所以这里按命名空间聚合；"*" 统一归一成空数组表示「全部工具」。
type permissionDTO struct {
	KeyID string `json:"key_id"`
	// Namespaces 是 {slug: [工具...]}。工具为空数组表示该命名空间下全部工具。
	Namespaces map[string][]string `json:"namespaces"`
}

func (s *Server) handleGetPermissions(w http.ResponseWriter, r *http.Request) {
	keyID, err := parseID(idgen.KindAPIKey, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_key_id", err.Error())
		return
	}

	perms, err := s.keys.ListPermissions(r.Context(), keyID)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	grouped := make(map[string][]string)
	for _, p := range perms {
		if _, ok := grouped[p.NamespaceSlug]; !ok {
			grouped[p.NamespaceSlug] = []string{}
		}
		// "*" 不落进数组：空数组本身就是「全部工具」的表达，
		// 混进一个 "*" 会让前端不得不同时处理两种「全部」。
		if p.ToolName == "*" {
			continue
		}
		grouped[p.NamespaceSlug] = append(grouped[p.NamespaceSlug], p.ToolName)
	}

	writeJSON(w, http.StatusOK, permissionDTO{KeyID: string(keyID), Namespaces: grouped})
}

// replacePermissionsRequest 是覆盖式授权请求体。
type replacePermissionsRequest struct {
	// Namespaces 是 {命名空间 slug: [工具名...]}。
	//
	// 传空对象 {} 表示收回全部权限。这是刻意的：授权变更必须是
	// 「把当前状态写成目标状态」，而不是增量打补丁——增量的并发语义
	// 很难说清，而且无法回答「到底还剩下什么」。
	Namespaces map[string][]string `json:"namespaces"`
}

func (s *Server) handleReplacePermissions(w http.ResponseWriter, r *http.Request) {
	keyID, err := parseID(idgen.KindAPIKey, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_key_id", err.Error())
		return
	}

	var req replacePermissionsRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Namespaces == nil {
		// namespaces 缺失与 {} 语义不同：前者多半是客户端漏字段，
		// 静默当成「收回全部权限」会造成线上事故。
		writeError(w, http.StatusBadRequest, "invalid_namespaces",
			"namespaces 字段必填；收回全部权限请显式传 {}")
		return
	}

	// 归一化 slug：库里存的是规范化后的小写，前端传 "Blender " 也应当匹配。
	normalized := make(map[string][]string, len(req.Namespaces))
	for slug, tools := range req.Namespaces {
		s := auth.NormalizeSlug(slug)
		if s == "" {
			writeError(w, http.StatusBadRequest, "invalid_namespace_slug", "命名空间 slug 不能为空")
			return
		}
		cleaned := make([]string, 0, len(tools))
		for _, t := range tools {
			t = strings.TrimSpace(t)
			if t == "" || t == "*" {
				continue
			}
			cleaned = append(cleaned, t)
		}
		normalized[s] = cleaned
	}

	// 先确认 Key 存在：ReplacePermissions 对不存在的 key_id 只会写进
	// 一堆孤儿行（外键若未开启级联），而且调用方会误以为授权成功。
	if _, err := s.keys.GetKeyByID(r.Context(), keyID); err != nil {
		s.fail(w, r, err)
		return
	}

	if err := s.keys.ReplacePermissions(r.Context(), keyID, normalized); err != nil {
		// 权限写了但缓存没失效 = 旧裁决继续生效，这是最危险的一类不一致，
		// 所以即使 ReplacePermissions 报错也要尝试清一遍。
		s.invalidateKey(keyID)
		s.fail(w, r, err)
		return
	}
	s.invalidateKey(keyID)

	writeJSON(w, http.StatusOK, permissionDTO{KeyID: string(keyID), Namespaces: normalized})
}
