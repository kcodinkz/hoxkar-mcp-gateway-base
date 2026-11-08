// Package quota 按额度解析的优先级解析用户实际可用额度。
//
// 解析顺序（高优先级覆盖低优先级）：
//
//	users.quota_overrides  >  租户有效套餐  >  全局默认
//
// # 为什么要有「租户作用域」
//
// 套餐可以挂在两个地方：租户上（tenants.plan_id，企业级）或用户上
// （users.plan_id）。一个用户属于哪个租户、该租户有没有企业套餐，
// 决定了「按哪个套餐算额度」。这套判断只允许存在于本包：
// 散落到各 handler 里，就会出现「列表页按免费版显示、实际限流按企业版放行」
// 这类只在特定页面暴露的错额度。
//
// 本包不查库。调用方把已经读出的 User / Tenant / Plan 传进来，
// 解析是纯函数，便于单测，也不会在一次请求里反复打库。
package quota

import (
	"fmt"
	"math"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// Limits 是解析后的最终额度，单位与数据库列一致。
type Limits struct {
	MaxAPIKeys         int
	RateLimitPerMinute int
	MaxUploadBytes     int64
	MaxConcurrentJobs  int
	MaxNamespaces      int
	MaxPoolSize        int
}

// Default 返回当套餐缺失时的兜底值，取免费版量级。
// 这是唯一允许硬编码额度的位置，见额度解析「红线」第 1 条。
func Default() Limits {
	return Limits{
		MaxAPIKeys:         2,
		RateLimitPerMinute: 30,
		MaxUploadBytes:     64 << 20,
		MaxConcurrentJobs:  1,
		MaxNamespaces:      1,
		MaxPoolSize:        1,
	}
}

// FromPlan 把套餐行转成 Limits。
func FromPlan(p *store.Plan) Limits {
	if p == nil {
		return Default()
	}
	return Limits{
		MaxAPIKeys:         p.MaxAPIKeys,
		RateLimitPerMinute: p.RateLimitPerMinute,
		MaxUploadBytes:     p.MaxUploadBytes,
		MaxConcurrentJobs:  p.MaxConcurrentJobs,
		MaxNamespaces:      p.MaxNamespaces,
		MaxPoolSize:        p.MaxPoolSize,
	}
}

// Scope 描述「按谁的套餐算额度」。由 ResolveScope 填好，供日志与审计引用。
type Scope struct {
	// TenantID 是数据归属边界，可为空（用户尚未归属任何租户）。
	TenantID idgen.ID
	// TenantApplied 为 true 表示额度来自租户的企业套餐，而不是用户套餐。
	TenantApplied bool
	// Plan 是最终生效的套餐行；为空表示连套餐都查不到，只能用 Default()。
	Plan *store.Plan
	// Degraded 为 true 表示租户企业套餐已到期或已停用，额度回落到用户套餐。
	Degraded bool
}

// Describe 便于日志与错误信息中直接引用。
func (s Scope) Describe() string {
	code := "<none>"
	if s.Plan != nil {
		code = s.Plan.Code
	}
	return fmt.Sprintf("tenant=%q plan=%s applied_by_tenant=%t degraded=%t",
		s.TenantID, code, s.TenantApplied, s.Degraded)
}

// ResolveScope 判断该用哪个套餐，返回作用域与生效套餐。
//
// 参数均可为空：tenant 为空表示用户尚未归属租户；user 为空表示只按租户算。
// 判定顺序：
//
//  1. 租户挂着企业套餐且租户仍然「可用」（active 或 expired 未真正到期）
//     → 用租户套餐
//  2. 否则用用户自己的套餐
//  3. 两者都没有 → Plan 留空，调用方自然落到 Default()
//
// 关键点：租户过期不等于数据丢失，也不等于立刻清零额度（租户与分片 §9.4）。
// 过期只意味着「不再享受企业版额度」，回落到 owner 的用户套餐继续可用。
// 把过期直接当成「无额度」是错的——那会把一个到期未续费的用户锁在门外，
// 连导出自己数据的请求都发不出来。
func ResolveScope(user *store.User, tenant *store.Tenant, tenantPlan, userPlan *store.Plan, now time.Time) Scope {
	sc := Scope{}

	if user != nil {
		sc.TenantID = user.TenantID
	}

	// 租户套餐优先级最高，但必须租户当前真的可用。
	if tenantPlan != nil && tenant != nil && tenant.IsEnterprise() &&
		tenant.IsUsable() && !expiredAt(tenant.ExpiresAt, now) {
		sc.Plan = tenantPlan
		sc.TenantApplied = true
		return sc
	}

	// 租户不可用（到期/停用）或没挂企业套餐 → 回落用户套餐。
	if userPlan != nil {
		sc.Plan = userPlan
		// 只有「本来该用租户套餐却用不了」才算降级，
		// 普通个人用户一直用自己套餐，不该在日志里被标成 degraded。
		sc.Degraded = tenantPlan != nil && tenant != nil && tenant.IsEnterprise()
		return sc
	}

	// 都没查到：Plan 留空，Resolve 会返回 Default()。
	sc.Degraded = tenant != nil && tenant.IsEnterprise()
	return sc
}

// expiredAt 判断到期时间是否已过。nil 视为永不到期。
func expiredAt(t *time.Time, now time.Time) bool {
	return t != nil && !t.After(now)
}

// Resolve 在套餐基础上应用逐用户覆盖值。
// overrides 中出现未知键会被忽略，避免脏数据导致启动失败。
func Resolve(p *store.Plan, overrides map[string]any) Limits {
	lim := FromPlan(p)
	if len(overrides) == 0 {
		return lim
	}

	if v, ok := intFrom(overrides, "max_api_keys"); ok {
		lim.MaxAPIKeys = v
	}
	if v, ok := intFrom(overrides, "rate_limit_per_minute"); ok {
		lim.RateLimitPerMinute = v
	}
	if v, ok := int64From(overrides, "max_upload_bytes"); ok {
		lim.MaxUploadBytes = v
	}
	if v, ok := intFrom(overrides, "max_concurrent_jobs"); ok {
		lim.MaxConcurrentJobs = v
	}
	if v, ok := intFrom(overrides, "max_namespaces"); ok {
		lim.MaxNamespaces = v
	}
	if v, ok := intFrom(overrides, "max_pool_size"); ok {
		lim.MaxPoolSize = v
	}
	return lim
}

// EffectiveRateLimit 计算单个 Key 的最终限流值：
// Key 自带 rate_limit > 用户套餐/覆盖值。
func EffectiveRateLimit(lim Limits, keyRateLimit *int) int {
	if keyRateLimit != nil && *keyRateLimit > 0 {
		return *keyRateLimit
	}
	if lim.RateLimitPerMinute > 0 {
		return lim.RateLimitPerMinute
	}
	return Default().RateLimitPerMinute
}

// Subject 描述一个 Key 在运行时的有效身份与额度。
//
// TenantID 不是「可选装饰」：它决定本次请求的流水写进哪个分片（租户与分片），
// 也决定限流与计费记到哪个边界上。为空即表示该用户尚未归属——
// 调用方应拒绝这类请求，而不是让它们落到默认分片。
type Subject struct {
	KeyID    idgen.ID
	UserID   idgen.ID
	TenantID idgen.ID
	KeyName  string
	PlanCode string
	Limits   Limits
}

// Describe 便于日志与错误信息中直接引用。
// 租户单独用 %q 输出：空租户会显示成 tenant=""，比静默省略更容易在日志里发现。
func (s Subject) Describe() string {
	return fmt.Sprintf("key=%s(%s) user=%s tenant=%q plan=%s",
		s.KeyID, s.KeyName, s.UserID, s.TenantID, s.PlanCode)
}

// intFrom 从 JSONB 解出的 any 中取整数，兼容 float64（JSON 数字默认类型）、
// int 以及字符串数字，避免因类型差异静默失效。
func intFrom(m map[string]any, key string) (int, bool) {
	raw, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := raw.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		if v > float64(math.MaxInt32) || v < 0 {
			return 0, false
		}
		return int(v), true
	case float32:
		return int(v), true
	default:
		return 0, false
	}
}

func int64From(m map[string]any, key string) (int64, bool) {
	raw, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := raw.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		if v < 0 {
			return 0, false
		}
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		return 0, false
	}
}
