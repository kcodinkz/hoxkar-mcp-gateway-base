package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// 软禁用原因。取值必须与 0001_schema.sql 的两个 CHECK 约束逐一对应，
// 多一个值数据库直接拒绝，少一个值就意味着某种状态恢复不回来。
//
//	api_keys.disabled_by    ∈ (plan_expiry, admin, tenant_closed)
//	namespaces.disabled_by  ∈ (plan_expiry, admin, registry_gone)
const (
	// DisabledByPlanExpiry 套餐到期导致的批量软禁用，续费后可原样恢复（租户与分片 §9.4）。
	DisabledByPlanExpiry = "plan_expiry"
	// DisabledByAdmin 管理员手工禁用。自动恢复**不得**覆盖它。
	DisabledByAdmin = "admin"
	// DisabledByTenantClosed 租户被注销/停用，名下 Key 全部失效。
	DisabledByTenantClosed = "tenant_closed"
	// DisabledByRegistryGone 注册中心里已找不到该服务，由同步器自动下线。
	DisabledByRegistryGone = "registry_gone"
)

// keyDisabledReasons 是 api_keys 允许的原因集合。
var keyDisabledReasons = map[string]struct{}{
	DisabledByPlanExpiry:   {},
	DisabledByAdmin:        {},
	DisabledByTenantClosed: {},
}

// namespaceDisabledReasons 是 namespaces 允许的原因集合。
var namespaceDisabledReasons = map[string]struct{}{
	DisabledByPlanExpiry:   {},
	DisabledByAdmin:        {},
	DisabledByRegistryGone: {},
}

// validateKeyDisabledReason 在应用层先拦一道非法原因。
//
// 数据库的 CHECK 只会回一句 "violates check constraint"，看不出是哪一行、
// 哪个调用点传错了；这里直接说清合法取值。
func validateKeyDisabledReason(reason string) error {
	if _, ok := keyDisabledReasons[reason]; !ok {
		return fmt.Errorf("非法的 API Key 禁用原因 %q，只能是 plan_expiry / admin / tenant_closed", reason)
	}
	return nil
}

// validateNamespaceDisabledReason 同上，针对 namespaces 的取值集合。
func validateNamespaceDisabledReason(reason string) error {
	if _, ok := namespaceDisabledReasons[reason]; !ok {
		return fmt.Errorf("非法的命名空间禁用原因 %q，只能是 plan_expiry / admin / registry_gone", reason)
	}
	return nil
}

// SweepTenantExpiry 把已到期的企业租户从 active 翻成 expired，返回本轮翻动的租户 ID。
//
// 关键设计：**到期只降级，不封停**。
// 租户与分片要求「到期不代表数据丢失，只是被限制看不到」，所以这里刻意
// 不去禁用 api_keys —— 若把 Key 一并停掉，用户就彻底进不来了，那叫封停。
// 到期后的真实效果由 quota.ResolveScope 决定：企业套餐失效，回落到 owner
// 的普通套餐额度（并置 Degraded=true），数据一行不删。
//
// 真要连访问一起掐断（例如欠费清退），调用方显式再调
// DisableKeysByTenant(..., DisabledByTenantClosed)，两步分开、各自可审计。
//
// limit <= 0 表示不限条数。按 expires_at 升序处理，先到期的先翻，
// 这样即使一次积压了几千条，多轮调用也能保证不会长期饿死早期记录。
func (s *Store) SweepTenantExpiry(ctx context.Context, now time.Time, limit int) ([]idgen.ID, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM tenants
			WHERE kind = 'enterprise'
			  AND status = 'active'
			  AND expires_at IS NOT NULL
			  AND expires_at <= $1
			ORDER BY expires_at
			LIMIT $2
		)
		UPDATE tenants t
		SET status = 'expired', updated_at = now()
		FROM due
		WHERE t.id = due.id
		RETURNING t.id`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("到期租户扫描失败: %w", err)
	}
	defer rows.Close()

	return collectIDs(rows)
}

// RenewTenant 续费：写入新的到期时间，并把 expired 状态拉回 active。
//
// 只在 kind='enterprise' 上允许续费；个人租户没有到期概念，传进来一定是调用方写错了。
// 已经 suspended / closed 的租户不在这里复活——那需要管理员显式操作，
// 否则一次「补缴续费」会顺手绕过封禁。
//
// 本方法只改 tenants 一行。若此前调用过 DisableKeysByTenant，调用方还需再调
// RestoreKeysByTenant(ctx, id, DisabledByPlanExpiry) 把 Key 按原因恢复，
// 两步刻意分开：恢复范围与恢复时机是两个独立决策。
func (s *Store) RenewTenant(ctx context.Context, id idgen.ID, newExpiresAt time.Time) (*Tenant, error) {
	if id.IsZero() {
		return nil, errors.New("租户 ID 不能为空")
	}
	if err := idgen.ValidateOf(idgen.KindTenant, string(id)); err != nil {
		return nil, err
	}
	if newExpiresAt.IsZero() {
		return nil, errors.New("续费必须给出新的到期时间")
	}

	row := s.pool.QueryRow(ctx, `
		UPDATE tenants SET
			expires_at = $2,
			status     = CASE WHEN status = 'expired' THEN 'active' ELSE status END,
			updated_at = now()
		WHERE id = $1 AND kind = 'enterprise'
		RETURNING `+tenantColumns, id, newExpiresAt)

	t, err := scanTenant(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("%w: 租户 %s 不存在或不是企业级，无法续费", ErrNotFound, id)
		}
		return nil, err
	}
	return t, nil
}

// DisableNamespace 软下线一个命名空间并记录原因。
//
// namespaces 是全局服务目录，不归租户所有，因此这里不接受租户维度。
// 同步器（注册中心同步）用 DisabledByRegistryGone，管理员操作用 DisabledByAdmin。
func (s *Store) DisableNamespace(ctx context.Context, id idgen.ID, reason string) error {
	if id.IsZero() {
		return errors.New("命名空间 ID 不能为空")
	}
	if err := validateNamespaceDisabledReason(reason); err != nil {
		return err
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE namespaces SET enabled = FALSE, disabled_by = $2, updated_at = now()
		WHERE id = $1`, id, reason)
	if err != nil {
		return fmt.Errorf("下线命名空间失败: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// EnableNamespace 重新启用命名空间并清空原因。
func (s *Store) EnableNamespace(ctx context.Context, id idgen.ID) error {
	if id.IsZero() {
		return errors.New("命名空间 ID 不能为空")
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE namespaces SET enabled = TRUE, disabled_by = NULL, updated_at = now()
		WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("启用命名空间失败: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
