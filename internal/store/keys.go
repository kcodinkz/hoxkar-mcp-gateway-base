package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// ErrNotFound 表示查询目标不存在。
var ErrNotFound = errors.New("记录不存在")

// ErrSlugConflict 表示同名 slug 已被「另一种来源」占用。
//
// 典型场景：管理员手工建了 slug=blender（source=manual），而注册中心里
// 也出现了一个叫 blender 的服务，同步器试图以 source=registry 写入时被挡下。
// 这不是故障，而是刻意的保护：不让自动发现覆盖人工配置。
// 调用方应当「跳过 + 记 warn 日志」，而不是把它当成错误上抛。
var ErrSlugConflict = errors.New("命名空间 slug 已被其他来源占用")

// APIKey 是 api_keys 表的一行。主键遵循ID 形态，使用 idgen.ID。
type APIKey struct {
	ID        idgen.ID
	UserID    idgen.ID
	Name      string
	KeyPrefix string
	KeyHash   []byte
	Enabled   bool

	// DisabledBy 记录软禁用原因，必须与 Enabled 成对维护。
	//
	// 数据库有 api_keys_disabled_consistency CHECK (enabled OR disabled_by IS NOT NULL)：
	// 只把 enabled 置 FALSE 而不写原因会被直接拒绝。这正是历史实现
	// （UPDATE api_keys SET enabled = FALSE）跑不通的原因，现在唯一的
	// 禁用入口是 DisableKeyWithReason，它在同一条 SQL 里写两个字段。
	//
	// 取值见 DisabledByPlanExpiry / DisabledByAdmin / DisabledByTenantClosed。
	DisabledBy *string

	RateLimit  *int
	ExpiresAt  *time.Time
	CreatedAt  time.Time
	LastUsedAt *time.Time
}

// IsExpired 判断 Key 是否已过期。
//
// 注意：Key 自身过期不会翻转 Enabled。原因有两点：
//  1. disabled_by 只表达「被谁停用」，把「自然到期」塞进去会污染审计语义；
//  2. 到期判定依赖当前时间，写进库里就变成了需要定时任务维护的冗余状态。
//
// 因此 authenticator 必须在放行前调用本方法，而不是只查 enabled。
func (k APIKey) IsExpired(now time.Time) bool {
	return k.ExpiresAt != nil && now.After(*k.ExpiresAt)
}

const keyColumns = `id, user_id, name, key_prefix, key_hash, enabled, disabled_by,
                    rate_limit, expires_at, created_at, last_used_at`

// CreateKey 写入一条新 Key 记录。
// 主键由调用方通过 idgen.NewRandom(idgen.KindAPIKey) 生成后传入，符合ID 形态。
func (s *Store) CreateKey(ctx context.Context, id, userID idgen.ID, name, prefix string, hash []byte, rateLimit *int, expiresAt *time.Time) (*APIKey, error) {
	if id.IsZero() || userID.IsZero() {
		return nil, errors.New("创建 API Key 需要非零的 keyID 与 userID")
	}

	var createdAt time.Time
	err := s.pool.QueryRow(ctx, `
		INSERT INTO api_keys (id, user_id, name, key_prefix, key_hash, rate_limit, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at`,
		id, userID, name, prefix, hash, rateLimit, expiresAt,
	).Scan(&createdAt)
	if err != nil {
		return nil, fmt.Errorf("创建 API Key 失败: %w", err)
	}

	return &APIKey{
		ID:        id,
		UserID:    userID,
		Name:      name,
		KeyPrefix: prefix,
		KeyHash:   hash,
		Enabled:   true,
		RateLimit: rateLimit,
		ExpiresAt: expiresAt,
		CreatedAt: createdAt,
	}, nil
}

// FindKeyByHash 按 sha256 值查 Key；不存在返回 ErrNotFound。
func (s *Store) FindKeyByHash(ctx context.Context, hash []byte) (*APIKey, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+keyColumns+` FROM api_keys WHERE key_hash = $1`, hash)

	return scanKey(row)
}

// GetKeyByID 按主键查 Key。
func (s *Store) GetKeyByID(ctx context.Context, id idgen.ID) (*APIKey, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+keyColumns+` FROM api_keys WHERE id = $1`, id)

	return scanKey(row)
}

// ListKeysByUser 返回某用户名下全部 Key，按创建时间倒序。
//
// 排序带上 id 作次级键（分页契约）：同一毫秒内批量创建的 Key 时间戳相同，
// 只按 created_at 排序时 PostgreSQL 不保证两次查询的相对顺序一致。
func (s *Store) ListKeysByUser(ctx context.Context, userID idgen.ID) ([]APIKey, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+keyColumns+` FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC, id DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("查询 API Key 列表失败: %w", err)
	}
	defer rows.Close()

	var out []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(keyScanTargets(&k)...); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// UpdateKey 局部更新 Key 的元数据。传 nil 表示该字段不改。
// clearExpiry 为 true 时把 expires_at 置 NULL（与传 nil 的「不修改」区分开）。
//
// 注意这里刻意不接收 enabled：启用/禁用必须走 EnableKey 与
// DisableKeyWithReason。因为 enabled 与 disabled_by 是两个必须同步改的列，
// 留一条「只改 enabled」的后门，等于把 constraint violation 留到运行期。
func (s *Store) UpdateKey(ctx context.Context, id idgen.ID, name *string, rateLimit *int, expiresAt *time.Time, clearExpiry bool) (*APIKey, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE api_keys SET
			name       = coalesce($2, name),
			rate_limit = coalesce($3, rate_limit),
			expires_at = CASE WHEN $5 THEN NULL ELSE coalesce($4, expires_at) END
		WHERE id = $1
		RETURNING `+keyColumns,
		id, name, rateLimit, expiresAt, clearExpiry)

	return scanKey(row)
}

// EnableKey 恢复一个 Key，并清空禁用原因。
func (s *Store) EnableKey(ctx context.Context, id idgen.ID) (*APIKey, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE api_keys SET enabled = TRUE, disabled_by = NULL
		WHERE id = $1
		RETURNING `+keyColumns, id)

	return scanKey(row)
}

// DisableKey 软删除：置 enabled=FALSE，不删除行，以保留审计线索。
//
// 等价于以 DisabledByAdmin 为原因调用 DisableKeyWithReason。
func (s *Store) DisableKey(ctx context.Context, id idgen.ID) error {
	return s.DisableKeyWithReason(ctx, id, DisabledByAdmin)
}

// DisableKeyWithReason 软删除并记录原因，是禁用 Key 的唯一入口。
//
// 幂等：已禁用的 Key 再禁用一次不会报错，只会覆写原因。
func (s *Store) DisableKeyWithReason(ctx context.Context, id idgen.ID, reason string) error {
	if err := validateKeyDisabledReason(reason); err != nil {
		return err
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE api_keys SET enabled = FALSE, disabled_by = $2
		WHERE id = $1`, id, reason)
	if err != nil {
		return fmt.Errorf("禁用 API Key 失败: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DisableKeysByTenant 软禁用某个租户名下全部**仍然启用**的 Key，返回受影响的 ID。
//
// api_keys 没有 tenant_id，归属链是 api_keys.user_id → users.tenant_id，
// 所以这里必须 JOIN。租户注销/到期都只动这两个字段，绝不删行（租户与分片）。
func (s *Store) DisableKeysByTenant(ctx context.Context, tenantID idgen.ID, reason string) ([]idgen.ID, error) {
	if tenantID.IsZero() {
		return nil, errors.New("租户 ID 不能为空")
	}
	if err := validateKeyDisabledReason(reason); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		UPDATE api_keys k SET enabled = FALSE, disabled_by = $2
		FROM users u
		WHERE k.user_id = u.id
		  AND u.tenant_id = $1
		  AND k.enabled
		RETURNING k.id`, tenantID, reason)
	if err != nil {
		return nil, fmt.Errorf("按租户禁用 API Key 失败: %w", err)
	}
	defer rows.Close()

	return collectIDs(rows)
}

// RestoreKeysByTenant 续费/恢复时按原因原样回置，是租户与分片 §9.4 的落地实现。
//
// 只恢复 disabled_by = reason 的行。管理员手工禁用的（admin）保持禁用 ——
// 自动恢复不能越过人工决定。
func (s *Store) RestoreKeysByTenant(ctx context.Context, tenantID idgen.ID, reason string) ([]idgen.ID, error) {
	if tenantID.IsZero() {
		return nil, errors.New("租户 ID 不能为空")
	}
	if err := validateKeyDisabledReason(reason); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		UPDATE api_keys k SET enabled = TRUE, disabled_by = NULL
		FROM users u
		WHERE k.user_id = u.id
		  AND u.tenant_id = $1
		  AND NOT k.enabled
		  AND k.disabled_by = $2
		RETURNING k.id`, tenantID, reason)
	if err != nil {
		return nil, fmt.Errorf("按租户恢复 API Key 失败: %w", err)
	}
	defer rows.Close()

	return collectIDs(rows)
}

// keyScanTargets 让所有扫描点共用一份列顺序，避免加字段时只改一处。
func keyScanTargets(k *APIKey) []any {
	return []any{
		&k.ID, &k.UserID, &k.Name, &k.KeyPrefix, &k.KeyHash,
		&k.Enabled, &k.DisabledBy,
		&k.RateLimit, &k.ExpiresAt, &k.CreatedAt, &k.LastUsedAt,
	}
}

// TouchKey 更新 last_used_at。失败不影响主流程，调用方可忽略错误。
func (s *Store) TouchKey(ctx context.Context, id idgen.ID) error {
	_, err := s.pool.Exec(ctx, `UPDATE api_keys SET last_used_at = now() WHERE id = $1`, id)
	return err
}

func scanKey(row pgx.Row) (*APIKey, error) {
	var k APIKey
	err := row.Scan(keyScanTargets(&k)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("读取 API Key 失败: %w", err)
	}
	return &k, nil
}

// collectIDs 读完一个只返回单列 ID 的结果集。
func collectIDs(rows pgx.Rows) ([]idgen.ID, error) {
	var out []idgen.ID
	for rows.Next() {
		var id idgen.ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
