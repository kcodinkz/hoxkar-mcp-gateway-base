package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// UserStatus 取值与 users.status 的 CHECK 约束保持一致。
type UserStatus string

const (
	UserActive    UserStatus = "active"
	UserSuspended UserStatus = "suspended"
	UserClosed    UserStatus = "closed"
)

// User 是 users 表的一行。
type User struct {
	ID          idgen.ID
	Email       string
	DisplayName string

	// TenantID 是数据归属边界（租户与分片），决定这个用户的流水落在哪个分片。
	//
	// 可为空（NOT NULL 策略）：账号先注册、租户后补挂是合法路径，例如邀请制入职
	// 或从旧系统导入。空值只意味着「还没归属」，不意味着可以当 0 处理——
	// 任何要按租户分片的逻辑都必须先判空并给出明确错误。
	TenantID idgen.ID

	PlanID         idgen.ID
	Status         UserStatus
	QuotaOverrides map[string]any
	ExpiresAt      *time.Time
	CreatedAt      time.Time
}

const userColumns = `id, email, coalesce(display_name, ''), plan_id, status,
                     quota_overrides, expires_at, created_at, coalesce(tenant_id, '')`

// CreateUser 写入账号。主键由调用方生成。
func (s *Store) CreateUser(ctx context.Context, id idgen.ID, email, displayName string, planID idgen.ID, overrides map[string]any, expiresAt *time.Time) (*User, error) {
	if id.IsZero() || planID.IsZero() {
		return nil, errors.New("创建用户需要非零的 userID 与 planID")
	}
	if overrides == nil {
		overrides = map[string]any{}
	}
	raw, err := json.Marshal(overrides)
	if err != nil {
		return nil, fmt.Errorf("序列化 quota_overrides 失败: %w", err)
	}

	var (
		createdAt time.Time
		status    string
	)
	err = s.pool.QueryRow(ctx, `
		INSERT INTO users (id, email, display_name, plan_id, quota_overrides, expires_at)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5::jsonb, $6)
		RETURNING created_at, status`,
		id, email, displayName, planID, string(raw), expiresAt,
	).Scan(&createdAt, &status)
	if err != nil {
		return nil, fmt.Errorf("创建用户失败: %w", err)
	}

	return &User{
		ID:             id,
		Email:          email,
		DisplayName:    displayName,
		PlanID:         planID,
		Status:         UserStatus(status),
		QuotaOverrides: overrides,
		ExpiresAt:      expiresAt,
		CreatedAt:      createdAt,
	}, nil
}

// GetUserByID 按主键取账号。
func (s *Store) GetUserByID(ctx context.Context, id idgen.ID) (*User, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id = $1`, id)
	return scanUser(row)
}

// GetUserByEmail 按邮箱取账号，用于登录与注册时的唯一性前置校验。
func (s *Store) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE email = $1`, email)
	return scanUser(row)
}

func scanUser(row pgx.Row) (*User, error) {
	var (
		u         User
		status    string
		raw       []byte
		tenantRaw string
	)
	// tenant_id 用 coalesce 读成空字符串再转 ID：idgen.ID 的零值就是 ""，
	// 所以这里不需要额外「有没有值」的分支，调用方用 IsZero 判断即可。
	err := row.Scan(&u.ID, &u.Email, &u.DisplayName, &u.PlanID, &status,
		&raw, &u.ExpiresAt, &u.CreatedAt, &tenantRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("读取用户失败: %w", err)
	}
	u.Status = UserStatus(status)
	u.TenantID = idgen.ID(tenantRaw)
	u.QuotaOverrides = map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &u.QuotaOverrides); err != nil {
			return nil, fmt.Errorf("解析用户 %s 的 quota_overrides 失败: %w", u.ID, err)
		}
	}
	return &u, nil
}

// CreateKeyWithQuotaCheck 在同一事务内完成「配额校验 + Key 写入」，
// 避免并发下越权超额，符合额度解析第 3 条。
//
// 返回值：
//   - 成功：新建的 Key
//   - 超限：ErrKeyQuotaExceeded，含当前数量与实际上限
//   - 用户无效：ErrNotFound 或 ErrUserNotActive
func (s *Store) CreateKeyWithQuotaCheck(
	ctx context.Context,
	keyID idgen.ID,
	userID idgen.ID,
	name, prefix string,
	hash []byte,
	rateLimit *int,
	expiresAt *time.Time,
	limit int,
) (*APIKey, error) {
	if keyID.IsZero() || userID.IsZero() {
		return nil, errors.New("创建 API Key 需要非零的 keyID 与 userID")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 锁住用户行，串行化同一用户的并发创建请求
	var status string
	if err := tx.QueryRow(ctx,
		`SELECT status FROM users WHERE id = $1 FOR UPDATE`, userID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("锁定用户失败: %w", err)
	}
	if status != string(UserActive) {
		return nil, fmt.Errorf("%w: 当前状态 %s", ErrUserNotActive, status)
	}

	var current int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM api_keys WHERE user_id = $1 AND enabled`, userID).Scan(&current); err != nil {
		return nil, fmt.Errorf("统计现有 Key 数量失败: %w", err)
	}
	if current >= limit {
		return nil, &KeyQuotaExceededError{Limit: limit, Current: current}
	}

	var createdAt time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO api_keys (id, user_id, name, key_prefix, key_hash, rate_limit, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at`,
		keyID, userID, name, prefix, hash, rateLimit, expiresAt,
	).Scan(&createdAt); err != nil {
		return nil, fmt.Errorf("创建 API Key 失败: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("提交事务失败: %w", err)
	}

	return &APIKey{
		ID:        keyID,
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

// ErrUserNotActive 表示账号不可用。
var ErrUserNotActive = errors.New("用户不可用")

// KeyQuotaExceededError 表示已达 Key 数量上限，携带具体数值便于前端提示。
type KeyQuotaExceededError struct {
	Limit   int
	Current int
}

func (e *KeyQuotaExceededError) Error() string {
	return fmt.Sprintf("已达到套餐 Key 上限 %d 个（当前 %d 个），请禁用不用的 Key 或升级套餐", e.Limit, e.Current)
}
