package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// Plan 是 plans 表的一行，即额度解析中的「套餐」。
type Plan struct {
	ID                 idgen.ID
	Code               string
	Name               string
	Description        string
	MaxAPIKeys         int
	RateLimitPerMinute int
	MaxUploadBytes     int64
	MaxConcurrentJobs  int
	MaxNamespaces      int
	MaxPoolSize        int
	Active             bool
}

// name 用 coalesce 包住：按 NOT NULL 策略它已放宽为可空（展示名不是结构必需的），
// 但 Plan.Name 是 string，裸列遇到 NULL 会直接 Scan 报错。
// 缺失时回落空串，由管理端决定是否用 code 顶替展示。
const planColumns = `id, code, coalesce(name, ''), coalesce(description, ''), max_api_keys,
                     rate_limit_per_minute, max_upload_bytes, max_concurrent_jobs,
                     max_namespaces, max_pool_size, active`

// GetPlanByCode 按稳定标识取套餐。
func (s *Store) GetPlanByCode(ctx context.Context, code string) (*Plan, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+planColumns+` FROM plans WHERE code = $1`, code)
	return scanPlan(row)
}

// GetPlanByID 按主键取套餐。
func (s *Store) GetPlanByID(ctx context.Context, id idgen.ID) (*Plan, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+planColumns+` FROM plans WHERE id = $1`, id)
	return scanPlan(row)
}

// ListPlans 返回全部启用套餐，供管理端展示。
func (s *Store) ListPlans(ctx context.Context) ([]Plan, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+planColumns+` FROM plans WHERE active ORDER BY max_api_keys`)
	if err != nil {
		return nil, fmt.Errorf("查询套餐列表失败: %w", err)
	}
	defer rows.Close()

	var out []Plan
	for rows.Next() {
		var p Plan
		if err := rows.Scan(&p.ID, &p.Code, &p.Name, &p.Description, &p.MaxAPIKeys,
			&p.RateLimitPerMinute, &p.MaxUploadBytes, &p.MaxConcurrentJobs,
			&p.MaxNamespaces, &p.MaxPoolSize, &p.Active); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func scanPlan(row pgx.Row) (*Plan, error) {
	var p Plan
	err := row.Scan(&p.ID, &p.Code, &p.Name, &p.Description, &p.MaxAPIKeys,
		&p.RateLimitPerMinute, &p.MaxUploadBytes, &p.MaxConcurrentJobs,
		&p.MaxNamespaces, &p.MaxPoolSize, &p.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("读取套餐失败: %w", err)
	}
	return &p, nil
}
