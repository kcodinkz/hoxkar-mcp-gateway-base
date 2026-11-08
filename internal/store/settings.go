package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// rawMessage 把「SQL NULL 归一后的文本」还原成 json.RawMessage。
//
// 为什么要专门归一，而不是直接把 NULL 扫进 []byte：
//   - pgx 把 SQL NULL 扫进 []byte 会报错，NULL 扫进 *[]byte 才得到 nil；
//   - 把 JSONB 列 coalesce 成 'null'::jsonb 也不行——值是文本 "null"，
//     json.Unmarshal 进 bool/int64/string 会「静默成功且不改目标」，
//     于是默认值被悄悄绕过（见 internal/settings 的 Snapshot 读取方法）。
//
// 约定：空串 = 该行没有值；"null" = 显式写入的 jsonb null。两者都算「无值」，
// 统一返回 nil，让上层用 `raw == nil` 判定并回落代码内置默认值。
func rawMessage(s string) json.RawMessage {
	if s == "" || s == "null" {
		return nil
	}
	return json.RawMessage(s)
}

// SettingRow 是 system_settings 的一行原始数据。
// 解析成具体类型由 internal/settings 负责，store 层只管搬运。
type SettingRow struct {
	Key         string
	Value       json.RawMessage
	ValueType   string
	Scope       string
	Description string
	IsSystem    bool
}

// ListSettings 全量读配置。网关启动与热更新都走这条。
//
// 配置项数量是几十条量级，全量读比逐条读更简单，也避免部分加载导致状态不一致。
//
// value 按 NOT NULL 策略已放宽为可空，这里把「没有值」归一成 Value == nil，
// 交给 internal/settings 的 Snapshot 回落内置默认值（其判定条件正是 raw == nil）。
// 之所以走 ::text 而不是直接扫 []byte 或 coalesce 成 'null'::jsonb，见 rawMessage 的注释。
func (s *Store) ListSettings(ctx context.Context) ([]SettingRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT key, coalesce(value::text, ''), value_type, scope, coalesce(description, ''), is_system
		FROM system_settings
		ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("查询系统配置失败: %w", err)
	}
	defer rows.Close()

	out := make([]SettingRow, 0, 32)
	for rows.Next() {
		var r SettingRow
		var raw string
		if err := rows.Scan(&r.Key, &raw, &r.ValueType, &r.Scope, &r.Description, &r.IsSystem); err != nil {
			return nil, err
		}
		// 两者语义都是「只声明不覆盖」，归一成 nil 后由读取侧回落默认值。
		r.Value = rawMessage(raw)
		out = append(out, r)
	}
	return out, rows.Err()
}

// SettingRevision 是配置变更历史的一行。
type SettingRevision struct {
	ID       idgen.ID
	Key      string
	OldValue json.RawMessage
	NewValue json.RawMessage
	AdminID  idgen.ID
	Reason   string
}

// UpsertSetting 写入配置，并同步留一条 revision。
//
// adminID 可以为 0（表示系统自身写入，例如迁移脚本后的自检）。
// reason 用于危险操作留痕（动态配置规则 11）。
func (s *Store) UpsertSetting(
	ctx context.Context,
	revID idgen.ID,
	key string,
	valueJSON []byte,
	valueType string,
	scope string,
	description string,
	adminID idgen.ID,
	reason string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// value 可空（NOT NULL 策略），裸列扫进 []byte 会在 NULL 上报错，所以转成文本。
	// 空串代表「没有旧值」，下面统一映射成 SQL NULL 写进 revision。
	var oldRaw string
	err = tx.QueryRow(ctx,
		`SELECT coalesce(value::text, '') FROM system_settings WHERE key = $1`, key).Scan(&oldRaw)
	switch {
	case err == nil:
	case err == pgx.ErrNoRows:
		oldRaw = ""
	default:
		return fmt.Errorf("读取旧配置失败: %w", err)
	}

	var adminArg any
	if !adminID.IsZero() {
		adminArg = adminID
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO system_settings (key, value, value_type, scope, description, updated_by)
		VALUES ($1, $2::jsonb, $3, $4, NULLIF($5, ''), $6)
		ON CONFLICT (key) DO UPDATE SET
			value       = EXCLUDED.value,
			value_type  = EXCLUDED.value_type,
			scope       = EXCLUDED.scope,
			description = COALESCE(EXCLUDED.description, system_settings.description),
			updated_by  = EXCLUDED.updated_by,
			updated_at  = now()`,
		key, string(valueJSON), valueType, scope, description, adminArg); err != nil {
		return fmt.Errorf("写入配置 %q 失败: %w", key, err)
	}

	var oldArg any
	if oldRaw != "" && oldRaw != "null" {
		oldArg = oldRaw
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO system_setting_revisions (id, key, old_value, new_value, admin_id, reason)
		VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, NULLIF($6, ''))`,
		revID, key, oldArg, string(valueJSON), adminArg, reason); err != nil {
		return fmt.Errorf("写入配置变更历史失败: %w", err)
	}

	return tx.Commit(ctx)
}

// ListSettingRevisions 返回某配置项的变更历史，按时间倒序。
func (s *Store) ListSettingRevisions(ctx context.Context, key string, limit int) ([]SettingRevision, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	// created_at 精度不足以区分同一批写入的多条修订，补 id 作次级键，
	// 否则管理端按 limit 拉「最近 N 条」时，相邻两次请求可能拿到不同的行集（分页契约）。
	// old_value / new_value 是 JSONB 且可空，同样不能裸扫进 []byte。
	// coalesce(..., '') 后的空串即「该侧没有值」，映射成 nil RawMessage，
	// 让上层能区分「首次设置（无旧值）」与「旧值是 jsonb null」。
	rows, err := s.pool.Query(ctx, `
		SELECT id, key, coalesce(old_value::text, ''), coalesce(new_value::text, ''),
		       coalesce(admin_id, 0), coalesce(reason, '')
		FROM system_setting_revisions
		WHERE ($1 = '' OR key = $1)
		ORDER BY created_at DESC, id DESC
		LIMIT $2`, key, limit)
	if err != nil {
		return nil, fmt.Errorf("查询配置变更历史失败: %w", err)
	}
	defer rows.Close()

	var out []SettingRevision
	for rows.Next() {
		var r SettingRevision
		var oldRaw, newRaw string
		if err := rows.Scan(&r.ID, &r.Key, &oldRaw, &newRaw, &r.AdminID, &r.Reason); err != nil {
			return nil, err
		}
		r.OldValue = rawMessage(oldRaw)
		r.NewValue = rawMessage(newRaw)
		out = append(out, r)
	}
	return out, rows.Err()
}
