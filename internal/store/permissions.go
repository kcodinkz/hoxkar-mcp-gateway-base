package store

import (
	"context"
	"fmt"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// Permission 是一条 key_permissions 记录。
type Permission struct {
	NamespaceID   idgen.ID
	NamespaceSlug string
	ToolName      string
}

// ReplacePermissions 覆盖式写入某 Key 的授权。
//
// perms 的键是命名空间 slug，值是该命名空间下的工具名列表；
// 空切片表示授该命名空间全部工具（落库为 tool_name='*'）。
func (s *Store) ReplacePermissions(ctx context.Context, keyID idgen.ID, perms map[string][]string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM key_permissions WHERE key_id = $1`, keyID); err != nil {
		return fmt.Errorf("清空权限失败: %w", err)
	}

	for slug, tools := range perms {
		var nsID idgen.ID
		if err := tx.QueryRow(ctx, `SELECT id FROM namespaces WHERE slug = $1`, slug).Scan(&nsID); err != nil {
			return fmt.Errorf("命名空间 %q 不存在: %w", slug, err)
		}
		if len(tools) == 0 {
			tools = []string{"*"}
		}
		for _, t := range tools {
			if t == "" {
				t = "*"
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO key_permissions (key_id, namespace_id, tool_name)
				 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				keyID, nsID, t); err != nil {
				return fmt.Errorf("写入权限 %s/%s 失败: %w", slug, t, err)
			}
		}
	}

	return tx.Commit(ctx)
}

// ListPermissions 返回某 Key 的全部授权。
func (s *Store) ListPermissions(ctx context.Context, keyID idgen.ID) ([]Permission, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT p.namespace_id, n.slug, p.tool_name
		FROM key_permissions p
		JOIN namespaces n ON n.id = p.namespace_id
		WHERE p.key_id = $1
		ORDER BY n.slug, p.tool_name`, keyID)
	if err != nil {
		return nil, fmt.Errorf("查询权限失败: %w", err)
	}
	defer rows.Close()

	var out []Permission
	for rows.Next() {
		var p Permission
		if err := rows.Scan(&p.NamespaceID, &p.NamespaceSlug, &p.ToolName); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CanUseNamespace 判断 Key 是否有该命名空间的任意访问权。
func (s *Store) CanUseNamespace(ctx context.Context, keyID, namespaceID idgen.ID) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM key_permissions
			WHERE key_id = $1 AND namespace_id = $2
		)`, keyID, namespaceID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("裁决命名空间权限失败: %w", err)
	}
	return exists, nil
}

// CanUseTool 判断 Key 是否有某工具的访问权。
// tool_name 为 '*' 的记录覆盖该命名空间下全部工具。
func (s *Store) CanUseTool(ctx context.Context, keyID, namespaceID idgen.ID, tool string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM key_permissions
			WHERE key_id = $1 AND namespace_id = $2
			  AND (tool_name = '*' OR tool_name = $3)
		)`, keyID, namespaceID, tool).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("裁决工具权限失败: %w", err)
	}
	return exists, nil
}
