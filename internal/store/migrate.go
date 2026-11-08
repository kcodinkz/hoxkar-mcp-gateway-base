package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/migrations"
)

// MigrateOptions 是迁移的附加开关。
//
// 生产路径使用零值，行为与加入本结构体之前完全一致。
type MigrateOptions struct {
	// ResetSchema 在执行迁移前先 DROP SCHEMA public CASCADE 再重建空 schema。
	//
	// 顺序是硬约束，不能改：必须先清库、再查 schema_migrations。
	// 若反过来（先查已应用版本、再清库），查到的是一份非空的版本集合，
	// 清库之后三份基线会全部被判为「已应用」而跳过，最终得到一个
	// 空空如也却显示「已是最新」的数据库 —— 这种状态只能靠人工排查。
	//
	// 该开关只供开发期使用，main.go 侧必须完成二次确认后再置为 true。
	ResetSchema bool

	// Logf 用于输出迁移进度；为 nil 时静默。
	Logf func(format string, args ...any)
}

func (o MigrateOptions) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// Migrate 按文件名升序执行 migrations 目录下尚未应用的脚本。
//
// 幂等性由 schema_migrations 表保证：已记录的版本直接跳过。
// 单个脚本内不隐式加事务外层的 BEGIN，交由 pq/pgx 的简单协议执行，
// 因为 CREATE EXTENSION 等语句在部分 PG 版本下不允许显式事务包裹。
func Migrate(ctx context.Context, s *Store) error {
	return MigrateWithOptions(ctx, s, MigrateOptions{})
}

// MigrateWithOptions 是 Migrate 的可配置版本。
func MigrateWithOptions(ctx context.Context, s *Store, opts MigrateOptions) error {
	if opts.ResetSchema {
		if err := resetSchema(ctx, s, opts.logf); err != nil {
			return err
		}
	}

	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    BIGINT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("创建 schema_migrations 失败: %w", err)
	}

	applied, err := appliedVersions(ctx, s)
	if err != nil {
		return err
	}

	entries, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return fmt.Errorf("枚举迁移脚本失败: %w", err)
	}
	sort.Strings(entries)

	for _, name := range entries {
		version, err := parseVersion(name)
		if err != nil {
			return err
		}
		if _, ok := applied[version]; ok {
			opts.logf("[migrate] 跳过 %s（版本 %d 已应用）", name, version)
			continue
		}

		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("读取 %s 失败: %w", name, err)
		}

		opts.logf("[migrate] 应用 %s", name)
		if err := s.runScript(ctx, version, name, string(body)); err != nil {
			return err
		}
	}

	return nil
}

// resetSchema 清空 public schema 并重建。
//
// 会一并删除：全部业务表、索引、视图、函数、触发器，以及建在 public 下的
// 扩展对象（vector / pg_trgm / pgcrypto）。扩展本身由 0001_schema.sql
// 的 CREATE EXTENSION IF NOT EXISTS 在随后重新装回，因此不需要在这里补。
//
// 先把当前库名打出来，是为了让开发者在日志里确认自己清的到底是哪一个库 ——
// 这个开关没有任何「只清某个前缀」的保护，清错了就是整个库没了。
func resetSchema(ctx context.Context, s *Store, logf func(string, ...any)) error {
	var dbName, userName string
	if err := s.pool.QueryRow(ctx,
		`SELECT current_database(), current_user`).Scan(&dbName, &userName); err != nil {
		return fmt.Errorf("重置 schema 前查询当前库失败: %w", err)
	}
	if logf != nil {
		logf("[migrate] 重置 schema：库=%s 用户=%s，将执行 DROP SCHEMA public CASCADE", dbName, userName)
	}

	// DROP + CREATE 放在同一段 Exec 里，PG 以隐式事务执行，不会出现
	// 「库已清空但 schema 未重建」的中间态。
	if _, err := s.pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS public CASCADE;
		CREATE SCHEMA public;
	`); err != nil {
		return fmt.Errorf("重置 public schema 失败: %w", err)
	}

	if logf != nil {
		logf("[migrate] public schema 已重置，开始执行全量基线")
	}
	return nil
}

// runScript 以隐式事务执行整段脚本并登记版本号。
// 使用 pgx 的 Exec 发送整段文本，PG 会把它当作一个隐式事务处理。
func (s *Store) runScript(ctx context.Context, version int64, name, body string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, body); err != nil {
		return fmt.Errorf("执行迁移 %s 失败: %w", name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`,
		version); err != nil {
		return fmt.Errorf("登记迁移版本 %d 失败: %w", version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("提交迁移 %s 失败: %w", name, err)
	}
	return nil
}

func appliedVersions(ctx context.Context, s *Store) (map[int64]struct{}, error) {
	rows, err := s.pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("查询已应用迁移失败: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]struct{}, 8)
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = struct{}{}
	}
	return out, rows.Err()
}

// parseVersion 从 0001_init.sql 这样的文件名取出 1。
func parseVersion(filename string) (int64, error) {
	base := strings.TrimSuffix(filename, ".sql")
	head := base
	if idx := strings.IndexByte(base, '_'); idx > 0 {
		head = base[:idx]
	}
	v, err := strconv.ParseInt(head, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("迁移文件名 %q 不符合 NNNN_name.sql 约定: %w", filename, err)
	}
	return v, nil
}
