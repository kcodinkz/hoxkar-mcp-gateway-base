// Package store 封装 PostgreSQL 访问。
//
// 覆盖范围：tenants / users / plans / api_keys / key_permissions /
// namespaces / namespace_tools / sessions / session_messages /
// usage_events / balances / ledger / reservations / price_rules /
// system_settings。call_logs 等留到 M4。
//
// 全部使用 pgx 原生接口与静态 SQL，不引 ORM。
//
// # 关于 NULL
//
// NOT NULL 策略把大部分列放宽成可空，理由是「大部分约束在代码层面实现」。
// 因此本包读取可空列时一律用 coalesce 兜底再 Scan，不用指针硬扛：
// pgx 把 SQL NULL 扫进 []byte 会直接报错，而 *[]byte 虽然能扫出 nil，
// 却把判空的责任散落到每个调用点。写法统一在 scan 系列函数里。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 是数据库访问入口。
type Store struct {
	pool *pgxpool.Pool

	// ownsPool 表示连接池归本 Store 所有，Close 时需要释放。
	//
	// 多租户编排下连接池由 shard.PoolSet 统一持有（同一个 DSN 绝不能
	// 建两个池：连接数白白翻倍，而且分片健康检查只会覆盖其中一个）。
	// 那种情况下 Store 只是「借用」默认池，ownsPool=false，
	// 释放交给 PoolSet，避免对同一个池 Close 两次。
	ownsPool bool
}

// Open 建立连接池并做一次 Ping 验证，确保 DSN 明显错误时立即失败。
func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析 DSN 失败: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建连接池失败: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		// Port 是 uint16，用 %d 而不是 %s；错误信息面向运维，格式必须正确
		return nil, fmt.Errorf("连接 %s:%d/%s 失败: %w",
			cfg.ConnConfig.Host, cfg.ConnConfig.Port, cfg.ConnConfig.Database, err)
	}

	return &Store{pool: pool, ownsPool: true}, nil
}

// Attach 用外部已有的连接池构造 Store，不接管其生命周期。
//
// 供多租户编排使用：连接池由 shard.PoolSet 统一建、统一 Ping、统一关，
// Store 只是拿默认池读写租户/用户/Key 这些「不分片」的元数据表。
// 分片库里的流水表（sessions / usage_events …）通过 PoolOfID 另取池，
// 不经过这里。
func Attach(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("store.Attach 需要一个非空连接池")
	}
	return &Store{pool: pool, ownsPool: false}, nil
}

// Close 释放连接池。借用来的池不在这里关，由持有方负责。
func (s *Store) Close() {
	if s != nil && s.pool != nil && s.ownsPool {
		s.pool.Close()
	}
}

// Pool 暴露底层连接池，供需要自定义查询的调用方使用。
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Ping 用于健康检查。
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Extensions 返回已安装的扩展版本，用于启动自检。
func (s *Store) Extensions(ctx context.Context) (map[string]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT extname, extversion FROM pg_extension`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string, 4)
	for rows.Next() {
		var name, version string
		if err := rows.Scan(&name, &version); err != nil {
			return nil, err
		}
		out[name] = version
	}
	return out, rows.Err()
}
