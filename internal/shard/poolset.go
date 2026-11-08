package shard

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// PoolSet 把「分片号 → 连接池」这件事收在一个地方。
//
// 它从第一天就存在，哪怕本期只配一个库。原因是扩库这个动作的形态：
// 如果今天代码里到处是 store.Pool()，明天要分库就得改遍全仓；
// 而有 PoolSet 之后，扩库只是**加一条配置 + 注册一个池**，调用方一行不动。
//
// 未单独注册的分片一律回落默认池——单库阶段所有分片都落在默认池，
// 分片号只体现在 ID 里，不体现在连接上。
type PoolSet struct {
	mu  sync.RWMutex
	def *pgxpool.Pool
	m   map[Shard]*pgxpool.Pool
}

// NewPoolSet 以 def 作为默认池创建集合。def 可以为 nil，此时
// 所有分片都必须显式 Register，否则 Pool 返回 ErrUnknownShard。
func NewPoolSet(def *pgxpool.Pool) *PoolSet {
	return &PoolSet{def: def, m: make(map[Shard]*pgxpool.Pool)}
}

// SetDefault 设置默认池，用于启动编排（先建集合、后连库）。
func (p *PoolSet) SetDefault(def *pgxpool.Pool) {
	p.mu.Lock()
	p.def = def
	p.mu.Unlock()
}

// Register 为一个分片登记专属连接池。
//
// 分片号会先校验：00 是保留值，登记时直接拒绝；未知名、非法字符同理。
// 之所以不悄悄忽略，是因为登记失败多半意味着配置写错了数据库地址——
// 让它在启动时就炸，好过在流量进来时把某个租户的数据写进默认库。
func (p *PoolSet) Register(sh Shard, pool *pgxpool.Pool) error {
	norm, err := Parse(string(sh))
	if err != nil {
		return fmt.Errorf("登记分片 %q 的连接池失败: %w", sh, err)
	}
	if pool == nil {
		return fmt.Errorf("登记分片 %q 的连接池失败: 连接池为 nil", sh)
	}

	p.mu.Lock()
	old := p.m[norm]
	p.m[norm] = pool
	def := p.def
	p.mu.Unlock()

	// 覆盖登记时，旧池若已不再被任何分片引用、也不是默认池，就关掉它，
	// 避免连接泄漏。判断放在锁外——Close 可能耗时，不该占着写锁。
	if old != nil && old != pool && old != def && !p.anyShardUses(old) {
		old.Close()
	}
	return nil
}

// defaultPool 返回默认池，nil 安全。
func (p *PoolSet) defaultPool() *pgxpool.Pool {
	p.mu.RLock()
	def := p.def
	p.mu.RUnlock()
	return def
}

// anyShardUses 判断某个池是否仍被某个分片引用。
func (p *PoolSet) anyShardUses(pool *pgxpool.Pool) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, v := range p.m {
		if v == pool {
			return true
		}
	}
	return false
}

// Default 返回默认池，可能为 nil。
func (p *PoolSet) Default() *pgxpool.Pool { return p.defaultPool() }

// Pool 返回该分片的连接池：优先取单独注册的池，否则回落默认池。
//
// 未知分片（既没注册、也没有默认池）返回 ErrUnknownShard 而不是 nil——
// 返回 nil 会让调用方在下一行 panic，错误信息就丢干净了。
func (p *PoolSet) Pool(sh Shard) (*pgxpool.Pool, error) {
	norm, err := Parse(string(sh))
	if err != nil {
		return nil, fmt.Errorf("查找分片 %q 的连接池失败: %w", sh, err)
	}

	p.mu.RLock()
	pool, ok := p.m[norm]
	def := p.def
	p.mu.RUnlock()

	if ok {
		return pool, nil
	}
	if def != nil {
		return def, nil
	}
	return nil, fmt.Errorf("%w: 分片 %q", ErrUnknownShard, norm)
}

// PoolOfID 按 ID 本身的形态选池。
//
//	形态 B（ses_/msg_/led_ …）→ ID 内嵌分片号，零查表，直接选该分片池
//	形态 A（tnt_/usr_/pln_ …）→ 目录型实体无分片位，走默认池
//
// 非法 ID 直接报错：让它在这一步失败，而不是带着一个可疑的 ID
// 落到默认库、写进去之后才发现前缀打错了。
func (p *PoolSet) PoolOfID(id idgen.ID) (*pgxpool.Pool, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("%w: ID 为空，无法确定目标分片", ErrNoShard)
	}
	if err := idgen.Validate(string(id)); err != nil {
		return nil, fmt.Errorf("无法为 ID %q 选池: %w", id, err)
	}

	if raw := idgen.ShardOf(id); raw != "" {
		return p.Pool(Shard(raw))
	}

	// 形态 A：目录型实体。
	if def := p.defaultPool(); def != nil {
		return def, nil
	}
	return nil, fmt.Errorf("%w: 目录型 ID %q 需要默认池，但未配置", ErrUnknownShard, id)
}

// Shards 返回所有单独注册过的分片号（升序），供启动日志与健康检查使用。
func (p *PoolSet) Shards() []Shard {
	p.mu.RLock()
	out := make([]Shard, 0, len(p.m))
	for k := range p.m {
		out = append(out, k)
	}
	p.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Ping 逐个校验默认池与各分片池的连通性。
//
// 启动时调用一次：某个分片库连不上，就别开始转发流量。
// 单库阶段这里只有一次 Ping，不构成负担。
func (p *PoolSet) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	def := p.defaultPool()

	p.mu.RLock()
	registered := make(map[Shard]*pgxpool.Pool, len(p.m))
	for k, v := range p.m {
		registered[k] = v
	}
	p.mu.RUnlock()

	if def != nil {
		if err := def.Ping(pingCtx); err != nil {
			return fmt.Errorf("默认池连通性检查失败: %w", err)
		}
	}
	for sh, pool := range registered {
		if pool == def {
			continue
		}
		if err := pool.Ping(pingCtx); err != nil {
			return fmt.Errorf("分片 %q 的连接池连通性检查失败: %w", sh, err)
		}
	}
	if def == nil && len(registered) == 0 {
		return fmt.Errorf("%w: 没有任何连接池", ErrUnknownShard)
	}
	return nil
}

// Close 关闭所有连接池。默认池与已注册池如果是同一个对象，只关一次。
func (p *PoolSet) Close() {
	p.mu.Lock()
	def := p.def
	pools := make([]*pgxpool.Pool, 0, len(p.m))
	seen := make(map[*pgxpool.Pool]bool, len(p.m)+1)
	if def != nil {
		seen[def] = true
		pools = append(pools, def)
	}
	for _, v := range p.m {
		if v == nil || seen[v] {
			continue
		}
		seen[v] = true
		pools = append(pools, v)
	}
	p.m = make(map[Shard]*pgxpool.Pool)
	p.def = nil
	p.mu.Unlock()

	for _, pool := range pools {
		pool.Close()
	}
}

// OpenPool 按 DSN 建一个懒连接的池。
//
// 与 internal/store.Open 的差别是这里不做 Ping：池是懒的，
// 真正要验证连通性请显式调 PoolSet.Ping。这样批量建池时
// 不会因为一个分片库慢而把启动拖成 N 次超时叠加。
func OpenPool(dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析分片 DSN 失败: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("创建分片连接池失败: %w", err)
	}
	return pool, nil
}

// Open 按配置批量建池：defaultDSN 建默认池，shardDSNs 按分片号建专属池。
//
// 任何一个建池失败都会把已建的池关掉再返回错误，
// 不留半开的连接给运维排查。
func Open(defaultDSN string, shardDSNs map[string]string, maxConns int32) (*PoolSet, error) {
	var def *pgxpool.Pool
	built := make([]*pgxpool.Pool, 0, len(shardDSNs)+1)

	closeAll := func() {
		for _, pool := range built {
			pool.Close()
		}
	}

	if defaultDSN != "" {
		pool, err := OpenPool(defaultDSN, maxConns)
		if err != nil {
			return nil, err
		}
		def = pool
		built = append(built, pool)
	}

	set := NewPoolSet(def)
	for rawShard, dsn := range shardDSNs {
		if dsn == "" {
			closeAll()
			return nil, fmt.Errorf("分片 %q 的 DSN 为空", rawShard)
		}
		sh, err := Parse(rawShard)
		if err != nil {
			closeAll()
			return nil, err
		}
		pool, err := OpenPool(dsn, maxConns)
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("分片 %q: %w", rawShard, err)
		}
		set.m[sh] = pool
		built = append(built, pool)
	}

	if def == nil && len(set.m) == 0 {
		return nil, fmt.Errorf("%w: 默认 DSN 与分片 DSN 都为空", ErrUnknownShard)
	}
	return set, nil
}
