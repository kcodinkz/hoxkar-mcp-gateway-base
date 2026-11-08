package shard

import (
	"context"
	"fmt"
	"sync"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// Sharder 回答「这个租户的数据在哪个分片」。
//
// 从第一天就是接口：将来若某个租户大到单分片放不下，把实现从
// 「按租户」换成「按租户 + 命名空间」即可，调用方代码不动。
// 反过来，如果一开始就写几个散落的哈希函数，那次改造会重写整条链路。
type Sharder interface {
	// ShardOf 返回租户的粘性分片。首次调用可能回源，之后命中缓存。
	ShardOf(ctx context.Context, tenantID idgen.ID) (Shard, error)
}

// Source 是 Directory 的数据来源，由 internal/store 实现。
//
// 抽成接口而不是直接依赖 store：一是避免 shard ⇄ store 的循环依赖
// （store 要按分片选池，就得用 shard），二是让缓存行为能脱离数据库测试。
type Source interface {
	// TenantShard 返回 tenants.shard 的原始值（未规范化）。
	// 租户不存在时返回什么错误由实现决定，Directory 原样上抛。
	TenantShard(ctx context.Context, tenantID idgen.ID) (string, error)
	// TenantShards 返回全部租户的分片，供启动预热。
	TenantShards(ctx context.Context) (map[idgen.ID]string, error)
}

// Directory 是「租户 → 分片」在本进程内的唯一真相。
//
// 启动时用 Warm 全量加载；之后未命中才回源，命中路径只加读锁。
// 租户的升降级不会改分片（租户与分片），所以除了新建租户，这张表几乎不变。
type Directory struct {
	src Source

	mu sync.RWMutex
	m  map[idgen.ID]Shard

	// loadMu 只保护「缓存未命中」这一段：串行化回源，
	// 避免同一租户的并发首查各打一次库（缓存击穿）。
	// 命中路径完全绕开它，所以不会成为热点锁。
	loadMu sync.Mutex
}

// NewDirectory 创建一个空目录。src 可为 nil，此时只能靠 Warm/Put 填充。
func NewDirectory(src Source) *Directory {
	return &Directory{src: src, m: make(map[idgen.ID]Shard)}
}

// SetSource 替换数据源，用于启动编排阶段（数据库就绪前先建目录）。
// 已经缓存的映射不受影响；运行期不要调用。
func (d *Directory) SetSource(src Source) { d.src = src }

// Warm 全量加载所有租户的分片，启动时调用一次。
//
// 遇到任何一行分片为空或非法，整个 Warm 失败：宁可起不来，
// 也不要带着一个「以为租户在 03、实际在 Z7」的目录开始转发流量——
// 那会把用户数据写到错误的分片里，事后只能靠迁移弥补。
func (d *Directory) Warm(ctx context.Context) error {
	if d.src == nil {
		return ErrNoSource
	}
	all, err := d.src.TenantShards(ctx)
	if err != nil {
		return fmt.Errorf("全量加载租户分片失败: %w", err)
	}
	next := make(map[idgen.ID]Shard, len(all))
	for tid, raw := range all {
		sh, err := fromColumn(tid, raw)
		if err != nil {
			return fmt.Errorf("预热中断: %w", err)
		}
		next[tid] = sh
	}

	d.mu.Lock()
	d.m = next
	d.mu.Unlock()
	return nil
}

// ShardOf 返回租户的粘性分片。
func (d *Directory) ShardOf(ctx context.Context, tenantID idgen.ID) (Shard, error) {
	if tenantID == idgen.Zero {
		return "", ErrNoTenant
	}
	if sh, ok := d.lookup(tenantID); ok {
		return sh, nil
	}

	d.loadMu.Lock()
	defer d.loadMu.Unlock()

	// 二次检查：等锁期间别的协程可能已经回源成功。
	if sh, ok := d.lookup(tenantID); ok {
		return sh, nil
	}
	if d.src == nil {
		return "", ErrNoSource
	}

	raw, err := d.src.TenantShard(ctx, tenantID)
	if err != nil {
		return "", fmt.Errorf("查询租户 %q 的分片失败: %w", tenantID, err)
	}
	sh, err := fromColumn(tenantID, raw)
	if err != nil {
		return "", err
	}

	d.mu.Lock()
	d.m[tenantID] = sh
	d.mu.Unlock()
	return sh, nil
}

// Put 登记一个租户的分片。租户创建流程在写入 tenants 行之后调用。
//
// 分片号先校验再落缓存：这里放进去的错值会一直用到进程重启。
func (d *Directory) Put(tenantID idgen.ID, sh Shard) error {
	if tenantID == idgen.Zero {
		return ErrNoTenant
	}
	norm, err := Parse(string(sh))
	if err != nil {
		return fmt.Errorf("登记租户 %q 的分片失败: %w", tenantID, err)
	}
	d.mu.Lock()
	d.m[tenantID] = norm
	d.mu.Unlock()
	return nil
}

// Forget 丢弃某个租户的缓存，下次 ShardOf 会重新回源。
// 用于租户被删除，或运维手工改了 tenants.shard 之后的纠偏。
func (d *Directory) Forget(tenantID idgen.ID) {
	d.mu.Lock()
	delete(d.m, tenantID)
	d.mu.Unlock()
}

// Len 返回已缓存的租户数量，供健康检查与调试用。
func (d *Directory) Len() int {
	d.mu.RLock()
	n := len(d.m)
	d.mu.RUnlock()
	return n
}

// Snapshot 返回缓存副本，供管理端展示与排障（调用方拿到的是拷贝）。
func (d *Directory) Snapshot() map[idgen.ID]Shard {
	d.mu.RLock()
	out := make(map[idgen.ID]Shard, len(d.m))
	for k, v := range d.m {
		out[k] = v
	}
	d.mu.RUnlock()
	return out
}

func (d *Directory) lookup(tenantID idgen.ID) (Shard, bool) {
	d.mu.RLock()
	sh, ok := d.m[tenantID]
	d.mu.RUnlock()
	return sh, ok
}

// 编译期断言：Directory 就是 Sharder 的实现。
var _ Sharder = (*Directory)(nil)
