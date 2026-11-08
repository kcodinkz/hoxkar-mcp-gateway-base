package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// PermissionStore 是 RBAC 裁决需要的存储能力。
//
// 与 KeyStore 分开声明而不是合成一个大接口：鉴权链路（每次请求都要走）
// 与权限链路（只在建连与 tools/call 时走）的调用频率差一个数量级，
// 拆开以后谁也不需要为对方的能力买单，测试替身也更小。
type PermissionStore interface {
	CanUseNamespace(ctx context.Context, keyID, namespaceID idgen.ID) (bool, error)
	CanUseTool(ctx context.Context, keyID, namespaceID idgen.ID, tool string) (bool, error)
	ListPermissions(ctx context.Context, keyID idgen.ID) ([]store.Permission, error)
}

// NamespaceLookup 是解析「slug → 命名空间行」的能力。
type NamespaceLookup interface {
	GetNamespaceBySlug(ctx context.Context, slug string) (*store.Namespace, error)
	GetNamespaceByID(ctx context.Context, id idgen.ID) (*store.Namespace, error)
}

// RBACOptions 是裁决层的可调参数。
type RBACOptions struct {
	// CacheTTL 是权限判定结果的缓存时长，默认 60 秒。
	//
	// 比 Key 缓存长是合理的：改权限是低频管理动作，而权限查询发生在
	// 每次建连与每次 tools/call 上。管理端改完权限会主动清缓存，
	// 所以 TTL 只是兜底，不是生效延迟。
	CacheTTL time.Duration
	// MaxCacheEntries 缓存上限，默认 16384。
	MaxCacheEntries int
	// Now 便于测试注入时钟。
	Now func() time.Time
}

func (o *RBACOptions) applyDefaults() {
	if o.CacheTTL <= 0 {
		o.CacheTTL = 60 * time.Second
	}
	if o.MaxCacheEntries <= 0 {
		o.MaxCacheEntries = 16384
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Authorizer 负责「这个 Key 能不能碰这个命名空间 / 这个工具」。
//
// 分层语义（与 store 层的 key_permissions 表一致）：
//
//	命名空间级：key_permissions 里存在该 (key, namespace) 任一行即算有权
//	工具级    ：再要求 tool_name 等于目标工具，或为 '*' 通配
//
// 两级是「与」的关系：命名空间有权 + 工具有权，才放行。因此
// 「授了 blender 但只给了 render 工具」这种配置，访问 blender/sculpt 会被拒。
type Authorizer struct {
	perms PermissionStore
	ns    NamespaceLookup
	opts  RBACOptions

	// 两个缓存分开存：命名空间判定落在建连时，工具判定落在每次
	// tools/call 时，生命周期与命中率都不同，混在一起会互相污染淘汰。
	nsCache   *Cache
	toolCache *Cache
}

// NewAuthorizer 构造裁决器。
func NewAuthorizer(p PermissionStore, ns NamespaceLookup, opts RBACOptions) *Authorizer {
	if p == nil || ns == nil {
		panic("auth.NewAuthorizer: perms 与 ns 都不能为 nil")
	}
	opts.applyDefaults()
	return &Authorizer{
		perms:     p,
		ns:        ns,
		opts:      opts,
		nsCache:   newCache(opts.CacheTTL, opts.MaxCacheEntries, opts.Now),
		toolCache: newCache(opts.CacheTTL, opts.MaxCacheEntries, opts.Now),
	}
}

// ResolveNamespace 把 slug 解析成命名空间行，并顺手拒绝不可用的命名空间。
//
// 三类拒绝都在这里完成，因为它们对客户端的意义完全相同：
// 「这个地址不可用」。区分「不存在」与「被禁用」只会给探测者提供情报。
func (a *Authorizer) ResolveNamespace(ctx context.Context, slug string) (*store.Namespace, error) {
	slug = NormalizeSlug(slug)
	if slug == "" {
		return nil, fmt.Errorf("%w: 命名空间为空", ErrForbidden)
	}

	ns, err := a.ns.GetNamespaceBySlug(ctx, slug)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("%w: 命名空间 %q 不存在", ErrForbidden, slug)
	}
	if err != nil {
		return nil, fmt.Errorf("查询命名空间 %q 失败: %w", slug, err)
	}

	if !ns.Enabled {
		return nil, fmt.Errorf("%w: 命名空间 %q 已下线", ErrForbidden, slug)
	}
	return ns, nil
}

// AllowsNamespace 判断 Key 能否使用该命名空间。
func (a *Authorizer) AllowsNamespace(ctx context.Context, p *Principal, ns *store.Namespace) (bool, error) {
	if p == nil || ns == nil {
		return false, ErrForbidden
	}

	key := nsCacheKey(p.KeyID, ns.ID)
	if v, err := a.nsCache.Get(key); err == nil {
		return v.(bool), nil
	} else if !errors.Is(err, errCacheMiss) {
		return false, err
	}

	ok, err := a.perms.CanUseNamespace(ctx, p.KeyID, ns.ID)
	if err != nil {
		return false, err
	}
	a.nsCache.Set(key, ok)
	return ok, nil
}

// AllowsTool 判断 Key 能否在该命名空间下调用某工具。
//
// 先过命名空间级，再过工具级：命名空间无权时直接返回，省掉一次查询。
// 这一步不能反过来做，否则「授了 tool 却漏授 namespace」这类脏数据
// 会让工具级检查通过、命名空间级失败，最终错误信息指向错误的方向。
func (a *Authorizer) AllowsTool(ctx context.Context, p *Principal, ns *store.Namespace, tool string) (bool, error) {
	ok, err := a.AllowsNamespace(ctx, p, ns)
	if err != nil || !ok {
		return false, err
	}
	if tool == "" {
		// 没有工具名意味着这是一次命名空间级操作（initialize / tools/list），
		// 命名空间有权就够了。
		return true, nil
	}

	key := toolCacheKey(p.KeyID, ns.ID, tool)
	if v, err := a.toolCache.Get(key); err == nil {
		return v.(bool), nil
	} else if !errors.Is(err, errCacheMiss) {
		return false, err
	}

	ok, err = a.perms.CanUseTool(ctx, p.KeyID, ns.ID, tool)
	if err != nil {
		return false, err
	}
	a.toolCache.Set(key, ok)
	return ok, nil
}

// VisibleTools 返回某个 Key 在某命名空间下可用的工具名列表。
//
// 用途有二：
//  1. tools/list 过滤 —— 客户端不该看到自己调不动的工具，
//     否则模型会规划出一条注定 403 的调用链；
//  2. 计费前置校验 —— 调用不可见工具应被当成越权而非正常计费。
//
// 返回 nil 表示「不限制」（该 Key 在此命名空间下拥有 '*'）。
func (a *Authorizer) VisibleTools(ctx context.Context, p *Principal, ns *store.Namespace) ([]string, error) {
	perms, err := a.perms.ListPermissions(ctx, p.KeyID)
	if err != nil {
		return nil, err
	}

	var out []string
	for _, perm := range perms {
		if perm.NamespaceID != ns.ID {
			continue
		}
		if perm.ToolName == "*" {
			// 通配优先：直接说「不限制」，不必再把其余逐个列出
			return nil, nil
		}
		out = append(out, perm.ToolName)
	}
	if out == nil {
		// nil 与空切片在这里语义不同：nil = 不限制，空 = 一个都不许。
		// 返回空切片让调用方能把 tools/list 过滤成空列表，而不是误判成全放行。
		out = []string{}
	}
	return out, nil
}

// InvalidateKey 清空某 Key 的全部权限缓存。
//
// 管理端通过 PUT /admin/v1/keys/{id}/permissions 改完权限后必须调用。
// 缓存键形如「keyID|nsID[|tool]」，因此可以按前缀精确清理，
// 不必把其他人的缓存一起清掉。
func (a *Authorizer) InvalidateKey(keyID idgen.ID) {
	prefix := string(keyID) + "|"
	a.nsCache.DeleteByKeyPrefix(prefix)
	a.toolCache.DeleteByKeyPrefix(prefix)
}

// Reset 清空全部权限缓存。
func (a *Authorizer) Reset() {
	a.nsCache.Reset()
	a.toolCache.Reset()
}

func nsCacheKey(keyID, nsID idgen.ID) string {
	return string(keyID) + "|" + string(nsID)
}

func toolCacheKey(keyID, nsID idgen.ID, tool string) string {
	return string(keyID) + "|" + string(nsID) + "|" + tool
}

// toolCallMethods 列出「工具调用」这一动作在历史协议中的各种写法。
//
// 写成表而不是散落的 if：MCP 的 method 命名存在 "tools/call" 与
// "mcp/tools/call" 两种历史写法，协议演进时新增别名只需改这张表，
// 裁决逻辑一行不动。
var toolCallMethods = map[string]bool{
	"tools/call":     true,
	"mcp/tools/call": true,
}

// ToolsCallMethod 判断一个 JSON-RPC method 是否为工具调用。
func ToolsCallMethod(method string) bool {
	return toolCallMethods[method]
}
