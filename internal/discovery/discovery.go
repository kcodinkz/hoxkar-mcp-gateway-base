// Package discovery 负责「服务名 → 健康实例地址列表」的解析。
//
// 服务发现：
//   - 注册者是 Rust 后端，不是网关。网关只做发现，不注册、不心跳。
//   - r-nacos 只是增强，不是单点依赖。fail_open 时必须能用 static_endpoints 兜底。
//   - 发现结果必须 TTL 缓存 + 原子快照替换，禁止每个请求同步打注册中心。
//   - 返回的实例地址必须是内网段，公网地址一律丢弃（防注册表投毒）。
//   - 网关不做并发调优：这里就是「一个 map + 一个定时器」，不要写成 RPC 框架。
package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Instance 是一个可转发的上游实例。
type Instance struct {
	IP       string
	Port     int
	Weight   float64
	Healthy  bool
	Metadata map[string]string
}

// Addr 返回 host:port 形式，便于直接拼 URL。
func (i Instance) Addr() string {
	return net.JoinHostPort(i.IP, fmt.Sprintf("%d", i.Port))
}

// URL 返回完整的上游地址，endpointPath 需以 / 开头。
func (i Instance) URL(scheme, endpointPath string) string {
	if scheme == "" {
		scheme = "http"
	}
	if endpointPath == "" {
		endpointPath = "/mcp"
	}
	if !strings.HasPrefix(endpointPath, "/") {
		endpointPath = "/" + endpointPath
	}
	return fmt.Sprintf("%s://%s%s", scheme, i.Addr(), endpointPath)
}

// Resolver 是发现层的唯一接口。static 与 rnacos 都实现它。
//
// 约定：Resolve 必须快速返回。实现内部自己维护缓存与后台刷新，
// 不允许把网络 IO 直接暴露给请求路径。
type Resolver interface {
	// Resolve 返回某个服务的健康实例快照。调用方不得修改返回的切片。
	Resolve(ctx context.Context, service string) ([]Instance, error)
	// Provider 返回实现名，用于日志与 /healthz 展示。
	Provider() string
	// Close 释放后台资源。
	Close() error
}

// Lister 是「枚举注册中心里有哪些服务」的可选能力。
//
// 为什么单独定义而不并进 Resolver：static 实现没有「服务清单」这个概念，
// 它只有一组写死的地址。把 List 塞进 Resolver 会逼着 static 返回一个
// 假服务名，反而污染语义。需要枚举的调用方用类型断言拿这个接口：
//
//	if l, ok := resolver.(discovery.Lister); ok { names, err := l.List(ctx) }
//
// 这是命名空间动态增减（服务发现扩展）的基础：网关靠 List 知道注册中心
// 现在有哪些 MCP 服务，从而自动补建 / 下线命名空间，而不是靠 config.yaml 写死。
type Lister interface {
	// List 返回「当前有可用实例」的服务名（不含实例地址）。调用方不得修改返回的切片。
	//
	// 语义是「本刻可用」而不是「曾经注册过」：注册中心通常不删除服务条目，
	// 实例全部注销后只留下一个没有实例的空壳。若 List 回空壳，同步器就永远
	// 观察不到服务消失，注册中心同步的软下线永远不会触发。
	// 因此实现方必须用「实例数/健康实例数」过滤，而不是直接回服务名清单；
	// 阈值应与 Resolve 的 healthyOnly 保持一致，避免两个口径互相矛盾。
	List(ctx context.Context) ([]string, error)
	// ListGroup 返回枚举结果所属的分组，用于落库到 discovery_group。
	ListGroup() string
}

// ErrNoInstances 表示服务存在但当前没有健康实例。
var ErrNoInstances = errors.New("发现层未返回任何健康实例")

// ---------------------------------------------------------------------------
// 内网地址校验：服务发现规则 5
// ---------------------------------------------------------------------------

// 允许的私有网段。localhost 也放行，便于单机联调。
var allowedNets = mustParseCIDRs(
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"127.0.0.0/8",
	"::1/128",
	"fc00::/7",
)

func mustParseCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic("discovery: 内置网段配置错误: " + c)
		}
		out = append(out, n)
	}
	return out
}

// IsInternalAddr 判断地址是否落在允许的内网段内。
// 传入主机名（非 IP）时返回 false，因为无法静态判定，直接拒绝更安全。
func IsInternalAddr(host string) bool {
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return false
	}
	for _, n := range allowedNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// sanitize 过滤掉不健康的、公网的、端口非法的实例，并去重。
// 静默丢弃是刻意的：投毒实例不应该让整个发现流程失败。
func sanitize(in []Instance) ([]Instance, int) {
	seen := make(map[string]struct{}, len(in))
	out := make([]Instance, 0, len(in))
	dropped := 0

	for _, it := range in {
		if !it.Healthy || it.Port <= 0 || it.Port > 65535 {
			dropped++
			continue
		}
		if !IsInternalAddr(it.IP) {
			dropped++
			continue
		}
		key := it.Addr()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, it)
	}

	// 稳定排序，保证 round-robin 在多实例之间行为可预测
	sort.Slice(out, func(i, j int) bool { return out[i].Addr() < out[j].Addr() })
	return out, dropped
}

// ---------------------------------------------------------------------------
// static provider：默认实现，从配置读取固定地址
// ---------------------------------------------------------------------------

// StaticResolver 把一组写死的 host:port 当作该服务的全部实例。
// 它永远健康、永远可用，是 fail_open 的最终兜底。
type StaticResolver struct {
	mu    sync.RWMutex
	addrs []Instance
}

// NewStatic 解析 "host:port" 列表。非法项直接报错，因为这是配置错误，
// 应当在启动阶段暴露，而不是运行期静默少一个实例。
func NewStatic(endpoints []string) (*StaticResolver, error) {
	r := &StaticResolver{}
	if err := r.SetEndpoints(endpoints); err != nil {
		return nil, err
	}
	return r, nil
}

// SetEndpoints 用于热更新兜底列表。
func (r *StaticResolver) SetEndpoints(endpoints []string) error {
	insts := make([]Instance, 0, len(endpoints))
	for _, ep := range endpoints {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}
		host, portStr, err := net.SplitHostPort(ep)
		if err != nil {
			return fmt.Errorf("discovery.static_endpoints 中 %q 不是 host:port 形式: %w", ep, err)
		}
		var port int
		if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil || port <= 0 || port > 65535 {
			return fmt.Errorf("discovery.static_endpoints 中 %q 端口非法", ep)
		}
		insts = append(insts, Instance{
			IP:      host,
			Port:    port,
			Weight:  1,
			Healthy: true,
		})
	}

	r.mu.Lock()
	r.addrs = insts
	r.mu.Unlock()
	return nil
}

// Resolve 忽略 service 名，始终返回同一组静态地址。
// 这是刻意的：static 模式通常是「单个已知后端」，不区分子服务。
func (r *StaticResolver) Resolve(_ context.Context, _ string) ([]Instance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.addrs) == 0 {
		return nil, ErrNoInstances
	}
	out := make([]Instance, len(r.addrs))
	copy(out, r.addrs)
	return out, nil
}

func (r *StaticResolver) Provider() string { return "static" }
func (r *StaticResolver) Close() error     { return nil }

// ---------------------------------------------------------------------------
// TTL 缓存 + 原子快照：rnacos 与未来其他实现的公共外壳
// ---------------------------------------------------------------------------

// cacheEntry 是一次成功的解析结果。
type cacheEntry struct {
	instances []Instance
	expiresAt time.Time
}

// cachingResolver 把任意「慢实现」包装成满足 Resolve 快速返回的形态。
//
// 行为：
//   - 命中且未过期 → 直接返回快照
//   - 未命中或已过期 → 同步调用一次上游（加 singleflight 式的互斥），
//     成功则替换快照，失败则按 fail_open 决定是报错还是返回过期快照
//
// 这里刻意不实现后台定时器：只有真正被访问的服务才会刷新，
// 避免命名空间很多时启动就打出几十个后台 goroutine（服务发现规则 8）。
type cachingResolver struct {
	upstream Resolver
	ttl      time.Duration
	failOpen bool

	mu    sync.Mutex
	cache map[string]*cacheEntry
}

func newCachingResolver(upstream Resolver, ttl time.Duration, failOpen bool) *cachingResolver {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return &cachingResolver{
		upstream: upstream,
		ttl:      ttl,
		failOpen: failOpen,
		cache:    make(map[string]*cacheEntry),
	}
}

func (c *cachingResolver) Resolve(ctx context.Context, service string) ([]Instance, error) {
	now := time.Now()

	c.mu.Lock()
	entry, ok := c.cache[service]
	c.mu.Unlock()

	if ok && now.Before(entry.expiresAt) {
		return entry.instances, nil
	}

	// 冷/过期：serialize 同一 service 的刷新，避免缓存击穿
	c.mu.Lock()
	defer c.mu.Unlock()

	// 双检：等锁期间可能已被其他 goroutine 刷新
	if entry, ok := c.cache[service]; ok && time.Now().Before(entry.expiresAt) {
		return entry.instances, nil
	}

	insts, err := c.upstream.Resolve(ctx, service)
	if err != nil || len(insts) == 0 {
		if c.failOpen {
			// 保留旧快照，哪怕已过期。宁可转发到可能的旧地址，也不要直接失败。
			if old, ok := c.cache[service]; ok && len(old.instances) > 0 {
				return old.instances, nil
			}
			return nil, fmt.Errorf("%w: service=%s err=%v", ErrNoInstances, service, err)
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: service=%s", ErrNoInstances, service)
	}

	c.cache[service] = &cacheEntry{instances: insts, expiresAt: time.Now().Add(c.ttl)}
	return insts, nil
}

func (c *cachingResolver) Provider() string { return c.upstream.Provider() }
func (c *cachingResolver) Close() error     { return c.upstream.Close() }

// ---------------------------------------------------------------------------
// round-robin 选择器
// ---------------------------------------------------------------------------

// Selector 从实例列表中轮询选取，索引按服务名独立维护。
type Selector struct {
	mu     sync.Mutex
	cursor map[string]uint64
}

// NewSelector 创建选择器。
func NewSelector() *Selector {
	return &Selector{cursor: make(map[string]uint64)}
}

// Pick 返回下一个实例。instances 为空时返回 false。
func (s *Selector) Pick(service string, instances []Instance) (Instance, bool) {
	if len(instances) == 0 {
		return Instance{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cursor == nil {
		s.cursor = make(map[string]uint64)
	}
	n := s.cursor[service]
	s.cursor[service] = n + 1

	return instances[n%uint64(len(instances))], true
}

// Reset 清除某个服务的游标（命名空间配置变更时调用）。
func (s *Selector) Reset(service string) {
	s.mu.Lock()
	delete(s.cursor, service)
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 计数器（原子），供 /healthz 与日志展示，避免为了统计去加锁
// ---------------------------------------------------------------------------

// Stats 记录发现层的运行情况。
type Stats struct {
	ResolveTotal  atomic.Int64
	ResolveFailed atomic.Int64
	FallbackTotal atomic.Int64
	DroppedTotal  atomic.Int64
}

// NewStats 创建一个统计对象。
func NewStats() *Stats { return &Stats{} }
