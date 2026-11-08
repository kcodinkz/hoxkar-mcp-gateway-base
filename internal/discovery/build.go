package discovery

import (
	"context"
	"fmt"
	"time"
)

// Options 是构造发现层的全部输入。
//
// 刻意不直接依赖 config.Config：发现层只需要这几个值，
// 传结构体会让这个包反向依赖配置层，测试也要跟着造一个 Config。
type Options struct {
	// Provider 取 "static" 或 "rnacos"
	Provider string
	// CacheTTL 是实例列表快照的有效期
	CacheTTL time.Duration
	// FailOpen 为 true 时，rnacos 故障会退化为使用静态兜底 / 过期快照
	FailOpen bool
	// StaticEndpoints 是 host:port 列表，static 模式的唯一来源，
	// 同时是 rnacos 模式下的兜底
	StaticEndpoints []string

	RnacosAddr        string
	RnacosNamespace   string
	RnacosGroup       string
	RnacosUsername    string
	RnacosPassword    string
	RnacosTimeout     time.Duration
	RnacosHealthyOnly bool
}

// Bundle 把解析器、选择器和统计一起交给调用方。
type Bundle struct {
	Resolver Resolver
	Selector *Selector
	Stats    *Stats
}

// Build 按 Options 组装发现层。
//
// 设计要点（服务发现）：
//   - 任何情况下都必须能返回一个可用的 Resolver，不允许因为 r-nacos
//     没起就拒绝启动。rnacos 不可用只会在 Resolve 时报错，
//     且 fail_open 时会退到静态兜底。
//   - static provider 不需要缓存包装，它本来就是内存查表。
func Build(opts Options) (*Bundle, error) {
	stats := NewStats()
	selector := NewSelector()

	static, err := NewStatic(opts.StaticEndpoints)
	if err != nil {
		return nil, err
	}

	provider := opts.Provider
	if provider == "" {
		provider = "static"
	}

	switch provider {
	case "static":
		if len(opts.StaticEndpoints) == 0 {
			return nil, fmt.Errorf("discovery.provider=static 时必须配置 discovery.static_endpoints")
		}
		return &Bundle{Resolver: static, Selector: selector, Stats: stats}, nil

	case "rnacos":
		remote := NewRnacos(
			opts.RnacosAddr,
			opts.RnacosNamespace,
			opts.RnacosGroup,
			opts.RnacosUsername,
			opts.RnacosPassword,
			opts.RnacosTimeout,
			opts.RnacosHealthyOnly,
		)

		// 组合顺序：rnacos --(失败时)--> static
		//
		// 注意不能用 cachingResolver 直接包 fallback：
		// 那样缓存键会混在一起，static 的结果会覆盖 rnacos 的结果。
		// 这里用一个显式的 fallbackResolver 表达「主/备」语义。
		var primary Resolver = newCachingResolver(remote, opts.CacheTTL, opts.FailOpen)

		// 关键：必须把 Lister 能力「透出」到最终的 Resolver 上。
		// 否则调用方对 Bundle.Resolver 做 `.(Lister)` 断言会永远失败，
		// 命名空间自动同步器等于没接上（见 plans/rnacos知识库.md §9）。
		if l, ok := any(remote).(Lister); ok {
			primary = listerPassthrough{Resolver: primary, Lister: l}
		}

		if !opts.FailOpen {
			return &Bundle{Resolver: primary, Selector: selector, Stats: stats}, nil
		}
		if len(opts.StaticEndpoints) == 0 {
			return nil, fmt.Errorf(
				"discovery.provider=rnacos 且 fail_open=true 时必须配置 discovery.static_endpoints，" +
					"否则 r-nacos 不可用时网关无处转发（见服务发现）")
		}

		fb := &fallbackResolver{primary: primary, backup: static, stats: stats}
		var outer Resolver = fb
		// static 兜底没有「服务清单」概念，所以 List 只在主源支持时透出。
		if l, ok := primary.(Lister); ok {
			outer = listerPassthrough{Resolver: fb, Lister: l}
		}
		return &Bundle{Resolver: outer, Selector: selector, Stats: stats}, nil

	default:
		return nil, fmt.Errorf("discovery.provider 取值非法: %q，可选 static 或 rnacos", provider)
	}
}

// listerPassthrough 把 Resolver 与 Lister 两个能力组合到同一个值上。
//
// 为什么不用「给 cachingResolver / fallbackResolver 直接加 List 方法」：
// 那样无论内层是否支持枚举，包装类型都会满足 Lister，
// 对 Bundle.Resolver 做类型断言将永远成立（假阳性），
// 调用 List 时才在运行期发现没人实现，错误被推迟且难以定位。
// 用组合类型则「内层支持才透出」，断言结果即为真实能力。
type listerPassthrough struct {
	Resolver // Resolve / Provider / Close
	Lister   // List / ListGroup
}

// fallbackResolver 是「主发现源 + 静态兜底」的组合。
//
// 语义：优先用 primary 的结果；primary 报错或返回空，则退到 backup，
// 并累加 FallbackTotal 供告警使用。
type fallbackResolver struct {
	primary Resolver
	backup  Resolver
	stats   *Stats
}

func (f *fallbackResolver) Resolve(ctx context.Context, service string) ([]Instance, error) {
	insts, err := f.primary.Resolve(ctx, service)
	if err == nil && len(insts) > 0 {
		return insts, nil
	}

	f.stats.FallbackTotal.Add(1)
	f.stats.ResolveFailed.Add(1)

	backup, bErr := f.backup.Resolve(ctx, service)
	if bErr != nil {
		// 主备都失败：把主因放前面，便于定位
		return nil, fmt.Errorf("发现层主源失败（%v），静态兜底亦失败（%v）", err, bErr)
	}
	return backup, nil
}

func (f *fallbackResolver) Provider() string {
	return f.primary.Provider() + "+static-fallback"
}

func (f *fallbackResolver) Close() error {
	err1 := f.primary.Close()
	err2 := f.backup.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
