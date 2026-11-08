package routing

// 本文件把 stdio 与 http 两种 transport 合成一个 BackendFactory。
//
// 为什么需要它：命名空间的传输方式存在数据库里、随时可由管理员改动，
// 而 Router 只认一个 factory。若把选择逻辑丢给装配层写 if/else，
// 每加一种传输方式（未来可能是 websocket / grpc）都要改 main.go；
// 放在这里，装配层永远只需要一行 NewTransportFactory(...)。

import (
	"context"
	"fmt"
	"strings"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// TransportFactory 按 ns.Transport 把请求分派给对应实现。
//
// 两个子工厂都可以为 nil：只为 stdio 调试装配时不配 HTTP 工厂是合法的，
// 反之亦然。缺失的那个传输方式在 Open 时返回 ErrNoBackend，
// 并明确说出「本网关未启用该传输方式」，而不是一个难以定位的空指针。
type TransportFactory struct {
	Stdio *StdioFactory
	HTTP  *HTTPFactory
}

var _ BackendFactory = (*TransportFactory)(nil)

// Open 按传输方式分派。
//
// transport 是空值时按 http 处理：迁移里 http 是远端服务的主流形态，
// 而空值只可能来自手工插库或极早期的行，把它当成集群主路径比当成
// 「本机子进程」安全得多 —— 后者会在网关机器上执行一个空命令。
func (f *TransportFactory) Open(ctx context.Context, ns *store.Namespace) (Backend, error) {
	if ns == nil {
		return nil, fmt.Errorf("%w: 命名空间为空", ErrNoBackend)
	}

	kind := strings.ToLower(strings.TrimSpace(ns.Transport))
	if kind == "" {
		kind = store.TransportHTTP
	}

	// 子工厂各自有严格的传输方式校验（认死自己那一种），所以归一化后的
	// 空值必须真的写进副本再交下去，否则「空 transport 按 http 处理」
	// 这条保证会在下一层被推翻。
	target := ns
	if target.Transport != kind {
		clone := *ns
		clone.Transport = kind
		target = &clone
	}

	switch kind {
	case store.TransportHTTP:
		if f.HTTP == nil {
			return nil, fmt.Errorf("%w: 命名空间 %q 是 http 传输，但网关未启用 HTTP 转发",
				ErrNoBackend, ns.Slug)
		}
		return f.HTTP.Open(ctx, target)
	case store.TransportStdio:
		if f.Stdio == nil {
			return nil, fmt.Errorf("%w: 命名空间 %q 是 stdio 传输，但网关未启用 stdio 进程池",
				ErrNoBackend, ns.Slug)
		}
		return f.Stdio.Open(ctx, target)
	default:
		return nil, fmt.Errorf("%w: 命名空间 %q 的传输方式 %q 不受支持",
			ErrNoBackend, ns.Slug, ns.Transport)
	}
}

// Close 关停两个子工厂，把错误合并后返回。
func (f *TransportFactory) Close() error {
	var first error
	if f.Stdio != nil {
		if err := f.Stdio.Close(); err != nil && first == nil {
			first = err
		}
	}
	if f.HTTP != nil {
		if err := f.HTTP.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// LiveStdioProcesses 返回 stdio 子进程总数，供关停日志与运维观察。
func (f *TransportFactory) LiveStdioProcesses() int {
	if f.Stdio == nil {
		return 0
	}
	total := 0
	for _, n := range f.Stdio.Stats() {
		total += n
	}
	return total
}
