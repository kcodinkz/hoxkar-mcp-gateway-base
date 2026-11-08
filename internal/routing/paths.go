package routing

import (
	"strings"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
)

// Endpoint 是一个请求命中的路由端点。
type Endpoint int

const (
	// EndpointNone 表示这条路径与 MCP 端点无关，调用方应继续按自己的规则处理
	// （例如 404，或交给已被 ServeHTTP 提前分派走的控制面挂载点）。
	EndpointNone Endpoint = iota
	// EndpointSSE 是建连端点。客户端在这里拿到 sessionID 与后续投递地址。
	EndpointSSE
	// EndpointMessage 是消息投递端点，客户端 POST 一条 JSON-RPC 消息上来。
	EndpointMessage
)

func (e Endpoint) String() string {
	switch e {
	case EndpointSSE:
		return "sse"
	case EndpointMessage:
		return "message"
	default:
		return "none"
	}
}

// Target 是路径解析结果。
//
// Slug 为空有两种来源，必须靠 Scoped 区分：
//   - 客户端用的是无前缀写法（/sse）—— 允许，按默认命名空间回落；
//   - 客户端写了前缀但段内是空白 —— 不允许，按 404 处理。
//
// 把两者混在一起会让 /sse/%20 悄悄落进默认命名空间，
// 于是「路径写错了」被伪装成「一切正常，只是流量记到了别人头上」。
type Target struct {
	Endpoint Endpoint
	Slug     string
	// Scoped 表示 Slug 是否来自路径里的命名空间段。
	Scoped bool
}

// ParsePath 把请求路径解析成路由目标。
//
// sseBase 与 messageBase 是配置里的端点路径（形如 /sse、/messages），
// 由调用方从 config 传入，路由层不硬编码这些值——否则改配置就得改代码。
//
// 只接受「精确匹配」与「恰好一段后缀」，不做前缀模糊匹配：
// /sse/blender/render 这种多段路径不会被当成 slug=blender，
// 因为命名空间 slug 里本来就不允许出现斜杠，猜它只会掩盖客户端的 bug。
func ParsePath(sseBase, messageBase, path string) Target {
	if slug, ok := matchEndpoint(path, sseBase); ok {
		return Target{Endpoint: EndpointSSE, Slug: slug, Scoped: slug != ""}
	}
	if slug, ok := matchEndpoint(path, messageBase); ok {
		return Target{Endpoint: EndpointMessage, Slug: slug, Scoped: slug != ""}
	}
	return Target{Endpoint: EndpointNone}
}

// matchEndpoint 判断 path 是否形如 base 或 base/{oneSegment}。
//
// 返回 ok=false 表示这条路径不属于该端点；返回 slug="" 且 ok=true
// 表示无前缀写法。
func matchEndpoint(path, base string) (slug string, ok bool) {
	base = strings.TrimSuffix(base, "/")
	if base == "" {
		return "", false
	}
	if path == base {
		return "", true
	}

	rest, found := strings.CutPrefix(path, base+"/")
	if !found || rest == "" || strings.Contains(rest, "/") {
		return "", false
	}

	// slug 大小写与空白在这里归一，但归一后为空说明路径其实是个空段，
	// 不能被当作无前缀写法放行。
	slug = auth.NormalizeSlug(rest)
	if slug == "" {
		return "", false
	}
	return slug, true
}
