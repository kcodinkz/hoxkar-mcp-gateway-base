package main

// 多租户模式的鉴权与管理端装配。
//
// 这个文件只做「把 internal/auth 与 internal/admin 接进 HTTP 服务」这一件事，
// 刻意独立于 main.go，原因有两条：
//
//  1. 兼容性红线：不带 -config 的旧启动路径一个字节都不能变。把新装配写成
//     独立函数后，main.go 里只是多一个分支，旧路径上不会被任何新代码碰到。
//  2. 装配顺序本身就是规格：Authenticator 与 Authorizer 必须共享同一个
//     store（缓存失效才是真的失效），管理端的写操作必须在两者都存在时才有
//     意义。集中在一处，读代码的人不必在 main.go 的几百行编排里拼出这个关系。

import (
	"log"
	"net/http"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/gateway"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/admin"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/config"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/routing"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// adminPrefix 是管理端接口的挂载前缀。
const adminPrefix = "/admin/v1"

// authStack 汇总多租户模式下需要长期持有的鉴权与路由组件。
type authStack struct {
	auth *auth.Authenticator

	// rbac 由管理端与命名空间路由共享同一份实例：管理端禁用权限后，
	// 路由层的裁决必须立刻跟着变，否则会出现「权限已撤销但仍能进 /sse/{ns}」。
	rbac *auth.Authorizer

	admin *admin.Server

	// limiter 是全局默认限流器：每把 Key 的真实额度由 Key.RateLimit 决定，
	// 这里是「Key 没配额度」时的兜底值。
	limiter *auth.RateLimiter

	// transport 是命名空间后端工厂：按 ns.Transport 分出 http（r-nacos 实例）
	// 与 stdio（本地子进程）两条路。路由层只认 Backend 接口，不知道两者的差别。
	transport *routing.TransportFactory

	router *routing.Router

	// mcp 是 /sse 与 /messages 在多租户模式下的真正处理器：
	// 鉴权之后按路径解析命名空间，再把消息转发给会话绑定的后端。
	mcp *gateway.NamespacedMCP
}

// buildAuthStack 按配置构造鉴权栈与命名空间路由栈。
//
// 三个依赖（auth.KeyStore / auth.PermissionStore / admin.DirectoryStore）全部由
// 同一个 *store.Store 满足。这不是为了省事：管理端禁用一把 Key 之后要立刻让
// 鉴权缓存失效，如果两者各自持有不同的存储实例，失效就只会作用在自己那份缓存上，
// 线上表现是「已经禁用了还能继续用最多 30 秒」。Authorizer 同理 ——
// 管理端写权限、路由层读权限，必须是同一份裁决结果。
//
// bundle 是服务发现的产物，只有 http 命名空间需要它（r-nacos 实例地址）。
// 传 nil 时不装 HTTP 转发，只保留 stdio —— 这样单元测试与「纯 stdio 部署」
// 不必为了构造一个假 resolver 而存在。
func buildAuthStack(st *store.Store, cfg *config.Config, bundle *discovery.Bundle) (*authStack, error) {
	// 缓存 TTL 直接来自配置。配置没写（<= 0）时传零值即可：
	// auth.Options.applyDefaults 会补成 30s，负缓存 5s，不要在这里重复维护一套默认值。
	cacheTTL := time.Duration(cfg.Auth.KeyCacheTTLSeconds) * time.Second
	if cacheTTL < 0 {
		cacheTTL = 0
	}

	authenticator := auth.New(st, auth.Options{CacheTTL: cacheTTL})
	authorizer := auth.NewAuthorizer(st, st, auth.RBACOptions{})

	adminServer, err := admin.New(st, st, st, authenticator, authorizer, admin.Options{
		AdminToken: cfg.Auth.AdminToken,
		Logf:       func(format string, args ...any) { log.Printf(format, args...) },
	})
	if err != nil {
		return nil, err
	}

	// 窗口固定为一分钟，速率来自 auth.default_rate_limit_per_minute；
	// 每把 Key 的真实额度由 Key.RateLimit 覆盖，这里只是兜底值。
	// 速率 <= 0 时 NewRateLimiter 内部会按「不限流」处理。
	limiter := auth.NewRateLimiter(cfg.Auth.DefaultRateLimitPerMinute, time.Minute)

	// ------------------------------------------------------------------
	// 命名空间路由：这是「网关 = 集群前沿」的落地点。
	// ------------------------------------------------------------------
	logf := func(format string, args ...any) { log.Printf(format, args...) }

	transport := &routing.TransportFactory{
		Stdio: routing.NewStdioFactory(logf),
	}
	if bundle != nil && bundle.Resolver != nil {
		transport.HTTP = routing.NewHTTPFactory(bundle.Resolver, routing.HTTPFactoryOptions{
			Logf: logf,
		})
	} else {
		// 没有发现层就只留 stdio。这里不报错：配置校验已经保证
		// 多租户模式下 http 命名空间必须带 discovery_service，
		// 真出现了也会在 Open 时得到 ErrNoBackend 的明确错误，
		// 而不是在启动阶段给一个跟配置对不上的 panic。
		log.Printf("警告：服务发现未就绪，transport=http 的命名空间将不可用")
	}

	router := routing.NewRouter(authorizer, transport, routing.Options{
		SSEPath:     cfg.Server.SSEPath,
		MessagePath: cfg.Server.MessagePath,
		SessionTTL:  12 * time.Hour,
	})

	namespaced := gateway.NewNamespacedMCP(router, gateway.NamespacedMCPOptions{
		MessageBase:  cfg.Server.MessagePath,
		KeepAlive:    time.Duration(cfg.Server.KeepAliveSeconds) * time.Second,
		MaxBodyBytes: int64(cfg.Server.MaxBodyBytes),
	})

	return &authStack{
		auth:      authenticator,
		rbac:      authorizer,
		admin:     adminServer,
		limiter:   limiter,
		transport: transport,
		router:    router,
		mcp:       namespaced,
	}, nil
}

// Close 回收路由栈持有的资源，顺序是「先断流，再关工厂」。
//
// 先断流的原因：每条流退出时 handleSSE 的 defer 会调 router.Release，
// 让会话独占的 stdio 子进程走正常收尾（关管道 → 进程退出 → 记日志）。
// 若先关工厂，子进程会被直接杀掉，日志里只剩一句「进程已退出」，
// 排查「谁先断的」时就没有线索了。
//
// 随后 transport.Close 再兜一次底：CloseAll 只是通知，流退出是异步的，
// 极端情况下仍有会话没来得及 Release，这些残留进程必须在这里回收，
// 否则二进制退出后它们会变成孤儿。
func (a *authStack) Close() error {
	if a == nil {
		return nil
	}
	if a.mcp != nil {
		if n := a.mcp.CloseAll(); n > 0 {
			log.Printf("[routing] 已通知 %d 条进行中的 MCP 流退出", n)
		}
	}
	if a.transport == nil {
		return nil
	}
	return a.transport.Close()
}

// options 把鉴权栈转成 SSE 服务需要的选项。
//
// 关键点：管理端是「挂载」而不是「包中间件」。它走 admin token，
// MCP 主路径走 API Key，两者是两套凭据，任何一方都不该拦截另一方。
func (a *authStack) options() []gateway.SSEOption {
	opts := []gateway.SSEOption{
		gateway.WithHandlerWrapper(a.middleware()),
		gateway.WithMount(adminPrefix, a.admin.Handler()),
	}
	// MCP 端点交给命名空间路由：/sse、/sse/{ns}、/messages、/messages/{ns}。
	// 它在 wrapper 之后分派，因此能看到 Principal；无前缀写法由路由层回落到
	// 默认命名空间，老客户端的 URL 不需要改。
	if a.mcp != nil {
		opts = append(opts, gateway.WithMCPRouter(a.mcp))
	}
	return opts
}

// middleware 组合鉴权与限流，顺序不可交换：
// 限流粒度是 Key，没有 Principal 就无法计数。
func (a *authStack) middleware() func(http.Handler) http.Handler {
	authMW := auth.Middleware(a.auth)
	limitMW := auth.LimitMiddleware(a.limiter, 0)

	return func(next http.Handler) http.Handler {
		return authMW(limitMW(next))
	}
}
