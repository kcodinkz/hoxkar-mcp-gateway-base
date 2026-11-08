package gateway_test

// 本文件验证装配契约：多租户模式下 /sse 与 /messages 是否真的
// 被 NamespacedMCP 接管，且接管时机是否在 API Key 中间件之后。
//
// 刻意不碰数据库与 Key 解析：wrapper 用一个直接注入 Principal 的假中间件
// 代替，把「鉴权是否被执行」与「鉴权怎么实现」分开验证。internal/auth
// 自己的测试已经覆盖后者，这里只关心顺序。

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/gateway"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/routing"
)

// newDispatchServer 组装一台与生产同构的 SSE 服务：
// wrapper（假装是 API Key 中间件）+ 命名空间路由 + 管理端挂载点。
//
// 返回值里的计数器是 wrapper 被调用的次数，用于证明挂载点不会被二次拦截。
func newDispatchServer(t *testing.T, withRouter bool) (*httptest.Server, *backend, *int32) {
	t.Helper()

	authz := auth.NewAuthorizer(perms{}, nslookup{}, auth.RBACOptions{})
	b := &backend{}
	router := routing.NewRouter(authz, &factory{b: b}, routing.Options{
		SSEPath:          "/sse",
		MessagePath:      "/messages",
		DefaultNamespace: "blender",
		SessionTTL:       time.Hour,
	})
	mcp := gateway.NewNamespacedMCP(router, gateway.NamespacedMCPOptions{
		MessageBase: "/messages",
	})

	var wrapperCalls int32
	opts := []gateway.SSEOption{
		gateway.WithSSEEndpoint("/sse"),
		gateway.WithMessageEndpoint("/messages"),
		gateway.WithHandlerWrapper(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&wrapperCalls, 1)
				// 真实中间件在这里注入 Principal。若分派顺序反了，
				// 路由层拿到的就是空身份，会返回 403，测试随即变红。
				ctx := auth.WithPrincipal(r.Context(), &auth.Principal{KeyID: keyID})
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		}),
		gateway.WithMount("/admin/v1", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})),
	}
	if withRouter {
		opts = append(opts, gateway.WithMCPRouter(mcp))
	}

	sse := gateway.NewSSEServer(server.NewMCPServer("dispatch-test", "0.0.0"), opts...)
	srv := httptest.NewServer(sse)
	t.Cleanup(srv.Close)

	return srv, b, &wrapperCalls
}

// 装好命名空间路由后，/sse/{ns} 必须由它接管：endpoint 事件要带 slug 与
// sessionId，消息要真的进到会话绑定的后端。
func TestSSEDispatchSendsMCPPathsToNamespacedMCP(t *testing.T) {
	srv, b, wrapperCalls := newDispatchServer(t, true)

	resp, addr, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	if !strings.HasPrefix(addr, "/messages/blender?sessionId=") {
		t.Fatalf("投递地址 = %q，期望由命名空间路由生成", addr)
	}
	if got := atomic.LoadInt32(wrapperCalls); got == 0 {
		t.Fatal("wrapper 未被调用：鉴权必须先于命名空间分派")
	}

	payload := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo"}}`
	post, err := http.Post(srv.URL+addr, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("投递失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusAccepted {
		t.Fatalf("状态 = %d，期望 202", post.StatusCode)
	}
	if len(b.requests) != 1 {
		t.Fatalf("后端收到 %d 条，期望 1 条", len(b.requests))
	}
}

// 不属于 MCP 端点的路径不能被吞掉，仍要落到旧逻辑的精确匹配上得到 404。
func TestSSEDispatchLeavesUnknownPathsToLegacy(t *testing.T) {
	srv, _, _ := newDispatchServer(t, true)

	resp, err := http.Get(srv.URL + "/sse/a/b")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("状态 = %d，期望 404", resp.StatusCode)
	}
}

// 不装命名空间路由时（旧启动路径），/sse/{ns} 依旧是未知路径。
// 这条锁死「旧路径一个字节都不能变」。
func TestSSEDispatchWithoutRouterKeepsLegacyBehavior(t *testing.T) {
	srv, _, _ := newDispatchServer(t, false)

	resp, err := http.Get(srv.URL + "/sse/blender")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("状态 = %d，期望 404", resp.StatusCode)
	}
}

// 管理端挂载点必须先于 wrapper 分派：它走管理 token，与 MCP 的 API Key
// 是两套凭据，任何一方都不该拦另一方。
func TestSSEDispatchAdminMountBypassesAuthWrapper(t *testing.T) {
	srv, _, wrapperCalls := newDispatchServer(t, true)

	resp, err := http.Get(srv.URL + "/admin/v1/keys")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("状态 = %d，期望 204", resp.StatusCode)
	}
	if got := atomic.LoadInt32(wrapperCalls); got != 0 {
		t.Fatalf("管理端请求被 wrapper 拦截了 %d 次", got)
	}
}

// 多租户模式的核心诉求：不带 -command 也能起服务。
func TestGatewayStartSkipsStdioWhenNoCommand(t *testing.T) {
	newBare := func() *gateway.GatewaySSEServer {
		return gateway.NewGatewaySSEServer(
			gateway.McpCommand{},
			gateway.NewSSEServer(server.NewMCPServer("start-test", "0.0.0")),
		)
	}

	// 99999 不是合法端口，ListenAndServe 会同步失败；这里关心的是「为什么」失败。
	err := newBare().Start("127.0.0.1:99999")
	if err == nil {
		t.Fatal("非法监听地址应当返回错误")
	}
	if strings.Contains(err.Error(), "exec") || strings.Contains(err.Error(), "MCP process") {
		t.Fatalf("空 -command 仍尝试拉起子进程: %v", err)
	}

	// 对照：给了命令就必须去拉进程，失败发生在 exec 阶段。
	withCmd := gateway.NewGatewaySSEServer(
		gateway.McpCommand{Command: "mcp-gateway-no-such-binary-xyz"},
		gateway.NewSSEServer(server.NewMCPServer("start-test", "0.0.0")),
	)
	err = withCmd.Start("127.0.0.1:99999")
	if err == nil || !strings.Contains(err.Error(), "MCP process") {
		t.Fatalf("有 -command 时应先尝试启动子进程，实际: %v", err)
	}
}
