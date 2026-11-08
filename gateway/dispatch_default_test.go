package gateway_test

// 本文件验证老客户端不带命名空间前缀的请求（/sse、/messages）
// 在**完整的 SSE 服务栈**里仍然可用，并且真的落到了 default 命名空间上。
//
// 与 namespaced_test.go 里那个 TestNoPrefixFallsBackToDefault 的区别：
// 那个测试直接调用 NamespacedMCP.ServeHTTP，只证明「路由层会回落」；
// 这里走的是 SSEServer.ServeHTTP → mounts → wrapper → serveMain → mcpRouter
// 的全链路，证明的是「回落这件事在生产装配下也成立」。
//
// 两点必须钉死，否则回落会在装配层被悄悄吃掉：
//
//  1. 分派顺序：/sse 既属于 NamespacedMCP，也属于旧 SSEServer 的精确匹配。
//     若旧逻辑先跑，客户端会收到 /message?sessionId=…（旧默认端点，单数），
//     而消息会被投给一个根本不存在的本地子进程。因此这里断言投递地址是
//     /messages?sessionId=…（新入口下发，复数、带 s）。
//  2. 默认命名空间：生产装配（auth_setup.go）**不传** DefaultNamespace，
//     落地值全靠 routing.Options.applyDefaults 兜成 "default"。
//     本测试同样不传，于是顺带锁死「默认值真的还在」。

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mark3labs/mcp-go/server"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/gateway"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/routing"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// defaultNSID 是回落目标的命名空间 ID。与 namespaced_test.go 里的 nsID
// 刻意区分开：那边的夹具只认 "blender"，这里只认 "default"，
// 于是「请求究竟落到了哪个命名空间」不再靠日志推断，而是靠夹具的分支走向。
const defaultNSID = idgen.ID("nsn_0000000000000002")

// defaultLookup 只解析 slug = "default"。
//
// 刻意不认 "blender"：如果路由层误把无前缀请求当成 blender 处理，
// 这里会返回 ErrNotFound，测试直接变红，而不是「碰巧也能跑」。
type defaultLookup struct{}

func (defaultLookup) GetNamespaceBySlug(_ context.Context, slug string) (*store.Namespace, error) {
	if slug == "default" {
		return &store.Namespace{ID: defaultNSID, Slug: "default", Enabled: true}, nil
	}
	return nil, store.ErrNotFound
}

func (defaultLookup) GetNamespaceByID(_ context.Context, id idgen.ID) (*store.Namespace, error) {
	if id == defaultNSID {
		return &store.Namespace{ID: defaultNSID, Slug: "default", Enabled: true}, nil
	}
	return nil, store.ErrNotFound
}

// defaultPerms 只给 keyID 在 defaultNSID 上的权限。
type defaultPerms struct{}

func (defaultPerms) CanUseNamespace(_ context.Context, k, ns idgen.ID) (bool, error) {
	return k == keyID && ns == defaultNSID, nil
}

func (defaultPerms) CanUseTool(_ context.Context, k, ns idgen.ID, tool string) (bool, error) {
	return k == keyID && ns == defaultNSID && tool == "echo", nil
}

func (defaultPerms) ListPermissions(context.Context, idgen.ID) ([]store.Permission, error) {
	return nil, nil
}

// newLegacyClientServer 按生产装配顺序搭一台服务：
// wrapper（API Key）→ 命名空间路由 → SSE 主入口。
//
// 与 dispatch_test.go 的 newDispatchServer 的差别只有一处：
// routing.Options 里**不写 DefaultNamespace**，模拟 auth_setup.go 的真实写法。
func newLegacyClientServer(t *testing.T) (*httptest.Server, *backend, *int32) {
	t.Helper()

	authz := auth.NewAuthorizer(defaultPerms{}, defaultLookup{}, auth.RBACOptions{})
	b := &backend{}
	router := routing.NewRouter(authz, &factory{b: b}, routing.Options{
		SSEPath:     "/sse",
		MessagePath: "/messages",
		// DefaultNamespace 故意留空 —— 期望 routing.Options 自己兜成 "default"。
	})
	mcp := gateway.NewNamespacedMCP(router, gateway.NamespacedMCPOptions{
		MessageBase: "/messages",
	})

	var wrapperCalls int32
	sse := gateway.NewSSEServer(
		server.NewMCPServer("legacy-client-test", "0.0.0"),
		// 旧端点也照生产配置成 /sse + /messages：这样「谁接管了请求」
		// 只能靠 endpoint 事件里的路径形状来区分，排除了配置差异的干扰。
		gateway.WithSSEEndpoint("/sse"),
		gateway.WithMessageEndpoint("/messages"),
		gateway.WithHandlerWrapper(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&wrapperCalls, 1)
				ctx := auth.WithPrincipal(r.Context(), &auth.Principal{KeyID: keyID})
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		}),
		gateway.WithMCPRouter(mcp),
	)

	srv := httptest.NewServer(sse)
	t.Cleanup(srv.Close)
	return srv, b, &wrapperCalls
}

// 无前缀建连：/sse 必须由命名空间路由接管，回落到 default，
// 并且下发的投递地址不带 slug —— 老客户端原样回投即可，URL 不用改。
func TestLegacyClientWithoutPrefixReachesDefaultNamespace(t *testing.T) {
	srv, b, wrapperCalls := newLegacyClientServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse")
	defer resp.Body.Close()

	if !strings.HasPrefix(addr, "/messages?sessionId=") {
		t.Fatalf("投递地址 = %q，期望 /messages?sessionId=…（无 slug）", addr)
	}
	// 旧 handleSSE 下发的是单数 /message：出现它说明分派顺序被打回旧逻辑了。
	if strings.HasPrefix(addr, "/message?") {
		t.Fatalf("投递地址 = %q，说明请求落到了旧 stdio handler 而非命名空间路由", addr)
	}
	if got := atomic.LoadInt32(wrapperCalls); got == 0 {
		t.Fatal("wrapper 未被调用：鉴权必须先于命名空间分派")
	}

	// 消息也必须真的进到 default 命名空间的后端，而不只是建连成功。
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

// 无前缀投递：/messages?sessionId=… 同样要认。
// 这一条单独写，是因为建连成功不代表回投成功 —— 回落逻辑在
// Lookup 里还要再过一次「slug 为空时不做一致性校验」的分支。
func TestLegacyClientMessageWithoutPrefixIsAccepted(t *testing.T) {
	srv, b, _ := newLegacyClientServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse")
	defer resp.Body.Close()

	payload := `{"jsonrpc":"2.0","id":7,"method":"tools/list"}`
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
