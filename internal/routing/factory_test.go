package routing

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// TestTransportFactoryDispatchesByTransport 验证两种传输方式各自落到对的子工厂。
func TestTransportFactoryDispatchesByTransport(t *testing.T) {
	srv := newMCPServer(t, echoHandler)
	httpF, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)
	stdioF := NewStdioFactory(func(string, ...any) {})
	t.Cleanup(func() { _ = stdioF.Close() })

	f := &TransportFactory{Stdio: stdioF, HTTP: httpF}

	t.Run("http", func(t *testing.T) {
		ns := httpNamespace("svc", 5000)
		b, err := f.Open(context.Background(), ns)
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		defer b.Close()
		if _, ok := b.(*httpBackend); !ok {
			t.Fatalf("http 命名空间应得到 httpBackend，实际 %T", b)
		}
	})

	t.Run("stdio", func(t *testing.T) {
		ns := stdioNamespace(1, 8000)
		ns.Slug = "debug2"
		ns.ID = idgen.ID("ns_stdio_test_2")
		b, err := f.Open(context.Background(), ns)
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		defer b.Close()
		if _, ok := b.(*stdioBackend); !ok {
			t.Fatalf("stdio 命名空间应得到 stdioBackend，实际 %T", b)
		}
	})
}

// TestTransportFactoryEmptyTransportDefaultsToHTTP 验证空 transport 走集群主路径。
//
// 这一条是安全边界：空 transport 若被当作 stdio，网关会去执行一个空命令；
// 当作 http 则最差只是「没有健康实例」，不会在网关机器上跑东西。
func TestTransportFactoryEmptyTransportDefaultsToHTTP(t *testing.T) {
	srv := newMCPServer(t, echoHandler)
	httpF, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)

	f := &TransportFactory{HTTP: httpF}

	ns := httpNamespace("svc", 5000)
	ns.Transport = ""

	b, err := f.Open(context.Background(), ns)
	if err != nil {
		t.Fatalf("空 transport 应当按 http 处理，实际: %v", err)
	}
	defer b.Close()
	if _, ok := b.(*httpBackend); !ok {
		t.Fatalf("期望 httpBackend，实际 %T", b)
	}
}

// TestTransportFactoryRejectsUnsupported 验证未知传输方式给出明确错误。
func TestTransportFactoryRejectsUnsupported(t *testing.T) {
	f := &TransportFactory{}

	ns := httpNamespace("svc", 5000)
	ns.Transport = "websocket"

	_, err := f.Open(context.Background(), ns)
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "websocket") {
		t.Fatalf("错误信息应点出这个传输方式，实际: %v", err)
	}
}

// TestTransportFactoryMissingSubFactory 验证没装配子工厂时报错可读、
// 而不是空指针崩溃。
func TestTransportFactoryMissingSubFactory(t *testing.T) {
	f := &TransportFactory{}

	t.Run("http", func(t *testing.T) {
		_, err := f.Open(context.Background(), httpNamespace("svc", 5000))
		if !errors.Is(err, ErrNoBackend) {
			t.Fatalf("期望 ErrNoBackend，实际: %v", err)
		}
		if !strings.Contains(err.Error(), "未启用 HTTP 转发") {
			t.Fatalf("错误信息应说明未启用，实际: %v", err)
		}
	})

	t.Run("stdio", func(t *testing.T) {
		_, err := f.Open(context.Background(), stdioNamespace(1, 1000))
		if !errors.Is(err, ErrNoBackend) {
			t.Fatalf("期望 ErrNoBackend，实际: %v", err)
		}
		if !strings.Contains(err.Error(), "未启用 stdio 进程池") {
			t.Fatalf("错误信息应说明未启用，实际: %v", err)
		}
	})
}

// TestTransportFactoryNilNamespace 验证空命名空间不会崩。
func TestTransportFactoryNilNamespace(t *testing.T) {
	f := &TransportFactory{}
	if _, err := f.Open(context.Background(), nil); !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
}

// TestTransportFactoryCloseStopsStdio 验证 Close 会连带回收 stdio 子进程。
func TestTransportFactoryCloseStopsStdio(t *testing.T) {
	stdioF := NewStdioFactory(func(string, ...any) {})
	f := &TransportFactory{Stdio: stdioF}

	ns := stdioNamespace(1, 8000)
	b, err := f.Open(context.Background(), ns)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	if f.LiveStdioProcesses() != 1 {
		t.Fatalf("LiveStdioProcesses = %d，期望 1", f.LiveStdioProcesses())
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if f.LiveStdioProcesses() != 0 {
		t.Fatalf("关停后仍有 %d 个 stdio 进程", f.LiveStdioProcesses())
	}
}

// TestTransportFactoryEndsToEndThroughRouter 是 M3 的收尾验证：
// Router + TransportFactory + 真实 httptest 上游，走完整裁决链路。
//
// 这里自带一套夹具而不复用 router_test.go 的 newFixture：那个夹具的
// nsBlender 只有 Transport 没有 DiscoveryService，HTTPFactory.Open 会
// 直接判 ErrNoBackend——路由没毛病，缺的是「后端在哪」。真实链路里
// 命名空间必须带上 r-nacos 服务名，所以这里按真实形状造一个。
func TestTransportFactoryEndsToEndThroughRouter(t *testing.T) {
	srv := newMCPServer(t, echoHandler)
	httpF, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)

	f := &TransportFactory{HTTP: httpF}

	const (
		nsID    = idgen.ID("nsn_00000000000000e1")
		keyID   = idgen.ID("key_00000000000000e1")
		svcName = "blender-svc"
	)
	ns := httpNamespace(svcName, 5000)
	ns.ID = nsID
	ns.Slug = "blender"
	ns.Tools = []string{"echo"}

	perms := &fakePerms{
		nsAllowed: map[string]bool{
			string(keyID) + "|" + string(nsID): true,
		},
		tools: map[string]map[string]bool{
			string(keyID) + "|" + string(nsID): {"echo": true},
		},
	}
	authz := auth.NewAuthorizer(perms, &fakeNS{bySlug: map[string]*store.Namespace{
		"blender": ns,
	}}, auth.RBACOptions{})

	router := NewRouter(authz, f, Options{
		DefaultNamespace: "blender",
		SessionTTL:       time.Hour,
	})

	p := &auth.Principal{KeyID: keyID, KeyPrefix: "hk_test"}
	target := router.Parse("/sse/blender")
	if target.Endpoint != EndpointSSE || target.Slug != "blender" {
		t.Fatalf("路径解析不对: %+v", target)
	}

	route, err := router.Open(context.Background(), p, target)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	router.Dial("sess-e2e", p, route)
	defer router.Release("sess-e2e")

	bound, err := router.Lookup("sess-e2e", p, "blender")
	if err != nil {
		t.Fatalf("Lookup 失败: %v", err)
	}
	if bound.Backend != route.Backend {
		t.Fatal("整个会话必须落在同一个后端实例上")
	}

	msg := json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	if err := router.AuthorizeTool(context.Background(), p, bound, msg); err != nil {
		t.Fatalf("工具级裁决失败: %v", err)
	}

	out, err := bound.Backend.Forward(context.Background(), msg)
	if err != nil {
		t.Fatalf("Forward 失败: %v", err)
	}
	if !strings.Contains(string(out), `"tools/list"`) {
		t.Fatalf("响应不对: %s", out)
	}
	if srv.count() != 1 {
		t.Fatalf("上游收到 %d 次请求，期望 1", srv.count())
	}

	// TransportFactory 会连带关掉两个子工厂；httpBackend 没有独占资源，Close 幂等。
	if err := f.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
}
