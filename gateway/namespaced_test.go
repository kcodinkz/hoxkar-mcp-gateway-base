package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/gateway"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/routing"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

const (
	nsID   = idgen.ID("nsn_0000000000000001")
	keyID  = idgen.ID("key_0000000000000001")
	keyID2 = idgen.ID("key_0000000000000002")
)

type perms struct{}

func (perms) CanUseNamespace(_ context.Context, k, ns idgen.ID) (bool, error) {
	return k == keyID && ns == nsID, nil
}

func (perms) CanUseTool(_ context.Context, k, ns idgen.ID, tool string) (bool, error) {
	return k == keyID && ns == nsID && tool == "echo", nil
}

func (perms) ListPermissions(context.Context, idgen.ID) ([]store.Permission, error) {
	return nil, nil
}

type nslookup struct{}

func (nslookup) GetNamespaceBySlug(_ context.Context, slug string) (*store.Namespace, error) {
	if slug == "blender" {
		return &store.Namespace{ID: nsID, Slug: "blender", Enabled: true}, nil
	}
	return nil, store.ErrNotFound
}

func (nslookup) GetNamespaceByID(_ context.Context, id idgen.ID) (*store.Namespace, error) {
	if id == nsID {
		return &store.Namespace{ID: nsID, Slug: "blender", Enabled: true}, nil
	}
	return nil, store.ErrNotFound
}

// backend 按需回固定应答，并记录收到的消息。
type backend struct {
	requests []json.RawMessage
}

func (b *backend) Forward(_ context.Context, msg json.RawMessage) (json.RawMessage, error) {
	b.requests = append(b.requests, append(json.RawMessage(nil), msg...))

	var probe struct {
		ID *json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(msg, &probe); err == nil && probe.ID == nil {
		return nil, nil // 通知
	}
	return json.RawMessage(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`), nil
}

func (b *backend) Close() error { return nil }

type factory struct{ b *backend }

func (f *factory) Open(context.Context, *store.Namespace) (routing.Backend, error) {
	return f.b, nil
}

func newServer(t *testing.T) (*gateway.NamespacedMCP, *backend, *httptest.Server) {
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

	// 用假 Principal 代替真实 Key 解析：本测试的目标是路由与端点语义，
	// 鉴权链路由 internal/auth 自己的测试覆盖。
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("asKey2") == "1" {
			mcp.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), &auth.Principal{KeyID: keyID2})))
			return
		}
		mcp.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), &auth.Principal{KeyID: keyID})))
	})

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return mcp, b, srv
}

// openSSE 建立 SSE 连接并读回 endpoint 事件里的投递地址。
func openSSE(t *testing.T, srv *httptest.Server, path string) (*http.Response, string, string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("建连失败: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("建连状态 %d: %s", resp.StatusCode, body)
	}

	// 读第一段事件（以空行结束）。
	buf := make([]byte, 512)
	n, err := resp.Body.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("读取 events 失败: %v", err)
	}
	event := string(buf[:n])
	if !strings.Contains(event, "event: endpoint") {
		t.Fatalf("首个事件不是 endpoint: %q", event)
	}

	idx := strings.Index(event, "data: ")
	if idx < 0 {
		t.Fatalf("endpoint 事件缺少 data: %q", event)
	}
	addr := strings.TrimSpace(event[idx+len("data: "):])
	addr = strings.SplitN(addr, "\n", 2)[0]
	addr = strings.TrimRight(addr, "\r")

	return resp, addr, event
}

func TestNamespacedMCPHandles(t *testing.T) {
	mcp, _, _ := newServer(t)

	for _, path := range []string{"/sse", "/sse/blender", "/messages", "/messages/blender"} {
		if !mcp.Handles(path) {
			t.Fatalf("Handles(%q) = false，期望由本入口接管", path)
		}
	}
	for _, path := range []string{"/admin/v1/keys", "/", "/sse/a/b"} {
		if mcp.Handles(path) {
			t.Fatalf("Handles(%q) = true，不应由本入口接管", path)
		}
	}
}

func TestEndpointEventCarriesSlugAndSession(t *testing.T) {
	_, _, srv := newServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	if !strings.HasPrefix(addr, "/messages/blender?sessionId=") {
		t.Fatalf("投递地址 = %q，期望 /messages/blender?sessionId=…", addr)
	}
}

// TestNoPrefixFallsBackToDefault 验证无前缀建连回落到默认命名空间。
//
// 这里只覆盖 NamespacedMCP 自身；生产装配下的全链路回落由
// dispatch_default_test.go 的 TestLegacyClient* 负责。
func TestNoPrefixFallsBackToDefault(t *testing.T) {
	_, _, srv := newServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse")
	defer resp.Body.Close()

	// 回落时路径里没有 slug，投递地址自然也不带 slug —— 客户端原样回投即可。
	if !strings.HasPrefix(addr, "/messages?sessionId=") {
		t.Fatalf("投递地址 = %q，期望 /messages?sessionId=…", addr)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态 %d", resp.StatusCode)
	}
}

func TestMessageForwardsToBackend(t *testing.T) {
	_, b, srv := newServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	payload := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hi"}}}`
	post, err := http.Post(srv.URL+addr, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("投递失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(post.Body)
		t.Fatalf("状态 = %d，期望 202：%s", post.StatusCode, body)
	}
	if len(b.requests) != 1 {
		t.Fatalf("后端收到 %d 条，期望 1 条", len(b.requests))
	}
	if !strings.Contains(string(b.requests[0]), "echo") {
		t.Fatalf("后端收到的内容不对: %s", b.requests[0])
	}
}

func TestNotificationReturns202WithoutBody(t *testing.T) {
	_, b, srv := newServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	// 无 id ⇒ 通知。
	payload := `{"jsonrpc":"2.0","method":"notifications/initialized"}`
	post, err := http.Post(srv.URL+addr, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("投递失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusAccepted {
		t.Fatalf("状态 = %d，期望 202", post.StatusCode)
	}
	body, _ := io.ReadAll(post.Body)
	if len(body) != 0 {
		t.Fatalf("通知不应有响应体，得到 %q", body)
	}
	if len(b.requests) != 1 {
		t.Fatalf("通知也应转发给后端，实际 %d 条", len(b.requests))
	}
}

func TestMessageRejectsUnknownSession(t *testing.T) {
	_, _, srv := newServer(t)

	post, err := http.Post(srv.URL+"/messages/blender?sessionId=bogus", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusNotFound {
		t.Fatalf("状态 = %d，期望 404", post.StatusCode)
	}
}

func TestMessageRejectsOtherKey(t *testing.T) {
	_, b, srv := newServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	// 换一把 Key 拿同一个 sessionId 投递。
	req, _ := http.NewRequest(http.MethodPost, srv.URL+addr+"&asKey2=1",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Content-Type", "application/json")
	post, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusForbidden {
		t.Fatalf("状态 = %d，期望 403", post.StatusCode)
	}
	if len(b.requests) != 0 {
		t.Fatalf("越权消息不应到达后端，实际 %d 条", len(b.requests))
	}
}

func TestMessageRejectsSlugMismatch(t *testing.T) {
	_, b, srv := newServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	// 把路径里的 slug 改掉，sessionId 保留。
	sessionID := addr[strings.Index(addr, "sessionId=")+len("sessionId="):]
	post, err := http.Post(srv.URL+"/messages/other?sessionId="+sessionID, "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusForbidden {
		t.Fatalf("状态 = %d，期望 403", post.StatusCode)
	}
	if len(b.requests) != 0 {
		t.Fatalf("跨命名空间投递不应到达后端，实际 %d 条", len(b.requests))
	}
}

func TestUnauthorizedToolIsForbidden(t *testing.T) {
	_, b, srv := newServer(t)

	resp, addr, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	payload := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"render"}}`
	post, err := http.Post(srv.URL+addr, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("投递失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusForbidden {
		t.Fatalf("状态 = %d，期望 403", post.StatusCode)
	}
	if len(b.requests) != 0 {
		t.Fatalf("未授权工具不应被转发，实际 %d 条", len(b.requests))
	}
}

func TestSSERejectsNonGET(t *testing.T) {
	_, _, srv := newServer(t)

	post, err := http.Post(srv.URL+"/sse/blender", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer post.Body.Close()

	if post.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("状态 = %d，期望 405", post.StatusCode)
	}
	if got := post.Header.Get("Allow"); got != http.MethodGet {
		t.Fatalf("Allow = %q，期望 GET", got)
	}
}

func TestMessageRejectsNonPOST(t *testing.T) {
	_, _, srv := newServer(t)

	resp, err := http.Get(srv.URL + "/messages/blender?sessionId=x")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("状态 = %d，期望 405", resp.StatusCode)
	}
}

func TestForbiddenNamespace(t *testing.T) {
	_, _, srv := newServer(t)

	resp, err := http.Get(srv.URL + "/sse/nope")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("状态 = %d，期望 403", resp.StatusCode)
	}
}

func TestCloseAllClosesStreams(t *testing.T) {
	mcp, _, srv := newServer(t)

	resp, _, _ := openSSE(t, srv, "/sse/blender")
	defer resp.Body.Close()

	if got := mcp.StreamCount(); got != 1 {
		t.Fatalf("StreamCount = %d，期望 1", got)
	}
	if n := mcp.CloseAll(); n != 1 {
		t.Fatalf("CloseAll 关闭 %d 条，期望 1 条", n)
	}
	if got := mcp.StreamCount(); got != 0 {
		t.Fatalf("CloseAll 后 StreamCount = %d，期望 0", got)
	}
}
