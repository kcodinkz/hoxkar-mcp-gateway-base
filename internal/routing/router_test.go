package routing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// 测试用固定 ID，形状与 idgen 一致（前缀 + 雪花），不必走真实生成器。
var (
	nsBlenderID = idgen.ID("nsn_0000000000000001")
	nsGoneID    = idgen.ID("nsn_0000000000000002")
	keyAID      = idgen.ID("key_0000000000000001")
	keyBID      = idgen.ID("key_0000000000000002")
)

var (
	nsBlender = &store.Namespace{ID: nsBlenderID, Slug: "blender", Enabled: true, Transport: store.TransportHTTP}
	nsGone    = &store.Namespace{ID: nsGoneID, Slug: "gone", Enabled: false, Transport: store.TransportHTTP}
)

// fakePerms 是 PermissionStore 的最小实现。
type fakePerms struct {
	nsAllowed map[string]bool            // "keyID|nsID"
	tools     map[string]map[string]bool // "keyID|nsID" → tool → ok
}

func (f *fakePerms) CanUseNamespace(_ context.Context, keyID, nsID idgen.ID) (bool, error) {
	return f.nsAllowed[string(keyID)+"|"+string(nsID)], nil
}

func (f *fakePerms) CanUseTool(_ context.Context, keyID, nsID idgen.ID, tool string) (bool, error) {
	return f.tools[string(keyID)+"|"+string(nsID)][tool], nil
}

func (f *fakePerms) ListPermissions(_ context.Context, keyID idgen.ID) ([]store.Permission, error) {
	var out []store.Permission
	for k, tools := range f.tools {
		if k != string(keyID)+"|"+string(nsBlenderID) {
			continue
		}
		for t, ok := range tools {
			if !ok {
				continue
			}
			out = append(out, store.Permission{NamespaceID: nsBlenderID, ToolName: t})
		}
	}
	return out, nil
}

// fakeNS 是 NamespaceLookup 的最小实现。
type fakeNS struct {
	bySlug map[string]*store.Namespace
}

func (f *fakeNS) GetNamespaceBySlug(_ context.Context, slug string) (*store.Namespace, error) {
	if ns, ok := f.bySlug[slug]; ok {
		return ns, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeNS) GetNamespaceByID(_ context.Context, id idgen.ID) (*store.Namespace, error) {
	for _, ns := range f.bySlug {
		if ns.ID == id {
			return ns, nil
		}
	}
	return nil, store.ErrNotFound
}

// fakeBackend 记录被转发的内容，便于断言「消息确实到了这个后端」。
type fakeBackend struct {
	name     string
	closed   bool
	received []json.RawMessage
	mu       sync.Mutex
}

func (b *fakeBackend) Forward(_ context.Context, msg json.RawMessage) (json.RawMessage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.received = append(b.received, append(json.RawMessage(nil), msg...))
	return json.RawMessage(`{"jsonrpc":"2.0","id":1,"result":{}}`), nil
}

func (b *fakeBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *fakeBackend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.received)
}

// fakeFactory 按 slug 计数开出的后端，用来验证「每次建连各开一个」。
type fakeFactory struct {
	mu     sync.Mutex
	opened map[string][]*fakeBackend
	err    error
}

func (f *fakeFactory) Open(_ context.Context, ns *store.Namespace) (Backend, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	b := &fakeBackend{name: ns.Slug}
	f.opened[ns.Slug] = append(f.opened[ns.Slug], b)
	return b, nil
}

func (f *fakeFactory) last(slug string) *fakeBackend {
	f.mu.Lock()
	defer f.mu.Unlock()
	bs := f.opened[slug]
	if len(bs) == 0 {
		return nil
	}
	return bs[len(bs)-1]
}

// fixture 把上面的替身装成一个 Router。
type fixture struct {
	router  *Router
	factory *fakeFactory
	perms   *fakePerms
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	f := &fixture{
		factory: &fakeFactory{opened: map[string][]*fakeBackend{}},
		perms: &fakePerms{
			nsAllowed: map[string]bool{
				string(keyAID) + "|" + string(nsBlenderID): true,
			},
			tools: map[string]map[string]bool{
				string(keyAID) + "|" + string(nsBlenderID): {"echo": true},
			},
		},
		now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	ns := &fakeNS{bySlug: map[string]*store.Namespace{
		"blender": nsBlender,
		"gone":    nsGone,
		"other":   {ID: idgen.ID("nsn_0000000000000003"), Slug: "other", Enabled: true},
	}}

	authz := auth.NewAuthorizer(f.perms, ns, auth.RBACOptions{
		Now: func() time.Time { return f.now },
	})

	f.router = NewRouter(authz, f.factory, Options{
		SSEPath:          "/sse",
		MessagePath:      "/messages",
		DefaultNamespace: "blender",
		SessionTTL:       time.Hour,
		Now:              func() time.Time { return f.now },
	})
	return f
}

func principal(id idgen.ID) *auth.Principal {
	return &auth.Principal{KeyID: id, KeyPrefix: "hk_test"}
}

func TestOpenWithPrefix(t *testing.T) {
	f := newFixture(t)
	target := f.router.Parse("/sse/blender")

	route, err := f.router.Open(context.Background(), principal(keyAID), target)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if route.Slug() != "blender" {
		t.Fatalf("Slug = %q，期望 blender", route.Slug())
	}
	if f.factory.last("blender") == nil {
		t.Fatal("没有为 blender 开后端")
	}
}

func TestOpenFallsBackToDefaultNamespace(t *testing.T) {
	f := newFixture(t)

	route, err := f.router.Open(context.Background(), principal(keyAID), f.router.Parse("/sse"))
	if err != nil {
		t.Fatalf("无前缀请求应回落到默认命名空间，却报错: %v", err)
	}
	if route.Slug() != "blender" {
		t.Fatalf("Slug = %q，期望落进默认命名空间 blender", route.Slug())
	}
}

func TestOpenRequiresPermission(t *testing.T) {
	f := newFixture(t)

	// keyB 没有任何权限。
	_, err := f.router.Open(context.Background(), principal(keyBID), f.router.Parse("/sse/blender"))
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("期望 ErrForbidden，得到 %v", err)
	}
	if f.factory.last("blender") != nil {
		t.Fatal("越权请求不应在开后端之后才被拒——上游资源会被白白占用")
	}
}

func TestOpenUnknownNamespaceIsForbidden(t *testing.T) {
	f := newFixture(t)

	_, err := f.router.Open(context.Background(), principal(keyAID), f.router.Parse("/sse/nope"))
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("不存在的命名空间应表现为 ErrForbidden，得到 %v", err)
	}
}

func TestOpenDisabledNamespaceIsForbidden(t *testing.T) {
	f := newFixture(t)

	_, err := f.router.Open(context.Background(), principal(keyAID), f.router.Parse("/sse/gone"))
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("已下线的命名空间应表现为 ErrForbidden，得到 %v", err)
	}
}

func TestOpenWithoutPrincipal(t *testing.T) {
	f := newFixture(t)

	_, err := f.router.Open(context.Background(), nil, f.router.Parse("/sse/blender"))
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("缺少身份应表现为 ErrForbidden，得到 %v", err)
	}
}

func TestOpenBlankSlugDoesNotFallBack(t *testing.T) {
	f := newFixture(t)

	// 直接构造 Scoped=true 且 Slug 为空的目标（/sse/%20 这种写法）。
	_, err := f.router.Open(context.Background(), principal(keyAID), Target{
		Endpoint: EndpointSSE,
		Scoped:   true,
	})
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("写了前缀但段内为空时不得回落，得到 %v", err)
	}
}

func TestOpenPropagatesNoBackend(t *testing.T) {
	f := newFixture(t)
	f.factory.err = ErrNoBackend

	_, err := f.router.Open(context.Background(), principal(keyAID), f.router.Parse("/sse/blender"))
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，得到 %v", err)
	}
}

func TestDialLookupKeepsSameBackend(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	p := principal(keyAID)

	route, err := f.router.Open(ctx, p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	f.router.Dial("sess-1", p, route)

	// 再 Open 一次会开出第二个后端；Lookup 必须仍然返回第一个。
	second, err := f.router.Open(ctx, p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("第二次 Open 失败: %v", err)
	}
	if second.Backend == route.Backend {
		t.Fatal("测试前提不成立：两次 Open 应该是两个不同后端")
	}

	found, err := f.router.Lookup("sess-1", p, "blender")
	if err != nil {
		t.Fatalf("Lookup 失败: %v", err)
	}
	if found.Backend != route.Backend {
		t.Fatal("同一会话的后端发生了漂移")
	}
	if f.router.SessionCount() != 1 {
		t.Fatalf("SessionCount = %d，期望 1", f.router.SessionCount())
	}
}

func TestLookupRejectsUnknownSession(t *testing.T) {
	f := newFixture(t)

	_, err := f.router.Lookup("nope", principal(keyAID), "blender")
	if !errors.Is(err, ErrSessionUnknown) {
		t.Fatalf("期望 ErrSessionUnknown，得到 %v", err)
	}
}

func TestLookupRejectsOtherKey(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route, err := f.router.Open(context.Background(), p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	f.router.Dial("sess-1", p, route)

	_, err = f.router.Lookup("sess-1", principal(keyBID), "blender")
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("别的 Key 猜中 sessionId 也必须被拒，得到 %v", err)
	}
}

func TestLookupRejectsSlugMismatch(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route, err := f.router.Open(context.Background(), p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	f.router.Dial("sess-1", p, route)

	_, err = f.router.Lookup("sess-1", p, "other")
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("会话与路径命名空间不一致必须被拒，得到 %v", err)
	}
}

func TestLookupExpiresAfterTTL(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route, err := f.router.Open(context.Background(), p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	f.router.Dial("sess-1", p, route)

	f.now = f.now.Add(2 * time.Hour)

	_, err = f.router.Lookup("sess-1", p, "blender")
	if !errors.Is(err, ErrSessionUnknown) {
		t.Fatalf("超过 TTL 的会话应视为不存在，得到 %v", err)
	}
	if f.router.SessionCount() != 0 {
		t.Fatal("过期会话应被顺手清掉")
	}
}

func TestReleaseClosesBackend(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route, err := f.router.Open(context.Background(), p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	backend := f.factory.last("blender")
	f.router.Dial("sess-1", p, route)

	f.router.Release("sess-1")

	if f.router.SessionCount() != 0 {
		t.Fatal("Release 后不应残留绑定")
	}
	if !backend.closed {
		t.Fatal("Release 必须关闭后端，否则进程/连接会泄漏")
	}
}

func TestSweepClosesExpiredOnly(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	oldRoute, err := f.router.Open(context.Background(), p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	oldBackend := f.factory.last("blender")
	f.router.Dial("old", p, oldRoute)

	f.now = f.now.Add(90 * time.Minute)

	newRoute, err := f.router.Open(context.Background(), p, f.router.Parse("/sse/blender"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	newBackend := f.factory.last("blender")
	f.router.Dial("new", p, newRoute)

	if n := f.router.Sweep(); n != 1 {
		t.Fatalf("Sweep 回收 %d 条，期望 1 条", n)
	}
	if !oldBackend.closed {
		t.Fatal("过期会话的后端应被关闭")
	}
	if newBackend.closed {
		t.Fatal("活跃会话的后端不应被关闭")
	}
	if f.router.SessionCount() != 1 {
		t.Fatalf("SessionCount = %d，期望 1", f.router.SessionCount())
	}
}

func TestToolCallParsing(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		tool string
		ok   bool
	}{
		{"tools/call", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{}}}`, "echo", true},
		{"历史别名", `{"jsonrpc":"2.0","id":1,"method":"mcp/tools/call","params":{"name":"echo"}}`, "echo", true},
		{"tools/list 不算调用", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, "", false},
		{"initialize 不算调用", `{"jsonrpc":"2.0","id":1,"method":"initialize"}`, "", false},
		{"坏 JSON", `{`, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tool, ok := ToolCall(json.RawMessage(tc.msg))
			if ok != tc.ok || tool != tc.tool {
				t.Fatalf("ToolCall = (%q,%v)，期望 (%q,%v)", tool, ok, tc.tool, tc.ok)
			}
		})
	}
}

func TestAuthorizeToolAllowsListed(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route := &Route{Namespace: nsBlender}
	msg := json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo"}}`)
	if err := f.router.AuthorizeTool(context.Background(), p, route, msg); err != nil {
		t.Fatalf("已授权工具不应被拒: %v", err)
	}
}

func TestAuthorizeToolRejectsUnlisted(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route := &Route{Namespace: nsBlender}
	msg := json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"render"}}`)
	err := f.router.AuthorizeTool(context.Background(), p, route, msg)
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("未授权工具应表现为 ErrForbidden，得到 %v", err)
	}
}

func TestAuthorizeToolRejectsEmptyName(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route := &Route{Namespace: nsBlender}
	msg := json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}`)
	err := f.router.AuthorizeTool(context.Background(), p, route, msg)
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("tools/call 缺工具名应被拒，得到 %v", err)
	}
}

func TestAuthorizeToolSkipsNonCall(t *testing.T) {
	f := newFixture(t)
	p := principal(keyAID)

	route := &Route{Namespace: nsBlender}
	msg := json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	if err := f.router.AuthorizeTool(context.Background(), p, route, msg); err != nil {
		t.Fatalf("非工具调用不应做工具级裁决: %v", err)
	}
}

func TestWriteErrorStatusMapping(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"缺少 token", auth.ErrMissingToken, http.StatusUnauthorized, "missing_token"},
		{"越权", auth.ErrForbidden, http.StatusForbidden, "forbidden"},
		{"会话未知", ErrSessionUnknown, http.StatusNotFound, "session_unknown"},
		{"未指定命名空间", ErrNamespaceRequired, http.StatusBadRequest, "namespace_required"},
		{"无后端", ErrNoBackend, http.StatusServiceUnavailable, "no_backend"},
		{"内部错误", errors.New("boom"), http.StatusInternalServerError, "internal_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/sse", nil)
			WriteError(rec, req, tc.err)

			if rec.Code != tc.status {
				t.Fatalf("status = %d，期望 %d", rec.Code, tc.status)
			}
			body := rec.Body.String()
			if !containsJSONCode(body, tc.code) {
				t.Fatalf("响应体缺少 code=%q：%s", tc.code, body)
			}
		})
	}
}

func TestWriteErrorNilIsNoop(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, httptest.NewRequest(http.MethodGet, "/sse", nil), nil)
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("err 为 nil 时不应写任何响应，得到 %d / %q", rec.Code, rec.Body.String())
	}
}

func TestNewRouterRejectsNil(t *testing.T) {
	assertPanic(t, "authz 为 nil", func() {
		NewRouter(nil, &fakeFactory{}, Options{})
	})
	assertPanic(t, "factory 为 nil", func() {
		NewRouter(auth.NewAuthorizer(&fakePerms{
			nsAllowed: map[string]bool{},
			tools:     map[string]map[string]bool{},
		}, &fakeNS{}, auth.RBACOptions{}), nil, Options{})
	})
}

func assertPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s 时应 panic", name)
		}
	}()
	fn()
}

func TestOptionsDefaults(t *testing.T) {
	o := Options{}
	o.applyDefaults()
	if o.SSEPath != "/sse" || o.MessagePath != "/messages" {
		t.Fatalf("端点默认值不对: %+v", o)
	}
	if o.DefaultNamespace != "default" {
		t.Fatalf("默认命名空间 = %q，期望 default", o.DefaultNamespace)
	}
	if o.SessionTTL != 12*time.Hour {
		t.Fatalf("SessionTTL = %v，期望 12h", o.SessionTTL)
	}
	if o.Now == nil {
		t.Fatal("Now 应有默认值")
	}
}

func containsJSONCode(body, code string) bool {
	return len(body) > 0 && json.Valid([]byte(body)) && containsSub(body, `"code":"`+code+`"`)
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
