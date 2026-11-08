package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// fakeResolver 固定返回一组实例，或一个错误。
type fakeResolver struct {
	insts   []discovery.Instance
	err     error
	mu      sync.Mutex
	calls   int
	service string
}

func (r *fakeResolver) Resolve(_ context.Context, service string) ([]discovery.Instance, error) {
	r.mu.Lock()
	r.calls++
	r.service = service
	r.mu.Unlock()
	if r.err != nil {
		return r.insts, r.err
	}
	return r.insts, nil
}

func (r *fakeResolver) Provider() string { return "fake" }
func (r *fakeResolver) Close() error     { return nil }

// httpNamespace 构造一个 http 传输的命名空间。
func httpNamespace(service string, timeoutMS int) *store.Namespace {
	return &store.Namespace{
		ID:                idgen.ID("ns_http_test"),
		Slug:              "blender",
		Transport:         store.TransportHTTP,
		DiscoveryProvider: "rnacos",
		DiscoveryService:  service,
		EndpointPath:      "/mcp",
		LBStrategy:        "round_robin",
		PoolSize:          4,
		TimeoutMS:         timeoutMS,
		Enabled:           true,
	}
}

// instancesOf 把若干 httptest 服务转成 discovery 实例。
func instancesOf(servers ...*httptest.Server) []discovery.Instance {
	out := make([]discovery.Instance, 0, len(servers))
	for _, s := range servers {
		host, portStr, err := net.SplitHostPort(strings.TrimPrefix(s.URL, "http://"))
		if err != nil {
			panic(err)
		}
		port, _ := strconv.Atoi(portStr)
		out = append(out, discovery.Instance{IP: host, Port: port, Weight: 1, Healthy: true})
	}
	return out
}

// mcpServer 起一个最小的 JSON-RPC 后端，记录收到的请求。
type mcpServer struct {
	*httptest.Server
	mu       sync.Mutex
	received []string
	sessions []string
	handler  func(w http.ResponseWriter, r *http.Request, body []byte)
}

func newMCPServer(t *testing.T, h func(w http.ResponseWriter, r *http.Request, body []byte)) *mcpServer {
	t.Helper()
	m := &mcpServer{handler: h}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 必须用 io.ReadAll：r.Body.Read 一次不保证读满，短读会让断言
		// 在负载稍大时随机失败。
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.received = append(m.received, string(body))
		if sid := r.Header.Get("Mcp-Session-Id"); sid != "" {
			m.sessions = append(m.sessions, sid)
		}
		m.mu.Unlock()
		m.handler(w, r, body)
	}))
	t.Cleanup(m.Server.Close)
	return m
}

func (m *mcpServer) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.received)
}

func (m *mcpServer) lastSession() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sessions) == 0 {
		return ""
	}
	return m.sessions[len(m.sessions)-1]
}

// echoHandler 回一个 id 匹配的标准 JSON 响应。
func echoHandler(w http.ResponseWriter, _ *http.Request, body []byte) {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	_ = json.Unmarshal(body, &req)

	w.Header().Set("Content-Type", "application/json")
	if len(req.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(req.ID),
		"result":  map[string]any{"method": req.Method},
	})
}

func newHTTPFactory(t *testing.T, insts []discovery.Instance, err error) (*HTTPFactory, *fakeResolver) {
	t.Helper()
	r := &fakeResolver{insts: insts, err: err}
	f := NewHTTPFactory(r, HTTPFactoryOptions{
		Client:   &http.Client{Timeout: 5 * time.Second},
		Logf:     func(string, ...any) {},
		Cooldown: 30 * time.Second,
	})
	t.Cleanup(func() { _ = f.Close() })
	return f, r
}

// TestHTTPForwardRoundTrip 覆盖集群主路径最短链路。
func TestHTTPForwardRoundTrip(t *testing.T) {
	srv := newMCPServer(t, echoHandler)
	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)

	b, err := f.Open(context.Background(), httpNamespace("blender-svc", 5000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	out, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatalf("Forward 失败: %v", err)
	}
	if !strings.Contains(string(out), `"tools/list"`) {
		t.Fatalf("响应不对: %s", out)
	}
	if srv.count() != 1 {
		t.Fatalf("上游收到 %d 次请求，期望 1", srv.count())
	}
}

// TestHTTPDroppedErrorStillForwards 是最容易写错的一处：
// 发现层在丢弃了部分实例时会「返回可用实例 + 同时返回错误」，
// 若把 err != nil 当作失败，一个投毒实例就能让整个命名空间 503。
func TestHTTPDroppedErrorStillForwards(t *testing.T) {
	srv := newMCPServer(t, echoHandler)
	droppedErr := &discovery.DroppedError{Service: "blender-svc", Count: 2}

	f, _ := newHTTPFactory(t, instancesOf(srv.Server), droppedErr)

	b, err := f.Open(context.Background(), httpNamespace("blender-svc", 5000))
	if err != nil {
		t.Fatalf("存在可用实例时不应失败，实际: %v", err)
	}
	defer b.Close()

	if _, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":9,"method":"ping"}`)); err != nil {
		t.Fatalf("应当正常转发，实际: %v", err)
	}
}

// TestHTTPNoInstancesIsNoBackend 验证没有实例时映射成可重试的 ErrNoBackend。
func TestHTTPNoInstancesIsNoBackend(t *testing.T) {
	f, _ := newHTTPFactory(t, nil, discovery.ErrNoInstances)

	_, err := f.Open(context.Background(), httpNamespace("gone-svc", 5000))
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
}

// TestHTTPResolverFailureIsNoBackend 验证注册中心故障也归为上游客不可用。
func TestHTTPResolverFailureIsNoBackend(t *testing.T) {
	f, _ := newHTTPFactory(t, nil, errors.New("rnacos: 502 bad gateway"))

	_, err := f.Open(context.Background(), httpNamespace("svc", 5000))
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
}

// TestHTTPRejectsWrongTransport 验证 stdio 命名空间不会被 HTTP 工厂接走。
func TestHTTPRejectsWrongTransport(t *testing.T) {
	f, _ := newHTTPFactory(t, nil, nil)
	ns := httpNamespace("svc", 5000)
	ns.Transport = store.TransportStdio

	_, err := f.Open(context.Background(), ns)
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
	if !strings.Contains(err.Error(), store.TransportHTTP) {
		t.Fatalf("错误信息应点明期望的传输方式，实际: %v", err)
	}
}

// TestHTTPRejectsMissingService 验证没配 discovery_service 时报错清晰。
func TestHTTPRejectsMissingService(t *testing.T) {
	f, _ := newHTTPFactory(t, nil, nil)
	ns := httpNamespace("   ", 5000)

	_, err := f.Open(context.Background(), ns)
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "discovery_service") {
		t.Fatalf("错误信息应指出缺少的配置项，实际: %v", err)
	}
}

// TestHTTPRoundRobinDistributes 验证多个实例之间真的轮询。
func TestHTTPRoundRobinDistributes(t *testing.T) {
	a := newMCPServer(t, echoHandler)
	b := newMCPServer(t, echoHandler)

	f, _ := newHTTPFactory(t, instancesOf(a.Server, b.Server), nil)
	ns := httpNamespace("svc", 5000)

	for i := 0; i < 10; i++ {
		backend, err := f.Open(context.Background(), ns)
		if err != nil {
			t.Fatalf("第 %d 次 Open 失败: %v", i, err)
		}
		_ = backend.Close()
	}

	// Open 只选实例、不发请求：建连阶段就去打上游会让「连上但没说话」的
	// 会话凭空在上游留下一串请求。
	if a.count() != 0 || b.count() != 0 {
		t.Fatalf("Open 不应发起转发，实际 a=%d b=%d", a.count(), b.count())
	}

	// 直接观察轮询结果：10 次选择应当 5/5 分配。
	if got := pickCounts(f, "svc", instancesOf(a.Server, b.Server), 10); got["a"] != 5 || got["b"] != 5 {
		t.Fatalf("轮询不均：%v", got)
	}
}

// pickCounts 通过 pick 直接统计选择分布，避免依赖真实网络。
func pickCounts(f *HTTPFactory, service string, insts []discovery.Instance, n int) map[string]int {
	out := map[string]int{"a": 0, "b": 0}
	for i := 0; i < n; i++ {
		chosen := f.pick(service, insts)
		if chosen.Port == insts[0].Port {
			out["a"]++
		} else {
			out["b"]++
		}
	}
	return out
}

// TestHTTPWeightedPickRespectsWeight 验证权重生效。
func TestHTTPWeightedPickRespectsWeight(t *testing.T) {
	f, _ := newHTTPFactory(t, nil, nil)
	insts := []discovery.Instance{
		{IP: "127.0.0.1", Port: 1001, Weight: 3, Healthy: true},
		{IP: "127.0.0.1", Port: 1002, Weight: 1, Healthy: true},
	}

	counts := map[int]int{}
	for i := 0; i < 40; i++ {
		counts[f.pick("svc", insts).Port]++
	}
	if counts[1001] != 30 || counts[1002] != 10 {
		t.Fatalf("加权轮询分布不对：%v", counts)
	}
}

// TestHTTPSessionIDIsSentAndStored 验证上游下发的会话 id 被保存并回传。
func TestHTTPSessionIDIsSentAndStored(t *testing.T) {
	var mu sync.Mutex
	var seen []string

	srv := newMCPServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Mcp-Session-Id"))
		mu.Unlock()
		w.Header().Set("Mcp-Session-Id", "upstream-sess-1")
		echoHandler(w, r, body)
	})

	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)
	b, err := f.Open(context.Background(), httpNamespace("svc", 5000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	// 第一次：没有会话 id，上游下发一个。
	if _, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`)); err != nil {
		t.Fatalf("首次 Forward 失败: %v", err)
	}
	// 第二次：必须带上第一次拿到的会话 id。
	if _, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)); err != nil {
		t.Fatalf("第二次 Forward 失败: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("上游收到 %d 次，期望 2", len(seen))
	}
	if seen[0] != "" {
		t.Fatalf("首次请求不应带会话 id，实际 %q", seen[0])
	}
	if seen[1] != "upstream-sess-1" {
		t.Fatalf("第二次请求应带上游下发的会话 id，实际 %q", seen[1])
	}
}

// TestHTTPUpstreamSessionLostClearsState 验证上游 404 时清掉会话 id 并报专用错误。
func TestHTTPUpstreamSessionLostClearsState(t *testing.T) {
	call := 0
	var mu sync.Mutex

	srv := newMCPServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		mu.Lock()
		call++
		n := call
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Mcp-Session-Id", "sess-abc")
			echoHandler(w, r, body)
			return
		}
		if r.Header.Get("Mcp-Session-Id") == "sess-abc" {
			// 模拟上游重启后不认识旧会话。
			w.WriteHeader(http.StatusNotFound)
			return
		}
		echoHandler(w, r, body)
	})

	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)
	b, err := f.Open(context.Background(), httpNamespace("svc", 5000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	if _, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`)); err != nil {
		t.Fatalf("首次 Forward 失败: %v", err)
	}

	_, err = b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`))
	if !errors.Is(err, ErrUpstreamSessionLost) {
		t.Fatalf("期望 ErrUpstreamSessionLost，实际: %v", err)
	}

	// 会话 id 必须已被清掉：下一次不带旧 id，因此上游不会再 404。
	if _, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`)); err != nil {
		t.Fatalf("清掉会话 id 后应当恢复，实际: %v", err)
	}
}

// TestHTTPNotificationReturnsNil 验证通知（无 id）不发请求体、只回 202。
func TestHTTPNotificationReturnsNil(t *testing.T) {
	srv := newMCPServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(http.StatusAccepted)
	})

	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)
	b, err := f.Open(context.Background(), httpNamespace("svc", 5000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	out, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	if err != nil {
		t.Fatalf("通知不应失败: %v", err)
	}
	if out != nil {
		t.Fatalf("通知不应返回响应体，实际: %s", out)
	}
}

// TestHTTPExtractsFromSSEResponse 验证上游用 SSE 形态回消息时也能取到结果。
func TestHTTPExtractsFromSSEResponse(t *testing.T) {
	srv := newMCPServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal(body, &req)

		w.Header().Set("Content-Type", "text/event-stream")
		// 先来一条无关的服务端通知，再来真正的响应。
		fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n")
		fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"ok\":true}}\n\n", req.ID)
	})

	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)
	b, err := f.Open(context.Background(), httpNamespace("svc", 5000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	out, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":"sse-1","method":"tools/call"}`))
	if err != nil {
		t.Fatalf("SSE 响应解析失败: %v", err)
	}
	if !strings.Contains(string(out), `"ok":true`) {
		t.Fatalf("取到的消息不对: %s", out)
	}
}

// TestHTTPExtractsFromBatchResponse 验证批量响应里挑出 id 匹配的那条。
func TestHTTPExtractsFromBatchResponse(t *testing.T) {
	srv := newMCPServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `[{"jsonrpc":"2.0","id":999,"result":{"other":1}},{"jsonrpc":"2.0","id":%s,"result":{"mine":1}}]`, req.ID)
	})

	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)
	b, err := f.Open(context.Background(), httpNamespace("svc", 5000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	out, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":42,"method":"ping"}`))
	if err != nil {
		t.Fatalf("批量响应解析失败: %v", err)
	}
	if !strings.Contains(string(out), `"mine":1`) {
		t.Fatalf("应当挑出 id=42 的那条，实际: %s", out)
	}
}

// TestHTTPUpstream5xxIsNotRetried 验证 5xx 原样上报、不切换实例。
//
// 这是正确性红线：一次「创建」类调用如果被重试两遍，用户会看到两份数据。
func TestHTTPUpstream5xxIsNotRetried(t *testing.T) {
	broken := newMCPServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	})
	healthy := newMCPServer(t, echoHandler)

	f, _ := newHTTPFactory(t, instancesOf(broken.Server, healthy.Server), nil)
	ns := httpNamespace("svc", 5000)

	// 反复开连，直到选中坏实例为止（轮询会把两者都用到）。
	var sawError bool
	for i := 0; i < 4; i++ {
		b, err := f.Open(context.Background(), ns)
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		_, ferr := b.Forward(context.Background(),
			json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`))
		_ = b.Close()
		if ferr != nil {
			sawError = true
			break
		}
	}
	if !sawError {
		t.Fatal("坏实例应当返回错误")
	}

	// 坏实例只应被请求一次：拿到 500 后不允许自动换实例重试。
	if broken.count() != 1 {
		t.Fatalf("坏实例收到 %d 次请求，期望 1（5xx 不得重试）", broken.count())
	}
}

// TestHTTPDialFailurePutsInstanceInCooldown 验证连不上时进入避让期。
func TestHTTPDialFailurePutsInstanceInCooldown(t *testing.T) {
	// 起一个立刻关掉的监听，保证端口上没有服务。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	_ = ln.Close()

	dead := discovery.Instance{IP: "127.0.0.1", Port: addr.Port, Weight: 1, Healthy: true}
	healthy := newMCPServer(t, echoHandler)

	f, _ := newHTTPFactory(t, append([]discovery.Instance{dead}, instancesOf(healthy.Server)...), nil)

	b, err := f.Open(context.Background(), httpNamespace("svc", 3000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	_, ferr := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	_ = b.Close()

	if ferr == nil {
		t.Skip("本次轮询恰好选中健康实例，跳过避让断言")
	}
	if !strings.Contains(ferr.Error(), "连不上") {
		t.Fatalf("错误信息应指出连接失败，实际: %v", ferr)
	}
	if f.DownInstances() != 1 {
		t.Fatalf("失败实例应进入避让期，DownInstances = %d", f.DownInstances())
	}

	// 避让期内新会话不应再选到它。
	for i := 0; i < 6; i++ {
		b2, err := f.Open(context.Background(), httpNamespace("svc", 3000))
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		if _, err := b2.Forward(context.Background(),
			json.RawMessage(`{"jsonrpc":"2.0","id":2,"method":"ping"}`)); err != nil {
			t.Fatalf("避让期后仍选中坏实例: %v", err)
		}
		_ = b2.Close()
	}
}

// TestHTTPTimeoutIsEnforced 验证命名空间级 timeout_ms 生效。
func TestHTTPTimeoutIsEnforced(t *testing.T) {
	srv := newMCPServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		time.Sleep(600 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)
	b, err := f.Open(context.Background(), httpNamespace("svc", 150))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	start := time.Now()
	_, err = b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"slow"}`))
	if err == nil {
		t.Fatal("超时应当报错")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("超时没有生效，耗时 %s", elapsed)
	}
}

// TestHTTPClosedBackendRejects 验证关掉的后端不再接受转发。
func TestHTTPClosedBackendRejects(t *testing.T) {
	srv := newMCPServer(t, echoHandler)
	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)

	b, err := f.Open(context.Background(), httpNamespace("svc", 3000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if _, err := b.Forward(context.Background(),
		json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)); err == nil {
		t.Fatal("已关闭的后端不应接受转发")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("重复 Close 应当安全: %v", err)
	}
}

// TestHTTPFactoryClosedRejectsOpen 验证关停后的工厂拒绝新会话。
func TestHTTPFactoryClosedRejectsOpen(t *testing.T) {
	srv := newMCPServer(t, echoHandler)
	f, _ := newHTTPFactory(t, instancesOf(srv.Server), nil)

	if err := f.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	_, err := f.Open(context.Background(), httpNamespace("svc", 3000))
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
}

// TestMatchMessageRejectsNotifications 验证 id 缺失/null 的消息不被当作响应。
func TestMatchMessageRejectsNotifications(t *testing.T) {
	if _, ok := matchMessage(json.RawMessage(`{"method":"x"}`), json.RawMessage("1")); ok {
		t.Fatal("没有 id 的消息不应当是响应")
	}
	if _, ok := matchMessage(json.RawMessage(`{"id":null}`), json.RawMessage("1")); ok {
		t.Fatal("id=null 的消息不应当是响应")
	}
	if _, ok := matchMessage(json.RawMessage(`{"id":2}`), json.RawMessage("1")); ok {
		t.Fatal("id 不匹配的消息不应当被当作响应")
	}
	if got, ok := matchMessage(json.RawMessage(`{"id":1,"result":{}}`), json.RawMessage("1")); !ok {
		t.Fatal("id 匹配的消息应当被选中")
	} else if !strings.Contains(string(got), `"result"`) {
		t.Fatalf("选中的内容不对: %s", got)
	}
}

// TestIsDialErrorDistinguishesPhases 验证「连不上」与「读写出错」被区分开。
func TestIsDialErrorDistinguishesPhases(t *testing.T) {
	dialErr := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	if !isDialError(dialErr) {
		t.Fatal("dial 阶段的错误应当被识别，否则无法安全重试")
	}
	readErr := &net.OpError{Op: "read", Err: errors.New("broken pipe")}
	if isDialError(readErr) {
		t.Fatal("读写阶段的错误不能被当成可以重试的 dial 错误")
	}
	if isDialError(errors.New("plain")) {
		t.Fatal("普通错误不应被识别为 dial 错误")
	}
}
