package gateway_test

// 本文件是回归护栏：旧启动方式（无数据库、无 -config）下
// 「单个 Bearer token」这条鉴权路径必须一字不变地继续可用。
//
// 为什么值得单独钉死：多租户模式引入的是 wrapper（API Key + Principal），
// 它和旧的 -oauth2Bearer 是两套完全不同的凭据体系，唯一共用的落点是
// serveMain 开头那一次 validateOAuth2Bearer。M2/M3 的改造动了 serveMain 的
// 分派逻辑（插入了 mcpRouter 分支），一旦有人把鉴权挪到分派之后，
// 或者把「空 token 直接放行」的短路删掉，出问题的恰恰是那些还没升级的老用户 ——
// 他们不会看日志，只会发现「连不上了」。所以这里把四个必须成立的语义逐条锁住：
//
//  1. 未配置 token 时一律放行（旧行为：不配就是不鉴权，不是「拒绝所有人」）；
//  2. 配置了 token 时，缺失 / 错误 / 形状不对的 Authorization 一律 401；
//  3. 形状必须严格是 `<scheme> <token>` 两段，scheme 大小写不敏感，token 精确相等；
//  4. 鉴权发生在分派之前 —— 连 /sse/xxx 这种非法路径、以及 /messages 这种
//     需要进一步查 session 的路径，都先被 401 拦住。
//
// 另外两条与「旧模式不认命名空间」有关的断言也在这里：
// 旧模式的 endpoint 事件下发的是单数 /message（新入口是复数 /messages），
// 以及 /sse/{ns} 在旧模式下仍然是 404 —— 这两点保证 M3 的路由没有被
// 意外地泄漏进旧启动路径。

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/gateway"
)

// legacyClient 带超时。
//
// 这不是为了「跑得快」，而是为了把失败模式改对：如果某个本该 401 的请求
// 意外被放行，它会进入 handleSSE 的长连接，io.ReadAll 永远读不到 EOF，
// 整个测试包会挂到 go test 的 10 分钟超时才带着 goroutine dump 退出。
// 加超时后同样的情况会在 5 秒内变成一条明确的 t.Fatalf。
var legacyClient = &http.Client{Timeout: 5 * time.Second}

// legacyToken 长度 29，刻意超过 config.Validate 对旧模式 16 字符的下限，
// 这样从配置一路到这里的 token 一定是「合法长度」，测试失败只可能是鉴权逻辑本身。
const legacyToken = "legacy-token-0123456789abcdef"

// newLegacyBearerServer 按 serveForever 的旧路径装配：
// 只有 SSE 端点 + 可选 token，没有 wrapper、没有挂载点、没有 mcpRouter。
//
// 三个「没有」正是旧模式的定义 —— 因此这里不传 WithBaseURL/WithSSEEndpoint，
// 让 NewSSEServer 的默认值（/sse 与单数 /message）生效，
// 顺便把默认端点也一并纳入回归范围。
func newLegacyBearerServer(t *testing.T, opts ...gateway.SSEOption) *httptest.Server {
	t.Helper()

	sse := gateway.NewSSEServer(server.NewMCPServer("legacy-bearer-test", "0.0.0"), opts...)
	srv := httptest.NewServer(sse)
	t.Cleanup(srv.Close)
	return srv
}

// doLegacy 发一个普通请求并读完整响应体，用于断言状态码与错误文案。
func doLegacy(t *testing.T, srv *httptest.Server, method, path, authorization, body string) (*http.Response, string) {
	t.Helper()

	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := legacyClient.Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取响应体失败: %v", err)
	}
	return resp, string(raw)
}

// openLegacySSE 建立 SSE 连接并解析 endpoint 事件里的投递地址。
//
// 与 namespaced_test.go 的 openSSE 的差别只有一处：这里可以带 Authorization，
// 因为旧模式的凭据在请求头里，而新模式的 Principal 是 wrapper 注入的。
func openLegacySSE(t *testing.T, srv *httptest.Server, path, authorization string) (*http.Response, string, string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("建连失败: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("建连状态 %d: %s", resp.StatusCode, raw)
	}

	buf := make([]byte, 512)
	n, err := resp.Body.Read(buf)
	if err != nil && err != io.EOF {
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

// TestLegacyNoTokenAllowsAnonymous 锁住「不配 token 就是不鉴权」这条短路。
//
// 这条最容易在重构里被误删：看起来像「鉴权被绕过了」的漏洞，
// 实际是旧用户的默认部署形态 —— 他们只在本机跑网关接一个 stdio 子进程。
func TestLegacyNoTokenAllowsAnonymous(t *testing.T) {
	srv := newLegacyBearerServer(t) // 不传 WithOAuth2Bearer

	resp, addr, _ := openLegacySSE(t, srv, "/sse", "")
	defer resp.Body.Close()

	if !strings.HasPrefix(addr, "/message?sessionId=") {
		t.Fatalf("投递地址 = %q，期望旧端点 /message?sessionId=…", addr)
	}
}

// TestLegacyBearerAcceptsExactToken 验证正确 token 能建连，
// 同时确认请求确实由旧的 handleSSE 服务（单数 /message）。
//
// 单数 / 复数的区别不是吹毛求疵：新入口 NamespacedMCP 下发的是 /messages，
// 若旧模式意外地用上了新入口，这里的断言会直接变红。
func TestLegacyBearerAcceptsExactToken(t *testing.T) {
	srv := newLegacyBearerServer(t, gateway.WithOAuth2Bearer(legacyToken))

	resp, addr, _ := openLegacySSE(t, srv, "/sse", "Bearer "+legacyToken)
	defer resp.Body.Close()

	if !strings.HasPrefix(addr, "/message?sessionId=") {
		t.Fatalf("投递地址 = %q，期望旧端点 /message?sessionId=…", addr)
	}
	if strings.Contains(addr, "/messages") {
		t.Fatalf("投递地址 = %q，出现了复数 /messages，说明请求被多租户入口接管了", addr)
	}
	if !strings.Contains(addr, "sessionId=") || strings.HasSuffix(addr, "sessionId=") {
		t.Fatalf("投递地址 = %q，缺少 sessionId", addr)
	}
}

// TestLegacyBearerSchemeIsCaseInsensitive 验证 scheme 大小写不敏感，
// 但 token 本体精确相等（大小写敏感）。
func TestLegacyBearerSchemeIsCaseInsensitive(t *testing.T) {
	srv := newLegacyBearerServer(t, gateway.WithOAuth2Bearer(legacyToken))

	for _, header := range []string{
		"Bearer " + legacyToken,
		"bearer " + legacyToken,
		"BEARER " + legacyToken,
		"BeArEr " + legacyToken,
	} {
		header := header
		t.Run(header[:6], func(t *testing.T) {
			resp, addr, _ := openLegacySSE(t, srv, "/sse", header)
			defer resp.Body.Close()
			if !strings.HasPrefix(addr, "/message?sessionId=") {
				t.Fatalf("投递地址 = %q", addr)
			}
		})
	}

	// token 本体大小写变了就必须拒绝。
	if _, body := doLegacy(t, srv, http.MethodGet, "/sse", "Bearer "+strings.ToUpper(legacyToken), ""); !strings.Contains(body, "Unauthorized") {
		t.Fatalf("大写的 token 竟然通过了: %q", body)
	}
}

// TestLegacyBearerRejectsBadAuthorization 覆盖所有「不该放行」的形状。
//
// 一次表驱动跑完，避免将来有人只补了「缺头」忘了「多空格」之类的边角。
func TestLegacyBearerRejectsBadAuthorization(t *testing.T) {
	srv := newLegacyBearerServer(t, gateway.WithOAuth2Bearer(legacyToken))

	cases := []struct {
		name   string
		header string
	}{
		{"完全没有 Authorization", ""},
		{"token 错误", "Bearer wrong-token-0123456789ab"},
		{"token 是正确值的前缀", "Bearer " + legacyToken[:len(legacyToken)-1]},
		{"token 多了尾巴", "Bearer " + legacyToken + "x"},
		{"缺 token", "Bearer"},
		{"多空格", "Bearer  " + legacyToken},
		{"scheme 不对", "Basic " + legacyToken},
		{"只是裸 token", legacyToken},
		// 注意：「尾部多空格」这一形状无法通过真实 HTTP 请求构造 ——
		// RFC 7230 的 OWS 规则会把它剥成正确 token，于是请求被放行、
		// 进入 SSE 长连接。要覆盖它只能直接调用 validateOAuth2Bearer，
		// 而那是包内符号，本文件是 gateway_test 外部包，故不在此断言。
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			resp, body := doLegacy(t, srv, http.MethodGet, "/sse", c.header, "")
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("状态 = %d，期望 401（body: %q）", resp.StatusCode, body)
			}
			if !strings.Contains(body, "Unauthorized") {
				t.Fatalf("body = %q，期望含 Unauthorized", body)
			}
		})
	}
}

// TestLegacyBearerGuardsBeforeDispatch 验证鉴权排在分派之前。
//
// 两个很有代表性的目标路径：
//   - /sse/blender：旧模式没有命名空间概念，正确 token 下应当是 404；
//     但 token 不对时必须先得到 401，而不是 404 —— 否则「路径存不存在」
//     会变成一个不需要凭据就能探测的侧信道。
//   - /messages：它需要继续查 session 才能处理，而旧实现里查完就会
//     deref 本地子进程连接。无 token 时必须在更早的地方就被拦下，
//     绝不能走到那一步。
func TestLegacyBearerGuardsBeforeDispatch(t *testing.T) {
	srv := newLegacyBearerServer(t, gateway.WithOAuth2Bearer(legacyToken))

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"SSE 带命名空间", http.MethodGet, "/sse/blender"},
		{"消息端点", http.MethodPost, "/messages"},
		{"消息端点带命名空间", http.MethodPost, "/messages/blender"},
		{"根路径", http.MethodGet, "/"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			resp, body := doLegacy(t, srv, c.method, c.path, "", "")
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("无凭据访问 %s %s 得到 %d，期望 401（body: %q）", c.method, c.path, resp.StatusCode, body)
			}
		})
	}

	// 凭据正确后，/sse/{ns} 在旧模式下仍是 404：旧启动路径不认命名空间。
	resp, body := doLegacy(t, srv, http.MethodGet, "/sse/blender", "Bearer "+legacyToken, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("状态 = %d，期望 404（body: %q）", resp.StatusCode, body)
	}
}

// TestLegacyBearerMessagePathRejectsUnknownSession 验证旧消息端点在通过鉴权后
// 仍然先做 session 校验，而不是直接去碰子进程连接。
//
// 这里必须用**单数** /message：它是 NewSSEServer 的默认消息端点，也是
// 旧版 config.yaml 里 message_path 的默认值。复数 /messages 是 M3 新入口的
// 约定，旧模式不认 —— 顺带在下面单独断言这一点。
//
// 这既是行为断言，也是一道安全网：旧实现里 forwardToStdio 会直接解引用
// s.conn，没有子进程时是 panic。用「不存在的 sessionId」覆盖这段，
// 可以保证校验顺序没有被后续改动倒过来。
func TestLegacyBearerMessagePathRejectsUnknownSession(t *testing.T) {
	srv := newLegacyBearerServer(t, gateway.WithOAuth2Bearer(legacyToken))

	// 先确认缺 sessionId 时给出的是参数错误，而不是 401 / panic。
	resp, body := doLegacy(t, srv, http.MethodPost, "/message", "Bearer "+legacyToken,
		`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("缺 sessionId 状态 = %d，期望 400（body: %q）", resp.StatusCode, body)
	}
	if !strings.Contains(body, "sessionId") {
		t.Fatalf("body = %q，期望提到 sessionId", body)
	}

	// sessionId 不存在时同样停在 400，不会继续往下走。
	resp, body = doLegacy(t, srv, http.MethodPost, "/message?sessionId=does-not-exist",
		"Bearer "+legacyToken, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("未知 session 状态 = %d，期望 400（body: %q）", resp.StatusCode, body)
	}
	if !strings.Contains(body, "Invalid session ID") {
		t.Fatalf("body = %q，期望含 Invalid session ID", body)
	}

	// 复数 /messages 只属于多租户入口；旧模式下应当 404，
	// 否则说明 M3 的路由被泄漏进了旧启动路径。
	resp, body = doLegacy(t, srv, http.MethodPost, "/messages?sessionId=does-not-exist",
		"Bearer "+legacyToken, `{"jsonrpc":"2.0","id":3,"method":"tools/list"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("复数 /messages 状态 = %d，期望 404（body: %q）", resp.StatusCode, body)
	}
}

// TestLegacyBearerIgnoresNonGetOnSSE 验证旧端点的方法约束没被分派改动带偏。
//
// 正确 token 下 POST /sse 应当得到 405（handleSSE 自身的方法校验），
// 而不是 401 或 404 —— 说明请求确实进入了旧 handler。
func TestLegacyBearerIgnoresNonGetOnSSE(t *testing.T) {
	srv := newLegacyBearerServer(t, gateway.WithOAuth2Bearer(legacyToken))

	resp, body := doLegacy(t, srv, http.MethodPost, "/sse", "Bearer "+legacyToken, `{}`)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("状态 = %d，期望 405（body: %q）", resp.StatusCode, body)
	}
}
