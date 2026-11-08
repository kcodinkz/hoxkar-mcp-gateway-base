// fake_slow_mcp 是一个「故意很慢」的远端 HTTP MCP 后端。
//
// 它是 M3（命名空间路由与 HTTP 转发）的测试替身，不是生产代码，所以刻意
// 不依赖 mcp-go 的服务端实现：
//
//   - 数据库里 http 命名空间的默认 endpoint_path 是 /mcp，而 mcp-go v0.22 的
//     服务端只有 SSE 传输（长连接 + message 端点回写），网关转发要改写 URL、
//     维持连接，验证成本极高。真实集群里后端说的是「一个 POST 一个响应」的
//     JSON-RPC over HTTP，网关只需要反向代理，这里就照着这个形态做。
//   - 替身必须能被完全控制：慢多久、回什么、报什么错，都由命令行参数决定。
//     用第三方服务端反而要先摸清它在哪里睡、错误怎么包。
//
// 支持的能力（够验收用，不追求完整）：
//
//	initialize          → protocolVersion 2024-11-05，capabilities.tools
//	notifications/*     → 202 无响应体（通知没有 id，不能回 JSON-RPC 响应）
//	ping                → {}
//	tools/list          → echo / slow_add / whoami
//	tools/call          → 按 latency 睡够再返回
//
// 另外两个端点：
//
//	GET /healthz        → 恒 200，用于探活
//
// 用法：
//
//	go run ./examples/fake_slow_mcp -listen :18080
//	go run ./examples/fake_slow_mcp -listen :18080 -registry http://127.0.0.1:8848 -service mcp-demo
//
// 带 -registry 时它会在启动时把自己注册进 r-nacos 并持续心跳，
// 于是网关侧的「注册 → 发现 → 转发」整条链路可以在本机跑通。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// JSON-RPC 信封
// ---------------------------------------------------------------------------

// rpcRequest 是请求的最小形态。id 用 RawMessage 而不是 int/string，
// 因为 MCP 允许数字与字符串两种 id，原样回显才不会让客户端对不上号。
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// hasID 判断是不是请求（有 id）而不是通知（无 id）。
// 通知必须回 202 且不带响应体，否则客户端会把它当成重复响应。
func (r rpcRequest) hasID() bool {
	return len(r.ID) > 0 && string(r.ID) != "null"
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// JSON-RPC 2.0 预定义错误码。
const (
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternal       = -32603
)

// ---------------------------------------------------------------------------
// MCP 结果结构
// ---------------------------------------------------------------------------

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type callToolResult struct {
	Content []textContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

type toolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ---------------------------------------------------------------------------
// backend
// ---------------------------------------------------------------------------

// backend 持有这个替身的全部可调行为。
type backend struct {
	// name 是替身自报的服务名，会出现在 serverInfo 与日志里。
	name string
	// latency 是每次 tools/call 的固定睡眠时长。
	latency time.Duration
	// identity 形如 127.0.0.1:18080，用于 whoami 验证负载均衡是否真的切换了实例。
	identity string
	// startedAt 便于 whoami 与健康检查展示存活时长。
	startedAt time.Time
}

func (b *backend) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","instance":%q,"uptime_s":%d}`,
		b.identity, int(time.Since(b.startedAt).Seconds()))
}

// handleMCP 是唯一的 MCP 入口。
//
// 只接受 POST：streamable HTTP 的 GET 是「可选的 SSE 流」，网关的转发路径
// 用不到，明确回 405 比返回半个流更能帮人定位问题。
func (b *backend) handleMCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "只支持 POST /mcp", http.StatusMethodNotAllowed)
		return
	}

	// 4 MiB 上限：替身不需要接受大 payload，但也不该被一个畸形请求拖住。
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
	if err != nil {
		http.Error(w, "读取请求体失败: "+err.Error(), http.StatusBadRequest)
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPCError(w, nil, codeInvalidParams, "请求体不是合法 JSON-RPC: "+err.Error())
		return
	}

	// 通知没有 id：执行完事实上的副作用后回 202，绝不返回 JSON-RPC 响应。
	if !req.hasID() {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	switch req.Method {
	case "initialize":
		writeRPCResult(w, req.ID, map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]any{
				"tools": map[string]any{"listChanged": false},
			},
			"serverInfo": map[string]any{
				"name":    b.name,
				"version": "0.1.0",
			},
		})

	case "ping":
		writeRPCResult(w, req.ID, map[string]any{})

	case "tools/list":
		writeRPCResult(w, req.ID, map[string]any{"tools": b.tools()})

	case "tools/call":
		b.handleCallTool(w, r, req)

	default:
		writeRPCError(w, req.ID, codeMethodNotFound, "未实现的方法: "+req.Method)
	}
}

func (b *backend) tools() []toolDef {
	return []toolDef{
		{
			Name:        "echo",
			Description: "回显输入文本；用来验证最长路径的转发与超时",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"text": map[string]any{"type": "string", "description": "要回显的文本"},
				},
				"required": []string{"text"},
			},
		},
		{
			Name:        "slow_add",
			Description: "两数相加，可指定耗时；用来稳定复现慢后端",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"a":        map[string]any{"type": "number"},
					"b":        map[string]any{"type": "number"},
					"delay_ms": map[string]any{"type": "number", "description": "覆盖默认睡眠时长（毫秒）"},
				},
				"required": []string{"a", "b"},
			},
		},
		{
			Name:        "whoami",
			Description: "返回当前处理请求的实例标识；用来验证 round-robin 是否真的换实例",
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		},
	}
}

func (b *backend) handleCallTool(w http.ResponseWriter, r *http.Request, req rpcRequest) {
	var params struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			writeRPCError(w, req.ID, codeInvalidParams, "params 解析失败: "+err.Error())
			return
		}
	}

	// whoami 不睡：它存在的意义是探测「这次落到了哪个实例」，
	// 睡眠只会让负载均衡的观测变慢。
	if params.Name == "whoami" {
		writeRPCResult(w, req.ID, callToolResult{
			Content: []textContent{{Type: "text", Text: b.identity}},
		})
		return
	}

	delay := b.latency
	if raw, ok := params.Arguments["delay_ms"]; ok {
		if ms, ok := toFloat(raw); ok && ms >= 0 {
			delay = time.Duration(ms) * time.Millisecond
		}
	}

	// 睡眠必须可被打断：客户端断开时请求 ctx 会取消，
	// 替身若继续睡下去，压测时就分不清「后端慢」和「后端卡死」。
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			// 客户端已放弃，写响应没有意义，也不该被记成后端错误。
			return
		}
	}

	switch params.Name {
	case "echo":
		text, _ := params.Arguments["text"].(string)
		writeRPCResult(w, req.ID, callToolResult{
			Content: []textContent{{Type: "text", Text: text}},
		})

	case "slow_add":
		a, _ := toFloat(params.Arguments["a"])
		bb, _ := toFloat(params.Arguments["b"])
		writeRPCResult(w, req.ID, callToolResult{
			Content: []textContent{{
				Type: "text",
				Text: fmt.Sprintf("%v（实例 %s，耗时 %s）", a+bb, b.identity, delay),
			}},
		})

	default:
		// 未知工具按 MCP 约定返回 isError 结果而不是 JSON-RPC error：
		// 前者是「工具调用失败」，后者是「协议层失败」，网关侧的计费口径不同。
		writeRPCResult(w, req.ID, callToolResult{
			Content: []textContent{{Type: "text", Text: "未知工具: " + params.Name}},
			IsError: true,
		})
	}
}

// toFloat 把 JSON 解出来的数字统一成 float64。
// encoding/json 默认给 float64，但测试里可能塞 int/string，这里一并容忍。
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	resp := rpcResponse{JSONRPC: "2.0", ID: id, Result: result}
	writeJSON(w, http.StatusOK, resp)
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	// 解析失败时 id 可能不存在，此时按规范填 null。
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	resp := rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}}
	writeJSON(w, http.StatusOK, resp)
}

// writeJSON 统一出口：协议层错误也是 200 + JSON-RPC error 体，
// 只有「请求根本进不来」才用 HTTP 状态码。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("写响应失败: %v", err)
	}
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	listen := flag.String("listen", ":18080", "HTTP 监听地址")
	advertiseIP := flag.String("advertise-ip", "", "注册到注册中心的 IP，空 = 从 -listen 推断，兜底 127.0.0.1")
	service := flag.String("service", "mcp-demo", "服务名（不含分组前缀）")
	registryAddr := flag.String("registry", "", "r-nacos 地址，如 http://127.0.0.1:8848；空 = 不注册")
	namespaceID := flag.String("namespace", "public", "注册中心命名空间 ID")
	group := flag.String("group", "DEFAULT_GROUP", "注册中心分组")
	latency := flag.Duration("latency", 2*time.Second, "tools/call 的默认睡眠时长，0 = 不睡")
	heartbeat := flag.Duration("heartbeat", 5*time.Second, "心跳间隔")
	flag.Parse()

	host, portStr, err := net.SplitHostPort(*listen)
	if err != nil {
		log.Fatalf("非法监听地址 %q: %v", *listen, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("非法端口 %q: %v", portStr, err)
	}

	// 注册进注册中心的 IP 必须是自己能被别人访问到的地址。
	// 监听 0.0.0.0/空 时推断不出对外地址，退回 127.0.0.1（单机联调场景）。
	ip := *advertiseIP
	if ip == "" {
		ip = host
	}
	if ip == "" || ip == "0.0.0.0" || ip == "::" {
		ip = "127.0.0.1"
	}

	be := &backend{
		name:      *service,
		latency:   *latency,
		identity:  net.JoinHostPort(ip, strconv.Itoa(port)),
		startedAt: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", be.handleMCP)
	mux.HandleFunc("/healthz", be.handleHealth)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var reg *registrar
	if *registryAddr != "" {
		reg = newRegistrar(registrarConfig{
			registryAddr: *registryAddr,
			namespace:    *namespaceID,
			group:        *group,
			service:      *service,
			ip:           ip,
			port:         port,
			metadata: map[string]string{
				// 元数据只是给人看的：网关的 endpoint_path 来自命名空间模板（/mcp），
				// 不从实例元数据里读，所以这里写清楚不等于网关会用它。
				"transport": "http",
				"endpoint":  "/mcp",
				"latency":   latency.String(),
			},
		})
		go reg.run(ctx, *heartbeat)
	}

	go func() {
		log.Printf("假慢 MCP 后端已启动：listen=%s identity=%s service=%s latency=%s registry=%q",
			*listen, be.identity, *service, *latency, *registryAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("收到退出信号，正在关闭")

	// 先注销再从注册中心摘掉，避免网关在关停窗口里继续把请求转进来。
	if reg != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		reg.deregister(shutdownCtx)
		cancel()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("优雅关闭失败: %v", err)
	}
}
