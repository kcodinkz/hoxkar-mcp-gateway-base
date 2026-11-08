package gateway

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/mark3labs/mcp-go/mcp"
)

type connection struct {
	stdin  io.Writer
	stdout io.Reader
	mutex  sync.Mutex
	reader *bufio.Reader
}

type McpCommand struct {
	Command string
	Args    []string
	Env     []string
}

type GatewaySSEServer struct {
	*SSEServer
	command McpCommand
}

func NewGatewaySSEServer(command McpCommand, sseServer *SSEServer) *GatewaySSEServer {
	return &GatewaySSEServer{
		SSEServer: sseServer,
		command:   command,
	}
}

// Start 起服务。有 -command 时先拉起本地子进程，没有时只起 HTTP 服务。
//
// 多租户模式本来就没有本地子进程：后端由 r-nacos 发现、由 routing 转发。
// 此时若仍然无条件 exec.Command("")，进程会在启动阶段就 fail fast，
// 得到一个与配置无关的 "exec: no command" —— 那是把「旧模式缺参数」
// 误报成了「新模式坏了」。因此这里按有无命令分两条路。
func (g *GatewaySSEServer) Start(addr string) error {
	if strings.TrimSpace(g.command.Command) == "" {
		log.Println("未指定 -command：跳过本地 stdio 子进程，仅提供 HTTP 服务")
		return g.SSEServer.Start(addr)
	}

	cmd := exec.Command(g.command.Command, g.command.Args...)
	cmd.Env = append(os.Environ(), g.command.Env...)

	if err := g.InitStdioConn(cmd); err != nil {
		return err
	}

	return g.SSEServer.Start(addr)
}

func (s *GatewaySSEServer) InitStdioConn(cmd *exec.Cmd) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	s.SSEServer.conn = &connection{
		stdin:  stdin,
		stdout: stdout,
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start MCP process: %w", err)
	}

	go func() {
		cmd.Wait()
		log.Println("MCP process exited")
	}()

	return nil
}

func (s *SSEServer) forwardToStdio(message json.RawMessage) (json.RawMessage, error) {
	var msgObj map[string]interface{}
	if err := json.Unmarshal(message, &msgObj); err == nil {
		if _, hasID := msgObj["id"]; !hasID {
			s.conn.mutex.Lock()
			defer s.conn.mutex.Unlock()

			if _, err := fmt.Fprintf(s.conn.stdin, "%s\n", message); err != nil {
				return nil, fmt.Errorf("write notification failed: %w", err)
			}
			return nil, nil
		}
	}

	s.conn.mutex.Lock()
	defer s.conn.mutex.Unlock()

	if s.conn.reader == nil {
		s.conn.reader = bufio.NewReader(s.conn.stdout)
	}

	if _, err := fmt.Fprintf(s.conn.stdin, "%s\n", message); err != nil {
		return nil, fmt.Errorf("write request failed: %w", err)
	}

	line, err := s.conn.reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	return json.RawMessage(line), nil
}

func (s *SSEServer) handleMessageToStdio(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSONRPCError(w, nil, mcp.INVALID_REQUEST, "Method not allowed")
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		s.writeJSONRPCError(w, nil, mcp.INVALID_PARAMS, "Missing sessionId")
		return
	}
	sessionI, ok := s.sessions.Load(sessionID)
	if !ok {
		s.writeJSONRPCError(w, nil, mcp.INVALID_PARAMS, "Invalid session ID")
		return
	}
	session := sessionI.(*sseSession)

	var rawMessage json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&rawMessage); err != nil {
		s.writeJSONRPCError(w, nil, mcp.PARSE_ERROR, "Parse error")
		return
	}

	// Process message through MCPServer
	response, err := s.forwardToStdio(rawMessage)
	if err != nil {
		s.writeJSONRPCError(w, nil, mcp.INTERNAL_ERROR, fmt.Sprintf("MCP communication error: %v", err))
		return
	}

	// Only send response if there is one (not for notifications)
	if response != nil {
		eventData, _ := json.Marshal(response)

		// Queue the event for sending via SSE
		select {
		case session.eventQueue <- fmt.Sprintf("event: message\ndata: %s\n\n", eventData):
			// Event queued successfully
		case <-session.done:
			// Session is closed, don't try to queue
		default:
			// Queue is full, could log this
		}

		// Send HTTP response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(response)
	} else {
		// For notifications, just send 202 Accepted with no body
		w.WriteHeader(http.StatusAccepted)
	}
}

func (s *SSEServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 控制面挂载点（如 /admin/v1/*）自带鉴权，必须排在 -oauth2Bearer 之前：
	// 管理 token 与 MCP 的 Bearer 是两套凭据，互相不能拦截。
	// 没有挂载点时这一段只是一个空循环，行为与改造前一致。
	for _, m := range s.mounts {
		if r.URL.Path == m.prefix || strings.HasPrefix(r.URL.Path, m.prefix+"/") {
			m.handler.ServeHTTP(w, r)
			return
		}
	}

	// wrapper（API Key 鉴权等）只包主路径。挂载点已经在上面的循环里返回了，
	// 因此不会被二次拦截 —— 这正是管理端「挂载而非包装」的落地点。
	// 没配 wrapper 时直接走 serveMain，与改造前零差异、无额外分配。
	if s.wrapper == nil {
		s.serveMain(w, r)
		return
	}
	s.mainHandler().ServeHTTP(w, r)
}

// mainHandler 返回套了 wrapper 的主路径处理器，只在首次调用时构建。
//
// 不在 Start 里包整个 SSEServer 的原因：挂载点是在 ServeHTTP 内部按前缀分派的，
// 若从外面整体包一层，/admin/v1/* 也会被 API Key 中间件拦下。
func (s *SSEServer) mainHandler() http.Handler {
	s.mainOnce.Do(func() {
		s.mainCached = s.wrapper(http.HandlerFunc(s.serveMain))
	})
	return s.mainCached
}

// serveMain 是 SSE 主路径（/sse、/messages）的实际分派逻辑。
func (s *SSEServer) serveMain(w http.ResponseWriter, r *http.Request) {
	if !s.validateOAuth2Bearer(r) {
		http.Error(w, "Unauthorized: Invalid or missing Bearer token", http.StatusUnauthorized)
		return
	}

	// 多租户模式：MCP 端点整段交给 NamespacedMCP。
	//
	// 必须排在校验之后：它的路由层要从 context 里读 Principal 做 RBAC 裁决，
	// 而 Principal 正是 wrapper 注入的。若挪到前面，请求会带着空身份进入
	// resolveNamespace，得到「缺少调用者身份」的 403 —— 看起来像权限配置错了，
	// 实际只是分派顺序错了。
	//
	// 只认 /sse、/sse/{ns}、/messages、/messages/{ns}。其余路径继续往下走，
	// 由旧的精确匹配收尾，因此旧客户端打错路径时仍然是 404 而不是别的。
	if s.mcpRouter != nil && s.mcpRouter.Handles(r.URL.Path) {
		s.mcpRouter.ServeHTTP(w, r)
		return
	}

	path := r.URL.Path
	// Use exact path matching rather than Contains
	ssePath := s.CompleteSsePath()
	if ssePath != "" && path == ssePath {
		s.handleSSE(w, r)
		return
	}
	messagePath := s.CompleteMessagePath()
	if messagePath != "" && path == messagePath {
		s.handleMessageToStdio(w, r)
		return
	}

	http.NotFound(w, r)
}

func (s *SSEServer) validateOAuth2Bearer(r *http.Request) bool {
	if s.oAuth2Bearer == "" {
		return true
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}

	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return false
	}

	token := parts[1]
	return token == s.oAuth2Bearer
}

func WithOAuth2Bearer(token string) SSEOption {
	return func(s *SSEServer) {
		s.oAuth2Bearer = token
	}
}
