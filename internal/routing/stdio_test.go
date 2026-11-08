package routing

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// TestStdioHelperProcess 不是真正的测试：它被 stdio 工厂当作「上游 MCP 服务」启动。
//
// 用测试二进制自身当子进程，是为了不引入任何外部依赖（不依赖 python/node），
// 也不在仓库里塞一个假的 .exe。行为由请求的 method 决定：
//
//	echo   → 回 {"result":{"method":..., "echo":<params>}}
//	noise  → 先输出一行非 JSON 和一条服务端通知，再回正响应（验证跳过逻辑）
//	slow   → 睡 3 秒再回（验证超时与杀进程）
//	die    → 直接退出（验证进程死亡后的错误路径）
func TestStdioHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64<<10), 4<<20)
	out := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(out)

	emit := func(v any) {
		_ = enc.Encode(v)
		_ = out.Flush()
	}

	for in.Scan() {
		line := bytes.TrimSpace(in.Bytes())
		if len(line) == 0 {
			continue
		}

		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(line, &msg); err != nil {
			fmt.Fprintln(os.Stderr, "helper: 收到非 JSON 输入")
			continue
		}

		switch msg.Method {
		case "die":
			os.Exit(3)
		case "noise":
			fmt.Fprintln(os.Stdout, "this line is not json")
			emit(map[string]any{"jsonrpc": "2.0", "method": "notifications/progress"})
		case "slow":
			time.Sleep(3 * time.Second)
		}

		if len(msg.ID) == 0 || string(bytes.TrimSpace(msg.ID)) == "null" {
			// 通知：不产生响应。
			continue
		}
		emit(map[string]any{
			"jsonrpc": "2.0",
			"id":      json.RawMessage(msg.ID),
			"result": map[string]any{
				"method": msg.Method,
				"echo":   json.RawMessage(msg.Params),
			},
		})
	}
	os.Exit(0)
}

// stdioNamespace 构造一个指向 helper 子进程的 stdio 命名空间。
func stdioNamespace(poolSize, timeoutMS int) *store.Namespace {
	return &store.Namespace{
		ID:        idgen.ID("ns_stdio_test"),
		Slug:      "debug",
		Transport: store.TransportStdio,
		Command:   os.Args[0],
		Args:      []string{"-test.run=^TestStdioHelperProcess$"},
		Env:       map[string]string{"GO_WANT_HELPER_PROCESS": "1"},
		PoolSize:  poolSize,
		TimeoutMS: timeoutMS,
		Enabled:   true,
	}
}

func mustForward(t *testing.T, b Backend, body string) string {
	t.Helper()
	out, err := b.Forward(context.Background(), json.RawMessage(body))
	if err != nil {
		t.Fatalf("Forward 失败: %v", err)
	}
	return string(out)
}

// TestStdioForwardRoundTrip 覆盖最基本的一条：请求写出、响应读回。
// 输入故意带换行与缩进，验证被压成单行后上游仍能解析。
func TestStdioForwardRoundTrip(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	b, err := f.Open(context.Background(), stdioNamespace(2, 10000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	got := mustForward(t, b, "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 7,\n  \"method\": \"echo\",\n  \"params\": {\"text\": \"你好\"}\n}")

	var resp struct {
		ID     int `json:"id"`
		Result struct {
			Method string `json:"method"`
			Echo   struct {
				Text string `json:"text"`
			} `json:"echo"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(got), &resp); err != nil {
		t.Fatalf("响应不是合法 JSON: %v (%s)", err, got)
	}
	if resp.ID != 7 {
		t.Fatalf("id = %d，期望 7", resp.ID)
	}
	if resp.Result.Method != "echo" {
		t.Fatalf("method = %q，期望 echo", resp.Result.Method)
	}
	if resp.Result.Echo.Text != "你好" {
		t.Fatalf("中文参数在合并为单行的过程中损坏了: %q", resp.Result.Echo.Text)
	}
}

// TestStdioNotificationDoesNotBlock 验证通知只写不读。
func TestStdioNotificationDoesNotBlock(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	b, err := f.Open(context.Background(), stdioNamespace(1, 1000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	done := make(chan error, 1)
	go func() {
		out, err := b.Forward(context.Background(),
			json.RawMessage(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
		if err == nil && out != nil {
			err = fmt.Errorf("通知不应返回响应体，实际收到 %s", out)
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("通知被阻塞了：Forward 在等一个永远不会来的响应")
	}
}

// TestStdioSkipsNoiseAndServerNotifications 验证非 JSON 行与服务端主动通知被跳过。
func TestStdioSkipsNoiseAndServerNotifications(t *testing.T) {
	var logs []string
	var mu sync.Mutex
	f := NewStdioFactory(func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	})
	defer f.Shutdown()

	b, err := f.Open(context.Background(), stdioNamespace(1, 5000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	got := mustForward(t, b, `{"jsonrpc":"2.0","id":"a-1","method":"noise","params":{}}`)
	if !strings.Contains(got, `"id":"a-1"`) {
		t.Fatalf("应当读到 id 匹配的响应，实际: %s", got)
	}

	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "非 JSON") {
		t.Fatalf("非 JSON 输出没有被记录，日志: %s", joined)
	}
	if !strings.Contains(joined, "通知已丢弃") {
		t.Fatalf("服务端通知没有被记录，日志: %s", joined)
	}
}

// TestStdioTimeoutKillsBackend 验证超时后立刻返回错误，且后端被标死。
func TestStdioTimeoutKillsBackend(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	b, err := f.Open(context.Background(), stdioNamespace(1, 150))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	start := time.Now()
	_, err = b.Forward(context.Background(), json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"slow"}`))
	if err == nil {
		t.Fatal("慢后端应当超时失败")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("超时没有生效，耗时 %s", elapsed)
	}
	if !strings.Contains(err.Error(), "没有响应") {
		t.Fatalf("错误信息应指出超时，实际: %v", err)
	}
	// 超时属于上游故障，不能被误判成「没有可用实例」（503 + Retry-After）。
	if errors.Is(err, ErrNoBackend) {
		t.Fatal("后端已启动但超时，不应包装成 ErrNoBackend")
	}

	// 已标死的后端必须立刻失败，而不是再等一次完整的超时。
	start = time.Now()
	if _, err := b.Forward(context.Background(), json.RawMessage(`{"jsonrpc":"2.0","id":2,"method":"echo"}`)); err == nil {
		t.Fatal("死掉的后端仍然成功响应了")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("死掉的后端应立刻失败，实际耗时 %s", elapsed)
	}
}

// TestStdioProcessDeathIsReported 验证子进程直接退出时的错误路径。
func TestStdioProcessDeathIsReported(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	b, err := f.Open(context.Background(), stdioNamespace(1, 3000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()

	if _, err := b.Forward(context.Background(), json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"die"}`)); err == nil {
		t.Fatal("子进程退出后 Forward 应当报错")
	}
}

// TestStdioRejectsWrongTransport 验证 http 命名空间不会被 stdio 工厂接走。
func TestStdioRejectsWrongTransport(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	ns := stdioNamespace(1, 1000)
	ns.Transport = store.TransportHTTP

	_, err := f.Open(context.Background(), ns)
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
	if !strings.Contains(err.Error(), store.TransportStdio) {
		t.Fatalf("错误信息应点明期望的传输方式，实际: %v", err)
	}
}

// TestStdioRejectsMissingCommand 验证没配 command 的 stdio 命名空间被拒绝。
func TestStdioRejectsMissingCommand(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	ns := stdioNamespace(1, 1000)
	ns.Command = "   "

	_, err := f.Open(context.Background(), ns)
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("期望 ErrNoBackend，实际: %v", err)
	}
}

// TestStdioPoolLimitBackpressure 验证 PoolSize 是并发上限：
// 超限时 Open 阻塞到 ctx 到期并返回 ErrNoBackend，会话结束后名额回到池里。
func TestStdioPoolLimitBackpressure(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	ns := stdioNamespace(1, 8000)

	first, err := f.Open(context.Background(), ns)
	if err != nil {
		t.Fatalf("第一个 Open 失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = f.Open(ctx, ns)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("池已满时第二个 Open 不应成功")
	}
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("池满应包装成 ErrNoBackend（上层 503），实际: %v", err)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("应当阻塞到 ctx 到期，实际只等了 %s", elapsed)
	}

	if stats := f.Stats(); stats["debug"] != 1 {
		t.Fatalf("等待超时后名额被泄漏了，Stats = %v", stats)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if stats := f.Stats(); stats["debug"] != 0 {
		t.Fatalf("会话结束后名额未归还，Stats = %v", stats)
	}

	// 名额归还后应当能再开一个。
	again, err := f.Open(context.Background(), ns)
	if err != nil {
		t.Fatalf("名额归还后 Open 失败: %v", err)
	}
	_ = again.Close()
}

// TestStdioCloseIsIdempotentAndReleasesOnce 验证重复 Close 不会重复归还名额。
func TestStdioCloseIsIdempotentAndReleasesOnce(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})
	defer f.Shutdown()

	// PoolSize=2：若重复 Close 会多还一次名额，第二次 Open 之后会出现
	// 「active 变成负数」而允许超过上限的会话，这里用 Stats 观察。
	ns := stdioNamespace(2, 8000)
	b, err := f.Open(context.Background(), ns)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("重复 Close 应当安全，实际: %v", err)
	}
	if stats := f.Stats(); stats["debug"] != 0 {
		t.Fatalf("重复 Close 导致计数错误，Stats = %v", stats)
	}
}

// TestStdioShutdownClosesLiveBackends 验证工厂关停会回收存活子进程。
func TestStdioShutdownClosesLiveBackends(t *testing.T) {
	f := NewStdioFactory(func(string, ...any) {})

	b, err := f.Open(context.Background(), stdioNamespace(1, 8000))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}

	if n := f.Shutdown(); n != 1 {
		t.Fatalf("Shutdown 关闭了 %d 个后端，期望 1", n)
	}

	if _, err := b.Forward(context.Background(), json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"echo"}`)); err == nil {
		t.Fatal("关停后的后端不应当还能转发")
	}

	if _, err := f.Open(context.Background(), stdioNamespace(1, 8000)); !errors.Is(err, ErrNoBackend) {
		t.Fatalf("关停后的工厂应当拒绝 Open，实际: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close 应当安全: %v", err)
	}
}

// TestStdioPoolCapacityGrowthWakesWaiter 直接测并发闸门：扩容后等待者被唤醒。
func TestStdioPoolCapacityGrowthWakesWaiter(t *testing.T) {
	p := &stdioPool{slug: "x", capacity: 1}

	release1, err := p.acquire(context.Background())
	if err != nil {
		t.Fatalf("第一个名额获取失败: %v", err)
	}

	got := make(chan struct{})
	go func() {
		release2, err := p.acquire(context.Background())
		if err == nil {
			release2()
		}
		close(got)
	}()

	select {
	case <-got:
		t.Fatal("容量为 1 时第二个 acquire 不应当立刻成功")
	case <-time.After(100 * time.Millisecond):
	}

	p.setCapacity(2)

	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("扩容后等待者没有被唤醒")
	}

	release1()
	if n := p.count(); n != 0 {
		t.Fatalf("全部归还后 count = %d，期望 0", n)
	}
}

// TestCompactMessageRejectsInvalid 验证非法 JSON 在写进管道前就被挡住。
func TestCompactMessageRejectsInvalid(t *testing.T) {
	if _, err := compactMessage(json.RawMessage("{not json")); err == nil {
		t.Fatal("非法 JSON 应当被拒绝")
	}
	if _, err := compactMessage(json.RawMessage("   ")); err == nil {
		t.Fatal("空消息应当被拒绝")
	}

	out, err := compactMessage(json.RawMessage("{\n\"a\": 1\n}"))
	if err != nil {
		t.Fatalf("合法 JSON 被拒绝: %v", err)
	}
	if bytes.ContainsRune(out, '\n') {
		t.Fatalf("压缩后仍含换行，stdio 的逐行协议会错位: %q", out)
	}
}

// TestIDEqualToleratesNumberForms 验证 1 与 1.0 被视作同一个 id。
func TestIDEqualToleratesNumberForms(t *testing.T) {
	if !idEqual(json.RawMessage("1"), json.RawMessage("1.0")) {
		t.Fatal("1 与 1.0 应当视为同一个 id")
	}
	if !idEqual(json.RawMessage(`"abc"`), json.RawMessage(`"abc"`)) {
		t.Fatal("相同字符串 id 应当相等")
	}
	if idEqual(json.RawMessage("1"), json.RawMessage("2")) {
		t.Fatal("不同 id 不应相等")
	}
}

// TestReadLineLimitedRejectsOversize 验证超长单行不会无限吃内存。
func TestReadLineLimitedRejectsOversize(t *testing.T) {
	long := strings.Repeat("x", 64)
	r := bufio.NewReaderSize(strings.NewReader(long), 16)

	_, err := readLineLimited(r, 32)
	if !errors.Is(err, errLineTooLong) {
		t.Fatalf("期望 errLineTooLong，实际: %v", err)
	}

	r = bufio.NewReaderSize(strings.NewReader("short line\nnext\n"), 16)
	line, err := readLineLimited(r, 64)
	if err != nil {
		t.Fatalf("正常行读取失败: %v", err)
	}
	if string(line) != "short line\n" {
		t.Fatalf("读到 %q", line)
	}
}

// TestMergeEnvOverridesCaseInsensitively 验证命名空间声明的环境变量能覆盖系统变量。
func TestMergeEnvOverridesCaseInsensitively(t *testing.T) {
	if len(mergeEnv(nil)) == 0 {
		t.Fatal("没有额外变量时也应返回系统环境")
	}

	merged := mergeEnv(map[string]string{"mcp_gw_test_key": "v1"})
	var found int
	for _, kv := range merged {
		if kv == "MCP_GW_TEST_KEY=v1" {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("环境变量未被规范化覆盖，出现 %d 次", found)
	}
}
