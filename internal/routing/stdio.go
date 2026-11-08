package routing

// 本文件是 transport=stdio：transport=stdio 的本地调试后端。
//
// 定位必须先说清楚，否则很容易做成错的东西：
//
//	集群主路径 是 transport=http —— 远端服务注册进 r-nacos，网关发现健康实例后
//	按 JSON-RPC 转发（http 反代工厂）。stdio 只是「本机调试某个 MCP
//	服务」这一条旁路，它的存在是为了让开发者在没有远端实例时也能把网关跑起来。
//
// 因此这里刻意不做进程共享，而是「一个 SSE 会话独占一个子进程」：
//
//	1. 多会话共用一个 stdio 进程需要重写 JSON-RPC id 并维护 (会话, 原 id) → 新 id
//	   的映射表，否则两个会话同时发 id=1 会把响应投递错人。这是可做但昂贵的机制，
//	   而它服务的场景（本机调试）根本不需要它。
//	2. MCP 的 initialize 握手是有状态的。共享进程时第二个会话再发 initialize，
//	   上游要么报错要么行为未定义；独占进程则每会话都是一次干净握手。
//	3. 进程崩溃只影响自己的会话，不会连带其他会话一起掉线。
//
// PoolSize 因此的含义是「该命名空间最多同时存在几个 stdio 进程」，也就是并发
// 上限而不是预先填满的池。超限时 Open 阻塞到有会话结束或 ctx 到期，返回 503
// 让客户端稍后重试 —— 这是可控的背压，比无限 spawn 把机器打死要好。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

const (
	// defaultStdioTimeout 与 store 对 timeout_ms 的兜底值保持一致（120000）。
	defaultStdioTimeout = 120 * time.Second

	// maxStdioLineBytes 是单条上游消息的上限。没有上限时，一个坏掉的上游
	// 可以靠输出一整块无换行的数据把网关内存吃光。
	maxStdioLineBytes = 32 << 20

	// maxStderrBytes 是保留的子进程错误输出尾部长度，用于报错时给出线索。
	maxStderrBytes = 4 << 10

	// stdioReadBuffer 是逐行读取的缓冲尺寸，超过即触发超长行判定。
	stdioReadBuffer = 64 << 10
)

// StdioFactory 是 transport=stdio 的 BackendFactory 实现。
//
// 它按命名空间维护并发闸门（PoolSize），并跟踪所有存活的子进程，
// 以便关停时统一回收 —— 否则二进制退出后这些进程会变成孤儿。
type StdioFactory struct {
	mu     sync.Mutex
	pools  map[string]*stdioPool
	live   map[*stdioBackend]struct{}
	logf   func(string, ...any)
	closed bool
}

// NewStdioFactory 构造工厂。logf 为 nil 时写标准日志。
func NewStdioFactory(logf func(string, ...any)) *StdioFactory {
	if logf == nil {
		logf = log.Printf
	}
	return &StdioFactory{
		pools: make(map[string]*stdioPool),
		live:  make(map[*stdioBackend]struct{}),
		logf:  logf,
	}
}

var _ BackendFactory = (*StdioFactory)(nil)

// Open 为一个 stdio 命名空间开出独占子进程。
//
// 失败原因一律包装成 ErrNoBackend（除子进程启动失败之外也用同一类），
// 这样上层不需要认识本实现的错误细节：池满、没配 command、传输方式不对，
// 对客户端都是「这个命名空间现在没有可用后端」。
func (f *StdioFactory) Open(ctx context.Context, ns *store.Namespace) (Backend, error) {
	if ns == nil {
		return nil, fmt.Errorf("%w: 命名空间为空", ErrNoBackend)
	}
	if ns.Transport != store.TransportStdio {
		return nil, fmt.Errorf("%w: 命名空间 %q 的传输方式是 %q，stdio 工厂只能处理 %q",
			ErrNoBackend, ns.Slug, ns.Transport, store.TransportStdio)
	}
	command := strings.TrimSpace(ns.Command)
	if command == "" {
		return nil, fmt.Errorf("%w: stdio 命名空间 %q 未配置 command", ErrNoBackend, ns.Slug)
	}

	pool, err := f.poolFor(ns)
	if err != nil {
		return nil, err
	}

	release, err := pool.acquire(ctx)
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(ns.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultStdioTimeout
	}

	b, err := spawnStdio(ns, command, timeout, f.logf, release)
	if err != nil {
		release()
		return nil, err
	}

	f.mu.Lock()
	f.live[b] = struct{}{}
	f.mu.Unlock()
	// onClose 必须在返回之前挂好：Open 一返回，调用方就可能立刻 Close。
	b.onClose = func() { f.forget(b) }
	return b, nil
}

// poolFor 取出（或建立）命名空间的并发闸门，并把 PoolSize 的最新值同步进去。
//
// 之所以每次都 setCapacity，是因为 PoolSize 是管理端可以随时调整的字段，
// 每次 Open 拿到的 ns 都是刚从库里读出来的行。
func (f *StdioFactory) poolFor(ns *store.Namespace) (*stdioPool, error) {
	key := string(ns.ID)
	if key == "" {
		key = ns.Slug
	}
	size := ns.PoolSize
	if size < 1 {
		size = 1
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, fmt.Errorf("%w: stdio 工厂已关闭", ErrNoBackend)
	}

	p, ok := f.pools[key]
	if !ok {
		p = &stdioPool{slug: ns.Slug, capacity: size}
		f.pools[key] = p
		return p, nil
	}
	p.slug = ns.Slug
	p.setCapacity(size)
	return p, nil
}

func (f *StdioFactory) forget(b *stdioBackend) {
	f.mu.Lock()
	delete(f.live, b)
	f.mu.Unlock()
}

// Shutdown 关闭所有存活子进程并拒绝后续 Open，返回被关闭的数量。
//
// 只关网关自己启动的进程：stdio 后端不接管用户已经跑起来的服务，也不发 stop 信号，
// 直接 Kill —— 这些是调试用的临时进程，没有需要优雅收尾的状态。
func (f *StdioFactory) Shutdown() int {
	f.mu.Lock()
	f.closed = true
	live := make([]*stdioBackend, 0, len(f.live))
	for b := range f.live {
		live = append(live, b)
	}
	f.live = make(map[*stdioBackend]struct{})
	f.mu.Unlock()

	for _, b := range live {
		_ = b.Close()
	}
	return len(live)
}

// Close 让工厂满足 io.Closer，语义等同 Shutdown。
func (f *StdioFactory) Close() error {
	f.Shutdown()
	return nil
}

// Stats 返回「命名空间 slug → 当前进程数」，供管理端与测试观察。
func (f *StdioFactory) Stats() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.pools))
	for _, p := range f.pools {
		out[p.slug] = p.count()
	}
	return out
}

// ---------------------------------------------------------------------------
// 并发闸门
// ---------------------------------------------------------------------------

// stdioPool 是带动态容量的计数信号量。
//
// 不用 semaphore.Weighted（x/sync）是因为这里需要「容量可以随时缩小/放大」，
// 而标准库层面手写一个计数 + 等待队列只有几十行，规则更好读、也好测。
type stdioPool struct {
	mu       sync.Mutex
	slug     string
	active   int
	capacity int
	waiters  []chan struct{}
}

// setCapacity 更新并发上限，并在有余量时唤醒等待者。
func (p *stdioPool) setCapacity(n int) {
	if n < 1 {
		n = 1
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capacity = n
	for p.active < p.capacity && len(p.waiters) > 0 {
		ch := p.waiters[0]
		p.waiters = p.waiters[1:]
		// 名额直接移交给等待者：active 不减不加，close 即「你拿到了」。
		p.active++
		close(ch)
	}
}

// acquire 抢占一个并发名额，返回归还函数。
//
// 归还函数用 sync.Once 包住：Close 重复调用、超时后 Close 再调一次，
// 都不能把名额重复还回去，否则上限会慢慢被撑破。
func (p *stdioPool) acquire(ctx context.Context) (func(), error) {
	p.mu.Lock()
	if p.active < p.capacity {
		p.active++
		p.mu.Unlock()
		return p.releaser(), nil
	}
	ch := make(chan struct{})
	p.waiters = append(p.waiters, ch)
	limit := p.capacity
	p.mu.Unlock()

	select {
	case <-ch:
		return p.releaser(), nil
	case <-ctx.Done():
		p.mu.Lock()
		removed := p.removeWaiter(ch)
		p.mu.Unlock()
		if !removed {
			// 取消的同时名额被移交了：必须还回去，否则永久漏一个。
			p.release()
		}
		return nil, fmt.Errorf("%w: stdio 进程池已满（上限 %d）: %v", ErrNoBackend, limit, ctx.Err())
	}
}

// removeWaiter 在等待队列里摘掉自己，返回是否摘到。
func (p *stdioPool) removeWaiter(ch chan struct{}) bool {
	for i, w := range p.waiters {
		if w == ch {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return true
		}
	}
	return false
}

func (p *stdioPool) releaser() func() {
	var once sync.Once
	return func() { once.Do(p.release) }
}

// release 归还名额：有等待者就移交，否则递减。
func (p *stdioPool) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.waiters) > 0 {
		ch := p.waiters[0]
		p.waiters = p.waiters[1:]
		close(ch)
		return
	}
	if p.active > 0 {
		p.active--
	}
}

func (p *stdioPool) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active
}

// ---------------------------------------------------------------------------
// 子进程后端
// ---------------------------------------------------------------------------

// stdioBackend 是一个独占的 stdio 子进程。
//
// 并发语义：Forward 之间串行（b.mu）。单进程单管道本来就不支持并发写入，
// 而「一个会话一个进程」的前提又保证了串行不会互相拖慢别人的会话。
type stdioBackend struct {
	slug    string
	cmd     *exec.Cmd
	stdin   *os.File
	stdout  *os.File
	reader  *bufio.Reader
	stderr  *tailWriter
	timeout time.Duration
	logf    func(string, ...any)
	release func()
	onClose func()

	mu      sync.Mutex
	closed  bool
	dead    bool
	deadErr error

	// waited 在 cmd.Wait 返回后关闭，用于 Close 等待真正的进程退出。
	waited chan struct{}
}

// spawnStdio 启动子进程并接管它的三条管道。
//
// 刻意不用 exec.CommandContext：传入的 ctx 是「本次 SSE 建连请求」的
// context，请求一结束就会被 cancel，若绑到进程上，子进程会在会话刚开始
// 时被顺手杀掉。子进程的生命周期只跟会话绑定，由 Close 负责。
func spawnStdio(ns *store.Namespace, command string, timeout time.Duration,
	logf func(string, ...any), release func()) (*stdioBackend, error) {

	cmd := exec.Command(command, ns.Args...)
	cmd.Env = mergeEnv(ns.Env)

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("%w: 创建 stdin 管道失败: %v", ErrNoBackend, err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		return nil, fmt.Errorf("%w: 创建 stdout 管道失败: %v", ErrNoBackend, err)
	}

	cmd.Stdin = stdinR
	cmd.Stdout = stdoutW
	cmd.Stderr = &tailWriter{max: maxStderrBytes}

	if err := cmd.Start(); err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		return nil, fmt.Errorf("%w: 启动 stdio 子进程 %q 失败: %v", ErrNoBackend, command, err)
	}

	// 父进程只保留自己那一端：子进程已持有 fd，这里不关就会让 EOF 永远不来，
	// 子进程读 stdin 时会一直阻塞在「等更多输入」而不是「输入结束」。
	_ = stdinR.Close()
	_ = stdoutW.Close()

	b := &stdioBackend{
		slug:    ns.Slug,
		cmd:     cmd,
		stdin:   stdinW,
		stdout:  stdoutR,
		reader:  bufio.NewReaderSize(stdoutR, stdioReadBuffer),
		stderr:  cmd.Stderr.(*tailWriter),
		timeout: timeout,
		logf:    logf,
		release: release,
		waited:  make(chan struct{}),
	}

	go func() {
		err := cmd.Wait()
		if err != nil && !b.isClosed() {
			logf("stdio 子进程 %q 已退出: %v (stderr: %s)", ns.Slug, err, b.stderrTail())
		}
		close(b.waited)
	}()

	return b, nil
}

// Forward 发一条 JSON-RPC 消息并等待对应响应。
//
// 通知（无 id）只写不读：MCP 的通知没有响应，等下去会一直阻塞到超时。
func (b *stdioBackend) Forward(ctx context.Context, message json.RawMessage) (json.RawMessage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.dead {
		return nil, b.failure()
	}

	compact, err := compactMessage(message)
	if err != nil {
		return nil, fmt.Errorf("stdio 后端 %q 收到的消息不是合法 JSON: %w", b.slug, err)
	}

	// stdio 传输是 NDJSON：一条消息必须在一行内，否则上游的逐行解析会错位。
	if _, err := b.stdin.Write(append(compact, '\n')); err != nil {
		b.markDead(fmt.Errorf("写入失败: %v", err))
		return nil, b.failure()
	}

	id, isRequest := messageID(compact)
	if !isRequest {
		return nil, nil
	}

	wait := b.timeout
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d < wait {
			wait = d
		}
	}

	type result struct {
		body json.RawMessage
		err  error
	}
	// 读协程只在 b.mu 之内启动；一旦超时就把进程标死，之后的 Forward
	// 不会再碰这个 reader，因此不存在两个协程同读一个 bufio.Reader。
	done := make(chan result, 1)
	go func() {
		body, err := b.readResponse(id)
		done <- result{body: body, err: err}
	}()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case r := <-done:
		if r.err != nil {
			b.markDead(r.err)
			return nil, b.failure()
		}
		return r.body, nil
	case <-timer.C:
		b.markDead(fmt.Errorf("上游 %s 在 %s 内没有响应", b.slug, wait))
		return nil, b.failure()
	case <-ctx.Done():
		b.markDead(ctx.Err())
		return nil, b.failure()
	}
}

// readResponse 逐行读到 id 匹配的那一条为止。
//
// 必须容错两件事，否则一个正常的上游也会被误判为故障：
//   - 服务端主动推送的通知（progress 之类）会插在响应之前；
//   - 有些实现会把日志打到 stdout，那不是 JSON。
//
// 两者都跳过并记录，绝不因为一行脏输出就毁掉整个会话。
func (b *stdioBackend) readResponse(wantID json.RawMessage) (json.RawMessage, error) {
	for {
		raw, err := readLineLimited(b.reader, maxStdioLineBytes)
		line := bytes.TrimSpace(raw)

		if len(line) > 0 {
			var probe struct {
				ID json.RawMessage `json:"id"`
			}
			if jsonErr := json.Unmarshal(line, &probe); jsonErr != nil {
				b.logf("stdio 后端 %q 的非 JSON 输出已忽略: %s", b.slug, truncate(line, 200))
			} else if len(probe.ID) == 0 || string(bytes.TrimSpace(probe.ID)) == "null" {
				// 服务端主动通知：当前 Backend 接口只支持「请求-响应」，
				// 这条通知无处可去。丢弃并记录，不阻断会话。
				b.logf("stdio 后端 %q 推送的通知已丢弃（当前不支持服务端主动推送）: %s",
					b.slug, truncate(line, 200))
			} else if idEqual(probe.ID, wantID) {
				return json.RawMessage(line), nil
			} else {
				b.logf("stdio 后端 %q 返回了非本请求的响应已忽略: %s", b.slug, truncate(line, 200))
			}
		}

		if err != nil {
			if errors.Is(err, errLineTooLong) {
				return nil, err
			}
			return nil, fmt.Errorf("读取失败: %v (stderr: %s)", err, b.stderrTail())
		}
	}
}

// Close 回收子进程并归还并发名额。重复调用安全。
//
// 顺序很关键：先关 stdin 让上游看到 EOF（能优雅退出的实现会自己走），
// 再 Kill 兜底（不听话的实现必须被干掉），最后等 Wait 返回，
// 否则进程会以僵尸状态留在系统里。
func (b *stdioBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	wasDead := b.dead
	b.dead = true
	if b.deadErr == nil {
		b.deadErr = errors.New("后端已关闭")
	}
	_ = b.stdin.Close()
	_ = b.stdout.Close()
	if !wasDead {
		_ = b.cmd.Process.Kill()
	}
	b.mu.Unlock()

	<-b.waited
	if b.release != nil {
		b.release()
	}
	if b.onClose != nil {
		b.onClose()
	}
	return nil
}

// markDead 把后端标为不可用并立刻杀掉子进程。
//
// 必须在 b.mu 之内调用。杀进程是为了让还在读管道的协程尽快拿到错误退出，
// 否则它会一直挂在那里等一行永远不会来的数据。
func (b *stdioBackend) markDead(cause error) {
	if b.dead {
		return
	}
	b.dead = true
	b.deadErr = cause
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	_ = b.stdin.Close()
	_ = b.stdout.Close()
}

// failure 把「为什么不可用」拼成一条可排查的错误。
//
// 刻意不包装 ErrNoBackend：后端已经起来了却失败，属于上游故障（502），
// 而不是「没有可用实例」（503 + Retry-After）。两者的告警含义完全不同。
func (b *stdioBackend) failure() error {
	msg := fmt.Sprintf("stdio 后端 %q 不可用", b.slug)
	if b.deadErr != nil {
		msg += ": " + b.deadErr.Error()
	}
	if tail := b.stderrTail(); tail != "" {
		msg += "（stderr: " + tail + "）"
	}
	return errors.New(msg)
}

func (b *stdioBackend) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *stdioBackend) stderrTail() string {
	if b.stderr == nil {
		return ""
	}
	return b.stderr.String()
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

// errLineTooLong 表示上游输出了一条超长单行。
var errLineTooLong = errors.New("上游输出的单条消息超过上限")

// readLineLimited 读一行，但拒绝超过 max 字节的行。
//
// 用 ReadSlice + 手写累积而不是 ReadBytes：ReadBytes 会无上限地扩容，
// 一个吐了 4GB 无换行数据的坏上游足以把网关撑爆。
func readLineLimited(r *bufio.Reader, max int) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if len(buf)+len(chunk) > max {
			return nil, errLineTooLong
		}
		buf = append(buf, chunk...)
		if err == nil {
			return buf, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return buf, err
	}
}

// compactMessage 把消息压成单行。
//
// 客户端可能发来缩进良好的多行 JSON，直接写进管道会让上游的逐行协议错位；
// json.Compact 顺带保证了「写出去的确实是合法 JSON」。
func compactMessage(message json.RawMessage) ([]byte, error) {
	if len(bytes.TrimSpace(message)) == 0 {
		return nil, errors.New("消息为空")
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, message); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// messageID 取出请求 id，并区分「请求」与「通知」。
func messageID(compact []byte) (json.RawMessage, bool) {
	var probe struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(compact, &probe); err != nil {
		return nil, false
	}
	id := bytes.TrimSpace(probe.ID)
	if len(id) == 0 || string(id) == "null" {
		return nil, false
	}
	return json.RawMessage(id), true
}

// idEqual 比较两个 id。
//
// 先做字节比较（绝大多数实现会原样回传），再退化成结构比较：
// 有的实现会把 1 回成 1.0，字节不等但语义相等，不该被当成「不是我的响应」。
func idEqual(a, b json.RawMessage) bool {
	if bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b)) {
		return true
	}
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// mergeEnv 合并系统环境与命名空间声明的环境变量。
//
// 走 map 而不是直接追加切片：Windows 的环境变量名不区分大小写，
// 重复键的行为未定义，先去重再排序可以得到稳定、可复现的环境。
func mergeEnv(extra map[string]string) []string {
	if len(extra) == 0 {
		return os.Environ()
	}
	merged := make(map[string]string, len(extra)+16)
	for _, kv := range os.Environ() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		merged[strings.ToUpper(k)] = v
	}
	for k, v := range extra {
		merged[strings.ToUpper(k)] = v
	}
	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// tailWriter 只保留最后 max 字节，用来在报错时给出 stderr 线索。
type tailWriter struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (w *tailWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	if len(w.buf) > w.max {
		w.buf = w.buf[len(w.buf)-w.max:]
	}
	return len(p), nil
}

func (w *tailWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.TrimSpace(string(w.buf))
}

// truncate 截断日志里的长内容。
func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
