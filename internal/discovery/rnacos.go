package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RnacosResolver 通过 Nacos OpenAPI v1 从 r-nacos 拉取健康实例。
//
// 事实来源：Rust 后端启动时向 r-nacos 注册自身并持续心跳（服务发现）。
// 网关只读不写，永远不会出现在注册表里。
//
// 注意 gRPC 端口（9848）不用于发现，只用于 Rust 侧的原生客户端；
// 网关走 HTTP 8848，少一个连接池，少一类故障。
type RnacosResolver struct {
	addr        string
	namespace   string
	group       string
	username    string
	password    string
	healthyOnly bool
	client      *http.Client

	// 令牌缓存。r-nacos 默认 tokenTtl 为 18000s，这里保守按 80% 使用
	tokenMu     sync.Mutex
	token       string
	tokenExpiry time.Time
}

// NewRnacos 构造 r-nacos 发现实现。addr 形如 http://127.0.0.1:8848。
func NewRnacos(addr, namespace, group, username, password string, timeout time.Duration, healthyOnly bool) *RnacosResolver {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	if namespace == "" {
		namespace = "public"
	}
	return &RnacosResolver{
		addr:        strings.TrimSuffix(addr, "/"),
		namespace:   namespace,
		group:       group,
		username:    username,
		password:    password,
		healthyOnly: healthyOnly,
		client: &http.Client{
			Timeout: timeout,
			// 发现请求不应跟随重定向，避免被引到意外地址
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Provider 实现 Resolver。
func (r *RnacosResolver) Provider() string { return "rnacos" }

// Close 实现 Resolver。http.Client 无需关闭。
func (r *RnacosResolver) Close() error { return nil }

// nacosInstanceList 是 /v1/ns/instance/list 的响应。
type nacosInstanceList struct {
	Name  string `json:"name"`
	Group string `json:"groupName"`
	Hosts []struct {
		IP       string            `json:"ip"`
		Port     int               `json:"port"`
		Healthy  bool              `json:"healthy"`
		Enabled  bool              `json:"enabled"`
		Weight   float64           `json:"weight"`
		Metadata map[string]string `json:"metadata"`
	} `json:"hosts"`
}

// Resolve 拉取某个服务的实例列表并做清洗。
func (r *RnacosResolver) Resolve(ctx context.Context, service string) ([]Instance, error) {
	if service == "" {
		return nil, fmt.Errorf("rnacos: service 名不能为空")
	}

	q := url.Values{}
	q.Set("serviceName", service)
	q.Set("groupName", r.group)
	q.Set("namespaceId", r.namespace)
	if r.healthyOnly {
		q.Set("healthyOnly", "true")
	}

	endpoint := r.addr + "/nacos/v1/ns/instance/list?" + q.Encode()

	body, err := r.doGet(ctx, endpoint, true)
	if err != nil {
		return nil, err
	}

	var resp nacosInstanceList
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("rnacos: 解析实例列表失败: %w", err)
	}

	insts := make([]Instance, 0, len(resp.Hosts))
	for _, h := range resp.Hosts {
		// enabled=false 表示被运维手动下线，即使 healthy 也要排除
		if !h.Enabled {
			continue
		}
		insts = append(insts, Instance{
			IP:       h.IP,
			Port:     h.Port,
			Weight:   h.Weight,
			Healthy:  h.Healthy,
			Metadata: h.Metadata,
		})
	}

	clean, dropped := sanitize(insts)
	if dropped > 0 {
		// 只记数量不记地址原文到业务日志；这里交给调用方决定如何处理
		return clean, &DroppedError{Service: service, Count: dropped}
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("%w: rnacos service=%s", ErrNoInstances, service)
	}
	return clean, nil
}

// nacosCatalogService 是 /v1/ns/catalog/services 响应里的单个服务条目。
//
// 为什么枚举必须走 catalog 而不是 /v1/ns/service/list：
// 后者只回服务名，而 r-nacos 在「实例全部注销」后并不删除服务，
// 只是把它留成一个空壳（count 不变、doms 里仍有该名字）。
// 于是同步器永远观察不到「服务消失」，注册中心同步的软下线形同虚设。
// catalog 额外给出 ipCount 与 healthyInstanceCount，才足以判断
// 「这个服务此刻还有没有活着的实例」。
type nacosCatalogService struct {
	Name                 string `json:"name"`
	GroupName            string `json:"groupName"`
	IPCount              int    `json:"ipCount"`
	HealthyInstanceCount int    `json:"healthyInstanceCount"`
}

// nacosCatalogServices 是 /v1/ns/catalog/services 的响应。
type nacosCatalogServices struct {
	Count       int                   `json:"count"`
	ServiceList []nacosCatalogService `json:"serviceList"`
}

// List 枚举该 namespace + group 下「当前有可用实例」的服务名。
//
// 用途（服务发现扩展）：网关据此自动增删命名空间。注册中心里出现一个新服务，
// 网关自动建一个 slug 对应的命名空间；服务注销后，网关自动把它下线。
// 这样「加法」和「减法」都只发生在注册中心侧，config.yaml 一行都不用改。
//
// 走 /nacos/v1/ns/catalog/services（Nacos OpenAPI v1，r-nacos 兼容）。
//
// 语义边界：只返回「Resolve 至少能拿到一个实例」的服务。
// 拉清单失败时整轮同步失败、绝不下线（由 registry.Synchronizer 保证），
// 所以这里不做降级到 service/list 的兜底——那会让「永远检测不到下线」
// 这个缺陷静默复活，比直接报错更难排查。
func (r *RnacosResolver) List(ctx context.Context) ([]string, error) {
	const pageSize = 500

	q := url.Values{}
	q.Set("namespaceId", r.namespace)
	q.Set("groupNameParam", r.group)
	q.Set("pageNo", "1")
	q.Set("pageSize", strconv.Itoa(pageSize))

	status, body, err := r.doGetRaw(ctx, r.addr+"/nacos/v1/ns/catalog/services?"+q.Encode(), true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("rnacos: 枚举服务失败（catalog 接口返回 %d）: %s", status, snippet(body))
	}

	var resp nacosCatalogServices
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("rnacos: 解析服务目录失败: %w", err)
	}

	// 超过单页上限时明确报错而不是静默截断：
	// 静默截断会让部分服务永远不被发现，还会让它们被误判为下线。
	if resp.Count > len(resp.ServiceList) {
		return nil, fmt.Errorf(
			"rnacos: 服务总数 %d 超过单页上限 %d，请调大 pageSize 或改用分页拉取",
			resp.Count, pageSize)
	}

	out := make([]string, 0, len(resp.ServiceList))
	for _, svc := range resp.ServiceList {
		// 分组是独立的过滤维度：同名服务在别的分组里不归本网关管。
		// 明确跳过而不是改名落库，否则会把两个环境的服务混成一个命名空间。
		if svc.GroupName != "" && svc.GroupName != r.group {
			continue
		}
		if !r.hasLiveInstance(svc) {
			continue
		}
		if s := r.bareServiceName(svc.Name); s != "" {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out, nil
}

// hasLiveInstance 判断服务此刻是否还有可用实例。
//
// 阈值跟随 healthyOnly，与 Resolve 保持同一口径：
// Resolve 在 healthyOnly=true 时只返回健康实例，那么「实例都在但全不健康」
// 就等同于服务不可用，命名空间也应软下线；否则会出现「命名空间 enabled，
// 但每个请求都 ErrNoInstances」的空转状态。
func (r *RnacosResolver) hasLiveInstance(svc nacosCatalogService) bool {
	if r.healthyOnly {
		return svc.HealthyInstanceCount > 0
	}
	return svc.IPCount > 0
}

// bareServiceName 把 Nacos 的全限定服务名 "GROUP@@name" 还原成裸服务名 "name"。
//
// 为什么必须剥离：service/list 的 doms 恒为全限定名（如
// "DEFAULT_GROUP@@blender-core-service"），而 Resolve 是「serviceName + 独立
// groupName 参数」的调用形态。若把全限定名直接落库成 discovery_service，
// 分组一旦变化（或迁移到另一个 Nacos）就会和服务端对不上。
//
// 兼容两种情况：带 "@@" 的剥离前缀；不带 "@@" 的原样返回（部分实现只回裸名）。
// 前缀与当前分组不一致时不剥离，避免把 "OTHER_GROUP@@x" 误写成 "x"。
func (r *RnacosResolver) bareServiceName(full string) string {
	s := strings.TrimSpace(full)
	if s == "" {
		return ""
	}
	i := strings.Index(s, "@@")
	if i < 0 {
		return s
	}
	if group := s[:i]; group != r.group {
		// 不是本组负责的服务，交给上层按分组过滤；这里原样保留以便日志可见。
		return s
	}
	return strings.TrimSpace(s[i+2:])
}

// ListGroup 返回枚举所用分组，调用方落库到 discovery_group。
func (r *RnacosResolver) ListGroup() string { return r.group }

// DroppedError 表示有实例因为不健康或公网地址被丢弃。
// 它同时携带可用实例，所以调用方应优先使用返回的切片。
type DroppedError struct {
	Service string
	Count   int
}

func (e *DroppedError) Error() string {
	return fmt.Sprintf("rnacos: service=%s 丢弃了 %d 个不可用或非内网实例", e.Service, e.Count)
}

// doGet 发起 GET，非 200 视为错误，401 时自动登录重试一次。
func (r *RnacosResolver) doGet(ctx context.Context, endpoint string, allowAuthRetry bool) ([]byte, error) {
	status, body, err := r.doGetRaw(ctx, endpoint, allowAuthRetry)
	if err != nil {
		return nil, err
	}
	switch status {
	case http.StatusOK:
		return body, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, fmt.Errorf("rnacos: 鉴权失败（%d），请检查 discovery.rnacos.username/password", status)
	default:
		return nil, fmt.Errorf("rnacos: 返回 %d: %s", status, snippet(body))
	}
}

// doGetRaw 发起 GET 并原样返回状态码与响应体。
//
// 为什么要把状态码交出来：枚举服务需要区分「接口不存在」（404，属于
// 部署形态不匹配，应给出针对性报错）与「请求失败」（网络、鉴权、5xx）。
// 401/403 的自动重登只在这一处实现，doGet 与 List 共用，避免两份重试逻辑
// 在后续维护中走偏。
func (r *RnacosResolver) doGetRaw(ctx context.Context, endpoint string, allowAuthRetry bool) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("rnacos: 构造请求失败: %w", err)
	}

	if tok := r.cachedToken(); tok != "" {
		q := req.URL.Query()
		q.Set("accessToken", tok)
		req.URL.RawQuery = q.Encode()
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("rnacos: 请求 %s 失败: %w", r.addr, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("rnacos: 读取响应失败: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		// 没配账号就不要假装能重登，直接把状态码交回调用方诊断。
		if !allowAuthRetry || r.username == "" {
			return resp.StatusCode, body, nil
		}
		r.invalidateToken()
		if err := r.login(ctx); err != nil {
			return resp.StatusCode, body, err
		}
		return r.doGetRaw(ctx, endpoint, false)
	}
	return resp.StatusCode, body, nil
}

// login 换取 accessToken。
func (r *RnacosResolver) login(ctx context.Context) error {
	form := url.Values{}
	form.Set("username", r.username)
	form.Set("password", r.password)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		r.addr+"/nacos/v1/auth/login", strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("rnacos: 构造登录请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("rnacos: 登录失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("rnacos: 读取登录响应失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rnacos: 登录返回 %d: %s", resp.StatusCode, snippet(body))
	}

	var out struct {
		AccessToken string `json:"accessToken"`
		TokenTTL    int    `json:"tokenTtl"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return fmt.Errorf("rnacos: 解析登录响应失败: %w", err)
	}
	if out.AccessToken == "" {
		return fmt.Errorf("rnacos: 登录未返回 accessToken")
	}

	ttl := time.Duration(out.TokenTTL) * time.Second
	if ttl <= 0 {
		ttl = 18000 * time.Second
	}
	// 提前 20% 过期，避免边界时刻被拒
	ttl = ttl * 8 / 10

	r.tokenMu.Lock()
	r.token = out.AccessToken
	r.tokenExpiry = time.Now().Add(ttl)
	r.tokenMu.Unlock()
	return nil
}

func (r *RnacosResolver) cachedToken() string {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	if r.token == "" || time.Now().After(r.tokenExpiry) {
		return ""
	}
	return r.token
}

func (r *RnacosResolver) invalidateToken() {
	r.tokenMu.Lock()
	r.token = ""
	r.tokenExpiry = time.Time{}
	r.tokenMu.Unlock()
}

// snippet 截断错误响应体，避免把整页 HTML 打进日志。
func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

// EnsureFresh 主动登录，供启动自检调用（可选）。
func (r *RnacosResolver) EnsureFresh(ctx context.Context) error {
	if r.username == "" {
		return nil
	}
	if r.cachedToken() != "" {
		return nil
	}
	return r.login(ctx)
}

// 编译期断言：确保实现满足接口。
var _ Resolver = (*RnacosResolver)(nil)
var _ Resolver = (*StaticResolver)(nil)
var _ Lister = (*RnacosResolver)(nil)

// parsePort 是给测试与静态配置复用的小工具。
func parsePort(s string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	if n <= 0 || n > 65535 {
		return 0, fmt.Errorf("端口越界: %d", n)
	}
	return n, nil
}
