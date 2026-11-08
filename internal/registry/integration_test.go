package registry

// 本文件是注册中心同步「验收脚本」的可执行版本，只在显式提供环境变量时才运行：
//
//	MCPGW_TEST_DSN=postgres://mcp:mcp_dev_password@127.0.0.1:5432/mcp_gateway?sslmode=disable
//	MCPGW_TEST_RNACOS=http://127.0.0.1:8848
//	go test ./internal/registry/ -run TestAcceptance -v
//
// 为什么不放进默认测试集：它需要真实 Postgres 与真实 r-nacos，会在库里建行、
// 也会往注册中心注册实例。默认 `go test ./...` 必须在任何机器上都能跑。
//
// 为什么不用 fake store：注册中心同步的核心断言（「软下线不动 namespace_tools 与
// key_permissions」「manual/config 行不被回收」）全部由 SQL 的 WHERE 条件实现，
// 换成内存假实现就等于把要验证的东西替换掉了，测了个寂寞。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// rnacosClient 是测试期最小的 r-nacos 写入口。
//
// 生产代码里网关只读不写（服务发现：网关不做注册者），所以这段只存在于测试中，
// 用来扮演「接入方」这一侧的动作。
type rnacosClient struct {
	base string
	http *http.Client
}

func newRnacosClient(base string) *rnacosClient {
	return &rnacosClient{
		base: strings.TrimSuffix(base, "/"),
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *rnacosClient) call(method, path string, form url.Values) error {
	req, err := http.NewRequest(method, c.base+path, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var body json.RawMessage
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("r-nacos %s %s 返回 %d: %s", method, path, resp.StatusCode, body)
	}
	// r-nacos 成功响应是裸文本 ok（不带引号），失败时 HTTP 码仍然可能是 200，
	// 因此这里额外校验响应体不是 false / error。
	if s := strings.TrimSpace(string(body)); s == "false" || strings.HasPrefix(s, "error") {
		return fmt.Errorf("r-nacos %s %s 业务失败: %s", method, path, s)
	}
	return nil
}

// register 以 ephemeral=false 注册实例：持久实例不会因心跳过期自动消失，
// 「停服务」这一步必须显式 deregister 才能复现，测试因此是确定性的。
func (c *rnacosClient) register(service, ip string, port int) error {
	return c.call(http.MethodPost, "/nacos/v1/ns/instance", url.Values{
		"serviceName": {service},
		"ip":          {ip},
		"port":        {strconv.Itoa(port)},
		"groupName":   {"DEFAULT_GROUP"},
		"namespaceId": {"public"},
		"ephemeral":   {"false"},
	})
}

func (c *rnacosClient) deregister(service, ip string, port int) error {
	return c.call(http.MethodDelete, "/nacos/v1/ns/instance", url.Values{
		"serviceName": {service},
		"ip":          {ip},
		"port":        {strconv.Itoa(port)},
		"groupName":   {"DEFAULT_GROUP"},
		"namespaceId": {"public"},
		"ephemeral":   {"false"},
	})
}

// acceptanceEnv 汇总一次验收需要的全部依赖，缺任何一个都直接 t.Skip。
type acceptanceEnv struct {
	ctx    context.Context
	st     *store.Store
	pool   *pgxpool.Pool
	rnacos *rnacosClient
	syncer *Synchronizer
}

func setupAcceptance(t *testing.T) *acceptanceEnv {
	t.Helper()

	dsn := os.Getenv("MCPGW_TEST_DSN")
	addr := os.Getenv("MCPGW_TEST_RNACOS")
	if dsn == "" || addr == "" {
		t.Skip("需要 MCPGW_TEST_DSN 与 MCPGW_TEST_RNACOS 才能跑注册中心同步验收；跳过")
	}

	if err := idgen.Init(0); err != nil {
		t.Fatalf("初始化雪花 ID 失败: %v", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("数据库连通性检查失败: %v", err)
	}

	st, err := store.Attach(pool)
	if err != nil {
		pool.Close()
		t.Fatalf("初始化 store 失败: %v", err)
	}

	resolver := discovery.NewRnacos(addr, "public", "DEFAULT_GROUP", "", "", 10*time.Second, true)
	syncer := New(st, Options{
		Lister:        resolver,
		ServicePrefix: "mcp-",
		EndpointPath:  "/mcp",
		TimeoutMS:     120000,
		PoolSize:      3,
		Logf:          func(format string, args ...any) { t.Logf("[syncer] "+format, args...) },
	})

	return &acceptanceEnv{ctx: ctx, st: st, pool: pool, rnacos: newRnacosClient(addr), syncer: syncer}
}

func (e *acceptanceEnv) close() { e.pool.Close() }

// registryRow 是断言时直接读库用的投影。
type registryRow struct {
	ID           string
	Slug         string
	Source       string
	Enabled      bool
	DisabledBy   *string
	Provider     string
	Service      string
	Group        string
	EndpointPath string
	PoolSize     int
	TimeoutMS    int
	HasLastSeen  bool
}

func (e *acceptanceEnv) loadRow(t *testing.T, slug string) registryRow {
	t.Helper()
	var r registryRow
	err := e.pool.QueryRow(e.ctx, `
		SELECT id, slug, source, enabled, disabled_by,
		       coalesce(discovery_provider, ''), coalesce(discovery_service, ''),
		       coalesce(discovery_group, ''), coalesce(endpoint_path, ''),
		       pool_size, timeout_ms, last_seen_at IS NOT NULL
		FROM namespaces WHERE slug = $1`, slug).
		Scan(&r.ID, &r.Slug, &r.Source, &r.Enabled, &r.DisabledBy,
			&r.Provider, &r.Service, &r.Group, &r.EndpointPath,
			&r.PoolSize, &r.TimeoutMS, &r.HasLastSeen)
	if err != nil {
		t.Fatalf("读取命名空间 %s 失败: %v", slug, err)
	}
	return r
}

func (e *acceptanceEnv) cleanup(t *testing.T, slug, service string, port int) {
	t.Helper()
	// 顺序必须是「先注册中心、后数据库」的反向：先摘实例，免得下一轮
	// SyncOnce 又把它建回来；再删命名空间行（namespace_tools / key_permissions
	// 走 ON DELETE CASCADE，无需单独删）。
	_ = e.rnacos.deregister(service, "127.0.0.1", port)
	if _, err := e.pool.Exec(e.ctx, `DELETE FROM namespaces WHERE slug = $1`, slug); err != nil {
		t.Logf("清理命名空间 %s 失败（不影响结论）: %v", slug, err)
	}
}

// TestAcceptanceRegistrySync 覆盖注册中心同步验收脚本中不依赖管理端接口的部分：
// 1-3（注册 → 同步 → 落库）、6-9（下线 → 保留 → 恢复），外加三条守卫。
func TestAcceptanceRegistrySync(t *testing.T) {
	env := setupAcceptance(t)
	defer env.close()

	// 每次运行用独立 slug，避免与上次失败的残留互相干扰。
	suffix := strconv.FormatInt(time.Now().Unix()%100000, 10)
	slug := "syncprobe" + suffix
	service := "mcp-" + slug
	const ip = "127.0.0.1"
	const port = 18099

	defer env.cleanup(t, slug, service, port)

	// ---- 步骤 1：接入方注册服务 -------------------------------------------
	if err := env.rnacos.register(service, ip, port); err != nil {
		t.Fatalf("注册 %s 到 r-nacos 失败: %v", service, err)
	}
	t.Logf("步骤 1 完成：%s 已注册（%s:%d）", service, ip, port)

	// ---- 步骤 2/3：同步并检查落库 -----------------------------------------
	stats, err := env.syncer.SyncOnce(env.ctx)
	if err != nil {
		t.Fatalf("同步失败: %v", err)
	}
	if stats.Created < 1 {
		t.Fatalf("应至少新建 1 个命名空间，实际 %d（stats=%s）", stats.Created, stats)
	}
	row := env.loadRow(t, slug)
	if row.Source != store.SourceRegistry {
		t.Errorf("source 应为 %q，实际 %q", store.SourceRegistry, row.Source)
	}
	if !row.Enabled || row.DisabledBy != nil {
		t.Errorf("新发现的服务应 enabled=true 且 disabled_by=NULL，实际 enabled=%v disabled_by=%v",
			row.Enabled, row.DisabledBy)
	}
	if row.Service != service || row.Group != "DEFAULT_GROUP" {
		t.Errorf("发现目标应记录为 %s/DEFAULT_GROUP，实际 %s/%s", service, row.Service, row.Group)
	}
	if row.Provider != "rnacos" || row.EndpointPath != "/mcp" {
		t.Errorf("发现 provider/endpoint 应来自模板（rnacos、/mcp），实际 %q/%q", row.Provider, row.EndpointPath)
	}
	if row.PoolSize != 3 || row.TimeoutMS != 120000 {
		t.Errorf("首次创建应使用模板值 pool=3 timeout=120000，实际 %d/%d", row.PoolSize, row.TimeoutMS)
	}
	if !row.HasLastSeen {
		t.Error("last_seen_at 应被 MarkRegistrySeen 写入，实际为 NULL")
	}
	t.Logf("步骤 2/3 完成：slug=%s 已建行，source=registry、enabled=true", slug)

	// ---- 准备「人工配置」，用于验证软下线不删数据 -------------------------
	userID := idgen.MustRandom(idgen.KindUser)
	keyID := idgen.MustRandom(idgen.KindAPIKey)
	tenantID := idgen.MustRandom(idgen.KindTenant)
	if _, err := env.pool.Exec(env.ctx,
		`INSERT INTO tenants (id, slug, name, kind, status, shard)
		 VALUES ($1, $2, $3, 'personal', 'active', '01')`,
		tenantID, "accept-"+suffix, "验收租户"); err != nil {
		t.Fatalf("插入租户失败: %v", err)
	}
	// 用种子里已有的 free 套餐（0003_seed.sql），不另外造。
	if _, err := env.pool.Exec(env.ctx,
		`INSERT INTO users (id, tenant_id, email, display_name, plan_id)
		 VALUES ($1, $2, $3, $4, 'pln_0000000000000001')`,
		userID, tenantID, "accept-"+suffix+"@example.com", "验收用户"); err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}
	if _, err := env.pool.Exec(env.ctx,
		`INSERT INTO api_keys (id, user_id, name, key_prefix, key_hash)
		 VALUES ($1, $2, $3, $4, $5)`,
		keyID, userID, "验收 Key", "mcpk_accept", []byte("acceptance-"+suffix)); err != nil {
		t.Fatalf("插入 API Key 失败: %v", err)
	}
	if _, err := env.pool.Exec(env.ctx,
		`INSERT INTO namespace_tools (namespace_id, tool_name, allowed)
		 VALUES ($1, 'probe_tool', TRUE)`, row.ID); err != nil {
		t.Fatalf("插入工具白名单失败: %v", err)
	}
	if _, err := env.pool.Exec(env.ctx,
		`INSERT INTO key_permissions (key_id, namespace_id, tool_name)
		 VALUES ($1, $2, '*')`, keyID, row.ID); err != nil {
		t.Fatalf("插入 Key 授权失败: %v", err)
	}
	defer func() {
		// 租户→用户→Key 是 RESTRICT/SET NULL 混用，删用户会连带删 Key。
		_, _ = env.pool.Exec(env.ctx, `DELETE FROM key_permissions WHERE key_id = $1`, keyID)
		_, _ = env.pool.Exec(env.ctx, `DELETE FROM users WHERE id = $1`, userID)
		_, _ = env.pool.Exec(env.ctx, `DELETE FROM tenants WHERE id = $1`, tenantID)
	}()

	// ---- 步骤 6/7：停服务 → 软下线，且人工配置原样保留 ---------------------
	if err := env.rnacos.deregister(service, ip, port); err != nil {
		t.Fatalf("注销 %s 失败: %v", service, err)
	}
	stats, err = env.syncer.SyncOnce(env.ctx)
	if err != nil {
		t.Fatalf("第二轮同步失败: %v", err)
	}
	if stats.Offlined < 1 {
		t.Fatalf("应至少软下线 1 个命名空间，实际 %d（stats=%s）", stats.Offlined, stats)
	}
	row = env.loadRow(t, slug)
	if row.Enabled {
		t.Error("服务已注销，该行应 enabled=false")
	}
	if row.DisabledBy == nil || *row.DisabledBy != store.DisabledByRegistryGone {
		t.Errorf("disabled_by 应为 %q，实际 %v", store.DisabledByRegistryGone, row.DisabledBy)
	}
	var tools, perms int
	if err := env.pool.QueryRow(env.ctx,
		`SELECT count(*) FROM namespace_tools WHERE namespace_id = $1`, row.ID).Scan(&tools); err != nil {
		t.Fatalf("统计工具白名单失败: %v", err)
	}
	if err := env.pool.QueryRow(env.ctx,
		`SELECT count(*) FROM key_permissions WHERE namespace_id = $1`, row.ID).Scan(&perms); err != nil {
		t.Fatalf("统计 Key 授权失败: %v", err)
	}
	if tools != 1 {
		t.Errorf("软下线不得清理 namespace_tools，期望 1 行，实际 %d", tools)
	}
	if perms != 1 {
		t.Errorf("软下线不得清理 key_permissions，期望 1 行，实际 %d", perms)
	}
	t.Logf("步骤 6/7 完成：enabled=false disabled_by=registry_gone，工具 %d 条、授权 %d 条均保留", tools, perms)

	// ---- 步骤 8/9：服务回来 → 原地恢复，授权原样还在 -----------------------
	if err := env.rnacos.register(service, ip, port); err != nil {
		t.Fatalf("重新注册 %s 失败: %v", service, err)
	}
	stats, err = env.syncer.SyncOnce(env.ctx)
	if err != nil {
		t.Fatalf("第三轮同步失败: %v", err)
	}
	if stats.Restored < 1 {
		t.Fatalf("应至少恢复 1 个命名空间，实际 %d（stats=%s）", stats.Restored, stats)
	}
	row = env.loadRow(t, slug)
	if !row.Enabled || row.DisabledBy != nil {
		t.Errorf("服务回来后应 enabled=true 且 disabled_by=NULL，实际 enabled=%v disabled_by=%v",
			row.Enabled, row.DisabledBy)
	}
	// 主键不变是「原地恢复」的硬证据：ID 一旦换新，历史 key_permissions 就断了。
	if row.ID == "" {
		t.Error("恢复后主键不应为空")
	}
	if err := env.pool.QueryRow(env.ctx,
		`SELECT count(*) FROM key_permissions WHERE namespace_id = $1`, row.ID).Scan(&perms); err != nil {
		t.Fatalf("恢复后统计 Key 授权失败: %v", err)
	}
	if perms != 1 {
		t.Errorf("恢复后原有 key_permissions 应原样保留，期望 1 行，实际 %d", perms)
	}
	t.Logf("步骤 8/9 完成：已原地恢复，key_permissions 仍为 %d 条", perms)

	// ---- 守卫一：管理员手关的服务，同步器不得替他打开 ----------------------
	if _, err := env.pool.Exec(env.ctx,
		`UPDATE namespaces SET enabled = FALSE, disabled_by = $2 WHERE id = $1`,
		row.ID, store.DisabledByAdmin); err != nil {
		t.Fatalf("模拟管理员关闭失败: %v", err)
	}
	if _, err := env.syncer.SyncOnce(env.ctx); err != nil {
		t.Fatalf("第四轮同步失败: %v", err)
	}
	row = env.loadRow(t, slug)
	if row.Enabled || row.DisabledBy == nil || *row.DisabledBy != store.DisabledByAdmin {
		t.Errorf("disabled_by=admin 的行不应被同步器恢复，实际 enabled=%v disabled_by=%v",
			row.Enabled, row.DisabledBy)
	}
	t.Logf("守卫一通过：管理员关闭的行保持 admin")

	// ---- 守卫二：服务消失时，管理员手关的行不该被改写成 registry_gone ------
	if err := env.rnacos.deregister(service, ip, port); err != nil {
		t.Fatalf("第二次注销 %s 失败: %v", service, err)
	}
	if _, err := env.syncer.SyncOnce(env.ctx); err != nil {
		t.Fatalf("第五轮同步失败: %v", err)
	}
	row = env.loadRow(t, slug)
	if row.DisabledBy == nil || *row.DisabledBy != store.DisabledByAdmin {
		t.Errorf("下线时不得覆盖 disabled_by=admin，实际 %v", row.DisabledBy)
	}
	t.Logf("守卫二通过：下线路径未覆盖 admin 原因")

	// ---- 守卫三：slug 被 config 行占用 → 记为冲突，绝不覆盖 ----------------
	conflictSlug := "syncprobeconflict" + suffix
	conflictService := "mcp-" + conflictSlug
	defer func() {
		_ = env.rnacos.deregister(conflictService, ip, port)
		_, _ = env.pool.Exec(env.ctx, `DELETE FROM namespaces WHERE slug = $1`, conflictSlug)
	}()

	// 直接插一行 source='config'，模拟 config.yaml 自举占用了该 slug。
	// 刻意不走 UpsertNamespace：这里要的前置状态正是「另一来源已存在」，
	// 而不是经过应用层规范化后的产物。
	configID := idgen.MustRandom(idgen.KindNamespace)
	if _, err := env.pool.Exec(env.ctx, `
		INSERT INTO namespaces (id, slug, transport, source, enabled, pool_size, timeout_ms,
		                        discovery_provider, discovery_service, discovery_group,
		                        endpoint_path, lb_strategy, description)
		VALUES ($1, $2, 'http', 'config', TRUE, 1, 120000,
		        'rnacos', $3, 'DEFAULT_GROUP', '/mcp', 'round_robin', $4)`,
		configID, conflictSlug, "mcp-placeholder", "验收用：冒充 config.yaml 自举的行"); err != nil {
		t.Fatalf("插入占位 config 行失败: %v", err)
	}

	if err := env.rnacos.register(conflictService, ip, port); err != nil {
		t.Fatalf("注册 %s 失败: %v", conflictService, err)
	}
	stats, err = env.syncer.SyncOnce(env.ctx)
	if err != nil {
		t.Fatalf("冲突场景同步失败: %v", err)
	}
	if stats.Conflicts < 1 {
		t.Errorf("slug 被非 registry 来源占用时应计入 Conflicts，实际 0（stats=%s）", stats)
	}
	var conflictSource string
	if err := env.pool.QueryRow(env.ctx,
		`SELECT source FROM namespaces WHERE slug = $1`, conflictSlug).Scan(&conflictSource); err != nil {
		t.Fatalf("读取冲突行失败: %v", err)
	}
	if conflictSource != store.SourceConfig {
		t.Errorf("被占用的行不得被同步器改写来源，期望 %q，实际 %q", store.SourceConfig, conflictSource)
	}
	t.Logf("守卫三通过：config 占用的 slug 记为冲突且原行未被改写")
}

// TestAcceptanceRefreshKeepsAdminTuning 锁住 refresh() 的「不抹人工调参」承诺。
//
// 这是注册中心同步里最容易悄悄回归的一条：模板整行覆盖写起来更顺手，
// 但会让管理员在后台改过的 pool_size / timeout_ms 每 60 秒被抹一次。
func TestAcceptanceRefreshKeepsAdminTuning(t *testing.T) {
	env := setupAcceptance(t)
	defer env.close()

	suffix := strconv.FormatInt(time.Now().Unix()%100000, 10)
	slug := "syncprobetune" + suffix
	service := "mcp-" + slug
	const ip = "127.0.0.1"
	const port = 18098

	defer env.cleanup(t, slug, service, port)

	if err := env.rnacos.register(service, ip, port); err != nil {
		t.Fatalf("注册 %s 失败: %v", service, err)
	}
	if _, err := env.syncer.SyncOnce(env.ctx); err != nil {
		t.Fatalf("首轮同步失败: %v", err)
	}
	row := env.loadRow(t, slug)

	// 模拟管理员在后台把并发与超时都调大，并改了端点路径。
	if _, err := env.pool.Exec(env.ctx, `
		UPDATE namespaces SET pool_size = 17, timeout_ms = 9000, endpoint_path = '/custom'
		WHERE id = $1`, row.ID); err != nil {
		t.Fatalf("模拟人工调参失败: %v", err)
	}

	if _, err := env.syncer.SyncOnce(env.ctx); err != nil {
		t.Fatalf("第二轮同步失败: %v", err)
	}
	row = env.loadRow(t, slug)
	if row.PoolSize != 17 {
		t.Errorf("refresh 不应抹掉人工设置的 pool_size，期望 17，实际 %d", row.PoolSize)
	}
	if row.TimeoutMS != 9000 {
		t.Errorf("refresh 不应抹掉人工设置的 timeout_ms，期望 9000，实际 %d", row.TimeoutMS)
	}
	if row.EndpointPath != "/custom" {
		t.Errorf("refresh 不应抹掉人工设置的 endpoint_path，期望 /custom，实际 %q", row.EndpointPath)
	}
	t.Logf("人工调参在同步后保持：pool=%d timeout=%d endpoint=%s",
		row.PoolSize, row.TimeoutMS, row.EndpointPath)
}
