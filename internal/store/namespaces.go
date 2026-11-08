package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// Namespace 是 namespaces 表的一行，附带已授权的工具白名单。
//
// transport 二分（服务发现）：
//   - stdio：本地子进程，用 Command/Args/Env，由进程池管理
//   - http ：远端服务，Command/Args/Env 必须为空，实例地址由发现层给出
//
// 数据库层有 namespaces_command_transport_check 与
// namespaces_http_args_env_empty_check 兜底，此处只做一次应用层预检，
// 让错误以可读中文返回，而不是抛一个 constraint violation。
type Namespace struct {
	ID          idgen.ID
	Slug        string
	Description string
	Transport   string // stdio | http
	Command     string // 仅 stdio 使用
	Args        []string
	Env         map[string]string
	PoolSize    int
	TimeoutMS   int
	Enabled     bool

	// DisabledBy 记录软下线原因，与 Enabled 成对维护。
	//
	// 数据库有 namespaces_disabled_consistency CHECK (enabled OR disabled_by IS NOT NULL)，
	// 只把 enabled 置 FALSE 而不写原因会被直接拒绝。取值见
	// DisabledByPlanExpiry / DisabledByAdmin / DisabledByRegistryGone。
	// 管理端靠它区分「管理员手关的」和「注册中心里没了的」，两者的处理方式完全不同。
	DisabledBy *string

	// 以下仅 transport = http 时生效
	DiscoveryProvider string // rnacos | static
	DiscoveryService  string // 注册中心里的服务名
	DiscoveryGroup    string
	EndpointPath      string
	LBStrategy        string // round_robin

	// Tools 是 namespace_tools 中 allowed=true 的工具名。
	// 空集合表示「不限制」，与数据库语义保持一致。
	Tools []string

	// Source 标记这一行是谁建的：manual / config / registry（见迁移 0010）。
	//
	// 这个字段是「动态增删」能安全落地的前提：同步器只允许回收
	// source=registry 的行。没有它，同步器无法区分「注册中心里已经注销的服务」
	// 和「超级管理员手工配好、只是暂时没实例的命名空间」，删哪个都是事故。
	Source string

	// LastSeenAt 仅在 source=registry 时有意义：最近一次在注册中心被看到的时间。
	// 留 NULL 表示从未由注册中心发现（manual / config 行恒为 NULL）。
	LastSeenAt *time.Time
}

// 传输方式的取值，避免各处写裸字符串。
const (
	TransportStdio = "stdio"
	TransportHTTP  = "http"
)

// 命名空间来源的取值，与迁移 0010 的 CHECK 约束一一对应。
const (
	// SourceManual 管理员手工创建，同步器永不自动下线。
	SourceManual = "manual"
	// SourceConfig 由 config.yaml 启动自举写入，同步器不碰。
	SourceConfig = "config"
	// SourceRegistry 由注册中心自动发现，服务消失后由同步器自动下线。
	SourceRegistry = "registry"
)

const namespaceColumns = `id, slug, coalesce(description, ''), transport, coalesce(command, ''),
                          args, env, pool_size, timeout_ms, enabled, disabled_by,
                          coalesce(discovery_provider, ''), coalesce(discovery_service, ''),
                          coalesce(discovery_group, ''), coalesce(endpoint_path, ''),
                          coalesce(lb_strategy, ''), source, last_seen_at`

// UpsertNamespace 按 slug 幂等写入，供 config.yaml 启动自举使用。
//
// 已存在时保留原主键（ID 不可轮换），仅更新配置字段。
// newID 仅在该 slug 首次插入时使用，由调用方通过 idgen.NewRandom(idgen.KindNamespace) 生成。
//
// 传入的 in 会被「就地规范化」后再落库：缺省值补齐、互斥字段清空、
// 非法组合直接报错。这样无论调用方是启动自举还是管理端接口，
// 落库口径都只有一套，不会出现「yaml 里写了 http 却带 command」这种脏数据。
func (s *Store) UpsertNamespace(ctx context.Context, newID idgen.ID, in Namespace) (*Namespace, error) {
	return s.upsertNamespace(ctx, newID, in, false)
}

// UpsertRegistryNamespace 是注册中心同步器的写入路径，与 UpsertNamespace 的唯一区别
// 是在冲突更新时多一道「不得覆盖管理员禁用」的守卫，见本文件末尾「注册中心同步」一节。
func (s *Store) UpsertRegistryNamespace(ctx context.Context, newID idgen.ID, in Namespace) (*Namespace, error) {
	return s.upsertNamespace(ctx, newID, in, true)
}

// upsertNamespace 是上面两个入口的共用实现。
//
// guardAdmin=true 时在 DO UPDATE 上追加「disabled_by 不是 admin」的条件。
// 守卫必须下沉到 SQL 而不是「调用方先查后写」：先查后写之间隔着一次网络往返，
// 管理员正好在这个窗口里点了禁用，同步器就会把他的操作覆盖掉。
func (s *Store) upsertNamespace(ctx context.Context, newID idgen.ID, in Namespace, guardAdmin bool) (*Namespace, error) {
	if err := NormalizeNamespace(&in); err != nil {
		return nil, err
	}

	argsJSON, err := json.Marshal(in.Args)
	if err != nil {
		return nil, fmt.Errorf("序列化 args 失败: %w", err)
	}
	envJSON, err := json.Marshal(in.Env)
	if err != nil {
		return nil, fmt.Errorf("序列化 env 失败: %w", err)
	}

	// 冲突时的 WHERE 是整套动态同步的安全阀：
	// 只允许「同来源」覆盖。于是
	//   - config.yaml 自举不会把 registry 行抢成 config（否则该服务从此不再被同步器回收）
	//   - 同步器也不会把管理员手工建的 manual 行抢成 registry（否则管理员配置会被自动下线）
	// 跨来源冲突时 DO UPDATE 不执行，RETURNING 无行，下面统一转成 ErrSlugConflict。
	row := s.pool.QueryRow(ctx, `
		INSERT INTO namespaces (
			id, slug, description, transport, command, args, env,
			pool_size, timeout_ms, enabled, disabled_by,
			discovery_provider, discovery_service, discovery_group, endpoint_path, lb_strategy,
			source, last_seen_at
		)
		VALUES (
			$1, $2, NULLIF($3, ''), $4, NULLIF($5, ''), $6::jsonb, $7::jsonb,
			$8, $9, $10, NULLIF($11, ''),
			NULLIF($12, ''), NULLIF($13, ''), NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, ''),
			$17,
			CASE WHEN $17 = 'registry' THEN now() ELSE NULL END
		)
		ON CONFLICT (slug) DO UPDATE SET
			description        = EXCLUDED.description,
			transport          = EXCLUDED.transport,
			command            = EXCLUDED.command,
			args               = EXCLUDED.args,
			env                = EXCLUDED.env,
			pool_size          = EXCLUDED.pool_size,
			timeout_ms         = EXCLUDED.timeout_ms,
			enabled            = EXCLUDED.enabled,
			disabled_by        = EXCLUDED.disabled_by,
			discovery_provider = EXCLUDED.discovery_provider,
			discovery_service  = EXCLUDED.discovery_service,
			discovery_group    = EXCLUDED.discovery_group,
			endpoint_path      = EXCLUDED.endpoint_path,
			lb_strategy        = EXCLUDED.lb_strategy,
			last_seen_at       = CASE WHEN EXCLUDED.source = 'registry' THEN now()
			                          ELSE namespaces.last_seen_at END,
			updated_at         = now()
		WHERE namespaces.source = EXCLUDED.source`+namespaceAdminGuard(guardAdmin)+`
		RETURNING `+namespaceColumns,
		newID, in.Slug, in.Description, in.Transport, in.Command,
		string(argsJSON), string(envJSON), in.PoolSize, in.TimeoutMS, in.Enabled, in.DisabledBy,
		in.DiscoveryProvider, in.DiscoveryService, in.DiscoveryGroup, in.EndpointPath, in.LBStrategy,
		in.Source)

	ns, err := scanNamespace(row)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// 走到这里只可能是 WHERE 挡住了 DO UPDATE。挡住的原因有两种，
			// 对调用方的处置完全不同，所以多查一次把话说明白，而不是笼统报「冲突」。
			return nil, s.explainUpsertMiss(ctx, in.Slug, in.Source)
		}
		return nil, err
	}
	return ns, nil
}

// ErrRegistryRowLocked 表示该行被管理员手工禁用，同步器必须原样跳过。
//
// 它不是故障，而是一个需要安静记录的结论：服务在注册中心里还活着，但有人
// 出于自己的理由关掉了它。同步器拿到它应当「continue + 记一条 debug」，
// 而不是告警——每 60 秒重复一次告警会淹没真正的问题。
var ErrRegistryRowLocked = errors.New("命名空间已被管理员手工禁用，同步器不覆盖")

// namespaceAdminGuard 生成 upsert 冲突更新时追加的守卫条件。
//
// 用独立函数而不是字符串常量：条件是否生效完全由 guardAdmin 决定，
// 写成一个用户可传的字符串就可能被误传成空串而悄悄失效。
func namespaceAdminGuard(guardAdmin bool) string {
	if !guardAdmin {
		return ""
	}
	return ` AND namespaces.disabled_by IS DISTINCT FROM 'admin'`
}

// explainUpsertMiss 在 upsert 一行都没写进去时，把原因查清并说明。
//
// 只有两条路径会走到这里：slug 被另一种 source 占着，或该行被管理员手工禁用。
// 前者需要人工介入改来源，后者是「按设计跳过」，混成一句「未找到」会让人
// 在管理端和日志之间反复猜。
func (s *Store) explainUpsertMiss(ctx context.Context, slug, source string) error {
	var existingSource string
	var disabledBy *string
	err := s.pool.QueryRow(ctx,
		`SELECT source, disabled_by FROM namespaces WHERE slug = $1`, slug).
		Scan(&existingSource, &disabledBy)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// upsert 之后行又没了：只可能是并发删除。如实报告，不要编造原因。
		return fmt.Errorf("%w: slug=%s 来源=%s 在写入瞬间被删除", ErrNotFound, slug, source)
	case err != nil:
		return fmt.Errorf("查明命名空间 %s 未写入的原因失败: %w", slug, err)
	}
	if existingSource != source {
		return fmt.Errorf("%w: slug=%s 已被来源 %s 占用，本次来源 %s",
			ErrSlugConflict, slug, existingSource, source)
	}
	if disabledBy != nil && *disabledBy == DisabledByAdmin {
		return fmt.Errorf("%w: slug=%s", ErrRegistryRowLocked, slug)
	}
	// 来源相同、也没被 admin 锁住却仍然没写入：来源与禁用原因之外还发生了什么，
	// 属于不该出现的情况，如实报出来好排查。
	return fmt.Errorf("%w: slug=%s 来源=%s 未写入（原因未知）", ErrNotFound, slug, source)
}

// NormalizeNamespace 校验并补齐命名空间字段，规则与迁移脚本里的 CHECK 一一对应。
//
// 之所以在应用层再写一遍：数据库报错是一串英文约束名，运维看不懂也定位不到
// config.yaml 的哪一行；这里能直接说出「blender 是 http 模式，不能配置 command」。
func NormalizeNamespace(in *Namespace) error {
	if strings.TrimSpace(in.Slug) == "" {
		return errors.New("命名空间 slug 不能为空")
	}
	in.Slug = strings.TrimSpace(in.Slug)

	// transport 缺省视为 stdio，兼容旧配置
	transport := strings.ToLower(strings.TrimSpace(in.Transport))
	if transport == "" {
		transport = TransportStdio
	}
	switch transport {
	case TransportStdio, TransportHTTP:
	default:
		return fmt.Errorf("命名空间 %s 的 transport=%q 非法，只能是 stdio 或 http", in.Slug, in.Transport)
	}
	in.Transport = transport

	if in.PoolSize <= 0 {
		in.PoolSize = 1
	}
	if in.TimeoutMS <= 0 {
		in.TimeoutMS = 120000
	}

	// enabled 与 disabled_by 必须成对落库：数据库有
	// namespaces_disabled_consistency CHECK (enabled OR disabled_by IS NOT NULL)。
	// 调用方只想「关掉」而没给原因时，按管理员下线处理，避免整条 upsert 被约束拒绝。
	if in.DisabledBy != nil {
		reason := strings.TrimSpace(*in.DisabledBy)
		if reason == "" {
			in.DisabledBy = nil
		} else {
			// 非法取值在应用层就挡掉：数据库只会回一句约束名，
			// 看不出是哪个调用点传错了，排查成本高得多。
			if err := validateNamespaceDisabledReason(reason); err != nil {
				return fmt.Errorf("命名空间 %s: %w", in.Slug, err)
			}
			in.DisabledBy = &reason
		}
	}
	if in.Enabled {
		in.DisabledBy = nil
	} else if in.DisabledBy == nil {
		reason := DisabledByAdmin
		in.DisabledBy = &reason
	}

	// 来源必须显式合法。缺省按 manual 处理：宁可当成「人工资产」不自动删，
	// 也不能因为漏填字段就被同步器当成自己的行回收掉。
	switch in.Source = strings.TrimSpace(in.Source); in.Source {
	case "":
		in.Source = SourceManual
	case SourceManual, SourceConfig, SourceRegistry:
	default:
		return fmt.Errorf("命名空间 %s 的 source=%q 非法，只能是 manual / config / registry", in.Slug, in.Source)
	}

	in.Args = normalizeArgs(in.Args)
	in.Env = normalizeEnv(in.Env)

	switch transport {
	case TransportStdio:
		// stdio 模式：必须有本地命令，且不允许配置发现字段
		in.Command = strings.TrimSpace(in.Command)
		if in.Command == "" {
			return fmt.Errorf("命名空间 %s 是 stdio 模式，必须配置 command", in.Slug)
		}
		if in.DiscoveryProvider != "" || in.DiscoveryService != "" {
			return fmt.Errorf("命名空间 %s 是 stdio 模式，不能配置 discovery_provider / discovery_service", in.Slug)
		}
		in.DiscoveryProvider = ""
		in.DiscoveryService = ""
		in.DiscoveryGroup = ""
		in.EndpointPath = ""
		in.LBStrategy = ""

	case TransportHTTP:
		// http 模式：地址由发现层给出，本地命令与参数必须清空
		if strings.TrimSpace(in.Command) != "" {
			return fmt.Errorf("命名空间 %s 是 http 模式，不能配置 command（地址由发现层给出）", in.Slug)
		}
		in.Command = ""
		in.Args = []string{}
		in.Env = map[string]string{}

		in.DiscoveryService = strings.TrimSpace(in.DiscoveryService)
		if in.DiscoveryService == "" {
			return fmt.Errorf("命名空间 %s 是 http 模式，必须配置 discovery_service（注册中心里的服务名）", in.Slug)
		}

		provider := strings.ToLower(strings.TrimSpace(in.DiscoveryProvider))
		if provider == "" {
			// 未显式指定时按 static 处理：静态地址列表是最小可用形态
			provider = "static"
		}
		switch provider {
		case "rnacos", "static":
		default:
			return fmt.Errorf("命名空间 %s 的 discovery_provider=%q 非法，只能是 rnacos 或 static", in.Slug, in.DiscoveryProvider)
		}
		in.DiscoveryProvider = provider

		if in.DiscoveryGroup = strings.TrimSpace(in.DiscoveryGroup); in.DiscoveryGroup == "" {
			in.DiscoveryGroup = "DEFAULT_GROUP"
		}
		if in.EndpointPath = strings.TrimSpace(in.EndpointPath); in.EndpointPath == "" {
			in.EndpointPath = "/mcp"
		}
		if !strings.HasPrefix(in.EndpointPath, "/") {
			in.EndpointPath = "/" + in.EndpointPath
		}

		if in.LBStrategy = strings.ToLower(strings.TrimSpace(in.LBStrategy)); in.LBStrategy == "" {
			in.LBStrategy = "round_robin"
		}
		// 本轮只实现轮询，其余策略明确拒绝而不是静默降级
		if in.LBStrategy != "round_robin" {
			return fmt.Errorf("命名空间 %s 的 lb_strategy=%q 暂不支持，本轮仅支持 round_robin", in.Slug, in.LBStrategy)
		}
	}

	// 注册中心只可能发现 http 服务：stdio 子进程只能由本地拉起，
	// 不存在「远端注册一个本地进程」这回事。与迁移 0010 的 CHECK 对齐。
	if in.Source == SourceRegistry && in.Transport != TransportHTTP {
		return fmt.Errorf("命名空间 %s 标记为 registry 来源，必须是 http 传输", in.Slug)
	}

	return nil
}

func normalizeArgs(in []string) []string {
	out := make([]string, 0, len(in))
	for _, a := range in {
		out = append(out, a)
	}
	return out
}

func normalizeEnv(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// GetNamespaceBySlug 按 slug 查单个命名空间（含工具白名单）。
func (s *Store) GetNamespaceBySlug(ctx context.Context, slug string) (*Namespace, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+namespaceColumns+` FROM namespaces WHERE slug = $1`, slug)

	ns, err := scanNamespace(row)
	if err != nil {
		return nil, err
	}
	if ns.Tools, err = s.listAllowedTools(ctx, ns.ID); err != nil {
		return nil, err
	}
	return ns, nil
}

// GetNamespaceByID 按主键查单个命名空间。
func (s *Store) GetNamespaceByID(ctx context.Context, id idgen.ID) (*Namespace, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+namespaceColumns+` FROM namespaces WHERE id = $1`, id)

	ns, err := scanNamespace(row)
	if err != nil {
		return nil, err
	}
	if ns.Tools, err = s.listAllowedTools(ctx, ns.ID); err != nil {
		return nil, err
	}
	return ns, nil
}

// ListNamespaces 返回全部命名空间（含工具白名单）。
func (s *Store) ListNamespaces(ctx context.Context) ([]Namespace, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+namespaceColumns+` FROM namespaces ORDER BY slug`)
	if err != nil {
		return nil, fmt.Errorf("查询命名空间列表失败: %w", err)
	}
	defer rows.Close()

	var out []Namespace
	for rows.Next() {
		ns, err := scanNamespaceRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ns)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Tools, err = s.listAllowedTools(ctx, out[i].ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ListEnabledNamespaces 只返回启用的命名空间，供启动时建进程池。
func (s *Store) ListEnabledNamespaces(ctx context.Context) ([]Namespace, error) {
	all, err := s.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, ns := range all {
		if ns.Enabled {
			out = append(out, ns)
		}
	}
	return out, nil
}

func (s *Store) listAllowedTools(ctx context.Context, namespaceID idgen.ID) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tool_name FROM namespace_tools
		WHERE namespace_id = $1 AND allowed
		ORDER BY tool_name`, namespaceID)
	if err != nil {
		return nil, fmt.Errorf("查询工具白名单失败: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// namespaceScanTargets 让单行扫描与多行扫描共用同一份参数顺序，
// 避免以后加字段时只改了一处，另一处悄悄错位。
func namespaceScanTargets(ns *Namespace, argsRaw, envRaw *[]byte) []any {
	return []any{
		&ns.ID, &ns.Slug, &ns.Description, &ns.Transport, &ns.Command,
		argsRaw, envRaw, &ns.PoolSize, &ns.TimeoutMS, &ns.Enabled, &ns.DisabledBy,
		&ns.DiscoveryProvider, &ns.DiscoveryService, &ns.DiscoveryGroup,
		&ns.EndpointPath, &ns.LBStrategy, &ns.Source, &ns.LastSeenAt,
	}
}

func scanNamespace(row pgx.Row) (*Namespace, error) {
	var (
		ns              Namespace
		argsRaw, envRaw []byte
	)
	err := row.Scan(namespaceScanTargets(&ns, &argsRaw, &envRaw)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("读取命名空间失败: %w", err)
	}
	if err := decodeNamespaceJSON(&ns, argsRaw, envRaw); err != nil {
		return nil, err
	}
	return &ns, nil
}

func scanNamespaceRows(rows pgx.Rows) (*Namespace, error) {
	var (
		ns              Namespace
		argsRaw, envRaw []byte
	)
	if err := rows.Scan(namespaceScanTargets(&ns, &argsRaw, &envRaw)...); err != nil {
		return nil, err
	}
	if err := decodeNamespaceJSON(&ns, argsRaw, envRaw); err != nil {
		return nil, err
	}
	return &ns, nil
}

func decodeNamespaceJSON(ns *Namespace, argsRaw, envRaw []byte) error {
	ns.Args = []string{}
	if len(argsRaw) > 0 {
		if err := json.Unmarshal(argsRaw, &ns.Args); err != nil {
			return fmt.Errorf("解析命名空间 %s 的 args 失败: %w", ns.Slug, err)
		}
	}
	ns.Env = map[string]string{}
	if len(envRaw) > 0 {
		if err := json.Unmarshal(envRaw, &ns.Env); err != nil {
			return fmt.Errorf("解析命名空间 %s 的 env 失败: %w", ns.Slug, err)
		}
	}
	return nil
}

// ReplaceTools 覆盖式写入某命名空间的工具白名单。
func (s *Store) ReplaceTools(ctx context.Context, namespaceID idgen.ID, tools []string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM namespace_tools WHERE namespace_id = $1`, namespaceID); err != nil {
		return fmt.Errorf("清空工具白名单失败: %w", err)
	}
	for _, t := range tools {
		if _, err := tx.Exec(ctx,
			`INSERT INTO namespace_tools (namespace_id, tool_name, allowed) VALUES ($1, $2, TRUE)
			 ON CONFLICT (namespace_id, tool_name) DO UPDATE SET allowed = TRUE, updated_at = now()`,
			namespaceID, t); err != nil {
			return fmt.Errorf("写入工具 %q 失败: %w", t, err)
		}
	}
	return tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// 注册中心同步（注册中心同步）
//
// 这一节只做数据访问：何时拉取、如何比对、要不要下线，全部由 internal/registry
// 的同步器决定；store 只负责「按同步器算出的结论改状态」。
//
// 三条铁律在这里被落成 SQL 条件，而不是靠调用方自觉：
//  1. 只碰 source='registry' 的行。管理员手工建的 manual 与 config.yaml 自举的
//     config 行永远不会被同步器回收（注册中心同步第 3 条）。
//  2. 不覆盖 disabled_by='admin'。管理员手关的服务，不会被一次例行同步悄悄打开。
//  3. 下线只写 enabled / disabled_by，绝不 DELETE，也不碰 namespace_tools
//     与 key_permissions（注册中心同步第 2 条 + 红线「禁止 DELETE 命名空间行」）。
// ---------------------------------------------------------------------------

// RegistryNamespace 是同步器比对用的最小投影。
//
// 为什么不直接复用 Namespace：ListNamespaces 会为每一行再查一次 namespace_tools
// （N+1），而同步器每 60 秒就要把全表读一遍，却完全不需要 args / env / 工具清单。
// 这里只取「判断该不该动、要不要恢复」真正用到的列。
// PoolSize / TimeoutMS 之所以也要取，不是为了比对，而是为了**保留**：
// 同步器刷新一行时不能拿模板值覆盖它们，否则管理员在后台调过的并发与超时
// 每 60 秒被抹一次，且没有任何提示。取出来原样写回是最省事也最可靠的做法。
type RegistryNamespace struct {
	ID                idgen.ID
	Slug              string
	DiscoveryProvider string
	DiscoveryService  string
	DiscoveryGroup    string
	EndpointPath      string
	PoolSize          int
	TimeoutMS         int
	Enabled           bool
	DisabledBy        *string
	LastSeenAt        *time.Time
}

const registryNamespaceColumns = `id, slug, coalesce(discovery_provider, ''),
	                                 coalesce(discovery_service, ''), coalesce(discovery_group, ''),
	                                 coalesce(endpoint_path, ''), pool_size, timeout_ms,
	                                 enabled, disabled_by, last_seen_at`

// ListRegistryNamespaces 列出全部 source='registry' 的行，供同步器与注册中心做差集比对。
func (s *Store) ListRegistryNamespaces(ctx context.Context) ([]RegistryNamespace, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+registryNamespaceColumns+` FROM namespaces WHERE source = 'registry' ORDER BY slug`)
	if err != nil {
		return nil, fmt.Errorf("查询注册中心命名空间失败: %w", err)
	}
	defer rows.Close()

	var out []RegistryNamespace
	for rows.Next() {
		var r RegistryNamespace
		if err := rows.Scan(&r.ID, &r.Slug, &r.DiscoveryProvider, &r.DiscoveryService,
			&r.DiscoveryGroup, &r.EndpointPath, &r.PoolSize, &r.TimeoutMS,
			&r.Enabled, &r.DisabledBy, &r.LastSeenAt); err != nil {
			return nil, fmt.Errorf("读取注册中心命名空间失败: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkRegistrySeen 把「本轮在注册中心观测到的 slug」的 last_seen_at 刷新到 at。
//
// 为什么不并进 upsert：被管理员手工关掉的 registry 行会被同步器故意跳过字段更新，
// 但「它此刻仍在注册中心活着」这个事实必须记下来，否则管理端无法区分
// 「服务下线了」和「服务好好的，只是我关了它」——而这两者的处置完全不同。
//
// 刻意不动 updated_at：last_seen_at 是心跳，不是配置变更。让它污染 updated_at，
// 「这条配置多久没改过」就失去意义了。
func (s *Store) MarkRegistrySeen(ctx context.Context, slugs []string, at time.Time) (int64, error) {
	if len(slugs) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE namespaces SET last_seen_at = $2
		WHERE source = 'registry' AND slug = ANY($1)`, slugs, at)
	if err != nil {
		return 0, fmt.Errorf("刷新命名空间观测时间失败: %w", err)
	}
	return tag.RowsAffected(), nil
}

// OfflineRegistryGone 把一批 registry 来源的命名空间软下线（注册中心同步第 2 条）。
//
// 只写 enabled / disabled_by / updated_at，理由是「不删数据」在服务维度上的落地：
//   - 不 DELETE：namespace_tools 与 key_permissions 必须原样保留。否则服务重新注册
//     回来时权限要重配一遍，等于「上游抖一下，客户就要重新申请」。
//   - 不碰 source / discovery_*：服务回来时靠这些字段原地复用同一行。
//
// 两个 WHERE 条件各有用途：
//   - disabled_by IS DISTINCT FROM 'admin'：管理员手关的行不改写原因，
//     否则「谁关的」会被 registry_gone 抹掉。
//   - enabled：已经下线的行不再改写，本方法因此天然幂等，
//     不会每 60 秒把 updated_at 顶一遍（那是同步噪音，不是状态变化）。
func (s *Store) OfflineRegistryGone(ctx context.Context, ids []idgen.ID) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	// 显式转成 []string：pgx 对 []idgen.ID 这种自定义元素类型的数组编码
	// 依赖 driver.Valuer 的反射包装，能跑但没必要赌，转一次成本可以忽略。
	idStrings := make([]string, 0, len(ids))
	for _, id := range ids {
		idStrings = append(idStrings, string(id))
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE namespaces
		SET enabled = FALSE, disabled_by = $2, updated_at = now()
		WHERE id = ANY($1) AND source = 'registry'
		  AND enabled
		  AND disabled_by IS DISTINCT FROM $3`, idStrings, DisabledByRegistryGone, DisabledByAdmin)
	if err != nil {
		return 0, fmt.Errorf("软下线命名空间失败: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RestoreRegistryNamespace 在服务重新注册回来时原地恢复（注册中心同步第 2 条）。
//
// 与 EnableNamespace 的唯一区别是那道 disabled_by IS DISTINCT FROM 'admin' 守卫：
// 管理员手关的服务，同步器不得因为「它又在注册中心出现了」就替他打开。
//
// 返回 changed 而不是错误：false 表示「本来就该是启用的」或「被 admin 锁着」，
// 这两种情况对调用方都要继续处理下一行，不需要区分成失败。
func (s *Store) RestoreRegistryNamespace(ctx context.Context, id idgen.ID) (bool, error) {
	if id.IsZero() {
		return false, errors.New("命名空间 ID 不能为空")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE namespaces
		SET enabled = TRUE, disabled_by = NULL, updated_at = now()
		WHERE id = $1 AND source = 'registry'
		  AND disabled_by IS DISTINCT FROM $2
		  AND NOT enabled`, id, DisabledByAdmin)
	if err != nil {
		return false, fmt.Errorf("恢复命名空间 %s 失败: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}
