// Package registry 是「注册中心 → 命名空间表」的同步器。
//
// 存在的理由（注册中心同步）：新增一个 MCP 服务时，网关侧允许改的东西只有零个——
// 不改代码、不改 config.yaml、不改 SQL。接入方把服务注册到 r-nacos，
// 同步器把服务清单拉下来，自动补建 / 恢复 / 软下线对应命名空间。
//
// 本包只做决策，不做数据访问细节：改哪一行、怎么改由这里决定，
// 具体的 SQL 与守卫条件写在 internal/store 的 namespaces.go 里。
// 这样「什么算下线」「谁有权覆盖谁」这类业务判断留在可测试的位置，
// 而「NULL 会不会扫崩」「字符串数组怎么编码」留在 store 层。
//
// 硬性边界（每一条都对应注册中心同步的一条规则）：
//   - 只碰 source='registry' 的行。manual / config 永远不动。
//   - 管理员手工禁用（disabled_by='admin'）的行，同步器不覆盖、不恢复。
//   - 下线是软的：enabled=false + disabled_by='registry_gone'，不 DELETE，
//     也不动 namespace_tools / key_permissions。
//   - 同步失败不是启动失败。注册中心不可用时网关照常服务已有命名空间。
package registry

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// ErrNotSupported 表示当前发现层实现不提供「枚举服务」能力。
//
// 典型场景是 discovery.provider=static：静态地址列表只有一组写死的地址，
// 根本没有「注册中心里有哪些服务」这个概念。这不是错误配置，
// 而是同步器在这种部署形态下无事可做，所以调用方应当按「跳过 + 记一行日志」处理。
var ErrNotSupported = errors.New("当前发现层不支持枚举服务，同步器无事可做")

// slug 格式与 0001_schema.sql 的 namespaces_slug_format 保持一致。
//
// 在这里先校验一次，而不是等 INSERT 被约束拒绝：约束报错只说「violates
// check constraint namespaces_slug_format」，运维无法知道是哪个服务名不合规，
// 而注册中心里的服务名往往由接入方随意起名。
var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// Options 是同步器的全部可调参数。
//
// 刻意不依赖 config.Config：本包只需要这几个值，传整个配置结构体会让
// 单测必须造一份完整 Config，也会让本包反向依赖配置层。
type Options struct {
	// Lister 是注册中心的枚举能力，通常由 discovery.Bundle.Resolver 断言得到。
	Lister discovery.Lister

	// ServicePrefix 只同步以此开头的服务名，并在生成 slug 时剥掉它。
	//
	// 为什么需要：注册中心往往同时承载业务微服务（user-service、order-service…），
	// 不设前缀会把它们统统变成 MCP 命名空间，管理端瞬间被垃圾数据填满。
	// 空表示不过滤、也不剥前缀。
	ServicePrefix string

	// EndpointPath 是新建命名空间的默认端点路径，空表示 /mcp。
	EndpointPath string

	// TimeoutMS 是新建命名空间的默认单次调用超时，<=0 表示 120000。
	TimeoutMS int

	// PoolSize 是新建命名空间的默认进程池大小，<=0 表示 1。
	//
	// http 命名空间的 pool_size 只是配额上限（真正并发由上游自己扛），
	// 与 stdio 的「本地子进程数」不是一回事，但表结构上共用一列。
	PoolSize int

	// Logf 是日志出口，nil 时静默。生产传 log.Printf，测试传 t.Logf。
	Logf func(format string, args ...any)
}

// Stats 是一次同步的结果，供日志与 /admin 接口展示。
//
// 分开统计新增 / 恢复 / 下线，而不是只报一个「处理了 N 条」：
// 出现异常时首先要知道的往往是「是不是误下线了」，而这个数字必须能独立看到。
type Stats struct {
	// Discovered 是注册中心里通过前缀过滤后、slug 合法的服务数。
	Discovered int
	// Created 是本次新建的命名空间数。
	Created int
	// Refreshed 是对已有行做配置刷新的次数。
	Refreshed int
	// Restored 是「服务重新出现」而原地恢复的次数。
	Restored int
	// Offlined 是「注册中心里消失」而软下线的次数。
	Offlined int
	// Skipped 是被跳过的行数（前缀不符、slug 非法、被管理员禁用）。
	Skipped int
	// Conflicts 是因 slug 已被 manual / config 占用而跳过写入的数量。
	Conflicts int

	// StartedAt / FinishedAt 便于管理端展示「上次同步耗时」。
	StartedAt  time.Time
	FinishedAt time.Time
}

// String 让日志里一行就能看清整轮同步发生了什么。
func (s Stats) String() string {
	return fmt.Sprintf(
		"发现 %d，新建 %d，刷新 %d，恢复 %d，下线 %d，跳过 %d，冲突 %d，耗时 %s",
		s.Discovered, s.Created, s.Refreshed, s.Restored, s.Offlined,
		s.Skipped, s.Conflicts, s.FinishedAt.Sub(s.StartedAt).Round(time.Millisecond))
}

// Synchronizer 把注册中心的服务清单同步进 namespaces 表。
//
// 不持有后台定时器：节奏由外部（main 或 admin 端点）控制。
// 这样「点一次立即同步」与「每 60 秒自动同步」走的是同一段代码，
// 不会出现两条语义略有差异的同步路径。
type Synchronizer struct {
	st   *store.Store
	opts Options
	now  func() time.Time
}

// New 构造同步器。
func New(st *store.Store, opts Options) *Synchronizer {
	if opts.EndpointPath == "" {
		opts.EndpointPath = "/mcp"
	}
	if !strings.HasPrefix(opts.EndpointPath, "/") {
		opts.EndpointPath = "/" + opts.EndpointPath
	}
	if opts.TimeoutMS <= 0 {
		opts.TimeoutMS = 120000
	}
	if opts.PoolSize <= 0 {
		opts.PoolSize = 1
	}
	return &Synchronizer{st: st, opts: opts, now: time.Now}
}

// WithClock 只在测试里用，用来把「现在」变成可控值。
func (s *Synchronizer) WithClock(now func() time.Time) *Synchronizer {
	s.now = now
	return s
}

// SyncOnce 执行一轮完整同步。
//
// 顺序是刻意的：先补建/恢复，再刷新观测时间，最后才下线。
// 如果反过来先下线再补建，一个抖动的注册中心会在同一轮里
// 先把行置为 registry_gone 再恢复，日志里全是无意义的翻转。
func (s *Synchronizer) SyncOnce(ctx context.Context) (Stats, error) {
	stats := Stats{StartedAt: s.now()}

	if s.opts.Lister == nil {
		stats.FinishedAt = s.now()
		return stats, ErrNotSupported
	}

	names, err := s.opts.Lister.List(ctx)
	if err != nil {
		// 拉不到清单时**绝不**下线任何东西：注册中心故障与「服务全没了」
		// 在数据上不可区分，按后者处理会一次性下线全部服务。
		stats.FinishedAt = s.now()
		return stats, fmt.Errorf("从注册中心拉取服务清单失败: %w", err)
	}
	group := strings.TrimSpace(s.opts.Lister.ListGroup())
	if group == "" {
		group = "DEFAULT_GROUP"
	}

	// slug → 原始服务名。同名服务只保留一个（List 已去重，这里再兜一次，
	// 因为不同服务名剥前缀后可能撞到同一个 slug）。
	desired := make(map[string]string, len(names))
	for _, raw := range names {
		slug, ok := s.slugOf(raw)
		if !ok {
			stats.Skipped++
			s.logf("跳过服务 %q：不满足 %s 前缀或 slug 不合法", raw, s.opts.ServicePrefix)
			continue
		}
		service := strings.TrimSpace(raw)
		if prev, dup := desired[slug]; dup {
			// 两个服务名映射到同一 slug 时不能猜，明确报出来。
			// 静默取其一会让另一个服务永远不可达，且没有任何日志。
			stats.Skipped++
			s.logf("跳过服务 %q：slug=%s 已被服务 %q 占用（剥前缀后重名）", raw, slug, prev)
			continue
		}
		desired[slug] = service
	}
	stats.Discovered = len(desired)

	existing, err := s.st.ListRegistryNamespaces(ctx)
	if err != nil {
		stats.FinishedAt = s.now()
		return stats, err
	}
	bySlug := make(map[string]store.RegistryNamespace, len(existing))
	for _, r := range existing {
		bySlug[r.Slug] = r
	}

	// 本轮在注册中心真实看到的 slug，用于刷新 last_seen_at。
	//
	// 包含「被管理员禁用」的行：那些行虽然不更新字段，但「服务活着」这个
	// 事实必须记下来，否则管理端无法区分「服务下线」与「我关了它」。
	seen := make([]string, 0, len(desired))

	// 按 slug 排序后再处理：SQL 与日志的顺序稳定，出问题时两次运行的
	// 输出可以逐行对比，不会因为 map 迭代顺序而看不出差异。
	slugs := make([]string, 0, len(desired))
	for slug := range desired {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)

	for _, slug := range slugs {
		service := desired[slug]
		seen = append(seen, slug)

		cur, exists := bySlug[slug]
		switch {
		case !exists:
			if err := s.create(ctx, slug, service, group); err != nil {
				// slug 被 manual / config 占用是「需要人处理」的事，
				// 与「这一行写失败了」分开统计：前者可能要改接入方的服务名，
				// 后者只是重试。混在一个数字里就分不清该找谁。
				if errors.Is(err, store.ErrSlugConflict) {
					stats.Conflicts++
					s.logf("跳过服务 %s：slug=%s 已被 manual/config 命名空间占用", service, slug)
					continue
				}
				// 单行失败不中断整轮：一个服务名有问题不该让其余服务也同步不了。
				stats.Skipped++
				s.logf("新建命名空间 %s（服务 %s）失败：%v", slug, service, err)
				continue
			}
			stats.Created++

		case cur.DisabledBy != nil && *cur.DisabledBy == store.DisabledByAdmin:
			// 管理员手关的服务即使还在注册中心里，也保持关闭。
			// 这是注册中心同步里「管理员配置优先」的唯一一处硬约束。
			stats.Skipped++
			s.logf("命名空间 %s 已被管理员禁用，同步器跳过（服务 %s 仍在注册中心）", slug, service)

		case !cur.Enabled:
			// 走到这里只可能是 registry_gone：服务重新出现了，原地恢复。
			// 用 RestoreRegistryNamespace 而不是再跑一次 upsert——
			// 后者会把 endpoint_path 等字段重置成默认模板，抹掉此前的调整。
			changed, err := s.st.RestoreRegistryNamespace(ctx, cur.ID)
			if err != nil {
				stats.Skipped++
				s.logf("恢复命名空间 %s 失败：%v", slug, err)
				continue
			}
			if changed {
				stats.Restored++
				s.logf("命名空间 %s 已恢复（服务 %s 重新出现在注册中心）", slug, service)
			}

		default:
			if err := s.refresh(ctx, cur, service, group); err != nil {
				stats.Skipped++
				s.logf("刷新命名空间 %s 失败：%v", slug, err)
				continue
			}
			stats.Refreshed++
		}
	}

	// 观测时间单独一轮批量刷新，而不是塞进上面的循环：
	// 那是 N 次 UPDATE，这里是 1 次，且不受单行失败影响。
	if n, err := s.st.MarkRegistrySeen(ctx, seen, s.now()); err != nil {
		// 刷新心跳失败不影响「该下线谁」的判断，记录即可。
		s.logf("刷新 last_seen_at 失败：%v", err)
	} else if n > 0 {
		s.logf("已刷新 %d 个命名空间的观测时间", n)
	}

	// 下线：注册中心里已经看不到的 registry 行。
	//
	// 判据是 slug 不在 seen 里，而不是「discovery_service 不在 names 里」：
	// 服务名到 slug 的映射可能随模板变化，用 slug 比对才与库里存的东西一致，
	// 也不会因为改名一次就误下线一遍。
	var gone []idgen.ID
	for _, r := range existing {
		if _, still := desired[r.Slug]; still {
			continue
		}
		// 已经下线的行不必再进候选：store 层也会过滤，但在这里先滤掉
		// 能让日志里的数字就是「本轮真正翻动的行数」。
		if !r.Enabled {
			continue
		}
		gone = append(gone, r.ID)
	}
	if len(gone) > 0 {
		n, err := s.st.OfflineRegistryGone(ctx, gone)
		if err != nil {
			stats.FinishedAt = s.now()
			return stats, err
		}
		stats.Offlined = int(n)
		if n > 0 {
			s.logf("已软下线 %d 个命名空间（注册中心里已消失，行与权限保留）", n)
		}
	}

	stats.FinishedAt = s.now()
	return stats, nil
}

// slugOf 把注册中心服务名映射成命名空间 slug。
//
// 规则见注册中心同步：serviceName 形如 mcp-blender → slug=blender。
// 前缀为空时直接用服务名，不做任何改写——「配置了空前缀」应当意味着
// 「注册中心里所有服务都是 MCP 服务」，而不是悄悄套一个默认前缀。
func (s *Synchronizer) slugOf(service string) (string, bool) {
	name := strings.TrimSpace(service)
	if name == "" {
		return "", false
	}
	if prefix := s.opts.ServicePrefix; prefix != "" {
		if !strings.HasPrefix(name, prefix) {
			return "", false
		}
		name = strings.TrimPrefix(name, prefix)
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || !slugPattern.MatchString(name) {
		return "", false
	}
	return name, true
}

// template 生成新建行的默认形态，集中在一处，避免 create/refresh 各写一份。
func (s *Synchronizer) template(slug, service, group string) store.Namespace {
	display := slug
	return store.Namespace{
		Slug:              slug,
		Description:       display,
		Transport:         store.TransportHTTP,
		PoolSize:          s.opts.PoolSize,
		TimeoutMS:         s.opts.TimeoutMS,
		Enabled:           true,
		DiscoveryProvider: "rnacos",
		DiscoveryService:  service,
		DiscoveryGroup:    group,
		EndpointPath:      s.opts.EndpointPath,
		LBStrategy:        "round_robin",
		Source:            store.SourceRegistry,
	}
}

func (s *Synchronizer) create(ctx context.Context, slug, service, group string) error {
	newID, err := idgen.NewRandom(idgen.KindNamespace)
	if err != nil {
		return fmt.Errorf("生成命名空间 ID 失败: %w", err)
	}
	_, err = s.st.UpsertRegistryNamespace(ctx, newID, s.template(slug, service, group))
	return err
}

// refresh 只更新「注册中心说了算」的字段。
//
// 不重写 PoolSize / TimeoutMS / Description：这三个都可能被管理员在后台调过，
// 而注册中心对它们一无所知。用模板整行覆盖等于每 60 秒抹一次人工配置，
// 抹掉之后还没有任何痕迹可查。做法是把库里现有值原样写回。
//
// 真正会被刷新的只有 discovery_service / discovery_group / endpoint_path：
// 接入方把服务挪到别的分组、或改了服务名而 slug 不变时，这里才能跟上。
func (s *Synchronizer) refresh(ctx context.Context, cur store.RegistryNamespace, service, group string) error {
	ns := s.template(cur.Slug, service, group)
	// 接管库里已有的人工配置，避免模板值把它们冲掉。
	ns.PoolSize = cur.PoolSize
	ns.TimeoutMS = cur.TimeoutMS
	// 保持原有端点路径：它是管理员可能改过的字段之一。
	if p := strings.TrimSpace(cur.EndpointPath); p != "" {
		ns.EndpointPath = p
	}
	_, err := s.st.UpsertRegistryNamespace(ctx, cur.ID, ns)
	return err
}

func (s *Synchronizer) logf(format string, args ...any) {
	if s.opts.Logf != nil {
		s.opts.Logf(format, args...)
	}
}
