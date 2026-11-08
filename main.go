// Package main 是网关进程的启动入口。
//
// 启动编排（M1）：
//
//	flag → 配置 → 雪花 ID → 数据库 → 迁移 → 动态配置中心 → 命名空间自举
//	     → 发现层 → 计费旁路 → 录制旁路 → HTTP/SSE 服务 → 信号优雅退出
//
// 兼容性红线：不带 -config 启动时，行为必须与改造前完全一致
// （-oauth2Bearer ceshi、-command 拉起单个 stdio 子进程）。
// 新增能力只在有多租户数据库时启用，绝不影响旧用法。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/gateway"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/billing"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/config"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/recorder"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/settings"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/shard"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// cliFlags 保留全部历史命令行参数，一个都不能少。
type cliFlags struct {
	configPath   string
	name         string
	version      string
	port         int
	baseURL      string
	ssePath      string
	messagePath  string
	command      string
	oauth2Bearer string

	// resetSchema 是建库自检专用的一次性开关：先 DROP SCHEMA public CASCADE，
	// 再从空库重建全部表与种子，然后直接退出（不进入网关服务）。
	//
	// 它刻意做成命令行参数而不是配置项：配置会跟着 git 走、会被 copy 到
	// 别处，而这种「清整个库」的动作必须由人在终端上手打一次。
	resetSchema bool

	// confirmReset 是 resetSchema 的二次确认，值必须正好是 "yes"。
	// 两道门是刻意的：只写一个 --reset-schema 很难误触，
	// 但复制粘贴一行历史命令时很容易把两个参数一起带过来，
	// 所以还要求 DSN 指向 localhost / 127.0.0.1（见 confirmResetSchema）。
	confirmReset string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[mcp-gateway] ")

	var f cliFlags
	flag.StringVar(&f.configPath, "config", "", "config.yaml 路径；留空则只用命令行参数（旧行为）")
	flag.StringVar(&f.name, "name", "hoxkar-mcp-gateway", "SSE Server name")
	flag.StringVar(&f.version, "version", "1.0.0", "SSE Server version")
	flag.IntVar(&f.port, "port", 7979, "Server listening port")
	flag.StringVar(&f.baseURL, "baseUrl", "http://127.0.0.1:7979", "Base URL")
	flag.StringVar(&f.ssePath, "ssePath", "/sse", "Base URL for SSE")
	flag.StringVar(&f.messagePath, "messagePath", "/messages", "Path for messages")
	flag.StringVar(&f.command, "command", "", "Command to run")
	flag.StringVar(&f.oauth2Bearer, "oauth2Bearer", "ceshi", "OAuth2 Bearer token")
	flag.BoolVar(&f.resetSchema, "reset-schema", false,
		"【开发期危险操作】清空 public schema 后重建全部表与种子，完成即退出；需同时传 -confirm-reset yes 且 DSN 指向本机")
	flag.StringVar(&f.confirmReset, "confirm-reset", "", "配合 -reset-schema 使用，值必须为 yes")
	flag.Parse()

	if err := run(f); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
}

// runtime 汇总所有需要随进程退出的组件，便于统一关闭。
//
// 关闭顺序刻意与启动顺序相反：先让旁路把缓冲刷进数据库，再断连接池。
type runtime struct {
	cfg *config.Config
	// pools 必须在 st 之前关闭，因此字段顺序也就是关闭顺序的一部分；
	// 实际关闭走 close()，不要依赖字段顺序做隐式清理。
	pools     *shard.PoolSet
	st        *store.Store
	dir       *shard.Directory
	settings  *settings.Manager
	billing   *billing.Service
	recorder  *recorder.Recorder
	discovery *discovery.Bundle
	// regsync 为 nil 表示本次部署没有启用注册中心自动同步（registry.auto_discover=false）
	regsync *registrySync
	maint   *maintenance
	// resetOnly 表示本次进程只做建库，跑完迁移就退出，不启动任何后台循环
	resetOnly bool
}

func (rt *runtime) close() {
	if rt == nil {
		return
	}
	// 第一步就要停后台扫描：它持有连接池，若等池关完再停，
	// 中途会有一次必然失败的查询，日志里全是噪声。
	//
	// regsync 必须排在最前：它会写 namespaces 表，而 migrations 之后的
	// 任何一次写入都可能与「正在退出」的其它组件抢连接。先停它，
	// 其余组件看到的表状态就是稳定的。
	if rt.regsync != nil {
		rt.regsync.Close()
	}
	if rt.maint != nil {
		rt.maint.Close()
	}
	// 旁路组件内部自己带超时，这里不会被无限拖住
	if rt.recorder != nil {
		rt.recorder.Close()
		log.Printf("[recorder] 已停止：%+v", rt.recorder.Stats())
	}
	if rt.billing != nil {
		rt.billing.Close()
		written, dropped, failed := rt.billing.Stats()
		log.Printf("[billing] 已停止：写入=%d 丢弃=%d 失败=%d", written, dropped, failed)
	}
	if rt.settings != nil {
		rt.settings.Close()
	}
	// Store 借的是 PoolSet 的默认池，自己不会关池，这里只是断引用
	if rt.st != nil {
		rt.st.Close()
	}
	// 池必须最后关：前面所有组件都可能还有在途查询
	if rt.pools != nil {
		rt.pools.Close()
	}
}

func run(f cliFlags) error {
	// -----------------------------------------------------------------------
	// 1. 配置：文件优先，命令行参数按「是否显式传入」覆盖
	// -----------------------------------------------------------------------
	cfg, err := config.Load(f.configPath)
	if err != nil {
		return err
	}
	applyFlagOverrides(cfg, f)

	// 只有显式使用配置文件时才做严格校验。
	// 旧用法（无 -config）必须原样放行，否则 -oauth2Bearer ceshi 会被拒。
	if f.configPath != "" {
		if err := cfg.Validate(); err != nil {
			return err
		}
	}

	// -----------------------------------------------------------------------
	// 2. 雪花 ID：必须在任何写库动作之前完成
	// -----------------------------------------------------------------------
	if err := idgen.Init(cfg.IDGen.NodeID); err != nil {
		return fmt.Errorf("初始化雪花 ID 失败（idgen.node_id=%d）: %w", cfg.IDGen.NodeID, err)
	}

	// -----------------------------------------------------------------------
	// 3. 无数据库：保持旧行为，只起 SSE 网关
	// -----------------------------------------------------------------------
	if cfg.Mode() != config.ModeMultiTenant {
		if f.resetSchema {
			return errors.New("拒绝清库：当前配置没有 database.dsn，不属于多租户模式，没有可建的库")
		}
		log.Printf("运行模式：%s（未配置 database.dsn，跳过数据库相关组件）", cfg.Mode())
		return serveForever(cfg, buildLegacyCommand(f.command))
	}

	// -----------------------------------------------------------------------
	// 3.5. 清库闸门：必须在建池之前判，不能等到连上库再拒绝
	//
	// 这里的顺序是有意的。若把确认放到 Ping 之后，非法 DSN 会先建立一次
	// 对外连接、失败后返回「连通性检查失败」——运维看到的是网络问题，
	// 而真实原因是「你差点清了生产库」。先拒绝，日志才说得清楚。
	// -----------------------------------------------------------------------
	if f.resetSchema {
		if err := confirmResetSchema(cfg.Database.DSN, f.confirmReset); err != nil {
			return err
		}
	}

	// -----------------------------------------------------------------------
	// 4. 多租户：完整编排
	// -----------------------------------------------------------------------
	log.Printf("运行模式：%s", cfg.Mode())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &runtime{cfg: cfg}
	defer rt.close()

	// 连接池由 shard.PoolSet 统一持有（租户与分片）。单库阶段 ShardDSNs 为空，
	// 集合里只有默认池；将来拆库只需在 config.yaml 加条目，
	// 这里的代码一行都不用改。
	pools, err := shard.Open(cfg.Database.DSN, cfg.Database.ShardDSNs, cfg.Database.MaxConns)
	if err != nil {
		return fmt.Errorf("创建数据库连接池失败: %w", err)
	}
	rt.pools = pools

	// 启动即校验全部池的连通性：某个分片库连不上就别开始接流量
	if err := pools.Ping(ctx); err != nil {
		return fmt.Errorf("数据库连通性检查失败: %w", err)
	}

	// Store 借用 PoolSet 的默认池，不重复建池、也不重复关池
	st, err := store.Attach(pools.Default())
	if err != nil {
		return fmt.Errorf("初始化数据库访问层失败: %w", err)
	}
	rt.st = st

	// 分片目录：租户 → 分片的粘性映射缓存。store 已经是 shard.Source，
	// 直接注入，避免再包一层适配器。
	dir := shard.NewDirectory(st)
	rt.dir = dir

	migrateOpts := store.MigrateOptions{
		ResetSchema: f.resetSchema,
		Logf:        func(format string, args ...any) { log.Printf(format, args...) },
	}
	rt.resetOnly = f.resetSchema
	if err := store.MigrateWithOptions(ctx, st, migrateOpts); err != nil {
		return fmt.Errorf("执行数据库迁移失败: %w", err)
	}
	log.Printf("数据库迁移已应用到最新版本")

	// 建库模式到此为止：不要往下走 bootstrap / discovery / 后台扫描。
	// 那些组件都假定表已经稳定存在，在刚清空的库上跑只会制造噪声日志。
	if rt.resetOnly {
		if err := verifySchema(ctx, st); err != nil {
			return fmt.Errorf("建库后自检未通过: %w", err)
		}
		log.Printf("建库完成：schema 已重建并自检通过，进程退出（本次未启动网关服务）")
		return nil
	}

	// 迁移之后才能 Warm：目录要读 tenants 表，表不存在时应当报错而不是静默空转
	if err := dir.Warm(ctx); err != nil {
		return fmt.Errorf("预热租户分片目录失败: %w", err)
	}
	log.Printf("租户分片目录已预热：%d 条映射", dir.Len())

	sm, err := settings.New(ctx, st)
	if err != nil {
		return fmt.Errorf("加载动态配置中心失败: %w", err)
	}
	rt.settings = sm
	// 后台监听 system_settings 变更，热更新失败只记录日志，不影响主流程（动态配置）
	sm.Watch(ctx)

	if err := bootstrapNamespaces(ctx, st, cfg.Namespaces); err != nil {
		return err
	}

	bundle, err := discovery.Build(discovery.Options{
		Provider:          cfg.Discovery.Provider,
		CacheTTL:          time.Duration(cfg.Discovery.CacheTTLSeconds) * time.Second,
		FailOpen:          cfg.Discovery.FailOpen,
		StaticEndpoints:   cfg.Discovery.StaticEndpoints,
		RnacosAddr:        cfg.Discovery.Rnacos.Addr,
		RnacosNamespace:   cfg.Discovery.Rnacos.Namespace,
		RnacosGroup:       cfg.Discovery.Rnacos.Group,
		RnacosUsername:    cfg.Discovery.Rnacos.Username,
		RnacosPassword:    cfg.Discovery.Rnacos.Password,
		RnacosTimeout:     time.Duration(cfg.Discovery.Rnacos.TimeoutSeconds) * time.Second,
		RnacosHealthyOnly: cfg.Discovery.Rnacos.HealthyOnly,
	})
	if err != nil {
		return fmt.Errorf("初始化服务发现失败: %w", err)
	}
	rt.discovery = bundle
	log.Printf("服务发现已就绪：provider=%s，fail_open=%v，静态兜底 %d 个地址",
		cfg.Discovery.Provider, cfg.Discovery.FailOpen, len(cfg.Discovery.StaticEndpoints))

	rt.billing = billing.New(ctx, st, sm, billing.Options{
		PriceTTL: 60 * time.Second,
	})

	rt.recorder = recorder.New(st, sm, recorder.Options{
		Enabled:             cfg.SessionRecording.Enabled,
		BatchSize:           cfg.SessionRecording.BatchSize,
		FlushInterval:       time.Duration(cfg.SessionRecording.FlushInterval) * time.Second,
		MaxPayloadBytes:     cfg.SessionRecording.MaxPayloadBytes,
		RedactSensitive:     cfg.SessionRecording.RedactSensitive,
		CaptureToolPayloads: cfg.SessionRecording.CaptureToolPayloads,
	})
	log.Printf("会话录制：enabled=%v（动态配置 recording.enabled 可覆盖）", rt.recorder.Enabled())

	// -----------------------------------------------------------------------
	// 5. 注册中心同步（注册中心同步）：把 r-nacos 里的服务清单变成命名空间行
	//
	// 位置是刻意的：必须排在 discovery.Build 之后（要拿它的 Lister 能力），
	// 又必须排在 serveForever 之前（否则首批请求到达时命名空间还没建出来）。
	//
	// 它不去动 config 自举出来的 stdio 命名空间，两者按 source 天然隔离：
	// 自举写 source='config'，同步器只碰 source='registry'。
	// -----------------------------------------------------------------------
	rt.regsync = startRegistrySync(ctx, st, cfg.Registry, bundle)

	// -----------------------------------------------------------------------
	// 6. 后台维护：预扣超时释放 + 企业租户到期降级
	//
	// 放在旁路组件之后启动，确保它们先拿到连接；关闭时则最先停，
	// 关闭顺序见 runtime.close。
	// -----------------------------------------------------------------------
	rt.maint = startMaintenance(ctx, st, cfg.Maintenance)

	// -----------------------------------------------------------------------
	// 7. 旧路径兼容：数据库里若有 stdio 的 default 命名空间，
	//    仍然可以让 -command 指向它，避免改动老的启动脚本
	// -----------------------------------------------------------------------
	return serveForever(cfg, buildLegacyCommand(f.command))
}

// applyFlagOverrides 只在参数被显式传入时才覆盖配置，
// 否则会用 flag 的默认值把 config.yaml 里的设置冲掉。
func applyFlagOverrides(cfg *config.Config, f cliFlags) {
	set := make(map[string]bool)
	flag.Visit(func(fl *flag.Flag) { set[fl.Name] = true })

	if set["name"] {
		cfg.Server.Name = f.name
	}
	if set["version"] {
		cfg.Server.Version = f.version
	}
	if set["port"] {
		oldPort := cfg.Server.Port
		cfg.Server.Port = f.port
		// 只改 -port 时必须同步 base_url 的端口，否则 /sse 下发给客户端的
		// POST 地址还指着旧端口，客户端连不上。若 base_url 端口与旧监听端口
		// 本就无关（反向代理/域名场景），保持原样不动。
		if !set["baseUrl"] {
			cfg.Server.BaseURL = rewriteBaseURLPort(cfg.Server.BaseURL, oldPort, f.port)
		}
	}
	if set["baseUrl"] {
		cfg.Server.BaseURL = f.baseURL
	}
	if set["ssePath"] {
		cfg.Server.SSEPath = f.ssePath
	}
	if set["messagePath"] {
		cfg.Server.MessagePath = f.messagePath
	}

	// 旧用法下 -oauth2Bearer 的默认值 ceshi 本身就是约定行为，必须保留
	if f.configPath == "" || set["oauth2Bearer"] {
		cfg.Auth.BearerToken = f.oauth2Bearer
	}
}

// rewriteBaseURLPort 在 base_url 的端口确实等于旧监听端口时换成新端口。
//
// 判定条件刻意保守：只有端口能解析出来、且与 oldPort 完全相同时才改写。
// 这样 dev 常见写法 http://127.0.0.1:7979 会因为端口一致而被跟随修正，
// 而 https://mcp.example.com 这类与监听端口无关的地址不会被碰。
func rewriteBaseURLPort(baseURL string, oldPort, newPort int) string {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || oldPort == newPort {
		return baseURL
	}

	port := u.Port()
	if port == "" {
		// 未显式写端口就不猜，交给运维自己决定
		return baseURL
	}
	p, err := strconv.Atoi(port)
	if err != nil || p != oldPort {
		return baseURL
	}

	u.Host = net.JoinHostPort(u.Hostname(), strconv.Itoa(newPort))
	return u.String()
}

// bootstrapNamespaces 把 config.yaml 里的命名空间幂等写入数据库。
//
// 只做「新增或更新」，不做删除：数据库里由管理员通过管理端建出来的
// 命名空间不应该因为部署了一份精简 config.yaml 就被抹掉。
//
// # 为什么必须显式写 SourceConfig
//
// 不写的话 store 层会按零值兜底成 manual，于是这些行看起来像是管理员手工建的。
// 而注册中心同步器只碰 source='registry' 的行（注册中心同步第一条铁律），
// 一旦 config 自举出来的行被标成 manual，语义上就分不清「谁该管它」：
// 接入方在注册中心注册了同名服务，同步器会因为 source 不匹配而永远建不出来，
// 报出来的却是一个看不出根因的 slug 冲突。
//
// 更重要的是 stdio 与 http 的分工：stdio 命名空间只能由 config 声明
// （命令、参数、环境变量这些东西注册中心根本不知道），因此它必须归 source='config'，
// 同步器不去碰；而 http 命名空间从此全部归同步器自动创建。
func bootstrapNamespaces(ctx context.Context, st *store.Store, entries []config.NamespaceEntry) error {
	if len(entries) == 0 {
		return nil
	}

	for _, e := range entries {
		enabled := true
		if e.Enabled != nil {
			enabled = *e.Enabled
		}

		ns := store.Namespace{
			Slug:              e.Slug,
			Description:       e.Description,
			Transport:         e.Transport,
			Command:           e.Command,
			Args:              e.Args,
			Env:               e.Env,
			PoolSize:          e.PoolSize,
			TimeoutMS:         e.TimeoutMS,
			Enabled:           enabled,
			DiscoveryProvider: e.DiscoveryProvider,
			DiscoveryService:  e.DiscoveryService,
			DiscoveryGroup:    e.DiscoveryGroup,
			EndpointPath:      e.EndpointPath,
			LBStrategy:        e.LBStrategy,
			Source:            store.SourceConfig,
		}

		newID, err := idgen.NewRandom(idgen.KindNamespace)
		if err != nil {
			return fmt.Errorf("为命名空间 %s 生成 ID 失败: %w", e.Slug, err)
		}

		saved, err := st.UpsertNamespace(ctx, newID, ns)
		if err != nil {
			return fmt.Errorf("自举命名空间 %s 失败: %w", e.Slug, err)
		}
		log.Printf("命名空间已同步：slug=%s transport=%s enabled=%v",
			saved.Slug, saved.Transport, saved.Enabled)
	}
	return nil
}

// buildLegacyCommand 复刻改造前的 -command 解析逻辑：按空格切分。
//
// 保持原样是有意为之——老用户的启动脚本写着 "-command npx -y xxx"，
// 一旦改成引号语义，他们的参数就会错位。
func buildLegacyCommand(command string) gateway.McpCommand {
	if command == "" {
		return gateway.McpCommand{}
	}
	commands := strings.Split(command, " ")
	mc := gateway.McpCommand{Command: commands[0]}
	if len(commands) > 1 {
		mc.Args = commands[1:]
	}
	return mc
}

// serveForever 组装 SSE 服务并阻塞到进程收到退出信号。
func serveForever(cfg *config.Config, mcpCommand gateway.McpCommand) error {
	s := server.NewMCPServer(cfg.Server.Name, cfg.Server.Version)

	var opt []gateway.SSEOption
	opt = append(opt,
		gateway.WithBaseURL(cfg.Server.BaseURL),
		gateway.WithSSEEndpoint(cfg.Server.SSEPath),
		gateway.WithMessageEndpoint(cfg.Server.MessagePath),
	)
	if cfg.Auth.BearerToken != "" {
		opt = append(opt, gateway.WithOAuth2Bearer(cfg.Auth.BearerToken))
	}

	sseServer := gateway.NewSSEServer(s, opt...)
	gatewayServer := gateway.NewGatewaySSEServer(mcpCommand, sseServer)

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	log.Printf("监听 %s，SSE=%s%s，message=%s%s",
		addr, cfg.Server.BaseURL, cfg.Server.SSEPath,
		cfg.Server.BaseURL, cfg.Server.MessagePath)

	errCh := make(chan error, 1)
	go func() { errCh <- gatewayServer.Start(addr) }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, os.ErrClosed) {
			return fmt.Errorf("HTTP 服务异常退出: %w", err)
		}
		return nil
	case sig := <-sigCh:
		log.Printf("收到信号 %v，开始优雅退出", sig)
		return nil
	}
}
