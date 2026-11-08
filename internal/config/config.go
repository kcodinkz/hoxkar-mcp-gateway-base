// Package config 负责加载 config.yaml 并支持 MCPGW_* 环境变量覆盖。
//
// 设计取舍：
//   - 不引 viper，避免带进一大堆传递依赖；只用 yaml.v3 + 手写环境变量遍历。
//   - 环境变量覆盖按「结构体字段路径」展开，命名规则 MCPGW_ + 路径大写 + 下划线。
//   - Load 不 panic，所有问题以 error 返回，由调用方决定退出方式。
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 是网关的完整配置。所有字段零值都有合理默认，Load 之后可直接使用。
type Config struct {
	Server           ServerConfig           `yaml:"server"`
	Database         DatabaseConfig         `yaml:"database"`
	Auth             AuthConfig             `yaml:"auth"`
	IDGen            IDGenConfig            `yaml:"idgen"`
	SessionRecording SessionRecordingConfig `yaml:"session_recording"`
	Discovery        DiscoveryConfig        `yaml:"discovery"`
	Registry         RegistryConfig         `yaml:"registry"`
	Maintenance      MaintenanceConfig      `yaml:"maintenance"`
	Namespaces       []NamespaceEntry       `yaml:"namespaces"`
}

// RegistryConfig 对应「注册中心 → 命名空间表」的自动同步（注册中心同步）。
//
// 这一段存在的意义就是让新增 MCP 服务时网关侧零改动：接入方把服务注册进
// r-nacos，同步器按 sync_interval_seconds 的节奏把它变成一行命名空间。
//
// 与 discovery 段的分工必须分清：discovery 回答「某个服务名现在有哪些健康实例」，
// registry 回答「注册中心里到底有哪些服务」。前者服务于请求转发，后者服务于建表。
type RegistryConfig struct {
	// AutoDiscover 为 false 时不同步，命名空间只能由手工 / config 自举产生。
	//
	// 默认关闭而不是默认开启：同步器会写数据库，一个没想清楚的部署被
	// 自动塞进一堆命名空间之后，回滚要一行行手工清理。
	AutoDiscover bool `yaml:"auto_discover"`

	// SyncIntervalSeconds 是后台同步周期，默认 60 秒。
	//
	// 不宜过短：每次同步是一次全量 List + 全表 registry 行比对，
	// 服务多的时候这本身就是可观的开销；也不宜过长，接入方删服务后
	// 网关会继续往一个已下线的名字上转发，直到下一轮才发现。
	SyncIntervalSeconds int `yaml:"sync_interval_seconds"`

	// ServicePrefix 只同步以此开头的服务名，生成 slug 时剥掉。
	//
	// 必填项之一（在 auto_discover=true 时）：注册中心通常同时承载普通微服务，
	// 不设前缀会把它们统统变成 MCP 命名空间。空值意味着「全都要」，
	// 这个决定必须由使用者明确做出，不能由默认值替他做。
	ServicePrefix string `yaml:"service_prefix"`

	// EndpointPath 是自动建出来的命名空间的默认端点路径，默认 /mcp。
	EndpointPath string `yaml:"endpoint_path"`

	// TimeoutMS 是自动建出来的命名空间的默认超时，默认 120000。
	TimeoutMS int `yaml:"timeout_ms"`

	// PoolSize 是自动建出来的命名空间的默认池大小，默认 1。
	PoolSize int `yaml:"pool_size"`
}

// SessionRecordingConfig 对应会话录制（会话录制）。
// 录制是旁路：任何异常都不得影响用户请求。
type SessionRecordingConfig struct {
	Enabled             bool  `yaml:"enabled"`
	MaxPayloadBytes     int64 `yaml:"max_payload_bytes"`
	CaptureToolPayloads bool  `yaml:"capture_tool_payloads"`
	RedactSensitive     bool  `yaml:"redact_sensitive"`
	RetentionDays       int   `yaml:"retention_days"`
	// 内部参数，不在 yaml 暴露：单批最大条数与 flush 间隔
	BatchSize     int `yaml:"-"`
	FlushInterval int `yaml:"-"`
}

// DiscoveryConfig 对应 Rust 后端的服务发现（服务发现）。
// 网关是「发现者」，不是注册者。
type DiscoveryConfig struct {
	Provider        string       `yaml:"provider"` // static | rnacos
	CacheTTLSeconds int          `yaml:"cache_ttl_seconds"`
	FailOpen        bool         `yaml:"fail_open"`
	StaticEndpoints []string     `yaml:"static_endpoints"`
	Rnacos          RnacosConfig `yaml:"rnacos"`
}

// RnacosConfig 是 r-nacos（Nacos 协议兼容）客户端参数。
type RnacosConfig struct {
	Addr           string `yaml:"addr"`
	Namespace      string `yaml:"namespace"`
	Group          string `yaml:"group"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
	HealthyOnly    bool   `yaml:"healthy_only"`
}

// IDGenConfig 对应雪花 ID 生成器。
// 多实例部署时 NodeID 必须互不相同，否则会产生重复主键（ID 形态）。
type IDGenConfig struct {
	NodeID int64 `yaml:"node_id"`
}

// ServerConfig 对应 HTTP 与 SSE 层参数。
type ServerConfig struct {
	Name             string   `yaml:"name"`
	Version          string   `yaml:"version"`
	Port             int      `yaml:"port"`
	BaseURL          string   `yaml:"base_url"`
	SSEPath          string   `yaml:"sse_path"`
	MessagePath      string   `yaml:"message_path"`
	KeepAliveSeconds int      `yaml:"keep_alive_seconds"`
	MaxBodyBytes     int64    `yaml:"max_body_bytes"`
	CORSAllowOrigins []string `yaml:"cors_allow_origins"`
}

// DatabaseConfig 描述 PostgreSQL 连接参数。
type DatabaseConfig struct {
	DSN      string `yaml:"dsn"`
	MaxConns int32  `yaml:"max_conns"`

	// ShardDSNs 是「分片号 → DSN」的显式映射，形如 {"03": "postgres://..."}。
	//
	// 单库阶段留空：所有分片共用 dsn 指定的那一个池，分片号只体现在 ID 里
	// （租户与分片）。将来拆库时只需要在这里加条目，代码一行不改——这正是
	// shard.PoolSet 从第一天就存在的原因。
	//
	// 分片号的合法性（两位 Crockford Base32、且不能是保留值 00）由
	// shard.Parse 在启动时校验，配置层不重复实现同一套规则。
	ShardDSNs map[string]string `yaml:"shard_dsns"`
}

// MaintenanceConfig 是后台维护任务的节奏参数。
//
// 这里只放「改一行就要重启进程」的调度参数；任务的开关与效果口径
// （是否降级、降级到哪个套餐）由 system_settings 决定（动态配置）。
type MaintenanceConfig struct {
	// Enabled 关闭后网关不再主动扫描：租户到期不翻状态、预扣超时不释放。
	// 只在排查问题时临时关闭，长期关闭会导致额度被永久冻结。
	Enabled bool `yaml:"enabled"`

	// ReservationSweepSeconds 预扣单超时扫描间隔（计量计费）。
	ReservationSweepSeconds int `yaml:"reservation_sweep_seconds"`

	// TenantExpirySweepSeconds 企业租户到期降级扫描间隔（租户与分片 §9.4）。
	// 刻意与预扣扫描分开：两者失败的后果完全不同，不该互相拖累。
	TenantExpirySweepSeconds int `yaml:"tenant_expiry_sweep_seconds"`

	// BatchSize 单轮处理上限，避免一次积压几万条时锁表过久。
	BatchSize int `yaml:"batch_size"`
}

// AuthConfig 描述鉴权参数。
type AuthConfig struct {
	AdminToken                string `yaml:"admin_token"`
	BearerToken               string `yaml:"bearer_token"`
	KeyCacheTTLSeconds        int    `yaml:"key_cache_ttl_seconds"`
	DefaultRateLimitPerMinute int    `yaml:"default_rate_limit_per_minute"`
}

// NamespaceEntry 是 config.yaml 里用于本地自举的命名空间定义。
// 启动时按 slug 幂等 upsert 到数据库，方便开发环境不必手工调管理接口。
//
// transport 二分（服务发现）：
//   - stdio：本地子进程，用 Command/Args/Env，由进程池管理
//   - http ：远端服务，实例地址由发现层给出，不配置 Command
type NamespaceEntry struct {
	Slug        string            `yaml:"slug"`
	Description string            `yaml:"description"`
	Transport   string            `yaml:"transport"` // stdio（默认）| http
	Command     string            `yaml:"command"`
	Args        []string          `yaml:"args"`
	Env         map[string]string `yaml:"env"`
	PoolSize    int               `yaml:"pool_size"`
	TimeoutMS   int               `yaml:"timeout_ms"`
	Enabled     *bool             `yaml:"enabled"`

	// 以下仅 transport = http 时生效
	DiscoveryProvider string `yaml:"discovery_provider"` // rnacos | static
	DiscoveryService  string `yaml:"discovery_service"`  // 如 blender-core-service
	DiscoveryGroup    string `yaml:"discovery_group"`
	EndpointPath      string `yaml:"endpoint_path"` // 如 /mcp
	LBStrategy        string `yaml:"lb_strategy"`   // round_robin
}

// Mode 表示网关的运行模式，决定鉴权走哪条路径。
type Mode int

const (
	// ModeDevelopment 既无数据库也无 token，明文放行，仅限本机调试。
	ModeDevelopment Mode = iota
	// ModeLegacy 有单 token 但无数据库，保持旧 --oauth2Bearer 行为。
	ModeLegacy
	// ModeMultiTenant 有数据库，走 API Key + 命名空间权限裁决。
	ModeMultiTenant
)

func (m Mode) String() string {
	switch m {
	case ModeMultiTenant:
		return "multi-tenant"
	case ModeLegacy:
		return "legacy-single-token"
	default:
		return "development"
	}
}

// Default 返回一份带默认值的配置。
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Name:             "hoxkar-mcp-gateway",
			Version:          "1.0.0",
			Port:             7979,
			BaseURL:          "http://127.0.0.1:7979",
			SSEPath:          "/sse",
			MessagePath:      "/messages",
			KeepAliveSeconds: 20,
			MaxBodyBytes:     8 << 20,
			CORSAllowOrigins: []string{"*"},
		},
		Database: DatabaseConfig{
			MaxConns: 10,
		},
		Auth: AuthConfig{
			KeyCacheTTLSeconds:        30,
			DefaultRateLimitPerMinute: 60,
		},
		SessionRecording: SessionRecordingConfig{
			Enabled:             true,
			MaxPayloadBytes:     1 << 20, // 1 MiB，超出转 MinIO 并截断
			CaptureToolPayloads: true,
			RedactSensitive:     true,
			RetentionDays:       0, // 0 = 永久保留
			BatchSize:           200,
			FlushInterval:       2, // 秒
		},
		Discovery: DiscoveryConfig{
			Provider:        "static",
			CacheTTLSeconds: 10,
			FailOpen:        true,
			Rnacos: RnacosConfig{
				Addr:           "http://127.0.0.1:8848",
				Namespace:      "public",
				Group:          "DEFAULT_GROUP",
				TimeoutSeconds: 5,
				HealthyOnly:    true,
			},
		},
		Registry: RegistryConfig{
			// 默认不自动同步：见 RegistryConfig.AutoDiscover 的说明。
			AutoDiscover:        false,
			SyncIntervalSeconds: 60,
			// 前缀默认给 mcp-，理由见注册中心同步的命名映射：这是接入方唯一
			// 需要遵守的约定，给个默认值比让人从文档里抄更省事。
			ServicePrefix: "mcp-",
			EndpointPath:  "/mcp",
			TimeoutMS:     120000,
			PoolSize:      1,
		},
		Maintenance: MaintenanceConfig{
			Enabled:                  true,
			ReservationSweepSeconds:  60,
			TenantExpirySweepSeconds: 60,
			BatchSize:                200,
		},
	}
}

// Load 读取配置文件（path 为空则跳过）并应用环境变量覆盖。
//
// 返回值保证非 nil，即使 error 非 nil 也可用于回退到默认值。
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("读取配置文件 %q 失败: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return cfg, fmt.Errorf("解析配置文件 %q 失败: %w", path, err)
		}
	}

	if err := applyEnv(cfg); err != nil {
		return cfg, err
	}

	cfg.normalize()
	return cfg, nil
}

// normalize 收敛不合法输入，保证后续代码不必到处判空。
func (c *Config) normalize() {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		c.Server.Port = 7979
	}
	if c.Server.SSEPath == "" || !strings.HasPrefix(c.Server.SSEPath, "/") {
		c.Server.SSEPath = "/sse"
	}
	if c.Server.MessagePath == "" || !strings.HasPrefix(c.Server.MessagePath, "/") {
		c.Server.MessagePath = "/messages"
	}
	c.Server.SSEPath = strings.TrimSuffix(c.Server.SSEPath, "/")
	c.Server.MessagePath = strings.TrimSuffix(c.Server.MessagePath, "/")
	c.Server.BaseURL = strings.TrimSuffix(c.Server.BaseURL, "/")
	if c.Server.MaxBodyBytes <= 0 {
		c.Server.MaxBodyBytes = 8 << 20
	}
	if c.Database.MaxConns <= 0 {
		c.Database.MaxConns = 10
	}
	if c.Auth.KeyCacheTTLSeconds <= 0 {
		c.Auth.KeyCacheTTLSeconds = 30
	}
	if c.Auth.DefaultRateLimitPerMinute <= 0 {
		c.Auth.DefaultRateLimitPerMinute = 60
	}

	// 会话录制：只兜底内部参数，开关与上限由使用者显式决定（会话录制）
	if c.SessionRecording.MaxPayloadBytes <= 0 {
		c.SessionRecording.MaxPayloadBytes = 1 << 20
	}
	if c.SessionRecording.BatchSize <= 0 {
		c.SessionRecording.BatchSize = 200
	}
	if c.SessionRecording.FlushInterval <= 0 {
		c.SessionRecording.FlushInterval = 2
	}

	// 服务发现（服务发现）
	if c.Discovery.Provider == "" {
		c.Discovery.Provider = "static"
	}
	c.Discovery.Provider = strings.ToLower(strings.TrimSpace(c.Discovery.Provider))
	if c.Discovery.CacheTTLSeconds <= 0 {
		c.Discovery.CacheTTLSeconds = 10
	}
	if c.Discovery.Rnacos.Addr == "" {
		c.Discovery.Rnacos.Addr = "http://127.0.0.1:8848"
	}
	c.Discovery.Rnacos.Addr = strings.TrimSuffix(c.Discovery.Rnacos.Addr, "/")
	if c.Discovery.Rnacos.Namespace == "" {
		c.Discovery.Rnacos.Namespace = "public"
	}
	if c.Discovery.Rnacos.Group == "" {
		c.Discovery.Rnacos.Group = "DEFAULT_GROUP"
	}
	if c.Discovery.Rnacos.TimeoutSeconds <= 0 {
		c.Discovery.Rnacos.TimeoutSeconds = 5
	}

	// 注册中心同步（注册中心同步）
	//
	// ServicePrefix 刻意不在这里兜底成 "mcp-"：Default() 已经给了默认值，
	// 而「使用者显式清空」与「使用者没写」在 yaml 反序列化后是同一个零值，
	// 无法区分。所以只需保证路径形态合法，前缀留给他自己决定。
	if c.Registry.SyncIntervalSeconds <= 0 {
		c.Registry.SyncIntervalSeconds = 60
	}
	if c.Registry.EndpointPath == "" {
		c.Registry.EndpointPath = "/mcp"
	}
	if !strings.HasPrefix(c.Registry.EndpointPath, "/") {
		c.Registry.EndpointPath = "/" + c.Registry.EndpointPath
	}
	if c.Registry.TimeoutMS <= 0 {
		c.Registry.TimeoutMS = 120000
	}
	if c.Registry.PoolSize <= 0 {
		c.Registry.PoolSize = 1
	}

	// 后台维护任务（计量计费 / 租户与分片 §9.4）
	if c.Maintenance.ReservationSweepSeconds <= 0 {
		c.Maintenance.ReservationSweepSeconds = 60
	}
	if c.Maintenance.TenantExpirySweepSeconds <= 0 {
		c.Maintenance.TenantExpirySweepSeconds = 60
	}
	if c.Maintenance.BatchSize <= 0 {
		c.Maintenance.BatchSize = 200
	}
	// 单轮上限收敛：ExpireStaleReservations 内部对 >500 会静默改写成 100，
	// 与其让配置与行为不一致，不如在加载期就把口径统一到 500。
	if c.Maintenance.BatchSize > 500 {
		c.Maintenance.BatchSize = 500
	}

	for i := range c.Namespaces {
		n := &c.Namespaces[i]
		n.Slug = strings.ToLower(strings.TrimSpace(n.Slug))
		if n.PoolSize <= 0 {
			n.PoolSize = 1
		}
		if n.TimeoutMS <= 0 {
			n.TimeoutMS = 120000
		}
		if n.Transport == "" {
			// 未显式声明时按旧行为当作 stdio，保证向后兼容
			n.Transport = "stdio"
		}
		n.Transport = strings.ToLower(strings.TrimSpace(n.Transport))
		if n.Transport == "http" {
			if n.EndpointPath == "" {
				n.EndpointPath = "/mcp"
			}
			if !strings.HasPrefix(n.EndpointPath, "/") {
				n.EndpointPath = "/" + n.EndpointPath
			}
			if n.LBStrategy == "" {
				n.LBStrategy = "round_robin"
			}
			n.LBStrategy = strings.ToLower(strings.TrimSpace(n.LBStrategy))
			if n.DiscoveryProvider == "" {
				n.DiscoveryProvider = c.Discovery.Provider
			}
			n.DiscoveryProvider = strings.ToLower(strings.TrimSpace(n.DiscoveryProvider))
			if n.DiscoveryGroup == "" {
				n.DiscoveryGroup = c.Discovery.Rnacos.Group
			}
		}
	}
}

// Mode 根据配置推导运行模式。
func (c *Config) Mode() Mode {
	switch {
	case c.Database.DSN != "":
		return ModeMultiTenant
	case c.Auth.BearerToken != "":
		return ModeLegacy
	default:
		return ModeDevelopment
	}
}

// Validate 检查当前模式下的必填项。返回的错误信息直接面向使用者。
func (c *Config) Validate() error {
	switch c.Mode() {
	case ModeMultiTenant:
		if c.Auth.AdminToken == "" {
			return errors.New("多租户模式需要设置 auth.admin_token，否则 /admin/v1/* 无人可管")
		}
		if len(c.Auth.AdminToken) < 16 {
			return errors.New("auth.admin_token 长度不足 16 位，存在被穷举风险")
		}
	case ModeLegacy:
		if c.Auth.BearerToken == "ceshi" {
			return errors.New("auth.bearer_token 仍是默认弱口令 ceshi，请更换")
		}
		if len(c.Auth.BearerToken) < 16 {
			return errors.New("auth.bearer_token 长度不足 16 位，存在被穷举风险")
		}
	}

	// 分片 DSN（租户与分片）：分片号合法性交给 shard.Parse 在启动时统一校验，
	// 这里只拦「写了分片但 DSN 为空」——那种配置会让 PoolSet 建出一个
	// 连不上的池，直到第一个请求打到该分片才炸。
	for rawShard, dsn := range c.Database.ShardDSNs {
		if strings.TrimSpace(dsn) == "" {
			return fmt.Errorf("database.shard_dsns[%q] 的 DSN 为空；要么写全，要么删掉这一条", rawShard)
		}
	}

	// 服务发现（服务发现）：provider 白名单 + fail_open 的兜底必须存在
	switch c.Discovery.Provider {
	case "static", "rnacos":
	case "":
		return errors.New("discovery.provider 不能为空，可选 static 或 rnacos")
	default:
		return fmt.Errorf("discovery.provider 取值非法: %q，可选 static 或 rnacos", c.Discovery.Provider)
	}
	if c.Discovery.Provider == "rnacos" && c.Discovery.FailOpen && len(c.Discovery.StaticEndpoints) == 0 {
		return errors.New("discovery.provider=rnacos 且 fail_open=true 时必须配置 discovery.static_endpoints，" +
			"否则 r-nacos 不可用时网关无处转发（见服务发现）")
	}

	// 注册中心同步（注册中心同步）
	//
	// 只有 rnacos 提供「枚举服务」能力：static 只是一组写死的地址，
	// 没有「注册中心里有哪些服务」这个概念。开着 auto_discover 却选了 static，
	// 同步器每轮都会返回 ErrNotSupported，日志刷满却什么也不做——
	// 这是配置错误，必须在启动时就拦下，而不是让它安静地空转。
	if c.Registry.AutoDiscover && c.Discovery.Provider != "rnacos" {
		return fmt.Errorf("registry.auto_discover=true 要求 discovery.provider=rnacos，"+
			"当前为 %q（注册中心同步）", c.Discovery.Provider)
	}

	// 命名空间（服务发现的 transport 二分）
	seen := make(map[string]struct{}, len(c.Namespaces))
	for i := range c.Namespaces {
		n := &c.Namespaces[i]
		if n.Slug == "" {
			return fmt.Errorf("namespaces[%d].slug 不能为空", i)
		}
		if _, dup := seen[n.Slug]; dup {
			return fmt.Errorf("命名空间 slug 重复: %q", n.Slug)
		}
		seen[n.Slug] = struct{}{}

		switch n.Transport {
		case "stdio":
			if n.Command == "" {
				return fmt.Errorf("命名空间 %q 的 transport=stdio，必须配置 command", n.Slug)
			}
			if n.DiscoveryProvider != "" || n.DiscoveryService != "" {
				return fmt.Errorf("命名空间 %q 的 transport=stdio，不允许配置 discovery_* 字段", n.Slug)
			}
		case "http":
			if n.DiscoveryService == "" {
				return fmt.Errorf("命名空间 %q 的 transport=http，必须配置 discovery_service", n.Slug)
			}
			switch n.DiscoveryProvider {
			case "rnacos", "static":
			default:
				return fmt.Errorf("命名空间 %q 的 discovery_provider 非法: %q，可选 rnacos 或 static",
					n.Slug, n.DiscoveryProvider)
			}
			if n.LBStrategy != "round_robin" {
				return fmt.Errorf("命名空间 %q 的 lb_strategy 暂只支持 round_robin，当前为 %q",
					n.Slug, n.LBStrategy)
			}
		default:
			return fmt.Errorf("命名空间 %q 的 transport 非法: %q，可选 stdio 或 http", n.Slug, n.Transport)
		}
	}

	// 会话录制（会话录制）：只校验取值范围，不做"必须开启"的强约束
	if c.SessionRecording.Enabled && c.SessionRecording.MaxPayloadBytes <= 0 {
		return errors.New("session_recording.max_payload_bytes 必须大于 0")
	}
	if c.SessionRecording.RetentionDays < 0 {
		return errors.New("session_recording.retention_days 不能为负数，0 表示永久保留")
	}
	return nil
}

// applyEnv 允许 MCPGW_* 覆盖配置，便于容器部署时以环境变量注入密钥。
func applyEnv(c *Config) error {
	if v, ok := os.LookupEnv("MCPGW_SERVER_NAME"); ok {
		c.Server.Name = v
	}
	if v, ok := os.LookupEnv("MCPGW_SERVER_VERSION"); ok {
		c.Server.Version = v
	}
	if v, ok := os.LookupEnv("MCPGW_SERVER_PORT"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MCPGW_SERVER_PORT 不是整数: %w", err)
		}
		c.Server.Port = n
	}
	if v, ok := os.LookupEnv("MCPGW_SERVER_BASE_URL"); ok {
		c.Server.BaseURL = v
	}
	if v, ok := os.LookupEnv("MCPGW_SERVER_SSE_PATH"); ok {
		c.Server.SSEPath = v
	}
	if v, ok := os.LookupEnv("MCPGW_SERVER_MESSAGE_PATH"); ok {
		c.Server.MessagePath = v
	}
	if v, ok := os.LookupEnv("MCPGW_DATABASE_DSN"); ok {
		c.Database.DSN = v
	}
	if v, ok := os.LookupEnv("MCPGW_DATABASE_MAX_CONNS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MCPGW_DATABASE_MAX_CONNS 不是整数: %w", err)
		}
		c.Database.MaxConns = int32(n)
	}
	if v, ok := os.LookupEnv("MCPGW_AUTH_ADMIN_TOKEN"); ok {
		c.Auth.AdminToken = v
	}
	if v, ok := os.LookupEnv("MCPGW_AUTH_BEARER_TOKEN"); ok {
		c.Auth.BearerToken = v
	}
	if v, ok := os.LookupEnv("MCPGW_AUTH_KEY_CACHE_TTL_SECONDS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MCPGW_AUTH_KEY_CACHE_TTL_SECONDS 不是整数: %w", err)
		}
		c.Auth.KeyCacheTTLSeconds = n
	}
	if v, ok := os.LookupEnv("MCPGW_IDGEN_NODE_ID"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("MCPGW_IDGEN_NODE_ID 不是整数: %w", err)
		}
		c.IDGen.NodeID = n
	}

	// 后台维护任务
	if v, ok := os.LookupEnv("MCPGW_MAINTENANCE_ENABLED"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("MCPGW_MAINTENANCE_ENABLED 不是布尔值: %w", err)
		}
		c.Maintenance.Enabled = b
	}
	if v, ok := os.LookupEnv("MCPGW_MAINTENANCE_BATCH_SIZE"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MCPGW_MAINTENANCE_BATCH_SIZE 不是整数: %w", err)
		}
		c.Maintenance.BatchSize = n
	}

	// 会话录制（会话录制）
	if v, ok := os.LookupEnv("MCPGW_SESSION_RECORDING_ENABLED"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("MCPGW_SESSION_RECORDING_ENABLED 不是布尔值: %w", err)
		}
		c.SessionRecording.Enabled = b
	}
	if v, ok := os.LookupEnv("MCPGW_SESSION_RECORDING_MAX_PAYLOAD_BYTES"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("MCPGW_SESSION_RECORDING_MAX_PAYLOAD_BYTES 不是整数: %w", err)
		}
		c.SessionRecording.MaxPayloadBytes = n
	}

	// 服务发现（服务发现）
	if v, ok := os.LookupEnv("MCPGW_DISCOVERY_PROVIDER"); ok {
		c.Discovery.Provider = v
	}
	if v, ok := os.LookupEnv("MCPGW_DISCOVERY_FAIL_OPEN"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("MCPGW_DISCOVERY_FAIL_OPEN 不是布尔值: %w", err)
		}
		c.Discovery.FailOpen = b
	}
	if v, ok := os.LookupEnv("MCPGW_DISCOVERY_RNACOS_ADDR"); ok {
		c.Discovery.Rnacos.Addr = v
	}
	if v, ok := os.LookupEnv("MCPGW_DISCOVERY_RNACOS_NAMESPACE"); ok {
		c.Discovery.Rnacos.Namespace = v
	}
	if v, ok := os.LookupEnv("MCPGW_DISCOVERY_RNACOS_USERNAME"); ok {
		c.Discovery.Rnacos.Username = v
	}
	if v, ok := os.LookupEnv("MCPGW_DISCOVERY_RNACOS_PASSWORD"); ok {
		c.Discovery.Rnacos.Password = v
	}

	// 注册中心同步（注册中心同步）
	if v, ok := os.LookupEnv("MCPGW_REGISTRY_AUTO_DISCOVER"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("MCPGW_REGISTRY_AUTO_DISCOVER 不是布尔值: %w", err)
		}
		c.Registry.AutoDiscover = b
	}
	if v, ok := os.LookupEnv("MCPGW_REGISTRY_SYNC_INTERVAL_SECONDS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MCPGW_REGISTRY_SYNC_INTERVAL_SECONDS 不是整数: %w", err)
		}
		c.Registry.SyncIntervalSeconds = n
	}
	if v, ok := os.LookupEnv("MCPGW_REGISTRY_SERVICE_PREFIX"); ok {
		c.Registry.ServicePrefix = v
	}
	if v, ok := os.LookupEnv("MCPGW_REGISTRY_ENDPOINT_PATH"); ok {
		c.Registry.EndpointPath = v
	}
	if v, ok := os.LookupEnv("MCPGW_REGISTRY_TIMEOUT_MS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MCPGW_REGISTRY_TIMEOUT_MS 不是整数: %w", err)
		}
		c.Registry.TimeoutMS = n
	}
	if v, ok := os.LookupEnv("MCPGW_REGISTRY_POOL_SIZE"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MCPGW_REGISTRY_POOL_SIZE 不是整数: %w", err)
		}
		c.Registry.PoolSize = n
	}
	return nil
}

// Endpoint 返回带 sessionId 的完整消息端点，供 SSE 的 endpoint 事件使用。
func (c *Config) Endpoint(sessionID string) string {
	return fmt.Sprintf("%s%s%s?sessionId=%s", c.Server.BaseURL, c.Server.MessagePath, "", sessionID)
}
