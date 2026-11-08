package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRegistryDefaults 钉住 registry 段的默认值来源。
// 这些值决定「自动建出来的命名空间长什么样」，改错不会报错，
// 只会让所有新接入的服务以错误的超时/池大小跑起来。
func TestRegistryDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Registry.AutoDiscover {
		t.Fatal("registry.auto_discover 默认必须是 false：同步器会写库，不能默认开")
	}
	if cfg.Registry.SyncIntervalSeconds != 60 {
		t.Fatalf("同步周期默认值错误：%d", cfg.Registry.SyncIntervalSeconds)
	}
	if cfg.Registry.ServicePrefix != "mcp-" {
		t.Fatalf("服务前缀默认值错误：%q", cfg.Registry.ServicePrefix)
	}
	if cfg.Registry.EndpointPath != "/mcp" {
		t.Fatalf("端点路径默认值错误：%q", cfg.Registry.EndpointPath)
	}
	if cfg.Registry.TimeoutMS != 120000 {
		t.Fatalf("超时默认值错误：%d", cfg.Registry.TimeoutMS)
	}
	if cfg.Registry.PoolSize != 1 {
		t.Fatalf("池大小默认值错误：%d", cfg.Registry.PoolSize)
	}
}

// TestRegistryNormalize 验证零值被收敛成可用的形态。
// 这里不兜底 ServicePrefix：yaml 里「没写」与「显式写空串」反序列化后
// 是同一个零值，无法区分，硬兜底会把使用者「全都要」的决定悄悄改掉。
func TestRegistryNormalize(t *testing.T) {
	cfg := Default()
	cfg.Registry = RegistryConfig{} // 全部归零
	cfg.normalize()

	if cfg.Registry.SyncIntervalSeconds != 60 {
		t.Fatalf("周期未收敛：%d", cfg.Registry.SyncIntervalSeconds)
	}
	if cfg.Registry.EndpointPath != "/mcp" {
		t.Fatalf("端点路径未收敛：%q", cfg.Registry.EndpointPath)
	}
	if cfg.Registry.TimeoutMS != 120000 || cfg.Registry.PoolSize != 1 {
		t.Fatalf("模板默认值未收敛：%+v", cfg.Registry)
	}
	if cfg.Registry.ServicePrefix != "" {
		t.Fatalf("空前缀被兜底成 %q，这会让「全都要」被悄悄改成有过滤", cfg.Registry.ServicePrefix)
	}
}

func TestRegistryEndpointPathKeepsLeadingSlash(t *testing.T) {
	cfg := Default()
	cfg.Registry.EndpointPath = "rpc"
	cfg.normalize()
	if cfg.Registry.EndpointPath != "/rpc" {
		t.Fatalf("端点路径未补前导斜杠：%q", cfg.Registry.EndpointPath)
	}
}

// TestRegistryValidateRequiresRnacos 是最重要的一条配置校验：
// 开着 auto_discover 却用 static，同步器每轮返回 ErrNotSupported，
// 日志刷满却什么也不做。必须启动就拒绝，不能让它安静空转。
func TestRegistryValidateRequiresRnacos(t *testing.T) {
	cfg := Default()
	cfg.Auth.AdminToken = "0123456789abcdef-admin"
	cfg.Database.DSN = "postgres://x/y"
	cfg.Discovery.Provider = "static"
	cfg.Discovery.FailOpen = true
	cfg.Discovery.StaticEndpoints = []string{"127.0.0.1:8080"}
	cfg.Registry.AutoDiscover = true

	err := cfg.Validate()
	if err == nil {
		t.Fatal("auto_discover=true 配 static provider 必须被拒绝")
	}
	if !strings.Contains(err.Error(), "rnacos") {
		t.Fatalf("错误信息应点明需要 rnacos，实际：%v", err)
	}
}

func TestRegistryValidateAllowsRnacos(t *testing.T) {
	cfg := Default()
	cfg.Auth.AdminToken = "0123456789abcdef-admin"
	cfg.Database.DSN = "postgres://x/y"
	cfg.Discovery.Provider = "rnacos"
	cfg.Discovery.FailOpen = false
	cfg.Registry.AutoDiscover = true

	if err := cfg.Validate(); err != nil {
		t.Fatalf("rnacos + auto_discover 应当通过校验，得到：%v", err)
	}
}

func TestRegistryValidateOfflineByDefault(t *testing.T) {
	cfg := Default()
	cfg.Auth.AdminToken = "0123456789abcdef-admin"
	cfg.Database.DSN = "postgres://x/y"
	cfg.Discovery.Provider = "static"
	cfg.Discovery.StaticEndpoints = []string{"127.0.0.1:8080"}

	// auto_discover 保持默认 false，应与 static 组合合法
	if err := cfg.Validate(); err != nil {
		t.Fatalf("auto_discover=false 时 static 组合应当合法：%v", err)
	}
}

func TestRegistryEnvOverride(t *testing.T) {
	t.Setenv("MCPGW_REGISTRY_AUTO_DISCOVER", "true")
	t.Setenv("MCPGW_REGISTRY_SYNC_INTERVAL_SECONDS", "15")
	t.Setenv("MCPGW_REGISTRY_SERVICE_PREFIX", "svc-")
	t.Setenv("MCPGW_REGISTRY_TIMEOUT_MS", "3000")
	t.Setenv("MCPGW_REGISTRY_POOL_SIZE", "4")
	t.Setenv("MCPGW_REGISTRY_ENDPOINT_PATH", "rpc")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load 失败：%v", err)
	}
	r := cfg.Registry
	if !r.AutoDiscover || r.SyncIntervalSeconds != 15 || r.ServicePrefix != "svc-" ||
		r.TimeoutMS != 3000 || r.PoolSize != 4 || r.EndpointPath != "/rpc" {
		t.Fatalf("环境变量覆盖未生效：%+v", r)
	}
}

// TestExampleConfigParses 加载仓库里的样例文件，确保「文档里写的配置」真的能用。
// 样例是使用者实际复制的来源，它一旦与代码里的 yaml tag 脱节，
// 报错会出现在别人第一次部署的时候。
func TestExampleConfigParses(t *testing.T) {
	path := filepath.Join("..", "..", "config.yaml.example")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("样例配置不存在，跳过：%v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("样例配置解析失败：%v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("样例配置校验失败：%v", err)
	}
	if len(cfg.Namespaces) == 0 {
		t.Fatal("样例配置里应当至少保留一个 stdio 命名空间")
	}
	for _, n := range cfg.Namespaces {
		if n.Transport == "http" {
			t.Fatalf("样例配置里不应再出现写死的 http 命名空间（%s）："+
				"http 服务应当由 registry.auto_discover 自动接入", n.Slug)
		}
	}
}
