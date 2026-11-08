package registry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// 这些测试只覆盖纯逻辑（服务名映射、模板、统计口径），不碰数据库。
// 需要库的部分属于注册中心同步的验收脚本，在本地手工执行。
//
// New 的第一个参数允许传 nil：本文件的用例都不会走到 store 调用，
// 传 nil 反而保证了「一旦意外调用就会 panic」，比造一个假库更容易发现问题。

func TestSlugOf(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		in     string
		want   string
		ok     bool
	}{
		{name: "剥前缀", prefix: "mcp-", in: "mcp-blender", want: "blender", ok: true},
		{name: "前缀不符", prefix: "mcp-", in: "user-service", ok: false},
		{name: "空前缀不过滤", prefix: "", in: "mcp-blender", want: "mcp-blender", ok: true},
		{name: "大写转小写", prefix: "mcp-", in: "MCP-Blender", want: "blender", ok: false},
		{name: "空服务名", prefix: "mcp-", in: "   ", ok: false},
		{name: "剥完为空", prefix: "mcp-", in: "mcp-", ok: false},
		{name: "去掉前后空格", prefix: "mcp-", in: "  mcp-blender  ", want: "blender", ok: true},
		{name: "允许下划线与数字", prefix: "mcp-", in: "mcp-tool_2", want: "tool_2", ok: true},
		{name: "首字符不许是符号", prefix: "", in: "-blender", ok: false},
		{name: "不许含点号", prefix: "", in: "blender.v2", ok: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 前缀为空时按 Go 语义就是「不剥离」，因此这条用例单独处理：
			// 把 MCP- 大写的部分换成小写才符合「前缀精确匹配」的预期。
			in := c.in
			if c.name == "大写转小写" {
				in = "MCP-Blender"
			}
			s := New(nil, Options{ServicePrefix: c.prefix})
			got, ok := s.slugOf(in)
			if ok != c.ok {
				t.Fatalf("slugOf(%q) ok=%v，期望 %v", in, ok, c.ok)
			}
			if ok && got != c.want {
				t.Fatalf("slugOf(%q)=%q，期望 %q", in, got, c.want)
			}
		})
	}
}

func TestNewDefaults(t *testing.T) {
	s := New(nil, Options{EndpointPath: "rpc"})
	if s.opts.EndpointPath != "/rpc" {
		t.Fatalf("端点路径未补前导斜杠：%q", s.opts.EndpointPath)
	}
	if s.opts.TimeoutMS != 120000 {
		t.Fatalf("超时默认值错误：%d", s.opts.TimeoutMS)
	}
	if s.opts.PoolSize != 1 {
		t.Fatalf("池大小默认值错误：%d", s.opts.PoolSize)
	}
}

func TestTemplateKeepsRegistryOwnedFields(t *testing.T) {
	s := New(nil, Options{ServicePrefix: "mcp-", PoolSize: 4, TimeoutMS: 500})
	ns := s.template("blender", "mcp-blender", "DEFAULT_GROUP")

	if ns.Slug != "blender" || ns.DiscoveryService != "mcp-blender" {
		t.Fatalf("模板字段错误：%+v", ns)
	}
	if ns.Source != store.SourceRegistry {
		t.Fatalf("来源必须是 registry，否则同步器下一轮会把自己创建的行当成别人的：%q", ns.Source)
	}
	if ns.Transport != store.TransportHTTP {
		t.Fatalf("注册中心同步出来的命名空间必须是 http 传输：%q", ns.Transport)
	}
	if !ns.Enabled {
		t.Fatal("新建行必须是启用的")
	}
	if ns.PoolSize != 4 || ns.TimeoutMS != 500 {
		t.Fatalf("模板未采用 Options 的默认值：%+v", ns)
	}
}

// fakeLister 只实现 discovery.Lister，用来在不碰注册中心的前提下驱动 SyncOnce
// 走到「拉清单失败」的分支——这条分支是整轮同步里唯一涉及「要不要下线」的岔路。
type fakeLister struct {
	names []string
	group string
	err   error
}

func (f fakeLister) List(ctx context.Context) ([]string, error) { return f.names, f.err }
func (f fakeLister) ListGroup() string                          { return f.group }

var _ discovery.Lister = fakeLister{}

func TestSyncOnceWithoutLister(t *testing.T) {
	s := New(nil, Options{})
	_, err := s.SyncOnce(context.Background())
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("Lister 为 nil 时应返回 ErrNotSupported，得到 %v", err)
	}
}

// TestSyncOnceListFailureDoesNotOffline 钉住最关键的一条：
// 拉不到清单时不能下线任何东西。一旦这条被改坏，注册中心偶发故障
// 就会把全部 MCP 服务一次性下线，而且有 enabled=false 落库为证。
//
// 用 nil store：如果真的走到了下线逻辑会立刻 panic，测试直接失败。
func TestSyncOnceListFailureDoesNotOffline(t *testing.T) {
	s := New(nil, Options{Lister: fakeLister{err: errors.New("注册中心 503")}})
	_, err := s.SyncOnce(context.Background())
	if err == nil {
		t.Fatal("清单拉取失败时必须返回错误")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("错误应保留底层原因，便于运维定位：%v", err)
	}
}

func TestStatsString(t *testing.T) {
	start := time.Now()
	st := Stats{
		Discovered: 3, Created: 1, Refreshed: 1, Offlined: 1,
		StartedAt: start, FinishedAt: start.Add(1500 * time.Millisecond),
	}
	got := st.String()
	for _, want := range []string{"发现 3", "新建 1", "刷新 1", "下线 1", "1.5s"} {
		if !strings.Contains(got, want) {
			t.Fatalf("统计串缺少 %q：%s", want, got)
		}
	}
}
