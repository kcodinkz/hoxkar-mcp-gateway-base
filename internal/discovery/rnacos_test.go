package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 这组测试锁死一个真实踩过的坑：
//
// r-nacos 在「实例全部注销」后并不删除服务，只是留一个空壳。
// 早先 List 走 /v1/ns/service/list，那份响应只有服务名，
// 于是同步器永远观察不到「服务消失」，注册中心同步的软下线从未触发过。
// 现在 List 走 /v1/ns/catalog/services，靠 ipCount / healthyInstanceCount
// 判断服务是否还活着。下面每个用例对应这条判据的一个分支。

// newCatalogServer 起一个只实现 catalog 接口的假 r-nacos。
// 断言请求确实打在 catalog 上、且带上正确的 namespace 与分组参数。
func newCatalogServer(t *testing.T, body string, wantStatus int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nacos/v1/ns/catalog/services" {
			t.Errorf("List 必须走 catalog 接口，实际请求 %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		q := r.URL.Query()
		if got := q.Get("namespaceId"); got != "public" {
			t.Errorf("namespaceId 应为 public，实际 %q", got)
		}
		// 刻意断言 groupNameParam 而不是 groupName：
		// catalog 接口用的是 groupNameParam，写错参数名会被服务端忽略，
		// 表现为「跨分组服务混进本网关」，而错误只会在数据里慢慢显现。
		if got := q.Get("groupNameParam"); got != "DEFAULT_GROUP" {
			t.Errorf("groupNameParam 应为 DEFAULT_GROUP，实际 %q", got)
		}
		w.WriteHeader(wantStatus)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestListFiltersServicesWithoutLiveInstances(t *testing.T) {
	// mcp-alive：有健康实例 → 应保留
	// mcp-gone：  ipCount=0、healthy=0 → 空壳，应丢弃（这就是软下线的触发条件）
	// mcp-unhealthy：实例在但全不健康，healthyOnly=true 时应丢弃
	// mcp-other：别的分组，不归本网关管
	body := `{"count":4,"serviceList":[
		{"name":"mcp-alive","groupName":"DEFAULT_GROUP","ipCount":2,"healthyInstanceCount":2},
		{"name":"mcp-gone","groupName":"DEFAULT_GROUP","ipCount":0,"healthyInstanceCount":0},
		{"name":"mcp-unhealthy","groupName":"DEFAULT_GROUP","ipCount":1,"healthyInstanceCount":0},
		{"name":"mcp-other","groupName":"OTHER_GROUP","ipCount":1,"healthyInstanceCount":1}
	]}`
	srv := newCatalogServer(t, body, http.StatusOK)

	r := NewRnacos(srv.URL, "public", "DEFAULT_GROUP", "", "", 0, true)
	got, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List 不应报错: %v", err)
	}
	if len(got) != 1 || got[0] != "mcp-alive" {
		t.Fatalf("只应返回 mcp-alive，实际 %v", got)
	}
}

// healthyOnly=false 时口径要放宽到 ipCount：与 Resolve 保持一致。
// 否则会出现「Resolve 能拿到实例、List 却认为服务已死」的自相矛盾，
// 导致命名空间被反复软下线又恢复。
func TestListWithoutHealthyOnlyAcceptsAnyInstance(t *testing.T) {
	body := `{"count":2,"serviceList":[
		{"name":"mcp-degraded","groupName":"DEFAULT_GROUP","ipCount":1,"healthyInstanceCount":0},
		{"name":"mcp-gone","groupName":"DEFAULT_GROUP","ipCount":0,"healthyInstanceCount":0}
	]}`
	srv := newCatalogServer(t, body, http.StatusOK)

	r := NewRnacos(srv.URL, "public", "DEFAULT_GROUP", "", "", 0, false)
	got, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List 不应报错: %v", err)
	}
	if len(got) != 1 || got[0] != "mcp-degraded" {
		t.Fatalf("healthyOnly=false 时应保留有实例但未健康的服务，实际 %v", got)
	}
}

// 结果必须有序：同步器按 slug 排序落库，枚举侧先排好可以让两次运行的日志逐行对比。
func TestListReturnsSortedNames(t *testing.T) {
	body := `{"count":3,"serviceList":[
		{"name":"mcp-zeta","groupName":"DEFAULT_GROUP","ipCount":1,"healthyInstanceCount":1},
		{"name":"mcp-alpha","groupName":"DEFAULT_GROUP","ipCount":1,"healthyInstanceCount":1},
		{"name":"mcp-mid","groupName":"DEFAULT_GROUP","ipCount":1,"healthyInstanceCount":1}
	]}`
	srv := newCatalogServer(t, body, http.StatusOK)

	r := NewRnacos(srv.URL, "public", "DEFAULT_GROUP", "", "", 0, true)
	got, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List 不应报错: %v", err)
	}
	if strings.Join(got, ",") != "mcp-alpha,mcp-mid,mcp-zeta" {
		t.Fatalf("应升序返回，实际 %v", got)
	}
}

// 超过单页上限必须报错而不是静默截断：
// 被截掉的服务既不会被发现，还会因为「不在清单里」被误判成下线。
func TestListRejectsOverPageLimit(t *testing.T) {
	body := `{"count":9999,"serviceList":[
		{"name":"mcp-a","groupName":"DEFAULT_GROUP","ipCount":1,"healthyInstanceCount":1}
	]}`
	srv := newCatalogServer(t, body, http.StatusOK)

	r := NewRnacos(srv.URL, "public", "DEFAULT_GROUP", "", "", 0, true)
	if _, err := r.List(context.Background()); err == nil {
		t.Fatal("count 超过单页上限时应报错")
	} else if !strings.Contains(err.Error(), "超过单页上限") {
		t.Fatalf("报错应说明是单页上限问题，实际: %v", err)
	}
}

// catalog 接口不存在时要给出可定位的报错，而不是一句笼统的「返回 404」。
// 部署到老版本 Nacos（无 catalog）时，运维需要立刻知道该升级还是该换接口。
func TestListReportsMissingCatalogEndpoint(t *testing.T) {
	srv := newCatalogServer(t, "not found", http.StatusNotFound)

	r := NewRnacos(srv.URL, "public", "DEFAULT_GROUP", "", "", 0, true)
	_, err := r.List(context.Background())
	if err == nil {
		t.Fatal("catalog 返回 404 时应报错")
	}
	if !strings.Contains(err.Error(), "catalog") {
		t.Fatalf("报错应指明 catalog 接口，实际: %v", err)
	}
}

// 空的 catalog 响应是合法状态：注册中心里一个服务都没有。
// 此时必须返回空切片且不报错——同步器会据此把在册的 registry 行全部软下线。
func TestListEmptyCatalogIsNotAnError(t *testing.T) {
	srv := newCatalogServer(t, `{"count":0,"serviceList":[]}`, http.StatusOK)

	r := NewRnacos(srv.URL, "public", "DEFAULT_GROUP", "", "", 0, true)
	got, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("空目录不应报错: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("空目录应返回空切片，实际 %v", got)
	}
}
