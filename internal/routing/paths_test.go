package routing

import "testing"

func TestParsePath(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		want   Endpoint
		slug   string
		scoped bool
	}{
		{name: "无前缀建连", path: "/sse", want: EndpointSSE},
		{name: "带前缀建连", path: "/sse/blender", want: EndpointSSE, slug: "blender", scoped: true},
		{name: "无前缀投递", path: "/messages", want: EndpointMessage},
		{name: "带前缀投递", path: "/messages/blender", want: EndpointMessage, slug: "blender", scoped: true},
		{name: "大写归一", path: "/sse/Blender", want: EndpointSSE, slug: "blender", scoped: true},
		{name: "尾部斜杠不认", path: "/sse/blender/", want: EndpointNone},
		{name: "多段不认", path: "/sse/blender/sub", want: EndpointNone},
		{name: "空段不认", path: "/sse/", want: EndpointNone},
		// r.URL.Path 已经是解码后的结果，所以这里直接用空格而不是 %20。
		{name: "纯空白段不认", path: "/sse/ ", want: EndpointNone},
		{name: "无关路径", path: "/admin/v1/keys", want: EndpointNone},
		{name: "前缀相似但不匹配", path: "/ssefoo", want: EndpointNone},
		{name: "根路径", path: "/", want: EndpointNone},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParsePath("/sse", "/messages", tc.path)
			if got.Endpoint != tc.want {
				t.Fatalf("Endpoint = %v，期望 %v", got.Endpoint, tc.want)
			}
			if got.Slug != tc.slug {
				t.Fatalf("Slug = %q，期望 %q", got.Slug, tc.slug)
			}
			if got.Scoped != tc.scoped {
				t.Fatalf("Scoped = %v，期望 %v", got.Scoped, tc.scoped)
			}
		})
	}
}

func TestParsePathCustomBase(t *testing.T) {
	got := ParsePath("/mcp", "/rpc", "/rpc/ns1")
	if got.Endpoint != EndpointMessage || got.Slug != "ns1" {
		t.Fatalf("自定义端点路径解析失败: %+v", got)
	}
	// 旧前缀在新配置下必须彻底失效，否则改配置等于没改。
	if ParsePath("/mcp", "/rpc", "/sse/x").Endpoint != EndpointNone {
		t.Fatal("旧前缀不应被识别")
	}
}

func TestParsePathBaseWithTrailingSlash(t *testing.T) {
	got := ParsePath("/sse/", "/messages/", "/sse/a")
	if got.Endpoint != EndpointSSE || got.Slug != "a" {
		t.Fatalf("端点路径带尾斜杠时解析失败: %+v", got)
	}
}

func TestEndpointString(t *testing.T) {
	cases := map[Endpoint]string{
		EndpointNone:    "none",
		EndpointSSE:     "sse",
		EndpointMessage: "message",
	}
	for ep, want := range cases {
		if ep.String() != want {
			t.Fatalf("%d.String() = %q，期望 %q", ep, ep.String(), want)
		}
	}
}
