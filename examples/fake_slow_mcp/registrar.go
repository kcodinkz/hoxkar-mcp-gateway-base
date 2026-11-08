package main

// r-nacos 注册与心跳（Nacos OpenAPI v1）。
//
// 为什么替身要自己注册，而不是由网关代劳：服务发现定死了方向——
// 注册者是后端服务，网关只读不写，永远不出现在注册表里。替身就是「后端」，
// 所以它必须自己走一遍注册 + 心跳，否则测不到网关的发现链路。
//
// 用到的三个接口（见 docs/reference/rnacos/rnacos-openapi-v1.md）：
//
//	POST   /nacos/v1/ns/instance        注册 / 更新实例
//	PUT    /nacos/v1/ns/instance/beat   临时实例心跳
//	DELETE /nacos/v1/ns/instance        注销
//
// 只实现「临时实例（ephemeral=true）」这一种：临时实例在心跳断掉后会被
// 注册中心自动剔除，正好用来验证网关的「后端消失 → 命名空间软下线」。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// registrarConfig 是注册所需的全部参数。
type registrarConfig struct {
	registryAddr string            // 例如 http://127.0.0.1:8848
	namespace    string            // 命名空间 ID，空串表示 public
	group        string            // 分组，默认 DEFAULT_GROUP
	service      string            // 服务名（不含分组前缀）
	ip           string            // 对外通告 IP
	port         int               // 对外通告端口
	metadata     map[string]string // 实例元数据
}

// registrar 负责注册、心跳、注销三件事。
type registrar struct {
	cfg    registrarConfig
	client *http.Client
	// serviceKey 是 Nacos 要求的 "分组@@服务名" 形式。
	// 注册、心跳、注销三处必须完全一致，否则会出现「注册成功但心跳打空」。
	serviceKey string
}

func newRegistrar(cfg registrarConfig) *registrar {
	if cfg.namespace == "" {
		cfg.namespace = "public"
	}
	if cfg.group == "" {
		cfg.group = "DEFAULT_GROUP"
	}
	return &registrar{
		cfg:        cfg,
		serviceKey: cfg.group + "@@" + cfg.service,
		client:     &http.Client{Timeout: 5 * time.Second},
	}
}

// run 先注册，然后按 interval 持续心跳，直到 ctx 结束。
//
// 注册失败不退出进程：替身是测试工具，能起来就比直接崩掉有用，
// 日志里说清楚「注册失败」比让使用者误以为整个后端坏了更好。
func (r *registrar) run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	if err := r.register(ctx); err != nil {
		log.Printf("注册到 r-nacos 失败（后端仍会提供服务）: %v", err)
	} else {
		log.Printf("已注册到 r-nacos：%s %s:%d", r.serviceKey, r.cfg.ip, r.cfg.port)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.beat(ctx); err != nil {
				log.Printf("心跳失败: %v", err)
				// 心跳失败常见于注册中心重启，重新注册一次就能恢复。
				if err := r.register(ctx); err != nil {
					log.Printf("重新注册失败: %v", err)
				} else {
					log.Printf("已重新注册：%s", r.serviceKey)
				}
			}
		}
	}
}

func (r *registrar) register(ctx context.Context) error {
	form := url.Values{}
	form.Set("ip", r.cfg.ip)
	form.Set("port", strconv.Itoa(r.cfg.port))
	form.Set("serviceName", r.serviceKey)
	form.Set("namespaceId", r.cfg.namespace)
	form.Set("groupName", r.cfg.group)
	form.Set("weight", "1.0")
	form.Set("enabled", "true")
	form.Set("ephemeral", "true")
	if len(r.cfg.metadata) > 0 {
		raw, err := json.Marshal(r.cfg.metadata)
		if err != nil {
			return fmt.Errorf("序列化 metadata 失败: %w", err)
		}
		form.Set("metadata", string(raw))
	}

	return r.do(ctx, http.MethodPost, "/nacos/v1/ns/instance", form)
}

func (r *registrar) beat(ctx context.Context) error {
	form := url.Values{}
	form.Set("serviceName", r.serviceKey)
	form.Set("namespaceId", r.cfg.namespace)
	form.Set("groupName", r.cfg.group)
	form.Set("ephemeral", "true")
	form.Set("ip", r.cfg.ip)
	form.Set("port", strconv.Itoa(r.cfg.port))

	return r.do(ctx, http.MethodPut, "/nacos/v1/ns/instance/beat", form)
}

// deregister 在进程退出时把自己摘掉。
// 失败只记日志：临时实例最终会被注册中心按心跳超时剔除，不必因此阻塞退出。
func (r *registrar) deregister(ctx context.Context) {
	form := url.Values{}
	form.Set("ip", r.cfg.ip)
	form.Set("port", strconv.Itoa(r.cfg.port))
	form.Set("serviceName", r.serviceKey)
	form.Set("namespaceId", r.cfg.namespace)
	form.Set("groupName", r.cfg.group)
	form.Set("ephemeral", "true")

	if err := r.do(ctx, http.MethodDelete, "/nacos/v1/ns/instance", form); err != nil {
		log.Printf("从 r-nacos 注销失败: %v", err)
		return
	}
	log.Printf("已从 r-nacos 注销：%s", r.serviceKey)
}

// do 是所有请求的统一出口。
//
// 参数一律走 query string 而不是 form body：r-nacos 两种都接受，
// 但 DELETE 带 body 在某些中间件下会被丢弃，统一成 query 少一类坑。
func (r *registrar) do(ctx context.Context, method, path string, form url.Values) error {
	endpoint := strings.TrimSuffix(r.cfg.registryAddr, "/") + path + "?" + form.Encode()

	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	text := strings.TrimSpace(string(body))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s 返回 HTTP %d: %s", method, path, resp.StatusCode, text)
	}
	// Nacos OpenAPI 用 200 + 文本 "ok" 表示成功；其它文本一律当失败，
	// 否则「注册被拒」会被静默吞掉，联调时表现为「实例一直不出现」。
	if text != "" && !strings.EqualFold(text, "ok") {
		return fmt.Errorf("%s %s 被拒绝: %s", method, path, text)
	}
	return nil
}
