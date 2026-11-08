package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/config"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/discovery"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/registry"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// registrySync 是进程内的「注册中心 → 命名空间表」同步循环（注册中心同步）。
//
// 与 maintenance 的关键差别只有一处，但足以决定两者的结构不同：
// maintenance 的两个任务都是「晚一分钟没影响」的后台工作，所以刻意不在
// 启动时跑首轮；而命名空间同步晚一分钟，就意味着接入方注册好的服务在这一分钟里
// 完全不可达——网关连它存在都不知道，请求只会 404。因此这里必须**立刻**跑首轮。
type registrySync struct {
	syncer   *registry.Synchronizer
	interval time.Duration
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	closeOnce sync.Once

	// mu 串行化「定时同步」与「手工触发」。
	//
	// 两者并发跑时，各自都会先 List 再逐行比对：两边同时看到「库里没有 slug=X」，
	// 于是各自 INSERT 一次，一次成功、一次撞上 slug 唯一约束。
	// 结果不算错，但日志里会冒出一个无法解释的冲突计数，
	// 排查时反而要花时间去证明「这个冲突是假的」。
	mu     sync.Mutex
	last   registry.Stats
	lastAt time.Time
}

// startRegistrySync 按配置启动同步循环；不需要同步时返回 nil。
//
// 返回 nil 而不是空壳：调用方（main）只在配置开启时才需要关心它，
// 一个「什么都不做但存在」的对象会让 Close 的顺序变得没有意义。
func startRegistrySync(parent context.Context, st *store.Store, cfg config.RegistryConfig, bundle *discovery.Bundle) *registrySync {
	if !cfg.AutoDiscover {
		log.Printf("[registry] 自动同步未启用（registry.auto_discover=false），" +
			"命名空间只能由 config 自举或管理端创建")
		return nil
	}
	if st == nil || bundle == nil || bundle.Resolver == nil {
		log.Printf("[registry] 自动同步已跳过：缺少数据访问层或发现层")
		return nil
	}

	// 枚举服务是发现层的可选能力（discovery.Lister），与「解析某个服务的实例」
	// 是两件事。这里如实判断而不是靠 provider 名字猜：
	// 将来若出现新的发现层实现，只要它实现了 Lister 就能自动接上。
	lister, ok := bundle.Resolver.(discovery.Lister)
	if !ok {
		log.Printf("[registry] 当前发现层不支持枚举服务，自动同步已跳过（见注册中心同步）")
		return nil
	}

	syncer := registry.New(st, registry.Options{
		Lister:        lister,
		ServicePrefix: cfg.ServicePrefix,
		EndpointPath:  cfg.EndpointPath,
		TimeoutMS:     cfg.TimeoutMS,
		PoolSize:      cfg.PoolSize,
		Logf:          func(format string, args ...any) { log.Printf("[registry] "+format, args...) },
	})

	ctx, cancel := context.WithCancel(parent)
	rs := &registrySync{
		syncer:   syncer,
		interval: time.Duration(cfg.SyncIntervalSeconds) * time.Second,
		cancel:   cancel,
	}

	rs.wg.Add(1)
	go rs.loop(ctx)

	log.Printf("[registry] 自动同步已启动：服务前缀=%q，周期 %s，新命名空间默认 endpoint=%s timeout=%dms pool=%d",
		cfg.ServicePrefix, rs.interval, cfg.EndpointPath, cfg.TimeoutMS, cfg.PoolSize)
	return rs
}

// loop 先立即同步一轮，之后按周期推进。
func (r *registrySync) loop(ctx context.Context) {
	defer r.wg.Done()

	r.runOnce(ctx, true)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[registry] 自动同步已停止")
			return
		case <-ticker.C:
			r.runOnce(ctx, false)
		}
	}
}

// runOnce 执行一轮同步，verbose 决定是否无条件打印统计。
//
// 默认只在「有实质变化」时打一行：每 60 秒一句「发现 5，新建 0，刷新 5…」
// 一天就是 1440 行纯噪声，真正需要看的那几行会被彻底淹没。
func (r *registrySync) runOnce(ctx context.Context, verbose bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats, err := r.syncer.SyncOnce(ctx)
	r.last = stats
	r.lastAt = time.Now()

	if err != nil {
		// 进程正在退出时同步必然失败，这种错误不值得记一行。
		if ctx.Err() != nil {
			return
		}
		log.Printf("[registry] 同步失败（下一轮重试，期间不改动任何命名空间）: %v", err)
		return
	}
	if !verbose && stats.Created+stats.Restored+stats.Offlined+stats.Conflicts == 0 {
		return
	}
	log.Printf("[registry] %s", stats.String())
}

// Trigger 供管理端「立即同步一次」使用，同步执行并返回本轮统计。
//
// 不在这里做超时：调用方是 HTTP handler，超时应当由 handler 的 context 决定，
// 在这里再套一层会让「请求已取消」与「同步本身超时」混成同一个错误。
func (r *registrySync) Trigger(ctx context.Context) (registry.Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats, err := r.syncer.SyncOnce(ctx)
	r.last = stats
	r.lastAt = time.Now()
	if err != nil {
		return stats, err
	}
	log.Printf("[registry] 手工触发同步：%s", stats.String())
	return stats, nil
}

// Last 返回上一轮同步的统计与时间，供管理端展示「上次同步发生在什么时候」。
//
// 零值时间表示从未同步过——这与「同步过但什么都没变」是两种状态，
// 管理端需要能区分，否则未启用自动同步时会显示成一个看似正常的上次同步时间。
func (r *registrySync) Last() (registry.Stats, time.Time) {
	if r == nil {
		return registry.Stats{}, time.Time{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last, r.lastAt
}

// Close 停止循环并等待退出。可重复调用。
func (r *registrySync) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
	})
}
