package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/config"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// maintenance 是进程内的后台维护循环：预扣超时释放 + 企业租户到期降级。
//
// # 为什么是两个独立 ticker 而不是一个
//
// 两件事失败的后果完全不同：
//
//	预扣释放失败  → 用户的额度被永久冻结（钱扣了、没写账），属于资金问题
//	到期降级失败  → 只是晚一点降级，期间额度仍然按企业档给，属于资损问题
//
// 一个 ticker 里串行执行的话，租户表一慢就会把预扣释放一起拖住；
// 拆开后各自按自己的节奏推进，互不拖累。
//
// # 为什么不在 system_settings 里配间隔
//
// 动态配置规定「业务可变项」由超级管理员动态管理，但扫描频率不是业务策略，
// 而是跟部署形态、数据库规格绑在一起的运维参数：改它需要的是重启，
// 不是打开管理端。所以放在 config.yaml 的 maintenance 段，
// 而「预扣多久算超时」（billing.reservation_ttl_seconds）仍然是动态配置。
type maintenance struct {
	st        *store.Store
	cfg       config.MaintenanceConfig
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// startMaintenance 启动后台维护循环。cfg.Enabled=false 时什么都不起。
//
// 返回的对象必须 Close()，否则 goroutine 会活到进程退出才被强行带走；
// 正常退出路径上先 Close 再关连接池，避免出现「池已关、后台任务还在查询」。
func startMaintenance(parent context.Context, st *store.Store, cfg config.MaintenanceConfig) *maintenance {
	m := &maintenance{st: st, cfg: cfg}
	if st == nil || !cfg.Enabled {
		log.Printf("[maintenance] 未启用：租户到期不翻状态、预扣超时不释放")
		return m
	}

	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel

	reservationEvery := time.Duration(cfg.ReservationSweepSeconds) * time.Second
	expiryEvery := time.Duration(cfg.TenantExpirySweepSeconds) * time.Second

	m.wg.Add(2)
	go m.loop(ctx, "预扣超时释放", reservationEvery, m.sweepReservations)
	go m.loop(ctx, "企业租户到期降级", expiryEvery, m.sweepTenantExpiry)

	log.Printf("[maintenance] 已启动：预扣扫描 %s，租户到期扫描 %s，单轮上限 %d 条",
		reservationEvery, expiryEvery, cfg.BatchSize)
	return m
}

// loop 是通用的定时执行骨架。
//
// 刻意不在启动时立刻跑一轮：网关刚起来时数据库可能还在承接迁移后的
// 首批流量，而这两件事都是「晚一分钟没影响」的后台工作，
// 让它们错开启动尖峰反而更稳。
func (m *maintenance) loop(ctx context.Context, name string, every time.Duration, job func(context.Context) (int, error)) {
	defer m.wg.Done()

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[maintenance] %s 已停止", name)
			return
		case <-ticker.C:
			n, err := job(ctx)
			if err != nil {
				// 后台任务失败不能拖垮网关：记录后等下一个 tick 重试。
				// 积压不会丢——数据都还在库里躺着，下一轮照样能被扫到。
				if ctx.Err() == nil {
					log.Printf("[maintenance] %s 失败（下一轮重试）: %v", name, err)
				}
				continue
			}
			if n > 0 {
				log.Printf("[maintenance] %s：本轮处理 %d 条", name, n)
			}
		}
	}
}

// sweepReservations 释放超过 TTL 仍未结算的预扣（计量计费）。
func (m *maintenance) sweepReservations(ctx context.Context) (int, error) {
	return m.st.ExpireStaleReservations(ctx, m.cfg.BatchSize)
}

// sweepTenantExpiry 把到期企业租户降级（租户与分片 §9.4）。只翻状态，不禁用 Key。
func (m *maintenance) sweepTenantExpiry(ctx context.Context) (int, error) {
	ids, err := m.st.SweepTenantExpiry(ctx, time.Now(), m.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		// 逐个打印而不是汇总成一句：降级是有后果的动作，
		// 出问题时运维需要知道「到底是哪个租户在几点被降级」。
		log.Printf("[maintenance] 租户到期降级：%s", id)
	}
	return len(ids), nil
}

// Close 停止后台循环并等待其退出。可重复调用。
func (m *maintenance) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
		m.wg.Wait()
	})
}
