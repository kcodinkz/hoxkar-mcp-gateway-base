// Package billing 是计量与计费的服务层。
//
// 计量计费：
//   - 金额一律 int64 微单位，禁止浮点。
//   - 计量与计费解耦：用量事实无条件记录，是否扣费由 billable 与费率决定。
//   - 写计量/账本必须旁路异步，失败只降级不阻断 MCP 请求。
//   - 费率来自数据库，禁止硬编码。
//
// 请求路径上的用法只有三步，全部是非阻塞的：
//
//	b := billing.FromContext(ctx)   // 由中间件注入
//	resID := b.BeginToolCall(...)   // 可选：需要预扣的昂贵操作
//	b.Record(usage)                 // 永远不返回 error
//	b.Finish(resID, actualMicros)   // 结算，失败只记日志
package billing

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/settings"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// Metric 是计价维度，必须与 migrations/0006 的 CHECK 约束一致。
type Metric string

const (
	MetricRequest      Metric = "request"
	MetricInputToken   Metric = "input_token"
	MetricOutputToken  Metric = "output_token"
	MetricToolCall     Metric = "tool_call"
	MetricRenderSecond Metric = "render_second"
	MetricUploadByte   Metric = "upload_byte"
	MetricStorageByte  Metric = "storage_byte"
)

// Usage 是一次用量上报。字段刻意扁平，方便在网关各处随手填写。
type Usage struct {
	// TenantID 是数据归属的唯一边界（租户与分片）。usage_events.tenant_id 为
	// NOT NULL，因此这条计量最终能不能落库，取决于调用方是否填了它。
	TenantID      idgen.ID
	UserID        idgen.ID
	APIKeyID      idgen.ID
	SessionID     idgen.ID
	NamespaceID   idgen.ID
	NamespaceSlug string
	PlanCode      string
	RequestID     string
	Metric        Metric
	Quantity      int64
	Unit          string
	// Billable 为 false 时只计量不扣费（压测、内部调用）
	Billable bool
	// ReservationID 非空时，费用计入该预扣单而不是直接扣余额
	ReservationID idgen.ID
	Occurred      time.Time

	// Shard 是分片号。留空时由 SessionID 直接读出（形态 B 第 5~6 位），
	// 不做任何查表——这就是租户与分片的零查表路由。两者都拿不到时，
	// 这条计量会被丢弃并计入 dropped，因为没有合法分片就拼不出
	// 合法的 uev_ ID，而 ID 是路由的唯一依据，不能瞎猜。
	Shard string
}

// resolveShard 决定这条用量应该落在哪个分片。
//
// 优先级：显式 Shard > SessionID 里的分片位 > 空。
// 刻意不拿 UserID 去推断：UserID 是形态 A，不含分片位，
// 「用户 → 分片」必须经 internal/shard.Directory 回源，那是查询不是推导。
func (u Usage) resolveShard() string {
	if u.Shard != "" {
		return u.Shard
	}
	return idgen.ShardOf(u.SessionID)
}

// Service 是计费服务。零值不可用，必须经 New 构造。
type Service struct {
	st       *store.Store
	settings *settings.Manager

	queue chan store.UsageEvent
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once

	// 费率缓存：纯配置，全量常驻内存
	priceMu  sync.RWMutex
	prices   map[string][]store.PriceRule // metric -> 按优先级排序
	pricesAt time.Time

	// 统计，供 /healthz 与日志
	dropped    atomic.Int64
	written    atomic.Int64
	writeFails atomic.Int64
}

// Options 是构造参数。
type Options struct {
	// QueueSize 是旁路队列容量。满了就丢弃并计数，绝不阻塞请求。
	QueueSize int
	// BatchSize 单批写入条数
	BatchSize int
	// FlushInterval 强制 flush 间隔
	FlushInterval time.Duration
	// PriceTTL 费率缓存时间
	PriceTTL time.Duration
}

// New 构造服务并加载费率。
//
// 费率加载失败不阻断启动：计费是旁路能力，元数据缺失时可以退化为
// 「只计量不计费」，但不能让网关起不来（计量计费红线）。
func New(ctx context.Context, st *store.Store, sm *settings.Manager, opts Options) *Service {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 4096
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 200
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 2 * time.Second
	}
	if opts.PriceTTL <= 0 {
		opts.PriceTTL = 60 * time.Second
	}

	s := &Service{
		st:       st,
		settings: sm,
		queue:    make(chan store.UsageEvent, opts.QueueSize),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		prices:   make(map[string][]store.PriceRule),
	}

	if err := s.reloadPrices(ctx); err != nil {
		log.Printf("[billing] 加载费率失败，本轮退化为只计量不计费: %v", err)
	}

	go s.run(opts.BatchSize, opts.FlushInterval)
	return s
}

// ---------------------------------------------------------------------------
// 费率解析
// ---------------------------------------------------------------------------

func (s *Service) reloadPrices(ctx context.Context) error {
	rules, err := s.st.ListActivePriceRules(ctx)
	if err != nil {
		return err
	}

	grouped := make(map[string][]store.PriceRule, len(rules))
	for _, r := range rules {
		grouped[r.Metric] = append(grouped[r.Metric], r)
	}

	// 优先级：namespace+plan > namespace > plan > 全局
	for metric := range grouped {
		rules := grouped[metric]
		// 用插入排序避免额外依赖，条数是几条量级
		for i := 1; i < len(rules); i++ {
			for j := i; j > 0 && specificity(rules[j]) > specificity(rules[j-1]); j-- {
				rules[j], rules[j-1] = rules[j-1], rules[j]
			}
		}
		grouped[metric] = rules
	}

	s.priceMu.Lock()
	s.prices = grouped
	s.pricesAt = time.Now()
	s.priceMu.Unlock()
	return nil
}

// specificity 越大越优先。
func specificity(r store.PriceRule) int {
	n := 0
	if r.NamespaceSlug != "" {
		n += 2
	}
	if r.PlanCode != "" {
		n++
	}
	return n
}

// matchPrice 找出适用于本次调用的费率。返回 nil 表示该维度不计费。
func (s *Service) matchPrice(metric Metric, namespaceSlug, planCode string) *store.PriceRule {
	s.priceMu.RLock()
	rules := s.prices[string(metric)]
	s.priceMu.RUnlock()

	for i := range rules {
		r := &rules[i]
		if r.NamespaceSlug != "" && r.NamespaceSlug != namespaceSlug {
			continue
		}
		if r.PlanCode != "" && r.PlanCode != planCode {
			continue
		}
		return r
	}
	return nil
}

// CostMicros 计算费用。整数运算，四舍五入到微单位。
//
// 公式：ceil(quantity / unitSize) * unitPrice
// 用向上取整是刻意的：不足一个计费单位也按一个算，
// 与「按千 token 计价」的商业惯例一致。
func CostMicros(quantity, unitSize, unitPrice int64) int64 {
	if quantity <= 0 || unitSize <= 0 || unitPrice <= 0 {
		return 0
	}
	units := (quantity + unitSize - 1) / unitSize
	return units * unitPrice
}

// EstimateMicros 估算一次调用的费用，用于预扣。
func (s *Service) EstimateMicros(metric Metric, quantity int64, namespaceSlug, planCode string) int64 {
	r := s.matchPrice(metric, namespaceSlug, planCode)
	if r == nil {
		return 0
	}
	return CostMicros(quantity, r.UnitSize, r.UnitPriceMicros)
}

// ---------------------------------------------------------------------------
// 旁路写入
// ---------------------------------------------------------------------------

// Record 上报一条用量。永不返回 error，永不阻塞。
//
// 队列满时直接丢弃并计数。宁可丢计量，也不能让 MCP 请求变慢或失败。
func (s *Service) Record(u Usage) {
	if u.Metric == "" {
		return
	}
	if u.Quantity < 0 {
		u.Quantity = 0
	}
	if u.Occurred.IsZero() {
		u.Occurred = time.Now()
	}

	// 分片先算出来：形态 B 的 uev_ ID 必须带分片位，
	// 拿不到合法分片就没有 ID 可造，只能丢弃。
	sh := u.resolveShard()
	if sh == "" {
		s.dropped.Add(1)
		return
	}

	ev := store.UsageEvent{
		TenantID:      u.TenantID,
		UserID:        u.UserID,
		APIKeyID:      u.APIKeyID,
		SessionID:     u.SessionID,
		NamespaceID:   u.NamespaceID,
		NamespaceSlug: u.NamespaceSlug,
		RequestID:     u.RequestID,
		Metric:        string(u.Metric),
		Quantity:      u.Quantity,
		Unit:          u.Unit,
		Billable:      u.Billable,
		OccurredAt:    u.Occurred,
		Currency:      s.currency(),
	}

	if ev.Unit == "" {
		ev.Unit = defaultUnit(u.Metric)
	}

	// 计价在入队前完成：单价来自内存缓存，无 IO
	if u.Billable && s.billingEnabled() {
		if r := s.matchPrice(u.Metric, u.NamespaceSlug, u.PlanCode); r != nil {
			ev.CostMicros = CostMicros(u.Quantity, r.UnitSize, r.UnitPriceMicros)
			ev.PriceRuleID = r.ID
			ev.Currency = r.Currency
		}
	}

	id, err := idgen.NewSharded(idgen.KindUsageEvent, sh)
	if err != nil {
		s.dropped.Add(1)
		return
	}
	ev.ID = id

	select {
	case s.queue <- ev:
	default:
		s.dropped.Add(1)
	}
}

func defaultUnit(m Metric) string {
	switch m {
	case MetricInputToken, MetricOutputToken:
		return "token"
	case MetricUploadByte, MetricStorageByte:
		return "byte"
	case MetricRenderSecond:
		return "second"
	default:
		return "count"
	}
}

func (s *Service) currency() string {
	if s.settings != nil {
		if v := s.settings.Current().String("billing.default_currency", ""); v != "" {
			return v
		}
	}
	return "CNY"
}

func (s *Service) billingEnabled() bool {
	if s.settings == nil {
		return true
	}
	return s.settings.Current().Bool("billing.enabled", true)
}

func (s *Service) run(batchSize int, flushEvery time.Duration) {
	defer close(s.done)

	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()

	buf := make([]store.UsageEvent, 0, batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := s.st.InsertUsageEvents(ctx, buf); err != nil {
			s.writeFails.Add(1)
			log.Printf("[billing] 批量写计量失败（%d 条）: %v", len(buf), err)
		} else {
			s.written.Add(int64(len(buf)))
		}
		cancel()
		buf = buf[:0]
	}

	for {
		select {
		case <-s.stop:
			// 关闭前尽力把剩余数据写完
			for {
				select {
				case ev := <-s.queue:
					buf = append(buf, ev)
					if len(buf) >= batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case ev := <-s.queue:
			buf = append(buf, ev)
			if len(buf) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// Close 停止写入协程并尽量刷完队列。
func (s *Service) Close() {
	s.once.Do(func() {
		close(s.stop)
	})
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		log.Printf("[billing] 关闭超时，可能有计量数据未落库")
	}
}

// Stats 返回 (已写入, 已丢弃, 写失败) 计数。
func (s *Service) Stats() (written, dropped, failed int64) {
	return s.written.Load(), s.dropped.Load(), s.writeFails.Load()
}

// ---------------------------------------------------------------------------
// 预扣
// ---------------------------------------------------------------------------

// ReservationHandle 是预扣的可选凭据。为空表示未做预扣。
//
// Shard 一并带出来：结算/释放时要造 led_ ID，形态 B 同样需要分片位，
// 而重新推导会多一次不必要的判断。
type ReservationHandle struct {
	ID           idgen.ID
	UserID       idgen.ID
	AmountMicros int64
	Shard        string
}

// Valid 表示确实存在一次预扣。
func (h ReservationHandle) Valid() bool { return !h.ID.IsZero() }

// Reserve 为一次昂贵调用预扣额度。
//
// 返回零值 handle 表示未预扣（未开启 / 估值为 0 / 出错降级），
// 调用方不需要区分原因，直接继续执行即可。
func (s *Service) Reserve(ctx context.Context, u Usage, estimatedMicros int64) ReservationHandle {
	if estimatedMicros <= 0 || u.UserID.IsZero() {
		return ReservationHandle{}
	}
	if s.settings != nil && !s.settings.Current().Bool("billing.enforce_balance", false) {
		// 初期不拦人：不做预扣，避免余额机制误伤
		return ReservationHandle{}
	}

	sh := u.resolveShard()
	if sh == "" {
		return ReservationHandle{}
	}

	resID, err := idgen.NewSharded(idgen.KindReservation, sh)
	if err != nil {
		return ReservationHandle{}
	}
	ledgerID, err := idgen.NewSharded(idgen.KindLedger, sh)
	if err != nil {
		return ReservationHandle{}
	}

	ttl := 300 * time.Second
	if s.settings != nil {
		ttl = s.settings.Current().Duration("billing.reservation_ttl_seconds", ttl)
	}

	res := store.Reservation{
		ID:           resID,
		TenantID:     u.TenantID,
		UserID:       u.UserID,
		APIKeyID:     u.APIKeyID,
		AmountMicros: estimatedMicros,
		ExpiresAt:    time.Now().Add(ttl),
	}
	if err := s.st.Reserve(ctx, res, ledgerID, s.currency()); err != nil {
		// 计费故障不得阻断请求（计量计费红线）
		log.Printf("[billing] 预扣失败，放行请求: %v", err)
		return ReservationHandle{}
	}
	return ReservationHandle{ID: resID, UserID: u.UserID, AmountMicros: estimatedMicros, Shard: sh}
}

// Settle 结算一次预扣。actualMicros 为 0 时等价于全额释放。
func (s *Service) Settle(ctx context.Context, h ReservationHandle, actualMicros int64) {
	if !h.Valid() {
		return
	}

	settleID, err := idgen.NewSharded(idgen.KindLedger, h.Shard)
	if err != nil {
		return
	}
	extraID, err := idgen.NewSharded(idgen.KindLedger, h.Shard)
	if err != nil {
		return
	}

	if actualMicros <= 0 {
		if err := s.st.Release(ctx, h.ID, settleID, "无实际用量"); err != nil {
			log.Printf("[billing] 释放预扣失败: %v", err)
		}
		return
	}

	if _, err := s.st.Settle(ctx, h.ID, h.UserID, actualMicros, s.currency(), settleID, extraID); err != nil {
		log.Printf("[billing] 结算失败: %v", err)
	}
}

// RefreshPrices 重新加载费率。由管理端改价后调用，或定时兜底。
func (s *Service) RefreshPrices(ctx context.Context) error {
	return s.reloadPrices(ctx)
}

// ---------------------------------------------------------------------------
// context 传递
// ---------------------------------------------------------------------------

type ctxKey struct{}

// WithService 把计费服务放进 context，供中间件与 handler 取用。
func WithService(ctx context.Context, s *Service) context.Context {
	return context.WithValue(ctx, ctxKey{}, s)
}

// FromContext 取出计费服务。不存在时返回 nil，调用方需判空。
func FromContext(ctx context.Context) *Service {
	s, _ := ctx.Value(ctxKey{}).(*Service)
	return s
}

// RecordUsage 是给不方便持有 Service 的调用点准备的便捷函数。
// 服务不存在时静默返回，保证调用点不需要写 if 嵌套。
func RecordUsage(ctx context.Context, u Usage) {
	if s := FromContext(ctx); s != nil {
		s.Record(u)
	}
}

// FormatMicros 把微单位金额格式化成人类可读字符串，仅用于展示与日志。
func FormatMicros(micros int64) string {
	sign := ""
	if micros < 0 {
		sign = "-"
		micros = -micros
	}
	return fmt.Sprintf("%s%d.%06d", sign, micros/1_000_000, micros%1_000_000)
}
