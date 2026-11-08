package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// PriceRule 是 price_rules 表的一行。
type PriceRule struct {
	ID              idgen.ID
	Code            string
	Metric          string
	UnitSize        int64
	UnitPriceMicros int64
	Currency        string
	NamespaceSlug   string
	PlanCode        string
}

// PriceLookup 是解析费率时的请求上下文。
type PriceLookup struct {
	Metric        string
	NamespaceSlug string
	PlanCode      string
}

// ListActivePriceRules 一次性取出所有生效规则。
//
// 费率是纯配置、量级极小（几十条），全量缓存在内存里按优先级匹配，
// 避免每次请求查库。计量计费规则 6 要求费率必须在表里，不是要求每请求查表。
func (s *Store) ListActivePriceRules(ctx context.Context) ([]PriceRule, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, code, metric, unit_size, unit_price_micros, currency,
		       coalesce(namespace_slug, ''), coalesce(plan_code, '')
		FROM price_rules
		WHERE active
		  AND effective_from <= now()
		  AND (effective_to IS NULL OR effective_to > now())
		ORDER BY metric, coalesce(namespace_slug, ''), coalesce(plan_code, '')`)
	if err != nil {
		return nil, fmt.Errorf("查询费率失败: %w", err)
	}
	defer rows.Close()

	var out []PriceRule
	for rows.Next() {
		var p PriceRule
		if err := rows.Scan(&p.ID, &p.Code, &p.Metric, &p.UnitSize,
			&p.UnitPriceMicros, &p.Currency, &p.NamespaceSlug, &p.PlanCode); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// UsageEvent 是一条待写入的计量事件。
//
// TenantID 允许为空：它由调用链上游的鉴权结果带下来，遗漏时应能落库、
// 事后由后台按 user_id 回填，而不是让整批计量写入直接失败（NOT NULL 策略）。
type UsageEvent struct {
	ID            idgen.ID
	TenantID      idgen.ID
	UserID        idgen.ID
	APIKeyID      idgen.ID
	SessionID     idgen.ID
	NamespaceID   idgen.ID
	NamespaceSlug string
	RequestID     string
	Metric        string
	Quantity      int64
	Unit          string
	Billable      bool
	CostMicros    int64
	PriceRuleID   idgen.ID
	Currency      string
	OccurredAt    time.Time
}

// InsertUsageEvents 批量写入计量事件。
//
// 用单条 INSERT ... VALUES (...),(...) 而不是循环 Exec：
// 批量写是计量计费规则 7 的硬要求，逐条往返会把旁路写放大成主路径开销。
func (s *Store) InsertUsageEvents(ctx context.Context, events []UsageEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	const sql = `
		INSERT INTO usage_events (
			id, tenant_id, user_id, api_key_id, session_id, namespace_id, namespace_slug,
			request_id, metric, quantity, unit, billable, cost_micros,
			price_rule_id, currency, occurred_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`

	for _, e := range events {
		batch.Queue(sql,
			e.ID, nullableID(e.TenantID), e.UserID,
			nullableID(e.APIKeyID), nullableID(e.SessionID), nullableID(e.NamespaceID),
			nullIfEmpty(e.NamespaceSlug), nullIfEmpty(e.RequestID),
			e.Metric, e.Quantity, e.Unit, e.Billable, e.CostMicros,
			nullableID(e.PriceRuleID), e.Currency, e.OccurredAt,
		)
	}

	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()

	for range events {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("写入计量事件失败: %w", err)
		}
	}
	return nil
}

// Balance 是 account_balances 的一行。
type Balance struct {
	UserID                 idgen.ID
	BalanceMicros          int64
	ReservedMicros         int64
	LifetimeGrantedMicros  int64
	LifetimeConsumedMicros int64
	CreditLimitMicros      int64
	Currency               string
}

// AvailableMicros 返回除去预扣后的可用额度。
func (b Balance) AvailableMicros() int64 {
	return b.BalanceMicros - b.ReservedMicros + b.CreditLimitMicros
}

// GetBalance 读取余额。用户没有余额行时返回零值而非错误，
// 由上层决定是否按「初始授信」补建。
func (s *Store) GetBalance(ctx context.Context, userID idgen.ID) (*Balance, error) {
	var b Balance
	err := s.pool.QueryRow(ctx, `
		SELECT user_id, balance_micros, reserved_micros,
		       lifetime_granted_micros, lifetime_consumed_micros,
		       credit_limit_micros, currency
		FROM account_balances WHERE user_id = $1`, userID).
		Scan(&b.UserID, &b.BalanceMicros, &b.ReservedMicros,
			&b.LifetimeGrantedMicros, &b.LifetimeConsumedMicros,
			&b.CreditLimitMicros, &b.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return &Balance{UserID: userID}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取余额失败: %w", err)
	}
	return &b, nil
}

// EnsureBalance 幂等创建余额行，并按初始授信发放额度。
//
// 发放必须同时写账本，否则余额没有来源可追溯（计量计费规则 4）。
// 返回实际发放的金额，0 表示已存在。
func (s *Store) EnsureBalance(ctx context.Context, ledgerID idgen.ID, userID idgen.ID, initialGrantMicros int64, currency string) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM account_balances WHERE user_id = $1)`, userID).Scan(&exists); err != nil {
		return 0, err
	}
	if exists {
		return 0, nil
	}

	if initialGrantMicros < 0 {
		initialGrantMicros = 0
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO account_balances (user_id, balance_micros, reserved_micros,
			lifetime_granted_micros, lifetime_consumed_micros, credit_limit_micros, currency)
		VALUES ($1, $2, 0, $2, 0, 0, $3)`,
		userID, initialGrantMicros, currency); err != nil {
		return 0, fmt.Errorf("创建余额失败: %w", err)
	}

	if initialGrantMicros > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO credit_ledger (id, user_id, entry_type, direction, amount_micros,
				balance_after_micros, currency, idempotency_key, memo)
			VALUES ($1, $2, 'grant', 'credit', $3, $3, $4, $5, '初始授信')`,
			ledgerID, userID, initialGrantMicros, currency,
			fmt.Sprintf("init-grant-%s", userID)); err != nil {
			return 0, fmt.Errorf("写初始授信账本失败: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return initialGrantMicros, nil
}

// LedgerEntry 是待写入的账本分录。
type LedgerEntry struct {
	ID             idgen.ID
	TenantID       idgen.ID
	UserID         idgen.ID
	EntryType      string
	Direction      string
	AmountMicros   int64
	Currency       string
	UsageEventID   idgen.ID
	ReservationID  idgen.ID
	InvoiceID      idgen.ID
	IdempotencyKey string
	Memo           string
}

// ErrDuplicateLedger 表示幂等键已存在，说明这笔账已经记过。
var ErrDuplicateLedger = errors.New("账本分录已存在（幂等键重复）")

// ApplyLedger 在一个事务里更新余额并写账本。
//
// 这是计费正确性的唯一入口：余额与账本必须在同一事务里变更，
// 否则会出现「钱扣了账本没记」或反之，无法对账。
//
// 返回变更后的余额，供调用方做上限判断与日志。
func (s *Store) ApplyLedger(ctx context.Context, e LedgerEntry) (int64, error) {
	if e.AmountMicros <= 0 {
		return 0, errors.New("账本金额必须为正数，方向由 Direction 表达")
	}
	if e.Direction != "debit" && e.Direction != "credit" {
		return 0, fmt.Errorf("账本方向非法: %q", e.Direction)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 幂等检查：先占位，冲突即视为重复提交
	if e.IdempotencyKey != "" {
		var dup bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM credit_ledger WHERE idempotency_key = $1)`,
			e.IdempotencyKey).Scan(&dup); err != nil {
			return 0, err
		}
		if dup {
			return 0, ErrDuplicateLedger
		}
	}

	// 锁余额行，防止并发扣成负数
	var balance, reserved int64
	err = tx.QueryRow(ctx,
		`SELECT balance_micros, reserved_micros FROM account_balances
		 WHERE user_id = $1 FOR UPDATE`, e.UserID).Scan(&balance, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("用户 %s 没有余额记录，请先初始化", e.UserID)
	}
	if err != nil {
		return 0, fmt.Errorf("锁定余额失败: %w", err)
	}

	var newBalance int64
	var grantedDelta, consumedDelta int64

	switch e.Direction {
	case "credit":
		newBalance = balance + e.AmountMicros
		grantedDelta = e.AmountMicros
	case "debit":
		newBalance = balance - e.AmountMicros
		consumedDelta = e.AmountMicros
	}

	if _, err := tx.Exec(ctx, `
		UPDATE account_balances SET
			balance_micros           = $2,
			lifetime_granted_micros  = lifetime_granted_micros  + $3,
			lifetime_consumed_micros = lifetime_consumed_micros + $4,
			updated_at               = now()
		WHERE user_id = $1`,
		e.UserID, newBalance, grantedDelta, consumedDelta); err != nil {
		return 0, fmt.Errorf("更新余额失败: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO credit_ledger (id, tenant_id, user_id, entry_type, direction, amount_micros,
			balance_after_micros, currency, usage_event_id, reservation_id, invoice_id,
			idempotency_key, memo)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13,''))`,
		e.ID, nullableID(e.TenantID), e.UserID, e.EntryType, e.Direction, e.AmountMicros,
		newBalance, e.Currency,
		nullableID(e.UsageEventID), nullableID(e.ReservationID), nullableID(e.InvoiceID),
		nullIfEmpty(e.IdempotencyKey), e.Memo); err != nil {
		return 0, fmt.Errorf("写账本失败: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return newBalance, nil
}

// Reservation 是预扣单。
type Reservation struct {
	ID            idgen.ID
	TenantID      idgen.ID
	UserID        idgen.ID
	APIKeyID      idgen.ID
	AmountMicros  int64
	Status        string
	SettledMicros int64
	ExpiresAt     time.Time
}

// ErrInsufficientBalance 表示可用额度不足。
var ErrInsufficientBalance = errors.New("可用额度不足")

// Reserve 预扣额度。
//
// 语义：balance_micros 不变，reserved_micros 增加。
// 这样账本上能看到 reserve 分录，同时可用额度立刻被压住。
func (s *Store) Reserve(ctx context.Context, res Reservation, ledgerID idgen.ID, currency string) error {
	if res.AmountMicros <= 0 {
		return errors.New("预扣金额必须为正数")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var balance, reserved, creditLimit int64
	err = tx.QueryRow(ctx,
		`SELECT balance_micros, reserved_micros, credit_limit_micros
		 FROM account_balances WHERE user_id = $1 FOR UPDATE`, res.UserID).
		Scan(&balance, &reserved, &creditLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("用户 %s 没有余额记录", res.UserID)
	}
	if err != nil {
		return fmt.Errorf("锁定余额失败: %w", err)
	}

	if balance-reserved+creditLimit < res.AmountMicros {
		return ErrInsufficientBalance
	}

	// 状态取 DDL 的 'open'（CHECK 只认 open/settled/released/expired）。
	// 早期代码写的是 'held'，会被 CHECK 直接拒绝。
	status := res.Status
	if status == "" {
		status = "open"
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO billing_reservations (id, tenant_id, user_id, api_key_id, amount_micros, status, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		res.ID, nullableID(res.TenantID), res.UserID, nullableID(res.APIKeyID),
		res.AmountMicros, status, res.ExpiresAt); err != nil {
		return fmt.Errorf("创建预扣单失败: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE account_balances SET reserved_micros = reserved_micros + $2, updated_at = now()
		 WHERE user_id = $1`, res.UserID, res.AmountMicros); err != nil {
		return fmt.Errorf("增加预扣额失败: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO credit_ledger (id, tenant_id, user_id, entry_type, direction, amount_micros,
			balance_after_micros, currency, reservation_id, idempotency_key, memo)
		VALUES ($1,$2,$3,'reserve','debit',$4,$5,$6,$7,$8,'预扣')`,
		ledgerID, nullableID(res.TenantID), res.UserID, res.AmountMicros, balance, currency, res.ID,
		fmt.Sprintf("reserve-%s", res.ID)); err != nil {
		return fmt.Errorf("写预扣账本失败: %w", err)
	}

	return tx.Commit(ctx)
}

// Settle 结算：把预扣转成实际消耗，未用完部分释放。
//
// 三种情况统一处理：
//   - actual == reserved：全额消耗
//   - actual <  reserved：消耗 actual，释放差额
//   - actual >  reserved：消耗 reserved 后再额外扣 actual-reserved
//
// 第三种的额外扣费可能让余额转负，这是刻意的：
// 请求已经发生、成本已经产生，事后拒绝对账没有意义。
// 是否允许透支由 credit_limit 与 settings 决定。
func (s *Store) Settle(ctx context.Context, resID idgen.ID, userID idgen.ID, actualMicros int64, currency string, ledgerSettleID, ledgerReleaseID idgen.ID) (int64, error) {
	if actualMicros < 0 {
		actualMicros = 0
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var reservedAmount int64
	var status string
	err = tx.QueryRow(ctx,
		`SELECT amount_micros, status FROM billing_reservations
		 WHERE id = $1 FOR UPDATE`, resID).Scan(&reservedAmount, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("预扣单 %s 不存在", resID)
	}
	if err != nil {
		return 0, err
	}
	// 'open' 才是可结算状态（DDL 的 CHECK 只认 open/settled/released/expired）。
	// 早期代码比对的是 'held'，与 DDL 不一致，导致预扣永远无法结算。
	if status != "open" {
		// 已结算或已释放，视为幂等成功
		return 0, nil
	}

	var balance, reservedTotal, consumedTotal int64
	if err := tx.QueryRow(ctx,
		`SELECT balance_micros, reserved_micros, lifetime_consumed_micros
		 FROM account_balances WHERE user_id = $1 FOR UPDATE`, userID).
		Scan(&balance, &reservedTotal, &consumedTotal); err != nil {
		return 0, fmt.Errorf("锁定余额失败: %w", err)
	}

	extra := int64(0)
	if actualMicros > reservedAmount {
		extra = actualMicros - reservedAmount
	}
	newBalance := balance - extra
	newReserved := reservedTotal - reservedAmount
	if newReserved < 0 {
		newReserved = 0
	}

	if _, err := tx.Exec(ctx, `
		UPDATE account_balances SET
			balance_micros           = $2,
			reserved_micros          = $3,
			lifetime_consumed_micros = lifetime_consumed_micros + $4,
			updated_at               = now()
		WHERE user_id = $1`,
		userID, newBalance, newReserved, actualMicros); err != nil {
		return 0, fmt.Errorf("更新余额失败: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE billing_reservations SET status = 'settled', settled_micros = $2, closed_at = now()
		 WHERE id = $1`, resID, actualMicros); err != nil {
		return 0, fmt.Errorf("更新预扣单失败: %w", err)
	}

	// 结算分录：预扣的金额视为已消耗
	if reservedAmount > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO credit_ledger (id, tenant_id, user_id, entry_type, direction, amount_micros,
				balance_after_micros, currency, reservation_id, idempotency_key, memo)
			VALUES ($1,(SELECT tenant_id FROM billing_reservations WHERE id = $2),
			        $3,'settle','debit',$4,$5,$6,$7,$8,'结算')`,
			ledgerSettleID, resID, userID, reservedAmount, newBalance, currency, resID,
			fmt.Sprintf("settle-%s", resID)); err != nil {
			return 0, fmt.Errorf("写结算账本失败: %w", err)
		}
	}

	// 超出预扣的部分单独记一笔，账面上能看出「超支」
	if extra > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO credit_ledger (id, tenant_id, user_id, entry_type, direction, amount_micros,
				balance_after_micros, currency, reservation_id, idempotency_key, memo)
			VALUES ($1,(SELECT tenant_id FROM billing_reservations WHERE id = $2),
			        $3,'consume','debit',$4,$5,$6,$7,$8,'超出预扣部分')`,
			ledgerReleaseID, resID, userID, extra, newBalance, currency, resID,
			fmt.Sprintf("settle-extra-%s", resID)); err != nil {
			return 0, fmt.Errorf("写超支账本失败: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return newBalance, nil
}

// Release 释放预扣（请求失败或未产生实际用量时）。
func (s *Store) Release(ctx context.Context, resID idgen.ID, ledgerID idgen.ID, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// user_id 与 tenant_id 都是 TEXT 形态的业务 ID，必须用 idgen.ID 接，
	// 旧代码用 int64 接 user_id，Scan 在运行期就会报类型不匹配。
	var userID, tenantID idgen.ID
	var amount int64
	var status string
	err = tx.QueryRow(ctx,
		`SELECT user_id, tenant_id, amount_micros, status FROM billing_reservations
		 WHERE id = $1 FOR UPDATE`, resID).Scan(&userID, &tenantID, &amount, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("预扣单 %s 不存在", resID)
	}
	if err != nil {
		return err
	}
	if status != "open" {
		return nil
	}

	if _, err := tx.Exec(ctx,
		`UPDATE account_balances SET reserved_micros = GREATEST(reserved_micros - $2, 0),
		 updated_at = now() WHERE user_id = $1`, userID, amount); err != nil {
		return fmt.Errorf("释放预扣额失败: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE billing_reservations SET status = 'released', closed_at = now() WHERE id = $1`,
		resID); err != nil {
		return err
	}

	var balance int64
	if err := tx.QueryRow(ctx,
		`SELECT balance_micros FROM account_balances WHERE user_id = $1`, userID).Scan(&balance); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO credit_ledger (id, tenant_id, user_id, entry_type, direction, amount_micros,
			balance_after_micros, currency, reservation_id, idempotency_key, memo)
		VALUES ($1,$2,$3,'release','credit',$4,$5,'CNY',$6,$7,NULLIF($8,''))`,
		ledgerID, nullableID(tenantID), userID, amount, balance, resID,
		fmt.Sprintf("release-%s", resID), reason); err != nil {
		return fmt.Errorf("写释放账本失败: %w", err)
	}

	return tx.Commit(ctx)
}

// ExpireStaleReservations 把超时未结算的预扣转成 expired 并释放额度。
//
// 进程崩溃或被 kill 会留下 open 记录，不清理会永久冻结用户额度。
// 由定时任务每分钟调用一次。
//
// 不再接收「账本 ID 种子」参数：那是 BIGINT 时代从主键做算术的遗留做法。
// 现在是字符串 ID，每行预扣单自己的 ID 里就带着分片位，
// 直接 idgen.ShardOf(id) 读出即可造出合法的 led_ ID，无需外部种子。
func (s *Store) ExpireStaleReservations(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id FROM billing_reservations
		WHERE status = 'open' AND expires_at < now()
		ORDER BY expires_at
		LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("查询过期预扣失败: %w", err)
	}

	var ids []idgen.ID
	for rows.Next() {
		var id idgen.ID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	released := 0
	for _, id := range ids {
		// 分片位直接来自预扣单 ID 本身（形态 B 第 5~6 字符），零查表。
		ledgerID, err := idgen.NewSharded(idgen.KindLedger, idgen.ShardOf(id))
		if err != nil {
			return released, fmt.Errorf("为预扣单 %s 生成账本 ID 失败: %w", id, err)
		}
		if err := s.Release(ctx, id, ledgerID, "预扣超时自动释放"); err != nil {
			return released, err
		}
		released++
	}
	return released, nil
}

// UsageSummary 是某用户的用量汇总。
type UsageSummary struct {
	Metric     string
	Quantity   int64
	CostMicros int64
	EventCount int64
}

// SumUsage 按维度汇总一段时间内的用量，供管理端报表使用。
func (s *Store) SumUsage(ctx context.Context, userID idgen.ID, from, to time.Time) ([]UsageSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT metric, sum(quantity), sum(cost_micros), count(*)
		FROM usage_events
		WHERE user_id = $1 AND occurred_at >= $2 AND occurred_at < $3
		GROUP BY metric
		ORDER BY metric`, userID, from, to)
	if err != nil {
		return nil, fmt.Errorf("汇总用量失败: %w", err)
	}
	defer rows.Close()

	var out []UsageSummary
	for rows.Next() {
		var u UsageSummary
		if err := rows.Scan(&u.Metric, &u.Quantity, &u.CostMicros, &u.EventCount); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

// nullableID 把零值 idgen.ID 转成 SQL NULL。
//
// ID 现在是带前缀的字符串，「空」指的是空串（idgen.Zero）而不是数字 0。
// 用 IsZero 而不是 == 0 是硬要求：idgen.ID 的底层类型已改为 string，
// 任何与整数字面量的比较都编不过。
func nullableID(id idgen.ID) any {
	if id.IsZero() {
		return nil
	}
	return id
}

// nullIfEmpty 把空串转成 SQL NULL，避免唯一索引把多个空串当成重复。
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
