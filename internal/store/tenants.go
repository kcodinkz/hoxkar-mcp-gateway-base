package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/shard"
)

// 租户状态与 tenants.status 的 CHECK 约束保持一致。
//
// 这四个状态的语义差别必须分清，否则「到期」会退化成「删除」：
//
//	active    正常可用
//	expired   套餐到期，数据全在，只是能力被降级（租户与分片 §9.4）
//	suspended 管理员主动停用，通常是欠费或违规
//	closed    用户主动注销，仍然不删数据，等保留期结束后人工清理
//
// 没有任何一个状态意味着「行可以删掉」。
type TenantStatus string

const (
	TenantActive    TenantStatus = "active"
	TenantExpired   TenantStatus = "expired"
	TenantSuspended TenantStatus = "suspended"
	TenantClosed    TenantStatus = "closed"
)

// 租户形态与 tenants.kind 的 CHECK 约束保持一致。
//
// kind 只影响「展示与能力门控」，禁止用它判断数据归属：
// 数据归属的唯一依据是 tenants.id（租户与分片）。用户从个人版升到企业版、
// 到期再回落，改的都是同一行的 kind，租户 ID 与分片终生不变。
type TenantKind string

const (
	TenantPersonal   TenantKind = "personal"
	TenantEnterprise TenantKind = "enterprise"
)

// TenantMemberRole 与 tenant_members.role 的 CHECK 约束保持一致。
type TenantMemberRole string

const (
	TenantRoleOwner  TenantMemberRole = "owner"
	TenantRoleAdmin  TenantMemberRole = "admin"
	TenantRoleMember TenantMemberRole = "member"
)

// ErrTenantShardImmutable 表示试图改写已经落地的分片号。
//
// 单独定义而不是复用普通错误：这条一旦被静默接受，该租户的历史流水行
// 会留在旧分片上，而 ID 里印着新分片号，数据永久错位且无法靠查询发现。
var ErrTenantShardImmutable = errors.New("租户分片一旦确定不可变更（租户与分片）")

// ErrTenantExpiryRequired 表示企业级租户没有给到期时间。
//
// DDL 里有 tenants_ent_has_expiry 兜底，这里先拦一次是为了给出
// 「哪个租户、缺什么」这种能直接定位的错误，而不是一个约束名（NOT NULL 策略）。
var ErrTenantExpiryRequired = errors.New("企业级租户必须设置到期时间")

// Tenant 是 tenants 表的一行。
type Tenant struct {
	ID     idgen.ID
	Slug   string
	Name   string
	Kind   TenantKind
	Status TenantStatus

	// PlanID 为空表示「跟随 owner 账号的 users.plan_id」。
	//
	// 这不是「数据缺失」，而是一种合法的企业级语义：企业套餐可以挂在整个
	// 租户上，也可以让 owner 的个人套餐代表整个租户。空值由
	// internal/quota 解析有效套餐时回落，不做回填（NOT NULL 策略）。
	PlanID idgen.ID

	// Shard 是创建时分配的两位分片号，终生不变（租户与分片）。
	// 只读：写入路径只有 CreateTenant 的初始值，没有 update 通道。
	Shard string

	// ExpiresAt 仅企业级有值；personal 恒为 nil。
	ExpiresAt *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// IsEnterprise 判断当前形态。仅用于展示与能力门控，不可用于判断数据归属。
func (t *Tenant) IsEnterprise() bool { return t.Kind == TenantEnterprise }

// IsUsable 判断这个租户当前能否承接新流量。
//
// expired 仍然算「可用」：到期只是降级到 free 额度，数据与读写都还在，
// 用户续费即刻恢复。这正是「不代表数据就丢失」的落点。
func (t *Tenant) IsUsable() bool {
	return t.Status == TenantActive || t.Status == TenantExpired
}

// name 用 coalesce 包住：按 NOT NULL 策略它已放宽为可空（展示名可后补，不参与任何约束），
// 而 Tenant.Name 是 string，裸列遇到 NULL 会直接 Scan 报错。
const tenantColumns = `id, slug, coalesce(name, ''), kind, status, coalesce(plan_id, ''),
                       shard, expires_at, created_at, updated_at`

// CreateTenant 写入租户。主键与分片号都由调用方生成。
//
// 分片为什么由调用方给而不是这里随机挑：分片选择属于「租户创建流程」的
// 策略（例如按当前各池的租户数做均衡），store 层只负责把决定落库。
// 这里做的是校验——非法分片、保留分片 00 一律拒绝，不入库。
func (s *Store) CreateTenant(
	ctx context.Context,
	id idgen.ID,
	slug, name string,
	kind TenantKind,
	planID idgen.ID,
	shardNo string,
	expiresAt *time.Time,
) (*Tenant, error) {
	if id.IsZero() {
		return nil, errors.New("创建租户需要非零的租户 ID")
	}
	// 前缀必须是 tnt_：租户 ID 混进别的形态，后面所有「按租户分片」
	// 的推导都会静默跑偏，所以在入口就拦。
	if err := idgen.ValidateOf(idgen.KindTenant, string(id)); err != nil {
		return nil, fmt.Errorf("租户 ID %q 不合法: %w", id, err)
	}
	// 校验规范化后再入库：库里存的永远是规范化后的大写两位。
	sh, err := shard.Parse(shardNo)
	if err != nil {
		return nil, fmt.Errorf("租户 %q 的分片号 %q 不合法: %w", id, shardNo, err)
	}
	if kind == TenantEnterprise && expiresAt == nil {
		return nil, fmt.Errorf("%w: 租户 %q", ErrTenantExpiryRequired, id)
	}
	if kind != TenantEnterprise && expiresAt != nil {
		// personal 恒为 NULL（DDL 注释）。传了就是调用方的 bug，
		// 不要静默丢弃——否则「为什么到期没生效」会查很久。
		return nil, fmt.Errorf("个人租户 %q 不应设置到期时间", id)
	}

	var (
		createdAt time.Time
		updatedAt time.Time
		gotKind   string
		gotStatus string
		gotPlan   string
		gotShard  string
		gotExpiry *time.Time
		gotSlug   string
		gotName   string
	)
	err = s.pool.QueryRow(ctx, `
		INSERT INTO tenants (id, slug, name, kind, plan_id, shard, expires_at)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7)
		RETURNING `+tenantColumns,
		id, slug, name, string(kind), string(planID), sh.String(), expiresAt,
	).Scan(&id, &gotSlug, &gotName, &gotKind, &gotStatus, &gotPlan,
		&gotShard, &gotExpiry, &createdAt, &updatedAt)
	if err != nil {
		return nil, fmt.Errorf("创建租户失败: %w", err)
	}

	return &Tenant{
		ID:        id,
		Slug:      gotSlug,
		Name:      gotName,
		Kind:      TenantKind(gotKind),
		Status:    TenantStatus(gotStatus),
		PlanID:    idgen.ID(gotPlan),
		Shard:     gotShard,
		ExpiresAt: gotExpiry,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// GetTenantByID 按主键取租户。
func (s *Store) GetTenantByID(ctx context.Context, id idgen.ID) (*Tenant, error) {
	if id.IsZero() {
		return nil, errors.New("查询租户需要非零的租户 ID")
	}
	row := s.pool.QueryRow(ctx,
		`SELECT `+tenantColumns+` FROM tenants WHERE id = $1`, id)
	return scanTenant(row)
}

// GetTenantBySlug 按 slug 取租户，用于注册时判断 slug 是否已被占用。
func (s *Store) GetTenantBySlug(ctx context.Context, slug string) (*Tenant, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+tenantColumns+` FROM tenants WHERE slug = $1`, slug)
	return scanTenant(row)
}

// ListTenants 分页列出租户，供管理端使用（分页契约）。
//
// 排序固定 (created_at DESC, id DESC)：created_at 精度可能相同，
// 补上 id 才能保证翻页不重不漏。id 是字符串，形态 A 无分片位，
// 同一时刻创建的租户按 ID 排序稳定即可，不要求与人眼顺序一致。
func (s *Store) ListTenants(ctx context.Context, page, pageSize int) ([]Tenant, int64, error) {
	page, pageSize, offset := normalizePage(page, pageSize)

	var total int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tenants`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("统计租户数量失败: %w", err)
	}
	if total == 0 {
		return []Tenant{}, 0, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+tenantColumns+` FROM tenants
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2`, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("列出租户失败: %w", err)
	}
	defer rows.Close()

	out := make([]Tenant, 0, pageSize)
	for rows.Next() {
		t, err := scanTenantRows(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, *t)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("遍历租户结果失败: %w", err)
	}
	return out, total, nil
}

// TenantPatch 是 UpdateTenant 的补丁。
//
// 用可选指针而不是整行覆盖：管理端「只改状态」不该顺手把 name 写成空串。
// nil 表示不动，非 nil 才是改。PlanID 指向空字符串表示清空（回落 owner）。
type TenantPatch struct {
	Name      *string
	Kind      *TenantKind
	Status    *TenantStatus
	PlanID    *idgen.ID
	ExpiresAt **time.Time
}

// UpdateTenant 按补丁更新租户的可变字段。
//
// 明确不包含 Shard：分片终生不变（租户与分片），任何「改了分片顺便迁移」的
// 需求都要先把 ID 体系改一遍，顺序不能反。传了也没用——本函数没有该字段。
func (s *Store) UpdateTenant(ctx context.Context, id idgen.ID, p TenantPatch) (*Tenant, error) {
	if id.IsZero() {
		return nil, errors.New("更新租户需要非零的租户 ID")
	}

	// 动态拼 SET 子句：列名全部是代码里的常量，值一律参数化。
	// $1 固定留给主键，其余按补丁字段顺序编号。
	set := make([]string, 0, 6)
	args := make([]any, 0, 7)
	args = append(args, id)

	// next 追加一个参数并返回它的占位符，避免手工数 $n 出错。
	next := func(val any) string {
		args = append(args, val)
		return fmt.Sprintf("$%d", len(args))
	}

	if p.Name != nil {
		set = append(set, "name = "+next(*p.Name))
	}
	if p.Kind != nil {
		set = append(set, "kind = "+next(string(*p.Kind)))
	}
	if p.Status != nil {
		set = append(set, "status = "+next(string(*p.Status)))
	}
	if p.PlanID != nil {
		// 空字符串 → NULL，表示回落 owner 的套餐（NOT NULL 策略：NULL 是合法语义）
		set = append(set, "plan_id = NULLIF("+next(string(*p.PlanID))+", '')")
	}
	if p.ExpiresAt != nil {
		set = append(set, "expires_at = "+next(*p.ExpiresAt))
	}
	if len(set) == 0 {
		// 空补丁不报错也不写库：调用方拿到当前行即可。
		return s.GetTenantByID(ctx, id)
	}

	set = append(set, "updated_at = now()")
	sql := `UPDATE tenants SET ` + strings.Join(set, ", ") +
		` WHERE id = $1 RETURNING ` + tenantColumns

	row := s.pool.QueryRow(ctx, sql, args...)
	return scanTenant(row)
}

// SetTenantStatus 是只改状态的快捷方式，到期扫描与管理员停用都走这里。
func (s *Store) SetTenantStatus(ctx context.Context, id idgen.ID, st TenantStatus) error {
	if id.IsZero() {
		return errors.New("更新租户状态需要非零的租户 ID")
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE tenants SET status = $2, updated_at = now() WHERE id = $1`,
		id, string(st))
	if err != nil {
		return fmt.Errorf("更新租户 %q 状态失败: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// -----------------------------------------------------------------------------
// shard.Source 实现：shard.Directory 通过这两个方法回源
//
// 返回的是 tenants.shard 的原始字符串、未做任何规范化——规范化统一由
// shard.fromColumn 负责。两处各做一遍迟早会分叉，然后出现「库里写 03、
// 缓存键算成 O3」这类只在高并发下暴露的错分片。
// -----------------------------------------------------------------------------

// TenantShard 返回租户的分片号原始值。
func (s *Store) TenantShard(ctx context.Context, tenantID idgen.ID) (string, error) {
	if tenantID.IsZero() {
		return "", errors.New("查询分片需要非零的租户 ID")
	}
	var raw string
	err := s.pool.QueryRow(ctx,
		`SELECT shard FROM tenants WHERE id = $1`, tenantID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: 租户 %q", ErrNotFound, tenantID)
	}
	if err != nil {
		return "", fmt.Errorf("查询租户 %q 的分片失败: %w", tenantID, err)
	}
	return raw, nil
}

// TenantShards 返回全部租户的分片，供 shard.Directory.Warm 启动预热。
//
// tenants.shard 是 NOT NULL，所以不存在「某行没有分片」需要在此忽略的情况。
// 若将来放宽该列，这里要保持「不跳过」——让 Warm 整体失败远好过悄悄少一个
// 租户，那会让该租户的请求落到默认池并写进错误的分片（见 Directory.Warm 注释）。
func (s *Store) TenantShards(ctx context.Context) (map[idgen.ID]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, shard FROM tenants`)
	if err != nil {
		return nil, fmt.Errorf("全量查询租户分片失败: %w", err)
	}
	defer rows.Close()

	out := make(map[idgen.ID]string)
	for rows.Next() {
		var (
			id  idgen.ID
			raw string
		)
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("读取租户分片行失败: %w", err)
		}
		out[id] = raw
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历租户分片结果失败: %w", err)
	}
	return out, nil
}

// 编译期断言：store 就是 shard 的分片数据源。
// 签名一旦改动，这里立刻编译失败，而不是等到启动预热时才发现。
var _ shard.Source = (*Store)(nil)

// -----------------------------------------------------------------------------
// 租户成员
// -----------------------------------------------------------------------------

// TenantMember 是 tenant_members 的一行。
//
// 没有独立主键：主键是 (tenant_id, user_id)，一个人在一个租户里只有一条身份。
// 所以这里不生成任何 ID，也不涉及 ID 形态的两种形态。
type TenantMember struct {
	TenantID idgen.ID
	UserID   idgen.ID
	Role     TenantMemberRole
	Status   string
	JoinedAt time.Time
}

// AddTenantMember 添加或更新成员身份（幂等）。
//
// 重复调用按最后一次写入为准：role 与 status 覆盖，joined_at 保留首次时间。
// 用 ON CONFLICT 而不是「先查后写」，避免并发注册同一人时插入撞唯一键。
func (s *Store) AddTenantMember(ctx context.Context, tenantID, userID idgen.ID, role TenantMemberRole) error {
	if tenantID.IsZero() || userID.IsZero() {
		return errors.New("添加租户成员需要非零的租户 ID 与用户 ID")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tenant_members (tenant_id, user_id, role)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, user_id)
		DO UPDATE SET role = EXCLUDED.role, status = 'active'`,
		tenantID, userID, string(role))
	if err != nil {
		return fmt.Errorf("添加租户 %q 的成员 %q 失败: %w", tenantID, userID, err)
	}
	return nil
}

// ListTenantMembers 列出某租户的全部成员（不分页：单租户成员量有界）。
func (s *Store) ListTenantMembers(ctx context.Context, tenantID idgen.ID) ([]TenantMember, error) {
	if tenantID.IsZero() {
		return nil, errors.New("列出租户成员需要非零的租户 ID")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id, user_id, role, status, joined_at
		FROM tenant_members WHERE tenant_id = $1
		ORDER BY joined_at, user_id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("列出租户 %q 的成员失败: %w", tenantID, err)
	}
	defer rows.Close()

	out := make([]TenantMember, 0, 8)
	for rows.Next() {
		var (
			m    TenantMember
			role string
		)
		if err := rows.Scan(&m.TenantID, &m.UserID, &role, &m.Status, &m.JoinedAt); err != nil {
			return nil, fmt.Errorf("读取租户成员行失败: %w", err)
		}
		m.Role = TenantMemberRole(role)
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历租户成员结果失败: %w", err)
	}
	return out, nil
}

// RemoveTenantMember 移除成员身份。
//
// 只删这一条关系行，不碰 users、不碰任何业务数据：成员离开租户不等于
// 账号被删除，该用户可能同时还在别的租户里。
func (s *Store) RemoveTenantMember(ctx context.Context, tenantID, userID idgen.ID) error {
	if tenantID.IsZero() || userID.IsZero() {
		return errors.New("移除租户成员需要非零的租户 ID 与用户 ID")
	}
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM tenant_members WHERE tenant_id = $1 AND user_id = $2`,
		tenantID, userID)
	if err != nil {
		return fmt.Errorf("移除租户 %q 的成员 %q 失败: %w", tenantID, userID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// -----------------------------------------------------------------------------
// 扫描辅助
// -----------------------------------------------------------------------------

func scanTenant(row pgx.Row) (*Tenant, error) {
	var (
		t                        Tenant
		kind, status, plan, shrd string
	)
	err := row.Scan(&t.ID, &t.Slug, &t.Name, &kind, &status, &plan,
		&shrd, &t.ExpiresAt, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("读取租户失败: %w", err)
	}
	t.Kind = TenantKind(kind)
	t.Status = TenantStatus(status)
	t.PlanID = idgen.ID(plan)
	t.Shard = shrd
	return &t, nil
}

func scanTenantRows(rows pgx.Rows) (*Tenant, error) {
	var (
		t                        Tenant
		kind, status, plan, shrd string
	)
	err := rows.Scan(&t.ID, &t.Slug, &t.Name, &kind, &status, &plan,
		&shrd, &t.ExpiresAt, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("读取租户行失败: %w", err)
	}
	t.Kind = TenantKind(kind)
	t.Status = TenantStatus(status)
	t.PlanID = idgen.ID(plan)
	t.Shard = shrd
	return &t, nil
}
