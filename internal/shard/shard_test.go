package shard

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// 本文件只测「不需要真实数据库」的部分：分片号解析、Directory 的
// 缓存与回源行为、PoolSet 的选池逻辑。涉及 pgxpool.Pool 的断言一律
// 停留在「nil / 非 nil / 同一个对象」这一层——连真实库属于建库自检的验收。

// TestMain 初始化雪花生成器：NewSharded 依赖它，不初始化会返回
// ErrNotInitialized。nodeID 取 99 是为了与 idgen 包测试用的 1 区分开，
// 两个包各跑各的进程，本来不冲突，这里只是避免将来并行跑测试时误判。
func TestMain(m *testing.M) {
	if err := idgen.Init(99); err != nil {
		panic("初始化 idgen 失败: " + err.Error())
	}
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// 分片号解析
// ---------------------------------------------------------------------------

// TestParseMatchesIdgenOverAllShards 逐一比对全部 1024 个分片号。
//
// 这是本包最容易出错的地方：分片号既是 ID 业务体的一段，又是缓存键。
// 两处归一化规则一旦分叉，就会出现「ID 里写着 03、缓存里算成 O3」的错分片，
// 而这种 bug 只在流量上来后才暴露——单个请求永远是「偶发数据不对」。
func TestParseMatchesIdgenOverAllShards(t *testing.T) {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

	checked := 0
	for i := 0; i < len(alphabet); i++ {
		for j := 0; j < len(alphabet); j++ {
			raw := string([]byte{alphabet[i], alphabet[j]})
			if raw == idgen.ReservedShard {
				continue
			}

			got, err := Parse(raw)
			if err != nil {
				t.Fatalf("Parse(%q) 意外失败: %v", raw, err)
			}
			if string(got) != raw {
				t.Fatalf("Parse(%q) 应为 %q，收到 %q", raw, raw, got)
			}
			if err := idgen.ValidateShard(raw); err != nil {
				t.Fatalf("idgen 与 shard 对 %q 的判断不一致: idgen 报 %v", raw, err)
			}
			checked++
		}
	}
	if checked != 1023 {
		t.Fatalf("应检查 1023 个非保留分片号，实际 %d", checked)
	}
}

// TestParseNormalizesLikeIdgen 覆盖 Crockford 的三个宽容映射。
func TestParseNormalizesLikeIdgen(t *testing.T) {
	cases := map[string]Shard{
		"03": "03",
		"o3": "03", // o → 0
		"O3": "03",
		"i3": "13", // i → 1
		"l3": "13", // l → 1
		"L3": "13",
		"z7": "Z7", // 小写转大写
		"Z7": "Z7",
	}
	for in, want := range cases {
		got, err := Parse(in)
		if err != nil {
			t.Fatalf("Parse(%q) 意外失败: %v", in, err)
		}
		if got != want {
			t.Errorf("Parse(%q) 应为 %q，收到 %q", in, want, got)
		}
	}
}

func TestParseRejects(t *testing.T) {
	cases := []struct {
		in     string
		reason string
	}{
		{"00", "保留分片必须被拒绝"},
		{"0", "长度不足"},
		{"003", "长度超长"},
		{"", "空串"},
		{"0U", "U 不在 Crockford 字符集里"},
		{"U0", "U 不在 Crockford 字符集里"},
		{"-1", "非法字符"},
	}
	for _, c := range cases {
		if _, err := Parse(c.in); err == nil {
			t.Errorf("Parse(%q) 应失败（%s），却成功了", c.in, c.reason)
		}
	}

	// 保留分片的错误必须能被 errors.Is 认出，运维脚本要靠它区分
	// 「配置写错」和「有人试图给业务数据分配保留分片」。
	if _, err := Parse("00"); !errors.Is(err, idgen.ErrReservedShard) {
		t.Errorf("Parse(\"00\") 应返回 idgen.ErrReservedShard，收到 %v", err)
	}
}

// TestOfIDNeedsNoLookup 覆盖第一条：
// 给定 ses_03XXXX…，ShardOf 必须直接返回 "03"，且不产生任何 SQL。
//
// 这里用空 Directory（没有数据源）证明这一点：如果 OfID 偷偷回源，
// 空目录会立刻报 ErrNoSource。
func TestOfIDNeedsNoLookup(t *testing.T) {
	id, err := idgen.NewSharded(idgen.KindSession, "03")
	if err != nil {
		t.Fatalf("构造分片型 ID 失败: %v", err)
	}

	sh, ok := OfID(id)
	if !ok {
		t.Fatalf("应能从 %q 读出分片号", id)
	}
	if sh != "03" {
		t.Fatalf("分片号应为 \"03\"，收到 %q", sh)
	}

	// 形态 A 没有分片位。
	userID := idgen.MustRandom(idgen.KindUser)
	if _, ok := OfID(userID); ok {
		t.Errorf("形态 A 的 %q 不应有分片号", userID)
	}

	// 非法 ID。
	if _, ok := OfID("ses_03"); ok {
		t.Errorf("非法 ID 不应读出分片号")
	}
}

// ---------------------------------------------------------------------------
// Directory
// ---------------------------------------------------------------------------

// fakeSource 是可编排的 Source：记录回源次数，用于验证缓存。
type fakeSource struct {
	calls atomic.Int64

	mu     sync.Mutex
	byID   map[idgen.ID]string
	err    error
	listed map[idgen.ID]string
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		byID:   make(map[idgen.ID]string),
		listed: make(map[idgen.ID]string),
	}
}

func (f *fakeSource) TenantShard(_ context.Context, tenantID idgen.ID) (string, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	raw, ok := f.byID[tenantID]
	if !ok {
		return "", errors.New("租户不存在")
	}
	return raw, nil
}

func (f *fakeSource) TenantShards(_ context.Context) (map[idgen.ID]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[idgen.ID]string, len(f.listed))
	for k, v := range f.listed {
		out[k] = v
	}
	return out, nil
}

// TestDirectoryShardOfCaches 覆盖第二条：
// 首次查库、二次命中缓存。
func TestDirectoryShardOfCaches(t *testing.T) {
	src := newFakeSource()
	tenantID := idgen.MustRandom(idgen.KindTenant)
	src.byID[tenantID] = "03"

	dir := NewDirectory(src)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		sh, err := dir.ShardOf(ctx, tenantID)
		if err != nil {
			t.Fatalf("第 %d 次 ShardOf 失败: %v", i+1, err)
		}
		if sh != "03" {
			t.Fatalf("第 %d 次分片号应为 \"03\"，收到 %q", i+1, sh)
		}
	}

	if got := src.calls.Load(); got != 1 {
		t.Fatalf("5 次查询应只回源 1 次，实际 %d 次（缓存失效）", got)
	}
	if dir.Len() != 1 {
		t.Fatalf("目录应有 1 条缓存，实际 %d", dir.Len())
	}
}

// TestDirectoryShardOfEmptyID 拒绝空租户，且不回源。
func TestDirectoryShardOfEmptyID(t *testing.T) {
	src := newFakeSource()
	dir := NewDirectory(src)

	if _, err := dir.ShardOf(context.Background(), idgen.Zero); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("空租户 ID 应返回 ErrNoTenant，收到 %v", err)
	}
	if got := src.calls.Load(); got != 0 {
		t.Fatalf("空租户 ID 不应回源，实际回源 %d 次", got)
	}
}

// TestDirectoryRejectsBadColumn 覆盖 tenants.shard 的坏数据。
//
// 注意 "zz" 是**合法**的（小写宽容 → "ZZ"，Z 在 Crockford 集内），
// 所以它不在拒绝清单里。真正非法的是含 U 之类的集外字符。
func TestDirectoryRejectsBadColumn(t *testing.T) {
	cases := map[string]string{
		"":   "列为空",
		"00": "保留分片",
		"0U": "U 不在 Crockford 字符集里",
		"7":  "长度不足",
	}

	src := newFakeSource()
	tenantID := idgen.MustRandom(idgen.KindTenant)
	dir := NewDirectory(src)
	ctx := context.Background()

	for raw, reason := range cases {
		src.byID[tenantID] = raw
		dir.Forget(tenantID)

		if _, err := dir.ShardOf(ctx, tenantID); err == nil {
			t.Errorf("shard 列=%q（%s）应报错，却返回成功", raw, reason)
		} else if !errors.Is(err, ErrNoShard) {
			t.Errorf("shard 列=%q 应包成 ErrNoShard，收到 %v", raw, err)
		}
	}

	// 反证：小写 zz 必须被接受并归一化成 ZZ。
	src.byID[tenantID] = "zz"
	dir.Forget(tenantID)
	if sh, err := dir.ShardOf(ctx, tenantID); err != nil || sh != "ZZ" {
		t.Errorf("shard 列=\"zz\" 应归一化为 \"ZZ\"，收到 %q %v", sh, err)
	}
}

// TestDirectoryWarm 全量预热后不再回源。
func TestDirectoryWarm(t *testing.T) {
	src := newFakeSource()
	a := idgen.MustRandom(idgen.KindTenant)
	b := idgen.MustRandom(idgen.KindTenant)
	src.listed[a] = "03"
	src.listed[b] = "Z7"

	dir := NewDirectory(src)
	if err := dir.Warm(context.Background()); err != nil {
		t.Fatalf("Warm 失败: %v", err)
	}
	if dir.Len() != 2 {
		t.Fatalf("预热后应有 2 条，实际 %d", dir.Len())
	}

	if sh, err := dir.ShardOf(context.Background(), b); err != nil || sh != "Z7" {
		t.Fatalf("预热后取分片失败: %q %v", sh, err)
	}
	if got := src.calls.Load(); got != 0 {
		t.Fatalf("预热后查询不应回源，实际 %d 次", got)
	}
}

// TestDirectoryWarmFailsFastOnBadRow 宁可起不来，也不要带着错分片跑。
func TestDirectoryWarmFailsFastOnBadRow(t *testing.T) {
	src := newFakeSource()
	src.listed[idgen.MustRandom(idgen.KindTenant)] = "03"
	src.listed[idgen.MustRandom(idgen.KindTenant)] = ""

	dir := NewDirectory(src)
	err := dir.Warm(context.Background())
	if err == nil {
		t.Fatal("存在空分片的租户时 Warm 必须失败")
	}
	if !errors.Is(err, ErrNoShard) {
		t.Fatalf("Warm 错误应包成 ErrNoShard，收到 %v", err)
	}
	if dir.Len() != 0 {
		t.Fatalf("Warm 失败后不应留下半份缓存，实际 %d 条", dir.Len())
	}
}

// TestDirectoryWarmReplacesCache 预热是「替换」而不是「合并」。
// 否则已删除的租户会永远留在缓存里，继续接收流量。
func TestDirectoryWarmReplacesCache(t *testing.T) {
	src := newFakeSource()
	gone := idgen.MustRandom(idgen.KindTenant)
	src.byID[gone] = "03"

	dir := NewDirectory(src)
	if _, err := dir.ShardOf(context.Background(), gone); err != nil {
		t.Fatalf("首次查询失败: %v", err)
	}

	if err := dir.Warm(context.Background()); err != nil {
		t.Fatalf("Warm 失败: %v", err)
	}
	if dir.Len() != 0 {
		t.Fatalf("预热应清掉已不存在的租户，实际仍剩 %d 条", dir.Len())
	}
}

// TestDirectoryPutAndForget 覆盖写路径与缓存失效路径。
func TestDirectoryPutAndForget(t *testing.T) {
	dir := NewDirectory(nil)
	tenantID := idgen.MustRandom(idgen.KindTenant)

	if err := dir.Put(tenantID, "z7"); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	if sh, err := dir.ShardOf(context.Background(), tenantID); err != nil || sh != "Z7" {
		t.Fatalf("Put 后应为 \"Z7\"（小写输入、大写存储），收到 %q %v", sh, err)
	}

	dir.Forget(tenantID)
	if dir.Len() != 0 {
		t.Fatalf("Forget 后应为空，实际 %d 条", dir.Len())
	}

	if err := dir.Put(tenantID, "00"); err == nil {
		t.Fatal("Put 保留分片必须失败")
	}
	if err := dir.Put(idgen.Zero, "03"); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("Put 空租户应返回 ErrNoTenant，收到 %v", err)
	}
}

// TestDirectoryConcurrentFirstLookup 并发首查只回源一次（防缓存击穿）。
// 用 -race 跑这条才有意义。
func TestDirectoryConcurrentFirstLookup(t *testing.T) {
	src := newFakeSource()
	tenantID := idgen.MustRandom(idgen.KindTenant)
	src.byID[tenantID] = "03"

	dir := NewDirectory(src)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := dir.ShardOf(ctx, tenantID); err != nil {
				t.Errorf("并发 ShardOf 失败: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := src.calls.Load(); got != 1 {
		t.Fatalf("64 并发首查应只回源 1 次，实际 %d 次（缓存击穿）", got)
	}
}

// TestDirectoryNoSource 没有数据源时必须报 ErrNoSource，而不是返回空分片。
func TestDirectoryNoSource(t *testing.T) {
	dir := NewDirectory(nil)
	if _, err := dir.ShardOf(context.Background(), idgen.MustRandom(idgen.KindTenant)); !errors.Is(err, ErrNoSource) {
		t.Fatalf("无数据源应返回 ErrNoSource，收到 %v", err)
	}
	if err := dir.Warm(context.Background()); !errors.Is(err, ErrNoSource) {
		t.Fatalf("无数据源时 Warm 应返回 ErrNoSource，收到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// PoolSet
// ---------------------------------------------------------------------------

// TestPoolSetDefaultFallback 未注册的分片回落默认池。
//
// 池对象这里用非 nil 的哨兵值代替不了（类型是 *pgxpool.Pool），
// 因此这组测试只覆盖「错误路径」与 nil 安全性；
// 「非 nil 池返回正确对象」需要真实 pgxpool，属于建库自检验收。
func TestPoolSetRejectsReservedAndBadShard(t *testing.T) {
	set := NewPoolSet(nil)

	if _, err := set.Pool("00"); err == nil {
		t.Fatal("查询保留分片的池必须失败")
	} else if !errors.Is(err, idgen.ErrReservedShard) {
		t.Fatalf("应包成 idgen.ErrReservedShard，收到 %v", err)
	}
	if _, err := set.Pool("0U"); err == nil {
		t.Fatal("查询非法分片的池必须失败")
	}
	if err := set.Register("00", nil); err == nil {
		t.Fatal("注册保留分片必须失败")
	}
}

func TestPoolSetUnknownShardWhenNoDefault(t *testing.T) {
	set := NewPoolSet(nil)
	if _, err := set.Pool("03"); !errors.Is(err, ErrUnknownShard) {
		t.Fatalf("无默认池时应返回 ErrUnknownShard，收到 %v", err)
	}
	if _, err := set.PoolOfID(idgen.MustRandom(idgen.KindUser)); !errors.Is(err, ErrUnknownShard) {
		t.Fatalf("形态 A 在无默认池时应返回 ErrUnknownShard，收到 %v", err)
	}
}

// TestPoolSetPoolOfIDRejectsBadID 前缀打错时必须在选池阶段失败。
func TestPoolSetPoolOfIDRejectsBadID(t *testing.T) {
	set := NewPoolSet(nil)

	if _, err := set.PoolOfID(idgen.Zero); !errors.Is(err, ErrNoShard) {
		t.Fatalf("空 ID 应返回 ErrNoShard，收到 %v", err)
	}
	if _, err := set.PoolOfID("xyz_0000000000000000"); err == nil {
		t.Fatal("未登记前缀必须失败")
	}
	if _, err := set.PoolOfID("ses_03"); err == nil {
		t.Fatal("长度不对的 ID 必须失败")
	}
}

// TestPoolSetShardsSorted 注册顺序不影响输出顺序（启动日志需要稳定）。
func TestPoolSetShardsSorted(t *testing.T) {
	set := NewPoolSet(nil)
	if got := set.Shards(); len(got) != 0 {
		t.Fatalf("空集合应返回空切片，收到 %v", got)
	}

	// 直接操作内部 map：这里不引入假池，只验证排序逻辑。
	set.m["Z7"] = nil
	set.m["03"] = nil
	set.m["AB"] = nil

	got := set.Shards()
	want := []Shard{"03", "AB", "Z7"}
	if len(got) != len(want) {
		t.Fatalf("应有 %d 个分片，实际 %v", len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("分片应升序 %v，收到 %v", want, got)
		}
	}
}

// TestPoolSetCloseIsIdempotent 关闭两次不应 panic。
func TestPoolSetCloseIsIdempotent(t *testing.T) {
	set := NewPoolSet(nil)
	set.Close()
	set.Close()

	if set.Default() != nil {
		t.Fatal("Close 后默认池应为 nil")
	}
	if _, err := set.Pool("03"); !errors.Is(err, ErrUnknownShard) {
		t.Fatalf("Close 后查询应返回 ErrUnknownShard，收到 %v", err)
	}
}

// TestPoolSetPingNeedsAtLeastOnePool 没有任何池时 Ping 必须报错，
// 否则启动自检会「通过」一个根本连不上库的网关。
func TestPoolSetPingNeedsAtLeastOnePool(t *testing.T) {
	set := NewPoolSet(nil)
	if err := set.Ping(context.Background()); !errors.Is(err, ErrUnknownShard) {
		t.Fatalf("无池时 Ping 应返回 ErrUnknownShard，收到 %v", err)
	}
}

// TestOpenRejectsBadConfig 建池的配置校验路径（不真正连库）。
func TestOpenRejectsBadConfig(t *testing.T) {
	if _, err := Open("", nil, 4); !errors.Is(err, ErrUnknownShard) {
		t.Fatalf("全空配置应返回 ErrUnknownShard，收到 %v", err)
	}
	if _, err := Open("", map[string]string{"03": ""}, 4); err == nil {
		t.Fatal("分片 DSN 为空必须失败")
	}
	if _, err := Open("", map[string]string{"00": "postgres://x/y"}, 4); err == nil {
		t.Fatal("分片号 00 必须被拒绝")
	}
	if _, err := Open("", map[string]string{"0U": "postgres://x/y"}, 4); err == nil {
		t.Fatal("非法分片号必须被失败")
	}
}

// TestDirectoryImplementsSharder 编译期接口一致性。
func TestDirectoryImplementsSharder(t *testing.T) {
	var s Sharder = NewDirectory(nil)
	if _, err := s.ShardOf(context.Background(), idgen.Zero); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("应返回 ErrNoTenant，收到 %v", err)
	}
}

// TestDirectoryContextCancel 回源时把 ctx 原样传下去。
//
// 判据用 Deadline 而不是 value：Directory 若自己造一个
// context.Background() 去回源，超时控制就断了，这里能立刻发现。
func TestDirectoryContextCancel(t *testing.T) {
	src := &ctxSource{}
	dir := NewDirectory(src)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if _, err := dir.ShardOf(ctx, idgen.MustRandom(idgen.KindTenant)); err == nil {
		t.Fatal("数据源报错时应上抛")
	}
	if !src.sawCtx {
		t.Fatal("Directory 必须把 ctx 传给 Source（否则回源会无视超时）")
	}
}

type ctxSource struct {
	mu     sync.Mutex
	sawCtx bool
}

func (c *ctxSource) TenantShard(ctx context.Context, _ idgen.ID) (string, error) {
	if _, ok := ctx.Deadline(); ok {
		c.mu.Lock()
		c.sawCtx = true
		c.mu.Unlock()
	}
	return "", errors.New("boom")
}

func (c *ctxSource) TenantShards(context.Context) (map[idgen.ID]string, error) {
	return nil, errors.New("boom")
}
