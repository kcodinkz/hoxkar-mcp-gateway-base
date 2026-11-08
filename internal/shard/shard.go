// Package shard 负责「租户 → 分片」的粘性映射，以及「分片 → 连接池」的寻址。
//
// 租户与分片：分片号在租户创建时确定，终生不变，禁止用
// hash(tenant_id) 之类的算法推导——算法一升级，同一租户的历史数据
// 就会被算到另一个分片上，而已经写下的 ID 里印着旧分片号，数据永久错位。
//
// 本包不生成 ID（那是 internal/idgen 的事），也不决定租户落在哪个分片
// （那是租户创建流程的事：它挑一个分片后调 Directory.Put 落地）。
// 本包只回答两个问题：
//
//	这个租户在哪个分片？      Directory.ShardOf
//	这个分片的连接池是哪个？  PoolSet.Pool
//
// # 零查表路由
//
// 形态 B 的 ID（ses_/msg_/led_ …）第 5~6 个字符就是分片号，
// 所以「拿到一个流水 ID，要知道去哪个库读它」不需要任何查表，
// 用 OfID 直接从字符串里读即可。只有「拿到租户 ID，要知道它的流水在
// 哪个库」才需要 Directory，而 Directory 是内存缓存。
//
// # 保留分片 00
//
// "00" 留给系统种子与全局配置。本包一律拒绝它：不和稀泥、不静默改写。
// 之所以连连接池都不给 00 注册：种子数据全部是形态 A（无分片位），
// 天然落在默认池里，没有哪个合法流水行会带 00。将来真要出现 00 分片的
// 流水行，先改 ID 体系设计，再改这里——顺序不能反。
package shard

import (
	"errors"
	"fmt"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// Shard 是两位 Crockford Base32 分片号，例如 "03"、"Z7"。
//
// 用独立类型而不是裸 string：分片号与租户 ID 都是字符串，
// 混着传编译器不会拦，出事时已经是数据写错库了。
type Shard string

// String 返回分片号本身。
func (s Shard) String() string { return string(s) }

// 错误。调用方用 errors.Is 判别，不要比字符串。
var (
	// ErrNoShard 表示租户存在但没有可用分片（列为空，或为保留值 00）。
	ErrNoShard = errors.New("租户没有可用分片")
	// ErrNoTenant 表示传入了空的租户 ID。
	ErrNoTenant = errors.New("租户 ID 为空")
	// ErrNoSource 表示 Directory 没有配置数据源，无法回源。
	ErrNoSource = errors.New("未配置租户分片数据源")
	// ErrUnknownShard 表示该分片既未注册连接池，也没有默认池可回落。
	ErrUnknownShard = errors.New("分片没有可用的连接池")
)

// Parse 校验并规范化一个分片号。
//
// 规范化规则与 idgen 完全一致（大小写宽容、i/l→1、o→0、输出大写），
// 因为分片号本身就是 ID 业务体的一段：两处规则一旦分叉，
// 就会出现「ID 里写 03、缓存键算成 O3」这种只在高并发下暴露的错分片。
// shard_test.go 用全部 1024 个取值逐一比对 idgen，把这条钉死。
func Parse(s string) (Shard, error) {
	if err := idgen.ValidateShard(s); err != nil {
		return "", err
	}
	buf := []byte(s)
	for i, c := range buf {
		switch c {
		case 'i', 'I', 'l', 'L':
			c = '1'
		case 'o', 'O':
			c = '0'
		}
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i] = c
	}
	return Shard(buf), nil
}

// MustParse 是 Parse 的 panic 版本，只用于常量与测试。
func MustParse(s string) Shard {
	sh, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return sh
}

// Validate 校验分片号是否合法（不含保留值 00）。
func Validate(s Shard) error {
	_, err := Parse(string(s))
	return err
}

// OfID 从形态 B 的 ID 里直接读出分片号，不查库、不分配。
//
// 这是路由热路径：拿到 ses_03XXXX… 就知道该去 03 号池。
// 形态 A（目录型，无分片位）或非法 ID 返回 false。
func OfID(id idgen.ID) (Shard, bool) {
	raw := idgen.ShardOf(id)
	if raw == "" {
		return "", false
	}
	return Shard(raw), true
}

// fromColumn 把 tenants.shard 列的原始值转成分片号。
//
// 与 Parse 的区别只在空值的语义：列可以为空（历史租户、或建租户时漏写），
// 那是「数据有问题」，要报 ErrNoShard 而不是 ErrBadFormat，运维才看得懂。
func fromColumn(tenantID idgen.ID, raw string) (Shard, error) {
	if raw == "" {
		return "", fmt.Errorf("%w: 租户 %q 的 tenants.shard 为空，请先分配分片", ErrNoShard, tenantID)
	}
	sh, err := Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: 租户 %q 的 tenants.shard=%q 不合法: %v", ErrNoShard, tenantID, raw, err)
	}
	return sh, nil
}
