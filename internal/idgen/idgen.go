// Package idgen 生成带类型前缀的业务 ID。
//
// ID 形态：所有表主键必须使用本包生成，禁止数据库自增。
// 租户与分片规则见租户与分片。完整说明见包内注释。
//
// # 两种合法形态
//
//	形态 A · 随机型（20 字符）  {3 字母前缀}_{16 位 Crockford Base32}
//	  例：usr_7K3M9QXZ2WVBN4TP
//	  用于目录型实体：tenants / users / api_keys / namespaces / plans
//	                / price_rules / admin_roles / admin_accounts
//	  特点：完全随机，不可遍历；不含时间信息，排序必须用 created_at。
//
//	形态 B · 分片型（19 字符）  {3 字母前缀}_{2 位分片}{13 位雪花 Base32}
//	  例：ses_03Z7K3M9QXZ2WV
//	  用于流水型实体：sessions / session_messages / usage_events / credit_ledger
//	                / billing_reservations / invoices / system_setting_revisions
//	                / admin_audit_logs / 对象存储引用
//	  特点：第 5~6 个字符就是分片号，路由时零查表；后 13 位是雪花值，可反解时间。
//
// # 字符集
//
// Crockford Base32：0123456789ABCDEFGHJKMNPQRSTVWXYZ
// 去掉了 I / L / O / U，避免肉眼与手抄混淆。输入大小写宽容
// （i/l 视为 1，o 视为 0），输出一律大写。
//
// # 分片号
//
// 分片号由租户在创建时确定，终生不变（租户与分片）——禁止用 hash(tenant_id) 推导，
// 否则算法一升级，同一租户的历史数据就会被算到另一个分片上。
// 本包只负责「把给定分片号编进 ID」和「从 ID 里读回分片号」，不负责分配。
// 分片分配属于 internal/shard.Directory 的职责。
//
// 分片号 "00" 保留给系统种子与全局配置，NewSharded 会拒绝它。
package idgen

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 形态与字符集
// ---------------------------------------------------------------------------

// Form 是 ID 的形态。
type Form uint8

const (
	// FormRandom 是形态 A：纯随机，目录型实体。
	FormRandom Form = iota
	// FormSharded 是形态 B：分片位 + 雪花，流水型实体。
	FormSharded

	// PrefixLen 是前缀（3 个字母）的长度。
	PrefixLen = 3
	// SepIndex 是分隔下划线的下标，同时也是业务体起始位置。
	SepIndex = 3
	// BodyStart 是业务体起始下标（= SepIndex + 1）。
	BodyStart = 4
	// RandomBodyLen 是形态 A 业务体长度。
	RandomBodyLen = 16
	// ShardLen 是分片号长度。
	ShardLen = 2
	// SnowBodyLen 是形态 B 中雪花段长度（13 * 5 = 65 位，足够放下 63 位雪花值）。
	SnowBodyLen = 13
	// ShardedBodyLen 是形态 B 业务体总长度。
	ShardedBodyLen = ShardLen + SnowBodyLen

	// RandomTotalLen 是形态 A 的总长度。
	RandomTotalLen = BodyStart + RandomBodyLen
	// ShardedTotalLen 是形态 B 的总长度。
	ShardedTotalLen = BodyStart + ShardedBodyLen

	// ReservedShard 是保留分片号，用于系统种子与全局配置。
	ReservedShard = "00"
)

// alphabet 是 Crockford Base32 的编码表。
const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// decodeTable 把字符映射到 5 位值。-1 表示非法字符。
var decodeTable [256]int8

func init() {
	for i := range decodeTable {
		decodeTable[i] = -1
	}
	for i := 0; i < len(alphabet); i++ {
		c := alphabet[i]
		decodeTable[c] = int8(i)
		if c >= 'A' && c <= 'Z' {
			// 小写宽容
			decodeTable[c+('a'-'A')] = int8(i)
		}
	}
	// Crockford 的四个「易混淆字符」宽容映射
	decodeTable['I'] = 1
	decodeTable['i'] = 1
	decodeTable['L'] = 1
	decodeTable['l'] = 1
	decodeTable['O'] = 0
	decodeTable['o'] = 0
}

// ---------------------------------------------------------------------------
// Kind 与前缀登记表
// ---------------------------------------------------------------------------

// Kind 是实体类别，取值即 ID 的前缀（3 个字母）。
type Kind string

// 形态 A：目录型实体。
//
// 判据是「这张表将来会不会大到需要单独一个库」。不会，就用形态 A：
// 无分片位、20 字符、纯随机体，永远留在中央库。
//
// rev_ / aud_ 归在此处而不是形态 B，是刻意的：
// system_setting_revisions 与 admin_audit_logs 是中央库的系统元数据表，
// 没有 tenant_id，也就不存在合法的非 00 分片。若把它们算形态 B，
// 则 NewSharded 会因拒绝保留分片 00 而让这两张表无 ID 可造；
// 若给它们加一个恒为 00 的生成列，又等于用分片机制表达「我没有分片」，
// 是纯粹的形式负担。
const (
	KindTenant       Kind = "tnt"
	KindUser         Kind = "usr"
	KindAPIKey       Kind = "key"
	KindNamespace    Kind = "nsp"
	KindPlan         Kind = "pln"
	KindPriceRule    Kind = "prc"
	KindAdminRole    Kind = "rol"
	KindAdminAccoun  Kind = "adm"
	KindAdminSession Kind = "asn"
	KindSettingRev   Kind = "rev"
	KindAuditLog     Kind = "aud"
)

// 形态 B：流水型实体。
//
// 只有这类表才携带分片位（19 字符），也只有这类表需要
// shard TEXT GENERATED ALWAYS AS (substring(id FROM 5 FOR 2)) STORED。
const (
	KindSession        Kind = "ses"
	KindSessionMessage Kind = "msg"
	KindUsageEvent     Kind = "uev"
	KindLedger         Kind = "led"
	KindReservation    Kind = "rsv"
	KindInvoice        Kind = "inv"
	KindObject         Kind = "obj"
)

// spec 是每个 Kind 的静态规格。
type spec struct {
	form Form
	desc string
}

// registry 是前缀登记表。新增 Kind 必须同时在此登记，
// 否则 NewRandom / NewSharded 会返回 ErrUnknownKind。
//
// 前缀与形态是**一对一**的：同一个前缀不允许既是随机型又是分片型。
var registry = map[Kind]spec{
	// 形态 A
	KindTenant:       {FormRandom, "租户（数据边界）"},
	KindUser:         {FormRandom, "终端用户"},
	KindAPIKey:       {FormRandom, "API Key"},
	KindNamespace:    {FormRandom, "命名空间（一个 MCP 服务）"},
	KindPlan:         {FormRandom, "套餐"},
	KindPriceRule:    {FormRandom, "计价规则"},
	KindAdminRole:    {FormRandom, "管理员角色"},
	KindAdminAccoun:  {FormRandom, "管理员账号"},
	KindAdminSession: {FormRandom, "管理员会话"},
	KindSettingRev:   {FormRandom, "配置版本"},
	KindAuditLog:     {FormRandom, "审计日志"},

	// 形态 B
	KindSession:        {FormSharded, "会话"},
	KindSessionMessage: {FormSharded, "会话消息"},
	KindUsageEvent:     {FormSharded, "用量事件"},
	KindLedger:         {FormSharded, "账本分录"},
	KindReservation:    {FormSharded, "预扣单"},
	KindInvoice:        {FormSharded, "发票"},
	KindObject:         {FormSharded, "对象存储引用"},
}

// FormOf 返回某个 Kind 的形态。未知 Kind 返回 false。
func FormOf(k Kind) (Form, bool) {
	s, ok := registry[k]
	if !ok {
		return 0, false
	}
	return s.form, true
}

// Kinds 返回全部已登记的 Kind，便于测试与文档生成。
func Kinds() []Kind {
	out := make([]Kind, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
}

// ---------------------------------------------------------------------------
// 错误
// ---------------------------------------------------------------------------

var (
	// ErrNotInitialized 表示在 Init 之前调用了生成函数。
	ErrNotInitialized = errors.New("idgen 未初始化，请先调用 idgen.Init")
	// ErrUnknownKind 表示 Kind 未在前缀登记表中登记。
	ErrUnknownKind = errors.New("未登记的 ID 前缀")
	// ErrReservedShard 表示使用了保留分片号 "00"。
	ErrReservedShard = errors.New("分片号 00 为保留值，不可用于业务 ID")
	// ErrBadFormat 表示 ID 字符串格式不合法。
	ErrBadFormat = errors.New("ID 格式不合法")
	// ErrKindMismatch 表示 ID 前缀与期望的 Kind 不一致。
	ErrKindMismatch = errors.New("ID 前缀与期望类型不一致")
)

// ---------------------------------------------------------------------------
// ID 类型
// ---------------------------------------------------------------------------

// ID 是带类型前缀的业务 ID。底层是 string，零值为 ""。
type ID string

// Zero 是 ID 的空值。业务上表示「未设置」。
const Zero ID = ""

// String 返回字符串本身。
func (id ID) String() string { return string(id) }

// IsZero 判断是否为空。
func (id ID) IsZero() bool { return id == "" }

// Kind 返回 ID 的前缀所对应的 Kind。非法 ID 返回 ""。
func (id ID) Kind() Kind {
	if len(id) < BodyStart {
		return ""
	}
	if id[SepIndex] != '_' {
		return ""
	}
	k := Kind(id[:SepIndex])
	if _, ok := registry[k]; !ok {
		return ""
	}
	return k
}

// Form 返回 ID 的形态。非法 ID 返回 false。
func (id ID) Form() (Form, bool) {
	k := id.Kind()
	if k == "" {
		return 0, false
	}
	return registry[k].form, true
}

// Shard 返回形态 B 的分片号。形态 A 或非法 ID 返回 ""。
func (id ID) Shard() string {
	f, ok := id.Form()
	if !ok || f != FormSharded {
		return ""
	}
	if len(id) != ShardedTotalLen {
		return ""
	}
	return string(id[BodyStart : BodyStart+ShardLen])
}

// CreatedAt 从形态 B 的雪花段还原生成时刻。
// 形态 A 不含时间信息，返回 ok=false。
func (id ID) CreatedAt() (time.Time, bool) {
	f, ok := id.Form()
	if !ok || f != FormSharded {
		return time.Time{}, false
	}
	if len(id) != ShardedTotalLen {
		return time.Time{}, false
	}
	v, ok := decode(string(id[BodyStart+ShardLen:]))
	if !ok {
		return time.Time{}, false
	}
	ms := int64(v>>timeShift) + epochMillis
	return time.UnixMilli(ms).UTC(), true
}

// Snowflake 返回形态 B 中的雪花原始值。形态 A 返回 false。
func (id ID) Snowflake() (uint64, bool) {
	f, ok := id.Form()
	if !ok || f != FormSharded {
		return 0, false
	}
	if len(id) != ShardedTotalLen {
		return 0, false
	}
	return decode(string(id[BodyStart+ShardLen:]))
}

// MarshalJSON 输出字符串。ID 底层就是字符串，这里只是显式声明契约 JSON 字符串契约。
func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(id))
}

// UnmarshalJSON 只接受字符串。数字一律拒绝——那正是ID 形态要消灭的东西。
func (id *ID) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*id = Zero
		return nil
	}
	if data[0] != '"' {
		return fmt.Errorf("%w: ID 必须是 JSON 字符串，收到 %s", ErrBadFormat, data)
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("%w: 解析 ID 字符串失败: %v", ErrBadFormat, err)
	}
	parsed, err := Parse(s)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// Value 实现 driver.Valuer，写入 TEXT 列。
func (id ID) Value() (driver.Value, error) {
	if id == "" {
		return nil, nil
	}
	return string(id), nil
}

// Scan 实现 sql.Scanner，从 TEXT 列读回。
// 空值读成 Zero，不做格式校验——历史数据与系统种子可能早于本包版本。
func (id *ID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*id = Zero
		return nil
	case string:
		*id = ID(v)
		return nil
	case []byte:
		*id = ID(string(v))
		return nil
	default:
		return fmt.Errorf("无法将 %T 转换为 idgen.ID", src)
	}
}

// ---------------------------------------------------------------------------
// 雪花生成器（形态 B 的时间唯一性来源）
// ---------------------------------------------------------------------------

const (
	// epochMillis 是本项目的纪元起点：2024-01-01T00:00:00Z。
	// 从此刻起算 41 位毫秒可用约 69 年（至 2093 年）。
	epochMillis int64 = 1704067200000

	nodeBits  uint8 = 10
	stepBits  uint8 = 12
	nodeMax         = int64(1)<<nodeBits - 1 // 1023
	stepMax         = int64(1)<<stepBits - 1 // 4095
	nodeShift       = stepBits
	timeShift       = nodeBits + stepBits
)

var (
	mu       sync.Mutex
	nodeID   int64 = -1
	lastTime int64
	step     int64
	inited   bool
)

// Init 初始化雪花生成器。nodeID 取值 0-1023，多实例部署必须互不相同。
// 重复调用且 nodeID 相同是安全的；nodeID 不同则返回错误，避免误配置。
func Init(id int64) error {
	if id < 0 || id > nodeMax {
		return fmt.Errorf("idgen nodeID 必须在 0-%d 之间，收到 %d", nodeMax, id)
	}

	mu.Lock()
	defer mu.Unlock()

	if inited {
		if nodeID == id {
			return nil
		}
		return fmt.Errorf("idgen 已用 nodeID=%d 初始化，不可改为 %d", nodeID, id)
	}

	nodeID = id
	inited = true
	return nil
}

// nextSnowflake 生成下一个 64 位雪花值。
//
// 时钟回拨策略：若当前时间早于上次生成时间，等待追平而非报错，
// 避免容器时间同步导致的偶发失败；回拨超过 5ms 则报错，
// 说明时钟严重异常，继续生成会破坏单调性。
func nextSnowflake() (uint64, error) {
	mu.Lock()
	defer mu.Unlock()

	if !inited {
		return 0, ErrNotInitialized
	}

	now := time.Now().UnixMilli()
	if now < lastTime {
		drift := lastTime - now
		if drift > 5 {
			return 0, fmt.Errorf("系统时钟回拨 %dms，拒绝生成 ID 以防重复", drift)
		}
		for now <= lastTime {
			now = time.Now().UnixMilli()
		}
	}

	if now == lastTime {
		step = (step + 1) & stepMax
		if step == 0 {
			// 当前毫秒序列耗尽，等到下一毫秒
			for now <= lastTime {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		step = 0
	}

	lastTime = now

	v := (uint64(now-epochMillis) << timeShift) | (uint64(nodeID) << nodeShift) | uint64(step)
	if v == 0 {
		return 0, errors.New("生成的雪花值为 0，请检查纪元配置")
	}
	return v, nil
}

// ---------------------------------------------------------------------------
// 生成
// ---------------------------------------------------------------------------

// NewRandom 生成形态 A 的 ID。仅适用于目录型 Kind。
func NewRandom(k Kind) (ID, error) {
	s, ok := registry[k]
	if !ok {
		return Zero, fmt.Errorf("%w: %q", ErrUnknownKind, k)
	}
	if s.form != FormRandom {
		return Zero, fmt.Errorf("Kind %q 是分片型，请改用 NewSharded", k)
	}

	body, err := randomBody(RandomBodyLen)
	if err != nil {
		return Zero, err
	}
	return ID(string(k) + "_" + body), nil
}

// MustRandom 是 NewRandom 的 panic 版本，仅用于测试与常量构造路径。
func MustRandom(k Kind) ID {
	id, err := NewRandom(k)
	if err != nil {
		panic(err)
	}
	return id
}

// NewSharded 生成形态 B 的 ID。仅适用于流水型 Kind。
//
// shard 必须是 2 个 Crockford Base32 字符且不为 "00"。
// 分片号由租户创建时确定、终生不变（租户与分片），调用方从
// internal/shard.Directory 取得，不得在此处自行推算。
func NewSharded(k Kind, shard string) (ID, error) {
	s, ok := registry[k]
	if !ok {
		return Zero, fmt.Errorf("%w: %q", ErrUnknownKind, k)
	}
	if s.form != FormSharded {
		return Zero, fmt.Errorf("Kind %q 是随机型，请改用 NewRandom", k)
	}
	if err := ValidateShard(shard); err != nil {
		return Zero, err
	}

	v, err := nextSnowflake()
	if err != nil {
		return Zero, err
	}

	return ID(string(k) + "_" + normalize(shard) + encode(v, SnowBodyLen)), nil
}

// MustSharded 是 NewSharded 的 panic 版本。
func MustSharded(k Kind, shard string) ID {
	id, err := NewSharded(k, shard)
	if err != nil {
		panic(err)
	}
	return id
}

// ---------------------------------------------------------------------------
// 解析与校验
// ---------------------------------------------------------------------------

// Parse 校验并规范化一个 ID 字符串。
//
// 规范化只作用于业务体（第 4 位之后）：转大写、i/l→1、o→0。
// 前缀保持小写原样——它是对外的稳定记号，大小写翻转会让
// 同一个 ID 出现两种写法，日志与 URL 里就无法直接字符串比较。
func Parse(s string) (ID, error) {
	if err := Validate(s); err != nil {
		return Zero, err
	}
	k := string(s[:SepIndex])
	return ID(k + "_" + normalize(s[BodyStart:])), nil
}

// Validate 校验 ID 是否为合法形态，不关心具体 Kind。
func Validate(s string) error {
	if len(s) != RandomTotalLen && len(s) != ShardedTotalLen {
		return fmt.Errorf("%w: 长度必须是 %d 或 %d，收到 %d（%q）",
			ErrBadFormat, RandomTotalLen, ShardedTotalLen, len(s), s)
	}
	if s[SepIndex] != '_' {
		return fmt.Errorf("%w: 第 %d 个字符必须是下划线（%q）", ErrBadFormat, SepIndex+1, s)
	}

	k := Kind(s[:SepIndex])
	sp, ok := registry[k]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownKind, k)
	}

	body := s[BodyStart:]
	switch sp.form {
	case FormRandom:
		if len(s) != RandomTotalLen {
			return fmt.Errorf("%w: 前缀 %q 是随机型，总长必须为 %d，收到 %d（%q）",
				ErrBadFormat, k, RandomTotalLen, len(s), s)
		}
	case FormSharded:
		if len(s) != ShardedTotalLen {
			return fmt.Errorf("%w: 前缀 %q 是分片型，总长必须为 %d，收到 %d（%q）",
				ErrBadFormat, k, ShardedTotalLen, len(s), s)
		}
		if err := ValidateShard(body[:ShardLen]); err != nil {
			return err
		}
	}

	if _, ok := decode(body); !ok {
		return fmt.Errorf("%w: 业务体含非 Crockford Base32 字符（%q）", ErrBadFormat, s)
	}
	return nil
}

// ValidateOf 在 Validate 基础上再校验 Kind 是否匹配。
// 入参校验用这个，别用 Validate 之后自己比前缀。
func ValidateOf(k Kind, s string) error {
	if err := Validate(s); err != nil {
		return err
	}
	if got := Kind(s[:SepIndex]); got != k {
		return fmt.Errorf("%w: 期望 %q，收到 %q（%q）", ErrKindMismatch, k, got, s)
	}
	return nil
}

// ValidateShard 校验分片号。
func ValidateShard(shard string) error {
	if len(shard) != ShardLen {
		return fmt.Errorf("%w: 分片号长度必须是 %d，收到 %d（%q）",
			ErrBadFormat, ShardLen, len(shard), shard)
	}
	norm := normalize(shard)
	if norm == ReservedShard {
		return fmt.Errorf("%w（收到 %q）", ErrReservedShard, shard)
	}
	if _, ok := decode(norm); !ok {
		return fmt.Errorf("%w: 分片号含非 Crockford Base32 字符（%q）", ErrBadFormat, shard)
	}
	return nil
}

// ShardOf 从形态 B 的 ID 里读出分片号，不做完整校验。
// 用于热路径：拿到一个已知合法的 ID 时零分配开销地取分片。
func ShardOf(id ID) string {
	if len(id) != ShardedTotalLen {
		return ""
	}
	return string(id[BodyStart : BodyStart+ShardLen])
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// randomBody 用 crypto/rand 生成 n 个 Crockford Base32 字符。
func randomBody(n int) (string, error) {
	buf := make([]byte, 10) // 80 位，足够 16 个字符
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("生成随机 ID 失败: %w", err)
	}

	var v uint64
	for _, b := range buf {
		v = v<<8 | uint64(b)
	}

	// 从高位起取 5 位一组。80 位正好 16 组，不会浪费。
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		shift := uint((n - 1 - i) * 5)
		out[i] = alphabet[(v>>shift)&31]
	}
	return string(out), nil
}

// encode 把 v 编码成 n 个字符（高位在前，不足补 0）。
func encode(v uint64, n int) string {
	out := make([]byte, n)
	for i := n - 1; i >= 0; i-- {
		out[i] = alphabet[v&31]
		v >>= 5
	}
	return string(out)
}

// decode 把字符串解成整数。含非法字符返回 false。
func decode(s string) (uint64, bool) {
	var v uint64
	for i := 0; i < len(s); i++ {
		d := decodeTable[s[i]]
		if d < 0 {
			return 0, false
		}
		v = v<<5 | uint64(d)
	}
	return v, true
}

// normalize 把 ID 规范化成大写，并把 i/l/o 换成 1/1/0。
func normalize(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case 'i', 'I', 'l', 'L':
			b[i] = '1'
		case 'o', 'O':
			b[i] = '0'
		default:
			if c >= 'a' && c <= 'z' {
				b[i] = c - ('a' - 'A')
			} else {
				b[i] = c
			}
		}
	}
	return string(b)
}
