package idgen

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

// 这两种正则就是数据库 CHECK 约束的镜像。任何一处改动必须同步两处。
var (
	reRandom  = regexp.MustCompile(`^usr_[0-9A-HJKMNP-TV-Z]{16}$`)
	reSharded = regexp.MustCompile(`^ses_[0-9A-HJKMNP-TV-Z]{2}[0-9A-HJKMNP-TV-Z]{13}$`)
)

func TestMain(m *testing.M) {
	if err := Init(1); err != nil {
		panic("idgen.Init 失败: " + err.Error())
	}
	m.Run()
}

// ---------------------------------------------------------------------------
// 覆盖：形态 A 随机型
// ---------------------------------------------------------------------------

func TestNewRandomFormat(t *testing.T) {
	seen := make(map[ID]struct{}, 10000)
	for i := 0; i < 10000; i++ {
		id, err := NewRandom(KindUser)
		if err != nil {
			t.Fatalf("第 %d 次生成失败: %v", i, err)
		}
		if !reRandom.MatchString(id.String()) {
			t.Fatalf("形态不符：%q 不匹配 %s", id, reRandom)
		}
		if len(id) != RandomTotalLen {
			t.Fatalf("长度应为 %d，收到 %d（%q）", RandomTotalLen, len(id), id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("10000 次生成出现重复：%q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewRandomRejectsShardedKind(t *testing.T) {
	if _, err := NewRandom(KindSession); err == nil {
		t.Fatal("KindSession 是分片型，NewRandom 应当报错")
	}
	if _, err := NewRandom(Kind("zzz")); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("未登记前缀应返回 ErrUnknownKind，收到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// 覆盖：形态 B 分片型
// ---------------------------------------------------------------------------

func TestNewShardedFormat(t *testing.T) {
	seen := make(map[ID]struct{}, 10000)
	for i := 0; i < 10000; i++ {
		id, err := NewSharded(KindSession, "03")
		if err != nil {
			t.Fatalf("第 %d 次生成失败: %v", i, err)
		}
		if !reSharded.MatchString(id.String()) {
			t.Fatalf("形态不符：%q 不匹配 %s", id, reSharded)
		}
		if len(id) != ShardedTotalLen {
			t.Fatalf("长度应为 %d，收到 %d（%q）", ShardedTotalLen, len(id), id)
		}
		if got := id.Shard(); got != "03" {
			t.Fatalf("分片号应为 03，收到 %q（%q）", got, id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("10000 次生成出现重复：%q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewShardedShardPositions(t *testing.T) {
	// 分片位必须固定在下标 4、5。这是 DDL 里 substring(id FROM 5 FOR 2) 依据的约定，
	// 一旦挪动，22 张表的生成列全部失效。
	const shard = "1Z"
	id, err := NewSharded(KindLedger, shard)
	if err != nil {
		t.Fatal(err)
	}
	if id[4:6] != shard {
		t.Fatalf("下标 4..6 应为 %q，实际 %q（%q）", shard, id[4:6], id)
	}
	if ShardOf(id) != shard {
		t.Fatalf("ShardOf 应返回 %q，实际 %q", shard, ShardOf(id))
	}
}

func TestNewShardedRejectsReservedAndBadShard(t *testing.T) {
	cases := []struct {
		shard string
		want  error
	}{
		{"00", ErrReservedShard},
		{"OO", ErrReservedShard}, // 宽容映射后仍是 00
		{"0", ErrBadFormat},      // 长度 1
		{"000", ErrBadFormat},    // 长度 3
		{"0$", ErrBadFormat},     // 非法字符
	}
	for _, c := range cases {
		_, err := NewSharded(KindSession, c.shard)
		if !errors.Is(err, c.want) {
			t.Errorf("分片号 %q 应返回 %v，收到 %v", c.shard, c.want, err)
		}
	}
}

func TestNewShardedRejectsRandomKind(t *testing.T) {
	if _, err := NewSharded(KindUser, "03"); err == nil {
		t.Fatal("KindUser 是随机型，NewSharded 应当报错")
	}
}

// ---------------------------------------------------------------------------
// 覆盖：解析与校验
// ---------------------------------------------------------------------------

func TestParseRoundTrip(t *testing.T) {
	rnd, _ := NewRandom(KindNamespace)
	shd, _ := NewSharded(KindSessionMessage, "0A")

	for _, id := range []ID{rnd, shd} {
		parsed, err := Parse(id.String())
		if err != nil {
			t.Fatalf("Parse(%q) 失败: %v", id, err)
		}
		if parsed != id {
			t.Fatalf("往返不一致：%q → %q", id, parsed)
		}
	}
}

func TestParseTolerantInput(t *testing.T) {
	// 输入全小写 + 易混淆字符（i/l/o）都应被接受，
	// 规范化只作用于业务体，前缀保持小写。
	original := MustRandom(KindUser)
	lower := strings.ToLower(string(original))

	parsed, err := Parse(lower)
	if err != nil {
		t.Fatalf("小写输入应被接受，收到 %v", err)
	}
	if parsed != original {
		t.Fatalf("规范化应还原成 %q，收到 %q", original, parsed)
	}
	if parsed.Kind() != KindUser {
		t.Fatalf("前缀解析错误：%q", parsed)
	}
}

// canonical 复刻 Parse 的规范化契约：前缀小写原样，业务体大写化。
func canonical(s string) ID {
	if len(s) < BodyStart {
		return ID(s)
	}
	return ID(strings.ToLower(s[:SepIndex]) + "_" + normalize(s[BodyStart:]))
}

func TestValidateRejects(t *testing.T) {
	good := MustRandom(KindUser)
	goodShd := MustSharded(KindSession, "03")

	cases := []struct {
		name string
		in   string
		want error
	}{
		{"空串", "", ErrBadFormat},
		{"纯数字", "1846230000000000001", ErrBadFormat},
		{"无前缀", "abc", ErrBadFormat},
		{"未登记前缀", "zzz_0123456789ABCDEF", ErrUnknownKind},
		{"缺下划线", "usrX0123456789ABCDEF", ErrBadFormat},
		{"随机型长度错", string(good)[:len(good)-1], ErrBadFormat},
		{"随机型用分片型长度", string(good) + "ABC", ErrBadFormat},
		{"分片型长度错", string(goodShd)[:len(goodShd)-1], ErrBadFormat},
		{"分片型配随机型长度", string(goodShd) + "ABCDEFGHIJKLMNOPQRSTUV", ErrBadFormat},
		{"含非法字符 U", "usr_0123456789ABCDE" + "U", ErrBadFormat},
		{"分片型保留分片", "ses_00" + string(goodShd[6:]), ErrReservedShard},
	}
	for _, c := range cases {
		err := Validate(c.in)
		if c.want == nil {
			if err != nil {
				t.Errorf("%s：期望通过，收到 %v", c.name, err)
			}
			continue
		}
		if !errors.Is(err, c.want) {
			t.Errorf("%s：期望 %v，收到 %v", c.name, c.want, err)
		}
	}
}

func TestValidateOfKindMismatch(t *testing.T) {
	usr := MustRandom(KindUser)
	if err := ValidateOf(KindUser, string(usr)); err != nil {
		t.Fatalf("同类型校验应通过，收到 %v", err)
	}
	if err := ValidateOf(KindTenant, string(usr)); !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("类型不匹配应返回 ErrKindMismatch，收到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// 覆盖：时间反解与形态区分
// ---------------------------------------------------------------------------

func TestCreatedAt(t *testing.T) {
	id, err := NewSharded(KindSession, "05")
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := id.CreatedAt()
	if !ok {
		t.Fatal("形态 B 应能反解出时间")
	}
	if d := time.Since(ts); d > 5*time.Second || d < -5*time.Second {
		t.Fatalf("反解时间偏离过大：%v（偏差 %v）", ts, d)
	}

	if _, ok := MustRandom(KindUser).CreatedAt(); ok {
		t.Fatal("形态 A 不含时间信息，应返回 ok=false")
	}
}

func TestFormDistinguishes(t *testing.T) {
	if f, ok := MustRandom(KindUser).Form(); !ok || f != FormRandom {
		t.Fatalf("usr_ 应为 FormRandom，收到 %v/%v", f, ok)
	}
	if f, ok := MustSharded(KindSession, "03").Form(); !ok || f != FormSharded {
		t.Fatalf("ses_ 应为 FormSharded，收到 %v/%v", f, ok)
	}
	if _, ok := ID("bogus").Form(); ok {
		t.Fatal("非法 ID 不应返回形态")
	}
}

// ---------------------------------------------------------------------------
// 覆盖：JSON 与 SQL 适配
// ---------------------------------------------------------------------------

func TestJSONIsStringAndRejectsNumber(t *testing.T) {
	id := MustSharded(KindLedger, "0B")
	raw, err := json.Marshal(struct {
		ID ID `json:"id"`
	}{id})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"` + string(id) + `"}`
	if string(raw) != want {
		t.Fatalf("JSON 输出应为 %s，收到 %s", want, raw)
	}

	// 必须拒绝数字：这正是ID 形态要消灭的形式。
	var got struct {
		ID ID `json:"id"`
	}
	if err := json.Unmarshal([]byte(`{"id":1846230000000000001}`), &got); !errors.Is(err, ErrBadFormat) {
		t.Fatalf("数字型 ID 应被拒绝，收到 %v", err)
	}
	if err := json.Unmarshal([]byte(`{"id":null}`), &got); err != nil || !got.ID.IsZero() {
		t.Fatalf("null 应解成 Zero，收到 %v / %q", err, got.ID)
	}
}

func TestValueAndScan(t *testing.T) {
	id := MustRandom(KindAPIKey)

	v, err := id.Value()
	if err != nil {
		t.Fatal(err)
	}
	if v != string(id) {
		t.Fatalf("Value 应返回字符串，收到 %v", v)
	}

	var back ID
	if err := back.Scan(v); err != nil {
		t.Fatal(err)
	}
	if back != id {
		t.Fatalf("Scan 往返不一致：%q → %q", id, back)
	}

	if err := back.Scan([]byte(string(id))); err != nil || back != id {
		t.Fatalf("[]byte 扫描失败：%v / %q", err, back)
	}

	if err := back.Scan(nil); err != nil || !back.IsZero() {
		t.Fatalf("nil 应解成 Zero：%v / %q", err, back)
	}

	if err := back.Scan(int64(1)); err == nil {
		t.Fatal("整数不应被 Scan 接受")
	}

	// Zero 写入数据库应为 NULL
	if zv, _ := Zero.Value(); zv != nil {
		t.Fatalf("Zero 的 Value 应为 nil，收到 %v", zv)
	}
}

// ---------------------------------------------------------------------------
// 覆盖：前缀登记表自洽性
// ---------------------------------------------------------------------------

func TestRegistryConsistency(t *testing.T) {
	seenPrefix := make(map[Kind]struct{}, len(registry))
	seenDesc := make(map[string]Kind, len(registry))

	for k, sp := range registry {
		if len(k) != PrefixLen {
			t.Errorf("前缀 %q 长度应为 %d", k, PrefixLen)
		}
		// 前缀是语义记号而不是 Base32 数据：usr / aud / uev 含 'u' 是合法的，
		// 因为 Crockford 的禁用字符只约束业务体，不约束前缀。
		// 这里只要求前缀是 3 位小写 ASCII 字母。
		for i := 0; i < len(k); i++ {
			if k[i] < 'a' || k[i] > 'z' {
				t.Errorf("前缀 %q 第 %d 位必须是 a-z 小写字母，收到 %q", k, i+1, k[i])
			}
		}
		if _, dup := seenPrefix[k]; dup {
			t.Errorf("前缀重复登记：%q", k)
		}
		seenPrefix[k] = struct{}{}

		if sp.desc == "" {
			t.Errorf("前缀 %q 缺少说明", k)
		}
		if prev, dup := seenDesc[sp.desc]; dup {
			t.Errorf("说明重复：%q 与 %q 都是 %q", prev, k, sp.desc)
		}
		seenDesc[sp.desc] = k
	}

	// 每个登记项都必须能真正生成出合法 ID
	for k := range registry {
		f, _ := FormOf(k)
		var id ID
		var err error
		if f == FormRandom {
			id, err = NewRandom(k)
		} else {
			id, err = NewSharded(k, "07")
		}
		if err != nil {
			t.Errorf("Kind %q 生成失败: %v", k, err)
			continue
		}
		if err := ValidateOf(k, id.String()); err != nil {
			t.Errorf("Kind %q 生成的 ID %q 校验失败: %v", k, id, err)
		}
	}
}
