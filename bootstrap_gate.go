package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// 本文件只服务建库自检那一次「不可回头」的建库动作，以及建库后的一组结构自检。
//
// 为什么单独一个文件：main.go 是编排，读的人应该一屏看到「配置 → 建池 →
// 迁移 → 装配组件 → 起服务」。把「清整个库」的闸门塞进主流程会让它变得
// 难以审阅，而这里恰恰是最需要被逐行读一遍的地方。

// confirmResetSchema 是 -reset-schema 的准入闸门。
//
// 三道门，缺一不可：
//  1. -confirm-reset 必须正好是 yes（复制粘贴历史命令时很难顺带蒙对）
//  2. DSN 主机必须是回环地址（localhost / 127.0.0.1 / ::1）
//  3. DSN 里必须指明数据库名
//
// 第 2 条是硬约束：resetSchema 的实现里是 DROP SCHEMA public CASCADE，
// 没有任何「只清某个前缀」的保护。连到远程库就等于把生产数据一次清空。
// 只靠命令行参数的自律不够 —— 生产配置里那条 DSN 通常就在同一个 shell
// 的历史记录里，所以这条检查必须写死在代码里。
func confirmResetSchema(dsn, confirm string) error {
	if confirm != "yes" {
		return errors.New(
			"拒绝清库：-reset-schema 需要同时传 -confirm-reset yes。" +
				"该参数会 DROP SCHEMA public CASCADE，请确认连的是本机开发库")
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("拒绝清库：DSN 无法解析（%v）", err)
	}

	host := u.Hostname()
	if !isLoopbackHost(host) {
		return fmt.Errorf(
			"拒绝清库：DSN 主机为 %q，只允许 localhost / 127.0.0.1 / ::1。"+
				"该开关会清空整个 public schema，不能对非本机数据库执行", host)
	}

	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return errors.New("拒绝清库：DSN 里没有数据库名，无法确认要清的是哪个库")
	}

	return nil
}

// isLoopbackHost 判断主机名是否为本机回环地址。
//
// 不用 net.ParseIP 单独判断，是因为 pgx 的 DSN 里也允许写 "localhost"。
// 空字符串（例如 unix socket 或依赖 PGHOST 环境变量）一律判为不安全：
// 这时候我们无法从 DSN 本身确认目标的身份，宁可拒绝。
func isLoopbackHost(host string) bool {
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// verifySchema 在迁移完成后做一组结构性自检，对应
// 项目内置自检清单里那几条「机器能判」的条目。
//
// 内置自检清单是人工检查清单，但人工检查的可靠性随时间下降。这里把其中
// 最容易在后续改动中被破坏、且一旦破坏就必须重写迁移的几条固化成断言：
// 主键形态、分片生成列、种子 ID 合法性、namespaces 的注册中心字段。
//
// 注意：这不是「迁移正确性」的完整证明，只是把最容易出错的几条钉住。
// 真正的验收仍然要逐条走内置自检清单。
func verifySchema(ctx context.Context, s *store.Store) error {
	checks := []struct {
		name string
		sql  string
		want int64
	}{
		{
			// 22 张业务表（不含 schema_migrations 自己）。
			// 表数一变就说明有人加了表没同步讨论，值得停下来看一眼。
			name: "业务表数量",
			sql: `SELECT count(*) FROM information_schema.tables
			      WHERE table_schema = 'public'
			        AND table_type = 'BASE TABLE'
			        AND table_name <> 'schema_migrations'`,
			want: 22,
		},
		{
			// ID 形态：不允许任何自增主键残留。
			name: "无自增/identity 列",
			sql: `SELECT count(*) FROM information_schema.columns
			      WHERE table_schema = 'public'
			        AND (is_identity = 'YES' OR column_default LIKE 'nextval%')`,
			want: 0,
		},
		{
			// ID 形态：业务主键统一 TEXT（唯一例外是 schema_migrations.version BIGINT）。
			name: "非 TEXT 的业务主键",
			sql: `SELECT count(*) FROM information_schema.columns c
			      WHERE c.table_schema = 'public'
			        AND c.column_name = 'id'
			        AND c.data_type <> 'text'`,
			want: 0,
		},
		{
			// 租户与分片：形态 B 的表必须有生成式 shard 列，且表达式固定为
			// substring(id FROM 5 FOR 2) —— 位置写错的后果是路由到错的分片。
			//
			// 两个坑都在实测中踩过：
			//  1. information_schema 回显的 generation_expression 是规范化后的
			//     大写 SUBSTRING(...)，按小写做 LIKE 永远是 0 行，所以两边都 upper()。
			//  2. tenants.shard 不是生成列 —— 它是创建时一次性写入的粘性分片
			//     （租户与分片），所以这里期望的是 6 张形态 B 流水表，不是 7。
			name: "形态 B 表的 shard 生成列",
			sql: `SELECT count(*) FROM information_schema.columns
			      WHERE table_schema = 'public'
			        AND column_name = 'shard'
			        AND is_generated = 'ALWAYS'
			        AND upper(generation_expression) LIKE 'SUBSTRING(ID FROM 5 FOR 2)%'`,
			want: 6,
		},
		{
			// 反向断言：tenants.shard 必须是普通列。
			// 若有人把它改成生成列，粘性分片就退化成「由 ID 推导」，
			// 等于把租户与分片的迁移能力拆掉了（将来换 shard 会改变 ID）。
			name: "tenants.shard 不是生成列",
			sql: `SELECT count(*) FROM information_schema.columns
			      WHERE table_schema = 'public'
			        AND table_name = 'tenants'
			        AND column_name = 'shard'
			        AND is_generated = 'NEVER'`,
			want: 1,
		},
		{
			// schema_migrations.version 是唯一被允许的 BIGINT（内置自检清单明确列出）。
			name: "schema_migrations.version 仍为 BIGINT",
			sql: `SELECT count(*) FROM information_schema.columns
			      WHERE table_schema = 'public'
			        AND table_name = 'schema_migrations'
			        AND column_name = 'version'
			        AND data_type = 'bigint'`,
			want: 1,
		},
	}

	for _, c := range checks {
		var got int64
		if err := s.Pool().QueryRow(ctx, c.sql).Scan(&got); err != nil {
			return fmt.Errorf("自检「%s」查询失败: %w", c.name, err)
		}
		if got != c.want {
			return fmt.Errorf("自检「%s」不通过：期望 %d，实际 %d", c.name, c.want, got)
		}
	}

	// 内置自检清单第 10 条：每一处 NOT NULL 都必须能归入 NOT NULL 策略的三类，
	// 或落在 NOT NULL 策略的「记录定义」白名单里。这条无法用一个等值断言表达，
	// 所以单独走一个「列出后与白名单求差」的检查。
	if err := verifyNotNullPolicy(ctx, s); err != nil {
		return err
	}

	// 种子数据：全部 ID 都必须能被 idgen 认出来。
	// 这里故意走 Go 侧的校验而不是 SQL 正则，因为「什么样的 ID 合法」的
	// 唯一权威是 idgen 的注册表，SQL 里的 CHECK 只是它的复制品。
	if err := verifySeedIDs(ctx, s); err != nil {
		return err
	}

	return nil
}

// notNullWhitelist 是 NOT NULL 策略允许保留 NOT NULL 的「非三类」列，即记录定义列。
//
// 与实现的对应关系：这是 NOT NULL 策略白名单表的镜像。
// 之所以在代码里再写一遍而不是从文档解析：文档是给人读的，格式会变；
// 这里是建库闸门，必须与 DDL 一起接受编译期检查，且改它会被 code review 看见。
// 两边都改才算完成一次调整——只改一边时，这个自检会立刻报出差异。
var notNullWhitelist = map[string]bool{
	"session_messages.seq":               true,
	"session_messages.direction":         true,
	"usage_events.metric":                true,
	"credit_ledger.entry_type":           true,
	"credit_ledger.direction":            true,
	"credit_ledger.amount_micros":        true,
	"credit_ledger.balance_after_micros": true,
	"billing_reservations.amount_micros": true,
	"billing_reservations.status":        true,
	"billing_reservations.expires_at":    true,
	"admin_audit_logs.action":            true,
	"admin_audit_logs.resource_type":     true,
	"admin_sessions.expires_at":          true,
	"system_setting_revisions.key":       true,
}

// verifyNotNullPolicy 校验「除 NOT NULL 策略三类与白名单之外，没有任何 NOT NULL」。
//
// 为什么这条值得单独写 40 行：NOT NULL 用错不会让迁移失败，它会在半年后
// 表现为「某条计量写不进去，于是整个请求被回滚」，而那时 NOT NULL 例外的例外早已失效，
// 只能再写一条新迁移。这个窗口只有一次，所以把它做成机器检查而不是人工清单。
func verifyNotNullPolicy(ctx context.Context, s *store.Store) error {
	// 一次查出全部「非主键、非 UNIQUE、无 DEFAULT、不参与 CHECK」的 NOT NULL 列。
	// contype 的四个值：p=主键、u=唯一、c=CHECK、f=外键（外键不算豁免）。
	rows, err := s.Pool().Query(ctx, `
		SELECT c.relname || '.' || a.attname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
		  AND c.relkind = 'r'
		  AND c.relname <> 'schema_migrations'
		  AND a.attnotnull
		  AND NOT a.attisdropped
		  AND a.attnum > 0
		  AND NOT a.atthasdef
		  AND NOT EXISTS (SELECT 1 FROM pg_constraint k
		                  WHERE k.conrelid = c.oid AND k.contype IN ('p', 'u')
		                    AND a.attnum = ANY (k.conkey))
		  AND NOT EXISTS (SELECT 1 FROM pg_constraint k
		                  WHERE k.conrelid = c.oid AND k.contype = 'c'
		                    AND a.attnum = ANY (k.conkey))
		ORDER BY 1`)
	if err != nil {
		return fmt.Errorf("自检「NOT NULL 归入 NOT NULL 策略」查询失败: %w", err)
	}
	defer rows.Close()

	var extra []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return fmt.Errorf("自检「NOT NULL 归入 NOT NULL 策略」读取失败: %w", err)
		}
		if !notNullWhitelist[col] {
			extra = append(extra, col)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("自检「NOT NULL 归入 NOT NULL 策略」遍历失败: %w", err)
	}
	if len(extra) > 0 {
		// 报出全部越界列而不是第一个：新增列时通常一次加好几处，
		// 逐个报会让人跑很多遍建库。
		return fmt.Errorf("自检「NOT NULL 归入 NOT NULL 策略」不通过：以下列既不属于主键/带 DEFAULT/参与 CHECK，也不在记录定义白名单里，应放宽为可空或将业务校验下移到 Go 层：%v", extra)
	}
	return nil
}

// verifySeedIDs 逐行校验种子表里的 ID 形态。
//
// 内置自检清单有一条「种子 ID 业务体不含 I/L/O/U」。这一条如果只靠 DDL 的正则，
// 出错时表现为「迁移执行失败」，而这里能直接报出是哪个表的哪个 ID 不合法，
// 定位成本低得多。
func verifySeedIDs(ctx context.Context, s *store.Store) error {
	seeds := []struct {
		table string
		kind  idgen.Kind
		col   string
	}{
		{"plans", idgen.KindPlan, "id"},
		{"admin_roles", idgen.KindAdminRole, "id"},
		{"tenants", idgen.KindTenant, "id"},
	}

	for _, seed := range seeds {
		rows, err := s.Pool().Query(ctx,
			fmt.Sprintf(`SELECT %s FROM %s`, seed.col, seed.table))
		if err != nil {
			return fmt.Errorf("自检种子 ID：读取 %s 失败: %w", seed.table, err)
		}

		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				return fmt.Errorf("自检种子 ID：读取 %s.%s 失败: %w", seed.table, seed.col, err)
			}
			if err := idgen.ValidateOf(seed.kind, raw); err != nil {
				rows.Close()
				return fmt.Errorf("自检种子 ID：%s 里的 %q 不合法: %w", seed.table, raw, err)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("自检种子 ID：遍历 %s 失败: %w", seed.table, err)
		}
	}

	return nil
}
