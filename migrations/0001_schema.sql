-- 0001_schema.sql
--
-- 基线表结构（唯一一份，取代旧 0001~0010 的全部 CREATE/ALTER）。
--
-- 基线约定：
--   ID 形态主键一律 TEXT + 3 字母前缀，禁止 SERIAL / IDENTITY / nextval / 任何 DEFAULT 生成主键
--   额度解析额度由 plans + users.quota_overrides 两级解析
--   会话录制会话录制旁路、统一脱敏、大 payload 分级
--   服务发现 transport 二分化：stdio 本地子进程 / http 远端发现
--   计量计费计量与计费链路第一天就在路径上
--   动态配置一切业务可变项由超级管理员动态管理
--   租户与分片租户是数据归属的唯一边界，分片粘性
--
-- ID 形态（见 internal/idgen）：
--   形态 A  20 字符  {3 前缀}_{16 位 Crockford Base32}      目录表，中央库，不分片
--   形态 B  19 字符  {3 前缀}_{2 位分片}{13 位雪花 Base32}  流水表，分片，带生成列
--
-- 字符集 0123456789ABCDEFGHJKMNPQRSTVWXYZ 排除 I / L / O / U。
--
-- 唯一例外：schema_migrations.version 保持 BIGINT —— 它是迁移框架元数据，
-- 不是业务实体，改文本只会让 migrate.go 的排序逻辑变复杂。此处显式声明，
-- 避免后人误以为漏改（ID 形态第 8 点）。
--
-- NOT NULL 策略（NOT NULL 策略）：业务语义由代码层校验，DDL 的 NOT NULL 只留在三类
-- 真正必要的位置，其余一律允许 NULL。
--   1. 主键 —— PG 隐式 NOT NULL，不重复声明；
--   2. 带 DEFAULT 的列 —— DDL 自己给值，NOT NULL 不会挡住任何写入；
--   3. 参与 CHECK / UNIQUE 的列 —— NULL 会让三值逻辑绕过校验，
--      使格式约束与唯一性形同虚设，这类必须 NOT NULL。
-- 由此推得：tenant_id / api_key_id 这类「由应用传入的归属信息」不设 NOT NULL。
-- 它们是跨进程契约字段，代码层漏填时应能落库并事后回填，而不是让整条
-- 计量/录制写入直接失败。外键与格式 CHECK 全部保留 —— 有值时照样校验，
-- 为 NULL 时明确表示「尚未归属」，可被后台任务扫出来补齐。

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- 迁移元数据（唯一允许 BIGINT 主键的表）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    BIGINT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN schema_migrations.version IS
    'ID 形态唯一例外：迁移框架版本号，非业务实体，保持 BIGINT';

-- ===========================================================================
-- 一、目录层：套餐 / 租户 / 用户 / Key / 命名空间
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 套餐：额度上限的唯一数据源，新增套餐只插数据不改代码
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plans (
    id                       TEXT        PRIMARY KEY
                             CONSTRAINT pln_id_fmt CHECK (id ~ '^pln_[0-9A-HJKMNP-TV-Z]{16}$'),
    code                     TEXT        NOT NULL UNIQUE,
    -- 可空：NOT NULL 策略。展示名不是结构必需的，缺失时管理端回落到 code
    name                     TEXT,
    description              TEXT,
    max_api_keys             INTEGER     NOT NULL DEFAULT 2,
    rate_limit_per_minute    INTEGER     NOT NULL DEFAULT 60,
    max_upload_bytes         BIGINT      NOT NULL DEFAULT 67108864,
    max_concurrent_jobs      INTEGER     NOT NULL DEFAULT 1,
    max_namespaces           INTEGER     NOT NULL DEFAULT 1,
    max_pool_size            INTEGER     NOT NULL DEFAULT 1,
    active                   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT plans_code_format CHECK (code ~ '^[a-z][a-z0-9_-]{1,31}$'),
    CONSTRAINT plans_limits_nonneg CHECK (
        max_api_keys >= 0 AND rate_limit_per_minute >= 0 AND
        max_upload_bytes >= 0 AND max_concurrent_jobs >= 0 AND
        max_namespaces >= 0 AND max_pool_size >= 0
    )
);

COMMENT ON COLUMN plans.max_api_keys IS '每用户可创建的启用态 Key 数量上限';
COMMENT ON COLUMN plans.max_upload_bytes IS '字节数，不是 ID，按 ID 形态仍用 BIGINT（计量计费）';

-- ---------------------------------------------------------------------------
-- 租户：数据归属的唯一边界，永久存在（租户与分片）
--
-- 用户注册即获得一个租户；升级企业级、到期回落，都是在这个租户行上改属性，
-- 而不是换租户、更不是删数据。分片在创建时分配一次，此后永不变更。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenants (
    id          TEXT        PRIMARY KEY
                CONSTRAINT tenants_id_fmt CHECK (id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$'),
    slug        TEXT        NOT NULL UNIQUE
                CONSTRAINT tenants_slug_fmt CHECK (slug ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    -- 可空：NOT NULL 策略。展示名可后补，不参与任何约束
    name        TEXT,
    -- 当前形态：随套餐变化，仅用于展示与能力门控，禁止用于判断数据归属
    kind        TEXT        NOT NULL DEFAULT 'personal'
                CONSTRAINT tenants_kind_valid CHECK (kind IN ('personal', 'enterprise')),
    -- 生命周期状态
    status      TEXT        NOT NULL DEFAULT 'active'
                CONSTRAINT tenants_status_valid
                CHECK (status IN ('active', 'expired', 'suspended', 'closed')),
    -- 企业级套餐；NULL 表示走 owner 账号的 users.plan_id
    plan_id     TEXT        REFERENCES plans(id) ON DELETE RESTRICT,
    -- 粘性分片：创建时确定，永不变更（租户与分片）
    shard       TEXT        NOT NULL
                CONSTRAINT tenants_shard_fmt CHECK (shard ~ '^[0-9A-HJKMNP-TV-Z]{2}$'),
    -- 企业级到期时间；personal 恒为 NULL
    expires_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenants_ent_has_expiry
        CHECK (kind <> 'enterprise' OR expires_at IS NOT NULL),
    -- 租户与分片第 9 点：保留分片 00 只属于系统种子与全局配置。
    -- 租户永远不该落在 00，否则「xx_00… = 内置数据」这条排查依据失效。
    -- 应用层 idgen.ValidateShard 与 shard.Parse 各拦一道，这里是第三道。
    CONSTRAINT tenants_shard_not_reserved CHECK (shard <> '00')
);

COMMENT ON TABLE  tenants IS '租户：数据归属的唯一边界，状态变化不删任何业务行（租户与分片）';
COMMENT ON COLUMN tenants.kind IS '当前形态，仅用于展示与门控；数据归属只认 tenant_id，与 kind 无关';
COMMENT ON COLUMN tenants.shard IS '粘性分片，创建时分配一次，永不变更；禁止等于保留值 00';

-- ---------------------------------------------------------------------------
-- 账号：Key 的归属主体，收费模式的承载点
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id               TEXT        PRIMARY KEY
                     CONSTRAINT usr_id_fmt CHECK (id ~ '^usr_[0-9A-HJKMNP-TV-Z]{16}$'),
    tenant_id        TEXT
                     CONSTRAINT usr_tenant_fmt
                     CHECK (tenant_id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$')
                     REFERENCES tenants(id) ON DELETE RESTRICT,
    email            TEXT        NOT NULL UNIQUE,
    display_name     TEXT,
    password_hash    TEXT,
    plan_id          TEXT        NOT NULL REFERENCES plans(id) ON DELETE RESTRICT
                     CONSTRAINT usr_plan_fmt CHECK (plan_id ~ '^pln_[0-9A-HJKMNP-TV-Z]{16}$'),
    status           TEXT        NOT NULL DEFAULT 'active',
    -- 逐用户覆盖套餐额度，例 {"max_api_keys": 20}；'{}' 表示完全跟随套餐
    quota_overrides  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    expires_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT users_status_valid CHECK (status IN ('active', 'suspended', 'closed')),
    CONSTRAINT users_email_nonempty CHECK (length(trim(email)) > 0)
);

COMMENT ON COLUMN users.quota_overrides IS
    '覆盖 plans 中同名字段，未出现的字段仍用套餐值（额度解析）';
COMMENT ON COLUMN users.tenant_id IS '所属租户；租户到期只影响可见性，不改变该列';

-- ---------------------------------------------------------------------------
-- 租户成员：为未来团队协作预留，本期仅写 owner 一行
--
-- 关系表：复合主键（两个 ID 列），不单独分配前缀（ID 形态）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_members (
    tenant_id   TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id     TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role        TEXT        NOT NULL DEFAULT 'member'
                CONSTRAINT tenant_members_role_valid
                CHECK (role IN ('owner', 'admin', 'member')),
    status      TEXT        NOT NULL DEFAULT 'active'
                CONSTRAINT tenant_members_status_valid
                CHECK (status IN ('active', 'inactive')),
    joined_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, user_id)
);

-- ---------------------------------------------------------------------------
-- API Key：归属 user，数量受配额约束
-- key_hash 存 sha256(token) 原始字节；明文仅在创建时返回一次
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_keys (
    id           TEXT        PRIMARY KEY
                 CONSTRAINT key_id_fmt CHECK (id ~ '^key_[0-9A-HJKMNP-TV-Z]{16}$'),
    user_id      TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE
                 CONSTRAINT key_user_fmt CHECK (user_id ~ '^usr_[0-9A-HJKMNP-TV-Z]{16}$'),
    name         TEXT        NOT NULL,
    key_prefix   TEXT        NOT NULL,
    key_hash     BYTEA       NOT NULL UNIQUE,
    enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    -- 软禁用原因：租户套餐到期时批量置 disabled_by='plan_expiry'，
    -- 续费后按该列原样回置，不需要人工翻日志对账（租户与分片 §9.4）
    disabled_by  TEXT,
    -- 该 Key 的限流覆盖值，NULL 则跟随套餐
    rate_limit   INTEGER,
    expires_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    CONSTRAINT api_keys_name_nonempty CHECK (length(trim(name)) > 0),
    CONSTRAINT api_keys_prefix_nonempty CHECK (length(key_prefix) > 0),
    CONSTRAINT api_keys_disabled_by_valid
        CHECK (disabled_by IS NULL OR disabled_by IN ('plan_expiry', 'admin', 'tenant_closed')),
    CONSTRAINT api_keys_disabled_consistency
        CHECK (enabled OR disabled_by IS NOT NULL)
);

COMMENT ON COLUMN api_keys.key_prefix  IS '明文前 12 位，仅用于列表展示与排查，不足以还原 Key';
COMMENT ON COLUMN api_keys.key_hash    IS 'sha256(token) 原始字节，校验时用常量时间比较';
COMMENT ON COLUMN api_keys.enabled     IS '软删除位；禁用后不占用套餐的 max_api_keys 配额';
COMMENT ON COLUMN api_keys.disabled_by IS '软禁用原因，plan_expiry 的行在续费后可批量恢复';

-- ---------------------------------------------------------------------------
-- 命名空间：一个上游 MCP 服务
--
-- 目录表（形态 A）：它是全局服务定义，不归属某个租户，因此不分片。
-- transport 二分化（服务发现）：
--   stdio → 本地子进程，用 command/args/env，由进程池管理
--   http  → 远端服务，command 必须为 NULL，实例地址由发现层给出
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS namespaces (
    id                 TEXT        PRIMARY KEY
                       CONSTRAINT nsp_id_fmt CHECK (id ~ '^nsp_[0-9A-HJKMNP-TV-Z]{16}$'),
    slug               TEXT        NOT NULL UNIQUE,
    description        TEXT,
    transport          TEXT        NOT NULL DEFAULT 'stdio',
    command            TEXT,
    args               JSONB       NOT NULL DEFAULT '[]'::jsonb,
    env                JSONB       NOT NULL DEFAULT '{}'::jsonb,
    pool_size          INTEGER     NOT NULL DEFAULT 1,
    timeout_ms         INTEGER     NOT NULL DEFAULT 120000,
    enabled            BOOLEAN     NOT NULL DEFAULT TRUE,
    -- 软禁用原因（租户到期/管理员下线），便于原样恢复
    disabled_by        TEXT,
    -- 仅 transport='http' 时生效
    discovery_provider TEXT,
    discovery_service  TEXT,
    discovery_group    TEXT,
    endpoint_path      TEXT,
    lb_strategy        TEXT,
    -- 来源标记：同步器只允许回收 registry 行
    source             TEXT        NOT NULL DEFAULT 'manual',
    last_seen_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT namespaces_slug_format CHECK (slug ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    CONSTRAINT namespaces_pool_size_positive CHECK (pool_size >= 1),
    CONSTRAINT namespaces_timeout_positive CHECK (timeout_ms >= 1000),
    CONSTRAINT namespaces_disabled_by_valid
        CHECK (disabled_by IS NULL OR disabled_by IN ('plan_expiry', 'admin', 'registry_gone')),
    CONSTRAINT namespaces_disabled_consistency
        CHECK (enabled OR disabled_by IS NOT NULL),

    CONSTRAINT namespaces_transport_check CHECK (transport IN ('stdio', 'http')),

    -- 谁该有 command：stdio 必填，http 必须为 NULL
    CONSTRAINT namespaces_command_transport_check
        CHECK (
            (transport = 'stdio'
                AND command IS NOT NULL
                AND length(btrim(command)) > 0)
            OR
            (transport = 'http'
                AND command IS NULL)
        ),

    -- http 模式下 args / env 无意义，强制为空（否则运维会误以为改这里生效）
    CONSTRAINT namespaces_http_args_env_empty_check
        CHECK (transport <> 'http' OR (args = '[]'::jsonb AND env = '{}'::jsonb)),

    -- stdio 不许配发现字段；http 必须指明服务名，provider 只能是 rnacos 或 static
    CONSTRAINT namespaces_transport_discovery_check
        CHECK (
            (transport = 'stdio'
                AND discovery_provider IS NULL
                AND discovery_service  IS NULL)
            OR
            (transport = 'http'
                AND discovery_service IS NOT NULL
                AND length(btrim(discovery_service)) > 0
                AND discovery_provider IN ('rnacos', 'static'))
        ),

    CONSTRAINT namespaces_lb_strategy_check
        CHECK (lb_strategy IS NULL OR lb_strategy IN ('round_robin')),

    CONSTRAINT namespaces_source_check
        CHECK (source IN ('manual', 'config', 'registry')),

    -- registry 行必然来自发现层，因此必须是 http 且带服务名
    CONSTRAINT namespaces_registry_consistency_check
        CHECK (
            source <> 'registry'
            OR (transport = 'http' AND discovery_service IS NOT NULL)
        )
);

COMMENT ON COLUMN namespaces.transport          IS 'stdio = 本地子进程；http = 远端服务，地址由发现层给出';
COMMENT ON COLUMN namespaces.command            IS 'stdio 模式的启动命令；transport=http 时必须为 NULL（地址由发现层给出）';
COMMENT ON COLUMN namespaces.discovery_provider IS 'NULL = 不用发现（stdio 或 static）；rnacos = 从注册中心发现';
COMMENT ON COLUMN namespaces.discovery_service  IS '注册中心里的服务名，如 blender-core-service';
COMMENT ON COLUMN namespaces.discovery_group    IS '注册中心分组，默认 DEFAULT_GROUP';
COMMENT ON COLUMN namespaces.endpoint_path      IS 'http 模式下的端点路径，如 /mcp';
COMMENT ON COLUMN namespaces.lb_strategy        IS '负载策略，本轮仅支持 round_robin';
COMMENT ON COLUMN namespaces.source             IS '来源：manual=管理员手工创建，config=config.yaml 自举，registry=注册中心自动发现（仅 registry 会被同步器自动下线）';
COMMENT ON COLUMN namespaces.last_seen_at       IS '该服务最近一次在注册中心被观测到的时间；NULL 表示从未由注册中心发现';
COMMENT ON COLUMN namespaces.disabled_by        IS '软禁用原因，registry_gone 表示注册中心里已消失';

-- 工具注册表：白名单来源，embedding 供 M6 的语义检索使用
CREATE TABLE IF NOT EXISTS namespace_tools (
    namespace_id TEXT        NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    tool_name    TEXT        NOT NULL,
    allowed      BOOLEAN     NOT NULL DEFAULT TRUE,
    description  TEXT,
    embedding    vector(768),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, tool_name)
);

-- 权限：tool_name 为 '*' 表示该命名空间下全部工具
CREATE TABLE IF NOT EXISTS key_permissions (
    key_id       TEXT        NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    namespace_id TEXT        NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    tool_name    TEXT        NOT NULL DEFAULT '*',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (key_id, namespace_id, tool_name)
);

-- ===========================================================================
-- 二、流水层（形态 B）：全部带生成列 shard
--
-- 生成列把分片物理化：id 里第 5~6 字节就是 shard，
-- 应用层不可能写进一个与 ID 不一致的分片值。
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- sessions：一次 SSE 连接 / 一段连续交互上下文
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT        PRIMARY KEY
                    CONSTRAINT ses_id_fmt CHECK (id ~ '^ses_[0-9A-HJKMNP-TV-Z]{15}$'),
    shard           TEXT        GENERATED ALWAYS AS (substring(id FROM 5 FOR 2)) STORED,
    tenant_id       TEXT        REFERENCES tenants(id) ON DELETE RESTRICT
                    CONSTRAINT ses_tenant_fmt CHECK (tenant_id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$'),
    user_id         TEXT        NOT NULL REFERENCES users(id) ON DELETE RESTRICT
                    CONSTRAINT ses_user_fmt CHECK (user_id ~ '^usr_[0-9A-HJKMNP-TV-Z]{16}$'),
    -- 可空：Key 被删除后会话必须留存（录制语料不能随 Key 消失）
    api_key_id      TEXT        REFERENCES api_keys(id) ON DELETE RESTRICT
                    CONSTRAINT ses_key_fmt CHECK (api_key_id ~ '^key_[0-9A-HJKMNP-TV-Z]{16}$'),
    namespace_id    TEXT        REFERENCES namespaces(id) ON DELETE SET NULL,
    -- 可空：NOT NULL 策略。与 namespace_id 一致——命名空间被删除后会话仍须留存，
    -- 此时快照列也没有值可写。Go 侧读取已用 coalesce(namespace_slug, '')。
    namespace_slug  TEXT,
    -- 网关 SSE 侧 sessionId（uuid），用于与应用日志对齐
    -- 可空：NOT NULL 策略。旧路径（无 SSE 会话）拿不到 sessionId，
    -- 不该因此丢一条录制语料。
    session_key     TEXT,
    client_ip       INET,
    user_agent      TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ,
    message_count   INTEGER     NOT NULL DEFAULT 0,
    input_bytes     BIGINT      NOT NULL DEFAULT 0,
    output_bytes    BIGINT      NOT NULL DEFAULT 0,
    error_count     INTEGER     NOT NULL DEFAULT 0,
    -- 训练语料标记，管理员在后台标注；本轮只埋点不使用
    training_label  TEXT,
    notes           TEXT,
    CONSTRAINT sessions_training_label_check
        CHECK (training_label IS NULL OR training_label IN ('approved', 'rejected', 'review')),
    CONSTRAINT sessions_counts_nonneg
        CHECK (message_count >= 0 AND input_bytes >= 0 AND output_bytes >= 0 AND error_count >= 0)
);

COMMENT ON TABLE  sessions IS '会话录制主表：一次 SSE 连接或一段连续交互';
COMMENT ON COLUMN sessions.session_key    IS '网关侧 SSE sessionId，用于与应用日志对齐';
COMMENT ON COLUMN sessions.training_label IS '训练语料标记：approved 可训练 / rejected 不可用 / review 待审';
COMMENT ON COLUMN sessions.shard          IS '生成列，恒等于 substring(id FROM 5 FOR 2)；禁止写入';

-- ---------------------------------------------------------------------------
-- session_messages：一次 JSON-RPC 往返
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS session_messages (
    id             TEXT        PRIMARY KEY
                   CONSTRAINT msg_id_fmt CHECK (id ~ '^msg_[0-9A-HJKMNP-TV-Z]{15}$'),
    shard          TEXT        GENERATED ALWAYS AS (substring(id FROM 5 FOR 2)) STORED,
    tenant_id      TEXT        REFERENCES tenants(id) ON DELETE RESTRICT
                   CONSTRAINT msg_tenant_fmt CHECK (tenant_id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$'),
    session_id     TEXT        NOT NULL REFERENCES sessions(id) ON DELETE CASCADE
                   CONSTRAINT msg_session_fmt CHECK (session_id ~ '^ses_[0-9A-HJKMNP-TV-Z]{15}$'),
    -- 会话内单调递增，便于按序回放
    seq            INTEGER     NOT NULL,
    direction      TEXT        NOT NULL,
    rpc_id         TEXT,
    method         TEXT,
    tool_name      TEXT,
    -- 小负载直存 JSONB；超阈值转对象存储，引用写 payload_ref
    payload        JSONB,
    payload_ref    TEXT,
    payload_bytes  INTEGER     NOT NULL DEFAULT 0,
    truncated      BOOLEAN     NOT NULL DEFAULT FALSE,
    redacted       BOOLEAN     NOT NULL DEFAULT FALSE,
    is_error       BOOLEAN     NOT NULL DEFAULT FALSE,
    -- JSON-RPC error.code 取值可能有负数与字符串，用 TEXT 而非 INTEGER
    error_code     TEXT,
    duration_ms    INTEGER,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT session_messages_direction_check
        CHECK (direction IN ('client_to_gateway', 'gateway_to_upstream',
                             'upstream_to_gateway', 'gateway_to_client')),
    CONSTRAINT session_messages_seq_unique UNIQUE (session_id, seq),
    CONSTRAINT session_messages_seq_positive CHECK (seq >= 0),
    CONSTRAINT session_messages_payload_bytes_nonneg CHECK (payload_bytes >= 0),
    CONSTRAINT session_messages_duration_nonneg
        CHECK (duration_ms IS NULL OR duration_ms >= 0)
);

COMMENT ON TABLE  session_messages IS '会话消息录制：入站与出站两侧正文';
COMMENT ON COLUMN session_messages.direction   IS '四段式方向，训练语料需区分用户原话与上游返回值';
COMMENT ON COLUMN session_messages.payload_ref IS '形如 s3://mcp-sessions/<sessionID>/<seq>.json';
COMMENT ON COLUMN session_messages.truncated   IS '正文超过 max_payload_bytes 被截断，payload 内为 _truncated 占位';
COMMENT ON COLUMN session_messages.redacted    IS '命中脱敏规则，敏感值已替换为 [REDACTED]';
COMMENT ON COLUMN session_messages.error_code  IS 'JSON-RPC 错误码，TEXT 以容纳负数与字符串码';
COMMENT ON COLUMN session_messages.shard       IS '生成列，恒等于 substring(id FROM 5 FOR 2)；禁止写入';

-- ===========================================================================
-- 三、计费与计量（计量计费）
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 计价规则：目录表（形态 A），全局共享，不归属租户
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_rules (
    id                TEXT        PRIMARY KEY
                      CONSTRAINT prc_id_fmt CHECK (id ~ '^prc_[0-9A-HJKMNP-TV-Z]{16}$'),
    code              TEXT        NOT NULL UNIQUE,
    -- 可空：NOT NULL 策略。同 plans.name
    name              TEXT,
    metric            TEXT        NOT NULL,
    unit_size         BIGINT      NOT NULL DEFAULT 1,
    unit_price_micros BIGINT      NOT NULL DEFAULT 0,
    currency          TEXT        NOT NULL DEFAULT 'CNY',
    namespace_slug    TEXT,
    plan_code         TEXT,
    active            BOOLEAN     NOT NULL DEFAULT TRUE,
    effective_from    TIMESTAMPTZ,
    effective_to      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT price_rules_code_format CHECK (code ~ '^[a-z][a-z0-9_.-]{1,47}$'),
    CONSTRAINT price_rules_metric_valid
        CHECK (metric IN ('request', 'input_token', 'output_token', 'tool_call',
                          'render_second', 'upload_byte', 'storage_byte')),
    CONSTRAINT price_rules_unit_size_positive CHECK (unit_size >= 1),
    CONSTRAINT price_rules_unit_price_nonneg CHECK (unit_price_micros >= 0),
    CONSTRAINT price_rules_window_valid
        CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_to > effective_from)
);

COMMENT ON COLUMN price_rules.unit_size IS '计价单位大小，如每 1000 token；不是 ID，按 ID 形态用 BIGINT';
COMMENT ON COLUMN price_rules.unit_price_micros IS '单价，百万分之一货币单位；金额用 BIGINT 而非浮点';

-- ---------------------------------------------------------------------------
-- 用量事件：一行一次可计量的使用量
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS usage_events (
    id             TEXT        PRIMARY KEY
                   CONSTRAINT uev_id_fmt CHECK (id ~ '^uev_[0-9A-HJKMNP-TV-Z]{15}$'),
    shard          TEXT        GENERATED ALWAYS AS (substring(id FROM 5 FOR 2)) STORED,
    tenant_id      TEXT        REFERENCES tenants(id) ON DELETE RESTRICT
                   CONSTRAINT uev_tenant_fmt CHECK (tenant_id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$'),
    user_id        TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE
                   CONSTRAINT uev_user_fmt CHECK (user_id ~ '^usr_[0-9A-HJKMNP-TV-Z]{16}$'),
    api_key_id     TEXT        REFERENCES api_keys(id) ON DELETE SET NULL,
    session_id     TEXT        REFERENCES sessions(id) ON DELETE SET NULL,
    namespace_id   TEXT        REFERENCES namespaces(id) ON DELETE SET NULL,
    namespace_slug TEXT,
    request_id     TEXT,
    metric         TEXT        NOT NULL,
    quantity       BIGINT      NOT NULL DEFAULT 0,
    unit           TEXT        NOT NULL DEFAULT 'count',
    billable       BOOLEAN     NOT NULL DEFAULT TRUE,
    cost_micros    BIGINT      NOT NULL DEFAULT 0,
    price_rule_id  TEXT        REFERENCES price_rules(id) ON DELETE SET NULL,
    currency       TEXT        NOT NULL DEFAULT 'CNY',
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT usage_events_metric_valid
        CHECK (metric IN ('request', 'input_token', 'output_token', 'tool_call',
                          'render_second', 'upload_byte', 'storage_byte')),
    CONSTRAINT usage_events_unit_valid
        CHECK (unit IN ('count', 'token', 'second', 'byte')),
    CONSTRAINT usage_events_quantity_nonneg CHECK (quantity >= 0),
    CONSTRAINT usage_events_cost_nonneg CHECK (cost_micros >= 0)
);

COMMENT ON COLUMN usage_events.quantity IS '计量数量，不是 ID，按 ID 形态用 BIGINT';
COMMENT ON COLUMN usage_events.cost_micros IS '本次费用，百万分之一货币单位';

-- ---------------------------------------------------------------------------
-- 账户余额：user 与余额一一对应，主键即 user_id（不是独立 ID 列）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS account_balances (
    user_id                 TEXT        PRIMARY KEY
                            REFERENCES users(id) ON DELETE CASCADE,
    balance_micros          BIGINT      NOT NULL DEFAULT 0,
    reserved_micros         BIGINT      NOT NULL DEFAULT 0,
    lifetime_granted_micros BIGINT      NOT NULL DEFAULT 0,
    lifetime_consumed_micros BIGINT     NOT NULL DEFAULT 0,
    credit_limit_micros     BIGINT      NOT NULL DEFAULT 0,
    currency                TEXT        NOT NULL DEFAULT 'CNY',
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT account_balances_nonneg
        CHECK (balance_micros >= 0 AND reserved_micros >= 0
               AND lifetime_granted_micros >= 0 AND lifetime_consumed_micros >= 0
               AND credit_limit_micros >= 0)
);

COMMENT ON TABLE account_balances IS '余额快照；权威账本是 credit_ledger，此表可重建';
COMMENT ON COLUMN account_balances.reserved_micros IS '预扣中金额，等于未关闭 billing_reservations 之和';

-- ---------------------------------------------------------------------------
-- 账本：只追加，不改不改删
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS credit_ledger (
    id                  TEXT        PRIMARY KEY
                        CONSTRAINT led_id_fmt CHECK (id ~ '^led_[0-9A-HJKMNP-TV-Z]{15}$'),
    shard               TEXT        GENERATED ALWAYS AS (substring(id FROM 5 FOR 2)) STORED,
    tenant_id           TEXT        REFERENCES tenants(id) ON DELETE RESTRICT
                        CONSTRAINT led_tenant_fmt CHECK (tenant_id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$'),
    user_id             TEXT        NOT NULL REFERENCES users(id) ON DELETE RESTRICT
                        CONSTRAINT led_user_fmt CHECK (user_id ~ '^usr_[0-9A-HJKMNP-TV-Z]{16}$'),
    entry_type          TEXT        NOT NULL,
    direction           TEXT        NOT NULL,
    amount_micros       BIGINT      NOT NULL,
    balance_after_micros BIGINT     NOT NULL,
    currency            TEXT        NOT NULL DEFAULT 'CNY',
    usage_event_id      TEXT        REFERENCES usage_events(id) ON DELETE SET NULL,
    reservation_id      TEXT,
    invoice_id          TEXT,
    idempotency_key     TEXT,
    memo                TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT credit_ledger_entry_type_valid
        CHECK (entry_type IN ('grant', 'reserve', 'settle', 'release',
                              'consume', 'refund', 'adjust', 'expire')),
    CONSTRAINT credit_ledger_direction_valid CHECK (direction IN ('credit', 'debit')),
    CONSTRAINT credit_ledger_amount_positive CHECK (amount_micros > 0)
);

COMMENT ON TABLE credit_ledger IS '权威账本，只追加；余额可由它重算';
COMMENT ON COLUMN credit_ledger.idempotency_key IS '幂等键，同一 key 只允许入账一次（部分唯一索引兜底）';

-- ---------------------------------------------------------------------------
-- 预扣单：请求前占用额度，结束后结算或释放
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS billing_reservations (
    id             TEXT        PRIMARY KEY
                   CONSTRAINT rsv_id_fmt CHECK (id ~ '^rsv_[0-9A-HJKMNP-TV-Z]{15}$'),
    shard          TEXT        GENERATED ALWAYS AS (substring(id FROM 5 FOR 2)) STORED,
    tenant_id      TEXT        REFERENCES tenants(id) ON DELETE RESTRICT
                   CONSTRAINT rsv_tenant_fmt CHECK (tenant_id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$'),
    user_id        TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE
                   CONSTRAINT rsv_user_fmt CHECK (user_id ~ '^usr_[0-9A-HJKMNP-TV-Z]{16}$'),
    api_key_id     TEXT        REFERENCES api_keys(id) ON DELETE SET NULL,
    amount_micros  BIGINT      NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'open',
    settled_micros BIGINT      NOT NULL DEFAULT 0,
    usage_event_id TEXT        REFERENCES usage_events(id) ON DELETE SET NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    closed_at      TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT billing_reservations_amount_positive CHECK (amount_micros > 0),
    CONSTRAINT billing_reservations_settled_nonneg CHECK (settled_micros >= 0),
    CONSTRAINT billing_reservations_status_valid
        CHECK (status IN ('open', 'settled', 'released', 'expired'))
);

-- ---------------------------------------------------------------------------
-- 发票：按账期汇总
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS invoices (
    id           TEXT        PRIMARY KEY
                 CONSTRAINT inv_id_fmt CHECK (id ~ '^inv_[0-9A-HJKMNP-TV-Z]{15}$'),
    shard        TEXT        GENERATED ALWAYS AS (substring(id FROM 5 FOR 2)) STORED,
    tenant_id    TEXT        REFERENCES tenants(id) ON DELETE RESTRICT
                 CONSTRAINT inv_tenant_fmt CHECK (tenant_id ~ '^tnt_[0-9A-HJKMNP-TV-Z]{16}$'),
    user_id      TEXT        NOT NULL REFERENCES users(id) ON DELETE RESTRICT
                 CONSTRAINT inv_user_fmt CHECK (user_id ~ '^usr_[0-9A-HJKMNP-TV-Z]{16}$'),
    period_start TIMESTAMPTZ NOT NULL,
    period_end   TIMESTAMPTZ NOT NULL,
    total_micros BIGINT      NOT NULL DEFAULT 0,
    currency     TEXT        NOT NULL DEFAULT 'CNY',
    status       TEXT        NOT NULL DEFAULT 'open',
    issued_at    TIMESTAMPTZ,
    paid_at      TIMESTAMPTZ,
    notes        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT invoices_period_valid CHECK (period_end > period_start),
    CONSTRAINT invoices_total_nonneg CHECK (total_micros >= 0),
    CONSTRAINT invoices_status_valid CHECK (status IN ('open', 'issued', 'paid', 'void')),
    CONSTRAINT invoices_user_period_unique UNIQUE (user_id, period_start, period_end)
);

-- ===========================================================================
-- 四、权限与动态配置（动态配置）
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 管理员角色
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS admin_roles (
    id          TEXT        PRIMARY KEY
                CONSTRAINT rol_id_fmt CHECK (id ~ '^rol_[0-9A-HJKMNP-TV-Z]{16}$'),
    code        TEXT        NOT NULL UNIQUE,
    -- 可空：NOT NULL 策略。同 plans.name
    name        TEXT,
    description TEXT,
    permissions JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT admin_roles_code_format CHECK (code ~ '^[a-z][a-z0-9_]{1,31}$'),
    CONSTRAINT admin_roles_permissions_array CHECK (jsonb_typeof(permissions) = 'array')
);

-- ---------------------------------------------------------------------------
-- 管理员账号
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS admin_accounts (
    id              TEXT        PRIMARY KEY
                    CONSTRAINT adm_id_fmt CHECK (id ~ '^adm_[0-9A-HJKMNP-TV-Z]{16}$'),
    email           TEXT        NOT NULL UNIQUE,
    -- 可空：NOT NULL 策略。将来接入 SSO / 服务账号时没有本地口令；
    -- 「必须设置口令」由 M2 后台创建流程校验，不由 DDL 承担。
    password_hash   TEXT,
    display_name    TEXT,
    role_id         TEXT        NOT NULL REFERENCES admin_roles(id) ON DELETE RESTRICT
                    CONSTRAINT adm_role_fmt CHECK (role_id ~ '^rol_[0-9A-HJKMNP-TV-Z]{16}$'),
    status          TEXT        NOT NULL DEFAULT 'active',
    mfa_secret      BYTEA,
    mfa_enabled     BOOLEAN     NOT NULL DEFAULT FALSE,
    failed_attempts INTEGER     NOT NULL DEFAULT 0,
    locked_until    TIMESTAMPTZ,
    last_login_at   TIMESTAMPTZ,
    last_login_ip   INET,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT admin_accounts_status_valid
        CHECK (status IN ('active', 'suspended', 'closed')),
    CONSTRAINT admin_accounts_failed_attempts_nonneg CHECK (failed_attempts >= 0)
);

-- ---------------------------------------------------------------------------
-- 管理员会话：服务端可撤销的登录态
-- 形态 A：它不是流水量表，会话条数受「管理员人数 × 在线设备数」约束，
-- 不需要分片，也永远留在中央库。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS admin_sessions (
    id         TEXT        PRIMARY KEY
               CONSTRAINT asn_id_fmt CHECK (id ~ '^asn_[0-9A-HJKMNP-TV-Z]{16}$'),
    admin_id   TEXT        NOT NULL REFERENCES admin_accounts(id) ON DELETE CASCADE
               CONSTRAINT asn_admin_fmt CHECK (admin_id ~ '^adm_[0-9A-HJKMNP-TV-Z]{16}$'),
    token_hash BYTEA       NOT NULL UNIQUE,
    ip         INET,
    user_agent TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN admin_sessions.token_hash IS 'sha256(token)，明文只在登录响应里出现一次';

-- ---------------------------------------------------------------------------
-- 动态配置：key 是业务主键（语义化字符串），不属于 ID 体系
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_settings (
    key         TEXT        PRIMARY KEY,
    -- 可空：NOT NULL 策略。NULL 表示「只声明不覆盖」，读取方回落到代码内置默认值
    value       JSONB,
    value_type  TEXT        NOT NULL DEFAULT 'json',
    scope       TEXT        NOT NULL DEFAULT 'global',
    description TEXT,
    is_system   BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_by  TEXT        REFERENCES admin_accounts(id) ON DELETE SET NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT system_settings_key_format CHECK (key ~ '^[a-z][a-z0-9_.]{1,63}$'),
    CONSTRAINT system_settings_value_type_valid
        CHECK (value_type IN ('bool', 'int', 'string', 'json', 'duration')),
    CONSTRAINT system_settings_scope_valid
        CHECK (scope IN ('global', 'billing', 'recording', 'auth', 'discovery'))
);

COMMENT ON TABLE system_settings IS
    '动态配置主表；key 是语义化业务主键，不是 ID（ID 形态不适用）';

-- ---------------------------------------------------------------------------
-- 配置变更历史：每次改配置落一条，谁改的、为什么、改前改后
-- 形态 A：低量元数据，中央库，不分片。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_setting_revisions (
    id         TEXT        PRIMARY KEY
               CONSTRAINT rev_id_fmt CHECK (id ~ '^rev_[0-9A-HJKMNP-TV-Z]{16}$'),
    key        TEXT        NOT NULL,
    old_value  JSONB,
    new_value  JSONB,
    admin_id   TEXT        REFERENCES admin_accounts(id) ON DELETE SET NULL,
    reason     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE system_setting_revisions IS
    '配置变更历史；低量元数据表，形态 A 不分片（见 internal/idgen 的形态判据）';

-- ---------------------------------------------------------------------------
-- 管理端审计日志
-- resource_id 是 TEXT，天然容纳任意形态的业务 ID
-- 形态 A：审计行虽多，但它是「谁做了什么」的旁路记录，不参与分片路由；
-- 真到瓶颈时按 created_at 分区，而不是按租户分片。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS admin_audit_logs (
    id            TEXT        PRIMARY KEY
                  CONSTRAINT aud_id_fmt CHECK (id ~ '^aud_[0-9A-HJKMNP-TV-Z]{16}$'),
    admin_id      TEXT        REFERENCES admin_accounts(id) ON DELETE SET NULL,
    admin_email   TEXT,
    action        TEXT        NOT NULL,
    resource_type TEXT        NOT NULL,
    resource_id   TEXT,
    before        JSONB,
    after         JSONB,
    ip            INET,
    user_agent    TEXT,
    request_id    TEXT,
    success       BOOLEAN     NOT NULL DEFAULT TRUE,
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE admin_audit_logs IS
    '管理端审计日志；resource_id 为 TEXT，适配任意形态业务 ID';

-- ===========================================================================
-- 五、辅助函数
-- ===========================================================================

-- 会话结束时一次性回填统计，避免录制器额外 COUNT。
-- 不在消息表挂触发器：高频写入下触发器会把开销放大。
-- p_session_id 是 TEXT（形态 B 的 ses_ ID）。
CREATE OR REPLACE FUNCTION refresh_session_stats(p_session_id TEXT)
RETURNS VOID AS $$
BEGIN
    UPDATE sessions s
       SET message_count = COALESCE(m.cnt, 0),
           input_bytes   = COALESCE(m.in_bytes, 0),
           output_bytes  = COALESCE(m.out_bytes, 0),
           error_count   = COALESCE(m.err_cnt, 0)
      FROM (
            SELECT COUNT(*) AS cnt,
                   COALESCE(SUM(payload_bytes) FILTER (
                       WHERE direction IN ('client_to_gateway', 'gateway_to_upstream')), 0) AS in_bytes,
                   COALESCE(SUM(payload_bytes) FILTER (
                       WHERE direction IN ('upstream_to_gateway', 'gateway_to_client')), 0) AS out_bytes,
                   COUNT(*) FILTER (WHERE is_error) AS err_cnt
              FROM session_messages
             WHERE session_id = p_session_id
           ) m
     WHERE s.id = p_session_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_session_stats(TEXT) IS
    '按 session_id 重算 sessions 上的计数与字节数；由录制器在会话结束时调用';

-- 命名空间变更通知：供 discovery 热重载使用。
-- PG NOTIFY 足够轻，不必为此再引入消息中间件；payload 只带 slug，消费方回查。
CREATE OR REPLACE FUNCTION notify_namespaces_changed()
RETURNS TRIGGER AS $$
DECLARE
    target_slug TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_slug := OLD.slug;
    ELSE
        target_slug := NEW.slug;
    END IF;

    PERFORM pg_notify('namespaces_changed', json_build_object(
        'op',   lower(TG_OP),
        'slug', target_slug
    )::text);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_namespaces_changed ON namespaces;
CREATE TRIGGER trg_namespaces_changed
    AFTER INSERT OR UPDATE OR DELETE ON namespaces
    FOR EACH ROW
    EXECUTE FUNCTION notify_namespaces_changed();

-- 动态配置变更通知：settings 包据此刷新内存快照，无需轮询。
CREATE OR REPLACE FUNCTION notify_system_settings_changed()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('system_settings_changed', json_build_object(
        'op',  lower(TG_OP),
        'key', COALESCE(NEW.key, OLD.key)
    )::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_system_settings_changed ON system_settings;
CREATE TRIGGER trg_system_settings_changed
    AFTER INSERT OR UPDATE OR DELETE ON system_settings
    FOR EACH ROW
    EXECUTE FUNCTION notify_system_settings_changed();
