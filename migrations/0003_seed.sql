-- 0003_seed.sql
--
-- 全局参照数据种子：套餐 / 计价规则 / 管理员角色 / 动态配置。
--
-- 三条硬规则：
--   1. 种子 ID 必须「任何环境插入都得到同一个值」，否则多环境无法互相引用。
--      因此一律用常量字面量，不用 idgen 生成，也不用 row_number() 之类的派生表达式。
--   2. 业务体必须只含 Crockford Base32 允许的字符
--      （0123456789ABCDEFGHJKMNPQRSTVWXYZ，排除 I / L / O / U）。
--      所以业务体统一为纯数字 000…000N。
--      像 pln_PRO、prc_OUT、rol_OPS、rol_SUPPORT 这类助记写法含禁用字符，
--      写进带 CHECK (id ~ '^pln_...$') 的表会直接被数据库拒掉。
--      ID 只承担「身份」，人可读的语义交给 code / metric 这类真正的语义列。
--   3. 全部 seed 语句必须可重复执行（ON CONFLICT DO NOTHING / WHERE NOT EXISTS）。
--
-- 注意：本文件不播种任何账号。管理员账号由部署流程显式创建，
-- 避免「默认密码」成为新的攻击面（动态配置安全基线）。

-- ===========================================================================
-- 一、套餐
--
-- 初期策略：额度给到极高、费率近零，让计量与计费链路第一天就跑在真实路径上，
-- 但不产生任何真实扣费压力。后期只需改这里的数据，不改代码。
-- ===========================================================================
INSERT INTO plans (id, code, name, description,
                   max_api_keys, rate_limit_per_minute, max_upload_bytes,
                   max_concurrent_jobs, max_namespaces, max_pool_size)
VALUES
    ('pln_0000000000000001', 'free', '免费版',
     '个人试用；初期额度已放开，仅作计量链路验证',
     50,    100000,   10737418240,   100,  20,  20),
    ('pln_0000000000000002', 'pro', '专业版',
     '独立开发者常用配置',
     500,   1000000,  107374182400,  500,  100, 100),
    ('pln_0000000000000003', 'team', '团队版',
     '团队协作，较高并发与更大上传',
     500,   1000000,  107374182400,  500,  100, 100)
ON CONFLICT (code) DO NOTHING;

COMMENT ON TABLE plans IS
    '套餐：额度上限的唯一数据源。新增套餐只插数据不改代码（额度解析）';

-- ===========================================================================
-- 二、计价规则
--
-- 七个 metric 全部建规则，结算链路从第一天就是通的；
-- 以后只把 unit_price_micros 改大即可开始收费。
-- request 保留 1 micro/次，用于肉眼确认「确实计费了」而不产生真实成本。
-- ===========================================================================
INSERT INTO price_rules (id, code, name, metric, unit_size, unit_price_micros, currency)
VALUES
    ('prc_0000000000000001', 'default.request',       '每次调用',   'request',       1,          1, 'CNY'),
    ('prc_0000000000000002', 'default.input_token',   '输入 Token', 'input_token',   1000,       0, 'CNY'),
    ('prc_0000000000000003', 'default.output_token',  '输出 Token', 'output_token',  1000,       0, 'CNY'),
    ('prc_0000000000000004', 'default.tool_call',     '工具调用',   'tool_call',     1,          0, 'CNY'),
    ('prc_0000000000000005', 'default.render_second', '渲染秒数',   'render_second', 1,          0, 'CNY'),
    ('prc_0000000000000006', 'default.upload_byte',   '上传体积',   'upload_byte',   1048576,    0, 'CNY'),
    ('prc_0000000000000007', 'default.storage_byte',  '存储占用',   'storage_byte',  1073741824, 0, 'CNY')
ON CONFLICT (code) DO NOTHING;

-- ===========================================================================
-- 三、管理员角色
--
-- 权限以 JSONB 数组表达，'*' 表示全部。角色是数据不是代码：
-- 新增角色（如 finance）只插一行，不需要改任何 Go 代码。
-- ===========================================================================
INSERT INTO admin_roles (id, code, name, description, permissions)
VALUES
    ('rol_0000000000000001', 'super_admin', '超级管理员',
     '全部权限；可修改任何动态配置与计费口径',
     '["*"]'::jsonb),
    ('rol_0000000000000002', 'ops', '运维',
     '命名空间与发现配置、会话查看；不可改计费与管理员',
     '["namespaces.read","namespaces.write","discovery.read","discovery.write","sessions.read","settings.read"]'::jsonb),
    ('rol_0000000000000003', 'support', '客服',
     '只读会话与用户，可禁用 Key；不可改配置',
     '["users.read","sessions.read","keys.read","keys.disable"]'::jsonb),
    ('rol_0000000000000004', 'readonly', '只读',
     '全部只读',
     '["users.read","namespaces.read","discovery.read","sessions.read","billing.read","settings.read","audit.read"]'::jsonb)
ON CONFLICT (code) DO NOTHING;

-- ===========================================================================
-- 四、动态配置
--
-- 这是动态配置的落地点：所有业务可变项都在这里，超级管理员改数据即生效，
-- 不需要改 config.yaml、不需要重启网关。
-- scope 用于管理端分页与权限分组；is_system=true 的行不允许被删除。
-- ===========================================================================
INSERT INTO system_settings (key, value, value_type, scope, description, is_system)
VALUES
    -- 计费
    ('billing.enabled',                'true'::jsonb,        'bool',     'billing',
     '计量与计费总开关', TRUE),
    ('billing.enforce_balance',        'false'::jsonb,       'bool',     'billing',
     '余额不足时是否拒绝请求；初期关闭，避免误伤', TRUE),
    ('billing.initial_grant_micros',   '1000000000000'::jsonb, 'int',    'billing',
     '新账号初始化额度（micros），1e12 = 100 万元', TRUE),
    ('billing.reservation_ttl_seconds','300'::jsonb,         'int',      'billing',
     '预扣单超时秒数，超时后自动释放', TRUE),
    ('billing.default_currency',       '"CNY"'::jsonb,       'string',   'billing',
     '默认结算货币', TRUE),

    -- 录制
    ('recording.enabled',              'true'::jsonb,        'bool',     'recording',
     '会话录制总开关', TRUE),
    ('recording.max_payload_bytes',    '1048576'::jsonb,     'int',      'recording',
     '单条消息正文上限（1 MiB），超出转对象存储', TRUE),
    ('recording.retention_days',       '0'::jsonb,           'int',      'recording',
     '录制保留天数；0 表示永久保留（训练语料）', TRUE),
    ('recording.capture_tool_payloads','true'::jsonb,        'bool',     'recording',
     '是否录制工具调用参数与返回值', TRUE),
    ('recording.redact_sensitive',     'true'::jsonb,        'bool',     'recording',
     '是否对敏感字段统一脱敏（会话录制要求默认开）', TRUE),

    -- 鉴权
    ('auth.key_cache_ttl_seconds',     '30'::jsonb,          'int',      'auth',
     'API Key 校验结果缓存秒数；缩短则更实时、数据库压力更大', TRUE),
    ('auth.rate_limit_enabled',        'false'::jsonb,       'bool',     'auth',
     '限流总开关；初期关闭，先看清真实流量分布', TRUE),
    ('auth.failed_login_lock_threshold','5'::jsonb,          'int',      'auth',
     '管理员连续登录失败锁号阈值', TRUE),
    ('auth.session_idle_minutes',      '30'::jsonb,          'int',      'auth',
     '管理端会话闲置超时分钟数', TRUE),

    -- 发现
    ('discovery.provider',             '"static"'::jsonb,    'string',   'discovery',
     '默认发现提供方；接入 r-nacos 后改为 rnacos', TRUE),
    ('discovery.cache_ttl_seconds',    '10'::jsonb,          'int',      'discovery',
     '实例列表缓存秒数', TRUE),
    ('discovery.fail_open',            'true'::jsonb,        'bool',     'discovery',
     '注册中心不可达时是否沿用上次已知实例', TRUE),

    -- 上游
    ('upstream.default_timeout_ms',    '120000'::jsonb,      'int',      'global',
     '上游 MCP 调用默认超时毫秒数；命名空间可单独覆盖', TRUE)
ON CONFLICT (key) DO NOTHING;

COMMENT ON TABLE system_settings IS
    '动态配置中心的数据源（动态配置）。超级管理员改这里即生效，无需改代码或重启';
