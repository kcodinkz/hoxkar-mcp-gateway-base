-- 0002_indexes.sql
--
-- 全部索引集中在此，便于一眼看清「哪些查询有支撑、哪些没有」。
--
-- 两条硬规则（ID 形态 / 分页契约）：
--   1. 形态 B（分片表）的业务索引一律以 shard 打头。
--      原因不是性能玄学，而是让「按分片路由」在索引层面就是第一级定位；
--      将来真按 shard 拆库时，这些索引可以原样带过去。
--   2. 目录表的列表排序一律 (created_at, id)，禁止只按 id 排序——
--      形态 A 的随机体没有时序性，按 id 排出来是乱序。
--
-- 部分索引（WHERE ...）刻意保留：软删除/软禁用的行不该占索引体积。

-- ===========================================================================
-- 一、目录层索引
-- ===========================================================================

-- users：按租户检索（管理端「这个租户有哪些人」）
CREATE INDEX IF NOT EXISTS idx_users_tenant ON users (tenant_id, created_at DESC);

-- users：按套餐检索 + 只扫活跃账号
CREATE INDEX IF NOT EXISTS idx_users_plan ON users (plan_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_users_status_active
    ON users (status, created_at DESC) WHERE status = 'active';

-- tenant_members：用户维度的反查（「这个人在哪些租户里」）
CREATE INDEX IF NOT EXISTS idx_tenant_members_user ON tenant_members (user_id);

-- tenants：到期扫描。只覆盖有到期时间的行（personal 恒为 NULL，不进索引）
CREATE INDEX IF NOT EXISTS idx_tenants_expiry
    ON tenants (expires_at) WHERE expires_at IS NOT NULL;

-- api_keys：用户维度的列表
CREATE INDEX IF NOT EXISTS idx_api_keys_user_created
    ON api_keys (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_enabled
    ON api_keys (user_id) WHERE enabled;
-- 鉴权热路径：按明文前缀先缩小范围
CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys (key_prefix);
-- 续费恢复：按禁用原因批量回置（租户与分片 §9.4 第 5 点）
CREATE INDEX IF NOT EXISTS idx_api_keys_disabled_by
    ON api_keys (disabled_by) WHERE disabled_by IS NOT NULL;

-- namespaces：启用态列表
CREATE INDEX IF NOT EXISTS idx_namespaces_enabled ON namespaces (enabled);
CREATE INDEX IF NOT EXISTS idx_namespaces_created
    ON namespaces (created_at DESC);
-- 发现层装载：找出所有走注册中心的启用命名空间
CREATE INDEX IF NOT EXISTS idx_namespaces_discovery
    ON namespaces (discovery_provider, discovery_service)
    WHERE discovery_provider IS NOT NULL AND enabled;
-- 同步器按服务名反查已有行，避免同一服务被重复插入
CREATE INDEX IF NOT EXISTS idx_namespaces_discovery_lookup
    ON namespaces (discovery_group, discovery_service)
    WHERE discovery_service IS NOT NULL;
-- 同步器回收：只扫 registry 行
CREATE INDEX IF NOT EXISTS idx_namespaces_registry_seen
    ON namespaces (last_seen_at) WHERE source = 'registry';

-- namespace_tools：按工具名模糊检索（M6 语义检索前的兜底）
CREATE INDEX IF NOT EXISTS idx_namespace_tools_name_trgm
    ON namespace_tools USING gin (tool_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_namespace_tools_allowed
    ON namespace_tools (namespace_id) WHERE allowed;

-- key_permissions：按命名空间反查（「谁能用这个服务」）
CREATE INDEX IF NOT EXISTS idx_key_permissions_ns
    ON key_permissions (namespace_id, key_id);

-- ===========================================================================
-- 二、形态 B 索引（一律 shard 打头）
-- ===========================================================================

-- sessions：管理端最常见的「某用户最近的会话」
CREATE INDEX IF NOT EXISTS idx_ses_shard_user_started
    ON sessions (shard, user_id, started_at DESC);
-- 全局时间线
CREATE INDEX IF NOT EXISTS idx_ses_shard_started
    ON sessions (shard, started_at DESC);
-- 按命名空间过滤
CREATE INDEX IF NOT EXISTS idx_ses_shard_ns_started
    ON sessions (shard, namespace_id, started_at DESC);
-- 按租户检索（管理员视角，跨用户）
CREATE INDEX IF NOT EXISTS idx_ses_shard_tenant_started
    ON sessions (shard, tenant_id, started_at DESC);
-- 只看已标注的语料
CREATE INDEX IF NOT EXISTS idx_ses_training_label
    ON sessions (training_label) WHERE training_label IS NOT NULL;

-- session_messages：回放（shard + session_id + seq 是物理顺序，也是唯一的读路径）
-- 注意 session_messages_seq_unique 已隐含 (session_id, seq) 的唯一索引，
-- 但那条索引不带 shard，跨分片拆分时用不上，因此这里再显式建一条。
CREATE INDEX IF NOT EXISTS idx_msg_shard_session_seq
    ON session_messages (shard, session_id, seq);
-- 统计：按方法聚合
CREATE INDEX IF NOT EXISTS idx_msg_shard_method
    ON session_messages (shard, method) WHERE method IS NOT NULL;
-- 统计：按工具聚合
CREATE INDEX IF NOT EXISTS idx_msg_shard_tool
    ON session_messages (shard, tool_name) WHERE tool_name IS NOT NULL;
-- 统计：只看错误
CREATE INDEX IF NOT EXISTS idx_msg_shard_errors
    ON session_messages (shard, created_at DESC) WHERE is_error;

-- usage_events：按用户 + 时间（账单页）、按租户 + 时间（超管页）
CREATE INDEX IF NOT EXISTS idx_uev_shard_user_time
    ON usage_events (shard, user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_uev_shard_tenant_time
    ON usage_events (shard, tenant_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_uev_shard_ns_time
    ON usage_events (shard, namespace_slug, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_uev_shard_metric_time
    ON usage_events (shard, metric, occurred_at DESC);
-- 会话关联的对账查询
CREATE INDEX IF NOT EXISTS idx_uev_session
    ON usage_events (session_id) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_uev_request
    ON usage_events (request_id) WHERE request_id IS NOT NULL;

-- credit_ledger：账本流水
CREATE INDEX IF NOT EXISTS idx_led_shard_user_time
    ON credit_ledger (shard, user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_led_shard_type_time
    ON credit_ledger (shard, entry_type, created_at DESC);
-- 幂等：同一个 key 只允许入账一次。这是资金安全的最后一道闸，
-- 应用层也查，但冲突判定必须交给数据库，否则并发下必然重入。
CREATE UNIQUE INDEX IF NOT EXISTS uq_led_idempotency
    ON credit_ledger (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_led_usage_event
    ON credit_ledger (usage_event_id) WHERE usage_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_led_reservation
    ON credit_ledger (reservation_id) WHERE reservation_id IS NOT NULL;

-- billing_reservations：过期回收扫描
CREATE INDEX IF NOT EXISTS idx_rsv_shard_open_expiry
    ON billing_reservations (shard, expires_at) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS idx_rsv_shard_user_time
    ON billing_reservations (shard, user_id, created_at DESC);

-- invoices：用户账期列表
CREATE INDEX IF NOT EXISTS idx_inv_shard_user_time
    ON invoices (shard, user_id, period_start DESC);
CREATE INDEX IF NOT EXISTS idx_inv_shard_status
    ON invoices (shard, status, period_start DESC);

-- ===========================================================================
-- 三、权限与配置索引
-- ===========================================================================

CREATE INDEX IF NOT EXISTS idx_admin_accounts_role ON admin_accounts (role_id);

-- 管理员会话：按管理员查在线设备
CREATE INDEX IF NOT EXISTS idx_asn_admin_created ON admin_sessions (admin_id, created_at DESC);
-- 清理过期会话时只扫未撤销的
CREATE INDEX IF NOT EXISTS idx_asn_active_expiry
    ON admin_sessions (expires_at) WHERE revoked_at IS NULL;

-- 配置变更历史：某个 key 的最近几次变更
CREATE INDEX IF NOT EXISTS idx_rev_key_time
    ON system_setting_revisions (key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rev_admin_time
    ON system_setting_revisions (admin_id, created_at DESC) WHERE admin_id IS NOT NULL;

-- 审计日志：三个最常用维度
CREATE INDEX IF NOT EXISTS idx_aud_created ON admin_audit_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aud_admin_time
    ON admin_audit_logs (admin_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aud_resource
    ON admin_audit_logs (resource_type, resource_id, created_at DESC)
    WHERE resource_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_aud_failed
    ON admin_audit_logs (created_at DESC) WHERE NOT success;

-- 计价规则：同一 (metric, namespace, plan) 组合只允许一条启用规则。
-- coalesce 把 NULL 归一成空串，否则唯一索引对 NULL 不生效、会漏进重复规则。
-- 这条索引是「改数据即改计费口径」的正确性保障，不能省。
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_rules_scope
    ON price_rules (metric, coalesce(namespace_slug, ''), coalesce(plan_code, ''))
    WHERE active;

-- 计价规则：按 metric 查启用规则（查价热路径）
CREATE INDEX IF NOT EXISTS idx_price_rules_metric_active
    ON price_rules (metric) WHERE active;

-- ===========================================================================
-- 四、日报视图
-- ===========================================================================

-- 用量日报：账期汇总与超管看板都基于它。
-- 用视图而不是物化视图：本轮数据量小，实时性更重要；
-- 真需要性能时再加 REFRESH MATERIALIZED VIEW，调用方不用改。
CREATE OR REPLACE VIEW user_usage_daily AS
SELECT user_id,
       namespace_slug,
       metric,
       date_trunc('day', occurred_at) AS day,
       sum(quantity)                  AS quantity,
       sum(cost_micros)               AS cost_micros,
       count(*)                       AS event_count
  FROM usage_events
 GROUP BY user_id, namespace_slug, metric, date_trunc('day', occurred_at);

COMMENT ON VIEW user_usage_daily IS
    '按天聚合的用量视图；账期与看板共用同一口径，避免两处各算一遍';
