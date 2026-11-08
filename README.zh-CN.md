# hoxkar-mcp-gateway

一个面向 [Model Context Protocol](https://modelcontextprotocol.io/)（MCP）服务器的多租户、注册中心感知网关。

它把任意数量的 MCP 后端收敛成一个带鉴权、带计量的 HTTP/SSE 出口，而且后端清单不是写死的——
它从服务注册中心（r-nacos）里发现。

> **项目状态：pre-1.0，积极开发中。**
> 持久化、服务发现、注册中心同步、API Key 鉴权（缓存 + RBAC）、`/admin/v1/*` 管理接口
> 与命名空间路由（stdio 进程池 + HTTP 后端）均已完成并有测试覆盖。
> 管理面板的 Web UI 尚未构建，详见[路线图](#路线图)。部署前请先读[已知限制](#已知限制)。

- [它解决什么问题](#它解决什么问题)
- [架构](#架构)
- [特性](#特性)
- [快速开始](#快速开始)
- [运行网关](#运行网关)
- [命令行参数](#命令行参数)
- [配置说明](#配置说明)
- [环境变量](#环境变量)
- [注册中心与零改动接入](#注册中心与零改动接入)
- [数据模型](#数据模型)
- [ID 体系](#id-体系)
- [计费、额度与计量](#计费额度与计量)
- [会话录制](#会话录制)
- [开发](#开发)
- [目录结构](#目录结构)
- [路线图](#路线图)
- [已知限制](#已知限制)
- [💡 关于开源范围与演进的说明](#-关于开源范围与演进的说明)
- [致谢与打赏](#致谢与打赏)
- [许可](#许可)
- [English](./README.md)

---

## 它解决什么问题

MCP 服务器通常是 stdio 进程。要把它接进一个网络产品，每次都要重新解决同一批问题：传输转换、
鉴权、租户隔离、限流、用量记账，以及"可用的服务集合本身在变"这件事。

这个网关把这些一次性解决：

1. **传输转换** —— stdio 的 MCP 服务器通过 HTTP + Server-Sent Events（SSE）暴露出去。
2. **后端动态化** —— 后端自己注册进 r-nacos 并持续心跳。网关轮询注册中心，把每个服务变成
   PostgreSQL 里的一行命名空间。新增一个后端，网关侧要改的代码、配置、SQL **都是零**。
3. **多租户持久化** —— 租户、用户、API Key、命名空间、权限、会话、用量事件、余额全部落在
   PostgreSQL，主键使用带前缀的非自增 ID。
4. **第一天就计量** —— 每个请求都能产生一条用量事件；金额以「微单位」整数记账，不碰浮点。
5. **可回放的录制** —— 请求与响应按用户留档，用户看不到，管理员可见。

核心设计原则是：**「有哪些后端」由注册中心说了算，「怎么启动一个 stdio 后端」留在配置里**。
因为注册中心不可能知道命令行。

---

## 架构

```
                     ┌──────────────────────────────────────────┐
   MCP 客户端         │            hoxkar-mcp-gateway            │
  (SSE / HTTP)       │                                          │
        │            │  ┌──────────────┐   ┌─────────────────┐  │
        └───────────►│  │  SSE 服务     │──►│  路由（待实现）  │  │
                     │  │  /sse        │   │  命名空间 → 后端 │  │
                     │  │  /messages   │   │                 │  │
                     │  └──────────────┘   └────────┬────────┘  │
                     │   鉴权中间件（待实现）        │           │
                     │   ┌──────────────┐           │           │
                     │   │ 计费 / 录制   │           │           │
                     │   └──────┬───────┘           │           │
                     │          │                   ▼           │
                     │   ┌──────┴───────┐   ┌───────────────┐   │
                     │   │ PostgreSQL   │   │  服务发现      │   │
                     │   │ (pgvector)   │   │  (r-nacos)    │   │
                     │   └──────▲───────┘   └───────▲───────┘   │
                     └──────────┼───────────────────┼───────────┘
                                │                   │
                     ┌──────────┴──────┐   ┌────────┴─────────┐
                     │  注册中心同步    │◄──│ r-nacos 服务目录 │
                     │  （写命名空间表） │   │                  │
                     └─────────────────┘   └────────▲─────────┘
                                                    │ 注册 + 心跳
                                          ┌─────────┴─────────┐
                                          │    MCP 后端        │
                                          │ (stdio / HTTP)    │
                                          └───────────────────┘
```

两条链路不能混淆，它们回答的是不同问题：

| 链路 | 回答的问题 | 所在包 |
| --- | --- | --- |
| **服务发现** | "服务 `mcp-blender` 此刻有哪些健康实例？" | `internal/discovery` |
| **注册中心同步** | "注册中心里到底存在哪些服务？" | `internal/registry` |

服务发现在请求路径上，对延迟敏感；注册中心同步跑在定时器上，只写表。把两者拆开，意味着
注册中心故障不会打断请求路由，而路由层的 bug 也不会写坏命名空间表。

---

## 特性

- **stdio MCP 服务器的 SSE 出口** —— 最初的功能；`--command "npx -y …"` 依然可用。
- **注册中心驱动的命名空间** —— 后端注册 `mcp-blender`，自动产生命名空间 `blender`；
  注销则软下线；重新注册则原地恢复同一行。
- **只软下线，绝不删行** —— 后端消失时置 `enabled = false`、`disabled_by = 'registry_gone'`，
  它的工具与 Key 授权记录全部保留。后端临时宕机或被改名再改回来，什么都不丢。
- **按来源隔离** —— 命名空间带 `source`（`manual` / `config` / `registry`）。同步器只碰
  `registry` 行，因此永远不可能覆盖管理员的手工配置或配置声明的 stdio 命名空间。
- **管理员意图优先** —— 管理员手动禁用的行（`disabled_by = 'admin'`）不会被同步器重新启用；
  下线路径也不会覆盖管理员填的原因。
- **管理端调参不被抹回** —— 运维在后台调过的 `pool_size`、`timeout_ms`、`endpoint_path`
  会跨同步周期保留；默认值只在首次创建时生效。
- **ID 为分片预留** —— 每个主键要么是随机串，要么是「分片字符 + 雪花」，都带类型前缀。
  ID 合法性由数据库 `CHECK` 约束校验，不只靠 Go 层。
- **代码层分片** —— 租户 → 分片的映射在应用代码里算并缓存，将来拆库只改配置。
- **整数金额** —— 余额与价格用 `BIGINT` 存微单位，没有浮点，没有 `NUMERIC` 舍入意外。
- **动态配置** —— 业务可调项存在 `system_settings`，通过 `LISTEN/NOTIFY` 热加载，
  并带一张修订历史表。
- **NOT NULL 克制使用** —— 只在真正必要处使用，且启动时自检策略是否仍成立，让 schema 漂移
  在启动时暴露，而不是在运行时。
- **统一 API Key 鉴权** —— Key 以哈希形式落库，校验一次后缓存，并带单 Key 限流与
  命名空间/工具级权限（RBAC）；所有裁决都在触达后端之前完成。
- **命名空间路由** —— `/sse/{ns}` 与 `/messages/{ns}` 按命名空间解析到对应后端：
  本地调试走 stdio 进程池，集群流量走 HTTP 反代到发现层给出的实例；
  每次 `tools/call` 都会经过路由层的工具级校验。
- **管理接口** —— `/admin/v1/*` 覆盖租户、用户、Key、命名空间、动态配置、用量与会话审阅，
  使用独立的管理 token 鉴权，绝不与 MCP 客户端凭据混用。

---

## 快速开始

### 前置条件

- Go **1.25+**
- Docker（用于 PostgreSQL；r-nacos 可选）
- Node.js / `npx`（想试真 MCP 服务器的话）

### 1. 启动 PostgreSQL

```bash
docker compose up -d
```

会拉起 `pgvector/pgvector:pg18`，库名 `mcp_gateway`、用户 `mcp`、密码 `mcp_dev_password`，端口 5432。
这是开发默认凭据 —— **在别的环境务必改掉**。

同时启动 r-nacos（注册中心驱动命名空间需要它）：

```bash
docker compose --profile discovery up -d
```

r-nacos 监听 `8848`（HTTP 控制台 + OpenAPI）、`9848`、`10848`（gRPC）。首次启动后请立刻修改
控制台默认账号，并且**不要**把 8848 暴露到公网。

### 2. 编译

```bash
go build -o hoxkar-mcp-gateway .
```

### 3. 建库

没有独立的迁移程序：迁移脚本通过 `go:embed` 编进可执行文件。

全新数据库跑一次 reset 开关。它会删除并重建 `public` schema、重放全部迁移与种子、自检，
然后**直接退出，不提供服务**：

```bash
./hoxkar-mcp-gateway --config config.yaml --reset-schema --confirm-reset yes
```

三道锁：开关本身、`--confirm-reset yes`、以及 DSN 必须指向 `localhost` / `127.0.0.1`。
从终端历史里复制粘贴一整行旧命令会被拒绝，而不是被执行。

此后每次启动会自动应用尚未执行的迁移。

### 4. 启动

```bash
cp config.yaml.example config.yaml
./hoxkar-mcp-gateway --config config.yaml --command "npx -y @modelcontextprotocol/server-filesystem /tmp"
```

SSE 端点即在 `http://127.0.0.1:7979/sse`。

---

## 运行网关

### 旧版单 token 模式

原有行为完整保留。不带 `--config` 时不做严格校验，`--oauth2Bearer` 默认值仍是 `ceshi`：

```bash
./hoxkar-mcp-gateway --port 7979 --baseUrl http://127.0.0.1:7979 \
  --command "npx -y @modelcontextprotocol/server-filesystem /tmp" \
  --oauth2Bearer "some-token"
```

这个模式刻意冻结：`--command` 按空格切分，所以老的启动脚本原样可用（代价是不支持带引号的参数）。

### 多租户模式

`database.dsn` 非空即进入多租户模式。启动顺序是刻意安排的，每一环都有依赖：

1. 加载 `config.yaml`，然后 `Validate()`（严格校验 —— 这就是坏配置会快速失败的原因）
2. 初始化雪花 ID 生成器
3. 打开连接池并 `Ping`
4. 构造分片目录包装
5. 应用迁移
6. 若带 `--reset-schema`：校验 schema 后退出
7. 从 `tenants` 预热「租户 → 分片」目录
8. 加载 `system_settings` 并启动 `LISTEN/NOTIFY` 监听
9. 把 `namespaces:` 条目以 `source='config'` 幂等写入
10. 构造服务发现
11. 启动计费与录制
12. 启动注册中心同步器
13. 启动后台维护循环
14. 提供服务

关闭顺序与之相反：先停同步器（它会写 `namespaces`），再停维护循环，然后冲刷带缓冲的旁路写入，
连接池最后关。

### 后台维护

两个独立扫描，均可配置：

- **预扣扫描** —— 释放永远没被结算的预扣单，使一次崩溃的请求不会永久冻结用户额度。
- **租户到期扫描** —— 降级到期的企业租户。降级只是限制可见性，绝不删数据，所以
  升级/降级一个来回是无损的。

---

## 命令行参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--config <path>` | *(空)* | `config.yaml` 路径。留空即旧模式：只用命令行参数，不做严格校验。 |
| `--name <string>` | `hoxkar-mcp-gateway` | 向 MCP 客户端报告的服务名。 |
| `--version <string>` | `1.0.0` | 向 MCP 客户端报告的版本。 |
| `--port <int>` | `7979` | 监听端口。 |
| `--baseUrl <url>` | `http://127.0.0.1:7979` | 对外基础 URL，用于拼出交给 SSE 客户端的 `POST` 地址。 |
| `--ssePath <path>` | `/sse` | SSE 订阅路径。 |
| `--messagePath <path>` | `/messages` | 客户端 `POST` 消息的路径。 |
| `--command <string>` | *(空)* | stdio MCP 命令，**按空格切分**。兼容旧用法。 |
| `--oauth2Bearer <string>` | `ceshi` | 非空时要求 `Authorization: Bearer <值>`。 |
| `--reset-schema` | `false` | **破坏性操作。** 删库重建 `public` schema 后退出。 |
| `--confirm-reset <string>` | *(空)* | 必须正好是 `yes`，`--reset-schema` 才会执行。 |

显式传的参数覆盖 `config.yaml`；保持默认值的参数会被忽略，因此不会冲掉文件里的设置。
只改 `--port` 时会同步改写 `--baseUrl` 里的端口，但仅当原端口确实等于旧的监听端口时才改 ——
反向代理后面的 `baseUrl` 不会被碰。

---

## 配置说明

`config.yaml` 已被 git 忽略，`config.yaml.example` 是入库模板。这里只放**改一行就要重启进程**的
参数。业务可调项（额度、费率、功能开关、录制上限）属于 `system_settings` 表，由管理员热更新。

### `server`

| 键 | 含义 |
| --- | --- |
| `name` / `version` | 报告给 MCP 客户端。 |
| `port` | 监听端口。 |
| `base_url` | 对外 URL，用于构造消息端点。 |
| `sse_path` / `message_path` | 端点路径。 |
| `keep_alive_seconds` | SSE 心跳间隔，`0` 表示关闭。 |
| `max_body_bytes` | 请求体上限。 |
| `cors_allow_origins` | 允许的来源。 |

### `database`

| 键 | 含义 |
| --- | --- |
| `dsn` | PostgreSQL 连接串。留空即旧模式或开发模式。 |
| `max_conns` | 连接池大小。 |
| `shard_dsns` | 可选的 `"<分片>" → DSN` 映射。留空表示所有分片共用 `dsn`，分片号只体现在 ID 里。将来加条目无需改代码。 |

### `auth`

| 键 | 含义 |
| --- | --- |
| `admin_token` | 多租户模式必填（≥16 字符），供管理接口使用。 |
| `bearer_token` | 旧版单 token 兼容，等价于 `--oauth2Bearer`。 |
| `key_cache_ttl_seconds` | API Key 校验结果的本地缓存时长。 |
| `default_rate_limit_per_minute` | 每个 Key 的默认限流。 |

### `discovery`

| 键 | 含义 |
| --- | --- |
| `provider` | `static`（固定地址清单）或 `rnacos`（实时查询）。 |
| `cache_ttl_seconds` | 实例清单缓存，避免每个请求都打注册中心。 |
| `fail_open` | 注册中心不可用时回退到 `static_endpoints`。要求该清单非空，否则校验拒绝启动。 |
| `static_endpoints` | 兜底地址，`ip:port`。`provider: static` 时是唯一数据源。 |
| `rnacos.addr` | r-nacos HTTP 地址。 |
| `rnacos.namespace` / `rnacos.group` | 查询的命名空间与分组。 |
| `rnacos.username` / `rnacos.password` | 可选；启用后会在 401/403 时自动重新登录。 |
| `rnacos.timeout_seconds` | HTTP 超时。 |
| `rnacos.healthy_only` | 只保留健康实例。不健康的直接丢弃，不做"碰运气转发"。 |

### `registry`

| 键 | 含义 |
| --- | --- |
| `auto_discover` | 总开关。默认 `false`：同步器会写表，没想清楚的部署不该被自动塞进一堆命名空间。 |
| `sync_interval_seconds` | 轮询周期。太短每轮都全量拉目录；太长会继续往已消失的名字上转发。 |
| `service_prefix` | 只同步以此开头的服务名，生成 slug 时剥掉（`mcp-blender` → `blender`）。留空表示"注册中心里所有服务都是 MCP 服务"——这是个需要明确做出的决定。 |
| `endpoint_path` | 自动创建的命名空间的默认上游路径。 |
| `timeout_ms` | 默认上游超时。 |
| `pool_size` | 默认进程池大小。 |

注意这里的联动要求：`auto_discover: true` 要求 `discovery.provider: rnacos`。`static` 没有
"有哪些服务"这个概念，这种组合会在校验阶段被拒。

`endpoint_path`、`timeout_ms`、`pool_size` 是**只在首次创建时生效的模板**。之后同步器会保留
管理员改过的值。

### `maintenance`

| 键 | 含义 |
| --- | --- |
| `enabled` | 完全关闭扫描（仅用于排查问题 —— 预扣单会一直冻结）。 |
| `reservation_sweep_seconds` | 预扣超时扫描周期。 |
| `tenant_expiry_sweep_seconds` | 租户到期扫描周期。分开是因为两种失败的后果不同。 |
| `batch_size` | 单轮处理条数，积压多时分多轮推进，避免长时间锁表。 |

### `namespaces`

启动时以 `source='config'` 幂等写入。**这里只放 stdio 命名空间** —— 命令行、参数、环境变量
正是注册中心不可能知道的东西。所有 `http` 命名空间都应当来自注册中心同步；在这里手写 http
清单与"动态管理"的目标直接冲突。

两种 transport：

- `stdio` —— 网关拉起并池化本地子进程（`command`、`args`、`env`）。
- `http` —— 后端是独立服务，网关通过发现层解析
  （`discovery_provider`、`discovery_service`、`discovery_group`、`endpoint_path`、`lb_strategy`）。

### `idgen`

`node_id`（0–1023）。每个副本必须不同，否则 ID 会撞号。

### `session_recording`

`enabled`、`max_payload_bytes`、`capture_tool_payloads`、`redact_sensitive`、
`retention_days`、`batch_size`、`flush_interval`。

---

## 环境变量

任意配置项都能用 `MCPGW_` + 路径大写 + 下划线覆盖：

```bash
MCPGW_SERVER_PORT=7979
MCPGW_DATABASE_DSN=postgres://mcp:mcp_dev_password@127.0.0.1:5432/mcp_gateway?sslmode=disable
MCPGW_DATABASE_MAX_CONNS=10
MCPGW_AUTH_ADMIN_TOKEN=...
MCPGW_AUTH_BEARER_TOKEN=...
MCPGW_IDGEN_NODE_ID=0
MCPGW_DISCOVERY_PROVIDER=rnacos
MCPGW_DISCOVERY_FAIL_OPEN=true
MCPGW_DISCOVERY_RNACOS_ADDR=http://127.0.0.1:8848
MCPGW_DISCOVERY_RNACOS_NAMESPACE=public
MCPGW_DISCOVERY_RNACOS_USERNAME=...
MCPGW_DISCOVERY_RNACOS_PASSWORD=...
MCPGW_REGISTRY_AUTO_DISCOVER=true
MCPGW_REGISTRY_SYNC_INTERVAL_SECONDS=60
MCPGW_REGISTRY_SERVICE_PREFIX=mcp-
MCPGW_REGISTRY_ENDPOINT_PATH=/mcp
MCPGW_REGISTRY_TIMEOUT_MS=120000
MCPGW_REGISTRY_POOL_SIZE=1
MCPGW_MAINTENANCE_ENABLED=true
MCPGW_MAINTENANCE_BATCH_SIZE=200
```

> Windows `cmd` 下请使用带引号的 `set "VAR=value"` 形式。`set VAR=value && next` 会把
> 尾随空格写进变量值，产生诸如 `invalid port ":8848 "` 这种莫名其妙的问题。

---

## 注册中心与零改动接入

### 接入一个新后端

1. 后端启动，以 `mcp-<名字>` 的身份注册进 r-nacos（持久实例，或临时实例 + 心跳）。
2. 在一个 `sync_interval_seconds` 之内，同步器创建一行命名空间：
   `slug = <名字>`、`transport = http`、`source = registry`。
3. 发往该命名空间的请求会被路由到发现层返回的健康实例。

**全程不涉及任何网关代码、配置或 SQL 改动。** 这就是重点。

### 下线一个后端

当最后一个实例被注销：

- 命名空间置为 `enabled = false`、`disabled_by = 'registry_gone'`
- 它的 `namespace_tools` 与 `key_permissions` 行**保留**
- 不删除任何东西

后端回来后，原地恢复同一行，管理员此前授予的权限仍在。

### 为什么用 catalog 而不是服务清单

这个坑值得写下来。r-nacos（和 Nacos 一样）在最后一个实例注销后**仍保留服务条目**，变成空壳。
普通服务清单接口（`/nacos/v1/ns/service/list`）照样返回这个空壳，于是基于它的同步器
**永远观察不到服务消失**，软下线路径一次都不会触发。

所以同步器改用 catalog 接口（`/nacos/v1/ns/catalog/services`）枚举，它按服务返回 `ipCount` 与
`healthyInstanceCount`，只有确实还有实例的服务才算活着 —— 并且阈值跟随 `healthy_only`，
让「枚举」与「解析」共用同一个"活着"的定义。两者口径不一致时，会出现"命名空间显示可用、
实际解析拿不到任何实例"的启用却不可用状态。

### 冲突处理

若某个注册中心服务映射到的 slug 已被 `manual` 或 `config` 命名空间占用，同步记一次冲突，
**原行完全不动**。两个真相来源绝不静默合并。

### 失败处理

拉取 catalog 失败时，同步器**不下线任何东西**。注册中心故障绝不能被解释成"所有后端都消失了"。

---

## 数据模型

迁移脚本在 `migrations/`，通过 `go:embed` 编进二进制 —— 空的 `migrations/` 目录意味着一份
无法启动的构建，所以 `.gitignore` 里绝不能出现 `*.sql` 或 `migrations`。

| 分组 | 表 |
| --- | --- |
| 迁移记账 | `schema_migrations` |
| 商业 | `plans`、`invoices`、`price_rules` |
| 租户与身份 | `tenants`、`users`、`tenant_members`、`api_keys` |
| 能力路由 | `namespaces`、`namespace_tools`、`key_permissions` |
| 会话捕获 | `sessions`、`session_messages` |
| 计量与账务 | `usage_events`、`account_balances`、`credit_ledger`、`billing_reservations` |
| 管理后台 | `admin_roles`、`admin_accounts`、`admin_sessions`、`admin_audit_logs` |
| 动态配置 | `system_settings`、`system_setting_revisions` |

约束承载的是真实不变量，不是装饰：

- ID 列按类型做模式校验，例如 `^key_[0-9A-HJKMNP-TV-Z]{16}$`。
- `namespaces_registry_consistency_check` —— `source='registry'` 推出 `transport='http'`
  且 `discovery_service` 非空。
- `namespaces_transport_discovery_check` —— transport 与 discovery 字段必须一致。
- `tenants_shard_not_reserved` —— `00` 为保留值。
- `api_keys_disabled_consistency` —— 禁用必须有原因。

`NOT NULL` 被刻意少量使用：多数不变量在应用层校验，那里能给出可操作的错误信息。启动时的自检
会验证该策略仍然成立，于是缺失的约束在启动时就被发现，而不是几个月后。

---

## ID 体系

两种形态，都带前缀，一眼能看出这是什么类型的 ID：

| 形态 | 结构 | 用于 |
| --- | --- | --- |
| **随机型** | `<前缀>_` + 16 位 Crockford Base32 字符（共 20 位） | 目录型实体：租户、用户、API Key、命名空间、套餐…… |
| **分片型** | 分片字符 + 雪花数字，共 19 位 | 高频、时间有序的记录 |

性质：

- **任何地方都没有自增整数主键。** 顺序 ID 会泄露规模，也让枚举变得轻而易举。
- **Crockford Base32** —— 排除 `I`、`L`、`O`、`U`，所以 ID 被口头念出或手抄也不会错。
- **类型从前缀可读**（`key_`、`pln_`、`prc_`、`rol_`、`ns_`……）。
- **分片在应用代码里算**（`internal/shard`），不是数据库算的，所以将来跨物理库拆分只是改配置。
  映射按租户粘性并缓存在内存；分片号本身是 ID 的一部分，因此永远不可改写。

JSON 编码始终是字符串；输入中的数字型 ID 一律拒绝。

---

## 计费、额度与计量

- 价格与余额用 `BIGINT` 存整数**微单位**。
- 每个可计费请求都能产生一行 `usage_events`。
- `credit_ledger` 只追加；余额由账本推导，不就地覆盖。
- 请求先**预扣**额度，再按实际用量**结算**并释放差额。崩溃的请求会留下一条预扣单，
  由维护扫描释放 —— 所以一次崩溃不会永久冻结用户额度。
- 套餐额度按固定优先级解析：逐用户覆盖 → 套餐 → 默认值，全部可从数据库读，
  超级管理员不发布版本就能改。
- 初始额度刻意设得极高，以便在"限制真正生效"之前先端到端验证计量链路。
- `billing.enforce_balance` 默认 `false`：计量开着，硬性拦截是可选项。

---

## 会话录制

每次请求/响应都可按用户留档，原样保存输入与输出。

- 对最终用户不可见，只通过管理接口暴露。
- 定位是未来的训练语料，所以正文捕获策略按部署可配。
- 敏感字段（`api_key`、`token`、`password`、`secret`、`authorization`……）会被脱敏，
  并对已知 token 形态（`sk-…`、`ghp_…`、`xox…-…`、`AKIA…`）做模式匹配。
- 超过 `max_payload_bytes` 的正文会被截断并打标记，而不是静默丢弃。
- **录制内容绝不能回显**到 `/sse` 或 `/messages` 的响应里。

---

## 开发

```bash
go build ./...
go vet ./...
gofmt -l .
go test ./... -count=1
```

### 单元测试

不需要任何外部依赖即可运行。它们是硬约束的可执行证明：ID 形态、分片粘性、`NOT NULL` 策略、
配置校验，以及注册中心同步的判定逻辑（包括那个让软下线真正生效的空壳过滤）。

### 验收测试

注册中心同步的验收套件需要真实 PostgreSQL 与真实 r-nacos。缺少下面两个环境变量时会整体跳过：

```bash
# bash
export MCPGW_TEST_DSN='postgres://mcp:mcp_dev_password@127.0.0.1:5432/mcp_gateway?sslmode=disable'
export MCPGW_TEST_RNACOS='http://127.0.0.1:8848'
go test ./internal/registry/ -run TestAcceptance -count=1 -v
```

```cmd
:: Windows cmd —— 注意用带引号的 set 形式
set "MCPGW_TEST_DSN=postgres://mcp:mcp_dev_password@127.0.0.1:5432/mcp_gateway?sslmode=disable"
set "MCPGW_TEST_RNACOS=http://127.0.0.1:8848"
go test ./internal/registry/ -run TestAcceptance -count=1 -v
```

套件注册一个临时服务、断言命名空间被创建、注销后断言软下线的行仍保留工具与权限、重新注册后
断言原地恢复，然后验三条守卫：管理员禁用的行不会被恢复、下线路径不会覆盖管理员填的原因、
与 `config` 命名空间撞 slug 时记为冲突且原行不变。第二个测试断言管理员调过的参数在同步后
依然保留。

它会自己清理，但异常中断可能留下一个 `mcp-syncprobe*` 服务：

```bash
curl -X DELETE "http://127.0.0.1:8848/nacos/v1/ns/service?serviceName=mcp-syncprobe1&groupName=DEFAULT_GROUP&namespaceId=public"
```

### 风格约定

- 静态 SQL，不用 ORM。
- `pgx/v5` 原生接口；`CGO_ENABLED=0`。
- 注释解释某个不显然的决定**为什么**这样定，而不是下一行**做了什么**。

---

## 目录结构

```
.
├── main.go                  # 入口：参数解析、启动顺序、关闭顺序
├── auth_setup.go            # 多租户模式的鉴权 + 管理端 + 路由装配
├── registry_sync.go         # 注册中心同步的装配
├── maintenance.go           # 后台扫描
├── bootstrap_gate.go        # 迁移后自检
├── gateway/
│   ├── sse.go               # SSE 传输 + 消息端点
│   ├── connection.go        # stdio <-> SSE 桥接
│   └── namespaced.go        # 命名空间 MCP 端点（/sse/{ns}、/messages/{ns}）
├── internal/
│   ├── config/              # config.yaml 加载、环境变量覆盖、校验
│   ├── discovery/           # 服务名 -> 健康实例（static | r-nacos）
│   ├── registry/            # 注册中心目录 -> 命名空间行
│   ├── routing/             # 命名空间 -> 后端（stdio 进程池、HTTP 反代）
│   ├── auth/                # API Key 校验、缓存、中间件、RBAC
│   ├── admin/               # /admin/v1/* 管理接口
│   ├── store/               # PostgreSQL 访问，按领域一个文件
│   ├── idgen/               # 带前缀的 ID 生成与解析
│   ├── shard/               # 租户 -> 分片映射，按分片的连接池
│   ├── settings/            # 动态配置 + LISTEN/NOTIFY 热加载
│   ├── quota/               # 有效额度：覆盖 -> 套餐 -> 默认
│   ├── billing/             # 计量、预扣、结算、释放
│   └── recorder/            # 会话捕获 + 脱敏
├── migrations/              # 内嵌 SQL：schema、索引、种子
└── examples/                # 最小示例后端 + 慢后端测试替身
```

---

## 路线图

| 里程碑 | 范围 | 状态 |
| --- | --- | --- |
| **持久化** | schema、迁移、带前缀 ID、分片、动态配置 | ✅ 完成 |
| **注册中心同步** | 目录轮询、upsert、软下线、来源隔离 | ✅ 完成 |
| **鉴权** | API Key 校验、缓存中间件、RBAC、管理接口 | ✅ 完成 |
| **路由** | 命名空间路由与进程池、慢后端测试替身 | ✅ 完成 |
| **管理面板** | Next.js 14 Web UI、发票、更细的计量 | 📋 计划中 |

---

## 已知限制

以下是当前真实存在的约束，不是愿望清单：

1. **还没有管理面板 Web UI。** `/admin/v1/*` HTTP 接口已完成并有测试覆盖，但驱动它的界面尚未构建，
   目前只能用 `curl` 完成管理操作。
2. **旧版 `--command` 路径是刻意冻结的。** 它仍使用单一共享 token（`--oauth2Bearer`）、
   按空格切分命令行，且完全不认识命名空间。请把它当作既有部署的兼容垫片，
   而不是运行网关的推荐方式。
3. **还没有独立的 `LICENSE` 文件。** 许可条款写在下文[许可](#许可)一节，机器可读的文件仍缺。

---

## 💡 关于开源范围与演进的说明

本仓库（`hoxkar-mcp-gateway-base`）仅仅是**底层、开源的基础控制平面**——即用于桥接、服务发现以及安全路由模型上下文协议（MCP）流量的基础设施。

它被精简定位为一个干净、开箱即用的工业级多租户网关捷径。然而，这只是冰山一角。

真正的核心系统——我们用于驱动分布式垂直领域流水线、有状态异步工作流以及企业级执行网格的闭环架构——作为核心资产完全运行在私有环境中。

*如果你当前的需求不仅仅是基础网关路由，而是希望探讨深度垂直流水线接入、定制化智能体执行或商业化授权，欢迎私下联系。*

---

## 致谢与打赏

打赏完全自愿，不打赏也不影响任何使用。打赏不附带技术支持承诺、不带来优先级、也不构成任何授权
——适用于本源码的条款以[许可](#许可)一节为准。

| 比特币（BTC） | EVM 兼容链（ETH / BSC / Polygon …） | Solana（SOL） |
| --- | --- | --- |
| <img src="./assets/btc.webp" alt="比特币打赏二维码" width="220" /> | <img src="./assets/EVM.webp" alt="EVM 兼容链打赏二维码" width="220" /> | <img src="./assets/solana.webp" alt="Solana 打赏二维码" width="220" /> |

---

## 许可

**有限开源 —— 商用需书面授权。**

本源码公开是为了可阅读、可学习、可评估并可用于非商业用途。它**不是**无限制的开源。

**无需询问即可做**

- 阅读、克隆、构建源码。
- 本地运行，包括个人实验与业余项目。
- 学术研究与教学。
- 为决定是否商用授权而进行评估。

**需要事先书面授权**

- 任何商业用途：售卖、把本软件（或其衍生版本）作为付费产品或托管服务提供、打包进商业产品，
  或在营利机构内超出评估期投入生产使用。
- 以其他名称发布修改后的版本。
- 作为其他产品的组成部分再分发。

**授权联系**

- 邮箱：**hasitieernuerlan@163.com**
- 请简要说明使用场景。商用条款按具体情况商谈。

本文件不授予上述明确许可之外的任何权利。未明确授予的权利均予保留。

Copyright © hoxkar-mcp-gateway authors.
