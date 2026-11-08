# hoxkar-mcp-gateway

A multi-tenant, registry-aware gateway for [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) servers.

It turns one or many MCP backends into a single, authenticated, metered HTTP/SSE surface — and discovers
those backends from a service registry (r-nacos) instead of a hard-coded list.

> **Project status: pre-1.0, under active development.**
> The persistence, discovery and registry-synchronisation layers are complete and covered by tests.
> The API-key authentication middleware, the `/admin/v1/*` management API and the namespace routing
> layer are **not implemented yet** — see [Roadmap](#roadmap). Read [Known limitations](#known-limitations)
> before deploying.

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Features](#features)
- [Quick start](#quick-start)
- [Running the gateway](#running-the-gateway)
- [Command-line flags](#command-line-flags)
- [Configuration](#configuration)
- [Environment variables](#environment-variables)
- [Registries and zero-touch onboarding](#registries-and-zero-touch-onboarding)
- [Data model](#data-model)
- [Identity scheme](#identity-scheme)
- [Billing, quotas and metering](#billing-quotas-and-metering)
- [Session recording](#session-recording)
- [Development](#development)
- [Project layout](#project-layout)
- [Roadmap](#roadmap)
- [Known limitations](#known-limitations)
- [中文文档](./README.zh-CN.md)

---

## What it does

MCP servers are usually stdio processes. Wiring them into a networked product means solving the same
problems every time: transport conversion, authentication, per-tenant isolation, rate limiting, usage
accounting, and the fact that the set of available servers changes over time.

This gateway solves them once:

1. **Transport conversion** — a stdio MCP server is exposed over HTTP with Server-Sent Events (SSE).
2. **Dynamic backends** — backends register themselves in r-nacos and send heartbeats. The gateway
   polls the registry and turns each service into a namespace row in PostgreSQL. No code change,
   no config change, no SQL change is needed on the gateway side to onboard a new backend.
3. **Multi-tenant persistence** — tenants, users, API keys, namespaces, permissions, sessions,
   usage events and balances all live in PostgreSQL, keyed by non-sequential, prefixed IDs.
4. **Metering from day one** — every request can produce a usage event, and balances are held in
   micro-units with an integer ledger (no floating point money).
5. **Recording for later use** — request/response pairs are recorded per user, invisible to the user,
   visible to administrators.

The core design principle is **the registry is the source of truth for *which* backends exist**,
while *how* to launch a stdio backend stays in configuration (a registry cannot know command lines).

---

## Architecture

```
                     ┌──────────────────────────────────────────┐
   MCP client        │            hoxkar-mcp-gateway            │
  (SSE / HTTP)       │                                          │
        │            │  ┌──────────────┐   ┌─────────────────┐  │
        └───────────►│  │  SSE server  │──►│  router (M3)    │  │
                     │  │  /sse        │   │  namespace →    │  │
                     │  │  /messages   │   │  backend        │  │
                     │  └──────────────┘   └────────┬────────┘  │
                     │   auth middleware (M2)       │           │
                     │   ┌──────────────┐           │           │
                     │   │ billing      │           │           │
                     │   │ recorder     │           │           │
                     │   └──────┬───────┘           │           │
                     │          │                   ▼           │
                     │   ┌──────┴───────┐   ┌───────────────┐   │
                     │   │ PostgreSQL   │   │ discovery     │   │
                     │   │ (pgvector)   │   │ (r-nacos)     │   │
                     │   └──────▲───────┘   └───────▲───────┘   │
                     └──────────┼───────────────────┼───────────┘
                                │                   │
                     ┌──────────┴──────┐   ┌────────┴─────────┐
                     │ registry sync   │◄──│ r-nacos          │
                     │ (namespaces)    │   │ service catalog  │
                     └─────────────────┘   └────────▲─────────┘
                                                    │ register + heartbeat
                                          ┌─────────┴─────────┐
                                          │  MCP backends     │
                                          │  (stdin/out, HTTP)│
                                          └───────────────────┘
```

Two flows must not be confused, because they answer different questions:

| Flow | Question it answers | Component |
| --- | --- | --- |
| **Discovery** | "Which healthy instances serve service `mcp-blender` *right now*?" | `internal/discovery` |
| **Registry sync** | "Which services exist in the registry at all?" | `internal/registry` |

Discovery is on the request path and is latency-sensitive. Registry sync runs on a timer and writes
rows. Keeping them separate means a registry outage cannot break request routing, and a routing bug
cannot corrupt the namespace table.

---

## Features

- **SSE transport for stdio MCP servers** — the original feature; `--command "npx -y …"` still works.
- **Registry-driven namespaces** — a backend registering `mcp-blender` produces namespace `blender`
  automatically. Deregistration soft-disables it; re-registration restores the same row in place.
- **Soft offline, never delete** — a vanished backend sets `enabled = false` and
  `disabled_by = 'registry_gone'`, keeping its tools and API-key permissions intact. Nothing is lost
  when a backend is temporarily down or renamed back.
- **Source isolation** — namespaces carry `source` (`manual` / `config` / `registry`). The
  synchroniser only ever touches `registry` rows, so it can never overwrite an administrator's manual
  work or a config-declared stdio namespace.
- **Administrator intent wins** — rows disabled by an admin (`disabled_by = 'admin'`) are never
  re-enabled by the synchroniser, and the offline path never overwrites an admin's reason.
- **Admin tuning survives** — values an operator adjusts in the dashboard (`pool_size`, `timeout_ms`,
  `endpoint_path`) are preserved across sync cycles; defaults only apply at first creation.
- **Sharding-ready IDs** — every primary key is either a random string or `shard chars + snowflake`,
  with a type prefix. IDs are validated by `CHECK` constraints in the database, not just in Go.
- **Code-level sharding** — tenant → shard mapping is computed in application code and cached, so
  splitting into multiple physical databases later requires configuration only.
- **Integer money** — balances and prices use micro-units in `BIGINT`; no `FLOAT`, no `NUMERIC`
  rounding surprises.
- **Dynamic settings** — business-tunable values live in `system_settings` and hot-reload via
  `LISTEN/NOTIFY`, with a revision history table.
- **Not-NULL discipline** — `NOT NULL` is used sparingly and the policy is self-checked at boot, so
  schema drift is caught early rather than at runtime.

---

## Quick start

### Prerequisites

- Go **1.25+**
- Docker (for PostgreSQL; optional for r-nacos)
- Node.js / `npx` if you want to try a real MCP server

### 1. Start PostgreSQL

```bash
docker compose up -d
```

This starts `pgvector/pgvector:pg18` with database `mcp_gateway`, user `mcp`, password `mcp_dev_password` on
port 5432. Credentials are development defaults — **change them anywhere else**.

To also start r-nacos (needed for registry-driven namespaces):

```bash
docker compose --profile discovery up -d
```

r-nacos listens on `8848` (HTTP console + OpenAPI), `9848` and `10848` (gRPC). On first start, change
the console's default credentials and never expose port 8848 to the internet.

### 2. Build

```bash
go build -o hoxkar-mcp-gateway .
```

### 3. Create the database schema

There is no separate migrator binary: migrations are embedded into the executable via `go:embed`.

For a fresh database, run once with the reset switch. It drops and rebuilds the `public` schema,
re-applies every migration and seed, self-checks the result, then **exits without serving**:

```bash
./hoxkar-mcp-gateway --config config.yaml --reset-schema --confirm-reset yes
```

Two locks guard this: the flag itself, plus `--confirm-reset yes`, plus the DSN must point at
`localhost` / `127.0.0.1`. Copy-pasting an old shell history line will be rejected, not executed.

Subsequent starts apply any pending migrations automatically.

### 4. Run

```bash
cp config.yaml.example config.yaml
./hoxkar-mcp-gateway --config config.yaml --command "npx -y @modelcontextprotocol/server-filesystem /tmp"
```

The SSE endpoint is then at `http://127.0.0.1:7979/sse`.

---

## Running the gateway

### Legacy single-token mode

The pre-existing behaviour is fully preserved. Without `--config`, no validation is applied and
`--oauth2Bearer` defaults to `ceshi`, exactly as before:

```bash
./hoxkar-mcp-gateway --port 7979 --baseUrl http://127.0.0.1:7979 \
  --command "npx -y @modelcontextprotocol/server-filesystem /tmp" \
  --oauth2Bearer "some-token"
```

This mode is intentionally frozen: `--command` is split on spaces, so existing startup scripts keep
working even though quoted arguments are not supported.

### Multi-tenant mode

Setting `database.dsn` to a non-empty value switches the process into multi-tenant mode. Startup order
is deliberate and load-bearing:

1. load `config.yaml`, then `Validate()` (strict — this is why a broken multi-tenant config fails fast)
2. initialise the snowflake generator
3. open the connection pool and `Ping`
4. build the shard directory wrapper
5. apply migrations
6. if `--reset-schema`: verify schema and exit
7. warm the tenant → shard directory from `tenants`
8. load `system_settings` and start the `LISTEN/NOTIFY` watcher
9. bootstrap `namespaces:` entries from config with `source='config'`
10. build the discovery bundle
11. start billing, recorder
12. start the registry synchroniser
13. start the background maintenance loop
14. serve

Shutdown reverses this: registry sync stops first (it writes to `namespaces`), then maintenance, then
the buffered side-writers are flushed, then the pool closes last.

### Background maintenance

Two independent sweeps, both configurable:

- **Reservation sweep** — releases billing reservations that were never settled, so credit cannot be
  frozen forever by a crashed request.
- **Tenant expiry sweep** — downgrades expired enterprise tenants. Downgrading restricts visibility;
  it never deletes data, so an upgrade/downgrade cycle is lossless.

---

## Command-line flags

| Flag | Default | Description |
| --- | --- | --- |
| `--config <path>` | *(empty)* | Path to `config.yaml`. Empty means legacy mode: command-line only, no strict validation. |
| `--name <string>` | `hoxkar-mcp-gateway` | Server name reported over MCP. |
| `--version <string>` | `1.0.0` | Server version reported over MCP. |
| `--port <int>` | `7979` | TCP port to listen on. |
| `--baseUrl <url>` | `http://127.0.0.1:7979` | Public base URL, used to build the `POST` endpoint handed to SSE clients. |
| `--ssePath <path>` | `/sse` | SSE subscription path. |
| `--messagePath <path>` | `/messages` | Path clients `POST` messages to. |
| `--command <string>` | *(empty)* | stdio MCP command, **split on spaces**. Legacy compatibility. |
| `--oauth2Bearer <string>` | `ceshi` | When non-empty, requires `Authorization: Bearer <value>`. |
| `--reset-schema` | `false` | **Destructive.** Drop and rebuild the `public` schema, then exit. |
| `--confirm-reset <string>` | *(empty)* | Must be exactly `yes` for `--reset-schema` to proceed. |

Explicitly passed flags override `config.yaml`. Flags left at their default are ignored, so they never
clobber file settings. Changing only `--port` also rewrites the port inside `--baseUrl`, but only when
the existing port already equals the old listening port — a `baseUrl` behind a reverse proxy is left
untouched.

---

## Configuration

`config.yaml` is git-ignored; `config.yaml.example` is the committed template. Only put values here
that **require a restart to change**. Business-tunable values (quotas, prices, feature switches,
recording limits) belong in the `system_settings` table and are hot-reloaded by administrators.

### `server`

| Key | Meaning |
| --- | --- |
| `name` / `version` | Reported to MCP clients. |
| `port` | Listen port. |
| `base_url` | Public URL used to construct the message endpoint. |
| `sse_path` / `message_path` | Endpoint paths. |
| `keep_alive_seconds` | SSE heartbeat interval; `0` disables. |
| `max_body_bytes` | Maximum request body size. |
| `cors_allow_origins` | Allowed origins. |

### `database`

| Key | Meaning |
| --- | --- |
| `dsn` | PostgreSQL connection string. Empty ⇒ legacy or development mode. |
| `max_conns` | Pool size. |
| `shard_dsns` | Optional map of `"<shard>" → DSN`. Empty means every shard uses `dsn`; the shard number then lives only in the IDs. Adding entries later needs no code change. |

### `auth`

| Key | Meaning |
| --- | --- |
| `admin_token` | Required in multi-tenant mode (≥16 chars) for the management API. |
| `bearer_token` | Legacy single-token compatibility, equivalent to `--oauth2Bearer`. |
| `key_cache_ttl_seconds` | Local cache lifetime for API-key validation results. |
| `default_rate_limit_per_minute` | Default per-key rate limit. |

### `discovery`

| Key | Meaning |
| --- | --- |
| `provider` | `static` (fixed endpoint list) or `rnacos` (live lookup). |
| `cache_ttl_seconds` | Instance-list cache to avoid hammering the registry per request. |
| `fail_open` | When the registry is unreachable, fall back to `static_endpoints`. Requires a non-empty list, otherwise validation refuses to start. |
| `static_endpoints` | Fallback addresses, `ip:port`. The sole data source when `provider: static`. |
| `rnacos.addr` | r-nacos HTTP base URL. |
| `rnacos.namespace` / `rnacos.group` | Namespace and group to query. |
| `rnacos.username` / `rnacos.password` | Optional; enables token login with automatic re-login on 401/403. |
| `rnacos.timeout_seconds` | HTTP timeout. |
| `rnacos.healthy_only` | Keep only healthy instances. Unhealthy instances are dropped, never forwarded to on a best-effort basis. |

### `registry`

| Key | Meaning |
| --- | --- |
| `auto_discover` | Master switch. Default `false`: the synchroniser writes rows, so a deployment that has not thought it through does not get silently filled with namespaces. |
| `sync_interval_seconds` | Poll period. Too short wastes a full catalog fetch per cycle; too long keeps forwarding to a name that no longer exists. |
| `service_prefix` | Only services with this prefix are synced; the prefix is stripped to form the slug (`mcp-blender` → `blender`). Empty means "every service in the registry is an MCP service" — a decision to make deliberately. |
| `endpoint_path` | Default upstream path for auto-created namespaces. |
| `timeout_ms` | Default upstream timeout. |
| `pool_size` | Default process-pool size. |

Note the coordination requirement: `auto_discover: true` requires `discovery.provider: rnacos`.
`static` has no concept of "which services exist", so that combination is rejected at validation.

`endpoint_path`, `timeout_ms` and `pool_size` are **templates applied at first creation only**.
Afterwards the synchroniser preserves whatever an administrator configured.

### `maintenance`

| Key | Meaning |
| --- | --- |
| `enabled` | Turn the sweeps off entirely (debugging only — reservations will stay frozen). |
| `reservation_sweep_seconds` | Reservation timeout scan period. |
| `tenant_expiry_sweep_seconds` | Tenant expiry scan period. Kept separate because the two failures have different consequences. |
| `batch_size` | Rows per round, so a large backlog is drained over several rounds instead of locking the table. |

### `namespaces`

Startup-time, idempotently upserted with `source='config'`. **Only stdio namespaces belong here** —
command lines, arguments and environment variables are precisely the things a registry cannot know.
Every `http` namespace should come from registry sync instead; keeping a hand-written http list here
directly contradicts the dynamic-management goal.

Two transports:

- `stdio` — the gateway spawns and pools a local subprocess (`command`, `args`, `env`).
- `http` — the backend is an independent service; the gateway resolves it through discovery
  (`discovery_provider`, `discovery_service`, `discovery_group`, `endpoint_path`, `lb_strategy`).

### `idgen`

`node_id` (0–1023). Every replica must use a distinct value or IDs collide.

### `session_recording`

`enabled`, `max_payload_bytes`, `capture_tool_payloads`, `redact_sensitive`, `retention_days`,
`batch_size`, `flush_interval`.

---

## Environment variables

Any setting can be overridden with `MCPGW_` + the path, upper-cased, joined by underscores:

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

> On Windows `cmd`, use the quoted `set "VAR=value"` form. `set VAR=value && next` embeds a trailing
> space into the value and produces confusing failures such as `invalid port ":8848 "`.

---

## Registries and zero-touch onboarding

### Onboarding a new backend

1. The backend starts and registers itself in r-nacos as `mcp-<name>` with a persistent (or ephemeral
   + heartbeat) instance.
2. Within one `sync_interval_seconds`, the synchroniser creates a namespace with
   `slug = <name>`, `transport = http`, `source = registry`.
3. Requests to that namespace are routed to whichever healthy instance discovery returns.

**No gateway code, config or SQL changes are involved.** That is the whole point.

### Offboarding a backend

When the last instance is deregistered:

- the namespace is set to `enabled = false`, `disabled_by = 'registry_gone'`
- its `namespace_tools` and `key_permissions` rows are **kept**
- nothing is deleted

If the backend comes back, the same row is restored in place, so permissions an administrator granted
earlier are still there.

### Why the catalog endpoint, not the service list

This is a subtle trap worth documenting. r-nacos (like Nacos) keeps a service *entry* after its last
instance is deregistered — it becomes an empty shell. The plain service-list endpoint
(`/nacos/v1/ns/service/list`) still returns that shell, so a synchroniser built on it **can never
observe a service disappearing**, and the soft-offline path never fires.

The synchroniser therefore enumerates via the catalog endpoint
(`/nacos/v1/ns/catalog/services`), which returns `ipCount` and `healthyInstanceCount` per service, and
treats a service as alive only if it has instances — matching `healthy_only` so that enumeration and
resolution share one definition of "alive". Without that agreement, a namespace can be reported alive
while resolution returns nothing, leaving an enabled-but-unusable entry.

### Conflict handling

If a registry service maps to a slug that a `manual` or `config` namespace already owns, the sync
counts a conflict and **leaves the existing row untouched**. Two sources of truth are never silently
merged.

### Failure handling

If the catalog fetch fails, the synchroniser offlines **nothing**. A registry outage must never be
interpreted as "every backend disappeared".

---

## Data model

Migrations live in `migrations/` and are embedded into the binary, so an empty `migrations/` directory
means a build that cannot boot — which is why `.gitignore` must never contain `*.sql` or
`migrations`.

| Group | Tables |
| --- | --- |
| Migration bookkeeping | `schema_migrations` |
| Commercial | `plans`, `invoices`, `price_rules` |
| Tenancy & identity | `tenants`, `users`, `tenant_members`, `api_keys` |
| Capability routing | `namespaces`, `namespace_tools`, `key_permissions` |
| Conversation capture | `sessions`, `session_messages` |
| Metering & credit | `usage_events`, `account_balances`, `credit_ledger`, `billing_reservations` |
| Administration | `admin_roles`, `admin_accounts`, `admin_sessions`, `admin_audit_logs` |
| Dynamic configuration | `system_settings`, `system_setting_revisions` |

Schema constraints carry real invariants, not decoration:

- ID columns are checked against a pattern per type, e.g. `^key_[0-9A-HJKMNP-TV-Z]{16}$`.
- `namespaces_registry_consistency_check` — `source='registry'` implies `transport='http'` and a
  non-null `discovery_service`.
- `namespaces_transport_discovery_check` — transport and discovery fields must agree.
- `tenants_shard_not_reserved` — `00` is reserved.
- `api_keys_disabled_consistency` — disabled implies a reason.

`NOT NULL` is applied sparingly, by design: most invariants are enforced in application code, where
they can produce actionable errors. A boot-time self-check verifies the policy still holds, so a
missing constraint is caught on startup rather than months later.

---

## Identity scheme

Two forms, both prefix-tagged so the kind of an ID is readable at a glance:

| Form | Shape | Used for |
| --- | --- | --- |
| **Random** | `<prefix>_` + 16 Crockford Base32 chars (20 total) | Directory-style entities: tenants, users, API keys, namespaces, plans, … |
| **Sharded** | shard chars + snowflake, 19 total | High-volume, time-ordered records |

Properties:

- **No auto-increment integers anywhere.** Sequential IDs leak volume and make enumeration trivial.
- **Crockford Base32** — excludes `I`, `L`, `O`, `U`, so IDs survive being read aloud or copied by hand.
- **Kind is visible** from the prefix (`key_`, `pln_`, `prc_`, `rol_`, `ns_`, …).
- **Sharding is computed in application code** (`internal/shard`), not by the database, so a future
  split across physical databases is a configuration change. The mapping is sticky per tenant and
  cached in memory; a shard number is part of the ID itself and therefore can never be rewritten.

JSON encoding is always a string; numeric JSON IDs are rejected on input.

---

## Billing, quotas and metering

- Prices and balances are integer **micro-units** in `BIGINT` columns.
- Every billable request can emit a `usage_events` row.
- `credit_ledger` is append-only; balances are derived, not overwritten in place.
- Requests **reserve** credit up front, then **settle** with the actual amount, releasing the
  remainder. A crashed request leaves a reservation that the maintenance sweep releases — so a crash
  cannot permanently freeze a user's quota.
- Plan limits are resolved in a fixed precedence: per-user override → plan → defaults, all readable
  from the database so a super administrator can change them without a deploy.
- Initial limits are deliberately very high so that metering can be validated end to end before
  enforcement matters.
- `billing.enforce_balance` defaults to `false`: metering is on, hard blocking is opt-in.

---

## Session recording

Each request/response pair can be persisted against the user, as raw input and raw output.

- Invisible to end users; exposed only through the administrative API.
- Intended as a future training corpus, which is why payload capture is configurable per deployment.
- Sensitive fields (`api_key`, `token`, `password`, `secret`, `authorization`, …) are redacted, and
  well-known token shapes (`sk-…`, `ghp_…`, `xox…-…`, `AKIA…`) are pattern-matched.
- Payloads larger than `max_payload_bytes` are truncated and flagged, not silently dropped.
- **Recorded content must never be echoed back** into an `/sse` or `/messages` response.

---

## Development

```bash
go build ./...
go vet ./...
gofmt -l .
go test ./... -count=1
```

### Unit tests

Run without any external dependency. They are the executable proof for the hard constraints:
ID shape, shard stickiness, `NOT NULL` policy, config validation, and registry-sync decision logic
(including the empty-shell filtering that keeps soft-offline working).

### Acceptance tests

The registry-sync acceptance suite needs a real PostgreSQL and a real r-nacos. It is skipped unless
both environment variables are present:

```bash
# bash
export MCPGW_TEST_DSN='postgres://mcp:mcp_dev_password@127.0.0.1:5432/mcp_gateway?sslmode=disable'
export MCPGW_TEST_RNACOS='http://127.0.0.1:8848'
go test ./internal/registry/ -run TestAcceptance -count=1 -v
```

```cmd
:: Windows cmd — note the quoted set form
set "MCPGW_TEST_DSN=postgres://mcp:mcp_dev_password@127.0.0.1:5432/mcp_gateway?sslmode=disable"
set "MCPGW_TEST_RNACOS=http://127.0.0.1:8848"
go test ./internal/registry/ -run TestAcceptance -count=1 -v
```

The suite registers a throwaway service, asserts namespace creation, deregisters it and asserts the
soft-offline row keeps its tools and permissions, re-registers and asserts in-place restoration, then
checks three guards: an admin-disabled row is not restored, the offline path does not overwrite an
admin's reason, and a slug collision with a `config` namespace is reported as a conflict without
altering the original row. A second test asserts that administrator tuning survives a sync cycle.

It cleans up after itself, but a crashed run can leave a `mcp-syncprobe*` service behind:

```bash
curl -X DELETE "http://127.0.0.1:8848/nacos/v1/ns/service?serviceName=mcp-syncprobe1&groupName=DEFAULT_GROUP&namespaceId=public"
```

### Style notes

- Static SQL, no ORM.
- `pgx/v5` native interface; `CGO_ENABLED=0`.
- Comments explain *why* a non-obvious decision was made, not *what* the next line does.

---

## Project layout

```
.
├── main.go                  # entry point: flags, startup order, shutdown order
├── registry_sync.go         # wiring for the registry synchroniser
├── maintenance.go           # background sweeps
├── bootstrap_gate.go        # post-migration self-checks
├── gateway/
│   ├── sse.go               # SSE transport + message endpoint
│   └── connection.go        # stdio <-> SSE bridging
├── internal/
│   ├── config/              # config.yaml loading, env overrides, validation
│   ├── discovery/           # service name -> healthy instances (static | r-nacos)
│   ├── registry/            # registry catalog -> namespaces rows
│   ├── store/               # PostgreSQL access, one file per domain
│   ├── idgen/               # prefixed ID generation and parsing
│   ├── shard/               # tenant -> shard mapping, per-shard connection pools
│   ├── settings/            # dynamic settings with LISTEN/NOTIFY hot reload
│   ├── quota/               # effective limits: override -> plan -> default
│   ├── billing/             # metering, reserve/settle/release
│   └── recorder/            # session capture + redaction
├── migrations/              # embedded SQL schema, indexes, seed data
└── examples/                # minimal example backend
```

---

## Roadmap

| Milestone | Scope | Status |
| --- | --- | --- |
| **Persistence** | Schema, migrations, prefixed IDs, sharding, settings, dynamic config | ✅ Complete |
| **Registry sync** | Catalog polling, upsert, soft offline, source isolation | ✅ Complete |
| **Authentication** | API-key verification, caching middleware, RBAC, management API | 🚧 Not started |
| **Routing** | Namespace routing and process pool; slow-backend fixture | 🚧 Not started |
| **Admin panel** | Web UI, invoices, richer metering | 📋 Planned |

---

## Known limitations

These are real, present constraints — not a wish list:

1. **The HTTP serving path is not wired for multi-tenant mode yet.** Startup still funnels through the
   legacy SSE server with `--command`. Without `--command` the process exits with
   `exec: no command`. Database work, discovery and registry sync all run first, so the symptom is a
   process that does a lot of correct work and then dies.
2. **`auth.admin_token` is required but no `/admin/v1/*` handler exists.** Validation runs ahead of
   implementation here.
3. **No API-key middleware and no namespace routing.** Requests are not yet authenticated per key and
   not yet routed per namespace.
4. **Repository line endings are mixed (CRLF and LF) with no `.gitattributes`.** `gofmt -l` therefore
   reports most files, because it only accepts LF. Normalising this is a standalone change, never a
   side effect of a feature commit.
5. **Repository line endings are mixed.** Most files are CRLF, but `gofmt` only accepts LF, so
   `gofmt -l` flags them. Normalising this is a standalone change, never a side effect of a feature
   commit.

---

## License

**Limited open source — commercial use requires written authorisation.**

This source is published so it can be read, studied, evaluated and used for non-commercial
purposes. It is **not** unrestricted open source.

**Permitted without asking**

- Reading, cloning and building the source.
- Running it locally, including personal experiments and hobby projects.
- Academic research and teaching.
- Evaluating it to decide whether to license it commercially.

**Requires prior written authorisation**

- Any commercial use: selling it, offering it (or a derivative) as a paid product or hosted
  service, bundling it into a commercial offering, or production deployment inside a for-profit
  organisation beyond an evaluation period.
- Publishing a modified version under a different name.
- Redistributing it as part of another product.

**Contact for licensing**

- Email: **hasitieernuerlan@163.com**
- Please describe your intended use briefly. Commercial licensing terms are negotiated per case.

Nothing in this document grants rights beyond the permissions listed above. All rights not
expressly granted are reserved.

Copyright © hoxkar-mcp-gateway authors.
