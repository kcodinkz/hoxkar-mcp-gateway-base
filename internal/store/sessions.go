package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
)

// Session 是 sessions 表的一行。
//
// TenantID 由鉴权链路带下来，允许为空：录制是旁路，不能因为归属信息
// 一时缺失就把整条会话丢掉（NOT NULL 策略）。
type Session struct {
	ID            idgen.ID
	TenantID      idgen.ID
	UserID        idgen.ID
	APIKeyID      idgen.ID
	NamespaceID   idgen.ID
	NamespaceSlug string
	SessionKey    string
	ClientIP      string
	UserAgent     string
	StartedAt     time.Time
	EndedAt       *time.Time
	MessageCount  int
	InputBytes    int64
	OutputBytes   int64
	ErrorCount    int
	TrainingLabel string
	Notes         string
}

// Direction 是四段式消息方向，训练语料必须能区分用户原话与上游返回。
type Direction string

const (
	DirClientToGateway   Direction = "client_to_gateway"
	DirGatewayToUpstream Direction = "gateway_to_upstream"
	DirUpstreamToGateway Direction = "upstream_to_gateway"
	DirGatewayToClient   Direction = "gateway_to_client"
)

// SessionMessage 是 session_messages 表的一行。
//
// ErrorCode 是 string：JSON-RPC 的 error.code 可能是负数，也可能被上游
// 写成字符串，DDL 里该列就是 TEXT。用 int 会在 Scan 时直接失败。
type SessionMessage struct {
	ID           idgen.ID
	TenantID     idgen.ID
	SessionID    idgen.ID
	Seq          int64
	Direction    Direction
	RPCID        string
	Method       string
	ToolName     string
	Payload      json.RawMessage
	PayloadRef   string
	PayloadBytes int64
	Truncated    bool
	Redacted     bool
	IsError      bool
	ErrorCode    string
	DurationMS   int
	CreatedAt    time.Time
}

// sessionColumns 里的 coalesce(..., ”) 是刻意的：
// api_key_id / namespace_id 都是 TEXT 前缀 ID，NULL 必须折叠成空串才能
// 被 idgen.ID.Scan 接住并还原成 idgen.Zero。若写成 coalesce(..., 0)，
// PG 会因 text 与 integer 类型不一致而直接报错。
const sessionColumns = `id, coalesce(tenant_id, ''), user_id,
                        coalesce(api_key_id, ''), coalesce(namespace_id, ''),
                        coalesce(namespace_slug, ''), coalesce(session_key, ''),
                        coalesce(client_ip, ''), coalesce(user_agent, ''),
                        started_at, ended_at, message_count, input_bytes, output_bytes,
                        error_count, coalesce(training_label, ''), coalesce(notes, '')`

// CreateSession 插入一条会话记录。
func (s *Store) CreateSession(ctx context.Context, sess *Session) error {
	if sess.ID.IsZero() {
		return errors.New("会话 ID 不能为空")
	}
	if sess.StartedAt.IsZero() {
		sess.StartedAt = time.Now()
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO sessions (id, tenant_id, user_id, api_key_id, namespace_id, namespace_slug,
			session_key, client_ip, user_agent, started_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		sess.ID, nullableID(sess.TenantID), sess.UserID,
		nullableID(sess.APIKeyID), nullableID(sess.NamespaceID),
		nullIfEmpty(sess.NamespaceSlug), nullIfEmpty(sess.SessionKey),
		nullIfEmpty(sess.ClientIP), nullIfEmpty(sess.UserAgent), sess.StartedAt)
	if err != nil {
		return fmt.Errorf("创建会话失败: %w", err)
	}
	return nil
}

// EndSession 标记会话结束。
func (s *Store) EndSession(ctx context.Context, id idgen.ID) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE sessions SET ended_at = now() WHERE id = $1 AND ended_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("结束会话失败: %w", err)
	}
	return nil
}

// InsertSessionMessages 批量写入会话消息。
//
// 与计量写入同一原则：单条批量 INSERT，不循环往返。
func (s *Store) InsertSessionMessages(ctx context.Context, msgs []SessionMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	const sql = `
		INSERT INTO session_messages (id, tenant_id, session_id, seq, direction, rpc_id, method,
			tool_name, payload, payload_ref, payload_bytes, truncated, redacted,
			is_error, error_code, duration_ms)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (session_id, seq) DO NOTHING`

	for _, m := range msgs {
		var payload any
		if len(m.Payload) > 0 {
			payload = string(m.Payload)
		}
		batch.Queue(sql,
			m.ID, nullableID(m.TenantID), m.SessionID, m.Seq, string(m.Direction),
			nullIfEmpty(m.RPCID), nullIfEmpty(m.Method), nullIfEmpty(m.ToolName),
			payload, nullIfEmpty(m.PayloadRef), m.PayloadBytes,
			m.Truncated, m.Redacted, m.IsError, nullIfEmpty(m.ErrorCode), m.DurationMS)
	}

	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()

	for range msgs {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("写入会话消息失败: %w", err)
		}
	}
	return nil
}

// GetSession 按主键取会话。
func (s *Store) GetSession(ctx context.Context, id idgen.ID) (*Session, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+sessionColumns+` FROM sessions WHERE id = $1`, id)
	return scanSession(row)
}

// ListSessionMessages 取某个会话的消息，按 seq 升序分页。
//
// 分页遵循分页契约契约，返回 total 供前端渲染页码。
func (s *Store) ListSessionMessages(ctx context.Context, sessionID idgen.ID, page, pageSize int) ([]SessionMessage, int64, error) {
	page, pageSize, offset := normalizePage(page, pageSize)

	var total int64
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM session_messages WHERE session_id = $1`, sessionID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("统计会话消息失败: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, session_id, seq, direction, coalesce(rpc_id, ''), coalesce(method, ''),
		       coalesce(tool_name, ''), payload, coalesce(payload_ref, ''), payload_bytes,
		       truncated, redacted, is_error, error_code, duration_ms, created_at
		FROM session_messages
		WHERE session_id = $1
		ORDER BY seq
		LIMIT $2 OFFSET $3`, sessionID, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("查询会话消息失败: %w", err)
	}
	defer rows.Close()

	var out []SessionMessage
	for rows.Next() {
		var m SessionMessage
		var dir string
		var payload []byte
		if err := rows.Scan(&m.ID, &m.SessionID, &m.Seq, &dir, &m.RPCID, &m.Method,
			&m.ToolName, &payload, &m.PayloadRef, &m.PayloadBytes,
			&m.Truncated, &m.Redacted, &m.IsError, &m.ErrorCode, &m.DurationMS,
			&m.CreatedAt); err != nil {
			return nil, 0, err
		}
		m.Direction = Direction(dir)
		m.Payload = json.RawMessage(payload)
		out = append(out, m)
	}
	return out, total, rows.Err()
}

// SessionFilter 是会话列表的查询条件。
type SessionFilter struct {
	UserID        idgen.ID
	NamespaceSlug string
	From          time.Time
	To            time.Time
	TrainingLabel string
}

// ListSessions 分页查询会话列表。
func (s *Store) ListSessions(ctx context.Context, f SessionFilter, page, pageSize int) ([]Session, int64, error) {
	page, pageSize, offset := normalizePage(page, pageSize)

	where := "WHERE 1=1"
	args := make([]any, 0, 8)
	add := func(cond string, v any) {
		args = append(args, v)
		where += fmt.Sprintf(cond, len(args))
	}

	if !f.UserID.IsZero() {
		add(" AND user_id = $%d", f.UserID)
	}
	if f.NamespaceSlug != "" {
		add(" AND namespace_slug = $%d", f.NamespaceSlug)
	}
	if !f.From.IsZero() {
		add(" AND started_at >= $%d", f.From)
	}
	if !f.To.IsZero() {
		add(" AND started_at < $%d", f.To)
	}
	if f.TrainingLabel != "" {
		add(" AND training_label = $%d", f.TrainingLabel)
	}

	var total int64
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM sessions `+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("统计会话失败: %w", err)
	}

	args = append(args, pageSize, offset)
	// 排序补上 id 作次级键：started_at 是 TIMESTAMPTZ，同一毫秒内可能有多条
	// （批量录制、并发会话），只按 started_at 排序时 PostgreSQL 不保证
	// 同样两行在两次查询里的相对顺序，OFFSET 翻页就会重复或漏行（分页契约）。
	rows, err := s.pool.Query(ctx, `SELECT `+sessionColumns+` FROM sessions `+where+
		fmt.Sprintf(" ORDER BY started_at DESC, id DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args)),
		args...)
	if err != nil {
		return nil, 0, fmt.Errorf("查询会话列表失败: %w", err)
	}
	defer rows.Close()

	var out []Session
	for rows.Next() {
		sess, err := scanSessionRows(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, *sess)
	}
	return out, total, rows.Err()
}

// LabelSession 给会话打训练语料标签。
func (s *Store) LabelSession(ctx context.Context, id idgen.ID, label, notes string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE sessions SET training_label = NULLIF($2, ''), notes = NULLIF($3, '') WHERE id = $1`,
		id, label, notes)
	if err != nil {
		return fmt.Errorf("标记会话失败: %w", err)
	}
	return nil
}

func scanSession(row pgx.Row) (*Session, error) {
	var sess Session
	var ended *time.Time
	err := row.Scan(&sess.ID, &sess.TenantID, &sess.UserID, &sess.APIKeyID, &sess.NamespaceID,
		&sess.NamespaceSlug, &sess.SessionKey, &sess.ClientIP, &sess.UserAgent,
		&sess.StartedAt, &ended, &sess.MessageCount, &sess.InputBytes, &sess.OutputBytes,
		&sess.ErrorCount, &sess.TrainingLabel, &sess.Notes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("读取会话失败: %w", err)
	}
	sess.EndedAt = ended
	return &sess, nil
}

func scanSessionRows(rows pgx.Rows) (*Session, error) {
	var sess Session
	var ended *time.Time
	err := rows.Scan(&sess.ID, &sess.TenantID, &sess.UserID, &sess.APIKeyID, &sess.NamespaceID,
		&sess.NamespaceSlug, &sess.SessionKey, &sess.ClientIP, &sess.UserAgent,
		&sess.StartedAt, &ended, &sess.MessageCount, &sess.InputBytes, &sess.OutputBytes,
		&sess.ErrorCount, &sess.TrainingLabel, &sess.Notes)
	if err != nil {
		return nil, fmt.Errorf("读取会话行失败: %w", err)
	}
	sess.EndedAt = ended
	return &sess, nil
}
