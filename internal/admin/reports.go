package admin

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// defaultUsageWindow 是未指定时间范围时的默认统计窗口。
//
// 选 30 天：管理端打开用量页的第一眼应当是「这个月花了多少」，
// 而不是「从数据库初始化以来的全部」。范围太大查询也会变慢。
const defaultUsageWindow = 30 * 24 * time.Hour

// usageSummaryDTO 是一行用量汇总。
//
// CostMicros 用微元（1e-6 元）而不是浮点元：计费链路上任何一次
// float64 都可能把对账差异放大成真实账单差异（见 billing 包注释）。
type usageSummaryDTO struct {
	Metric     string `json:"metric"`
	Quantity   int64  `json:"quantity"`
	CostMicros int64  `json:"cost_micros"`
	EventCount int64  `json:"event_count"`
}

// usageResponse 带上实际生效的时间窗，前端不必自己猜默认值。
type usageResponse struct {
	UserID string            `json:"user_id"`
	From   string            `json:"from"`
	To     string            `json:"to"`
	Items  []usageSummaryDTO `json:"items"`

	// TotalCostMicros 是 Items 的合计，省掉前端一次 reduce，
	// 也避免前端各自实现时把 int64 溢出成 float。
	TotalCostMicros int64 `json:"total_cost_micros"`
}

// handleUserUsage 汇总某个用户一段时间内的用量。
//
// 查询参数：from / to（RFC3339，均可省略）。
// 省略两者时默认「最近 30 天」，省略 from 表示不设下界、省略 to 表示至今。
func (s *Server) handleUserUsage(w http.ResponseWriter, r *http.Request) {
	userID, err := parseID(idgen.KindUser, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_id", err.Error())
		return
	}

	q := r.URL.Query()
	from, err := parseTime(q.Get("from"), "from")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_from", err.Error())
		return
	}
	to, err := parseTime(q.Get("to"), "to")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_to", err.Error())
		return
	}

	now := s.now()
	if from.IsZero() && to.IsZero() {
		to = now
		from = now.Add(-defaultUsageWindow)
	}
	if from.IsZero() {
		// 只给了 to：取到 to 之前的全部历史。上界必须留零值语义，
		// 由 store 的 `occurred_at >= $2` 处理（零值时间就是 1970 年，等价于不限）。
		from = time.Time{}
	}
	if to.IsZero() {
		to = now
	}
	if !from.IsZero() && !to.After(from) {
		writeError(w, http.StatusBadRequest, "invalid_range", "to 必须晚于 from")
		return
	}

	// 先确认用户存在，否则「查无此人」会返回空报表，看起来像「这个月没花钱」。
	if _, err := s.dir.GetUserByID(r.Context(), userID); err != nil {
		s.fail(w, r, err)
		return
	}

	sums, err := s.reports.SumUsage(r.Context(), userID, from, to)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	items := make([]usageSummaryDTO, 0, len(sums))
	var total int64
	for _, u := range sums {
		items = append(items, usageSummaryDTO{
			Metric:     u.Metric,
			Quantity:   u.Quantity,
			CostMicros: u.CostMicros,
			EventCount: u.EventCount,
		})
		total += u.CostMicros
	}

	resp := usageResponse{
		UserID:          string(userID),
		To:              to.UTC().Format(time.RFC3339),
		Items:           items,
		TotalCostMicros: total,
	}
	if !from.IsZero() {
		resp.From = from.UTC().Format(time.RFC3339)
	}

	writeJSON(w, http.StatusOK, resp)
}

// -----------------------------------------------------------------------------
// 会话录制（训练语料）
// -----------------------------------------------------------------------------

// sessionDTO 是会话列表的一行。
//
// 这是「用户看不见、管理员看得见」的培训语料入口，所以这里同时给出
// 体量与错误计数，让管理员能在不点进去看原文的情况下筛出值得人工标注的会话。
type sessionDTO struct {
	ID            string     `json:"id"`
	TenantID      string     `json:"tenant_id,omitempty"`
	UserID        string     `json:"user_id"`
	APIKeyID      string     `json:"api_key_id,omitempty"`
	NamespaceID   string     `json:"namespace_id,omitempty"`
	NamespaceSlug string     `json:"namespace_slug,omitempty"`
	SessionKey    string     `json:"session_key,omitempty"`
	ClientIP      string     `json:"client_ip,omitempty"`
	UserAgent     string     `json:"user_agent,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
	MessageCount  int        `json:"message_count"`
	InputBytes    int64      `json:"input_bytes"`
	OutputBytes   int64      `json:"output_bytes"`
	ErrorCount    int        `json:"error_count"`
	TrainingLabel string     `json:"training_label,omitempty"`
	Notes         string     `json:"notes,omitempty"`
}

func toSessionDTO(sess *store.Session) sessionDTO {
	dto := sessionDTO{
		ID:            string(sess.ID),
		UserID:        string(sess.UserID),
		NamespaceSlug: sess.NamespaceSlug,
		SessionKey:    sess.SessionKey,
		ClientIP:      sess.ClientIP,
		UserAgent:     sess.UserAgent,
		StartedAt:     sess.StartedAt,
		EndedAt:       sess.EndedAt,
		MessageCount:  sess.MessageCount,
		InputBytes:    sess.InputBytes,
		OutputBytes:   sess.OutputBytes,
		ErrorCount:    sess.ErrorCount,
		TrainingLabel: sess.TrainingLabel,
		Notes:         sess.Notes,
	}
	if !sess.TenantID.IsZero() {
		dto.TenantID = string(sess.TenantID)
	}
	if !sess.APIKeyID.IsZero() {
		dto.APIKeyID = string(sess.APIKeyID)
	}
	if !sess.NamespaceID.IsZero() {
		dto.NamespaceID = string(sess.NamespaceID)
	}
	return dto
}

// handleListSessions 分页查询会话。
//
// 支持的过滤：user_id / namespace / from / to / training_label。
// 全部为可选；不传即全量，这是管理端「先看总量再收窄」的使用习惯。
func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var f store.SessionFilter

	if raw := strings.TrimSpace(q.Get("user_id")); raw != "" {
		id, err := parseID(idgen.KindUser, raw, "user_id")
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_user_id", err.Error())
			return
		}
		f.UserID = id
	}

	f.NamespaceSlug = strings.ToLower(strings.TrimSpace(q.Get("namespace")))

	from, err := parseTime(q.Get("from"), "from")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_from", err.Error())
		return
	}
	to, err := parseTime(q.Get("to"), "to")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_to", err.Error())
		return
	}
	if !from.IsZero() && !to.IsZero() && !to.After(from) {
		writeError(w, http.StatusBadRequest, "invalid_range", "to 必须晚于 from")
		return
	}
	f.From = from
	f.To = to
	f.TrainingLabel = strings.TrimSpace(q.Get("training_label"))

	page, pageSize := pageParams(r)

	sessions, total, err := s.reports.ListSessions(r.Context(), f, page, pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	items := make([]sessionDTO, 0, len(sessions))
	for i := range sessions {
		items = append(items, toSessionDTO(&sessions[i]))
	}

	writeJSON(w, http.StatusOK, store.NewPageResult(items, page, pageSize, total))
}

// sessionMessageDTO 是一条录制消息。
//
// Payload 原样透出（json.RawMessage）：训练语料的价值就在于「用户原话与
// 上游返回的原文」，任何在这里做的改写都会毁掉语料。
// 需要注意 RedactJSON 在录制侧已经做过一次脱敏，此处不再二次处理。
type sessionMessageDTO struct {
	ID           string          `json:"id"`
	SessionID    string          `json:"session_id"`
	Seq          int64           `json:"seq"`
	Direction    string          `json:"direction"`
	RPCID        string          `json:"rpc_id,omitempty"`
	Method       string          `json:"method,omitempty"`
	ToolName     string          `json:"tool_name,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	PayloadRef   string          `json:"payload_ref,omitempty"`
	PayloadBytes int64           `json:"payload_bytes"`
	Truncated    bool            `json:"truncated"`
	Redacted     bool            `json:"redacted"`
	IsError      bool            `json:"is_error"`
	ErrorCode    string          `json:"error_code,omitempty"`
	DurationMS   int             `json:"duration_ms"`
	CreatedAt    time.Time       `json:"created_at"`
}

func (s *Server) handleListSessionMessages(w http.ResponseWriter, r *http.Request) {
	sessionID, err := parseID(idgen.KindSession, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_session_id", err.Error())
		return
	}

	page, pageSize := pageParams(r)

	msgs, total, err := s.reports.ListSessionMessages(r.Context(), sessionID, page, pageSize)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	items := make([]sessionMessageDTO, 0, len(msgs))
	for _, m := range msgs {
		items = append(items, sessionMessageDTO{
			ID:           string(m.ID),
			SessionID:    string(m.SessionID),
			Seq:          m.Seq,
			Direction:    string(m.Direction),
			RPCID:        m.RPCID,
			Method:       m.Method,
			ToolName:     m.ToolName,
			Payload:      m.Payload,
			PayloadRef:   m.PayloadRef,
			PayloadBytes: m.PayloadBytes,
			Truncated:    m.Truncated,
			Redacted:     m.Redacted,
			IsError:      m.IsError,
			ErrorCode:    m.ErrorCode,
			DurationMS:   m.DurationMS,
			CreatedAt:    m.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, store.NewPageResult(items, page, pageSize, total))
}

// labelSessionRequest 给会话打训练语料标签。
type labelSessionRequest struct {
	// Label 是语料筛选标签，空字符串表示清除标签。
	// 允许为空是刻意的：标错了要能撤销，而不是只能换个错标签。
	Label string `json:"label"`

	Notes string `json:"notes"`
}

func (s *Server) handleLabelSession(w http.ResponseWriter, r *http.Request) {
	sessionID, err := parseID(idgen.KindSession, r.PathValue("id"), "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_session_id", err.Error())
		return
	}

	var req labelSessionRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	req.Label = strings.TrimSpace(req.Label)
	if len(req.Label) > 64 {
		writeError(w, http.StatusBadRequest, "invalid_label", "label 最长 64 个字符")
		return
	}
	if len(req.Notes) > 2000 {
		writeError(w, http.StatusBadRequest, "invalid_notes", "notes 最长 2000 个字符")
		return
	}

	if err := s.reports.LabelSession(r.Context(), sessionID, req.Label, req.Notes); err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":             string(sessionID),
		"training_label": req.Label,
		"notes":          req.Notes,
	})
}
