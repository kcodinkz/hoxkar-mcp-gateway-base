package admin

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/auth"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// testAdminToken 刻意长于 MinAdminTokenLen，否则 New 会直接拒绝。
const testAdminToken = "test-admin-token-0123456789"

var initIDGenOnce sync.Once

// ensureIDGen 只做一次节点初始化：形态 B（session / session_message 等）
// 的 ID 需要雪花时钟，而形态 A（user / tenant / plan / namespace / api_key）
// 走随机体，不需要初始化。
func ensureIDGen() {
	initIDGenOnce.Do(func() {
		if err := idgen.Init(7); err != nil {
			panic(err)
		}
	})
}

// -----------------------------------------------------------------------------
// 测试替身
// -----------------------------------------------------------------------------

// fakeStore 同时实现 admin.KeyStore / DirectoryStore / ReportStore
// 以及 auth.KeyStore / PermissionStore / NamespaceLookup。
//
// 这不是偷懒：线上这三条链路本来由同一个 *store.Store 承担，管理端的
// 「改了就立刻生效」正是靠共享同一份数据。用两个互不相通的假 Store
// 去测缓存失效，只能证明「我调用了一个方法」，证明不了「结论真的变了」。
type fakeStore struct {
	mu sync.Mutex

	keys     map[idgen.ID]*store.APIKey
	byHash   map[string]*store.APIKey // hex(KeyHash) → key
	users    map[idgen.ID]*store.User
	tenants  map[idgen.ID]*store.Tenant
	plans    map[idgen.ID]*store.Plan
	nsByID   map[idgen.ID]*store.Namespace
	nsBySlug map[string]*store.Namespace

	// perms: keyID → namespaceID → 工具名（"*" 表示全部）
	perms map[idgen.ID]map[idgen.ID][]string

	sessions []store.Session
	messages []store.SessionMessage
	usage    []store.UsageSummary

	findCalls  int
	permCalls  int
	touchCalls int

	errListUsers error
	errGetUser   error
	errCreateKey error
	errUpdateKey error
	errReplace   error
	errLabel     error
	errSum       error

	now time.Time

	lastLimit    int
	lastFilter   store.SessionFilter
	lastFrom     time.Time
	lastTo       time.Time
	lastPage     int
	lastPageSize int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		keys:     make(map[idgen.ID]*store.APIKey),
		byHash:   make(map[string]*store.APIKey),
		users:    make(map[idgen.ID]*store.User),
		tenants:  make(map[idgen.ID]*store.Tenant),
		plans:    make(map[idgen.ID]*store.Plan),
		nsByID:   make(map[idgen.ID]*store.Namespace),
		nsBySlug: make(map[string]*store.Namespace),
		perms:    make(map[idgen.ID]map[idgen.ID][]string),
	}
}

func (f *fakeStore) addPlan(p *store.Plan) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *p
	f.plans[p.ID] = &cp
}

func (f *fakeStore) addUser(u *store.User) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *u
	f.users[u.ID] = &cp
}

func (f *fakeStore) addTenant(t *store.Tenant) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *t
	f.tenants[t.ID] = &cp
}

func (f *fakeStore) addNamespace(ns *store.Namespace) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *ns
	f.nsByID[ns.ID] = &cp
	f.nsBySlug[ns.Slug] = &cp
}

// putKey 同时写入主键索引与哈希索引，让 Finder 与管理端看到同一行。
func (f *fakeStore) putKey(k *store.APIKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *k
	f.keys[k.ID] = &cp
	if len(k.KeyHash) > 0 {
		f.byHash[hex.EncodeToString(k.KeyHash)] = &cp
	}
}

func (f *fakeStore) grant(keyID, nsID idgen.ID, tools ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.perms[keyID]
	if m == nil {
		m = make(map[idgen.ID][]string)
		f.perms[keyID] = m
	}
	cleaned := append([]string(nil), tools...)
	if len(cleaned) == 0 {
		cleaned = []string{"*"}
	}
	m[nsID] = cleaned
}

func (f *fakeStore) addSession(s store.Session) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions = append(f.sessions, s)
}

func (f *fakeStore) addMessage(m store.SessionMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, m)
}

func (f *fakeStore) setUsage(items ...store.UsageSummary) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usage = append([]store.UsageSummary(nil), items...)
}

func (f *fakeStore) namespaceBySlug(slug string) *store.Namespace {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nsBySlug[slug]
}

func (f *fakeStore) key(id idgen.ID) *store.APIKey {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keys[id]
}

func (f *fakeStore) finds() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.findCalls
}

func (f *fakeStore) permsCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.permCalls
}

// --- admin.KeyStore ----------------------------------------------------------

func (f *fakeStore) CreateKeyWithQuotaCheck(_ context.Context, keyID, userID idgen.ID,
	name, prefix string, hash []byte, rateLimit *int, expiresAt *time.Time, limit int) (*store.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.errCreateKey != nil {
		return nil, f.errCreateKey
	}

	u, ok := f.users[userID]
	if !ok {
		return nil, store.ErrNotFound
	}
	if u.Status != store.UserActive {
		return nil, fmt.Errorf("%w: 当前状态 %s", store.ErrUserNotActive, u.Status)
	}

	enabled := 0
	for _, k := range f.keys {
		if k.UserID == userID && k.Enabled {
			enabled++
		}
	}
	if enabled >= limit {
		return nil, &store.KeyQuotaExceededError{Limit: limit, Current: enabled}
	}

	f.lastLimit = limit
	k := &store.APIKey{
		ID:        keyID,
		UserID:    userID,
		Name:      name,
		KeyPrefix: prefix,
		KeyHash:   hash,
		Enabled:   true,
		RateLimit: rateLimit,
		ExpiresAt: expiresAt,
		CreatedAt: f.now,
	}
	f.keys[keyID] = k
	if len(hash) > 0 {
		f.byHash[hex.EncodeToString(hash)] = k
	}

	cp := *k
	return &cp, nil
}

func (f *fakeStore) GetKeyByID(_ context.Context, id idgen.ID) (*store.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *k
	return &cp, nil
}

func (f *fakeStore) ListKeysByUser(_ context.Context, userID idgen.ID) ([]store.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.APIKey, 0, len(f.keys))
	for _, k := range f.keys {
		if k.UserID == userID {
			out = append(out, *k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *fakeStore) UpdateKey(_ context.Context, id idgen.ID, name *string,
	rateLimit *int, expiresAt *time.Time, clearExpiry bool) (*store.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.errUpdateKey != nil {
		return nil, f.errUpdateKey
	}
	k, ok := f.keys[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if name != nil {
		k.Name = *name
	}
	if rateLimit != nil {
		k.RateLimit = rateLimit
	}
	if expiresAt != nil {
		k.ExpiresAt = expiresAt
	}
	if clearExpiry {
		k.ExpiresAt = nil
	}
	cp := *k
	return &cp, nil
}

func (f *fakeStore) EnableKey(_ context.Context, id idgen.ID) (*store.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	k.Enabled = true
	k.DisabledBy = nil
	cp := *k
	return &cp, nil
}

func (f *fakeStore) DisableKeyWithReason(_ context.Context, id idgen.ID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[id]
	if !ok {
		return store.ErrNotFound
	}
	// 与数据库 CHECK 约束同源：enabled 与 disabled_by 必须成对写。
	k.Enabled = false
	reasonCopy := reason
	k.DisabledBy = &reasonCopy
	return nil
}

func (f *fakeStore) ReplacePermissions(_ context.Context, keyID idgen.ID, perms map[string][]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.errReplace != nil {
		return f.errReplace
	}

	next := make(map[idgen.ID][]string, len(perms))
	for slug, tools := range perms {
		ns, ok := f.nsBySlug[slug]
		if !ok {
			// 刻意不包 store.ErrNotFound：真实实现里上游是 pgx.ErrNoRows，
			// 也不是 ErrNotFound，因此这里必须复现「非哨兵错误」的 500 路径。
			return fmt.Errorf("命名空间 %q 不存在", slug)
		}
		cleaned := append([]string(nil), tools...)
		if len(cleaned) == 0 {
			cleaned = []string{"*"}
		}
		next[ns.ID] = cleaned
	}
	f.perms[keyID] = next
	return nil
}

func (f *fakeStore) ListPermissions(_ context.Context, keyID idgen.ID) ([]store.Permission, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	m := f.perms[keyID]
	ids := make([]idgen.ID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := make([]store.Permission, 0, len(ids))
	for _, id := range ids {
		slug := ""
		if ns, ok := f.nsByID[id]; ok {
			slug = ns.Slug
		}
		for _, tool := range m[id] {
			out = append(out, store.Permission{NamespaceID: id, NamespaceSlug: slug, ToolName: tool})
		}
	}
	return out, nil
}

// --- auth.PermissionStore ----------------------------------------------------

func (f *fakeStore) CanUseNamespace(_ context.Context, keyID, namespaceID idgen.ID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.permCalls++
	m := f.perms[keyID]
	if m == nil {
		return false, nil
	}
	_, ok := m[namespaceID]
	return ok, nil
}

func (f *fakeStore) CanUseTool(_ context.Context, keyID, namespaceID idgen.ID, tool string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.permCalls++
	for _, t := range f.perms[keyID][namespaceID] {
		if t == "*" || t == tool {
			return true, nil
		}
	}
	return false, nil
}

// --- admin.DirectoryStore ----------------------------------------------------

func (f *fakeStore) GetUserByID(_ context.Context, id idgen.ID) (*store.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errGetUser != nil {
		return nil, f.errGetUser
	}
	u, ok := f.users[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *u
	return &cp, nil
}

func (f *fakeStore) ListUsers(_ context.Context, page, pageSize int) ([]store.User, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errListUsers != nil {
		return nil, 0, f.errListUsers
	}
	f.lastPage, f.lastPageSize = page, pageSize

	all := make([]store.User, 0, len(f.users))
	for _, u := range f.users {
		all = append(all, *u)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	return paginate(all, page, pageSize)
}

func (f *fakeStore) GetTenantByID(_ context.Context, id idgen.ID) (*store.Tenant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tenants[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (f *fakeStore) ListTenants(_ context.Context, page, pageSize int) ([]store.Tenant, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	all := make([]store.Tenant, 0, len(f.tenants))
	for _, t := range f.tenants {
		all = append(all, *t)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	return paginate(all, page, pageSize)
}

func (f *fakeStore) GetPlanByID(_ context.Context, id idgen.ID) (*store.Plan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.plans[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *p
	return &cp, nil
}

func (f *fakeStore) ListPlans(_ context.Context) ([]store.Plan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Plan, 0, len(f.plans))
	for _, p := range f.plans {
		// 与真实 SQL 一致：只吐启用的套餐。
		if p.Active {
			out = append(out, *p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MaxAPIKeys < out[j].MaxAPIKeys })
	return out, nil
}

func (f *fakeStore) ListNamespaces(_ context.Context) ([]store.Namespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Namespace, 0, len(f.nsByID))
	for _, ns := range f.nsByID {
		out = append(out, *ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out, nil
}

func (f *fakeStore) GetNamespaceBySlug(_ context.Context, slug string) (*store.Namespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns, ok := f.nsBySlug[slug]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *ns
	return &cp, nil
}

func (f *fakeStore) GetNamespaceByID(_ context.Context, id idgen.ID) (*store.Namespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns, ok := f.nsByID[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *ns
	return &cp, nil
}

func (f *fakeStore) DisableNamespace(_ context.Context, id idgen.ID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns, ok := f.nsByID[id]
	if !ok {
		return store.ErrNotFound
	}
	ns.Enabled = false
	reasonCopy := reason
	ns.DisabledBy = &reasonCopy
	return nil
}

func (f *fakeStore) EnableNamespace(_ context.Context, id idgen.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns, ok := f.nsByID[id]
	if !ok {
		return store.ErrNotFound
	}
	ns.Enabled = true
	ns.DisabledBy = nil
	return nil
}

// --- auth.KeyStore -----------------------------------------------------------

func (f *fakeStore) FindKeyByHash(_ context.Context, hash []byte) (*store.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.findCalls++

	k, ok := f.byHash[hex.EncodeToString(hash)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *k
	return &cp, nil
}

func (f *fakeStore) TouchKey(_ context.Context, id idgen.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touchCalls++
	return nil
}

// --- admin.ReportStore -------------------------------------------------------

func (f *fakeStore) SumUsage(_ context.Context, _ idgen.ID, from, to time.Time) ([]store.UsageSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errSum != nil {
		return nil, f.errSum
	}
	f.lastFrom, f.lastTo = from, to
	return append([]store.UsageSummary(nil), f.usage...), nil
}

func (f *fakeStore) ListSessions(_ context.Context, filter store.SessionFilter, page, pageSize int) ([]store.Session, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastFilter = filter

	all := make([]store.Session, 0, len(f.sessions))
	for _, s := range f.sessions {
		if !filter.UserID.IsZero() && s.UserID != filter.UserID {
			continue
		}
		if filter.NamespaceSlug != "" && s.NamespaceSlug != filter.NamespaceSlug {
			continue
		}
		if filter.TrainingLabel != "" && s.TrainingLabel != filter.TrainingLabel {
			continue
		}
		if !filter.From.IsZero() && s.StartedAt.Before(filter.From) {
			continue
		}
		if !filter.To.IsZero() && s.StartedAt.After(filter.To) {
			continue
		}
		all = append(all, s)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	return paginate(all, page, pageSize)
}

func (f *fakeStore) ListSessionMessages(_ context.Context, sessionID idgen.ID, page, pageSize int) ([]store.SessionMessage, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	all := make([]store.SessionMessage, 0, len(f.messages))
	for _, m := range f.messages {
		if m.SessionID == sessionID {
			all = append(all, m)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Seq < all[j].Seq })
	return paginate(all, page, pageSize)
}

func (f *fakeStore) LabelSession(_ context.Context, id idgen.ID, label, notes string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errLabel != nil {
		return f.errLabel
	}
	for i := range f.sessions {
		if f.sessions[i].ID == id {
			f.sessions[i].TrainingLabel = label
			f.sessions[i].Notes = notes
			return nil
		}
	}
	return store.ErrNotFound
}

// paginate 复现 store 层的分页语义：内存切片 + 同一个收敛规则。
//
// 返回值刻意与 store 的方法签名一致（含 error），这样测试替身可以直接
// 把结果透传给接口，不必每处再补一个 nil。
func paginate[T any](items []T, page, pageSize int) ([]T, int64, error) {
	page, pageSize = clampPage(page, pageSize)
	start := (page - 1) * pageSize
	if start >= len(items) {
		return []T{}, int64(len(items)), nil
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], int64(len(items)), nil
}

// -----------------------------------------------------------------------------
// 夹具
// -----------------------------------------------------------------------------

type captureLog struct {
	mu    sync.Mutex
	lines []string
}

func (c *captureLog) logf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *captureLog) joined() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

type fixture struct {
	t    *testing.T
	srv  *Server
	st   *fakeStore
	a    *auth.Authenticator
	az   *auth.Authorizer
	logs *captureLog

	token string
	now   time.Time

	userID     idgen.ID
	tenantID   idgen.ID
	freePlanID idgen.ID
	paidPlanID idgen.ID
	blenderID  idgen.ID
	imagegenID idgen.ID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ensureIDGen()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	st := newFakeStore()
	st.now = now

	f := &fixture{
		t:          t,
		st:         st,
		logs:       &captureLog{},
		token:      testAdminToken,
		now:        now,
		userID:     idgen.MustRandom(idgen.KindUser),
		tenantID:   idgen.MustRandom(idgen.KindTenant),
		freePlanID: idgen.MustRandom(idgen.KindPlan),
		paidPlanID: idgen.MustRandom(idgen.KindPlan),
		blenderID:  idgen.MustRandom(idgen.KindNamespace),
		imagegenID: idgen.MustRandom(idgen.KindNamespace),
	}

	st.addPlan(&store.Plan{
		ID: f.freePlanID, Code: "free", Name: "免费版",
		MaxAPIKeys: 2, RateLimitPerMinute: 30, MaxUploadBytes: 64 << 20,
		MaxConcurrentJobs: 1, MaxNamespaces: 1, MaxPoolSize: 1, Active: true,
	})
	st.addPlan(&store.Plan{
		ID: f.paidPlanID, Code: "pro", Name: "专业版",
		MaxAPIKeys: 10, RateLimitPerMinute: 600, MaxUploadBytes: 512 << 20,
		MaxConcurrentJobs: 4, MaxNamespaces: 10, MaxPoolSize: 8, Active: true,
	})

	st.addTenant(&store.Tenant{
		ID: f.tenantID, Slug: "acme", Name: "Acme",
		Kind: store.TenantEnterprise, Status: store.TenantActive,
		PlanID: f.paidPlanID, Shard: "01",
		CreatedAt: now, UpdatedAt: now,
	})

	st.addUser(&store.User{
		ID: f.userID, Email: "dev@example.com", DisplayName: "开发者",
		TenantID: f.tenantID, PlanID: f.freePlanID,
		Status: store.UserActive, CreatedAt: now,
	})

	st.addNamespace(&store.Namespace{
		ID: f.blenderID, Slug: "blender", Description: "3D 建模能力",
		Transport: store.TransportHTTP, Enabled: true, PoolSize: 2, TimeoutMS: 30000,
		DiscoveryProvider: "rnacos", DiscoveryService: "blender-mcp",
		DiscoveryGroup: "DEFAULT_GROUP", EndpointPath: "/sse",
		LBStrategy: "round_robin", Source: store.SourceRegistry,
		// 故意塞一个敏感值：管理端只允许回显键名。
		Env: map[string]string{"UPSTREAM_TOKEN": "super-secret-value"},
	})
	st.addNamespace(&store.Namespace{
		ID: f.imagegenID, Slug: "imagegen",
		Transport: store.TransportHTTP, Enabled: true, PoolSize: 1, TimeoutMS: 60000,
		Source: store.SourceManual,
	})

	a := auth.New(st, auth.Options{Now: clock})
	az := auth.NewAuthorizer(st, st, auth.RBACOptions{Now: clock})

	srv, err := New(st, st, st, a, az, Options{
		AdminToken: testAdminToken,
		Now:        clock,
		Logf:       f.logs.logf,
	})
	if err != nil {
		t.Fatalf("New 应成功: %v", err)
	}
	f.srv, f.a, f.az = srv, a, az
	return f
}

// call 带管理 token 发请求。
func (f *fixture) call(method, path, body string) *httptest.ResponseRecorder {
	f.t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if r != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+f.token)
	rec := httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(rec, req)
	return rec
}

func (f *fixture) do(req *http.Request) *httptest.ResponseRecorder {
	f.t.Helper()
	rec := httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(rec, req)
	return rec
}

// createKey 建一把 Key 并返回创建响应。
func (f *fixture) createKey(userID idgen.ID, extra string) (createKeyResponse, int) {
	f.t.Helper()
	body := `{"user_id":"` + string(userID) + `","name":"测试 Key"`
	if extra != "" {
		body += "," + extra
	}
	body += "}"
	rec := f.call(http.MethodPost, "/admin/v1/keys", body)
	var resp createKeyResponse
	if rec.Code == http.StatusCreated {
		resp = decodeBody[createKeyResponse](f.t, rec)
	}
	return resp, rec.Code
}

// -----------------------------------------------------------------------------
// 断言工具
// -----------------------------------------------------------------------------

func requireStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("状态码 = %d，期望 %d，body = %s", rec.Code, want, rec.Body.String())
	}
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("解析响应失败: %v，body = %s", err, rec.Body.String())
	}
	return v
}

type errorEnvelope struct {
	Error struct {
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Details json.RawMessage `json:"details"`
	} `json:"error"`
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) errorEnvelope {
	t.Helper()
	return decodeBody[errorEnvelope](t, rec)
}

func requireErrorCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) errorEnvelope {
	t.Helper()
	requireStatus(t, rec, status)
	env := decodeError(t, rec)
	if env.Error.Code != code {
		t.Fatalf("错误码 = %q，期望 %q，body = %s", env.Error.Code, code, rec.Body.String())
	}
	return env
}

type pageEnvelope struct {
	Items      []json.RawMessage `json:"items"`
	Pagination store.Pagination  `json:"pagination"`
}

// requirePageShape 校验分页契约：items 永不为 null，pagination 字段齐全。
func requirePageShape(t *testing.T, rec *httptest.ResponseRecorder) pageEnvelope {
	t.Helper()
	requireStatus(t, rec, http.StatusOK)
	env := decodeBody[pageEnvelope](t, rec)
	if env.Items == nil {
		t.Fatalf("items 不能为 null 或缺失，body = %s", rec.Body.String())
	}
	if env.Pagination.Page < 1 {
		t.Fatalf("pagination.page = %d，应 >= 1", env.Pagination.Page)
	}
	if env.Pagination.PageSize < 1 || env.Pagination.PageSize > store.MaxPageSize {
		t.Fatalf("pagination.page_size = %d，超出范围", env.Pagination.PageSize)
	}
	return env
}

// -----------------------------------------------------------------------------
// 构造参数校验
// -----------------------------------------------------------------------------

func TestNewValidatesOptions(t *testing.T) {
	ensureIDGen()
	st := newFakeStore()
	a := auth.New(st, auth.Options{})
	az := auth.NewAuthorizer(st, st, auth.RBACOptions{})
	longToken := strings.Repeat("k", MinAdminTokenLen)

	cases := []struct {
		name    string
		keys    KeyStore
		dir     DirectoryStore
		reports ReportStore
		token   string
		want    string
	}{
		{"nil keys", nil, st, st, longToken, "数据源不能为空"},
		{"nil directory", st, nil, st, longToken, "数据源不能为空"},
		{"nil reports", st, st, nil, longToken, "数据源不能为空"},
		{"blank token", st, st, st, "   ", "管理 token 不能为空"},
		{"short token", st, st, st, "short", "至少"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.keys, c.dir, c.reports, a, az, Options{AdminToken: c.token}); err == nil ||
				!strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v，期望包含 %q", err, c.want)
			}
		})
	}

	if _, err := New(st, st, st, a, az, Options{AdminToken: longToken}); err != nil {
		t.Fatalf("合法参数不应报错: %v", err)
	}
}

func TestNewToleratesNilAuthHandles(t *testing.T) {
	ensureIDGen()
	st := newFakeStore()
	// 只读部署（报表服务）可以不传 auth / rbac，写操作会失去即时失效。
	srv, err := New(st, st, st, nil, nil, Options{AdminToken: testAdminToken})
	if err != nil {
		t.Fatalf("New 应允许 nil auth: %v", err)
	}
	if srv.Handler() == nil {
		t.Fatal("Handler() 不应为 nil")
	}
	// 触发一次带失效的写路径，确认 nil 安全。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/keys/bogus/disable", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	srv.Handler().ServeHTTP(rec, req)
	requireStatus(t, rec, http.StatusBadRequest)
}

// -----------------------------------------------------------------------------
// 管理 token 门禁
// -----------------------------------------------------------------------------

func TestAdminTokenGuard(t *testing.T) {
	f := newFixture(t)

	denied := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"无凭据", func(*http.Request) {}},
		{"Bearer 错误", func(r *http.Request) { r.Header.Set("Authorization", "Bearer nope") }},
		{"正确 token 的前缀", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+f.token[:8])
		}},
		{"X-Admin-Token 错误", func(r *http.Request) { r.Header.Set("X-Admin-Token", "nope") }},
		{"X-API-Key 不被接受", func(r *http.Request) { r.Header.Set("X-API-Key", f.token) }},
	}

	for _, c := range denied {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/v1/health", nil)
			c.mutate(req)
			rec := f.do(req)

			requireErrorCode(t, rec, http.StatusUnauthorized, "unauthorized")
			if got := rec.Header().Get("WWW-Authenticate"); got != `Bearer realm="admin"` {
				t.Fatalf("WWW-Authenticate = %q", got)
			}
			if got := rec.Header().Get("Cache-Control"); got == "no-store" {
				t.Fatal("失败的鉴权响应不应带 no-store")
			}
		})
	}

	t.Run("Bearer 正确", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/health", nil)
		req.Header.Set("Authorization", "Bearer "+f.token)
		rec := f.do(req)

		requireStatus(t, rec, http.StatusOK)
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q，期望 no-store", got)
		}
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("X-Content-Type-Options = %q", got)
		}

		var body struct {
			Status string `json:"status"`
		}
		body = decodeBody[struct {
			Status string `json:"status"`
		}](t, rec)
		if body.Status != "ok" {
			t.Fatalf("status = %q", body.Status)
		}
	})

	t.Run("X-Admin-Token 正确", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/health", nil)
		req.Header.Set("X-Admin-Token", "  "+f.token+"  ")
		requireStatus(t, f.do(req), http.StatusOK)
	})
}

// -----------------------------------------------------------------------------
// 签发 API Key
// -----------------------------------------------------------------------------

func TestCreateKeyReturnsTokenOnce(t *testing.T) {
	f := newFixture(t)

	rec := f.call(http.MethodPost, "/admin/v1/keys",
		`{"user_id":"`+string(f.userID)+`","name":"本地调试"}`)
	requireStatus(t, rec, http.StatusCreated)

	createRaw := rec.Body.String()
	if strings.Contains(createRaw, "key_hash") {
		t.Fatal("创建响应中不能出现 key_hash")
	}

	resp := decodeBody[createKeyResponse](t, rec)
	if !strings.HasPrefix(resp.Token, "mcpgw_") {
		t.Fatalf("token = %q，期望 mcpgw_ 前缀", resp.Token)
	}
	if resp.Warning == "" {
		t.Fatal("必须提示 token 只显示一次")
	}
	if resp.Key.KeyPrefix == "" || len(resp.Key.KeyPrefix) > auth.DisplayPrefixLen {
		t.Fatalf("key_prefix = %q", resp.Key.KeyPrefix)
	}
	if resp.Key.UserID != string(f.userID) {
		t.Fatalf("user_id = %q", resp.Key.UserID)
	}
	if !resp.Key.Enabled || resp.Key.Expired {
		t.Fatalf("新建 Key 应为启用且未过期: %+v", resp.Key)
	}
	if resp.Key.DisabledBy != nil {
		t.Fatal("新建 Key 不应带 disabled_by")
	}

	// 库侧只保存哈希，而且必须与明文对得上。
	stored := f.st.key(idgen.ID(resp.Key.ID))
	if stored == nil {
		t.Fatal("Key 未写入存储")
	}
	if string(stored.KeyHash) != string(auth.HashToken(resp.Token)) {
		t.Fatal("存储的哈希与返回的明文不匹配")
	}

	// 之后的查询不得回显明文或哈希。
	get := f.call(http.MethodGet, "/admin/v1/keys/"+resp.Key.ID, "")
	requireStatus(t, get, http.StatusOK)
	detail := get.Body.String()
	if strings.Contains(detail, resp.Token) {
		t.Fatal("详情响应不能回显明文 token")
	}
	if strings.Contains(detail, "key_hash") {
		t.Fatal("详情响应不能包含 key_hash")
	}
	if strings.Contains(detail, hex.EncodeToString(stored.KeyHash)) {
		t.Fatal("详情响应不能包含哈希的十六进制形式")
	}
}

func TestCreateKeyValidation(t *testing.T) {
	f := newFixture(t)
	longName := strings.Repeat("a", 121)
	uid := string(f.userID)

	cases := []struct {
		name string
		body string
		code string
	}{
		{"user_id 为空", `{"name":"k"}`, "invalid_user_id"},
		{"user_id 非法", `{"user_id":"nope","name":"k"}`, "invalid_user_id"},
		{"user_id 类型不符", `{"user_id":"` + string(f.blenderID) + `","name":"k"}`, "invalid_user_id"},
		{"name 为空", `{"user_id":"` + uid + `","name":"  "}`, "invalid_name"},
		{"name 过长", `{"user_id":"` + uid + `","name":"` + longName + `"}`, "invalid_name"},
		{"expires_at 格式错", `{"user_id":"` + uid + `","name":"k","expires_at":"明天"}`, "invalid_expires_at"},
		{"expires_at 已过去", `{"user_id":"` + uid + `","name":"k","expires_at":"2020-01-01T00:00:00Z"}`, "invalid_expires_at"},
		{"rate_limit 为负", `{"user_id":"` + uid + `","name":"k","rate_limit":-1}`, "invalid_rate_limit"},
		{"未知字段", `{"user_id":"` + uid + `","name":"k","bogus":1}`, "invalid_json"},
		{"不是 JSON", `not-json`, "invalid_json"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := f.call(http.MethodPost, "/admin/v1/keys", c.body)
			requireErrorCode(t, rec, http.StatusBadRequest, c.code)
		})
	}
}

func TestCreateKeyMapsStoreErrors(t *testing.T) {
	f := newFixture(t)

	t.Run("用户不存在", func(t *testing.T) {
		unknown := idgen.MustRandom(idgen.KindUser)
		rec := f.call(http.MethodPost, "/admin/v1/keys",
			`{"user_id":"`+string(unknown)+`","name":"k"}`)
		requireErrorCode(t, rec, http.StatusNotFound, "not_found")
	})

	t.Run("用户被停用", func(t *testing.T) {
		uid := idgen.MustRandom(idgen.KindUser)
		f.st.addUser(&store.User{
			ID: uid, Email: "suspended@example.com", PlanID: f.freePlanID,
			Status: store.UserSuspended, CreatedAt: f.now,
		})
		rec := f.call(http.MethodPost, "/admin/v1/keys",
			`{"user_id":"`+string(uid)+`","name":"k"}`)
		env := requireErrorCode(t, rec, http.StatusConflict, "user_not_active")
		if !strings.Contains(env.Error.Message, "不可用") {
			t.Fatalf("message = %q", env.Error.Message)
		}
	})

	t.Run("配额超限", func(t *testing.T) {
		// 无租户 → 只按用户套餐（free：max_api_keys=2）解析额度。
		uid := idgen.MustRandom(idgen.KindUser)
		f.st.addUser(&store.User{
			ID: uid, Email: "quota@example.com", PlanID: f.freePlanID,
			Status: store.UserActive, CreatedAt: f.now,
		})
		for i := 0; i < 2; i++ {
			f.st.putKey(&store.APIKey{
				ID:        idgen.MustRandom(idgen.KindAPIKey),
				UserID:    uid,
				Name:      fmt.Sprintf("已存在-%d", i),
				KeyPrefix: "mcpgw_AAAA",
				KeyHash:   []byte{byte(i + 1)},
				Enabled:   true,
				CreatedAt: f.now,
			})
		}

		rec := f.call(http.MethodPost, "/admin/v1/keys",
			`{"user_id":"`+string(uid)+`","name":"第三把"}`)
		env := requireErrorCode(t, rec, http.StatusConflict, "key_quota_exceeded")
		if len(env.Error.Details) == 0 {
			t.Fatal("配额错误必须带 details")
		}
		var details struct {
			Limit   int `json:"limit"`
			Current int `json:"current"`
		}
		if err := json.Unmarshal(env.Error.Details, &details); err != nil {
			t.Fatalf("解析 details 失败: %v", err)
		}
		if details.Limit != 2 || details.Current != 2 {
			t.Fatalf("details = %+v，期望 limit=2 current=2", details)
		}
	})
}

func TestCreateKeyEnterpriseTenantRaisesLimit(t *testing.T) {
	f := newFixture(t)
	// 夹具用户挂在企业租户下（租户套餐 pro：max_api_keys=10），
	// 因此额度应当按租户套餐解析，而不是用户自己的 free 套餐（2）。
	for i := 0; i < 3; i++ {
		if _, code := f.createKey(f.userID, ""); code != http.StatusCreated {
			t.Fatalf("第 %d 把 Key 应创建成功，状态码 = %d", i+1, code)
		}
	}
	if got := f.st.lastLimit; got != 10 {
		t.Fatalf("解析出的 max_api_keys = %d，期望 10（租户套餐优先）", got)
	}
}

func TestCreateKeyPermissionsFailureKeepsToken(t *testing.T) {
	f := newFixture(t)
	f.st.errReplace = errors.New("namespaces 表不可用")

	rec := f.call(http.MethodPost, "/admin/v1/keys",
		`{"user_id":"`+string(f.userID)+`","name":"带权限","permissions":{"blender":[]}}`)
	env := requireErrorCode(t, rec, http.StatusConflict, "permissions_failed")
	if len(env.Error.Details) == 0 {
		t.Fatal("授权失败必须回传 key_id 与 token，否则这把 Key 会变成孤儿")
	}
	var details struct {
		KeyID string `json:"key_id"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal(env.Error.Details, &details); err != nil {
		t.Fatalf("解析 details 失败: %v", err)
	}
	if details.KeyID == "" || details.Token == "" {
		t.Fatalf("details = %+v", details)
	}
	if !strings.Contains(f.logs.joined(), "重试授权") {
		t.Fatalf("应记录待重试授权的日志，实际日志 = %q", f.logs.joined())
	}
	// Key 本身已经落库，但没有任何权限行，等价于「安全的半成品」。
	if k := f.st.key(idgen.ID(details.KeyID)); k == nil {
		t.Fatal("Key 应当已创建")
	}
}

func TestCreateKeyWithPermissions(t *testing.T) {
	f := newFixture(t)
	resp, code := f.createKey(f.userID, `"permissions":{"blender":["render"]}`)
	if code != http.StatusCreated {
		t.Fatalf("状态码 = %d", code)
	}
	perms, err := f.st.ListPermissions(context.Background(), idgen.ID(resp.Key.ID))
	if err != nil {
		t.Fatalf("ListPermissions: %v", err)
	}
	if len(perms) != 1 || perms[0].ToolName != "render" {
		t.Fatalf("permissions = %+v", perms)
	}
}

// -----------------------------------------------------------------------------
// 查询与更新 API Key
// -----------------------------------------------------------------------------

func TestGetKeyErrors(t *testing.T) {
	f := newFixture(t)

	requireErrorCode(t, f.call(http.MethodGet, "/admin/v1/keys/nope", ""),
		http.StatusBadRequest, "invalid_key_id")

	// 类型不符：传一个命名空间 ID 进 Key 接口。
	requireErrorCode(t, f.call(http.MethodGet, "/admin/v1/keys/"+string(f.blenderID), ""),
		http.StatusBadRequest, "invalid_key_id")

	requireErrorCode(t, f.call(http.MethodGet, "/admin/v1/keys/"+string(idgen.MustRandom(idgen.KindAPIKey)), ""),
		http.StatusNotFound, "not_found")
}

func TestUpdateKeyValidationAndSuccess(t *testing.T) {
	f := newFixture(t)
	created, code := f.createKey(f.userID, `"expires_at":"2099-01-01T00:00:00Z"`)
	if code != http.StatusCreated {
		t.Fatalf("状态码 = %d", code)
	}
	path := "/admin/v1/keys/" + created.Key.ID

	cases := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"清空与设置到期冲突", `{"clear_expiry":true,"expires_at":"2099-01-01T00:00:00Z"}`,
			http.StatusBadRequest, "conflicting_expiry"},
		{"name 改为空", `{"name":"   "}`, http.StatusBadRequest, "invalid_name"},
		{"name 过长", `{"name":"` + strings.Repeat("b", 121) + `"}`, http.StatusBadRequest, "invalid_name"},
		{"rate_limit 为负", `{"rate_limit":-5}`, http.StatusBadRequest, "invalid_rate_limit"},
		{"expires_at 已过去", `{"expires_at":"2000-01-01T00:00:00Z"}`, http.StatusBadRequest, "invalid_expires_at"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			requireErrorCode(t, f.call(http.MethodPatch, path, c.body), c.status, c.code)
		})
	}

	t.Run("改名与限流", func(t *testing.T) {
		rec := f.call(http.MethodPatch, path, `{"name":"  生产环境  ","rate_limit":120}`)
		requireStatus(t, rec, http.StatusOK)
		dto := decodeBody[keyDTO](t, rec)
		if dto.Name != "生产环境" {
			t.Fatalf("name = %q，应已 trim", dto.Name)
		}
		if dto.RateLimit == nil || *dto.RateLimit != 120 {
			t.Fatalf("rate_limit = %v", dto.RateLimit)
		}
		if dto.ExpiresAt == nil {
			t.Fatal("未传 clear_expiry 时不应清空到期时间")
		}
	})

	t.Run("显式清空到期", func(t *testing.T) {
		rec := f.call(http.MethodPatch, path, `{"clear_expiry":true}`)
		requireStatus(t, rec, http.StatusOK)
		if strings.Contains(rec.Body.String(), "expires_at") {
			t.Fatalf("清空后不应再输出 expires_at，body = %s", rec.Body.String())
		}
		if dto := decodeBody[keyDTO](t, rec); dto.Expired {
			t.Fatal("永不过期的 Key 不应标记 expired")
		}
	})

	t.Run("未知字段被拒", func(t *testing.T) {
		requireErrorCode(t, f.call(http.MethodPatch, path, `{"name":"x","enabled":false}`),
			http.StatusBadRequest, "invalid_json")
	})
}

// -----------------------------------------------------------------------------
// 禁用 / 启用 —— 行为级验证缓存失效
// -----------------------------------------------------------------------------

func TestDisableKeyInvalidatesAuthCache(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	plaintext, prefix, hash, err := auth.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	keyID := idgen.MustRandom(idgen.KindAPIKey)
	f.st.putKey(&store.APIKey{
		ID: keyID, UserID: f.userID, Name: "CI", KeyPrefix: prefix,
		KeyHash: hash, Enabled: true, CreatedAt: f.now,
	})

	// 前两次鉴权：第一次查库，第二次命中正缓存。
	if _, err := f.a.Authenticate(ctx, plaintext); err != nil {
		t.Fatalf("首次鉴权失败: %v", err)
	}
	if _, err := f.a.Authenticate(ctx, plaintext); err != nil {
		t.Fatalf("二次鉴权失败: %v", err)
	}
	if got := f.st.finds(); got != 1 {
		t.Fatalf("第二次鉴权应命中缓存，查库次数 = %d", got)
	}

	rec := f.call(http.MethodPost, "/admin/v1/keys/"+string(keyID)+"/disable", "")
	requireStatus(t, rec, http.StatusOK)

	var body map[string]any
	body = decodeBody[map[string]any](t, rec)
	if body["reason"] != store.DisabledByAdmin {
		t.Fatalf("reason = %v，必须是 %q，否则自动续费恢复会连人工封禁一起解开",
			body["reason"], store.DisabledByAdmin)
	}
	if body["enabled"] != false {
		t.Fatalf("enabled = %v", body["enabled"])
	}
	if body["id"] != string(keyID) {
		t.Fatalf("id = %v", body["id"])
	}

	// 关键断言：禁用立刻生效，不能等 TTL。
	_, err = f.a.Authenticate(ctx, plaintext)
	if !errors.Is(err, auth.ErrKeyDisabled) {
		t.Fatalf("禁用后鉴权 err = %v，期望 ErrKeyDisabled", err)
	}
	if got := f.st.finds(); got != 2 {
		t.Fatalf("失效后必须重新查库，查库次数 = %d", got)
	}

	// 重新启用：复用同一路由，确认 enabled 与 disabled_by 成对回滚。
	rec = f.call(http.MethodPost, "/admin/v1/keys/"+string(keyID)+"/enable", "")
	requireStatus(t, rec, http.StatusOK)
	dto := decodeBody[keyDTO](t, rec)
	if !dto.Enabled || dto.DisabledBy != nil {
		t.Fatalf("启用后状态不对: %+v", dto)
	}
}

func TestKeyActionsOnUnknownKey(t *testing.T) {
	f := newFixture(t)
	missing := string(idgen.MustRandom(idgen.KindAPIKey))

	requireErrorCode(t, f.call(http.MethodPost, "/admin/v1/keys/"+missing+"/disable", ""),
		http.StatusNotFound, "not_found")
	requireErrorCode(t, f.call(http.MethodPost, "/admin/v1/keys/"+missing+"/enable", ""),
		http.StatusNotFound, "not_found")
	requireErrorCode(t, f.call(http.MethodPut, "/admin/v1/keys/"+missing+"/permissions", `{"namespaces":{}}`),
		http.StatusNotFound, "not_found")
}

// -----------------------------------------------------------------------------
// 权限
// -----------------------------------------------------------------------------

func TestReplacePermissionsInvalidatesRBACCache(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	created, code := f.createKey(f.userID, "")
	if code != http.StatusCreated {
		t.Fatalf("状态码 = %d", code)
	}
	keyID := idgen.ID(created.Key.ID)
	ns := f.st.namespaceBySlug("blender")
	f.st.grant(keyID, ns.ID, "*")

	p := &auth.Principal{KeyID: keyID, UserID: f.userID}

	for i := 0; i < 2; i++ {
		ok, err := f.az.AllowsNamespace(ctx, p, ns)
		if err != nil || !ok {
			t.Fatalf("第 %d 次裁决: ok = %v, err = %v", i+1, ok, err)
		}
	}
	if got := f.st.permsCalls(); got != 1 {
		t.Fatalf("第二次裁决应命中缓存，权限查询次数 = %d", got)
	}

	rec := f.call(http.MethodPut, "/admin/v1/keys/"+string(keyID)+"/permissions", `{"namespaces":{}}`)
	requireStatus(t, rec, http.StatusOK)

	ok, err := f.az.AllowsNamespace(ctx, p, ns)
	if err != nil {
		t.Fatalf("裁决失败: %v", err)
	}
	if ok {
		t.Fatal("权限收回后必须立刻拒绝，不能等 TTL")
	}
	if got := f.st.permsCalls(); got != 2 {
		t.Fatalf("失效后必须重新裁决，权限查询次数 = %d", got)
	}
}

func TestReplacePermissionsValidation(t *testing.T) {
	f := newFixture(t)
	created, code := f.createKey(f.userID, "")
	if code != http.StatusCreated {
		t.Fatalf("状态码 = %d", code)
	}
	path := "/admin/v1/keys/" + created.Key.ID + "/permissions"

	t.Run("缺少 namespaces 字段", func(t *testing.T) {
		requireErrorCode(t, f.call(http.MethodPut, path, `{}`),
			http.StatusBadRequest, "invalid_namespaces")
		requireErrorCode(t, f.call(http.MethodPut, path, `{"namespaces":null}`),
			http.StatusBadRequest, "invalid_namespaces")
	})

	t.Run("slug 为空", func(t *testing.T) {
		requireErrorCode(t, f.call(http.MethodPut, path, `{"namespaces":{"   ":[]}}`),
			http.StatusBadRequest, "invalid_namespace_slug")
	})

	t.Run("归一化与清理", func(t *testing.T) {
		rec := f.call(http.MethodPut, path,
			`{"namespaces":{"  Blender  ["render"],"imagegen":[""]}}`)
		// 上面故意写坏 JSON 的先验校验：先确认坏 JSON 被拒。
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("非法 JSON 应被拒，状态码 = %d", rec.Code)
		}

		rec = f.call(http.MethodPut, path,
			`{"namespaces":{"  Blender  ":[" render ","*",""],"imagegen":[]}}`)
		requireStatus(t, rec, http.StatusOK)

		dto := decodeBody[permissionDTO](t, rec)
		if got := dto.Namespaces["blender"]; len(got) != 1 || got[0] != "render" {
			t.Fatalf("blender = %v，期望 [render]（trim 后丢弃 * 与空串）", got)
		}
		// 空数组表示「该命名空间下全部工具」，归一时落成空切片。
		if got, ok := dto.Namespaces["imagegen"]; !ok || got == nil || len(got) != 0 {
			t.Fatalf("imagegen = %v，期望空数组", got)
		}
	})
}

func TestGetPermissionsCollapsesWildcard(t *testing.T) {
	f := newFixture(t)
	created, code := f.createKey(f.userID, "")
	if code != http.StatusCreated {
		t.Fatalf("状态码 = %d", code)
	}
	keyID := idgen.ID(created.Key.ID)
	f.st.grant(keyID, f.blenderID, "*")
	f.st.grant(keyID, f.imagegenID, "render")

	rec := f.call(http.MethodGet, "/admin/v1/keys/"+string(keyID)+"/permissions", "")
	requireStatus(t, rec, http.StatusOK)

	dto := decodeBody[permissionDTO](t, rec)
	if dto.KeyID != string(keyID) {
		t.Fatalf("key_id = %q", dto.KeyID)
	}
	tools, ok := dto.Namespaces["blender"]
	if !ok {
		t.Fatal("blender 必须出现（即使是全部工具）")
	}
	if tools == nil {
		t.Fatal("全部工具应序列化为 []，而不是 null")
	}
	if len(tools) != 0 {
		t.Fatalf("blender = %v，'*' 不应出现在数组里", tools)
	}
	if got := dto.Namespaces["imagegen"]; len(got) != 1 || got[0] != "render" {
		t.Fatalf("imagegen = %v", got)
	}
}

func TestPermissionsOnMissingKeyReturnsEmpty(t *testing.T) {
	f := newFixture(t)
	missing := string(idgen.MustRandom(idgen.KindAPIKey))

	rec := f.call(http.MethodGet, "/admin/v1/keys/"+missing+"/permissions", "")
	requireStatus(t, rec, http.StatusOK)
	dto := decodeBody[permissionDTO](t, rec)
	if dto.Namespaces == nil {
		t.Fatal("namespaces 应为 {} 而不是 null")
	}
	if len(dto.Namespaces) != 0 {
		t.Fatalf("namespaces = %v", dto.Namespaces)
	}
}

// -----------------------------------------------------------------------------
// 目录：用户 / 租户 / 套餐 / 命名空间
// -----------------------------------------------------------------------------

func TestUserDTOExpiredFlag(t *testing.T) {
	f := newFixture(t)
	uid := idgen.MustRandom(idgen.KindUser)
	past := f.now.Add(-time.Hour)
	f.st.addUser(&store.User{
		ID: uid, Email: "old@example.com", PlanID: f.freePlanID,
		Status: store.UserActive, ExpiresAt: &past, CreatedAt: f.now,
	})

	rec := f.call(http.MethodGet, "/admin/v1/users/"+string(uid), "")
	requireStatus(t, rec, http.StatusOK)

	dto := decodeBody[userDTO](t, rec)
	if !dto.Expired {
		t.Fatal("已过期的用户应标记 expired")
	}
	if dto.TenantID != "" {
		t.Fatalf("无租户时不应输出 tenant_id，实际 = %q", dto.TenantID)
	}
	if dto.Status != string(store.UserActive) {
		t.Fatalf("status = %q", dto.Status)
	}
	// 用户接口不得回显任何凭据字段。
	if strings.Contains(rec.Body.String(), "key_hash") || strings.Contains(rec.Body.String(), "password") {
		t.Fatal("用户接口不应包含凭据字段")
	}
}

func TestListUserKeysRequiresExistingUser(t *testing.T) {
	f := newFixture(t)
	unknown := idgen.MustRandom(idgen.KindUser)

	requireErrorCode(t, f.call(http.MethodGet, "/admin/v1/users/"+string(unknown)+"/keys", ""),
		http.StatusNotFound, "not_found")

	// 用户存在但没有任何 Key：items 必须是空数组而不是 null。
	rec := f.call(http.MethodGet, "/admin/v1/users/"+string(f.userID)+"/keys", "")
	page := requirePageShape(t, rec)
	if len(page.Items) != 0 {
		t.Fatalf("未签发 Key 时 items 应为空，实际 %d 条", len(page.Items))
	}
	if page.Pagination.Total != 0 || page.Pagination.TotalPages != 0 {
		t.Fatalf("分页计数应全为 0，实际 total=%d total_pages=%d",
			page.Pagination.Total, page.Pagination.TotalPages)
	}

	// 非法 page 参数按分页契约回落为 1，而不是报错。
	rec = f.call(http.MethodGet, "/admin/v1/users/"+string(f.userID)+"/keys?page=abc", "")
	page = requirePageShape(t, rec)
	if page.Pagination.Page != 1 {
		t.Fatalf("page=abc 应回落为 1，实际 %d", page.Pagination.Page)
	}
}

func TestNamespaceDTOHidesEnvValues(t *testing.T) {
	f := newFixture(t)

	rec := f.call(http.MethodGet, "/admin/v1/namespaces?page_size=50", "")
	requireStatus(t, rec, http.StatusOK)

	raw := rec.Body.String()
	if strings.Contains(raw, "super-secret-value") {
		t.Fatal("命名空间 env 的值绝不能回显")
	}
	if !strings.Contains(raw, "UPSTREAM_TOKEN") {
		t.Fatal("env 的键名需要回显，运维才知道有没有配")
	}

	var page struct {
		Items []namespaceDTO `json:"items"`
	}
	page = decodeBody[struct {
		Items []namespaceDTO `json:"items"`
	}](t, rec)

	var blender *namespaceDTO
	for i := range page.Items {
		if page.Items[i].Slug == "blender" {
			blender = &page.Items[i]
		}
	}
	if blender == nil {
		t.Fatal("未返回 blender 命名空间")
	}
	if len(blender.EnvKeys) != 1 || blender.EnvKeys[0] != "UPSTREAM_TOKEN" {
		t.Fatalf("env_keys = %v", blender.EnvKeys)
	}
	if blender.DiscoveryService != "blender-mcp" || blender.LBStrategy != "round_robin" {
		t.Fatalf("发现配置丢失: %+v", blender)
	}
	if blender.Source != store.SourceRegistry {
		t.Fatalf("source = %q", blender.Source)
	}
}

func TestDisableEnableNamespace(t *testing.T) {
	f := newFixture(t)

	// 大写 + 空格都要被 NormalizeSlug 收敛到 blender。
	rec := f.call(http.MethodPost, "/admin/v1/namespaces/Blender/disable", "")
	requireStatus(t, rec, http.StatusOK)

	var disabled map[string]any
	disabled = decodeBody[map[string]any](t, rec)
	if disabled["slug"] != "blender" {
		t.Fatalf("slug = %v", disabled["slug"])
	}
	if disabled["enabled"] != false {
		t.Fatalf("enabled = %v", disabled["enabled"])
	}
	if disabled["reason"] != store.DisabledByAdmin {
		t.Fatalf("reason = %v，必须是 admin（registry_gone 归同步器专用）", disabled["reason"])
	}
	if ns := f.st.namespaceBySlug("blender"); ns.Enabled || ns.DisabledBy == nil {
		t.Fatal("存储侧未同步禁用状态")
	}

	requireErrorCode(t, f.call(http.MethodPost, "/admin/v1/namespaces/missing/disable", ""),
		http.StatusNotFound, "namespace_not_found")

	rec = f.call(http.MethodPost, "/admin/v1/namespaces/blender/enable", "")
	requireStatus(t, rec, http.StatusOK)
	if ns := f.st.namespaceBySlug("blender"); !ns.Enabled || ns.DisabledBy != nil {
		t.Fatal("启用后应清空 disabled_by")
	}
}

// -----------------------------------------------------------------------------
// 报表：用量 / 会话 / 消息 / 标注
// -----------------------------------------------------------------------------

func TestUserUsageDefaultWindow(t *testing.T) {
	f := newFixture(t)
	f.st.setUsage(
		store.UsageSummary{Metric: "tool_calls", Quantity: 12, CostMicros: 3400, EventCount: 12},
		store.UsageSummary{Metric: "input_tokens", Quantity: 900, CostMicros: 1600, EventCount: 12},
	)

	rec := f.call(http.MethodGet, "/admin/v1/users/"+string(f.userID)+"/usage", "")
	requireStatus(t, rec, http.StatusOK)

	resp := decodeBody[usageResponse](t, rec)
	wantFrom := f.now.Add(-defaultUsageWindow).UTC().Format(time.RFC3339)
	wantTo := f.now.UTC().Format(time.RFC3339)
	if resp.From != wantFrom {
		t.Fatalf("from = %q，期望 %q（默认最近 30 天）", resp.From, wantFrom)
	}
	if resp.To != wantTo {
		t.Fatalf("to = %q，期望 %q", resp.To, wantTo)
	}
	if resp.Items == nil {
		t.Fatal("items 不能为 null")
	}
	if len(resp.Items) != 2 {
		t.Fatalf("items 数量 = %d", len(resp.Items))
	}
	if resp.TotalCostMicros != 5000 {
		t.Fatalf("total_cost_micros = %d，期望 3400+1600", resp.TotalCostMicros)
	}
	if !f.st.lastFrom.Equal(f.now.Add(-defaultUsageWindow)) || !f.st.lastTo.Equal(f.now) {
		t.Fatalf("传给存储的时间窗不对: %v ~ %v", f.st.lastFrom, f.st.lastTo)
	}

	requireErrorCode(t,
		f.call(http.MethodGet, "/admin/v1/users/"+string(f.userID)+"/usage?from=2026-03-02T00:00:00Z&to=2026-03-01T00:00:00Z", ""),
		http.StatusBadRequest, "invalid_range")

	requireErrorCode(t,
		f.call(http.MethodGet, "/admin/v1/users/"+string(f.userID)+"/usage?from=昨天", ""),
		http.StatusBadRequest, "invalid_from")

	requireErrorCode(t,
		f.call(http.MethodGet, "/admin/v1/users/"+string(idgen.MustRandom(idgen.KindUser))+"/usage", ""),
		http.StatusNotFound, "not_found")
}

func TestListSessionsFilters(t *testing.T) {
	ensureIDGen()
	f := newFixture(t)
	sessID := idgen.MustSharded(idgen.KindSession, "01")
	f.st.addSession(store.Session{
		ID: sessID, UserID: f.userID, NamespaceSlug: "blender",
		StartedAt: f.now, MessageCount: 2, TrainingLabel: "good",
	})

	requireErrorCode(t, f.call(http.MethodGet, "/admin/v1/sessions?user_id=bad", ""),
		http.StatusBadRequest, "invalid_user_id")
	requireErrorCode(t,
		f.call(http.MethodGet, "/admin/v1/sessions?from=2026-03-02T00:00:00Z&to=2026-03-01T00:00:00Z", ""),
		http.StatusBadRequest, "invalid_range")

	rec := f.call(http.MethodGet, "/admin/v1/sessions?namespace=%20%20Blender%20%20&training_label=%20good%20", "")
	requireStatus(t, rec, http.StatusOK)

	if f.st.lastFilter.NamespaceSlug != "blender" {
		t.Fatalf("namespace 过滤值 = %q，期望已小写并 trim", f.st.lastFilter.NamespaceSlug)
	}
	if f.st.lastFilter.TrainingLabel != "good" {
		t.Fatalf("training_label = %q", f.st.lastFilter.TrainingLabel)
	}
	if !f.st.lastFilter.UserID.IsZero() {
		t.Fatal("未传 user_id 时不应构造过滤条件")
	}
}

func TestLabelSession(t *testing.T) {
	ensureIDGen()
	f := newFixture(t)
	sessID := idgen.MustSharded(idgen.KindSession, "01")
	f.st.addSession(store.Session{ID: sessID, UserID: f.userID, NamespaceSlug: "blender", StartedAt: f.now})
	path := "/admin/v1/sessions/" + string(sessID) + "/label"

	requireErrorCode(t, f.call(http.MethodPost, path, `{"label":"`+strings.Repeat("x", 65)+`"}`),
		http.StatusBadRequest, "invalid_label")
	requireErrorCode(t, f.call(http.MethodPost, path, `{"notes":"`+strings.Repeat("y", 2001)+`"}`),
		http.StatusBadRequest, "invalid_notes")
	requireErrorCode(t, f.call(http.MethodPost, "/admin/v1/sessions/nope/label", `{"label":"a"}`),
		http.StatusBadRequest, "invalid_session_id")

	rec := f.call(http.MethodPost, path, `{"label":"  high-quality  ","notes":"值得收录"}`)
	requireStatus(t, rec, http.StatusOK)

	var body map[string]any
	body = decodeBody[map[string]any](t, rec)
	if body["training_label"] != "high-quality" {
		t.Fatalf("training_label = %v，期望已 trim", body["training_label"])
	}
	if body["id"] != string(sessID) {
		t.Fatalf("id = %v", body["id"])
	}

	// 允许清空标签：标错了要能撤销。
	rec = f.call(http.MethodPost, path, `{"label":"","notes":""}`)
	requireStatus(t, rec, http.StatusOK)
}

func TestLabelSessionNotFound(t *testing.T) {
	ensureIDGen()
	f := newFixture(t)
	missing := string(idgen.MustSharded(idgen.KindSession, "02"))

	requireErrorCode(t, f.call(http.MethodPost, "/admin/v1/sessions/"+missing+"/label", `{"label":"a"}`),
		http.StatusNotFound, "not_found")
}

// -----------------------------------------------------------------------------
// 分页契约
// -----------------------------------------------------------------------------

func TestPaginationContractOnEveryListEndpoint(t *testing.T) {
	ensureIDGen()
	f := newFixture(t)

	sessID := idgen.MustSharded(idgen.KindSession, "01")
	f.st.addSession(store.Session{
		ID: sessID, UserID: f.userID, NamespaceSlug: "blender",
		StartedAt: f.now, MessageCount: 1,
	})
	f.st.addMessage(store.SessionMessage{
		ID:        idgen.MustSharded(idgen.KindSessionMessage, "01"),
		SessionID: sessID,
		Seq:       1,
		Direction: store.DirClientToGateway,
		Method:    "tools/call",
		Payload:   json.RawMessage(`{"jsonrpc":"2.0","id":1}`),
		CreatedAt: f.now,
	})
	if _, code := f.createKey(f.userID, ""); code != http.StatusCreated {
		t.Fatalf("建 Key 失败，状态码 = %d", code)
	}

	paths := []string{
		"/admin/v1/users",
		"/admin/v1/tenants",
		"/admin/v1/plans",
		"/admin/v1/namespaces",
		"/admin/v1/users/" + string(f.userID) + "/keys",
		"/admin/v1/sessions",
		"/admin/v1/sessions/" + string(sessID) + "/messages",
	}

	for _, p := range paths {
		t.Run(p, func(t *testing.T) {
			env := requirePageShape(t, f.call(http.MethodGet, p, ""))
			if env.Pagination.Total < 1 {
				t.Fatalf("total = %d，应至少 1 条", env.Pagination.Total)
			}
			if env.Pagination.Page != 1 || env.Pagination.PageSize != store.DefaultPageSize {
				t.Fatalf("默认分页应为 1/%d，实际 %d/%d",
					store.DefaultPageSize, env.Pagination.Page, env.Pagination.PageSize)
			}
			if env.Pagination.HasPrev {
				t.Fatal("第一页不应有上一页")
			}
		})
	}

	clamp := []struct {
		query    string
		page     int
		pageSize int
	}{
		{"?page=0&page_size=0", 1, store.DefaultPageSize},
		{"?page=2&page_size=999", 2, store.MaxPageSize},
		{"?page=abc&page_size=-1", 1, store.DefaultPageSize},
	}
	for _, c := range clamp {
		t.Run(c.query, func(t *testing.T) {
			env := requirePageShape(t, f.call(http.MethodGet, "/admin/v1/plans"+c.query, ""))
			if env.Pagination.Page != c.page || env.Pagination.PageSize != c.pageSize {
				t.Fatalf("pagination = %+v，期望 page=%d page_size=%d",
					env.Pagination, c.page, c.pageSize)
			}
		})
	}

	t.Run("空结果", func(t *testing.T) {
		env := requirePageShape(t, f.call(http.MethodGet, "/admin/v1/sessions?training_label=不存在", ""))
		if env.Pagination.Total != 0 || env.Pagination.TotalPages != 0 {
			t.Fatalf("pagination = %+v，total 为 0 时 total_pages 必须是 0", env.Pagination)
		}
		if env.Pagination.HasNext || env.Pagination.HasPrev {
			t.Fatal("空结果不应有 next / prev")
		}
		if len(env.Items) != 0 {
			t.Fatalf("items 长度 = %d", len(env.Items))
		}
	})
}

// -----------------------------------------------------------------------------
// 500 映射
// -----------------------------------------------------------------------------

func TestInternalErrorIsOpaque(t *testing.T) {
	f := newFixture(t)
	f.st.errListUsers = errors.New("dial tcp 10.0.0.1:5432: connect: connection refused")

	rec := f.call(http.MethodGet, "/admin/v1/users", "")
	env := requireErrorCode(t, rec, http.StatusInternalServerError, "internal_error")

	if !strings.Contains(env.Error.Message, "服务端处理失败") {
		t.Fatalf("message = %q", env.Error.Message)
	}
	if strings.Contains(rec.Body.String(), "5432") {
		t.Fatal("不能把底层错误回显给客户端")
	}
	if !strings.Contains(f.logs.joined(), "5432") {
		t.Fatalf("底层错误必须进日志，实际日志 = %q", f.logs.joined())
	}
}

func TestFailMapsSentinelErrors(t *testing.T) {
	f := newFixture(t)
	// 借 handleGetUser 走一遍 fail：GetUserByID 返回非哨兵错误 → 500。
	f.st.errGetUser = errors.New("boom")
	requireErrorCode(t, f.call(http.MethodGet, "/admin/v1/users/"+string(f.userID), ""),
		http.StatusInternalServerError, "internal_error")
}
