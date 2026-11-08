package auth

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// -----------------------------------------------------------------------------
// 测试替身
// -----------------------------------------------------------------------------

// fakeKeyStore 用内存表模拟 api_keys + users。
//
// 刻意做成可以「记账」的形式：findCalls 用于断言缓存是否真的生效，
// 而不用去数日志。缓存功能的唯一意义就是少查库，测不到这个就等于没测。
type fakeKeyStore struct {
	mu sync.Mutex

	keys  map[string]*store.APIKey // key: 哈希的十六进制串
	users map[idgen.ID]*store.User

	findErr  error
	userErr  error
	touchErr error

	findCalls  int32
	userCalls  int32
	touchCalls int32

	// findDelay 用于单飞测试：让查库慢到足以让并发请求全部撞上。
	findDelay time.Duration
}

func newFakeKeyStore() *fakeKeyStore {
	return &fakeKeyStore{
		keys:  make(map[string]*store.APIKey),
		users: make(map[idgen.ID]*store.User),
	}
}

func (f *fakeKeyStore) put(key *store.APIKey, u *store.User) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[hex.EncodeToString(key.KeyHash)] = key
	if u != nil {
		f.users[u.ID] = u
	}
}

func (f *fakeKeyStore) FindKeyByHash(_ context.Context, hash []byte) (*store.APIKey, error) {
	atomic.AddInt32(&f.findCalls, 1)
	if f.findDelay > 0 {
		time.Sleep(f.findDelay)
	}
	if f.findErr != nil {
		return nil, f.findErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[hex.EncodeToString(hash)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *k
	return &cp, nil
}

func (f *fakeKeyStore) GetUserByID(_ context.Context, id idgen.ID) (*store.User, error) {
	atomic.AddInt32(&f.userCalls, 1)
	if f.userErr != nil {
		return nil, f.userErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.users[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *u
	return &cp, nil
}

func (f *fakeKeyStore) TouchKey(_ context.Context, _ idgen.ID) error {
	atomic.AddInt32(&f.touchCalls, 1)
	return f.touchErr
}

func (f *fakeKeyStore) finds() int { return int(atomic.LoadInt32(&f.findCalls)) }

// fakePerms 用字符串键模拟 key_permissions。
type fakePerms struct {
	mu    sync.Mutex
	ns    map[string]bool
	tools map[string]bool
	list  []store.Permission
	calls int32
	err   error
}

func newFakePerms() *fakePerms {
	return &fakePerms{ns: map[string]bool{}, tools: map[string]bool{}}
}

func (f *fakePerms) grantNamespace(keyID, nsID idgen.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ns[nsCacheKey(keyID, nsID)] = true
}

func (f *fakePerms) grantTool(keyID, nsID idgen.ID, tool string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tools[toolCacheKey(keyID, nsID, tool)] = true
}

func (f *fakePerms) CanUseNamespace(_ context.Context, keyID, namespaceID idgen.ID) (bool, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.err != nil {
		return false, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ns[nsCacheKey(keyID, namespaceID)], nil
}

func (f *fakePerms) CanUseTool(_ context.Context, keyID, namespaceID idgen.ID, tool string) (bool, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.err != nil {
		return false, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tools[toolCacheKey(keyID, namespaceID, tool)], nil
}

func (f *fakePerms) ListPermissions(_ context.Context, _ idgen.ID) ([]store.Permission, error) {
	return f.list, nil
}

// fakeNamespaces 模拟 slug/id → 命名空间行的查询。
type fakeNamespaces struct {
	bySlug map[string]*store.Namespace
	byID   map[string]*store.Namespace
	err    error
}

func newFakeNamespaces() *fakeNamespaces {
	return &fakeNamespaces{
		bySlug: make(map[string]*store.Namespace),
		byID:   make(map[string]*store.Namespace),
	}
}

func (f *fakeNamespaces) add(ns *store.Namespace) {
	f.bySlug[ns.Slug] = ns
	f.byID[string(ns.ID)] = ns
}

func (f *fakeNamespaces) GetNamespaceBySlug(_ context.Context, slug string) (*store.Namespace, error) {
	if f.err != nil {
		return nil, f.err
	}
	ns, ok := f.bySlug[slug]
	if !ok {
		return nil, store.ErrNotFound
	}
	return ns, nil
}

func (f *fakeNamespaces) GetNamespaceByID(_ context.Context, id idgen.ID) (*store.Namespace, error) {
	if f.err != nil {
		return nil, f.err
	}
	ns, ok := f.byID[string(id)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return ns, nil
}

// 固定测试用 ID 与 token，避免每个用例都自己拼装。
func testIDs() (keyID, userID, tenantID, nsID idgen.ID) {
	return idgen.MustRandom(idgen.KindAPIKey),
		idgen.MustRandom(idgen.KindUser),
		idgen.MustRandom(idgen.KindTenant),
		idgen.MustRandom(idgen.KindNamespace)
}

func activeUser(id, tenantID idgen.ID) *store.User {
	return &store.User{
		ID:       id,
		Email:    "dev@example.com",
		TenantID: tenantID,
		Status:   store.UserActive,
	}
}

// newActiveFixture 造一个「全绿」的组合，供正向用例复用。
func newActiveFixture(t *testing.T) (*fakeKeyStore, *Authenticator, string, *Principal) {
	t.Helper()
	keyID, userID, tenantID, _ := testIDs()

	plain, prefix, hash, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	fs := newFakeKeyStore()
	fs.put(&store.APIKey{
		ID:        keyID,
		UserID:    userID,
		Name:      "test",
		KeyPrefix: prefix,
		KeyHash:   hash,
		Enabled:   true,
	}, activeUser(userID, tenantID))

	a := New(fs, Options{
		CacheTTL:        time.Minute,
		NegativeTTL:     time.Second,
		TouchInterval:   time.Minute,
		MaxCacheEntries: 64,
	})
	return fs, a, plain, &Principal{KeyID: keyID, UserID: userID, TenantID: tenantID}
}

// -----------------------------------------------------------------------------
// 鉴权主流程
// -----------------------------------------------------------------------------

func TestAuthenticateHappyPath(t *testing.T) {
	_, a, token, want := newActiveFixture(t)

	got, err := a.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("Authenticate 应成功，得到错误: %v", err)
	}
	if got.KeyID != want.KeyID || got.UserID != want.UserID || got.TenantID != want.TenantID {
		t.Fatalf("身份不匹配: got %+v want %+v", got, want)
	}
	if got.UserEmail != "dev@example.com" {
		t.Fatalf("邮箱应带出: %q", got.UserEmail)
	}
}

func TestAuthenticateEmptyToken(t *testing.T) {
	_, a, _, _ := newActiveFixture(t)

	for _, tok := range []string{"", "   ", "\t\n"} {
		if _, err := a.Authenticate(context.Background(), tok); !errors.Is(err, ErrMissingToken) {
			t.Fatalf("token=%q 应返回 ErrMissingToken，得到 %v", tok, err)
		}
	}
}

func TestAuthenticateUnknownToken(t *testing.T) {
	_, a, _, _ := newActiveFixture(t)

	_, err := a.Authenticate(context.Background(), "mcpgw_不存在的密钥")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("未知 token 应返回 ErrInvalidToken，得到 %v", err)
	}
}

func TestAuthenticateDisabledKey(t *testing.T) {
	keyID, userID, tenantID, _ := testIDs()
	plain, prefix, hash, _ := Generate()
	reason := store.DisabledByAdmin

	fs := newFakeKeyStore()
	fs.put(&store.APIKey{
		ID: keyID, UserID: userID, KeyPrefix: prefix, KeyHash: hash,
		Enabled: false, DisabledBy: &reason,
	}, activeUser(userID, tenantID))

	a := New(fs, Options{})
	if _, err := a.Authenticate(context.Background(), plain); !errors.Is(err, ErrKeyDisabled) {
		t.Fatalf("应返回 ErrKeyDisabled，得到 %v", err)
	}
}

func TestAuthenticateExpiredKey(t *testing.T) {
	keyID, userID, tenantID, _ := testIDs()
	plain, prefix, hash, _ := Generate()
	expired := time.Now().Add(-time.Second)

	fs := newFakeKeyStore()
	fs.put(&store.APIKey{
		ID: keyID, UserID: userID, KeyPrefix: prefix, KeyHash: hash,
		Enabled: true, ExpiresAt: &expired,
	}, activeUser(userID, tenantID))

	a := New(fs, Options{})
	if _, err := a.Authenticate(context.Background(), plain); !errors.Is(err, ErrKeyExpired) {
		t.Fatalf("应返回 ErrKeyExpired，得到 %v", err)
	}
}

func TestAuthenticateSuspendedUser(t *testing.T) {
	keyID, userID, tenantID, _ := testIDs()
	plain, prefix, hash, _ := Generate()

	fs := newFakeKeyStore()
	fs.put(&store.APIKey{
		ID: keyID, UserID: userID, KeyPrefix: prefix, KeyHash: hash, Enabled: true,
	}, &store.User{ID: userID, Email: "x@example.com", TenantID: tenantID, Status: store.UserSuspended})

	a := New(fs, Options{})
	if _, err := a.Authenticate(context.Background(), plain); !errors.Is(err, ErrUserNotActive) {
		t.Fatalf("应返回 ErrUserNotActive，得到 %v", err)
	}
}

func TestAuthenticateOrphanKeyIsInvalidNotLeaky(t *testing.T) {
	// Key 指向一个已被删除的用户：对外必须表现为「凭据无效」，
	// 而不是「用户不存在」—— 后者泄露了「这把 Key 曾经存在」。
	keyID, userID, _, _ := testIDs()
	plain, prefix, hash, _ := Generate()

	fs := newFakeKeyStore()
	fs.put(&store.APIKey{
		ID: keyID, UserID: userID, KeyPrefix: prefix, KeyHash: hash, Enabled: true,
	}, nil)

	a := New(fs, Options{})
	if _, err := a.Authenticate(context.Background(), plain); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("应返回 ErrInvalidToken，得到 %v", err)
	}
}

func TestAuthenticateDatabaseErrorIsNotCachedAsInvalid(t *testing.T) {
	fs, a, token, _ := newActiveFixture(t)

	// 先让库挂掉
	boom := errors.New("connection reset by peer")
	fs.mu.Lock()
	fs.findErr = boom
	fs.mu.Unlock()

	_, err := a.Authenticate(context.Background(), token)
	if err == nil {
		t.Fatal("库故障时应返回错误")
	}
	if errors.Is(err, ErrInvalidToken) {
		t.Fatal("库故障绝不能归类为凭据无效，否则会被负缓存放大成持续 401")
	}

	// 库恢复后立即可用 —— 证明上一步没有把失败结论写进负缓存
	fs.mu.Lock()
	fs.findErr = nil
	fs.mu.Unlock()

	if _, err := a.Authenticate(context.Background(), token); err != nil {
		t.Fatalf("库恢复后应能通过，得到 %v", err)
	}
}

// -----------------------------------------------------------------------------
// 缓存语义
// -----------------------------------------------------------------------------

func TestPositiveCacheAvoidsRepeatedQueries(t *testing.T) {
	fs, a, token, _ := newActiveFixture(t)

	for i := 0; i < 20; i++ {
		if _, err := a.Authenticate(context.Background(), token); err != nil {
			t.Fatalf("第 %d 次鉴权失败: %v", i, err)
		}
	}
	if got := fs.finds(); got != 1 {
		t.Fatalf("20 次鉴权应只查 1 次库，实际 %d 次", got)
	}
}

func TestNegativeCacheSuppressesBruteForce(t *testing.T) {
	fs, a, _, _ := newActiveFixture(t)

	for i := 0; i < 50; i++ {
		if _, err := a.Authenticate(context.Background(), "mcpgw_扫描用的废key"); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("应返回 ErrInvalidToken，得到 %v", err)
		}
	}
	if got := fs.finds(); got != 1 {
		t.Fatalf("重复的无效 Key 应只查 1 次库，实际 %d 次", got)
	}
}

func TestNegativeCacheHitRestoresError(t *testing.T) {
	// 这是极易写错的一条：负缓存命中时必须把存下来的错误返回，
	// 而不是返回 nil。返回 nil 等于「无效 Key 被放行」。
	_, a, _, _ := newActiveFixture(t)

	first, err1 := a.Authenticate(context.Background(), "mcpgw_废key")
	if first != nil || !errors.Is(err1, ErrInvalidToken) {
		t.Fatalf("首次应失败: got=%v err=%v", first, err1)
	}
	second, err2 := a.Authenticate(context.Background(), "mcpgw_废key")
	if second != nil {
		t.Fatal("负缓存命中时不得返回 Principal")
	}
	if !errors.Is(err2, ErrInvalidToken) {
		t.Fatalf("负缓存命中应还原 ErrInvalidToken，得到 %v", err2)
	}
}

func TestInvalidateDropsPositiveCache(t *testing.T) {
	fs, a, token, p := newActiveFixture(t)

	if _, err := a.Authenticate(context.Background(), token); err != nil {
		t.Fatalf("首次鉴权失败: %v", err)
	}
	a.Invalidate(p.KeyID)

	// 禁用后再鉴权：必须重新查库并拿到「已禁用」
	fs.mu.Lock()
	k := fs.keys[hex.EncodeToString(HashToken(token))]
	k.Enabled = false
	reason := store.DisabledByAdmin
	k.DisabledBy = &reason
	fs.mu.Unlock()

	if _, err := a.Authenticate(context.Background(), token); !errors.Is(err, ErrKeyDisabled) {
		t.Fatalf("失效后应重新查到禁用状态，得到 %v", err)
	}
}

func TestAuthenticateSingleflightCollapsesConcurrentColdMisses(t *testing.T) {
	fs, a, token, _ := newActiveFixture(t)
	fs.mu.Lock()
	fs.findDelay = 40 * time.Millisecond
	fs.mu.Unlock()

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = a.Authenticate(context.Background(), token)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("并发第 %d 个请求失败: %v", i, err)
		}
	}
	if got := fs.finds(); got != 1 {
		t.Fatalf("%d 个并发冷未命中应只查 1 次库，实际 %d 次", n, got)
	}
}

// -----------------------------------------------------------------------------
// Cache 本体
// -----------------------------------------------------------------------------

func TestCacheTTLExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	c := newCache(10*time.Second, 8, func() time.Time { return now })

	c.Set("k", "v")
	if v, err := c.Get("k"); err != nil || v != "v" {
		t.Fatalf("未过期应命中: v=%v err=%v", v, err)
	}

	now = now.Add(11 * time.Second)
	if _, err := c.Get("k"); !errors.Is(err, errCacheMiss) {
		t.Fatalf("过期后应未命中，得到 %v", err)
	}
	if c.Len() != 0 {
		t.Fatalf("过期项应被惰性清除，实际残留 %d 条", c.Len())
	}
}

func TestCacheEvictsWhenFull(t *testing.T) {
	c := newCache(time.Minute, 4, time.Now)
	for i := 0; i < 100; i++ {
		c.Set(string(rune('a'+i%26))+string(rune('0'+i/26)), i)
	}
	if c.Len() > 4 {
		t.Fatalf("容量上限 4，实际 %d", c.Len())
	}
}

func TestCacheDeleteByKeyPrefix(t *testing.T) {
	c := newCache(time.Minute, 64, time.Now)
	c.Set("key-a|nsp-1", 1)
	c.Set("key-a|nsp-1|render", 2)
	c.Set("key-ab|nsp-1", 3) // 前缀相似的邻居，绝不能被误删
	c.Set("key-b|nsp-1", 4)

	c.DeleteByKeyPrefix("key-a|")

	if _, err := c.Get("key-a|nsp-1"); !errors.Is(err, errCacheMiss) {
		t.Fatal("key-a 的命名空间缓存应被清掉")
	}
	if _, err := c.Get("key-a|nsp-1|render"); !errors.Is(err, errCacheMiss) {
		t.Fatal("key-a 的工具缓存应被清掉")
	}
	if _, err := c.Get("key-ab|nsp-1"); err != nil {
		t.Fatal("key-ab 是别人的缓存，不该被误删")
	}
	if _, err := c.Get("key-b|nsp-1"); err != nil {
		t.Fatal("key-b 不该被删")
	}
}

func TestCacheDeleteByKeyPrefixEmptyIsNoop(t *testing.T) {
	c := newCache(time.Minute, 8, time.Now)
	c.Set("a", 1)
	c.Set("b", 2)

	c.DeleteByKeyPrefix("")

	if c.Len() != 2 {
		t.Fatalf("空前缀不应清空整表，实际剩 %d 条", c.Len())
	}
}

func TestCacheDeleteByValue(t *testing.T) {
	c := newCache(time.Minute, 8, time.Now)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	c.DeleteByValue(func(v any) bool { return v.(int)%2 == 1 })

	if c.Len() != 1 {
		t.Fatalf("应只剩偶数项，实际 %d 条", c.Len())
	}
	if _, err := c.Get("b"); err != nil {
		t.Fatal("b 应保留")
	}
}

// -----------------------------------------------------------------------------
// token 生成与解析
// -----------------------------------------------------------------------------

func TestGenerateShape(t *testing.T) {
	plain, prefix, hash, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(plain) <= DisplayPrefixLen {
		t.Fatalf("密钥长度 %d 太短", len(plain))
	}
	if plain[:len(tokenPrefix)] != tokenPrefix {
		t.Fatalf("密钥应以 %q 开头，实际 %q", tokenPrefix, plain[:len(tokenPrefix)])
	}
	if len(prefix) != DisplayPrefixLen {
		t.Fatalf("展示前缀长度应为 %d，实际 %d", DisplayPrefixLen, len(prefix))
	}
	if want := HashToken(plain); hex.EncodeToString(want) != hex.EncodeToString(hash) {
		t.Fatal("返回的 hash 与 HashToken(plain) 不一致")
	}

	other, _, _, _ := Generate()
	if other == plain {
		t.Fatal("两次生成不应重复")
	}
}

func TestHashTokenIsStableAndNotReversible(t *testing.T) {
	a := HashToken("mcpgw_abc")
	b := HashToken("mcpgw_abc")
	if hex.EncodeToString(a) != hex.EncodeToString(b) {
		t.Fatal("同一输入应得到同一哈希")
	}
	if len(a) != 32 {
		t.Fatalf("sha256 应为 32 字节，实际 %d", len(a))
	}
	if hex.EncodeToString(a) == "mcpgw_abc" {
		t.Fatal("哈希不应等于原文")
	}
}

func TestExtractTokenBearer(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/sse/blender", nil)
	r.Header.Set("Authorization", "Bearer mcpgw_xxx")

	got, err := ExtractToken(r)
	if err != nil || got != "mcpgw_xxx" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestExtractTokenCaseInsensitiveScheme(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "bEaReR mcpgw_xxx")

	if got, err := ExtractToken(r); err != nil || got != "mcpgw_xxx" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestExtractTokenAPIKeyHeader(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-API-Key", "  mcpgw_xxx  ")

	if got, err := ExtractToken(r); err != nil || got != "mcpgw_xxx" {
		t.Fatalf("X-API-Key 应被 TrimSpace 后取出: %q %v", got, err)
	}
}

func TestExtractTokenRejectsQueryString(t *testing.T) {
	// 凭据放 query 会进 access log / 浏览器历史，必须一律不认。
	r := httptest.NewRequest(http.MethodGet, "/sse/blender?api_key=mcpgw_leak", nil)

	if _, err := ExtractToken(r); !errors.Is(err, ErrNoToken) {
		t.Fatalf("query 里的凭据不应被接受，得到 %v", err)
	}
}

func TestExtractTokenMissing(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, err := ExtractToken(r); !errors.Is(err, ErrNoToken) {
		t.Fatalf("应返回 ErrNoToken，得到 %v", err)
	}
	var nilReq *http.Request
	if _, err := ExtractToken(nilReq); !errors.Is(err, ErrNoToken) {
		t.Fatalf("nil 请求应返回 ErrNoToken，得到 %v", err)
	}
}

func TestSecureCompare(t *testing.T) {
	if !SecureCompare("admin-token", "admin-token") {
		t.Fatal("相同字符串应返回 true")
	}
	if SecureCompare("admin-token", "admin-tokeN") {
		t.Fatal("不同字符串应返回 false")
	}
	if SecureCompare("admin-token", "admin-token-longer") {
		t.Fatal("长度不同应返回 false")
	}
}

// -----------------------------------------------------------------------------
// HTTP 错误映射
// -----------------------------------------------------------------------------

func TestStatusOf(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{ErrMissingToken, http.StatusUnauthorized},
		{ErrInvalidToken, http.StatusUnauthorized},
		{ErrKeyDisabled, http.StatusForbidden},
		{ErrKeyExpired, http.StatusForbidden},
		{ErrUserNotActive, http.StatusForbidden},
		{ErrForbidden, http.StatusForbidden},
		{errors.New("db down"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		if got := statusOf(c.err); got != c.want {
			t.Fatalf("%v 应映射为 %d，实际 %d", c.err, c.want, got)
		}
	}
}

func TestWriteAuthErrorBody(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/messages/blender", nil)
	WriteAuthError(rec, r, ErrInvalidToken)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("状态码应为 401，实际 %d", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatal("401 应带 WWW-Authenticate，客户端据此决定认证方式")
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("鉴权失败响应不应被缓存，实际 Cache-Control=%q", got)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("响应体不应为空")
	}
}

func TestWriteAuthErrorHidesInternalDetail(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	WriteAuthError(rec, r, errors.New("pq: password authentication failed for user \"gw\""))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("库故障应为 500，实际 %d", rec.Code)
	}
	// 内部错误细节只进日志，不能进响应体，否则等于给攻击者做侦查
	if body := rec.Body.String(); stringContains(body, "password authentication") {
		t.Fatalf("响应体泄露了数据库细节: %s", body)
	}
}

func stringContains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// -----------------------------------------------------------------------------
// 中间件
// -----------------------------------------------------------------------------

func TestMiddlewareInjectsPrincipal(t *testing.T) {
	_, a, token, want := newActiveFixture(t)

	var seen *Principal
	h := Middleware(a)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = PrincipalFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/sse/blender", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("应放行，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if seen == nil || seen.KeyID != want.KeyID {
		t.Fatalf("context 里的 Principal 不正确: %+v", seen)
	}
}

func TestMiddlewareRejectsWithoutToken(t *testing.T) {
	_, a, _, _ := newActiveFixture(t)

	called := false
	h := Middleware(a)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sse/blender", nil))

	if called {
		t.Fatal("未携带凭据的请求不该进入业务 handler")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("应为 401，实际 %d", rec.Code)
	}
}

func TestMiddlewareMarksUsedOncePerInterval(t *testing.T) {
	fs, a, token, _ := newActiveFixture(t)

	h := Middleware(a)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/sse/blender", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	// 5 个请求落在同一个 TouchInterval 内，只该写一次库
	if got := atomic.LoadInt32(&fs.touchCalls); got != 1 {
		t.Fatalf("last_used_at 应节流为 1 次写入，实际 %d 次", got)
	}
}

func TestMiddlewareSurvivesTouchFailure(t *testing.T) {
	fs, a, token, _ := newActiveFixture(t)
	fs.mu.Lock()
	fs.touchErr = errors.New("write failed")
	fs.mu.Unlock()

	h := Middleware(a)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/sse/blender", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("last_used_at 写失败不该影响本次调用，实际 %d", rec.Code)
	}
}

// -----------------------------------------------------------------------------
// 限流
// -----------------------------------------------------------------------------

func TestRateLimiterFixedWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	l := NewRateLimiter(2, time.Minute)
	l.now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		if ok, _ := l.AllowN("k", 2); !ok {
			t.Fatalf("第 %d 次应放行", i+1)
		}
	}
	ok, retry := l.AllowN("k", 2)
	if ok {
		t.Fatal("超过上限应拒绝")
	}
	if retry <= 0 {
		t.Fatal("拒绝时应给出 Retry-After")
	}

	// 进入下一个窗口后恢复
	now = now.Add(61 * time.Second)
	if ok, _ := l.AllowN("k", 2); !ok {
		t.Fatal("新窗口应重新放行")
	}
}

func TestRateLimiterPerKeyIsolation(t *testing.T) {
	l := NewRateLimiter(1, time.Minute)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("a 首次应放行")
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Fatal("b 不应被 a 影响")
	}
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("a 第二次应被拒")
	}
}

func TestRateLimiterAllowNOverridesConfiguredLimit(t *testing.T) {
	// 这是「Key 级配额静默失效」那个 bug 的回归测试：
	// 构造时 limit=1000，但本次要求 1，那么第二次就必须被拒。
	l := NewRateLimiter(1000, time.Minute)

	if ok, _ := l.AllowN("k", 1); !ok {
		t.Fatal("第 1 次应放行")
	}
	if ok, _ := l.AllowN("k", 1); ok {
		t.Fatal("AllowN 必须使用传入的 limit，而不是构造时的默认值")
	}
}

func TestRateLimiterDisabledWhenLimitNonPositive(t *testing.T) {
	l := NewRateLimiter(0, time.Minute)
	for i := 0; i < 100; i++ {
		if ok, _ := l.Allow("k"); !ok {
			t.Fatal("limit<=0 表示不限流")
		}
	}
	var nilLimiter *RateLimiter
	if ok, _ := nilLimiter.AllowN("k", 1); !ok {
		t.Fatal("nil limiter 应放行而不是 panic")
	}
}

func TestLimitMiddlewareAppliesKeyOverride(t *testing.T) {
	l := NewRateLimiter(1000, time.Minute)

	p := &Principal{KeyID: idgen.MustRandom(idgen.KindAPIKey)}
	override := 1

	h := LimitMiddleware(l, 1000)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	call := func() int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(WithPrincipal(req.Context(), p))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	p.RateLimit = &override
	if got := call(); got != http.StatusOK {
		t.Fatalf("第 1 次应放行，实际 %d", got)
	}
	if got := call(); got != http.StatusTooManyRequests {
		t.Fatalf("第 2 次应被 Key 自己的配额（1）限流，实际 %d", got)
	}
}

func TestLimitMiddlewareWithoutPrincipalPassesThrough(t *testing.T) {
	l := NewRateLimiter(1, time.Minute)
	called := false
	h := LimitMiddleware(l, 1)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("没有 Principal 时应放行，鉴权缺失由上一层负责")
	}
}

func TestNormalizeSlug(t *testing.T) {
	cases := map[string]string{
		"  Blender ": "blender",
		"BLENDER":    "blender",
		"blender":    "blender",
		"":           "",
	}
	for in, want := range cases {
		if got := NormalizeSlug(in); got != want {
			t.Fatalf("NormalizeSlug(%q)=%q，期望 %q", in, got, want)
		}
	}
}

// -----------------------------------------------------------------------------
// RBAC
// -----------------------------------------------------------------------------

func newRBACFixture(t *testing.T) (*fakePerms, *fakeNamespaces, *Authorizer, *store.Namespace) {
	t.Helper()
	perms := newFakePerms()
	nss := newFakeNamespaces()
	ns := &store.Namespace{
		ID:        idgen.MustRandom(idgen.KindNamespace),
		Slug:      "blender",
		Transport: store.TransportHTTP,
		Enabled:   true,
	}
	nss.add(ns)

	az := NewAuthorizer(perms, nss, RBACOptions{CacheTTL: time.Minute, MaxCacheEntries: 64})
	return perms, nss, az, ns
}

func TestResolveNamespace(t *testing.T) {
	_, _, az, ns := newRBACFixture(t)

	got, err := az.ResolveNamespace(context.Background(), "  BLENDER ")
	if err != nil {
		t.Fatalf("应解析成功: %v", err)
	}
	if got.ID != ns.ID {
		t.Fatal("解析到了错误的命名空间")
	}

	if _, err := az.ResolveNamespace(context.Background(), "nope"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("不存在的命名空间应为 ErrForbidden（不泄露存在性），得到 %v", err)
	}
	if _, err := az.ResolveNamespace(context.Background(), ""); !errors.Is(err, ErrForbidden) {
		t.Fatalf("空 slug 应为 ErrForbidden，得到 %v", err)
	}
}

func TestResolveNamespaceRejectsDisabled(t *testing.T) {
	_, nss, az, ns := newRBACFixture(t)

	off := *ns
	off.Enabled = false
	nss.add(&off)

	if _, err := az.ResolveNamespace(context.Background(), "blender"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("已下线命名空间应为 ErrForbidden，得到 %v", err)
	}
}

func TestResolveNamespacePropagatesDatabaseError(t *testing.T) {
	_, nss, az, _ := newRBACFixture(t)
	nss.err = errors.New("connection refused")

	_, err := az.ResolveNamespace(context.Background(), "blender")
	if err == nil {
		t.Fatal("库故障应上抛")
	}
	if errors.Is(err, ErrForbidden) {
		t.Fatal("库故障不该被伪装成「无权访问」")
	}
}

func TestAllowsNamespaceCachesDecision(t *testing.T) {
	perms, _, az, ns := newRBACFixture(t)
	keyID := idgen.MustRandom(idgen.KindAPIKey)
	p := &Principal{KeyID: keyID}

	ok, err := az.AllowsNamespace(context.Background(), p, ns)
	if err != nil || ok {
		t.Fatalf("未授权时不应放行: ok=%v err=%v", ok, err)
	}

	perms.grantNamespace(keyID, ns.ID)
	// 缓存里已经是 false，必须等失效后才看得到新授权
	if ok, _ := az.AllowsNamespace(context.Background(), p, ns); ok {
		t.Fatal("TTL 内应沿用缓存的否定结论")
	}

	az.InvalidateKey(keyID)
	if ok, err := az.AllowsNamespace(context.Background(), p, ns); err != nil || !ok {
		t.Fatalf("失效后应读到新授权: ok=%v err=%v", ok, err)
	}
}

func TestAllowsToolRequiresNamespaceFirst(t *testing.T) {
	perms, _, az, ns := newRBACFixture(t)
	keyID := idgen.MustRandom(idgen.KindAPIKey)
	p := &Principal{KeyID: keyID}

	// 只授了工具、没授命名空间：必须拒绝
	perms.grantTool(keyID, ns.ID, "render")
	ok, err := az.AllowsTool(context.Background(), p, ns, "render")
	if err != nil {
		t.Fatalf("不应报错: %v", err)
	}
	if ok {
		t.Fatal("只有工具授权、没有命名空间授权时应拒绝")
	}

	// 补上命名空间授权后放行
	perms.grantNamespace(keyID, ns.ID)
	az.InvalidateKey(keyID)
	if ok, err := az.AllowsTool(context.Background(), p, ns, "render"); err != nil || !ok {
		t.Fatalf("两级都授权后应放行: ok=%v err=%v", ok, err)
	}

	// 同命名空间下未单独授予的工具应拒绝
	if ok, err := az.AllowsTool(context.Background(), p, ns, "sculpt"); err != nil || ok {
		t.Fatalf("未授权工具应拒绝: ok=%v err=%v", ok, err)
	}
}

func TestAllowsToolEmptyToolIsNamespaceLevel(t *testing.T) {
	perms, _, az, ns := newRBACFixture(t)
	keyID := idgen.MustRandom(idgen.KindAPIKey)
	p := &Principal{KeyID: keyID}
	perms.grantNamespace(keyID, ns.ID)

	ok, err := az.AllowsTool(context.Background(), p, ns, "")
	if err != nil || !ok {
		t.Fatalf("initialize/tools-list 这类命名空间级操作应放行: ok=%v err=%v", ok, err)
	}
}

func TestVisibleTools(t *testing.T) {
	perms, _, az, ns := newRBACFixture(t)
	keyID := idgen.MustRandom(idgen.KindAPIKey)
	p := &Principal{KeyID: keyID}

	perms.list = []store.Permission{
		{NamespaceID: ns.ID, ToolName: "render"},
		{NamespaceID: ns.ID, ToolName: "sculpt"},
		{NamespaceID: idgen.MustRandom(idgen.KindNamespace), ToolName: "other"},
	}

	got, err := az.VisibleTools(context.Background(), p, ns)
	if err != nil {
		t.Fatalf("VisibleTools: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("应只返回本命名空间的工具，实际 %v", got)
	}

	// '*' 表示不限制，返回 nil 以区别于「一个都没有」
	perms.list = []store.Permission{{NamespaceID: ns.ID, ToolName: "*"}}
	got, err = az.VisibleTools(context.Background(), p, ns)
	if err != nil || got != nil {
		t.Fatalf("通配应返回 nil（不限制）: got=%v err=%v", got, err)
	}

	// 完全没有授权：返回空切片（非 nil），让上层能把 tools/list 过滤成空
	perms.list = nil
	got, err = az.VisibleTools(context.Background(), p, ns)
	if err != nil {
		t.Fatalf("VisibleTools: %v", err)
	}
	if got == nil {
		t.Fatal("无授权应返回空切片而不是 nil，否则会被误判成全放行")
	}
	if len(got) != 0 {
		t.Fatalf("无授权不应有可见工具，实际 %v", got)
	}
}

func TestToolsCallMethod(t *testing.T) {
	for _, m := range []string{"tools/call", "mcp/tools/call"} {
		if !ToolsCallMethod(m) {
			t.Fatalf("%q 应被识别为工具调用", m)
		}
	}
	for _, m := range []string{"tools/list", "initialize", "", "Tools/Call"} {
		if ToolsCallMethod(m) {
			t.Fatalf("%q 不该被识别为工具调用", m)
		}
	}
}

func TestAuthorizerPanicsOnNilDependency(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("传入 nil 依赖应立即 panic，而不是等到线上才发现")
		}
	}()
	NewAuthorizer(nil, newFakeNamespaces(), RBACOptions{})
}
