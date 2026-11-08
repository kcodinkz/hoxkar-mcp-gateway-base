// Package auth 是多租户模式下的统一鉴权层。
//
// 定位：网关是「服务端集群的前沿」。客户端拿着同一把 API Key 连到网关，
// 靠 URL 里的命名空间决定要调哪一类 MCP 能力。因此鉴权与路由是两个正交维度：
//
//	鉴权（本包）：这把 Key 是谁的、还能不能用、能碰哪些命名空间
//	路由（internal/routing）：这次请求要转给哪个命名空间的后端
//
// 本包只回答第一个问题，绝不关心后端在哪、是 stdio 还是 http。
//
// # 为什么必须缓存
//
// 每一次 SSE 建连、每一次 POST /messages 都要过鉴权。MCP 客户端在初始化阶段
// 会连续发 initialize / notifications/initialized / tools/list 多个请求，
// 如果每个请求都查一次 api_keys + users，数据库会被无意义的重复查询打满，
// 而 Key 的校验结果在几十秒内几乎没有变化的可能。
//
// 缓存的键一律是「token 的 sha256 十六进制串」，不是 token 本身：
// 内存里任何时刻都不该出现明文凭据，否则一次 core dump 就等于泄露全部 Key。
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/idgen"
	"github.com/kcodinkz/hoxkar-mcp-gateway-base/internal/store"
)

// 鉴权失败的分类。
//
// 分成这么多而不是一个笼统的 "unauthorized"，是因为它们对应的 HTTP 语义、
// 客户端该做什么、以及「能不能缓存」三件事都不一样：
//
//	401：凭据缺失或根本不存在 —— 客户端应该换一把 Key
//	403：凭据有效但当前状态不允许 —— 客户端换 Key 也没用，要联系管理员
//
// 另外只有「结论确定」的错误才允许进缓存。数据库连接超时这类瞬时故障
// 若被缓存，就会把一次网络抖动放大成几十秒的全员不可用。
var (
	// ErrMissingToken 请求里根本没有携带任何凭据。
	ErrMissingToken = errors.New("缺少 API Key")
	// ErrInvalidToken 凭据格式正确但库里查不到对应记录。
	ErrInvalidToken = errors.New("API Key 无效")
	// ErrKeyDisabled Key 被管理员、套餐到期或租户注销而停用。
	ErrKeyDisabled = errors.New("API Key 已被禁用")
	// ErrKeyExpired Key 自身超过 expires_at。
	ErrKeyExpired = errors.New("API Key 已过期")
	// ErrUserNotActive 账号被挂起或注销，其名下所有 Key 一并失效。
	ErrUserNotActive = errors.New("账号状态不可用")
	// ErrForbidden 鉴权通过但无权访问目标命名空间/工具。
	ErrForbidden = errors.New("无权访问该资源")
)

// cacheableError 说明这个错误是「确定的结论」，可以进缓存。
func cacheableError(err error) bool {
	return errors.Is(err, ErrInvalidToken) ||
		errors.Is(err, ErrKeyDisabled) ||
		errors.Is(err, ErrKeyExpired) ||
		errors.Is(err, ErrUserNotActive)
}

// Principal 是一次成功鉴权得到的调用者身份。
//
// 它会被塞进请求的 context，供路由层与计费层读取。刻意不复用 store.APIKey：
// 那个结构带着 KeyHash 字段，一旦被随手打印或序列化就是凭据泄露；
// 这里的每个字段都可以安全地写进日志。
type Principal struct {
	KeyID     idgen.ID
	KeyPrefix string
	KeyName   string

	UserID    idgen.ID
	UserEmail string
	TenantID  idgen.ID

	// RateLimit 是 Key 级别的每分钟请求上限，nil 表示走全局默认值。
	RateLimit *int
	// ExpiresAt 是 Key 的到期时间，nil 表示永不过期。
	ExpiresAt *time.Time
}

// String 让日志输出保持可读，且保证不含任何密钥材料。
func (p *Principal) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("key=%s(prefix=%s) user=%s email=%s tenant=%s",
		p.KeyID, p.KeyPrefix, p.UserID, p.UserEmail, p.TenantID)
}

// KeyStore 是 Authenticator 需要的存储能力。
//
// 声明成接口而不是直接吃 *store.Store，是为了让单元测试可以不连数据库：
// 用一张内存表就能覆盖「禁用/过期/账号挂起/不存在」全部分支，
// 而这些分支恰恰是最容易写错、又最难在真库里构造的。
type KeyStore interface {
	FindKeyByHash(ctx context.Context, hash []byte) (*store.APIKey, error)
	GetUserByID(ctx context.Context, id idgen.ID) (*store.User, error)
	TouchKey(ctx context.Context, id idgen.ID) error
}

// Options 是 Authenticator 的可调参数，零值即合理默认。
type Options struct {
	// CacheTTL 是「鉴权通过」结果的缓存时长。默认 30 秒。
	CacheTTL time.Duration
	// NegativeTTL 是「鉴权失败」结果的缓存时长。默认 5 秒。
	//
	// 比正缓存短得多：管理员在后台点「启用」之后，用户不该等半分钟才生效。
	// 但也不能为 0 —— 一个拿着废 Key 疯狂重试的客户端会把库打穿。
	NegativeTTL time.Duration
	// MaxCacheEntries 缓存条目上限，默认 8192。超过后先清过期项，仍然超就整体清空。
	MaxCacheEntries int
	// TouchInterval 是 last_used_at 的写库节流间隔，默认 60 秒。
	//
	// 不节流的话每个请求都要一次 UPDATE，等于把只读鉴权变成写操作。
	TouchInterval time.Duration
	// Now 便于测试注入时钟，默认 time.Now。
	Now func() time.Time
}

func (o *Options) applyDefaults() {
	if o.CacheTTL <= 0 {
		o.CacheTTL = 30 * time.Second
	}
	if o.NegativeTTL <= 0 {
		o.NegativeTTL = 5 * time.Second
	}
	if o.MaxCacheEntries <= 0 {
		o.MaxCacheEntries = 8192
	}
	if o.TouchInterval <= 0 {
		o.TouchInterval = 60 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Authenticator 把「一段 token 文本」解析成 Principal。
type Authenticator struct {
	store KeyStore
	opts  Options

	positive *Cache
	negative *Cache

	// flights 保证同一把 Key 在缓存冷启动时的并发请求只查一次库。
	flights *flightGroup

	// touchMu/touchedAt 实现 last_used_at 的节流，避免每个请求一次写库。
	touchMu   sync.Mutex
	touchedAt map[idgen.ID]time.Time
}

// New 构造一个 Authenticator。
func New(s KeyStore, opts Options) *Authenticator {
	if s == nil {
		panic("auth.New: store 不能为 nil")
	}
	opts.applyDefaults()

	return &Authenticator{
		store:     s,
		opts:      opts,
		positive:  newCache(opts.CacheTTL, opts.MaxCacheEntries, opts.Now),
		negative:  newCache(opts.NegativeTTL, opts.MaxCacheEntries, opts.Now),
		flights:   newFlightGroup(),
		touchedAt: make(map[idgen.ID]time.Time),
	}
}

// Authenticate 校验 token 并返回调用者身份。
//
// 返回的错误可以用 errors.Is 与 ErrInvalidToken / ErrKeyDisabled /
// ErrKeyExpired / ErrUserNotActive 比较；非上述错误说明是数据库侧故障，
// 调用方应当按 5xx 处理，而不是当成「这个人没权限」。
func (a *Authenticator) Authenticate(ctx context.Context, token string) (*Principal, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, ErrMissingToken
	}

	hash := HashToken(token)
	cacheKey := hex.EncodeToString(hash)

	// 1. 负缓存优先：命中说明这把 Key 刚被判定为无效，直接拒绝。
	//    这是抵挡暴力扫描的第一道闸，不给数据库任何压力。
	if v, err := a.negative.Get(cacheKey); err == nil {
		return nil, v.(error)
	} else if !errors.Is(err, errCacheMiss) {
		return nil, err
	}

	// 2. 正缓存：命中直接放行，这是绝大多数请求的实际路径。
	if v, err := a.positive.Get(cacheKey); err == nil {
		return v.(*Principal), nil
	} else if !errors.Is(err, errCacheMiss) {
		return nil, err
	}

	// 3. 单飞查库：缓存冷启动时同一把 Key 的并发请求只真正查一次，
	//    等待者复用发起者的结果（含错误），错误分类在并发下因此保持一致。
	v, err, _ := a.flights.Do(cacheKey, func() (any, error) {
		return a.resolve(ctx, hash)
	})

	// 4. 回填缓存。只有确定性结论才写负缓存，瞬时故障不写。
	switch {
	case err == nil:
		p := v.(*Principal)
		a.positive.Set(cacheKey, p)
		return p, nil
	case cacheableError(err):
		a.negative.Set(cacheKey, err)
	}
	return nil, err
}

// resolve 是一次真正的数据库查证，只在缓存未命中时执行。
func (a *Authenticator) resolve(ctx context.Context, hash []byte) (*Principal, error) {
	key, err := a.store.FindKeyByHash(ctx, hash)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrInvalidToken
	}
	if err != nil {
		return nil, fmt.Errorf("查询 API Key 失败: %w", err)
	}

	// enabled 与 disabled_by 在库里有 CHECK 约束成对维护，这里只需看 enabled。
	if !key.Enabled {
		return nil, ErrKeyDisabled
	}
	// 到期不翻转 enabled（见 store.APIKey.IsExpired 的说明），必须在这里判定。
	if key.IsExpired(a.opts.Now()) {
		return nil, ErrKeyExpired
	}

	user, err := a.store.GetUserByID(ctx, key.UserID)
	if errors.Is(err, store.ErrNotFound) {
		// Key 指向一个已不存在的用户，说明数据被手工删过。按无效凭据处理，
		// 不泄露「这把 Key 曾经存在」这一事实。
		return nil, ErrInvalidToken
	}
	if err != nil {
		return nil, fmt.Errorf("查询 API Key 归属用户失败: %w", err)
	}
	if user.Status != store.UserActive {
		return nil, ErrUserNotActive
	}

	return &Principal{
		KeyID:     key.ID,
		KeyPrefix: key.KeyPrefix,
		KeyName:   key.Name,
		UserID:    user.ID,
		UserEmail: user.Email,
		TenantID:  user.TenantID,
		RateLimit: key.RateLimit,
		ExpiresAt: key.ExpiresAt,
	}, nil
}

// Invalidate 让某把 Key 的缓存结论立即失效。
//
// 管理端在禁用/启用/改权限之后必须调用它，否则改动最多要等一个 TTL 才生效，
// 而「我已经禁用了它却还能用」在安全上是不可接受的。
//
// 按下标遍历是刻意的：缓存键是 token 的哈希，无法由 KeyID 反推，
// 所以只能扫一遍。缓存规模在万级以内，这个代价远小于一次误放行。
func (a *Authenticator) Invalidate(keyID idgen.ID) {
	a.positive.DeleteByValue(func(v any) bool {
		p, ok := v.(*Principal)
		return ok && p.KeyID == keyID
	})
}

// Reset 清空全部缓存，供管理端做批量变更（例如整租户停用）后调用。
func (a *Authenticator) Reset() {
	a.positive.Reset()
	a.negative.Reset()
}

// MarkUsed 记录 Key 的最近使用时间，带节流。
//
// 失败一律忽略：last_used_at 是运营观测字段，绝不能因为它写不进去
// 就把一次合法的 MCP 调用判成失败。
func (a *Authenticator) MarkUsed(ctx context.Context, keyID idgen.ID) {
	if keyID.IsZero() {
		return
	}
	now := a.opts.Now()

	a.touchMu.Lock()
	last, ok := a.touchedAt[keyID]
	if ok && now.Sub(last) < a.opts.TouchInterval {
		a.touchMu.Unlock()
		return
	}
	a.touchedAt[keyID] = now
	// 顺手清理长时间没动的条目，防止 map 随 Key 数量无限增长
	if len(a.touchedAt) > 4096 {
		for id, ts := range a.touchedAt {
			if now.Sub(ts) > 10*a.opts.TouchInterval {
				delete(a.touchedAt, id)
			}
		}
	}
	a.touchMu.Unlock()

	// 脱离请求上下文：请求可能已经返回，但这次写库仍应完成。
	touchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()

	_ = a.store.TouchKey(touchCtx, keyID)
}

// -----------------------------------------------------------------------------
// token 生成与解析
// -----------------------------------------------------------------------------

const (
	// tokenPrefix 让密钥在日志/截图中可被肉眼识别，也方便密钥扫描工具定位。
	tokenPrefix = "mcpgw_"
	// tokenBodyBytes 是随机部分的字节数。32 字节 = 256 位，远超暴力破解可行范围。
	tokenBodyBytes = 32
	// DisplayPrefixLen 是落库到 key_prefix 的前缀长度，用于后台列表展示。
	// 必须 >= len(tokenPrefix)，否则列表中所有 Key 看起来一模一样。
	DisplayPrefixLen = 12
)

// HashToken 计算 token 的 sha256。
//
// 用 sha256 而不是 bcrypt/argon2 是有意的：API Key 是 256 位高熵随机串，
// 不存在被字典攻击的现实路径，不需要慢哈希；而鉴权在每次请求上都要走一遍，
// 慢哈希会把网关吞吐直接砍掉一个数量级。
func HashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// Generate 生成一把新的 API Key。
//
// 返回的 plaintext 只在此刻存在，之后库里只剩 hash 与前缀 —— 这与业界惯例一致：
// 密钥遗失只能重新签发，不能找回。调用方必须立即把它交给用户，且只返回一次。
func Generate() (plaintext, displayPrefix string, hash []byte, err error) {
	buf := make([]byte, tokenBodyBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", "", nil, fmt.Errorf("生成随机密钥失败: %w", err)
	}
	// base64url 无填充：48 个字符，放进 URL 与 shell 都不需要转义
	plaintext = tokenPrefix + base64.RawURLEncoding.EncodeToString(buf)

	displayPrefix = plaintext
	if len(displayPrefix) > DisplayPrefixLen {
		displayPrefix = displayPrefix[:DisplayPrefixLen]
	}
	return plaintext, displayPrefix, HashToken(plaintext), nil
}

// ErrNoToken 表示请求里既没有 Authorization 也没有 X-API-Key。
var ErrNoToken = errors.New("请求未携带 API Key")

// ExtractToken 从请求中取出 token。
//
// 支持两种携带方式：
//
//	Authorization: Bearer <key>   —— MCP 客户端与 OpenAI 风格 SDK 的默认行为
//	X-API-Key: <key>              —— 便于 curl / 浏览器插件等无法自定义
//	                                 Authorization 的客户端
//
// 不接受 query string：URL 会进 access log、Referer、浏览器历史，
// 把凭据放进去等于主动泄露。
func ExtractToken(r *http.Request) (string, error) {
	if r == nil {
		return "", ErrNoToken
	}

	if raw := r.Header.Get("Authorization"); raw != "" {
		parts := strings.Fields(raw)
		switch {
		case len(parts) == 2 && strings.EqualFold(parts[0], "bearer"):
			if parts[1] != "" {
				return parts[1], nil
			}
		case len(parts) == 1 && parts[0] != "" && !strings.EqualFold(parts[0], "bearer"):
			// 容忍部分自研客户端直接把 key 放在头里
			return parts[0], nil
		}
	}

	if v := strings.TrimSpace(r.Header.Get("X-API-Key")); v != "" {
		return v, nil
	}

	return "", ErrNoToken
}

// SecureCompare 做常量时间的字符串比较，用于管理端 token 校验。
//
// 普通 == 会在第一个不同字节处返回，攻击者可以通过响应耗时逐字节猜出
// 管理 token。管理端能改全库数据，不值得为省几行代码留这个口子。
func SecureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// ExtractBearer 从 Authorization 头中取出 Bearer 值，用于管理端。
func ExtractBearer(r *http.Request) string {
	raw := r.Header.Get("Authorization")
	parts := strings.Fields(raw)
	if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
		return parts[1]
	}
	if len(parts) == 1 && raw != "" {
		return parts[0]
	}
	return ""
}
