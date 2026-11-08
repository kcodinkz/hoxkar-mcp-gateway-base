package auth

import (
	"errors"
	"strings"
	"sync"
	"time"
)

// errCacheMiss 是缓存内部使用的哨兵值，不会逃出本包。
var errCacheMiss = errors.New("缓存未命中")

// Cache 是一个带 TTL 与容量上限的并发安全内存缓存。
//
// 刻意不引第三方缓存库：需要的语义只有「按 TTL 过期 + 有界」，两三百行
// 就能写清楚，而引入一个依赖意味着凭空多出一套版本兼容与配置面。
//
// 淘汰策略是「先清过期，仍然超限就整体清空」而不是 LRU：鉴权缓存里
// 每一条的代价完全相同（都是一次主键查询），没有「更值得留下」的条目，
// 精确 LRU 带来的只是额外的链表维护与锁竞争。整体清空的后果仅仅是
// 下一批请求各多查一次库，对正确性零影响。
type Cache struct {
	mu         sync.RWMutex
	entries    map[string]cacheEntry
	ttl        time.Duration
	maxEntries int
	now        func() time.Time
}

type cacheEntry struct {
	value   any
	expires time.Time
}

func newCache(ttl time.Duration, maxEntries int, now func() time.Time) *Cache {
	if now == nil {
		now = time.Now
	}
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	return &Cache{
		entries:    make(map[string]cacheEntry, 64),
		ttl:        ttl,
		maxEntries: maxEntries,
		now:        now,
	}
}

// Get 取出未过期的值；不存在或已过期返回 errCacheMiss。
func (c *Cache) Get(key string) (any, error) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, errCacheMiss
	}
	if !c.now().Before(e.expires) {
		// 惰性删除：过期项在下次访问时才清掉，省掉一个后台 ticker goroutine
		c.mu.Lock()
		// 二次确认，避免并发下把刚被人刷新过的条目误删
		if cur, still := c.entries[key]; still && !c.now().Before(cur.expires) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return nil, errCacheMiss
	}
	return e.value, nil
}

// Set 写入一条记录，TTL 由创建时的参数决定。
func (c *Cache) Set(key string, value any) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.maxEntries {
		if _, exists := c.entries[key]; !exists {
			c.evictLocked(now)
		}
	}
	c.entries[key] = cacheEntry{value: value, expires: now.Add(c.ttl)}
}

// evictLocked 先清过期项；若一条都没过期，则整体清空。
// 调用方必须已持有写锁。
func (c *Cache) evictLocked(now time.Time) {
	removed := 0
	for k, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, k)
			removed++
		}
	}
	if removed > 0 {
		return
	}
	// 全部都在有效期内：说明键的数量真的超过了容量上限。
	// 整体清空比逐个随机淘汰更简单，且保证不会退化成「永远清不干净」。
	clear(c.entries)
}

// Delete 删除指定键。
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// DeleteByValue 删除所有满足谓词的条目。
//
// 之所以需要「按值删」：缓存键是 token 的 sha256，管理端只知道 KeyID，
// 无法从 KeyID 反推哈希键。扫描一遍是唯一办法，而缓存规模本就是万级以内。
func (c *Cache) DeleteByValue(pred func(any) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if pred(e.value) {
			delete(c.entries, k)
		}
	}
}

// DeleteByKeyPrefix 删除所有以 prefix 开头的键。
//
// 用于 RBAC 缓存的「按 Key 失效」：RBAC 的缓存键形如 "keyID|nsID" 与
// "keyID|nsID|tool"，KeyID 正好是前缀，因此前缀匹配比按值扫描更准也更省。
// 缓存键里 KeyID 后面紧跟的永远是 '|' 这个分隔符，不会出现
// "key-1" 误伤 "key-12" 的情况。
func (c *Cache) DeleteByKeyPrefix(prefix string) {
	if prefix == "" {
		// 空前缀会清空整张表，调用方传空多半是 bug，这里选择不做任何事
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			delete(c.entries, k)
		}
	}
}

// Reset 清空全部条目。
func (c *Cache) Reset() {
	c.mu.Lock()
	clear(c.entries)
	c.mu.Unlock()
}

// Len 返回当前条目数（含尚未惰性清除的过期项），用于观测与测试。
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// -----------------------------------------------------------------------------
// 单飞（single-flight）
// -----------------------------------------------------------------------------

// flightGroup 保证同一个 key 在任意时刻只有一个真正的「查库」在执行。
//
// 为什么必须做：MCP 客户端在初始化阶段会同时发出多个请求，缓存冷启动时
// 它们会同时未命中，于是一把 Key 触发了 N 次完全相同的查询。更糟的是
// 攻击者可以用同一把废 Key 发起大量并发，把「查不到」这个结论反复打到库里。
//
// 实现上只共享「结果」不共享「错误类型」：等待者拿到与发起者完全一致的
// 返回值，因此错误分类（401/403/5xx）在并发下依然一致。
type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	wg    sync.WaitGroup
	value any
	err   error
}

func newFlightGroup() *flightGroup {
	return &flightGroup{calls: make(map[string]*flightCall)}
}

// Do 执行 fn，相同 key 的并发调用只会真正执行一次。
func (g *flightGroup) Do(key string, fn func() (any, error)) (any, error, bool) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.value, c.err, true
	}
	c := &flightCall{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.value, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return c.value, c.err, false
}
