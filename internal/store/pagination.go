package store

// 分页契约（分页契约）。
//
// 所有列表接口一律返回同一形状，前端只需一套 DataTable 组件：
//
//	{
//	  "items": [...],
//	  "pagination": {
//	    "page": 1, "page_size": 20, "total": 137,
//	    "total_pages": 7, "has_next": true, "has_prev": false
//	  }
//	}
//
// 页号从 1 开始，page_size 上限 200。
// 这里只提供归一化与结构体，HTTP 序列化在 handler 层。
//
// # 排序必须带唯一兜底键
//
// OFFSET/LIMIT 分页要求「排序键组合唯一」，否则同一毫秒内的多行没有确定
// 相对顺序，PostgreSQL 两次查询可以返回不同排列，翻页就会重复或漏行。
// 因此所有分页查询一律写成 `ORDER BY <时间列> DESC, id DESC`（或 ASC 同理），
// 把主键当最后一个 tiebreaker。主键唯一，组合排序键就一定唯一。
//
// 反例：`ORDER BY started_at DESC` 单独用——批量写入的会话 started_at
// 常常完全相同，翻到第 2 页会看到第 1 页出现过的那几条。

const (
	// DefaultPageSize 默认每页条数
	DefaultPageSize = 20
	// MaxPageSize 单页上限，防止一次拉全表
	MaxPageSize = 200
)

// normalizePage 把任意输入收敛到合法范围，并算出 SQL OFFSET。
func normalizePage(page, pageSize int) (int, int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	if pageSize > MaxPageSize {
		pageSize = MaxPageSize
	}
	return page, pageSize, (page - 1) * pageSize
}

// Pagination 是响应里的分页元信息。
type Pagination struct {
	Page       int   `json:"page"`
	PageSize   int   `json:"page_size"`
	Total      int64 `json:"total"`
	TotalPages int   `json:"total_pages"`
	HasNext    bool  `json:"has_next"`
	HasPrev    bool  `json:"has_prev"`
}

// NewPagination 由 total 与请求参数算出完整元信息。
//
// total_pages 用向上取整；total=0 时 total_pages=0，has_next/has_prev 均为 false，
// 前端据此显示「暂无数据」而不是「第 1/0 页」。
func NewPagination(page, pageSize int, total int64) Pagination {
	page, pageSize, _ = normalizePage(page, pageSize)

	var totalPages int
	if total > 0 {
		totalPages = int((total + int64(pageSize) - 1) / int64(pageSize))
	}

	return Pagination{
		Page:       page,
		PageSize:   pageSize,
		Total:      total,
		TotalPages: totalPages,
		HasNext:    page < totalPages,
		HasPrev:    page > 1 && totalPages > 0,
	}
}

// PageResult 是列表接口的统一返回体，泛型在 handler 层展开。
// 这里定义是为了让文档与实现引用同一份定义。
type PageResult[T any] struct {
	Items      []T        `json:"items"`
	Pagination Pagination `json:"pagination"`
}

// NewPageResult 组装返回体，保证 items 永不为 null（前端可比面 null 判断）。
func NewPageResult[T any](items []T, page, pageSize int, total int64) PageResult[T] {
	if items == nil {
		items = []T{}
	}
	return PageResult[T]{
		Items:      items,
		Pagination: NewPagination(page, pageSize, total),
	}
}
