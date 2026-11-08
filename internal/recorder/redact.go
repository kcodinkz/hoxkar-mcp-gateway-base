package recorder

import (
	"encoding/json"
	"regexp"
	"strings"
)

// 脱敏是录制链路的强制项（会话录制规则）：录下来的副本用于训练与审计，
// 但绝不能把用户的凭据一并存进库里，否则等于给管理员开了后门。

// sensitiveKeys 是命中即整体替换的字段名（不区分大小写、忽略下划线连字符）。
var sensitiveKeys = map[string]struct{}{
	"apikey":           {},
	"api_key":          {},
	"token":            {},
	"access_token":     {},
	"accesstoken":      {},
	"refresh_token":    {},
	"password":         {},
	"passwd":           {},
	"secret":           {},
	"client_secret":    {},
	"authorization":    {},
	"auth":             {},
	"cookie":           {},
	"setcookie":        {},
	"set_cookie":       {},
	"privatekey":       {},
	"private_key":      {},
	"credential":       {},
	"credentials":      {},
	"session":          {},
	"sessionid":        {},
	"session_id":       {},
	"dsn":              {},
	"connectionstring": {},
}

// RedactedPlaceholder 是被替换后的占位符。
const RedactedPlaceholder = "***REDACTED***"

// 形如 "Bearer eyJhbGciOi..." 的头部，以及常见 token 前缀。
var (
	bearerRe = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._\-+/=]{8,}`)
	// 常见厂商 Key 前缀：sk-xxx / ghp_xxx / xoxb-xxx / AKIA...
	knownKeyRe = regexp.MustCompile(`\b(sk-[A-Za-z0-9]{16,}|ghp_[A-Za-z0-9]{20,}|xox[baprs]-[A-Za-z0-9\-]{10,}|AKIA[0-9A-Z]{12,})\b`)
	// URL 里的 user:password@host
	urlCredRe = regexp.MustCompile(`(?i)(://[^:/\s]+):([^@/\s]+)@`)
)

// normalizeKey 统一大小写与分隔符，便于匹配 aPi_Key / api-key / apiKey。
func normalizeKey(k string) string {
	k = strings.ToLower(k)
	k = strings.ReplaceAll(k, "-", "")
	k = strings.ReplaceAll(k, "_", "")
	return k
}

// isSensitiveKey 判断字段名是否敏感。
func isSensitiveKey(k string) bool {
	n := normalizeKey(k)
	if _, ok := sensitiveKeys[n]; ok {
		return true
	}
	// 命中组合词：x-api-key、my_secret_value 之类
	for s := range sensitiveKeys {
		if s == "" {
			continue
		}
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}

// RedactJSON 对一段 JSON 做深度脱敏 + 可控截断。
//
// 行为：
//   - 对象字段名命中敏感词 → 值替换为占位符
//   - 任意字符串值命中 bearer / 厂商 Key 模式 → 部分替换
//   - 数组与嵌套对象递归处理
//   - 结果序列化后超过 maxBytes → 重新生成一个「截断标记 + 预览」的对象
//
// 返回 (处理后的字节, 是否脱敏过, 是否被截断)。
func RedactJSON(raw []byte, maxBytes int64) ([]byte, bool, bool) {
	if len(raw) == 0 {
		return raw, false, false
	}

	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		// 不是合法 JSON：当纯文本处理，只做模式替换
		s := redactString(string(raw))
		out := []byte(s)
		truncated := false
		if maxBytes > 0 && int64(len(out)) > maxBytes {
			out = out[:maxBytes]
			truncated = true
		}
		return out, s != string(raw), truncated
	}

	redacted := false
	v = redactValue(v, &redacted)

	out, err := json.Marshal(v)
	if err != nil {
		return raw, false, false
	}

	truncated := false
	if maxBytes > 0 && int64(len(out)) > maxBytes {
		preview := string(out[:minInt64(int64(len(out)), maxBytes)])
		envelope := map[string]any{
			"_truncated":     true,
			"_original_size": len(out),
			"_preview":       preview,
		}
		if b, err := json.Marshal(envelope); err == nil {
			out = b
		}
		truncated = true
	}

	return out, redacted, truncated
}

func redactValue(v any, redacted *bool) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if isSensitiveKey(k) {
				if s, ok := val.(string); ok && s == "" {
					continue
				}
				t[k] = RedactedPlaceholder
				*redacted = true
				continue
			}
			t[k] = redactValue(val, redacted)
		}
		return t

	case []any:
		for i := range t {
			t[i] = redactValue(t[i], redacted)
		}
		return t

	case string:
		s := redactString(t)
		if s != t {
			*redacted = true
		}
		return s

	default:
		return v
	}
}

func redactString(s string) string {
	if s == "" {
		return s
	}
	out := bearerRe.ReplaceAllString(s, "$1 "+RedactedPlaceholder)
	out = knownKeyRe.ReplaceAllString(out, RedactedPlaceholder)
	out = urlCredRe.ReplaceAllString(out, "$1:"+RedactedPlaceholder+"@")
	return out
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// 供测试与文档引用的导出包装。

// IsSensitiveKey 暴露字段名判断，便于管理端做「哪些字段会被脱敏」的说明。
func IsSensitiveKey(k string) bool { return isSensitiveKey(k) }

// RedactText 对纯文本做模式级脱敏。
func RedactText(s string) string { return redactString(s) }
