// Package migrations 通过 go:embed 把 SQL 迁移脚本编进二进制，
// 避免部署时遗漏迁移目录。
package migrations

import "embed"

// FS 包含本目录下全部 .sql 文件，按文件名升序执行。
//
//go:embed *.sql
var FS embed.FS
