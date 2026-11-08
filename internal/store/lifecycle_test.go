package store

import (
	"strings"
	"testing"
)

// TestDisabledReasonSetsMatchDDL 锁住原因取值与 0001_schema.sql 的 CHECK 一致。
//
// 这组常量是「续费能否原样恢复」的唯一依据：多一个值数据库直接拒绝，
// 少一个值意味着某种状态再也恢复不回来。改这里必须同步改迁移脚本。
func TestDisabledReasonSetsMatchDDL(t *testing.T) {
	wantKeys := []string{"plan_expiry", "admin", "tenant_closed"}
	wantNamespaces := []string{"plan_expiry", "admin", "registry_gone"}

	if len(keyDisabledReasons) != len(wantKeys) {
		t.Fatalf("api_keys 允许的禁用原因应有 %d 个，实际 %d 个", len(wantKeys), len(keyDisabledReasons))
	}
	for _, r := range wantKeys {
		if _, ok := keyDisabledReasons[r]; !ok {
			t.Errorf("api_keys 缺少禁用原因 %q", r)
		}
	}

	if len(namespaceDisabledReasons) != len(wantNamespaces) {
		t.Fatalf("namespaces 允许的禁用原因应有 %d 个，实际 %d 个",
			len(wantNamespaces), len(namespaceDisabledReasons))
	}
	for _, r := range wantNamespaces {
		if _, ok := namespaceDisabledReasons[r]; !ok {
			t.Errorf("namespaces 缺少禁用原因 %q", r)
		}
	}
}

// TestKeyDisabledReasonRejectsRegistryGone 说明两张表的取值**不通用**。
//
// registry_gone 只对命名空间有意义；把它喂给 api_keys 会被 CHECK 拒绝，
// 因此在应用层就拦下，而不是留给数据库抛 constraint violation。
func TestKeyDisabledReasonRejectsRegistryGone(t *testing.T) {
	if err := validateKeyDisabledReason(DisabledByRegistryGone); err == nil {
		t.Fatal("registry_gone 不该被 api_keys 接受")
	}
	if err := validateNamespaceDisabledReason(DisabledByTenantClosed); err == nil {
		t.Fatal("tenant_closed 不该被 namespaces 接受")
	}
}

func TestDisabledReasonValidatorsAcceptKnownValues(t *testing.T) {
	for _, r := range []string{DisabledByPlanExpiry, DisabledByAdmin, DisabledByTenantClosed} {
		if err := validateKeyDisabledReason(r); err != nil {
			t.Errorf("api_keys 应接受 %q，却报错: %v", r, err)
		}
	}
	for _, r := range []string{DisabledByPlanExpiry, DisabledByAdmin, DisabledByRegistryGone} {
		if err := validateNamespaceDisabledReason(r); err != nil {
			t.Errorf("namespaces 应接受 %q，却报错: %v", r, err)
		}
	}
}

func TestDisabledReasonValidatorRejectsEmptyAndUnknown(t *testing.T) {
	for _, r := range []string{"", "PLAN_EXPIRY", "plan-expiry", "unknown"} {
		if err := validateKeyDisabledReason(r); err == nil {
			t.Errorf("api_keys 不该接受 %q", r)
		}
	}
}

// TestNormalizeNamespaceFillsDisabledBy 保证「只想关掉、没写原因」不会炸在数据库上。
//
// 数据库有 namespaces_disabled_consistency CHECK (enabled OR disabled_by IS NOT NULL)，
// 若这里不补值，UpsertNamespace 会以 constraint violation 失败，
// 而真正的原因是调用方少填了一个字段——报错信息完全对不上。
func TestNormalizeNamespaceFillsDisabledBy(t *testing.T) {
	ns := Namespace{
		Slug:       "demo",
		Transport:  TransportStdio,
		Command:    "/bin/echo",
		Enabled:    false,
		DisabledBy: nil,
		Source:     SourceManual,
	}
	if err := NormalizeNamespace(&ns); err != nil {
		t.Fatalf("规范化失败: %v", err)
	}
	if ns.DisabledBy == nil || *ns.DisabledBy != DisabledByAdmin {
		t.Fatalf("enabled=false 且未给原因时应补 admin，实际 %v", ns.DisabledBy)
	}
}

func TestNormalizeNamespaceClearsDisabledByWhenEnabled(t *testing.T) {
	reason := DisabledByRegistryGone
	ns := Namespace{
		Slug:       "demo",
		Transport:  TransportHTTP,
		Enabled:    true,
		DisabledBy: &reason,
		Source:     SourceRegistry,
	}
	ns.DiscoveryService = "demo-svc"
	if err := NormalizeNamespace(&ns); err != nil {
		t.Fatalf("规范化失败: %v", err)
	}
	if ns.DisabledBy != nil {
		t.Fatalf("enabled=true 时必须清空 disabled_by，实际 %q", *ns.DisabledBy)
	}
}

func TestNormalizeNamespaceRejectsBadReason(t *testing.T) {
	bad := "tenant_closed" // 属于 api_keys，不属于 namespaces
	ns := Namespace{
		Slug:       "demo",
		Transport:  TransportStdio,
		Command:    "/bin/echo",
		Enabled:    false,
		DisabledBy: &bad,
	}
	err := NormalizeNamespace(&ns)
	if err == nil {
		t.Fatal("namespaces 不该接受 tenant_closed")
	}
	if !strings.Contains(err.Error(), "disabled_by") && !strings.Contains(err.Error(), "禁用原因") {
		t.Fatalf("错误信息应指明是禁用原因问题，实际: %v", err)
	}
}
