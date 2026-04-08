package rewriter

import (
	"strings"
	"testing"
)

func TestRewrite_SimpleSelect(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders` WHERE amount > 100"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "SELECT * FROM `orders` WHERE amount > 100"
	if result.SQL != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, result.SQL)
	}
	if result.RewrittenTables["myproj.dataset.orders"] != "orders" {
		t.Errorf("expected rewrite map entry")
	}
}

func TestRewrite_MultipleTableJoin(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders` o JOIN `myproj.dataset.users` u ON o.user_id = u.id"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
		"myproj.dataset.users":  "users",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.SQL, "FROM `orders` o") {
		t.Errorf("expected orders replacement, got: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "JOIN `users` u") {
		t.Errorf("expected users replacement, got: %s", result.SQL)
	}
}

func TestRewrite_PartialRewrite(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders` o JOIN `myproj.dataset.users` u ON o.user_id = u.id"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	passthrough := map[string]bool{
		"myproj.dataset.users": true,
	}
	result, err := Rewrite(sql, rewriteMap, passthrough)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.SQL, "FROM `orders` o") {
		t.Errorf("expected orders replacement, got: %s", result.SQL)
	}
	// users should remain unchanged
	if !strings.Contains(result.SQL, "`myproj.dataset.users`") {
		t.Errorf("expected users to remain, got: %s", result.SQL)
	}
}

func TestRewrite_UnresolvedTables(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders` o JOIN `myproj.dataset.users` u ON o.user_id = u.id"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.UnresolvedTables) != 1 || result.UnresolvedTables[0] != "myproj.dataset.users" {
		t.Errorf("expected unresolved myproj.dataset.users, got %v", result.UnresolvedTables)
	}
}

func TestRewrite_WithCTE(t *testing.T) {
	sql := "WITH cte AS (SELECT * FROM `myproj.dataset.orders`) SELECT * FROM cte"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.SQL, "FROM `orders`") {
		t.Errorf("expected orders replacement, got: %s", result.SQL)
	}
	// CTE reference should remain
	if !strings.Contains(result.SQL, "FROM cte") {
		t.Errorf("expected cte reference to remain, got: %s", result.SQL)
	}
}

func TestRewrite_TwoPartRefMatchesThreePartFixture(t *testing.T) {
	sql := "SELECT * FROM `dx_018_reservation.reservation_basic` WHERE id = 1"
	rewriteMap := map[string]string{
		"m2m-core.dx_018_reservation.reservation_basic": "reservation_basic",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "SELECT * FROM `reservation_basic` WHERE id = 1"
	if result.SQL != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, result.SQL)
	}
	if result.RewrittenTables["dx_018_reservation.reservation_basic"] != "reservation_basic" {
		t.Errorf("expected rewrite map entry for 2-part ref, got %v", result.RewrittenTables)
	}
	if len(result.UnresolvedTables) != 0 {
		t.Errorf("expected no unresolved tables, got %v", result.UnresolvedTables)
	}
}

func TestRewrite_TwoPartRefWithMultipleProjects(t *testing.T) {
	sql := "SELECT * FROM `dataset.orders` o JOIN `dataset.users` u ON o.user_id = u.id"
	rewriteMap := map[string]string{
		"projA.dataset.orders": "orders",
		"projB.dataset.users":  "users",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.SQL, "FROM `orders` o") {
		t.Errorf("expected orders replacement, got: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "JOIN `users` u") {
		t.Errorf("expected users replacement, got: %s", result.SQL)
	}
}

func TestRewrite_ThreePartRefStillWorks(t *testing.T) {
	// Ensure 3-part refs still work as before (no regression)
	sql := "SELECT * FROM `myproj.dataset.orders` WHERE amount > 100"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "SELECT * FROM `orders` WHERE amount > 100"
	if result.SQL != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, result.SQL)
	}
}

func TestRewrite_TwoPartRefUnresolvedWhenNoProjectMatch(t *testing.T) {
	sql := "SELECT * FROM `dataset.unknown_table`"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.UnresolvedTables) != 1 || result.UnresolvedTables[0] != "dataset.unknown_table" {
		t.Errorf("expected unresolved dataset.unknown_table, got %v", result.UnresolvedTables)
	}
}

func TestRewrite_ReservedWordTableName(t *testing.T) {
	sql := "SELECT * FROM `m2m-core.rm_hozin_case.case` WHERE id = 1"
	rewriteMap := map[string]string{
		"m2m-core.rm_hozin_case.case": "case",
	}
	result, err := Rewrite(sql, rewriteMap, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// "case" is a reserved word; temp name must be backtick-quoted
	expected := "SELECT * FROM `case` WHERE id = 1"
	if result.SQL != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, result.SQL)
	}
}

func TestValidateRewrite_TwoPartRefMatchesThreePartFixture(t *testing.T) {
	sql := "SELECT * FROM `dx_018_reservation.reservation_basic`"
	rewriteMap := map[string]string{
		"m2m-core.dx_018_reservation.reservation_basic": "reservation_basic",
	}
	if err := ValidateRewrite(sql, rewriteMap, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateRewrite_Success(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders`"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	if err := ValidateRewrite(sql, rewriteMap, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateRewrite_Failure(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders` JOIN `myproj.dataset.users` ON 1=1"
	rewriteMap := map[string]string{
		"myproj.dataset.orders": "orders",
	}
	err := ValidateRewrite(sql, rewriteMap, nil)
	if err == nil {
		t.Errorf("expected validation error for missing users fixture")
	}
}
