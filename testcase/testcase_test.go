package testcase

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParse_Basic(t *testing.T) {
	yaml := `
test_name: basic_test
description: A simple test
sql: "SELECT * FROM ` + "`myproj.dataset.orders`" + `"
fixtures:
  - table: myproj.dataset.orders
    rows:
      - {order_id: 1, user_id: 10, amount: 100}
      - {order_id: 2, user_id: 10, amount: 200}
expected:
  rows:
    - {order_id: 1, user_id: 10, amount: 100}
    - {order_id: 2, user_id: 10, amount: 200}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.TestName != "basic_test" {
		t.Errorf("expected test_name=basic_test, got %s", tc.TestName)
	}
	if len(tc.Fixtures) != 1 {
		t.Fatalf("expected 1 fixture, got %d", len(tc.Fixtures))
	}
	if len(tc.Fixtures[0].Rows) != 2 {
		t.Errorf("expected 2 fixture rows, got %d", len(tc.Fixtures[0].Rows))
	}
	if len(tc.Expected.Rows) != 2 {
		t.Errorf("expected 2 expected rows, got %d", len(tc.Expected.Rows))
	}
}

func TestParse_WithTempName(t *testing.T) {
	yaml := `
test_name: temp_name_test
sql: "SELECT 1"
fixtures:
  - table: myproj.dataset.orders
    temp_name: my_orders
    rows:
      - {order_id: 1}
expected:
  rows:
    - {order_id: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := tc.RewriteMap()
	if m["myproj.dataset.orders"] != "my_orders" {
		t.Errorf("expected my_orders, got %s", m["myproj.dataset.orders"])
	}
}

func TestParse_DefaultTempName(t *testing.T) {
	yaml := `
test_name: default_temp_test
sql: "SELECT 1"
fixtures:
  - table: myproj.dataset.orders
    rows:
      - {order_id: 1}
expected:
  rows:
    - {order_id: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := tc.RewriteMap()
	if m["myproj.dataset.orders"] != "orders" {
		t.Errorf("expected orders, got %s", m["myproj.dataset.orders"])
	}
}

func TestParse_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{"missing test_name", `sql: "SELECT 1"
expected:
  rows: [{a: 1}]`},
		{"missing sql", `test_name: test
expected:
  rows: [{a: 1}]`},
		{"missing expected", `test_name: test
sql: "SELECT 1"
fixtures:
  - table: t
    rows: [{a: 1}]`},
		{"fixture missing table", `test_name: test
sql: "SELECT 1"
fixtures:
  - rows: [{a: 1}]
expected:
  rows: [{a: 1}]`},
		{"fixture missing rows", `test_name: test
sql: "SELECT 1"
fixtures:
  - table: t
expected:
  rows: [{a: 1}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Errorf("expected validation error")
			}
		})
	}
}

func TestLoadFile_WithSQLFile(t *testing.T) {
	dir := t.TempDir()

	sqlContent := "SELECT * FROM `myproj.dataset.orders`"
	if err := os.WriteFile(filepath.Join(dir, "query.sql"), []byte(sqlContent), 0644); err != nil {
		t.Fatal(err)
	}

	yamlContent := `
test_name: file_test
sql_file: query.sql
fixtures:
  - table: myproj.dataset.orders
    rows:
      - {order_id: 1}
expected:
  rows:
    - {order_id: 1}
`
	yamlPath := filepath.Join(dir, "test.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	tc, err := LoadFile(yamlPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.SQL != sqlContent {
		t.Errorf("expected SQL to be loaded from file, got %q", tc.SQL)
	}
}

func TestRewriteMap_CollisionAutoDisambiguate(t *testing.T) {
	yaml := `
test_name: collision_test
sql: "SELECT 1"
fixtures:
  - table: m2m-core.sumyca_prod.reservation
    rows:
      - {id: 1}
  - table: m2m-core.m2m_core_prod.reservation
    rows:
      - {id: 2}
expected:
  rows:
    - {id: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := tc.RewriteMap()
	name1 := m["m2m-core.sumyca_prod.reservation"]
	name2 := m["m2m-core.m2m_core_prod.reservation"]
	if name1 == name2 {
		t.Errorf("expected different temp names, both got %s", name1)
	}
	if name1 != "sumyca_prod__reservation" {
		t.Errorf("expected sumyca_prod__reservation, got %s", name1)
	}
	if name2 != "m2m_core_prod__reservation" {
		t.Errorf("expected m2m_core_prod__reservation, got %s", name2)
	}
}

func TestRewriteMap_CustomTempNameNotOverridden(t *testing.T) {
	yaml := `
test_name: custom_temp_test
sql: "SELECT 1"
fixtures:
  - table: m2m-core.sumyca_prod.reservation
    temp_name: my_reservation
    rows:
      - {id: 1}
  - table: m2m-core.m2m_core_prod.reservation
    rows:
      - {id: 2}
expected:
  rows:
    - {id: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := tc.RewriteMap()
	if m["m2m-core.sumyca_prod.reservation"] != "my_reservation" {
		t.Errorf("expected my_reservation, got %s", m["m2m-core.sumyca_prod.reservation"])
	}
	// Only one auto-generated name — no collision, so no prefix needed.
	if m["m2m-core.m2m_core_prod.reservation"] != "reservation" {
		t.Errorf("expected reservation, got %s", m["m2m-core.m2m_core_prod.reservation"])
	}
}

func TestRewriteMap_TwoPartVsThreePartSameTable(t *testing.T) {
	yaml := `
test_name: dedup_test
sql: "SELECT 1"
fixtures:
  - table: dx_018_reservation.reservation_basic
    rows:
      - {id: 1}
  - table: m2m-core.dx_018_reservation.reservation_basic
    rows:
      - {id: 2}
expected:
  rows:
    - {id: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := tc.RewriteMap()
	// Both refer to the same dataset.table — only one entry should appear.
	// The 2-part key should be in the map (it was first).
	if len(m) != 1 {
		t.Errorf("expected 1 entry in rewrite map, got %d: %v", len(m), m)
	}
}

func TestRewriteMap_HyphenInDatasetSanitized(t *testing.T) {
	yaml := `
test_name: sanitize_test
sql: "SELECT 1"
fixtures:
  - table: proj.my-dataset.orders
    rows:
      - {id: 1}
  - table: proj.other-dataset.orders
    rows:
      - {id: 2}
expected:
  rows:
    - {id: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := tc.RewriteMap()
	if m["proj.my-dataset.orders"] != "my_dataset__orders" {
		t.Errorf("expected my_dataset__orders, got %s", m["proj.my-dataset.orders"])
	}
	if m["proj.other-dataset.orders"] != "other_dataset__orders" {
		t.Errorf("expected other_dataset__orders, got %s", m["proj.other-dataset.orders"])
	}
}

func TestRewriteMap_NoCollisionNoPrefix(t *testing.T) {
	yaml := `
test_name: no_collision_test
sql: "SELECT 1"
fixtures:
  - table: proj.dataset.orders
    rows:
      - {id: 1}
  - table: proj.dataset.users
    rows:
      - {id: 2}
expected:
  rows:
    - {id: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := tc.RewriteMap()
	if m["proj.dataset.orders"] != "orders" {
		t.Errorf("expected orders, got %s", m["proj.dataset.orders"])
	}
	if m["proj.dataset.users"] != "users" {
		t.Errorf("expected users, got %s", m["proj.dataset.users"])
	}
}

func TestParse_ColumnsWithEmptyRows(t *testing.T) {
	yaml := `
test_name: empty_table_test
sql: "SELECT 1"
fixtures:
  - table: myproj.dataset.empty_tbl
    columns:
      id: INT64
      name: STRING
    rows: []
expected:
  rows:
    - {a: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tc.Fixtures[0].Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(tc.Fixtures[0].Columns))
	}
	if len(tc.Fixtures[0].Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(tc.Fixtures[0].Rows))
	}
}

func TestParse_ColumnsWithoutRowsField(t *testing.T) {
	yaml := `
test_name: columns_only_test
sql: "SELECT 1"
fixtures:
  - table: myproj.dataset.tbl
    columns:
      amount: FLOAT64
expected:
  rows:
    - {a: 1}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tc.Fixtures[0].Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(tc.Fixtures[0].Columns))
	}
}

func TestParse_NoRowsNoColumnsNoSQL(t *testing.T) {
	yaml := `
test_name: invalid_test
sql: "SELECT 1"
fixtures:
  - table: myproj.dataset.tbl
expected:
  rows:
    - {a: 1}
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Errorf("expected validation error for fixture with no rows, columns, or sql")
	}
}

func TestParse_SQLFixture(t *testing.T) {
	yaml := `
test_name: sql_fixture_test
sql: "SELECT 1"
fixtures:
  - table: myproj.dataset.orders
    sql: "SELECT 1 AS order_id, 10 AS user_id"
expected:
  rows:
    - {order_id: 1, user_id: 10}
`
	tc, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.Fixtures[0].SQL == "" {
		t.Errorf("expected SQL fixture to be set")
	}
}
