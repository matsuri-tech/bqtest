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
