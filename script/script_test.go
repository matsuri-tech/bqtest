package script

import (
	"strings"
	"testing"

	"github.com/matsuri-tech/bqtest/testcase"
)

func TestGenerate_Basic(t *testing.T) {
	tc := &testcase.TestCase{
		TestName: "basic",
		SQL:      "SELECT * FROM orders",
		Fixtures: []testcase.Fixture{
			{
				Table:    "myproj.dataset.orders",
				TempName: "orders",
				Rows: []map[string]any{
					{"order_id": 1, "amount": 100},
					{"order_id": 2, "amount": 200},
				},
			},
		},
		Expected: testcase.Expected{
			Rows: []map[string]any{
				{"order_id": 1, "amount": 100},
				{"order_id": 2, "amount": 200},
			},
		},
	}

	result := Generate(tc, "SELECT * FROM orders")

	// Should contain fixture TEMP TABLE with UNNEST
	if !strings.Contains(result, "CREATE TEMP TABLE orders AS") {
		t.Errorf("expected fixture temp table, got:\n%s", result)
	}
	if !strings.Contains(result, "STRUCT(") {
		t.Errorf("expected STRUCT in fixture, got:\n%s", result)
	}

	// Should contain the rewritten SQL
	if !strings.Contains(result, "SELECT * FROM orders;") {
		t.Errorf("expected rewritten SQL, got:\n%s", result)
	}

	// Should NOT contain expected temp table or diff queries (moved to Go side)
	if strings.Contains(result, "__bqtest_expected") {
		t.Errorf("expected no __bqtest_expected (diff is now Go-side), got:\n%s", result)
	}
}

func TestGenerate_SQLFixture(t *testing.T) {
	tc := &testcase.TestCase{
		TestName: "sql_fixture",
		SQL:      "SELECT * FROM orders",
		Fixtures: []testcase.Fixture{
			{
				Table:    "myproj.dataset.orders",
				TempName: "orders",
				SQL:      "SELECT 1 AS order_id, 100 AS amount",
			},
		},
		Expected: testcase.Expected{
			Rows: []map[string]any{
				{"order_id": 1, "amount": 100},
			},
		},
	}

	result := Generate(tc, "SELECT * FROM orders")

	if !strings.Contains(result, "CREATE TEMP TABLE orders AS\nSELECT 1 AS order_id, 100 AS amount;") {
		t.Errorf("expected SQL fixture, got:\n%s", result)
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{nil, "NULL"},
		{"hello", "'hello'"},
		{true, "TRUE"},
		{false, "FALSE"},
		{42, "42"},
		{int64(99), "99"},
		{3.14, "3.14"},
		{100.0, "100"},
	}
	for _, tt := range tests {
		got := formatValue(tt.input)
		if got != tt.expected {
			t.Errorf("formatValue(%v): expected %s, got %s", tt.input, tt.expected, got)
		}
	}
}
