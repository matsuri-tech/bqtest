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

	// Should contain fixture TEMP TABLE with UNNEST (backtick-quoted)
	if !strings.Contains(result, "CREATE TEMP TABLE `orders` AS") {
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

	if !strings.Contains(result, "CREATE TEMP TABLE `orders` AS\nSELECT 1 AS order_id, 100 AS amount;") {
		t.Errorf("expected SQL fixture, got:\n%s", result)
	}
}

func TestGenerate_CollisionUsesRewriteMap(t *testing.T) {
	tc := &testcase.TestCase{
		TestName: "collision",
		SQL:      "SELECT * FROM sumyca_prod__reservation",
		Fixtures: []testcase.Fixture{
			{
				Table: "m2m-core.sumyca_prod.reservation",
				Rows:  []map[string]any{{"id": 1}},
			},
			{
				Table: "m2m-core.m2m_core_prod.reservation",
				Rows:  []map[string]any{{"id": 2}},
			},
		},
		Expected: testcase.Expected{
			Rows: []map[string]any{{"id": 1}},
		},
	}

	result := Generate(tc, "SELECT * FROM sumyca_prod__reservation")

	// Should use disambiguated names from RewriteMap, not just "reservation"
	if !strings.Contains(result, "CREATE TEMP TABLE `sumyca_prod__reservation` AS") {
		t.Errorf("expected disambiguated temp name sumyca_prod__reservation, got:\n%s", result)
	}
	if !strings.Contains(result, "CREATE TEMP TABLE `m2m_core_prod__reservation` AS") {
		t.Errorf("expected disambiguated temp name m2m_core_prod__reservation, got:\n%s", result)
	}
	// Should NOT have a bare "CREATE TEMP TABLE reservation AS"
	if strings.Contains(result, "CREATE TEMP TABLE `reservation` AS") {
		t.Errorf("should not have bare 'reservation' temp table, got:\n%s", result)
	}
}

func TestGenerate_DedupFixtures(t *testing.T) {
	tc := &testcase.TestCase{
		TestName: "dedup",
		SQL:      "SELECT * FROM reservation_basic",
		Fixtures: []testcase.Fixture{
			{
				Table: "dx_018_reservation.reservation_basic",
				Rows:  []map[string]any{{"id": 1}},
			},
			{
				Table: "m2m-core.dx_018_reservation.reservation_basic",
				Rows:  []map[string]any{{"id": 2}},
			},
		},
		Expected: testcase.Expected{
			Rows: []map[string]any{{"id": 1}},
		},
	}

	result := Generate(tc, "SELECT * FROM reservation_basic")

	// Should only create one TEMP TABLE since both refer to the same dataset.table
	count := strings.Count(result, "CREATE TEMP TABLE")
	if count != 1 {
		t.Errorf("expected 1 CREATE TEMP TABLE (dedup), got %d:\n%s", count, result)
	}
}

func TestGenerate_ReservedWordTableName(t *testing.T) {
	tc := &testcase.TestCase{
		TestName: "reserved_word",
		SQL:      "SELECT * FROM `case`",
		Fixtures: []testcase.Fixture{
			{
				Table:    "m2m-core.rm_hozin_case.case",
				TempName: "case",
				Rows: []map[string]any{
					{"id": 1, "name": "test"},
				},
			},
		},
		Expected: testcase.Expected{
			Rows: []map[string]any{
				{"id": 1, "name": "test"},
			},
		},
	}

	result := Generate(tc, "SELECT * FROM `case`")

	// temp table name must be backtick-quoted to avoid reserved word conflict
	if !strings.Contains(result, "CREATE TEMP TABLE `case` AS") {
		t.Errorf("expected backtick-quoted temp table for reserved word, got:\n%s", result)
	}
	// The rewritten SQL should also use backtick-quoted temp name
	if !strings.Contains(result, "SELECT * FROM `case`;") {
		t.Errorf("expected backtick-quoted reference in rewritten SQL, got:\n%s", result)
	}
}

func TestGenerate_EmptyRowsWithColumns(t *testing.T) {
	tc := &testcase.TestCase{
		TestName: "empty_table",
		SQL:      "SELECT * FROM empty_tbl",
		Fixtures: []testcase.Fixture{
			{
				Table:    "myproj.dataset.empty_tbl",
				TempName: "empty_tbl",
				Columns: map[string]string{
					"id":   "INT64",
					"name": "STRING",
				},
				Rows: []map[string]any{},
			},
		},
		Expected: testcase.Expected{
			Rows: []map[string]any{},
		},
	}

	result := Generate(tc, "SELECT * FROM empty_tbl")

	expected := "CREATE TEMP TABLE `empty_tbl` AS\nSELECT CAST(NULL AS INT64) AS id, CAST(NULL AS STRING) AS name LIMIT 0;"
	if !strings.Contains(result, expected) {
		t.Errorf("expected empty table SQL:\n%s\n\ngot:\n%s", expected, result)
	}
}

func TestGenerate_ColumnsOnlyNoRows(t *testing.T) {
	// Fixture with columns and nil Rows (not explicitly set)
	tc := &testcase.TestCase{
		TestName: "columns_only",
		SQL:      "SELECT * FROM tbl",
		Fixtures: []testcase.Fixture{
			{
				Table:    "myproj.dataset.tbl",
				TempName: "tbl",
				Columns: map[string]string{
					"amount": "FLOAT64",
				},
			},
		},
		Expected: testcase.Expected{
			Rows: []map[string]any{},
		},
	}

	result := Generate(tc, "SELECT * FROM tbl")

	expected := "CAST(NULL AS FLOAT64) AS amount LIMIT 0"
	if !strings.Contains(result, expected) {
		t.Errorf("expected CAST expression, got:\n%s", result)
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
