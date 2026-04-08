package parser

import (
	"strings"
	"testing"
)

func TestExtractTables_SimpleSelect(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders` JOIN `myproj.dataset.users` ON orders.user_id = users.id"
	result, err := ExtractTables(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.SourceTables) != 2 {
		t.Fatalf("expected 2 source tables, got %d: %+v", len(result.SourceTables), result.SourceTables)
	}
	paths := map[string]bool{}
	for _, ref := range result.SourceTables {
		paths[ref.Path] = true
	}
	if !paths["myproj.dataset.orders"] {
		t.Errorf("missing myproj.dataset.orders")
	}
	if !paths["myproj.dataset.users"] {
		t.Errorf("missing myproj.dataset.users")
	}
}

func TestExtractTables_WithCTE(t *testing.T) {
	sql := `
WITH cte AS (
  SELECT * FROM ` + "`myproj.dataset.orders`" + `
)
SELECT * FROM cte
`
	result, err := ExtractTables(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// cte should not appear as a source table
	for _, ref := range result.SourceTables {
		if ref.Path == "cte" {
			t.Errorf("CTE name should not appear as source table")
		}
	}
	if len(result.SourceTables) != 1 || result.SourceTables[0].Path != "myproj.dataset.orders" {
		t.Errorf("expected only myproj.dataset.orders, got %+v", result.SourceTables)
	}
}

func TestExtractTables_CreateTableAS(t *testing.T) {
	sql := "CREATE OR REPLACE TABLE `myproj.dataset.output` AS SELECT * FROM `myproj.dataset.input`"
	result, err := ExtractTables(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.DestinationTables) != 1 || result.DestinationTables[0].Path != "myproj.dataset.output" {
		t.Errorf("expected destination myproj.dataset.output, got %+v", result.DestinationTables)
	}
	if len(result.SourceTables) != 1 || result.SourceTables[0].Path != "myproj.dataset.input" {
		t.Errorf("expected source myproj.dataset.input, got %+v", result.SourceTables)
	}
}

func TestExtractTables_MergeStatement(t *testing.T) {
	sql := `
MERGE ` + "`myproj.dataset.target`" + ` AS T
USING ` + "`myproj.dataset.source`" + ` AS S
ON T.id = S.id
WHEN MATCHED THEN UPDATE SET T.value = S.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (S.id, S.value)
`
	result, err := ExtractTables(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	destPaths := map[string]bool{}
	for _, ref := range result.DestinationTables {
		destPaths[ref.Path] = true
	}
	srcPaths := map[string]bool{}
	for _, ref := range result.SourceTables {
		srcPaths[ref.Path] = true
	}
	if !destPaths["myproj.dataset.target"] {
		t.Errorf("expected myproj.dataset.target as destination")
	}
	if !srcPaths["myproj.dataset.source"] {
		t.Errorf("expected myproj.dataset.source as source")
	}
}

func TestExtractTables_Subquery(t *testing.T) {
	sql := `
SELECT * FROM (
  SELECT a.*, b.name
  FROM ` + "`myproj.dataset.orders`" + ` a
  JOIN ` + "`myproj.dataset.products`" + ` b ON a.product_id = b.id
) sub
`
	result, err := ExtractTables(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.SourceTables) != 2 {
		t.Fatalf("expected 2 source tables, got %d", len(result.SourceTables))
	}
}

func TestExtractTables_ScriptWithDeclare(t *testing.T) {
	sql := `
DECLARE x INT64 DEFAULT 1;
SELECT * FROM ` + "`myproj.dataset.orders`" + `;
`
	result, err := ExtractTables(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.SourceTables) != 1 || result.SourceTables[0].Path != "myproj.dataset.orders" {
		t.Errorf("expected myproj.dataset.orders, got %+v", result.SourceTables)
	}
}

func TestExtractTables_Offsets(t *testing.T) {
	sql := "SELECT * FROM `myproj.dataset.orders`"
	result, err := ExtractTables(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ref := result.SourceTables[0]
	if ref.StartOffset == 0 && ref.EndOffset == 0 {
		t.Errorf("expected non-zero offsets, got start=%d end=%d", ref.StartOffset, ref.EndOffset)
	}
}

func TestClassifySQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want SQLKind
	}{
		{"SELECT", "SELECT 1", SQLKindSelect},
		{"SELECT FROM", "SELECT * FROM `p.d.t`", SQLKindSelect},
		{"CREATE TABLE AS", "CREATE OR REPLACE TABLE `p.d.t` AS SELECT 1", SQLKindCreateTableAS},
		{"INSERT", "INSERT INTO `p.d.t` (id) VALUES (1)", SQLKindInsert},
		{"DELETE", "DELETE FROM `p.d.t` WHERE id = 1", SQLKindDelete},
		{"UPDATE", "UPDATE `p.d.t` SET x = 1 WHERE id = 1", SQLKindUpdate},
		{"MERGE", "MERGE `p.d.t` T USING `p.d.s` S ON T.id = S.id WHEN MATCHED THEN UPDATE SET T.x = S.x", SQLKindMerge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ClassifySQL(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ClassifySQL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStripDDL_Select(t *testing.T) {
	sql := "SELECT * FROM `p.d.t`"
	got, kind, err := StripDDL(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != SQLKindSelect {
		t.Errorf("expected SELECT, got %v", kind)
	}
	if got != sql {
		t.Errorf("expected unchanged SQL, got %q", got)
	}
}

func TestStripDDL_CreateTableAS(t *testing.T) {
	sql := "CREATE OR REPLACE TABLE `p.d.output` AS SELECT user_id, SUM(amount) FROM `p.d.orders` GROUP BY user_id"
	got, kind, err := StripDDL(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != SQLKindCreateTableAS {
		t.Errorf("expected CREATE TABLE AS, got %v", kind)
	}
	if !strings.Contains(got, "SELECT") {
		t.Errorf("expected SELECT in result, got %q", got)
	}
	if strings.Contains(got, "CREATE") {
		t.Errorf("expected no CREATE in result, got %q", got)
	}
}

func TestStripDDL_RejectsInsert(t *testing.T) {
	sql := "INSERT INTO `p.d.t` (id) VALUES (1)"
	_, _, err := StripDDL(sql)
	if err == nil {
		t.Error("expected error for INSERT")
	}
}

func TestStripDDL_RejectsDelete(t *testing.T) {
	sql := "DELETE FROM `p.d.t` WHERE id = 1"
	_, _, err := StripDDL(sql)
	if err == nil {
		t.Error("expected error for DELETE")
	}
}

func TestStripDDL_RejectsUpdate(t *testing.T) {
	sql := "UPDATE `p.d.t` SET x = 1 WHERE id = 1"
	_, _, err := StripDDL(sql)
	if err == nil {
		t.Error("expected error for UPDATE")
	}
}

func TestStripDDL_RejectsMerge(t *testing.T) {
	sql := "MERGE `p.d.t` T USING `p.d.s` S ON T.id = S.id WHEN MATCHED THEN UPDATE SET T.x = S.x"
	_, _, err := StripDDL(sql)
	if err == nil {
		t.Error("expected error for MERGE")
	}
}

func TestStripDDL_RejectsDDL(t *testing.T) {
	sql := "CREATE TABLE `p.d.t` (id INT64)"
	_, _, err := StripDDL(sql)
	if err == nil {
		t.Error("expected error for DDL")
	}
}

func TestStripDDL_MultiStatementScript(t *testing.T) {
	sql := "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM `p.d.orders`;"
	got, kind, err := StripDDL(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != SQLKindSelect {
		t.Errorf("expected SELECT, got %v", kind)
	}
	// Multi-statement scripts are returned unchanged
	if got != sql {
		t.Errorf("expected original SQL preserved, got %q", got)
	}
}

func TestClassifySQL_MultiStatement(t *testing.T) {
	sql := "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM `p.d.orders`;"
	got, err := ClassifySQL(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != SQLKindSelect {
		t.Errorf("expected SELECT for multi-statement script, got %v", got)
	}
}
