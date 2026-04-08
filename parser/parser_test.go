package parser

import (
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
