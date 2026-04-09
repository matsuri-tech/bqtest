package script

import (
	"fmt"
	"sort"
	"strings"

	"github.com/matsuri-tech/bqtest/testcase"
)

// Generate builds a BigQuery script that:
// 1. Creates TEMP TABLEs for fixtures
// 2. Executes the rewritten SQL and returns the result
func Generate(tc *testcase.TestCase, rewrittenSQL string) string {
	var sb strings.Builder

	// Use RewriteMap to get disambiguated temp names and deduplicated fixtures.
	rewriteMap := tc.RewriteMap()

	// Track which temp names have already been emitted to handle dedup
	// (RewriteMap deduplicates fixtures that refer to the same table).
	emitted := make(map[string]bool)

	// 1. Fixture TEMP TABLEs
	for _, f := range tc.Fixtures {
		tempName, ok := rewriteMap[f.Table]
		if !ok {
			// This fixture was deduplicated away by RewriteMap; skip it.
			continue
		}
		if emitted[tempName] {
			// Already created this temp table (dedup case).
			continue
		}
		emitted[tempName] = true
		sb.WriteString(generateFixtureSQL(tempName, f))
		sb.WriteString("\n\n")
	}

	// 2. Execute rewritten SQL and return results
	fmt.Fprintf(&sb, "%s;", rewrittenSQL)

	return sb.String()
}

func generateFixtureSQL(tempName string, f testcase.Fixture) string {
	if f.SQL != "" {
		return fmt.Sprintf("CREATE TEMP TABLE `%s` AS\n%s;", tempName, f.SQL)
	}
	if len(f.Columns) > 0 && len(f.Rows) == 0 {
		return fmt.Sprintf("CREATE TEMP TABLE `%s` AS\n%s;", tempName, generateEmptyTableSQL(f.Columns))
	}
	return fmt.Sprintf("CREATE TEMP TABLE `%s` AS\nSELECT * FROM UNNEST([%s]);", tempName, generateStructArray(f.Rows, f.Columns))
}

// generateEmptyTableSQL builds a SELECT with CAST(NULL AS type) for each column, with LIMIT 0.
func generateEmptyTableSQL(columns map[string]string) string {
	keys := sortedColumnKeys(columns)
	var fields []string
	for _, k := range keys {
		colType := columns[k]
		if colType == "" || strings.ContainsAny(colType, ";'\"\\") {
			// Defensive: skip columns with empty or suspicious type strings.
			// Validation in testcase should catch this earlier.
			continue
		}
		fields = append(fields, fmt.Sprintf("CAST(NULL AS %s) AS `%s`", strings.ToUpper(colType), k))
	}
	return fmt.Sprintf("SELECT %s LIMIT 0", strings.Join(fields, ", "))
}

func sortedColumnKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// generateStructArray builds a comma-separated list of STRUCT literals from rows.
// If columns is provided, values are automatically cast to the specified types.
func generateStructArray(rows []map[string]any, columns map[string]string) string {
	if len(rows) == 0 {
		return ""
	}
	keys := sortedKeys(rows[0])

	var structs []string
	for _, row := range rows {
		var fields []string
		for _, k := range keys {
			colType := ""
			if columns != nil {
				colType = strings.ToUpper(columns[k])
			}
			fields = append(fields, fmt.Sprintf("%s AS %s", formatValue(row[k], colType), k))
		}
		structs = append(structs, fmt.Sprintf("STRUCT(%s)", strings.Join(fields, ", ")))
	}
	return "\n  " + strings.Join(structs, ",\n  ") + "\n"
}

func sortedKeys(row map[string]any) []string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// literalTypes are BigQuery types that use the TYPE 'value' literal syntax.
var literalTypes = map[string]bool{
	"DATE":       true,
	"TIMESTAMP":  true,
	"DATETIME":   true,
	"TIME":       true,
	"NUMERIC":    true,
	"BIGNUMERIC": true,
	"JSON":       true,
}

// formatValue converts a Go value to a BigQuery SQL literal.
// If colType is non-empty, the value is cast/typed to that BigQuery type.
func formatValue(v any, colType string) string {
	if colType != "" {
		return formatWithColumnType(v, colType)
	}

	switch val := v.(type) {
	case nil:
		return "NULL"
	case string:
		escaped := strings.ReplaceAll(val, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	default:
		return fmt.Sprintf("'%v'", val)
	}
}

// formatWithColumnType wraps a raw value with the specified BigQuery type.
func formatWithColumnType(v any, colType string) string {
	if v == nil {
		return fmt.Sprintf("CAST(NULL AS %s)", colType)
	}

	raw := fmt.Sprintf("%v", v)

	if literalTypes[colType] {
		escaped := strings.ReplaceAll(raw, "'", "\\'")
		return fmt.Sprintf("%s '%s'", colType, escaped)
	}

	// BYTES uses B'value' prefix notation
	if colType == "BYTES" {
		escaped := strings.ReplaceAll(raw, "'", "\\'")
		return fmt.Sprintf("B'%s'", escaped)
	}

	// For other types (INT64, FLOAT64, BOOL, STRING, etc.), use CAST
	if colType == "STRING" {
		escaped := strings.ReplaceAll(raw, "'", "\\'")
		return fmt.Sprintf("CAST('%s' AS %s)", escaped, colType)
	}
	return fmt.Sprintf("CAST(%s AS %s)", raw, colType)
}
