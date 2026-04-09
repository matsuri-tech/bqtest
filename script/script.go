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
	return fmt.Sprintf("CREATE TEMP TABLE `%s` AS\nSELECT * FROM UNNEST([%s]);", tempName, generateStructArray(f.Rows))
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
func generateStructArray(rows []map[string]any) string {
	if len(rows) == 0 {
		return ""
	}
	keys := sortedKeys(rows[0])

	var structs []string
	for _, row := range rows {
		var fields []string
		for _, k := range keys {
			fields = append(fields, fmt.Sprintf("%s AS %s", formatValue(row[k]), k))
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

// typedLiteralPrefixes are BigQuery types that use the TYPE 'value' literal syntax.
var typedLiteralPrefixes = map[string]bool{
	"DATE":       true,
	"TIMESTAMP":  true,
	"DATETIME":   true,
	"TIME":       true,
	"NUMERIC":    true,
	"BIGNUMERIC": true,
	"INTERVAL":   true,
	"JSON":       true,
	"BYTES":      true,
}

// typedCastPrefixes are BigQuery types that use the CAST(value AS TYPE) syntax.
var typedCastPrefixes = map[string]bool{
	"INT64":   true,
	"FLOAT64": true,
	"BOOL":    true,
	"STRING":  true,
}

func formatValue(v any) string {
	switch val := v.(type) {
	case nil:
		return "NULL"
	case string:
		if idx := strings.Index(val, ":"); idx > 0 {
			prefix := val[:idx]
			value := val[idx+1:]
			if typedLiteralPrefixes[prefix] {
				escaped := strings.ReplaceAll(value, "'", "\\'")
				return fmt.Sprintf("%s '%s'", prefix, escaped)
			}
			if typedCastPrefixes[prefix] {
				if prefix == "STRING" {
					escaped := strings.ReplaceAll(value, "'", "\\'")
					return fmt.Sprintf("CAST('%s' AS %s)", escaped, prefix)
				}
				return fmt.Sprintf("CAST(%s AS %s)", value, prefix)
			}
		}
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
