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
		return fmt.Sprintf("CREATE TEMP TABLE %s AS\n%s;", tempName, f.SQL)
	}
	return fmt.Sprintf("CREATE TEMP TABLE %s AS\nSELECT * FROM UNNEST([%s]);", tempName, generateStructArray(f.Rows))
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

func formatValue(v any) string {
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
