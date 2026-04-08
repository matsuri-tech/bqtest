package script

import (
	"fmt"
	"sort"
	"strings"

	"github.com/matsuri-tech/bqtest/testcase"
)

// Generate builds a BigQuery script that:
// 1. Creates TEMP TABLEs for fixtures
// 2. Creates a TEMP TABLE for expected results
// 3. Executes the rewritten SQL and stores the result as actual
// 4. Computes diff between actual and expected
func Generate(tc *testcase.TestCase, rewrittenSQL string) string {
	var sb strings.Builder

	// 1. Fixture TEMP TABLEs
	for _, f := range tc.Fixtures {
		tempName := f.TempName
		if tempName == "" {
			parts := strings.Split(f.Table, ".")
			tempName = parts[len(parts)-1]
		}
		sb.WriteString(generateFixtureSQL(tempName, f))
		sb.WriteString("\n\n")
	}

	// 2. Expected TEMP TABLE
	sb.WriteString(generateExpectedSQL(tc.Expected))
	sb.WriteString("\n\n")

	// 3. Actual TEMP TABLE from rewritten query
	fmt.Fprintf(&sb, "CREATE TEMP TABLE __bqtest_actual AS\n%s;\n\n", rewrittenSQL)

	// 4. Diff queries using expected column names for consistent ordering
	columns := expectedColumns(tc.Expected)
	sb.WriteString(generateDiffSQL(columns))

	return sb.String()
}

func generateFixtureSQL(tempName string, f testcase.Fixture) string {
	if f.SQL != "" {
		return fmt.Sprintf("CREATE TEMP TABLE %s AS\n%s;", tempName, f.SQL)
	}
	return fmt.Sprintf("CREATE TEMP TABLE %s AS\nSELECT * FROM UNNEST([%s]);", tempName, generateStructArray(f.Rows))
}

func generateExpectedSQL(expected testcase.Expected) string {
	if expected.SQL != "" {
		return fmt.Sprintf("CREATE TEMP TABLE __bqtest_expected AS\n%s;", expected.SQL)
	}
	return fmt.Sprintf("CREATE TEMP TABLE __bqtest_expected AS\nSELECT * FROM UNNEST([%s]);", generateStructArray(expected.Rows))
}

// generateStructArray builds a comma-separated list of STRUCT literals from rows.
func generateStructArray(rows []map[string]any) string {
	if len(rows) == 0 {
		return ""
	}
	// Use sorted keys from the first row for consistent column ordering
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

// expectedColumns extracts sorted column names from the expected definition.
func expectedColumns(expected testcase.Expected) []string {
	if len(expected.Rows) == 0 {
		return nil
	}
	keySet := make(map[string]bool)
	for _, row := range expected.Rows {
		for k := range row {
			keySet[k] = true
		}
	}
	cols := make([]string, 0, len(keySet))
	for k := range keySet {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

func sortedKeys(row map[string]any) []string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func generateDiffSQL(columns []string) string {
	if len(columns) == 0 {
		// Fallback: use * (less safe but works for SQL-defined expected)
		return `SELECT
  (SELECT COUNT(*) FROM (
    SELECT * FROM __bqtest_actual EXCEPT DISTINCT SELECT * FROM __bqtest_expected
  )) AS extra_count,
  (SELECT COUNT(*) FROM (
    SELECT * FROM __bqtest_expected EXCEPT DISTINCT SELECT * FROM __bqtest_actual
  )) AS missing_count,
  (SELECT COUNT(*) FROM __bqtest_actual) AS actual_count,
  (SELECT COUNT(*) FROM __bqtest_expected) AS expected_count;`
	}

	// Use explicit column list to ensure consistent ordering between actual and expected
	colList := strings.Join(columns, ", ")
	return fmt.Sprintf(`-- Result summary (using explicit column list for order-independent comparison)
SELECT
  (SELECT COUNT(*) FROM (
    SELECT %s FROM __bqtest_actual
    EXCEPT DISTINCT
    SELECT %s FROM __bqtest_expected
  )) AS extra_count,
  (SELECT COUNT(*) FROM (
    SELECT %s FROM __bqtest_expected
    EXCEPT DISTINCT
    SELECT %s FROM __bqtest_actual
  )) AS missing_count,
  (SELECT COUNT(*) FROM __bqtest_actual) AS actual_count,
  (SELECT COUNT(*) FROM __bqtest_expected) AS expected_count;`, colList, colList, colList, colList)
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
