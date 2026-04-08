package script

import (
	"fmt"
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
	sb.WriteString(fmt.Sprintf("CREATE TEMP TABLE __bqtest_actual AS\n%s;\n\n", rewrittenSQL))

	// 4. Diff queries
	sb.WriteString(generateDiffSQL())

	return sb.String()
}

func generateFixtureSQL(tempName string, f testcase.Fixture) string {
	if f.SQL != "" {
		return fmt.Sprintf("CREATE TEMP TABLE %s AS\n%s;", tempName, f.SQL)
	}

	if len(f.Rows) == 0 {
		return fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM UNNEST([]);", tempName)
	}

	// Build UNION ALL of SELECT statements for each row
	var selects []string
	for _, row := range f.Rows {
		var cols []string
		for k, v := range row {
			cols = append(cols, fmt.Sprintf("%s AS %s", formatValue(v), k))
		}
		selects = append(selects, fmt.Sprintf("SELECT %s", strings.Join(cols, ", ")))
	}

	return fmt.Sprintf("CREATE TEMP TABLE %s AS\n%s;", tempName, strings.Join(selects, "\nUNION ALL\n"))
}

func generateExpectedSQL(expected testcase.Expected) string {
	if expected.SQL != "" {
		return fmt.Sprintf("CREATE TEMP TABLE __bqtest_expected AS\n%s;", expected.SQL)
	}

	if len(expected.Rows) == 0 {
		return "CREATE TEMP TABLE __bqtest_expected AS SELECT 1 WHERE FALSE;"
	}

	var selects []string
	for _, row := range expected.Rows {
		var cols []string
		for k, v := range row {
			cols = append(cols, fmt.Sprintf("%s AS %s", formatValue(v), k))
		}
		selects = append(selects, fmt.Sprintf("SELECT %s", strings.Join(cols, ", ")))
	}

	return fmt.Sprintf("CREATE TEMP TABLE __bqtest_expected AS\n%s;", strings.Join(selects, "\nUNION ALL\n"))
}

func generateDiffSQL() string {
	return `-- Rows in actual but not in expected
CREATE TEMP TABLE __bqtest_extra AS
SELECT * FROM __bqtest_actual
EXCEPT DISTINCT
SELECT * FROM __bqtest_expected;

-- Rows in expected but not in actual
CREATE TEMP TABLE __bqtest_missing AS
SELECT * FROM __bqtest_expected
EXCEPT DISTINCT
SELECT * FROM __bqtest_actual;

-- Result summary
SELECT
  (SELECT COUNT(*) FROM __bqtest_extra) AS extra_count,
  (SELECT COUNT(*) FROM __bqtest_missing) AS missing_count,
  (SELECT COUNT(*) FROM __bqtest_actual) AS actual_count,
  (SELECT COUNT(*) FROM __bqtest_expected) AS expected_count;`
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
		// YAML parses numbers as float64 by default
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	default:
		return fmt.Sprintf("'%v'", val)
	}
}
