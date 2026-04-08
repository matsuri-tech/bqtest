package diff

import (
	"fmt"
	"sort"
	"strings"
)

// Row is a map of column name to value.
type Row map[string]any

// Result holds the diff between actual and expected rows.
type Result struct {
	Match   bool
	Extra   []Row // in actual but not in expected
	Missing []Row // in expected but not in actual
	Columns []string
}

// Compare performs an unordered set comparison between actual and expected rows.
// Values are compared by their string representation for simplicity.
func Compare(actual []Row, expected []Row) *Result {
	columns := collectColumns(actual, expected)

	// Build multiset of row keys
	actualKeys := make(map[string]int)
	actualByKey := make(map[string][]Row)
	for _, row := range actual {
		key := rowKey(row, columns)
		actualKeys[key]++
		actualByKey[key] = append(actualByKey[key], row)
	}

	expectedKeys := make(map[string]int)
	expectedByKey := make(map[string][]Row)
	for _, row := range expected {
		key := rowKey(row, columns)
		expectedKeys[key]++
		expectedByKey[key] = append(expectedByKey[key], row)
	}

	var extra, missing []Row

	// Find rows in actual but not in expected (or more copies than expected)
	for key, aCount := range actualKeys {
		eCount := expectedKeys[key]
		for i := 0; i < aCount-eCount; i++ {
			extra = append(extra, actualByKey[key][eCount+i])
		}
	}

	// Find rows in expected but not in actual (or more copies than expected)
	for key, eCount := range expectedKeys {
		aCount := actualKeys[key]
		for i := 0; i < eCount-aCount; i++ {
			missing = append(missing, expectedByKey[key][aCount+i])
		}
	}

	return &Result{
		Match:   len(extra) == 0 && len(missing) == 0,
		Extra:   extra,
		Missing: missing,
		Columns: columns,
	}
}

// Format returns a human-readable diff string.
func (r *Result) Format() string {
	if r.Match {
		return ""
	}

	var sb strings.Builder

	if len(r.Missing) > 0 || len(r.Extra) > 0 {
		// Calculate column widths
		widths := make(map[string]int)
		for _, col := range r.Columns {
			widths[col] = len(col)
		}
		allRows := append(r.Missing, r.Extra...)
		for _, row := range allRows {
			for _, col := range r.Columns {
				w := len(formatVal(row[col]))
				if w > widths[col] {
					widths[col] = w
				}
			}
		}

		// Header
		sb.WriteString("  ")
		sb.WriteString(formatTableRow(r.Columns, widths, "  "))
		sb.WriteString("\n")

		// Missing rows (expected but not in actual)
		for _, row := range r.Missing {
			sb.WriteString("- ")
			sb.WriteString(formatRowValues(row, r.Columns, widths))
			sb.WriteString("\n")
		}

		// Extra rows (in actual but not in expected)
		for _, row := range r.Extra {
			sb.WriteString("+ ")
			sb.WriteString(formatRowValues(row, r.Columns, widths))
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

func formatTableRow(columns []string, widths map[string]int, prefix string) string {
	var parts []string
	for _, col := range columns {
		parts = append(parts, fmt.Sprintf("%-*s", widths[col], col))
	}
	return strings.Join(parts, "  ")
}

func formatRowValues(row Row, columns []string, widths map[string]int) string {
	var parts []string
	for _, col := range columns {
		val := formatVal(row[col])
		parts = append(parts, fmt.Sprintf("%-*s", widths[col], val))
	}
	return strings.Join(parts, "  ")
}

func rowKey(row Row, columns []string) string {
	var parts []string
	for _, col := range columns {
		parts = append(parts, fmt.Sprintf("%s=%s", col, formatVal(row[col])))
	}
	return strings.Join(parts, "|")
}

func formatVal(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

func collectColumns(actual []Row, expected []Row) []string {
	colSet := make(map[string]bool)
	for _, row := range actual {
		for k := range row {
			colSet[k] = true
		}
	}
	for _, row := range expected {
		for k := range row {
			colSet[k] = true
		}
	}
	cols := make([]string, 0, len(colSet))
	for k := range colSet {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}
