package diff

import (
	"fmt"
	"sort"
	"strings"
)

// Row is a map of column name to value.
type Row map[string]any

// IndexedRow is a row with its original index in the expected/actual list.
type IndexedRow struct {
	Row   Row
	Index int // 1-based line number
}

// Result holds the diff between actual and expected rows.
type Result struct {
	Match   bool
	Extra   []IndexedRow // in actual but not in expected
	Missing []IndexedRow // in expected but not in actual
	Columns []string
}

// Compare performs an unordered set comparison between actual and expected rows.
func Compare(actual []Row, expected []Row) *Result {
	columns := collectColumns(actual, expected)

	type indexedEntry struct {
		rows    []IndexedRow
		count   int
	}

	actualByKey := make(map[string]*indexedEntry)
	for i, row := range actual {
		key := rowKey(row, columns)
		e := actualByKey[key]
		if e == nil {
			e = &indexedEntry{}
			actualByKey[key] = e
		}
		e.rows = append(e.rows, IndexedRow{Row: row, Index: i + 1})
		e.count++
	}

	expectedByKey := make(map[string]*indexedEntry)
	for i, row := range expected {
		key := rowKey(row, columns)
		e := expectedByKey[key]
		if e == nil {
			e = &indexedEntry{}
			expectedByKey[key] = e
		}
		e.rows = append(e.rows, IndexedRow{Row: row, Index: i + 1})
		e.count++
	}

	var extra, missing []IndexedRow

	for key, ae := range actualByKey {
		eCount := 0
		if ee, ok := expectedByKey[key]; ok {
			eCount = ee.count
		}
		for i := 0; i < ae.count-eCount; i++ {
			extra = append(extra, ae.rows[eCount+i])
		}
	}

	for key, ee := range expectedByKey {
		aCount := 0
		if ae, ok := actualByKey[key]; ok {
			aCount = ae.count
		}
		for i := 0; i < ee.count-aCount; i++ {
			missing = append(missing, ee.rows[aCount+i])
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
		for _, ir := range r.Missing {
			updateWidths(widths, ir.Row, r.Columns)
		}
		for _, ir := range r.Extra {
			updateWidths(widths, ir.Row, r.Columns)
		}

		// Header
		sb.WriteString("  ")
		sb.WriteString(formatTableRow(r.Columns, widths))
		sb.WriteString("\n")

		// Missing rows (expected but not in actual)
		for _, ir := range r.Missing {
			fmt.Fprintf(&sb, "- %s  (expected row %d)\n", formatRowValues(ir.Row, r.Columns, widths), ir.Index)
		}

		// Extra rows (in actual but not in expected)
		for _, ir := range r.Extra {
			fmt.Fprintf(&sb, "+ %s  (actual row %d)\n", formatRowValues(ir.Row, r.Columns, widths), ir.Index)
		}
	}

	return sb.String()
}

func updateWidths(widths map[string]int, row Row, columns []string) {
	for _, col := range columns {
		w := len(formatVal(row[col]))
		if w > widths[col] {
			widths[col] = w
		}
	}
}

func formatTableRow(columns []string, widths map[string]int) string {
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
