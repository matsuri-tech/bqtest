package runner

import (
	"context"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/bigquery"

	"github.com/matsuri-tech/bqtest/diff"
	"github.com/matsuri-tech/bqtest/executor"
	"github.com/matsuri-tech/bqtest/parser"
	"github.com/matsuri-tech/bqtest/rewriter"
	"github.com/matsuri-tech/bqtest/script"
	"github.com/matsuri-tech/bqtest/testcase"
)

// RunOptions configures a test run.
type RunOptions struct {
	ProjectID  string
	Location   string
	Debug      bool
	KeepScript bool
	Output     io.Writer
}

// RunResult is the outcome of running a single test case.
type RunResult struct {
	TestName     string
	Success      bool
	Diff         *diff.Result
	ActualCount  int
	Script       string
	RewrittenSQL string
	JobID        string
	Err          error
}

// Run executes a single test case and returns the result.
func Run(ctx context.Context, tc *testcase.TestCase, opts RunOptions) *RunResult {
	out := opts.Output
	if out == nil {
		out = os.Stdout
	}

	result := &RunResult{TestName: tc.TestName}

	// 1. Strip DDL wrapper (CREATE TABLE AS -> SELECT)
	sql, kind, err := parser.StripDDL(tc.SQL)
	if err != nil {
		result.Err = fmt.Errorf("unsupported SQL: %w", err)
		return result
	}
	if kind == parser.SQLKindCreateTableAS {
		fmt.Fprintf(out, "  note: stripped CREATE TABLE AS wrapper, testing inner SELECT\n")
	}

	// 2. Build rewrite map and passthrough set
	rewriteMap := tc.RewriteMap()
	passthrough := make(map[string]bool)
	for _, p := range tc.Passthrough {
		passthrough[p] = true
	}

	// 3. Rewrite SQL
	rewriteResult, err := rewriter.Rewrite(sql, rewriteMap, passthrough)
	if err != nil {
		result.Err = fmt.Errorf("rewrite error: %w", err)
		return result
	}

	if len(rewriteResult.UnresolvedTables) > 0 && len(passthrough) == 0 {
		result.Err = fmt.Errorf("source tables without fixtures: %v (add to fixtures or passthrough)", rewriteResult.UnresolvedTables)
		return result
	}

	result.RewrittenSQL = rewriteResult.SQL

	// 3. Generate BigQuery script
	bqScript := script.Generate(tc, rewriteResult.SQL)
	result.Script = bqScript

	if opts.Debug {
		fmt.Fprintf(out, "=== Rewritten SQL ===\n%s\n\n", rewriteResult.SQL)
		fmt.Fprintf(out, "=== Generated Script ===\n%s\n\n", bqScript)
	}

	if opts.KeepScript {
		scriptFile := tc.TestName + ".bqtest.sql"
		if err := os.WriteFile(scriptFile, []byte(bqScript), 0644); err != nil {
			fmt.Fprintf(out, "warning: could not write script file: %v\n", err)
		} else {
			fmt.Fprintf(out, "Script saved to %s\n", scriptFile)
		}
	}

	// 4. Execute on BigQuery
	cfg := executor.Config{
		ProjectID: opts.ProjectID,
		Location:  opts.Location,
	}
	execResult, err := executor.Execute(ctx, cfg, bqScript)
	if err != nil {
		result.Err = fmt.Errorf("execution error: %w", err)
		return result
	}
	result.JobID = execResult.JobID

	// 5. Convert BQ rows to diff.Row
	actualRows := convertBQRows(execResult.Rows)
	result.ActualCount = len(actualRows)

	// 6. Compare with expected
	expectedRows := convertExpectedRows(tc.Expected.Rows)
	diffResult := diff.Compare(actualRows, expectedRows)
	result.Diff = diffResult
	result.Success = diffResult.Match

	return result
}

// convertBQRows converts BigQuery result rows to diff.Row.
func convertBQRows(rows []map[string]bigquery.Value) []diff.Row {
	result := make([]diff.Row, len(rows))
	for i, row := range rows {
		r := make(diff.Row)
		for k, v := range row {
			r[k] = v
		}
		result[i] = r
	}
	return result
}

// convertExpectedRows converts YAML expected rows to diff.Row.
func convertExpectedRows(rows []map[string]any) []diff.Row {
	result := make([]diff.Row, 0, len(rows))
	for _, row := range rows {
		r := make(diff.Row)
		for k, v := range row {
			r[k] = normalizeValue(v)
		}
		result = append(result, r)
	}
	return result
}

// normalizeValue converts YAML values to types comparable with BigQuery results.
// YAML parses numbers as int (small) or float64, BigQuery returns int64.
func normalizeValue(v any) any {
	switch val := v.(type) {
	case int:
		return int64(val)
	case float64:
		if val == float64(int64(val)) {
			return int64(val)
		}
		return val
	default:
		return v
	}
}

// Report prints the test result.
func Report(out io.Writer, r *RunResult) {
	if r.Err != nil {
		fmt.Fprintf(out, "FAIL  %s\n", r.TestName)
		fmt.Fprintf(out, "  Error: %v\n", r.Err)
		return
	}

	if r.Success {
		fmt.Fprintf(out, "PASS  %s (%d rows)\n", r.TestName, r.ActualCount)
	} else {
		fmt.Fprintf(out, "FAIL  %s\n", r.TestName)
		fmt.Fprintf(out, "  actual:   %d rows\n", r.ActualCount)
		fmt.Fprintf(out, "  expected: %d rows\n", len(r.Diff.Missing)+r.ActualCount-len(r.Diff.Extra))
		if len(r.Diff.Extra) > 0 {
			fmt.Fprintf(out, "  extra:    %d rows (in actual, not in expected)\n", len(r.Diff.Extra))
		}
		if len(r.Diff.Missing) > 0 {
			fmt.Fprintf(out, "  missing:  %d rows (in expected, not in actual)\n", len(r.Diff.Missing))
		}
		fmt.Fprintf(out, "\n")
		fmt.Fprintf(out, "%s", r.Diff.Format())
	}

	if r.JobID != "" {
		fmt.Fprintf(out, "  job: %s\n", r.JobID)
	}
}
