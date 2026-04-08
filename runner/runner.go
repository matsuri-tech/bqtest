package runner

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/matsuri-tech/bqtest/executor"
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
	TestName    string
	Success     bool
	ExtraCount  int64
	MissingCount int64
	ActualCount int64
	ExpectedCount int64
	Script      string
	RewrittenSQL string
	JobID       string
	Err         error
}

// Run executes a single test case and returns the result.
func Run(ctx context.Context, tc *testcase.TestCase, opts RunOptions) *RunResult {
	out := opts.Output
	if out == nil {
		out = os.Stdout
	}

	result := &RunResult{TestName: tc.TestName}

	// 1. Build rewrite map and passthrough set
	rewriteMap := tc.RewriteMap()
	passthrough := make(map[string]bool)
	for _, p := range tc.Passthrough {
		passthrough[p] = true
	}

	// 2. Rewrite SQL
	rewriteResult, err := rewriter.Rewrite(tc.SQL, rewriteMap, passthrough)
	if err != nil {
		result.Err = fmt.Errorf("rewrite error: %w", err)
		return result
	}

	// Check for unresolved tables (passthrough disabled by default)
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

	result.Success = execResult.Success
	result.ExtraCount = execResult.ExtraCount
	result.MissingCount = execResult.MissingCount
	result.ActualCount = execResult.ActualCount
	result.ExpectedCount = execResult.ExpectedCount
	result.JobID = execResult.JobID

	return result
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
		fmt.Fprintf(out, "  expected: %d rows\n", r.ExpectedCount)
		if r.ExtraCount > 0 {
			fmt.Fprintf(out, "  extra (in actual, not in expected): %d rows\n", r.ExtraCount)
		}
		if r.MissingCount > 0 {
			fmt.Fprintf(out, "  missing (in expected, not in actual): %d rows\n", r.MissingCount)
		}
	}

	if r.JobID != "" {
		fmt.Fprintf(out, "  job: %s\n", r.JobID)
	}
}
