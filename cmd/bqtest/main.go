package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/matsuri-tech/bqtest/rewriter"
	"github.com/matsuri-tech/bqtest/runner"
	"github.com/matsuri-tech/bqtest/script"
	"github.com/matsuri-tech/bqtest/testcase"
	"github.com/spf13/cobra"
)

const exitFail = 1

var (
	projectID  string
	location   string
	debug      bool
	keepScript bool
	dryRun     bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "bqtest [flags] <testfile>...",
		Short: "BigQuery SQL Test Runner",
		Long:                  "Test BigQuery SQL by replacing table references with test fixtures,\nexecuting on BigQuery, and comparing results with expected output.",
		Args:                  cobra.ArbitraryArgs,
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			return executeRun(args)
		},
	}

	rootCmd.SetHelpTemplate(`bqtest - BigQuery SQL Test Runner

{{.Long}}

Usage:
  bqtest [flags] <testfile>...

Available Commands:
  run         Run test cases (same as 'bqtest <testfile>...')

Options:
  --project <id>    BigQuery project ID (default: BQTEST_PROJECT env or gcloud config)
  --location <loc>  BigQuery location (default: BQTEST_LOCATION env)
  --dry-run         Parse and show test details without executing on BigQuery
  --debug           Show rewritten SQL and generated BigQuery script
  --keep-script     Save generated script to <test_name>.bqtest.sql
  -h, --help        Show this help

Examples:
  bqtest tests/test_1.yaml                 # Run a single test
  bqtest tests/*.yaml                      # Run all tests matching glob
  bqtest --dry-run tests/test_1.yaml       # Validate without executing
  bqtest --debug tests/test_1.yaml         # Show rewritten SQL and generated script
  bqtest --project my-proj tests/*.yaml    # Specify BigQuery project
  bqtest run tests/*.yaml                  # Explicit 'run' subcommand (same behavior)

YAML Test Format:
  test_name: user_total_amount
  description: Verify SUM aggregation by user_id
  sql_file: queries/total_amount.sql       # Path to SQL file (relative to YAML)
  fixtures:
    - table: myproj.dataset.orders         # Fully-qualified table to replace
      rows:
        - {order_id: 1, user_id: 10, amount: 100}
        - {order_id: 2, user_id: 10, amount: 200}
        - {order_id: 3, user_id: 20, amount: 50}
  expected:
    rows:
      - {user_id: 10, total_amount: 300}
      - {user_id: 20, total_amount: 50}

  # Or inline SQL instead of sql_file:
  # sql: "SELECT user_id, SUM(amount) AS total_amount FROM ` + "`myproj.dataset.orders`" + ` GROUP BY user_id"

  # For complex types (STRUCT, ARRAY), use SQL fixtures:
  # fixtures:
  #   - table: myproj.dataset.events
  #     sql: "SELECT 1 AS id, STRUCT('a' AS key, 1 AS val) AS metadata"
`)

	runCmd := &cobra.Command{
		Use:    "run [flags] <testfile>...",
		Short:  "Run test cases (same as 'bqtest <testfile>...')",
		Hidden: false,
		Args:   cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return executeRun(args)
		},
	}

	// Register flags on both root and run commands
	for _, cmd := range []*cobra.Command{rootCmd, runCmd} {
		cmd.Flags().StringVar(&projectID, "project", os.Getenv("BQTEST_PROJECT"), "BigQuery project ID")
		cmd.Flags().StringVar(&location, "location", os.Getenv("BQTEST_LOCATION"), "BigQuery location")
		cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Parse and show test details without executing")
		cmd.Flags().BoolVar(&debug, "debug", false, "Show rewritten SQL and generated script")
		cmd.Flags().BoolVar(&keepScript, "keep-script", false, "Save generated script to file")
	}

	rootCmd.AddCommand(runCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(exitFail)
	}
}

func executeRun(args []string) error {
	if dryRun {
		return executeDryRun(args)
	}

	if projectID == "" {
		projectID = detectDefaultProject()
	}
	if projectID == "" {
		fmt.Fprintln(os.Stderr, "Error: BigQuery project ID is required.\n\nSpecify it with one of:\n  --project <project-id>\n  BQTEST_PROJECT=<project-id>\n  gcloud config set project <project-id>")
		os.Exit(exitFail)
	}

	ctx := context.Background()
	failed := false

	for _, path := range args {
		tc, err := testcase.LoadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading %s: %v\n", path, err)
			failed = true
			continue
		}

		result := runner.Run(ctx, tc, runner.RunOptions{
			ProjectID:  projectID,
			Location:   location,
			Debug:      debug,
			KeepScript: keepScript,
			Output:     os.Stdout,
		})

		runner.Report(os.Stdout, result)

		if result.Err != nil || !result.Success {
			failed = true
		}
	}

	if failed {
		os.Exit(exitFail)
	}
	return nil
}

func executeDryRun(args []string) error {
	hasError := false
	for _, path := range args {
		tc, err := testcase.LoadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading %s: %v\n", path, err)
			hasError = true
			continue
		}

		fmt.Printf("Test:        %s\n", tc.TestName)
		if tc.Description != "" {
			fmt.Printf("Description: %s\n", tc.Description)
		}
		if len(tc.Tags) > 0 {
			fmt.Printf("Tags:        %v\n", tc.Tags)
		}
		fmt.Printf("Fixtures:    %d\n", len(tc.Fixtures))
		rewriteMap := tc.RewriteMap()
		for table, tempName := range rewriteMap {
			var rowCount int
			for _, f := range tc.Fixtures {
				if f.Table == table {
					rowCount = len(f.Rows)
					break
				}
			}
			fmt.Printf("  %s -> %s (%d rows)\n", table, tempName, rowCount)
		}
		fmt.Printf("Expected:    %d rows\n", len(tc.Expected.Rows))
		if len(tc.Passthrough) > 0 {
			fmt.Printf("Passthrough: %v\n", tc.Passthrough)
		}

		passthrough := make(map[string]bool)
		for _, p := range tc.Passthrough {
			passthrough[p] = true
		}
		rewriteResult, err := rewriter.Rewrite(tc.SQL, rewriteMap, passthrough)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Rewrite error: %v\n", err)
			hasError = true
			continue
		}
		if len(rewriteResult.UnresolvedTables) > 0 {
			fmt.Printf("  Unresolved: %v\n", rewriteResult.UnresolvedTables)
		}

		fmt.Printf("\n=== Rewritten SQL ===\n%s\n", rewriteResult.SQL)
		fmt.Printf("\n=== Generated Script ===\n%s\n", script.Generate(tc, rewriteResult.SQL))
		fmt.Println()
	}

	if hasError {
		os.Exit(exitFail)
	}
	return nil
}

func detectDefaultProject() string {
	for _, env := range []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT"} {
		if v := os.Getenv(env); v != "" {
			return v
		}
	}
	out, err := exec.Command("gcloud", "config", "get-value", "project").Output()
	if err == nil {
		if p := strings.TrimSpace(string(out)); p != "" && p != "(unset)" {
			return p
		}
	}
	return ""
}
