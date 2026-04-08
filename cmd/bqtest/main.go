package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/matsuri-tech/bqtest/runner"
	"github.com/matsuri-tech/bqtest/testcase"
	"github.com/spf13/cobra"
)

const (
	exitSuccess    = 0
	exitTestFail   = 1
	exitConfigErr  = 2
	exitExecErr    = 3
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "bqtest",
		Short: "BigQuery SQL Test Runner",
		Long: `bqtest - BigQuery SQL Test Runner

Test BigQuery SQL by replacing table references with test fixtures,
executing on BigQuery, and comparing results with expected output.

Examples:
  bqtest run tests/test_1.yaml             # Run a single test
  bqtest run tests/*.yaml                  # Run all tests matching glob
  bqtest run --debug tests/test_1.yaml     # Show rewritten SQL and generated script
  bqtest run --project my-proj test.yaml   # Specify BigQuery project
  bqtest inspect tests/test_1.yaml         # Show test case details without running

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

Exit Codes:
  0  All tests passed
  1  One or more tests failed
  2  Configuration or parsing error
  3  BigQuery execution error`,
	}

	var projectID, location string
	var debug, keepScript bool

	runCmd := &cobra.Command{
		Use:   "run <testfile>...",
		Short: "Run test cases",
		Example: `  bqtest run tests/test_1.yaml
  bqtest run tests/*.yaml
  bqtest run --debug tests/test_1.yaml
  bqtest run --project my-project --location asia-northeast1 tests/*.yaml`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if projectID == "" {
				projectID = detectDefaultProject()
			}
			if projectID == "" {
				fmt.Fprintln(os.Stderr, "Error: BigQuery project ID is required.\n\nSpecify it with one of:\n  --project <project-id>\n  BQTEST_PROJECT=<project-id>\n  gcloud config set project <project-id>")
				os.Exit(exitConfigErr)
			}

			ctx := context.Background()
			hasFailure := false
			hasError := false

			for _, path := range args {
				tc, err := testcase.LoadFile(path)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error loading %s: %v\n", path, err)
					hasError = true
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

				if result.Err != nil {
					hasError = true
				} else if !result.Success {
					hasFailure = true
				}
			}

			if hasError {
				os.Exit(exitExecErr)
			}
			if hasFailure {
				os.Exit(exitTestFail)
			}
			return nil
		},
	}

	runCmd.Flags().StringVar(&projectID, "project", os.Getenv("BQTEST_PROJECT"), "BigQuery project ID")
	runCmd.Flags().StringVar(&location, "location", os.Getenv("BQTEST_LOCATION"), "BigQuery location")
	runCmd.Flags().BoolVar(&debug, "debug", false, "Show rewritten SQL and generated script")
	runCmd.Flags().BoolVar(&keepScript, "keep-script", false, "Save generated script to file")

	inspectCmd := &cobra.Command{
		Use:   "inspect <testfile>",
		Short: "Show parsed test case details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tc, err := testcase.LoadFile(args[0])
			if err != nil {
				return err
			}

			fmt.Printf("Test:        %s\n", tc.TestName)
			if tc.Description != "" {
				fmt.Printf("Description: %s\n", tc.Description)
			}
			if len(tc.Tags) > 0 {
				fmt.Printf("Tags:        %v\n", tc.Tags)
			}
			fmt.Printf("Fixtures:    %d\n", len(tc.Fixtures))
			for _, f := range tc.Fixtures {
				tempName := f.TempName
				if tempName == "" {
					parts := splitLast(f.Table, ".")
					tempName = parts
				}
				fmt.Printf("  %s -> %s (%d rows)\n", f.Table, tempName, len(f.Rows))
			}
			fmt.Printf("Expected:    %d rows\n", len(tc.Expected.Rows))
			if len(tc.Passthrough) > 0 {
				fmt.Printf("Passthrough: %v\n", tc.Passthrough)
			}
			return nil
		},
	}

	rootCmd.AddCommand(runCmd, inspectCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(exitConfigErr)
	}
}

// detectDefaultProject tries to find the default GCP project from environment or gcloud config.
func detectDefaultProject() string {
	// 1. Standard env vars
	for _, env := range []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT"} {
		if v := os.Getenv(env); v != "" {
			return v
		}
	}
	// 2. gcloud config
	out, err := exec.Command("gcloud", "config", "get-value", "project").Output()
	if err == nil {
		if p := strings.TrimSpace(string(out)); p != "" && p != "(unset)" {
			return p
		}
	}
	return ""
}

func splitLast(s, sep string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if string(s[i]) == sep {
			return s[i+1:]
		}
	}
	return s
}
