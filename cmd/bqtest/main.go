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
		Long:  "Test BigQuery SQL by replacing table references with fixtures and comparing results.",
	}

	var projectID, location string
	var debug, keepScript bool

	runCmd := &cobra.Command{
		Use:   "run <testfile>...",
		Short: "Run test cases",
		Args:  cobra.MinimumNArgs(1),
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
