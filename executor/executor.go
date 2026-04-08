package executor

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// TestResult holds the outcome of a test execution.
type TestResult struct {
	Success       bool
	ExtraCount    int64
	MissingCount  int64
	ActualCount   int64
	ExpectedCount int64
	JobID         string
}

// Config holds BigQuery execution settings.
type Config struct {
	ProjectID string
	Location  string
}

// Execute runs the generated BigQuery script and returns the test result.
func Execute(ctx context.Context, cfg Config, script string) (*TestResult, error) {
	client, err := bigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	defer client.Close()

	if cfg.Location != "" {
		client.Location = cfg.Location
	}

	q := client.Query(script)
	q.UseLegacySQL = false

	job, err := q.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("waiting for job: %w", err)
	}
	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("job failed: %w", err)
	}

	result := &TestResult{
		JobID: job.ID(),
	}

	// The last SELECT in the script returns the summary
	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading results: %w", err)
	}

	var summary map[string]bigquery.Value
	for {
		err := it.Next(&summary)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading summary row: %w", err)
		}
	}

	if summary == nil {
		return nil, fmt.Errorf("no summary result returned from script")
	}

	result.ExtraCount = toInt64(summary["extra_count"])
	result.MissingCount = toInt64(summary["missing_count"])
	result.ActualCount = toInt64(summary["actual_count"])
	result.ExpectedCount = toInt64(summary["expected_count"])
	result.Success = result.ExtraCount == 0 && result.MissingCount == 0

	return result, nil
}

func toInt64(v bigquery.Value) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case float64:
		return int64(val)
	default:
		return 0
	}
}
