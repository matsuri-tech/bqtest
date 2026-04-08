package executor

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// QueryResult holds the rows returned by executing a BigQuery script.
type QueryResult struct {
	Rows  []map[string]bigquery.Value
	JobID string
}

// Config holds BigQuery execution settings.
type Config struct {
	ProjectID string
	Location  string
}

// Execute runs the generated BigQuery script and returns the actual result rows.
func Execute(ctx context.Context, cfg Config, script string) (*QueryResult, error) {
	client, err := bigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	defer client.Close()

	client.Location = cfg.Location
	if client.Location == "" {
		client.Location = "US"
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

	result := &QueryResult{
		JobID: job.ID(),
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading results: %w", err)
	}

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading row: %w", err)
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}
