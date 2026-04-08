# bqtest

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev)

BigQuery SQL test runner. Replaces table references in SQL with test fixtures, executes on BigQuery, and compares results against expected output.

**[日本語ドキュメント (Japanese)](./README.ja.md)**

## Features

- **AST-based table reference analysis** - Accurate BigQuery SQL parsing via go-zetasql (Google ZetaSQL)
- **Declarative test definitions** - Define target SQL, fixtures, and expected output in YAML
- **Runs on real BigQuery** - Not an emulator; tests execute on the actual BigQuery engine
- **Clear diff output** - Shows want vs got with row numbers on failure
- **CI ready** - Exit 0 on success, exit 1 on failure

## Installation

### Download binary

```bash
mkdir -p ~/.local/bin

# macOS (Apple Silicon)
curl -sL -o ~/.local/bin/bqtest https://github.com/matsuri-tech/bqtest/releases/latest/download/bqtest-darwin-arm64
chmod +x ~/.local/bin/bqtest

# Linux (amd64)
curl -sL -o ~/.local/bin/bqtest https://github.com/matsuri-tech/bqtest/releases/latest/download/bqtest-linux-amd64
chmod +x ~/.local/bin/bqtest
```

Make sure `~/.local/bin` is in your `PATH`.

### From source

```bash
go install github.com/matsuri-tech/bqtest/cmd/bqtest@latest
```

## Quick Start

### 1. Prepare the target SQL

```sql
-- queries/total_amount.sql
SELECT
  user_id,
  SUM(amount) AS total_amount
FROM `myproj.dataset.orders`
GROUP BY user_id
```

### 2. Define a test case in YAML

```yaml
# tests/total_amount_test.yaml
test_name: total_amount
sql_file: ../queries/total_amount.sql
fixtures:
  - table: myproj.dataset.orders
    rows:
      - {order_id: 1, user_id: 10, amount: 100}
      - {order_id: 2, user_id: 10, amount: 200}
      - {order_id: 3, user_id: 20, amount: 50}
expected:
  rows:
    - {user_id: 10, total_amount: 300}
    - {user_id: 20, total_amount: 50}
```

### 3. Run the test

```bash
bqtest tests/total_amount_test.yaml
```

```
PASS  total_amount (2 rows)
  job: abc123def456
```

## Usage

```bash
bqtest <testfile>...                     # Run tests
bqtest tests/*.yaml                      # Run multiple tests with glob
bqtest --dry-run tests/test.yaml         # Parse and show details without executing
bqtest --debug tests/test.yaml           # Show rewritten SQL and generated script
bqtest --project my-proj tests/*.yaml    # Specify BigQuery project
```

### Options

| Option | Description |
|---|---|
| `--project <id>` | BigQuery project ID (default: `BQTEST_PROJECT` env or `gcloud config`) |
| `--location <loc>` | BigQuery location (default: `BQTEST_LOCATION` env) |
| `--dry-run` | Parse and show test details without executing on BigQuery |
| `--debug` | Show rewritten SQL and generated BigQuery script |
| `--keep-script` | Save generated script to `<test_name>.bqtest.sql` |

## YAML Test Case Format

```yaml
test_name: my_test                       # Required: test name
description: Test description            # Optional
tags: [regression, billing]              # Optional

# Target SQL (specify one)
sql_file: path/to/query.sql             # SQL file path (relative to YAML file)
sql: "SELECT * FROM `proj.ds.table`"    # Or inline SQL

# Fixtures: test data to replace table references
fixtures:
  - table: myproj.dataset.orders         # Fully-qualified table name to replace
    rows:                                # Test data
      - {order_id: 1, user_id: 10, amount: 100}
      - {order_id: 2, user_id: 10, amount: 200}

  # Use SQL fixtures for complex types (STRUCT, ARRAY)
  - table: myproj.dataset.events
    sql: "SELECT 1 AS id, STRUCT('a' AS key, 1 AS val) AS metadata"

# Expected output
expected:
  rows:
    - {user_id: 10, total_amount: 300}

# Optional: tables to access directly without fixtures
passthrough:
  - myproj.dataset.master_data
```

## Failure Output

```
FAIL  my_test
  actual:   2 rows
  expected: 2 rows
  extra:    1 rows (in actual, not in expected)
  missing:  1 rows (in expected, not in actual)

  total_amount  user_id
- 999           10       (expected row 1)
+ 300           10       (actual row 1)
  job: abc123def456
```

- `-` : Row expected but not found in actual results
- `+` : Row found in actual results but not expected

## How It Works

1. Parse SQL as BigQuery dialect and extract table references from AST
2. Rewrite fixture-defined tables to TEMP TABLE names
3. Generate BigQuery script (`CREATE TEMP TABLE` + rewritten query)
4. Execute via BigQuery API and retrieve result rows
5. Compute and display diff between actual and expected on the Go side

## Prerequisites

- GCP credentials with BigQuery execution permissions
- `gcloud auth application-default login` or a service account
- Fully-qualified table names in target SQL

## License

MIT
