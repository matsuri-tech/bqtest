package testcase

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// TestCase represents a single bqtest test case loaded from YAML.
type TestCase struct {
	TestName    string    `yaml:"test_name"`
	Description string    `yaml:"description,omitempty"`
	Tags        []string  `yaml:"tags,omitempty"`
	SQL         string    `yaml:"sql,omitempty"`
	SQLFile     string    `yaml:"sql_file,omitempty"`
	Fixtures    []Fixture `yaml:"fixtures"`
	Expected    Expected  `yaml:"expected"`
	Passthrough []string  `yaml:"passthrough,omitempty"`
}

// Fixture defines test input data for a source table.
type Fixture struct {
	// Table is the fully-qualified BigQuery table name to replace.
	Table string `yaml:"table"`
	// TempName is the TEMP TABLE name to use. Defaults to the table's short name.
	TempName string `yaml:"temp_name,omitempty"`
	// Rows is the fixture data as a list of maps.
	Rows []map[string]any `yaml:"rows"`
	// SQL is an alternative fixture definition as raw SQL (for complex types).
	SQL string `yaml:"sql,omitempty"`
}

// Expected defines the expected output of a test.
type Expected struct {
	Rows []map[string]any `yaml:"rows,omitempty"`
	SQL  string           `yaml:"sql,omitempty"`
}

// LoadFile loads a test case from a YAML file.
func LoadFile(path string) (*TestCase, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading test file: %w", err)
	}
	tc, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	// Resolve sql_file relative to the YAML file's directory
	if tc.SQLFile != "" && tc.SQL == "" {
		dir := filepath.Dir(path)
		sqlPath := filepath.Join(dir, tc.SQLFile)
		sqlData, err := os.ReadFile(sqlPath)
		if err != nil {
			return nil, fmt.Errorf("reading sql_file %s: %w", sqlPath, err)
		}
		tc.SQL = string(sqlData)
	}

	return tc, nil
}

// Parse parses a YAML byte slice into a TestCase.
func Parse(data []byte) (*TestCase, error) {
	var tc TestCase
	if err := yaml.Unmarshal(data, &tc); err != nil {
		return nil, fmt.Errorf("yaml parse error: %w", err)
	}
	if err := tc.Validate(); err != nil {
		return nil, err
	}
	return &tc, nil
}

// Validate checks that the test case has required fields.
func (tc *TestCase) Validate() error {
	if tc.TestName == "" {
		return fmt.Errorf("test_name is required")
	}
	if tc.SQL == "" && tc.SQLFile == "" {
		return fmt.Errorf("either sql or sql_file is required")
	}
	if len(tc.Expected.Rows) == 0 && tc.Expected.SQL == "" {
		return fmt.Errorf("expected rows or sql is required")
	}
	for i, f := range tc.Fixtures {
		if f.Table == "" {
			return fmt.Errorf("fixture[%d]: table is required", i)
		}
		if len(f.Rows) == 0 && f.SQL == "" {
			return fmt.Errorf("fixture[%d] (%s): rows or sql is required", i, f.Table)
		}
	}
	return nil
}

// RewriteMap returns a mapping of fully-qualified table name to temp table name.
func (tc *TestCase) RewriteMap() map[string]string {
	m := make(map[string]string)
	for _, f := range tc.Fixtures {
		tempName := f.TempName
		if tempName == "" {
			// Use the last part of the fully-qualified name
			parts := splitTablePath(f.Table)
			tempName = parts[len(parts)-1]
		}
		m[f.Table] = tempName
	}
	return m
}

func splitTablePath(path string) []string {
	// Simple split by dot
	parts := []string{}
	current := ""
	for _, c := range path {
		if c == '.' {
			if current != "" {
				parts = append(parts, current)
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}
