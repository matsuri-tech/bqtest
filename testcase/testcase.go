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
	// Columns defines the schema (column name → BigQuery type) for the table.
	// Required only when defining an empty table via rows (rows is empty/nil and SQL is not set).
	Columns map[string]string `yaml:"columns,omitempty"`
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
		if len(f.Rows) == 0 && f.SQL == "" && len(f.Columns) == 0 {
			return fmt.Errorf("fixture[%d] (%s): rows, sql, or columns is required", i, f.Table)
		}
		for colName, colType := range f.Columns {
			if colName == "" {
				return fmt.Errorf("fixture[%d] (%s): column name must not be empty", i, f.Table)
			}
			if colType == "" {
				return fmt.Errorf("fixture[%d] (%s): column %q has an empty type", i, f.Table, colName)
			}
		}
	}
	return nil
}

// normalizeTablePath returns a canonical dataset.table form for comparison.
// For 3-part names (project.dataset.table), the project is stripped so that
// a 2-part name "dataset.table" matches "project.dataset.table".
func normalizeTablePath(table string) string {
	parts := splitTablePath(table)
	if len(parts) >= 2 {
		// Use only the last two segments (dataset.table) for dedup.
		return parts[len(parts)-2] + "." + parts[len(parts)-1]
	}
	return joinParts(parts)
}

func joinParts(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += "."
		}
		result += p
	}
	return result
}

// RewriteMap returns a mapping of fully-qualified table name to temp table name.
// When multiple fixtures share the same short table name (last segment) but
// come from different datasets, the method automatically disambiguates by
// prefixing with the dataset name (e.g. "dataset__table"). User-specified
// temp_name values always take priority and are never modified.
func (tc *TestCase) RewriteMap() map[string]string {
	// First pass: deduplicate fixtures that refer to the same table
	// (e.g. 2-part vs 3-part names) and collect auto-generated temp names.
	type entry struct {
		fixture   *Fixture
		autoName  string // short table name (last segment)
		dataset   string // dataset segment, if available
		hasCustom bool
	}

	seen := make(map[string]*entry)   // normalized table -> entry
	entries := make([]*entry, 0, len(tc.Fixtures))

	for i := range tc.Fixtures {
		f := &tc.Fixtures[i]
		norm := normalizeTablePath(f.Table)

		if prev, ok := seen[norm]; ok {
			// Same table referenced twice — keep the one with a custom name
			if f.TempName != "" {
				prev.fixture = f
				prev.hasCustom = true
			}
			continue
		}

		parts := splitTablePath(f.Table)
		shortName := parts[len(parts)-1]
		dataset := ""
		if len(parts) >= 2 {
			dataset = parts[len(parts)-2]
		}

		e := &entry{
			fixture:   f,
			autoName:  shortName,
			dataset:   dataset,
			hasCustom: f.TempName != "",
		}
		seen[norm] = e
		entries = append(entries, e)
	}

	// Second pass: detect collisions among auto-generated names.
	nameCount := make(map[string]int)
	for _, e := range entries {
		if e.hasCustom {
			continue
		}
		nameCount[e.autoName]++
	}

	// Third pass: build the final map, disambiguating where needed.
	// Replace hyphens with underscores in dataset names so the result is a
	// valid SQL identifier.
	m := make(map[string]string)
	for _, e := range entries {
		if e.hasCustom {
			m[e.fixture.Table] = e.fixture.TempName
			continue
		}
		tempName := e.autoName
		if nameCount[tempName] > 1 && e.dataset != "" {
			tempName = sanitizeIdentifier(e.dataset) + "__" + tempName
		}
		m[e.fixture.Table] = tempName
	}
	return m
}

// sanitizeIdentifier replaces characters that are invalid in SQL identifiers
// (e.g. hyphens) with underscores.
func sanitizeIdentifier(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			out[i] = c
		} else {
			out[i] = '_'
		}
	}
	return string(out)
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
