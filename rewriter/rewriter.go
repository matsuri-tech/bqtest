package rewriter

import (
	"fmt"
	"sort"
	"strings"

	"github.com/matsuri-tech/bqtest/parser"
)


// RewriteResult contains the rewritten SQL and metadata.
type RewriteResult struct {
	// SQL is the rewritten SQL with table references replaced.
	SQL string
	// RewrittenTables maps original table path to temp table name.
	RewrittenTables map[string]string
	// UnresolvedTables are source tables with no fixture and not in passthrough.
	UnresolvedTables []string
}

// Rewrite replaces source table references in SQL based on the rewrite map.
// rewriteMap: fully-qualified table name -> temp table name
// passthrough: set of table paths that are allowed without fixtures
func Rewrite(sql string, rewriteMap map[string]string, passthrough map[string]bool) (*RewriteResult, error) {
	// Collect all replacements needed, keyed by offset to avoid duplicates
	type replacement struct {
		start   int
		end     int
		newText string
	}
	var replacements []replacement

	result := &RewriteResult{
		RewrittenTables: make(map[string]string),
	}

	// We need all occurrences (not deduped) to get correct offsets.
	// Re-extract with offsets by parsing again and collecting all refs.
	allRefs, err := extractAllRefs(sql)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	for _, ref := range allRefs {
		if !ref.IsSource {
			continue
		}
		tempName, hasFixture := rewriteMap[ref.Path]
		if hasFixture {
			replacements = append(replacements, replacement{
				start:   ref.StartOffset,
				end:     ref.EndOffset,
				newText: tempName,
			})
			result.RewrittenTables[ref.Path] = tempName
		} else if !passthrough[ref.Path] && !seen[ref.Path] {
			result.UnresolvedTables = append(result.UnresolvedTables, ref.Path)
			seen[ref.Path] = true
		}
	}

	// Sort replacements by offset descending so we can replace from end to start
	sort.Slice(replacements, func(i, j int) bool {
		return replacements[i].start > replacements[j].start
	})

	// Check for overlapping replacements
	for i := 1; i < len(replacements); i++ {
		if replacements[i-1].start < replacements[i].end {
			return nil, fmt.Errorf("overlapping replacements at offsets %d-%d and %d-%d",
				replacements[i].start, replacements[i].end,
				replacements[i-1].start, replacements[i-1].end)
		}
	}

	// Apply replacements from end to start
	rewritten := sql
	for _, r := range replacements {
		// Handle backtick-quoted references: the AST offsets point inside the backticks
		// but the backticks themselves are part of the SQL text
		start := r.start
		end := r.end
		if start > 0 && rewritten[start-1] == '`' && end < len(rewritten) && rewritten[end] == '`' {
			start--
			end++
		}
		rewritten = rewritten[:start] + r.newText + rewritten[end:]
	}

	result.SQL = rewritten
	return result, nil
}

// extractAllRefs extracts all table references with their offsets (not deduped).
func extractAllRefs(sql string) ([]parser.TableRef, error) {
	return parser.ExtractAllTableRefs(sql)
}

// ValidateRewrite checks that all source tables have fixtures or are in passthrough.
func ValidateRewrite(sql string, rewriteMap map[string]string, passthrough map[string]bool) error {
	parseResult, err := parser.ExtractTables(sql)
	if err != nil {
		return err
	}

	var missing []string
	for _, ref := range parseResult.SourceTables {
		if _, hasFixture := rewriteMap[ref.Path]; hasFixture {
			continue
		}
		if passthrough[ref.Path] {
			continue
		}
		missing = append(missing, ref.Path)
	}

	if len(missing) > 0 {
		return fmt.Errorf("source tables without fixtures or passthrough: %s", strings.Join(missing, ", "))
	}
	return nil
}
