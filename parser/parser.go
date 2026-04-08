package parser

import (
	"fmt"
	"strings"

	zetasql "github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
)

// TableRef represents a table reference found in SQL.
type TableRef struct {
	// Path is the fully-qualified table path, e.g. "m2m-core.dx_501_clearing.nodes"
	Path string
	// Parts is the individual path components, e.g. ["m2m-core", "dx_501_clearing", "nodes"]
	Parts []string
	// IsSource indicates this is a source table (FROM, JOIN, etc.)
	IsSource bool
	// IsDestination indicates this is a destination table (INSERT INTO, CREATE TABLE, etc.)
	IsDestination bool
	// StartOffset is the byte offset in the original SQL where this reference starts
	StartOffset int
	// EndOffset is the byte offset in the original SQL where this reference ends
	EndOffset int
}

// ParseResult contains the result of parsing a SQL string.
type ParseResult struct {
	// SourceTables are tables referenced in FROM, JOIN, etc.
	SourceTables []TableRef
	// DestinationTables are tables referenced in INSERT INTO, CREATE TABLE, etc.
	DestinationTables []TableRef
}

func newParserOptions() *zetasql.ParserOptions {
	opts := zetasql.NewParserOptions()
	langOpts := zetasql.NewLanguageOptions()
	langOpts.EnableMaximumLanguageFeaturesForDevelopment()
	langOpts.SetSupportsAllStatementKinds()
	opts.SetLanguageOptions(langOpts)
	return opts
}

// ExtractTables parses BigQuery SQL and extracts all table references.
// It handles both single statements and multi-statement scripts.
func ExtractTables(sql string) (*ParseResult, error) {
	opts := newParserOptions()
	result := &ParseResult{}

	loc := zetasql.NewParseResumeLocation(sql)
	for {
		stmt, isEnd, err := zetasql.ParseNextScriptStatement(loc, opts)
		if err != nil {
			return nil, fmt.Errorf("parse error: %w", err)
		}
		if stmt != nil {
			extractFromStatement(stmt, result)
		}
		if isEnd {
			break
		}
	}

	result.SourceTables = dedup(result.SourceTables)
	result.DestinationTables = dedup(result.DestinationTables)
	return result, nil
}

func extractFromStatement(node ast.Node, result *ParseResult) {
	// Collect CTE names to exclude from source tables
	cteNames := collectCTENames(node)
	// Collect destination tables directly (they appear as PathExpressionNode, not TablePathExpressionNode)
	destRefs := collectDestinationRefs(node)
	destPaths := make(map[string]bool)
	for _, ref := range destRefs {
		destPaths[ref.Path] = true
		result.DestinationTables = append(result.DestinationTables, ref)
	}

	// Walk for source tables (TablePathExpressionNode)
	ast.Walk(node, func(n ast.Node) error {
		tpe, ok := n.(*ast.TablePathExpressionNode)
		if !ok {
			return nil
		}
		pathExpr := tpe.PathExpr()
		if pathExpr == nil {
			return nil
		}
		ref := buildTableRef(pathExpr)
		if ref == nil {
			return nil
		}
		// Skip CTE names and destination tables
		if cteNames[ref.Path] || destPaths[ref.Path] {
			return nil
		}
		ref.IsSource = true
		result.SourceTables = append(result.SourceTables, *ref)
		return nil
	})
}

// collectCTENames finds all CTE alias names defined in WITH clauses.
func collectCTENames(node ast.Node) map[string]bool {
	names := make(map[string]bool)
	ast.Walk(node, func(n ast.Node) error {
		if entry, ok := n.(*ast.WithClauseEntryNode); ok {
			if alias := entry.Alias(); alias != nil {
				names[alias.Name()] = true
			}
		}
		return nil
	})
	return names
}

// collectDestinationRefs extracts destination table refs from DDL/DML statements.
// These appear as bare PathExpressionNode (not TablePathExpressionNode).
func collectDestinationRefs(node ast.Node) []TableRef {
	var refs []TableRef
	addFromPath := func(pe *ast.PathExpressionNode) {
		if pe == nil {
			return
		}
		if ref := buildTableRef(pe); ref != nil {
			ref.IsDestination = true
			refs = append(refs, *ref)
		}
	}
	addFromGeneralized := func(gpe ast.GeneralizedPathExpressionNode) {
		if gpe == nil {
			return
		}
		if pe, ok := gpe.(*ast.PathExpressionNode); ok {
			addFromPath(pe)
		}
	}

	ast.Walk(node, func(n ast.Node) error {
		switch stmt := n.(type) {
		case *ast.InsertStatementNode:
			addFromGeneralized(stmt.TargetPath())
		case *ast.CreateTableStatementNode:
			addFromPath(stmt.Name())
		case *ast.MergeStatementNode:
			addFromPath(stmt.TargetPath())
		case *ast.UpdateStatementNode:
			addFromGeneralized(stmt.TargetPath())
		case *ast.DeleteStatementNode:
			addFromGeneralized(stmt.TargetPath())
		}
		return nil
	})
	return refs
}

func buildTableRef(pathExpr *ast.PathExpressionNode) *TableRef {
	names := pathExpr.Names()
	if len(names) == 0 {
		return nil
	}
	parts := make([]string, len(names))
	for i, ident := range names {
		parts[i] = ident.Name()
	}
	return &TableRef{
		Path:        strings.Join(parts, "."),
		Parts:       parts,
		StartOffset: int(pathExpr.ParseLocationRange().Start().ByteOffset()),
		EndOffset:   int(pathExpr.ParseLocationRange().End().ByteOffset()),
	}
}

func dedup(refs []TableRef) []TableRef {
	seen := make(map[string]bool)
	var out []TableRef
	for _, ref := range refs {
		if !seen[ref.Path] {
			seen[ref.Path] = true
			out = append(out, ref)
		}
	}
	return out
}
