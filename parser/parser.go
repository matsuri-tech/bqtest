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

// ExtractAllTableRefs parses SQL and returns all table references with offsets (not deduped).
func ExtractAllTableRefs(sql string) ([]TableRef, error) {
	opts := newParserOptions()
	var allRefs []TableRef

	loc := zetasql.NewParseResumeLocation(sql)
	for {
		stmt, isEnd, err := zetasql.ParseNextScriptStatement(loc, opts)
		if err != nil {
			return nil, fmt.Errorf("parse error: %w", err)
		}
		if stmt != nil {
			cteNames := collectCTENames(stmt)
			destRefs := collectDestinationRefs(stmt)
			destPaths := make(map[string]bool)
			for _, ref := range destRefs {
				destPaths[ref.Path] = true
			}
			allRefs = append(allRefs, destRefs...)

			ast.Walk(stmt, func(n ast.Node) error {
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
				if cteNames[ref.Path] || destPaths[ref.Path] {
					return nil
				}
				ref.IsSource = true
				allRefs = append(allRefs, *ref)
				return nil
			})
		}
		if isEnd {
			break
		}
	}
	return allRefs, nil
}

// SQLKind represents the kind of SQL statement.
type SQLKind int

const (
	SQLKindSelect        SQLKind = iota // Pure SELECT query
	SQLKindCreateTableAS                // CREATE [OR REPLACE] TABLE ... AS SELECT
	SQLKindInsert                       // INSERT INTO
	SQLKindDelete                       // DELETE FROM
	SQLKindUpdate                       // UPDATE
	SQLKindMerge                        // MERGE INTO
	SQLKindDDL                          // Other DDL (CREATE TABLE without AS, DROP, ALTER, etc.)
	SQLKindOther                        // Anything else
)

func (k SQLKind) String() string {
	switch k {
	case SQLKindSelect:
		return "SELECT"
	case SQLKindCreateTableAS:
		return "CREATE TABLE AS"
	case SQLKindInsert:
		return "INSERT"
	case SQLKindDelete:
		return "DELETE"
	case SQLKindUpdate:
		return "UPDATE"
	case SQLKindMerge:
		return "MERGE"
	case SQLKindDDL:
		return "DDL"
	default:
		return "OTHER"
	}
}

// ClassifySQL returns the kind of the last meaningful statement in the SQL.
// Scripting statements (DECLARE, SET, etc.) are treated as pass-through
// so multi-statement scripts are classified by their final executable statement.
func ClassifySQL(sql string) (SQLKind, error) {
	opts := newParserOptions()
	loc := zetasql.NewParseResumeLocation(sql)

	var lastKind SQLKind
	seen := false

	for {
		stmt, isEnd, err := zetasql.ParseNextScriptStatement(loc, opts)
		if err != nil {
			return SQLKindOther, fmt.Errorf("parse error: %w", err)
		}
		if stmt != nil {
			seen = true
			kind := classifyNode(stmt)
			if kind != SQLKindOther {
				lastKind = kind
			}
		}
		if isEnd {
			break
		}
	}

	if !seen {
		return SQLKindOther, fmt.Errorf("empty SQL")
	}
	return lastKind, nil
}

func classifyNode(node ast.Node) SQLKind {
	switch n := node.(type) {
	case *ast.QueryStatementNode:
		return SQLKindSelect
	case *ast.CreateTableStatementNode:
		if n.Query() != nil {
			return SQLKindCreateTableAS
		}
		return SQLKindDDL
	case *ast.InsertStatementNode:
		return SQLKindInsert
	case *ast.DeleteStatementNode:
		return SQLKindDelete
	case *ast.UpdateStatementNode:
		return SQLKindUpdate
	case *ast.MergeStatementNode:
		return SQLKindMerge
	default:
		return SQLKindOther
	}
}

// StripDDL extracts the inner SELECT from CREATE TABLE AS statements.
// Returns the original SQL unchanged for pure SELECT queries and scripts.
// Returns an error for unsupported statement types (INSERT, DELETE, etc.).
// Parses the SQL only once.
func StripDDL(sql string) (string, SQLKind, error) {
	opts := newParserOptions()
	loc := zetasql.NewParseResumeLocation(sql)

	var lastKind SQLKind
	var lastStmt ast.Node
	seen := false

	for {
		stmt, isEnd, err := zetasql.ParseNextScriptStatement(loc, opts)
		if err != nil {
			return "", SQLKindOther, fmt.Errorf("parse error: %w", err)
		}
		if stmt != nil {
			seen = true
			kind := classifyNode(stmt)
			if kind != SQLKindOther {
				lastKind = kind
				lastStmt = stmt
			}
		}
		if isEnd {
			break
		}
	}

	if !seen {
		return "", SQLKindOther, fmt.Errorf("empty SQL")
	}

	switch lastKind {
	case SQLKindSelect, SQLKindOther:
		return sql, lastKind, nil
	case SQLKindCreateTableAS:
		createStmt, ok := lastStmt.(*ast.CreateTableStatementNode)
		if !ok {
			return "", lastKind, fmt.Errorf("expected CreateTableStatementNode")
		}
		query := createStmt.Query()
		if query == nil {
			return "", lastKind, fmt.Errorf("CREATE TABLE statement has no AS SELECT clause")
		}
		start := int(query.ParseLocationRange().Start().ByteOffset())
		end := int(query.ParseLocationRange().End().ByteOffset())
		if start >= 0 && end <= len(sql) && start < end {
			return sql[start:end], lastKind, nil
		}
		return "", lastKind, fmt.Errorf("could not extract SELECT from CREATE TABLE AS")
	case SQLKindInsert:
		return "", lastKind, fmt.Errorf("INSERT statements are not supported as test targets")
	case SQLKindDelete:
		return "", lastKind, fmt.Errorf("DELETE statements are not supported as test targets")
	case SQLKindUpdate:
		return "", lastKind, fmt.Errorf("UPDATE statements are not supported as test targets")
	case SQLKindMerge:
		return "", lastKind, fmt.Errorf("MERGE statements are not supported as test targets")
	case SQLKindDDL:
		return "", lastKind, fmt.Errorf("DDL statements (without AS SELECT) are not supported as test targets")
	default:
		return "", lastKind, fmt.Errorf("unsupported statement type: %s", lastKind)
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
