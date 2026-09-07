package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEveryFileReadInThisPackageIsBounded refuses a bare os.ReadFile in this
// package's non-test files: every file the command reads whole goes through
// [readBoundedFile] with its bound named beside it (#1767), and a reader added
// with os.ReadFile would be the one file read here without a limit. The
// helper itself opens and reads through a limit, so nothing is allowed.
func TestEveryFileReadInThisPackageIsBounded(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var seen int
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		seen++
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if ok && pkg.Name == "os" && sel.Sel.Name == "ReadFile" {
				t.Errorf("%s: os.ReadFile reads a file with no bound; use readBoundedFile with the bound named beside it",
					fset.Position(sel.Pos()))
			}
			return true
		})
	}
	require.Positive(t, seen, "no source files were walked; the glob is wrong, not the package")
}
