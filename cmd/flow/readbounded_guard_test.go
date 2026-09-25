package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEveryFileReadInThisPackageIsBounded refuses a bare os.ReadFile in every
// non-test Go file under cmd/flow, this package and the packages beneath it,
// build-ignored generators included: every file the command reads whole goes
// through [readBoundedFile] with its bound named beside it (#1767), and a
// reader added with os.ReadFile would be the one file read here without a
// limit. The helper itself opens and reads through a limit, so nothing is
// allowed; a package that cannot import it reads through its own limit.
func TestEveryFileReadInThisPackageIsBounded(t *testing.T) {
	t.Parallel()

	var paths []string
	require.NoError(t, filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			paths = append(paths, path)
		}
		return nil
	}))

	fset := token.NewFileSet()
	var seen int
	for _, path := range paths {
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
	require.Greater(t, seen, 50, "too few source files were walked; the walk is wrong, not the package")
}
