package authz_test

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

// TestNoActionCheckOutsideAuthz holds every enforcement point to the one
// decision. The check "does this caller hold this action" was once written six
// times, each copy with its own idea of what an absent action list meant; a
// seventh copy would be a new place for that meaning to drift. Reading
// Principal.Actions to answer the question belongs in this package, and in the
// auth package that builds the list, and nowhere else.
func TestNoActionCheckOutsideAuthz(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")

	var offences []string

	for _, dir := range []string{"pkg", "cmd", "plugins", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}

			slash := filepath.ToSlash(path)
			if strings.Contains(slash, "/v1/authz/") || strings.Contains(slash, "/v1/auth/") || strings.HasSuffix(slash, ".pb.go") {
				return nil
			}

			file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if parseErr != nil {
				return parseErr
			}

			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Actions" {
					return true
				}
				// A policy entry's own list (validation, cloning) is not a caller's;
				// only a Principal's list answers a caller's authority.
				if inner, ok := selector.X.(*ast.Ident); ok && (inner.Name == "principal" || inner.Name == "p") {
					offences = append(offences, slash)
				}
				return true
			})

			return nil
		})
		require.NoError(t, err)
	}

	require.Empty(t, offences, "read a caller's actions through authz.Decide, not by comparing the list")
}
