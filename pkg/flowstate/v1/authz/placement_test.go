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
// an Actions list to answer the question belongs in this package, and in the
// auth package that builds the list, and nowhere else. The principal package
// is the one other reader: it projects the list into a rule's CEL identity and
// only smooths a nil list to an empty one; it decides nothing.
func TestNoActionCheckOutsideAuthz(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")

	var offences []string

	for _, dir := range []string{"pkg", "cmd", "plugins", "internal", "tools", "examples"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}

			slash := filepath.ToSlash(path)
			if strings.Contains(slash, "/pkg/flowstate/v1/authz/") || strings.Contains(slash, "/pkg/flowstate/v1/auth/") ||
				strings.HasSuffix(slash, "/pkg/flowstate/v1/principal/principal.go") ||
				strings.HasSuffix(slash, ".pb.go") {
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
				// Any read of an Actions field is an offence, whatever the receiver
				// is called, except a policy entry's own list that validation walks:
				// that is configuration, not a caller's authority.
				if inner, ok := selector.X.(*ast.Ident); ok && inner.Name == "issuer" {
					return true
				}
				offences = append(offences, slash)
				return true
			})

			return nil
		})
		require.NoError(t, err)
	}

	require.Empty(t, offences, "read a caller's actions through authz.Decide, not by comparing the list")
}
