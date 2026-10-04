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

// TestEveryEgressPolicyATestAppliesIsPutBack holds every test in this package
// that applies an egress policy directly to [restoreDefaultRegistryAfter], and
// to running serially as that helper requires. Applying one replaces the
// process-wide http task, and a test that does not put it back leaves every
// later test in the binary under its policy: under `-shuffle=on` that is an
// order-dependent failure somewhere else (#1829, and a deny-everything MCP
// policy that reached a `flow task run` test on #2173).
func TestEveryEgressPolicyATestAppliesIsPutBack(t *testing.T) {
	t.Parallel()

	appliers := map[string]bool{"applyEgressPolicy": true, "applyMCPEgressPolicy": true, "applyExecPolicy": true}

	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)

	fset := token.NewFileSet()
	checked := 0
	for _, path := range files {
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			var applies, restores, parallel bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					applies = applies || appliers[fun.Name]
					restores = restores || fun.Name == "restoreDefaultRegistryAfter"
				case *ast.SelectorExpr:
					if recv, ok := fun.X.(*ast.Ident); ok && recv.Name == "t" && fun.Sel.Name == "Parallel" {
						parallel = true
					}
				}
				return true
			})
			if !applies {
				continue
			}
			checked++
			if !restores {
				t.Errorf("%s: %s applies an egress policy and does not call restoreDefaultRegistryAfter",
					fset.Position(fn.Pos()), fn.Name.Name)
			}
			if parallel {
				t.Errorf("%s: %s applies an egress policy and runs in parallel; the registry is process-wide",
					fset.Position(fn.Pos()), fn.Name.Name)
			}
		}
	}
	require.Positive(t, checked, "no test applying an egress policy was found; the walk is wrong, not the package")
}
