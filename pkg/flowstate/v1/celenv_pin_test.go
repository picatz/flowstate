package flowstatev1_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEveryStringsExtensionIsVersionPinned is the tripwire on a guarantee that
// cannot be tested by running anything.
//
// `ext.Strings()` with no version means MaxUint32 — every function, including
// whatever a future cel-go adds. An environment built that way changes meaning
// when a dependency is bumped, which is precisely the reinterpretation bug the
// profile mechanism exists to prevent for stored expressions. No evaluation test
// can catch it, because against today's cel-go the pinned and unpinned
// environments are identical; the difference only exists in a future nobody can
// import yet. What can be checked is the shape of the call, so the tree is
// walked and every `ext.Strings(...)` is required to carry an
// `ext.StringsVersion(...)` option.
//
// This found one real gap on the day it was written: all three policy surfaces
// pinned version 5 and the workflow core — whose expressions live longest and
// travel furthest — did not. The test exists so the fourth copy of that decision
// cannot quietly become a fifth surface's omission.
func TestEveryStringsExtensionIsVersionPinned(t *testing.T) {
	t.Parallel()

	root := repoRootDir(t)

	var found int
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == ".claude" || name == "vendor" || name == "plugins" {
				// plugins are separate modules with their own go.sum and their
				// own cel-go if they take one; this test guards the engine's
				// environments, and a plugin's are its own module's to guard.
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".pb.go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}

		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Strings" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "ext" {
				return true
			}

			found++
			require.True(t, hasStringsVersionOption(call.Args),
				"%s calls ext.Strings without ext.StringsVersion, so the environment it "+
					"builds gains whatever functions a future cel-go ships — an expression "+
					"stored today could mean something else after a dependency bump", path)

			return true
		})

		return nil
	}))

	// Four today: the profile map and the three policy surfaces. The floor is
	// what makes the walk honest — a corpus test that finds nothing passes every
	// assertion above.
	require.GreaterOrEqual(t, found, 4,
		"fewer ext.Strings call sites than expected were found, so this walked the wrong tree")
}

// hasStringsVersionOption reports whether one of a call's arguments is an
// ext.StringsVersion(...) call.
func hasStringsVersionOption(args []ast.Expr) bool {
	for _, arg := range args {
		call, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "StringsVersion" {
			return true
		}
	}

	return false
}

// repoRootDir walks up from the test's directory to the go.mod root.
func repoRootDir(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "walked to the filesystem root without finding go.mod")
		dir = parent
	}

	t.Fatal("go.mod not found within ten directories of the test")

	return ""
}
