package authz_test

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/authz"
)

var update = flag.Bool("update", false, "rewrite docs/AUTHORIZATION.md from the registry")

func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "docs", "AUTHORIZATION_FRESHNESS.md")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no repository root above the test")
		dir = parent
	}
}

// testFuncs returns the name of every top-level Test function under root.
func testFuncs(t *testing.T, root string) map[string]bool {
	t.Helper()

	names := map[string]bool{}
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "node_modules":
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
				names[fn.Name.Name] = true
			}
		}

		return nil
	}))

	return names
}

// TestEveryDecisionPointIsProven holds the registry to the tree: each point is
// unique, names the file that enforces it, says why when it is not closed, and
// is proven by tests that exist.
func TestEveryDecisionPointIsProven(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	tests := testFuncs(t, root)
	seen := map[string]bool{}

	for _, p := range authz.DecisionPoints() {
		require.False(t, seen[p.ID], "%s is listed twice", p.ID)
		seen[p.ID] = true

		require.NotEmpty(t, p.Name, p.ID)
		require.NotEmpty(t, p.Layer, p.ID)
		require.NotEmpty(t, p.ZeroNote, "%s does not say what its zero case does", p.ID)

		path, _, ok := strings.Cut(p.Enforced, ": ")
		require.True(t, ok, "%s: Enforced must read `path: symbol`", p.ID)
		_, err := os.Stat(filepath.Join(root, path))
		require.NoError(t, err, "%s: enforced in a file that does not exist", p.ID)

		if len(p.Proof) == 0 {
			require.True(t, strings.HasPrefix(path, "proto/"), "%s has no proof and is a point the host enforces", p.ID)
		}
		for _, name := range p.Proof {
			require.True(t, tests[name], "%s: proof %s is not a test in this repository", p.ID, name)
		}
	}
}

// TestAuthorizationDocIsCurrent keeps docs/AUTHORIZATION.md equal to what the
// registry renders. Run with -update after changing the registry.
func TestAuthorizationDocIsCurrent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "docs", "AUTHORIZATION.md")
	want := authz.Document()

	if *update {
		require.NoError(t, os.WriteFile(path, []byte(want), 0o644))
		return
	}

	got, err := os.ReadFile(path)
	require.NoError(t, err, "docs/AUTHORIZATION.md is missing; run with -update")
	require.Equal(t, want, string(got), "docs/AUTHORIZATION.md is stale; run: go test ./pkg/flowstate/v1/authz -run TestAuthorizationDocIsCurrent -update")
}
