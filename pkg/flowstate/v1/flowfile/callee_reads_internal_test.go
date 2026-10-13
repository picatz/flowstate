package flowfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const readsHead = "edition: v2026.4\nname: n\nsteps:\n"

func readsFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, text := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644))
	}

	return dir
}

// Many call steps reaching one failing callee name it once: what is remembered
// is bounded by distinct files, not by call sites.
func TestCalleeReadsAreBoundedByDistinctFiles(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	b.WriteString(readsHead)
	for i := range 200 {
		fmt.Fprintf(&b, "  - id: c%d\n    call: ./gone.yaml\n", i)
	}
	dir := readsFixture(t, map[string]string{"root.yaml": b.String()})
	path := filepath.Join(dir, "root.yaml")

	_, pos, _ := parse(Env{}, []byte(b.String()), path, nil, new(int), nil)

	require.Len(t, pos.reads, 1)
	require.Len(t, pos.CalleeFiles(), 1)
}

// A callee that compiles but pushes the call tree past the expansion budget has
// no frame, yet what it called is still read.
func TestCalleeReadsIncludeWhatAnOverBudgetCalleeCalled(t *testing.T) {
	t.Parallel()
	steps := func(n int) string {
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, "  - id: s%d\n    log:\n      message: hi\n", i)
		}

		return b.String()
	}
	leaf := readsHead + steps(1)
	files := map[string]string{
		"big.yaml": readsHead + steps(3000),
		// Compiles cleanly and calls a leaf, but is what takes the tree over the bound.
		"over.yaml": readsHead + steps(1500) + "  - id: tail\n    call: ./leaf.yaml\n",
		"leaf.yaml": leaf,
	}
	var root strings.Builder
	root.WriteString(readsHead)
	for i := range 33 {
		fmt.Fprintf(&root, "  - id: a%d\n    call: ./big.yaml\n", i)
	}
	root.WriteString("  - id: over\n    call: ./over.yaml\n")
	files["root.yaml"] = root.String()
	dir := readsFixture(t, files)

	_, pos, err := parse(Env{}, []byte(root.String()), filepath.Join(dir, "root.yaml"), nil, new(int), nil)
	require.ErrorContains(t, err, "more than")

	var rel []string
	for _, f := range pos.CalleeFiles() {
		r, err := filepath.Rel(dir, f)
		require.NoError(t, err)
		rel = append(rel, r)
	}
	require.Contains(t, rel, "leaf.yaml", "the over-budget callee's own callee is read")
}
