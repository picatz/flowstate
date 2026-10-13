package flowfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// CalleeFiles names every file the compile read for `call:` steps: nested ones,
// each once, sorted, and a callee that was never read or did not compile too.
func TestCalleeFilesNamesEveryFileReadForCalls(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name, text string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(text), 0o644))

		return path
	}
	head := "edition: v2026.4\nname: n\nsteps:\n"
	write("lib/leaf.yaml", head+"  - id: s\n    log:\n      message: hi\n")
	// Calls the leaf, a file that is not there, and the leaf again.
	write("lib/mid.yaml", head+"  - id: a\n    call: ./leaf.yaml\n  - id: b\n    call: ./gone.yaml\n  - id: c\n    call: ./leaf.yaml\n")
	root := write("root.yaml", head+"  - id: x\n    call: ./lib/mid.yaml\n  - id: y\n    call: ./lib/leaf.yaml\n  - id: z\n    call: ./nope.yaml\n")
	data, err := os.ReadFile(root)
	require.NoError(t, err)

	_, files, err := flowfile.Env{}.ValidateSourceAtCachedReading(nil, data, root)
	require.Error(t, err, "the unreadable callees are problems")

	var rel []string
	for _, f := range files {
		r, err := filepath.Rel(dir, f)
		require.NoError(t, err)
		rel = append(rel, filepath.ToSlash(r))
	}
	require.Equal(t, []string{"lib/gone.yaml", "lib/leaf.yaml", "lib/mid.yaml", "nope.yaml"}, rel)
}
