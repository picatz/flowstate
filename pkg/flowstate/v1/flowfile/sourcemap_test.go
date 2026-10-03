package flowfile_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

func TestSourceMapBindsSitesToTheirLines(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := filepath.Join(dir, "main.yaml")
	child := filepath.Join(dir, "child.yaml")
	rootText := `edition: v2026.4
name: main
steps:
  - id: pages
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: page
          log:
            message: one
  - id: more
    for_each:
      items: ${[1]}
      as: n
      steps:
        - id: page
          log:
            message: two
  - id: nested
    call: ./child.yaml
`
	childText := `edition: v2026.4
name: child
steps:
  - id: greet
    log:
      message: hi
`
	require.NoError(t, os.WriteFile(root, []byte(rootText), 0o600))
	require.NoError(t, os.WriteFile(child, []byte(childText), 0o600))

	workflow, positions, err := flowfile.ParseFile(root)
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap(root, []byte(rootText), workflow, positions)

	assert.Equal(t, flowfile.IRDigest(workflow), sourceMap.GetIrDigest())
	require.Len(t, sourceMap.GetDocuments(), 2, "the callee file is a document of its own")
	assert.Equal(t, v1.ContentDigest([]byte(rootText)), sourceMap.GetDocuments()[0].GetDigest())

	lines := map[string]uint32{}
	for _, entry := range sourceMap.GetEntries() {
		lines[v1.DebugSiteKey(entry.GetSite())] = entry.GetLocation().GetRange().GetStartLine()
	}
	assert.Equal(t, uint32(9), lines["main:pages/page"])
	assert.Equal(t, uint32(17), lines["main:more/page"], "the same id in a sibling loop is its own site")
	assert.Equal(t, uint32(4), lines["child:greet"])
}

func TestSourceMapLeavesOutAChangedCallee(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := filepath.Join(dir, "main.yaml")
	child := filepath.Join(dir, "child.yaml")
	rootText := "edition: v2026.4\nname: main\nsteps:\n  - id: nested\n    call: ./child.yaml\n"
	require.NoError(t, os.WriteFile(root, []byte(rootText), 0o600))
	require.NoError(t, os.WriteFile(child, []byte("edition: v2026.4\nname: child\nsteps:\n  - id: greet\n    log:\n      message: hi\n"), 0o600))

	workflow, positions, err := flowfile.ParseFile(root)
	require.NoError(t, err)

	// Edited after compiling: the recorded digest no longer holds.
	require.NoError(t, os.WriteFile(child, []byte("edition: v2026.4\nname: child\n\n\nsteps:\n  - id: greet\n    log:\n      message: hi\n"), 0o600))

	sourceMap := flowfile.SourceMap(root, []byte(rootText), workflow, positions)
	require.Len(t, sourceMap.GetDocuments(), 1, "a callee whose bytes changed is not mapped to lines it no longer holds")
}

// TestSourceMapStopsAtTheSchemasDocumentBound calls more distinct files than
// a map may name: the map stays inside the bound, and the files past it are
// left for address-only debugging rather than mapped past what a reader of the
// schema will accept.
func TestSourceMapStopsAtTheSchemasDocumentBound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	var root strings.Builder
	root.WriteString("edition: v2026.4\nname: main\nsteps:\n")
	for i := range flowfile.MaxSourceMapDocuments + 4 {
		name := fmt.Sprintf("c%d", i)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+".yaml"), []byte(
			"edition: v2026.4\nname: "+name+"\nsteps:\n  - id: s\n    log:\n      message: hi\n"), 0o600))
		fmt.Fprintf(&root, "  - id: %s\n    call: ./%s.yaml\n", name, name)
	}
	path := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(path, []byte(root.String()), 0o600))

	workflow, positions, err := flowfile.ParseFile(path)
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap(path, []byte(root.String()), workflow, positions)

	assert.Len(t, sourceMap.GetDocuments(), flowfile.MaxSourceMapDocuments)
	for _, entry := range sourceMap.GetEntries() {
		assert.Less(t, int(entry.GetLocation().GetDocument()), flowfile.MaxSourceMapDocuments)
	}
}
