package lsp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// exampleWorkflowPaths returns the shipped examples' Flowfiles, the same corpus
// `flow fmt`'s idempotence test and `flow fix`'s byte-identity test use.
func exampleWorkflowPaths(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	return paths
}

// readWorkflowFile reads an example's bytes.
func readWorkflowFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

// TestFormattingReturnsMarshalsOutput is the property the feature exists for: a
// formatting request produces the same document `flow fmt` would write, through
// the same [flowfile.Marshal] path.
func TestFormattingReturnsMarshalsOutput(t *testing.T) {
	t.Parallel()

	const src = `# a comment Marshal does not carry through
edition: v2026.2
name: greeter
steps:
- id: greet
  log:
    message: hello world
`

	c := newClient(t)
	c.initialize()
	c.open("file:///format.yaml", src)

	edits := c.format("file:///format.yaml")
	require.Len(t, edits, 1, "a document with something to change draws exactly one full-document edit")

	workflow, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)
	want, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	assert.Equal(t, string(want), edits[0].NewText)
	assert.Equal(t, 0, edits[0].Range.Start.Line)
	assert.Equal(t, 0, edits[0].Range.Start.Character)
}

// TestFormattingOfAnAlreadyFormattedDocumentReturnsNoEdits checks the other
// direction: a document Marshal would write back unchanged draws an empty list
// rather than a same-text edit an editor would still have to apply.
func TestFormattingOfAnAlreadyFormattedDocumentReturnsNoEdits(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.2
name: greeter
steps:
- id: greet
  log:
    message: hello world
`
	workflow, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)
	formatted, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	c := newClient(t)
	c.initialize()
	c.open("file:///already-formatted.yaml", string(formatted))

	edits := c.format("file:///already-formatted.yaml")
	assert.Empty(t, edits)
}

// TestFormattingOfABrokenDocumentReturnsNoEdits is the property that makes the
// feature safe to bind to save-on-format: a document that does not compile has
// no workflow for Marshal to render, and inventing one to patch around the
// break would be exactly the class of mistake `flow fix` refuses to make.
func TestFormattingOfABrokenDocumentReturnsNoEdits(t *testing.T) {
	t.Parallel()

	t.Run("invalid YAML", func(t *testing.T) {
		t.Parallel()
		c := newClient(t)
		c.initialize()
		c.open("file:///broken.yaml", "name: x\n  steps: [\n")

		assert.Empty(t, c.format("file:///broken.yaml"))
	})

	t.Run("a task that does not compile", func(t *testing.T) {
		t.Parallel()
		// Compiles as YAML but is missing what the grammar requires: a step
		// naming no task at all.
		const src = `edition: v2026.2
name: x
steps:
- id: greet
`
		c := newClient(t)
		c.initialize()
		c.open("file:///incomplete.yaml", src)

		assert.Empty(t, c.format("file:///incomplete.yaml"))
	})
}

// TestFormattingRoundTripsExamples checks the feature against every shipped
// example, the same corpus `flow fmt`'s idempotence test uses: whatever
// Marshal(Unmarshal(x)) produces for a real file, formatting returns as the edit.
func TestFormattingRoundTripsExamples(t *testing.T) {
	t.Parallel()

	paths := exampleWorkflowPaths(t)
	require.NotEmpty(t, paths, "no examples found, so this proves nothing")

	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data := readWorkflowFile(t, path)
			// The real, absolute path rather than a synthetic one: `call-a-
			// workflow` names a sibling file relative to its own directory, and a
			// URI that does not correspond to where the file actually lives would
			// resolve that path against nothing. flowfile.ParseFile is what `flow
			// validate` and `flow fmt` both compile the example through, so want
			// is computed the same way this server's own path-aware branch
			// computes it.
			abs, err := filepath.Abs(path)
			require.NoError(t, err)
			workflow, _, err := flowfile.ParseFile(abs)
			require.NoError(t, err)
			want, err := flowfile.Marshal(workflow)
			require.NoError(t, err)

			uri := "file://" + abs
			c := newClient(t)
			c.initialize()
			c.open(uri, string(data))

			edits := c.format(uri)
			if string(want) == string(data) {
				assert.Empty(t, edits)
				return
			}
			require.Len(t, edits, 1)
			assert.Equal(t, string(want), edits[0].NewText)
		})
	}
}
