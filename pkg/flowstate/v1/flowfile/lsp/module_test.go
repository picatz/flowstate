package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const moduleSource = `edition: ` + flowfile.CurrentEdition + `
name: ids
types:
  Uuid:
    type: string
    must: isUuid(this)
errors:
  NotFound:
    description: The customer does not exist.
functions:
  isUuid:
    description: Whether a string is a UUID.
    params:
      s: string
    returns: bool
    body: ${s.matches("^[0-9a-f]{8}$")}
`

// TestAModuleIsAFirstClassFileInTheEditor: a file with no steps that declares
// types, functions and errors opens without a diagnostic, hover answers on its
// keys, and the outline lists its declarations.
func TestAModuleIsAFirstClassFileInTheEditor(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///ids.yaml"
	require.Empty(t, messages(c.open(uri, moduleSource).Diagnostics), "a module is not a workflow with no steps")

	pos := positionOf(t, moduleSource, "types:", 1)
	hover := c.hover(uri, pos.Line, pos.Character)
	require.NotNil(t, hover, "hover answers on a module's keys as it does on a workflow's")
	assert.Contains(t, hoverText(hover), "Declares named record types")

	type symbol struct{ name, container string }
	var got []symbol
	for _, s := range c.symbols(uri) {
		got = append(got, symbol{s.Name, s.ContainerName})
	}
	assert.Equal(t, []symbol{{"Uuid", "type"}, {"NotFound", "error"}, {"isUuid", "function"}}, got,
		"the outline of a module is its declarations, in the order written")
}

// TestAStepsLessFileThatIsNotAModuleStillSaysSo: the diagnostic for a file with
// nothing declared is unchanged, so a module is never inferred from emptiness.
func TestAStepsLessFileThatIsNotAModuleStillSaysSo(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	got := messages(c.open("file:///empty.yaml", "edition: "+flowfile.CurrentEdition+"\nname: empty\n").Diagnostics)
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "workflow has no steps")
}
