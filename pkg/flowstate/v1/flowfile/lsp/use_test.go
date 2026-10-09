package lsp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const usedModuleSource = `edition: ` + flowfile.CurrentEdition + `
name: ids
types:
  Uuid:
    type: string
    must: isUuid(this)
    description: A lowercase UUID.
errors:
  NotFound: {}
functions:
  isUuid:
    description: Whether a string is a UUID.
    params:
      s: string
    returns: bool
    body: ${s.matches("^[0-9a-f]{8}$")}
`

const useSource = `edition: ` + flowfile.CurrentEdition + `
name: bill
use:
  ids:
    path: ./lib/ids.yaml
inputs:
  ref:
    type: ids.Uuid
    required: true
steps:
  - id: check
    if: ${!ids.isUuid(inputs.ref)}
    fail:
      error: ids.NotFound
`

// TestQualifiedNamesAnswerInTheEditor: a file that uses a module compiles in the
// editor without a diagnostic, hover describes a module's function and the scalar
// type of an input, go-to-definition on the `use:` path opens the module, and
// completion after the alias offers the module's functions.
func TestQualifiedNamesAnswerInTheEditor(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	module := filepath.Join(dir, "lib", "ids.yaml")
	require.NoError(t, os.WriteFile(module, []byte(usedModuleSource), 0o644))
	path := filepath.Join(dir, "bill.yaml")
	require.NoError(t, os.WriteFile(path, []byte(useSource), 0o644))
	uri := "file://" + path

	c := newClient(t)
	c.initialize()
	require.Empty(t, messages(c.open(uri, useSource).Diagnostics), "premise: the file compiles")

	call := positionOf(t, useSource, "ids.isUuid(", 1)
	hover := hoverText(c.hover(uri, call.Line, call.Character))
	assert.Contains(t, hover, "ids.isUuid(s: string) -> bool")
	assert.Contains(t, hover, "declared by the module `ids`")

	// On the alias or the function, the same name.
	onName := hoverText(c.hover(uri, call.Line, call.Character+len("ids.is")))
	assert.Contains(t, onName, "ids.isUuid(s: string) -> bool")

	input := positionOf(t, useSource, "inputs.ref", 1)
	assert.Contains(t, hoverText(c.hover(uri, input.Line, input.Character+len("inputs."))), "ids.Uuid")

	target := positionOf(t, useSource, "./lib/ids.yaml", 3)
	got := c.definition(uri, target.Line, target.Character)
	require.Len(t, got, 1)
	assert.Equal(t, "file://"+module, string(got[0].URI), "the path names the file the compiler reads")

	// The key's own words are not a path.
	key := positionOf(t, useSource, "use:", 1)
	assert.Empty(t, c.definition(uri, key.Line, key.Character))

	menu := "edition: " + flowfile.CurrentEdition + "\nname: bill\nuse:\n  ids:\n    path: ./lib/ids.yaml\nsteps:\n  - id: check\n    log:\n      message: ${ids.|}\n"
	text, cursor := splitCursor(t, menu)
	menuPath := filepath.Join(dir, "menu.yaml")
	require.NoError(t, os.WriteFile(menuPath, []byte(text), 0o644))
	menuURI := "file://" + menuPath
	c.open(menuURI, text)
	assert.Equal(t, []string{"isUuid"}, labels(c.complete(menuURI, cursor.Line, cursor.Character).Items),
		"a module's functions are offered where an expression is written")
}

// TestAModuleThatDoesNotResolveIsReportedAtTheUse: a path outside what a `use:` may
// read is a diagnostic in the editor, and nothing is navigated to.
func TestAModuleThatDoesNotResolveIsReportedAtTheUse(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := "edition: " + flowfile.CurrentEdition + "\nname: bill\nuse:\n  ids:\n    path: ../ids.yaml\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	path := filepath.Join(dir, "bill.yaml")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o644))
	uri := "file://" + path

	c := newClient(t)
	c.initialize()
	got := messages(c.open(uri, src).Diagnostics)
	require.NotEmpty(t, got)
	assert.Contains(t, got[0], "climbs above the directory")

	target := positionOf(t, src, "../ids.yaml", 3)
	assert.Empty(t, c.definition(uri, target.Line, target.Character))
}
