package lsp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCallArgumentsNameTheirDeclaredContainerType pins #1640's last printers: a
// callee's `list(string)` input is shown as written, in both the completion
// detail and the hover, rather than as the legacy word `list` that the type
// said nothing more precise than.
func TestCallArgumentsNameTheirDeclaredContainerType(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	callee := `edition: v2026.4
name: place
inputs:
  hosts:
    type: list(string)
    required: true
  limits:
    type: map(string, int)
  anything:
    type: list(dyn)
steps:
  - id: announce
    log:
      message: hello
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"), []byte(callee), 0o644))

	src := `edition: v2026.4
name: caller
steps:
  - id: place
    call: ./callee.yaml
    with:
      hosts: [a]
      limits: {a: 1}
      anything: [1]
`
	caller := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

	c := newClient(t)
	c.initialize()
	uri := "file://" + caller
	c.open(uri, src)

	for key, want := range map[string]string{
		"hosts: [a]":    "**`hosts`** · `list(string)` · required",
		"limits: {a":    "**`limits`** · `map(string, int)` · optional",
		"anything: [1]": "**`anything`** · `list(dyn)` · optional",
	} {
		pos := positionOf(t, src, key, 1)
		assert.Contains(t, hoverText(c.hover(uri, pos.Line, pos.Character)), want, key)
	}

	menu := "edition: v2026.4\nname: caller\nsteps:\n  - id: place\n    call: ./callee.yaml\n    with:\n      |\n"
	text, pos := splitCursor(t, menu)
	menuPath := filepath.Join(dir, "menu.yaml")
	require.NoError(t, os.WriteFile(menuPath, []byte(text), 0o644))
	menuURI := "file://" + menuPath
	c.open(menuURI, text)

	got := c.complete(menuURI, pos.Line, pos.Character)
	hosts := findItem(got.Items, "hosts")
	require.NotNil(t, hosts)
	assert.Equal(t, "list(string) (required)", hosts.Detail)

	limits := findItem(got.Items, "limits")
	require.NotNil(t, limits)
	assert.Equal(t, "map(string, int)", limits.Detail)
}
