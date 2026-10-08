package lsp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCallOutputsAreReadFromTheCallee holds the half of #1434 that concerns a
// `call:` step: hover and completion on `steps.<call>.<name>` answer from the
// callee's declared outputs, exactly those, and say nothing about a name it does
// not declare or when the callee will not compile.
func TestCallOutputsAreReadFromTheCallee(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	callee := `edition: v2026.4
name: provision
outputs:
  tenant:
    value: ${"acme"}
    type: string
    description: the tenant id that was created
  seats:
    value: ${3}
    type: int
steps:
  - id: noop
    log:
      message: hi
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"), []byte(callee), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "broken.yaml"), []byte("edition: v2026.4\nname: [\n"), 0o644))

	caller := func(target string) string {
		return "edition: v2026.4\nname: caller\nsteps:\n  - id: provision\n    call: " + target + "\n  - id: use\n    log:\n      message: ${steps.provision.tenant}\n"
	}

	c := newClient(t)
	c.initialize()

	src := caller("./callee.yaml")
	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o644))
	uri := "file://" + path
	c.open(uri, src)

	pos := positionOf(t, src, "provision.tenant", 1)
	hover := hoverText(c.hover(uri, pos.Line, pos.Character+len("provision.")))
	assert.Contains(t, hover, "**`steps.provision.tenant`** · `string`")
	assert.Contains(t, hover, "the tenant id that was created")
	assert.Contains(t, hover, "callee.yaml")

	// The step itself lists what the callee produces; a name it does not
	// declare gets no invented meaning.
	bare := "edition: v2026.4\nname: caller\nsteps:\n  - id: provision\n    call: ./callee.yaml\n  - id: use\n    log:\n      message: ${steps.provision}\n"
	barePath := filepath.Join(dir, "bare.yaml")
	require.NoError(t, os.WriteFile(barePath, []byte(bare), 0o644))
	c.open("file://"+barePath, bare)
	step := positionOf(t, bare, "steps.provision}", 1)
	assert.Contains(t, hoverText(c.hover("file://"+barePath, step.Line, step.Character+len("steps.pro"))), "Outputs: `tenant`, `seats`")
	missing := "edition: v2026.4\nname: caller\nsteps:\n  - id: provision\n    call: ./callee.yaml\n  - id: use\n    log:\n      message: ${steps.provision.nope}\n"
	missingPath := filepath.Join(dir, "missing.yaml")
	require.NoError(t, os.WriteFile(missingPath, []byte(missing), 0o644))
	c.open("file://"+missingPath, missing)
	at := positionOf(t, missing, "provision.nope", 1)
	assert.Empty(t, hoverText(c.hover("file://"+missingPath, at.Line, at.Character+len("provision."))))

	menu := "edition: v2026.4\nname: caller\nsteps:\n  - id: provision\n    call: ./callee.yaml\n  - id: use\n    log:\n      message: ${steps.provision.|}\n"
	text, cursor := splitCursor(t, menu)
	menuPath := filepath.Join(dir, "menu.yaml")
	require.NoError(t, os.WriteFile(menuPath, []byte(text), 0o644))
	menuURI := "file://" + menuPath
	c.open(menuURI, text)
	assert.ElementsMatch(t, []string{"tenant", "seats"}, labels(c.complete(menuURI, cursor.Line, cursor.Character).Items))

	// The prefix narrows the answer, which is resolved only for the call being
	// completed (the source is asked after the dot, not when candidates are built).
	narrow := "edition: v2026.4\nname: caller\nsteps:\n  - id: provision\n    call: ./callee.yaml\n  - id: use\n    log:\n      message: ${steps.provision.se|}\n"
	text, cursor = splitCursor(t, narrow)
	narrowPath := filepath.Join(dir, "narrow.yaml")
	require.NoError(t, os.WriteFile(narrowPath, []byte(text), 0o644))
	c.open("file://"+narrowPath, text)
	assert.Equal(t, []string{"seats"}, labels(c.complete("file://"+narrowPath, cursor.Line, cursor.Character).Items))

	// A callee that does not compile declares nothing this can be sure of.
	brokenMenu := "edition: v2026.4\nname: caller\nsteps:\n  - id: provision\n    call: ./broken.yaml\n  - id: use\n    log:\n      message: ${steps.provision.|}\n"
	text, cursor = splitCursor(t, brokenMenu)
	brokenPath := filepath.Join(dir, "brokenmenu.yaml")
	require.NoError(t, os.WriteFile(brokenPath, []byte(text), 0o644))
	brokenURI := "file://" + brokenPath
	c.open(brokenURI, text)
	assert.Empty(t, labels(c.complete(brokenURI, cursor.Line, cursor.Character).Items))
}
