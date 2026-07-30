package lsp

import (
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const navigationSource = `edition: v2026.2
name: navigation
steps:
  - id: web
    http:
      url: https://example.com
  - id: status
    log:
      message: ${string(steps.web.status_code)}
  - id: shout
    shell:
      message: hi
`

// TestDefinition checks that a reference jumps to the step that declares it.
func TestDefinition(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///navigation.yaml"
	c.open(uri, navigationSource)

	t.Run("reference resolves to the step id", func(t *testing.T) {
		pos := positionOf(t, navigationSource, "web.status_code", 1)
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, lsp.DocumentURI(uri), got[0].URI)
		assert.Equal(t, "web", textInRange(navigationSource, got[0].Range))
		assert.Equal(t, 3, got[0].Range.Start.Line, "should point at the id declaration")
	})

	t.Run("the output name also resolves to the step", func(t *testing.T) {
		pos := positionOf(t, navigationSource, "status_code)", 2)
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, "web", textInRange(navigationSource, got[0].Range))
	})

	t.Run("the root segment also resolves to the step", func(t *testing.T) {
		// The root is part of the reference rather than a token of its own, so
		// landing on it navigates to the step the reference names — the same
		// answer as from either segment after it.
		pos := positionOf(t, navigationSource, "steps.web.status_code", 1)
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, "web", textInRange(navigationSource, got[0].Range))
	})

	t.Run("nothing to go to outside an expression", func(t *testing.T) {
		pos := positionOf(t, navigationSource, "https://example.com", 2)
		assert.Empty(t, c.definition(uri, pos.Line, pos.Character))
	})

	t.Run("a forward reference does not resolve", func(t *testing.T) {
		const src = `name: fwd
steps:
  - id: a
    log:
      message: ${steps.later.status_code}
  - id: later
    http:
      url: https://example.com
`
		c.open("file:///fwd.yaml", src)
		pos := positionOf(t, src, "${steps.later.status_code}", len("${steps."))
		assert.Empty(t, c.definition("file:///fwd.yaml", pos.Line, pos.Character),
			"jumping to a step that has not run would suggest the reference works")
	})

	t.Run("the retired spelling does not resolve", func(t *testing.T) {
		// A bare `later.result` names no step in this grammar, so there is
		// nowhere to jump to. Following it to the step anyway would tell an
		// author the spelling still works while `flow validate` tells them to run
		// `flow fix`.
		const src = `name: bare
steps:
  - id: earlier
    http:
      url: https://example.com
  - id: b
    log:
      message: ${earlier.status_code}
edition: v2026.2
`
		params := c.open("file:///bare-nav.yaml", src)
		require.Len(t, params.Diagnostics, 1, "premise: the compiler refuses the bare spelling")

		pos := positionOf(t, src, "${earlier.status_code}", 3)
		assert.Empty(t, c.definition("file:///bare-nav.yaml", pos.Line, pos.Character))
	})
}

// TestDocumentSymbols checks the outline an editor shows.
func TestDocumentSymbols(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///symbols.yaml"
	c.open(uri, navigationSource)

	got := c.symbols(uri)
	require.Len(t, got, 3)

	// The step's key is the task, so the outline's second column names the task
	// with nothing in the document having to spell it out.
	assert.Equal(t, "web", got[0].Name)
	assert.Equal(t, "http", got[0].ContainerName)
	assert.Equal(t, lsp.SKFunction, got[0].Kind)
	assert.Equal(t, lsp.DocumentURI(uri), got[0].Location.URI)

	assert.Equal(t, "status", got[1].Name)
	assert.Equal(t, "log", got[1].ContainerName)

	// An unregistered task is labelled as such rather than presented as real.
	assert.Equal(t, "shout", got[2].Name)
	assert.Equal(t, "shell (unknown task)", got[2].ContainerName)

	// Each symbol's range must cover its whole step and nothing beyond it, or an
	// editor's breadcrumb flickers as the cursor moves through the file.
	//
	// assignStepRanges ends a step on the line before the next step's dash, and
	// walks the last step's end back over trailing blank lines. The dashes in
	// navigationSource are on lines 3, 6 and 9, so the steps own 3-5, 6-8 and
	// 9-11 — the whole file below `steps:`, partitioned with no line left over.
	//
	// One more than they read before the `edition:` marker was required, which is
	// also why that marker is written *first* in this fixture where the rest of the
	// package appends it: a top-level key written after the steps would extend the
	// last step's range past its own content.
	assert.Equal(t, 3, got[0].Location.Range.Start.Line)
	assert.Equal(t, 5, got[0].Location.Range.End.Line)
	assert.Equal(t, 6, got[1].Location.Range.Start.Line)
	assert.Equal(t, 8, got[1].Location.Range.End.Line)
	assert.Equal(t, 9, got[2].Location.Range.Start.Line)
	// The last step is the one with no successor to bound it, so it is the only
	// one that can run off the end of the file. It must stop at its last content
	// line rather than at the empty line the trailing newline leaves behind.
	assert.Equal(t, 11, got[2].Location.Range.End.Line)
}

// TestDocumentSymbolsNamesAnUnnamedStep checks that a step with no id still appears
// in the outline, since an empty row is worse than a placeholder.
//
// The step was written `task:`/`name:`/`inputs:` before verb-key flattening. The
// question it asked — what an editor shows for a step whose id is missing — is
// unchanged, but the new grammar states it more sharply: the task key is now the
// step's only key, so there is nothing else the outline could have fallen back to.
// The container is asserted alongside the name for that reason, because a
// placeholder name on its own would look identical if the step had not been
// recognized as a step at all.
func TestDocumentSymbolsNamesAnUnnamedStep(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	c.open("file:///noid.yaml", `name: noid
steps:
  - log:
      message: hi
`)
	got := c.symbols("file:///noid.yaml")
	require.Len(t, got, 1)
	assert.Equal(t, "(step with no id)", got[0].Name)
	assert.Equal(t, "log", got[0].ContainerName)
}

// TestDocumentSymbolsLeaveTheDescriptionOut pins the choice symbols.go argues for,
// because a step's prose is the obvious thing to put in an outline row.
//
// A SymbolInformation has two fields a reader sees and no third to grow into, and
// both are spent on facts with nowhere else to appear: the id a reference in
// another step spells, and what kind of work the step does plus which block it is
// inside. Prose is unbounded and author-written, so spending either on it costs a
// fact to repeat something hover already says with room to say it.
func TestDocumentSymbolsLeaveTheDescriptionOut(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///described-outline.yaml"
	c.open(uri, `name: outline
steps:
  - id: loop
    description: Fan out over everyone on the rota.
    for_each:
      items: "${['a']}"
      steps:
        - id: body
          description: Greet one person.
          log:
            message: hi
`)

	got := c.symbols(uri)
	require.Len(t, got, 2)

	assert.Equal(t, "loop", got[0].Name)
	assert.Equal(t, "for_each", got[0].ContainerName)
	assert.Equal(t, "body", got[1].Name)
	// The nesting is what prose would have displaced, and it is the only place a
	// flat outline can say this step runs inside the loop.
	assert.Equal(t, "log in loop", got[1].ContainerName)

	for _, s := range got {
		for _, word := range []string{"rota", "Greet"} {
			assert.NotContains(t, s.Name, word)
			assert.NotContains(t, s.ContainerName, word)
		}
	}
}

// TestSymbolsAndDefinitionOnUnparseableDocument checks that both features return an
// empty result rather than stale or invented positions.
func TestSymbolsAndDefinitionOnUnparseableDocument(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	// The tab in the last line's indentation is what YAML refuses; the step's key
	// is incidental, and is spelled the way a step spells one so that the fixture
	// stops being valid Flowfile for exactly one reason. Character 5 of that line
	// lands inside the key either way.
	c.open("file:///broken.yaml", "name: x\nsteps:\n  - id: a\n  \tlog: y\n")

	assert.Empty(t, c.symbols("file:///broken.yaml"))
	assert.Empty(t, c.definition("file:///broken.yaml", 3, 5))
}
