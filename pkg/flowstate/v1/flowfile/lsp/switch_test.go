package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A switch runs exactly one body in the enclosing scope and merges its outputs
// into the enclosing namespace, so its steps are part of the file's model: in the
// outline, visible after the switch, and invisible to a sibling case.
const switchSource = `name: routed
steps:
  - id: on_event
    switch:
      value: ${"opened"}
      default:
        steps:
          - id: unhandled
            log:
              message: nothing
      cases:
        - case: opened
          steps:
            - id: triage
              http:
                url: https://example.com
            - id: after_triage
              log:
                message: ${steps.triage.status_code}
        - case: closed
          steps:
            - id: archive
              log:
                message: ${steps.triage.status_code}
  - id: wrap_up
    log:
      message: ${steps.triage.status_code} ${steps.unhandled.result}
`

func TestSwitchBodiesAreInTheModel(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, switchSource)

	var ids []string
	for _, sym := range documentSymbols(doc) {
		ids = append(ids, sym.Name)
	}
	assert.ElementsMatch(t, []string{"on_event", "unhandled", "triage", "after_triage", "archive", "wrap_up"}, ids)

	// A later step reads a case body's output: bodies merge into the namespace.
	loc := definitionAt(doc, positionOf(t, switchSource, "steps.triage.status_code} ${steps.unhandled", len("steps.")+1))
	require.Len(t, loc, 1)
	assert.Equal(t, "triage", textInRange(switchSource, loc[0].Range))
	loc = definitionAt(doc, positionOf(t, switchSource, "steps.unhandled", len("steps.")+1))
	require.Len(t, loc, 1, "the default body merges too, wherever default: is written")
	assert.Equal(t, "unhandled", textInRange(switchSource, loc[0].Range))

	// A step in the same case reads its sibling; another case does not.
	pos := positionOf(t, switchSource, "- id: after_triage", 0)
	pos.Line += 2 // the message line of after_triage
	pos.Character = len("                message: ${steps.") + 1
	require.Len(t, definitionAt(doc, pos), 1, "a step reads an earlier step of its own case")
	pos = positionOf(t, switchSource, "- id: archive", 0)
	pos.Line += 2
	pos.Character = len("                message: ${steps.") + 1
	assert.Empty(t, definitionAt(doc, pos), "a sibling case's step is not visible: only one body runs")

	// `default:` is written after the cases' bodies, in the last body's range, and
	// still documents itself.
	h := hoverAt(doc, positionOf(t, switchSource, "default:", 1))
	require.NotNil(t, h)
}

func TestRenameSeesAStepInASwitchBody(t *testing.T) {
	t.Parallel()
	const src = `name: routed
steps:
  - id: on_event
    switch:
      value: ${"opened"}
      cases:
        - case: opened
          steps:
            - id: triage
              http:
                url: https://example.com
  - id: wrap_up
    log:
      message: ${steps.triage.status_code}
`
	doc := refsDoc(t, src)

	// Renaming into an id a switch body holds is a collision: bodies share the
	// enclosing namespace.
	_, err := renameAt(doc, positionOf(t, src, "id: wrap_up", len("id: ")), "triage")
	require.Error(t, err)

	// Renaming a body step reaches the reference after the switch.
	edit, err := renameAt(doc, positionOf(t, src, "id: triage", len("id: ")), "fetch")
	require.NoError(t, err)
	got := applyEdits(t, src, edit.Changes["file:///refs.yaml"])
	assert.Contains(t, got, "id: fetch")
	assert.Contains(t, got, "steps.fetch.status_code")
	assert.NotContains(t, got, "steps.triage")
}
