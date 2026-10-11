package lsp

import (
	"fmt"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const inlaySource = `name: hints
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com
  - id: each
    for_each:
      items: "${['a', 'b']}"
      steps:
        - id: say
          log:
            message: ${item}
edition: v2026.4
`

func hintsFor(t *testing.T, src string, visible lsp.Range) []inlayHint {
	t.Helper()
	return inlayHints(newDocument("file:///hints.yaml", 1, src, nil), visible)
}

func wholeDocument() lsp.Range {
	return lsp.Range{End: lsp.Position{Line: 1 << 20}}
}

func TestInlayHintsShowCheckerTypes(t *testing.T) {
	t.Parallel()

	got := hintsFor(t, inlaySource, wholeDocument())

	var labels []string
	for _, h := range got {
		labels = append(labels, h.Label)
	}
	// The item type is the checker's own answer, the one hover gives.
	assert.Contains(t, labels, "item: string")
	// A task step's outputs are the descriptor's, with their types.
	var fetch *inlayHint
	for i, h := range got {
		if strings.HasPrefix(h.Label, "→ ") && h.Position.Line == 2 {
			fetch = &got[i]
		}
	}
	require.NotNil(t, fetch, "no output hint after the http step's id: %v", labels)
	assert.Contains(t, fetch.Label, ": ")
	assert.Equal(t, inlayHintKindType, fetch.Kind)

	// The hint sits at the end of what it annotates.
	idEnd := positionOf(t, inlaySource, "id: fetch", len("id: fetch"))
	assert.Equal(t, idEnd, fetch.Position)
}

func TestInlayHintsHonorVisibleRange(t *testing.T) {
	t.Parallel()

	// Only the first step is visible: the loop's item hint, on line 8, must not
	// be built.
	got := hintsFor(t, inlaySource, lsp.Range{End: lsp.Position{Line: 5, Character: 100}})
	require.NotEmpty(t, got)
	for _, h := range got {
		assert.LessOrEqual(t, h.Position.Line, 5, "hint outside the range: %+v", h)
		assert.NotEqual(t, "item: string", h.Label)
	}

	// A range holding no annotated position yields none.
	assert.Empty(t, hintsFor(t, inlaySource, lsp.Range{
		Start: lsp.Position{Line: 14}, End: lsp.Position{Line: 14, Character: 5},
	}))
}

func TestInlayHintsFailClosed(t *testing.T) {
	t.Parallel()

	got := hintsFor(t, "name: x\nsteps:\n  - id: a\n    log: [unclosed\n", wholeDocument())
	assert.NotNil(t, got, "the wire answer is [] not null")
	assert.Empty(t, got)

	// Partial documents never panic and stay quiet where they cannot be typed.
	for _, src := range []string{
		"", "steps:", "steps:\n  - id:", "steps:\n  - id: a\n    for_each:",
		"steps:\n  - id: a\n    for_each:\n      items:\n", "steps:\n  - id: a\n    nope:\n",
	} {
		assert.NotPanics(t, func() { hintsFor(t, src, wholeDocument()) }, src)
	}

	// Past the size bound there is no parsed model, so no hints.
	big := inlaySource + "# " + strings.Repeat("x", maxDocumentBytes)
	assert.Empty(t, hintsFor(t, big, wholeDocument()))

	// A test document is not a workflow.
	doc := newDocument("file:///x.test.yaml", 1, inlaySource, nil)
	assert.Empty(t, inlayHints(doc, wholeDocument()))
}

func TestInlayHintsAreCapped(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("name: many\nsteps:\n")
	for i := range maxInlayHints + 50 {
		fmt.Fprintf(&b, "  - id: s%d\n    http:\n      url: https://example.com\n", i)
	}
	b.WriteString("edition: v2026.4\n")

	got := hintsFor(t, b.String(), wholeDocument())
	assert.Len(t, got, maxInlayHints)
}

func TestInlayHintOverTheWire(t *testing.T) {
	t.Parallel()

	const uri = "file:///hints.yaml"

	p := newRawPeer(t)
	p.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
	p.receive()
	p.send(map[string]any{"method": "textDocument/didOpen", "params": map[string]any{
		"textDocument": map[string]any{"uri": uri, "languageId": "yaml", "version": 1, "text": inlaySource},
	}})
	p.send(map[string]any{"id": 2, "method": "textDocument/inlayHint", "params": map[string]any{
		"textDocument": map[string]any{"uri": uri},
		"range":        map[string]any{"start": map[string]any{"line": 0, "character": 0}, "end": map[string]any{"line": 50, "character": 0}},
	}})
	for {
		resp := p.receive()
		if resp["id"] != float64(2) {
			continue // diagnostics
		}
		result, ok := resp["result"].([]any)
		require.True(t, ok, "result is a list, got %v", resp)
		assert.NotEmpty(t, result)
		return
	}
}

func TestInlayHintRangeIsEndExclusive(t *testing.T) {
	t.Parallel()

	full := hintsFor(t, inlaySource, wholeDocument())
	require.NotEmpty(t, full)
	at := full[0].Position

	atStart := hintsFor(t, inlaySource, lsp.Range{Start: at, End: lsp.Position{Line: 1 << 20}})
	assert.Contains(t, hintPositions(atStart), at, "a hint at range.start is included")

	atEnd := hintsFor(t, inlaySource, lsp.Range{End: at})
	assert.NotContains(t, hintPositions(atEnd), at, "a hint at range.end is excluded")
}

func hintPositions(hs []inlayHint) []lsp.Position {
	var out []lsp.Position
	for _, h := range hs {
		out = append(out, h.Position)
	}
	return out
}

func TestInlayHintsBuildTheCheckerTableOnce(t *testing.T) {
	t.Parallel()

	src := "name: loops\nsteps:\n"
	for i := range 5 {
		src += fmt.Sprintf("  - id: l%d\n    for_each:\n      items: \"${['a']}\"\n      steps:\n        - id: b%d\n          log:\n            message: ${item}\n", i, i)
	}
	src += "edition: v2026.4\n"

	builds := 0
	counting := func(wf *v1.Workflow) func(string, string) (string, bool) {
		builds++
		return flowfile.IteratorTyper(wf)
	}
	got := inlayHintsWith(newDocument("file:///loops.yaml", 1, src, nil), wholeDocument(), counting)

	items := 0
	for _, h := range got {
		if h.Label == "item: string" {
			items++
		}
	}
	assert.Equal(t, 5, items, "every loop is hinted")
	assert.Equal(t, 1, builds, "one table for the request, not one per loop")

	// Loops the checker cannot type produce no hint, and must not each cost a
	// scan: the table is still built once.
	unresolved := "name: loops\nsteps:\n"
	for i := range 300 {
		unresolved += fmt.Sprintf("  - id: l%d\n    for_each:\n      items: \"${inputs.missing}\"\n      steps:\n        - id: b%d\n          log:\n            message: ${item}\n", i, i)
	}
	builds = 0
	got = inlayHintsWith(newDocument("file:///unresolved.yaml", 1, unresolved, nil), wholeDocument(), counting)
	assert.LessOrEqual(t, builds, 1)
	for _, h := range got {
		assert.NotContains(t, h.Label, "item:", "an untypable item gets no hint")
	}

	// No loop in range, no table at all.
	builds = 0
	inlayHintsWith(newDocument("file:///loops.yaml", 1, src, nil), lsp.Range{End: lsp.Position{Line: 2}}, counting)
	assert.Zero(t, builds)
}
