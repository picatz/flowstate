package lsp

import (
	"slices"
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const refsSource = `edition: v2026.4
name: refs
steps:
  - id: web
    http:
      url: https://example.com
  - id: status
    if: ${steps.web.status_code == 200}
    log:
      message: ${string(steps.web.status_code)} and ${"steps.web is text"}
  - id: other
    log:
      message: ${steps.status.result}
outputs:
  code:
    value: ${steps.web.status_code}
`

func refsDoc(t *testing.T, src string) *document {
	t.Helper()
	doc := newDocument("file:///refs.yaml", 1, src, nil)
	require.NotNil(t, doc.parsed, "the fixture must parse: %v", doc.parseErr)
	return doc
}

// applyEdits applies a WorkspaceEdit's edits for the one document, last first so
// earlier offsets stay valid.
func applyEdits(t *testing.T, src string, edits []lsp.TextEdit) string {
	t.Helper()
	ix := newLineIndex(src)
	edits = slices.Clone(edits)
	slices.SortFunc(edits, func(a, b lsp.TextEdit) int {
		return ix.offsetOfPosition(b.Range.Start) - ix.offsetOfPosition(a.Range.Start)
	})
	for _, e := range edits {
		start, end := ix.offsetOfPosition(e.Range.Start), ix.offsetOfPosition(e.Range.End)
		src = src[:start] + e.NewText + src[end:]
	}
	return src
}

func TestReferencesFindEveryReadOfAStepID(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, refsSource)

	// From the declaration, and from a reference: the same answer.
	for name, pos := range map[string]lsp.Position{
		"declaration": positionOf(t, refsSource, "id: web", len("id: ")),
		"reference":   positionOf(t, refsSource, "steps.web.status_code == 200", len("steps.")+1),
		"output name": positionOf(t, refsSource, "steps.web.status_code == 200", len("steps.web.")+2),
	} {
		got := referencesAt(doc, pos, true)
		require.Len(t, got, 4, name)
		for _, l := range got {
			assert.Equal(t, "web", textInRange(refsSource, l.Range), name)
		}
		assert.Equal(t, 3, got[0].Range.Start.Line, "%s: the declaration comes first", name)

		without := referencesAt(doc, pos, false)
		assert.Len(t, without, 3, "%s: includeDeclaration=false drops the id", name)
	}

	// `"steps.web is text"` is a string literal, not a reference.
	text := positionOf(t, refsSource, "steps.web is text", len("steps."))
	assert.Empty(t, referencesAt(doc, text, true), "the cursor is in a string, on no step")
}

func TestHighlightMarksTheDeclarationAsAWrite(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, refsSource)

	got := highlightsAt(doc, positionOf(t, refsSource, "steps.status.result", len("steps.")))
	require.Len(t, got, 2)
	assert.Equal(t, lsp.Write, got[0].Kind)
	assert.Equal(t, lsp.Read, got[1].Kind)
}

func TestPrepareRenameSelectsOnlyTheID(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, refsSource)

	rng, placeholder, ok := prepareRenameAt(doc, positionOf(t, refsSource, "steps.web.status_code == 200", len("steps.")+1))
	require.True(t, ok)
	assert.Equal(t, "web", textInRange(refsSource, rng))
	assert.Equal(t, "web", placeholder)

	for name, pos := range map[string]lsp.Position{
		"the steps root":  positionOf(t, refsSource, "steps.web.status_code == 200", 2),
		"the output name": positionOf(t, refsSource, "steps.web.status_code == 200", len("steps.web.")+2),
		"a plain value":   positionOf(t, refsSource, "https://example.com", 3),
	} {
		_, _, ok := prepareRenameAt(doc, pos)
		assert.False(t, ok, name)
	}
}

func TestRenameChangesTheDeclarationAndEveryReference(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, refsSource)

	edit, err := renameAt(doc, positionOf(t, refsSource, "id: web", len("id: ")), "fetch")
	require.NoError(t, err)
	got := applyEdits(t, refsSource, edit.Changes["file:///refs.yaml"])

	want := strings.NewReplacer("id: web", "id: fetch", "steps.web.status_code", "steps.fetch.status_code").Replace(refsSource)
	assert.Equal(t, want, got)
	assert.Contains(t, got, `"steps.web is text"`, "a string literal is text, not a reference")

	// The renamed file is still a Flowfile whose references resolve.
	renamed := refsDoc(t, got)
	assert.Len(t, referencesAt(renamed, positionOf(t, got, "id: fetch", len("id: ")), true), 4)
}

func TestRenameKeepsAQuotedIDQuoted(t *testing.T) {
	t.Parallel()
	const src = `name: q
steps:
  - id: "web"
    http:
      url: https://example.com
  - id: next
    log:
      message: ${steps.web.status_code}
`
	doc := refsDoc(t, src)
	edit, err := renameAt(doc, positionOf(t, src, `"web"`, 2), "fetch")
	require.NoError(t, err)
	got := applyEdits(t, src, edit.Changes["file:///refs.yaml"])
	assert.Contains(t, got, `id: "fetch"`)
	assert.Contains(t, got, "steps.fetch.status_code")

	// A name YAML would read as a boolean is quoted so it stays a string.
	for _, name := range []string{"yes", "True", "NULL"} {
		edit, err = renameAt(doc, positionOf(t, src, `"web"`, 2), name)
		require.NoError(t, err, name)
		got = applyEdits(t, src, edit.Changes["file:///refs.yaml"])
		assert.Contains(t, got, `id: "`+name+`"`, name)
	}
}

func TestRenameRefusesWhatWouldLeaveAStaleReference(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, refsSource)
	at := positionOf(t, refsSource, "id: web", len("id: "))

	for name, newName := range map[string]string{
		"not an identifier":     "my-step",
		"a root":                "inputs",
		"a CEL keyword":         "null",
		"empty":                 "",
		"another step's id":     "status",
		"starts with a digit":   "1st",
		"carries a path":        "a.b",
		"carries an expression": "x}",
	} {
		_, err := renameAt(doc, at, newName)
		assert.Error(t, err, name)
	}

	// An expression the model does not walk blocks the rename instead of leaving
	// a broken reference behind.
	const mentioned = `name: m
steps:
  - id: web
    description: feeds ${steps.web.status_code} downstream
    http:
      url: https://example.com
  - id: next
    log:
      message: ${steps.web.status_code}
`
	md := refsDoc(t, mentioned)
	_, err := renameAt(md, positionOf(t, mentioned, "id: web", len("id: ")), "fetch")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not track")

	// A comment is not a mention.
	const commented = `name: c
steps:
  - id: web
    http:
      url: https://example.com
  # reads steps.web.status_code below
  - id: next
    log:
      message: ${steps.web.status_code}
`
	cd := refsDoc(t, commented)
	_, err = renameAt(cd, positionOf(t, commented, "id: web", len("id: ")), "fetch")
	assert.NoError(t, err)
}

func TestRenameOfSameNameIsNoEdit(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, refsSource)
	edit, err := renameAt(doc, positionOf(t, refsSource, "id: web", len("id: ")), "web")
	require.NoError(t, err)
	assert.Empty(t, edit.Changes)
}

// Two sibling loops may each declare a body step of the same id; the references
// in one loop's `until:` belong to that loop's step and renaming it must not
// touch the other's.
func TestRenameStaysInsideItsScope(t *testing.T) {
	t.Parallel()
	const src = `name: scoped
steps:
  - id: first
    loop:
      as: n
      init: 0
      until: ${steps.page.status_code == 200}
      update: ${n + 1}
      steps:
        - id: page
          http:
            url: https://example.com
  - id: second
    loop:
      as: n
      init: 0
      until: ${steps.page.status_code == 404}
      update: ${n + 1}
      steps:
        - id: page
          http:
            url: https://example.org
`
	doc := refsDoc(t, src)
	refs := referencesAt(doc, positionOf(t, src, "id: page", len("id: ")), true)
	require.Len(t, refs, 2)
	assert.Equal(t, []int{6, 9}, []int{refs[0].Range.Start.Line, refs[1].Range.Start.Line},
		"the first loop's until: and its body step, not the second loop's")

	edit, err := renameAt(doc, positionOf(t, src, "id: page", len("id: ")), "poll")
	require.NoError(t, err)
	got := applyEdits(t, src, edit.Changes["file:///refs.yaml"])
	assert.Equal(t, 1, strings.Count(got, "id: poll"))
	assert.Equal(t, 1, strings.Count(got, "id: page"), "the second loop's step keeps its name")
	assert.Contains(t, got, "steps.poll.status_code == 200")
	assert.Contains(t, got, "steps.page.status_code == 404")
}

func TestStepRefsInIgnoresStringsAndFieldSelects(t *testing.T) {
	t.Parallel()
	for src, want := range map[string][]string{
		`steps.a.out + steps.b.out`:       {"a", "b"},
		`"steps.a.out"`:                   nil,
		`'it''s' + steps.a.out`:           {"a"},
		`x.steps.a.out`:                   nil,
		`mysteps.a.out`:                   nil,
		`size(steps.a.items) > 0`:         {"a"},
		`"unterminated steps.a.out`:       nil,
		`r"raw \" steps.a" + steps.b.out`: {"b"},
		`"""steps.a""" + steps.b.out`:     {"b"},
	} {
		var got []string
		for _, r := range stepRefsIn(src) {
			got = append(got, r.step)
		}
		assert.Equal(t, want, got, src)
	}
}

// TestReferencesAndRenameOverTheWire proves the capabilities are routed, with
// the options-form rename provider and a refusal carried as an error.
func TestReferencesAndRenameOverTheWire(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///refs.yaml"
	c.open(uri, refsSource)

	pos := positionOf(t, refsSource, "id: web", len("id: "))
	var refs []lsp.Location
	require.NoError(t, c.conn.Call(t.Context(), "textDocument/references", lsp.ReferenceParams{
		TextDocumentPositionParams: lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: uri}, Position: pos,
		},
		Context: lsp.ReferenceContext{IncludeDeclaration: true},
	}, &refs))
	assert.Len(t, refs, 4)

	var prepared prepareRenameResult
	require.NoError(t, c.conn.Call(t.Context(), "textDocument/prepareRename", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri}, Position: pos,
	}, &prepared))
	assert.Equal(t, "web", prepared.Placeholder)

	var edit lsp.WorkspaceEdit
	require.NoError(t, c.conn.Call(t.Context(), "textDocument/rename", lsp.RenameParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri}, Position: pos, NewName: "fetch",
	}, &edit))
	assert.Len(t, edit.Changes[uri], 4)

	err := c.conn.Call(t.Context(), "textDocument/rename", lsp.RenameParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri}, Position: pos, NewName: "inputs",
	}, &edit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inputs")
}
