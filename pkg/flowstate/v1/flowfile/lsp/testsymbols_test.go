package lsp

import (
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTestDocumentSymbolsNamesEachCase: a suite with two independent cases
// (no `cases:` rows) gets one symbol per `tests:` entry, named by its own
// `name:`.
func TestTestDocumentSymbolsNamesEachCase(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := `tests:
  - name: happy path
    expect:
      ran: [a]
  - name: sad path
    expect:
      failed: true
`
	c.open("file:///suite.test.yaml", text)
	got := c.symbols("file:///suite.test.yaml")

	require.Len(t, got, 2)
	assert.Equal(t, "happy path", got[0].Name)
	assert.Equal(t, "sad path", got[1].Name)
	for _, s := range got {
		assert.Equal(t, lsp.SKMethod, s.Kind)
		assert.Equal(t, lsp.DocumentURI("file:///suite.test.yaml"), s.Location.URI)
	}
}

// TestTestDocumentSymbolsNamesRowsNotTheTemplate: an entry declaring `cases:`
// rows does not itself run (flowtest.Test.Cases' own doc), so the outline
// must show only the rows, named `<entry name>/<row name>` — the identity
// flow test's own report uses — and not the entry's bare name.
func TestTestDocumentSymbolsNamesRowsNotTheTemplate(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := `tests:
  - name: table
    cases:
      - name: small
        inputs: {n: 1}
      - name: large
        inputs: {n: 100}
`
	c.open("file:///suite.test.yaml", text)
	got := c.symbols("file:///suite.test.yaml")

	var names []string
	for _, s := range got {
		names = append(names, s.Name)
	}
	assert.ElementsMatch(t, []string{"table/small", "table/large"}, names,
		"the template entry's own bare name must not appear alongside its rows")
}

// TestTestDocumentSymbolsMixesTemplatesAndBareCases: a file with one bare
// case and one table, in document order, produces symbols in that order —
// the outline pane and breadcrumbs read top to bottom the way the file does.
func TestTestDocumentSymbolsMixesTemplatesAndBareCases(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := `tests:
  - name: solo
    expect:
      ran: [a]
  - name: table
    cases:
      - name: row
        inputs: {}
`
	c.open("file:///suite.test.yaml", text)
	got := c.symbols("file:///suite.test.yaml")

	var names []string
	for _, s := range got {
		names = append(names, s.Name)
	}
	assert.Equal(t, []string{"solo", "table/row"}, names)
}

// TestTestDocumentSymbolsPointAtTheNameValue: a symbol's location lands on
// the case's own `name:` value, on the line the author would want to land
// on when they pick it from an outline or a breadcrumb.
func TestTestDocumentSymbolsPointAtTheNameValue(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := "tests:\n  - name: the case\n    expect:\n      ran: [a]\n"
	c.open("file:///suite.test.yaml", text)
	got := c.symbols("file:///suite.test.yaml")

	require.Len(t, got, 1)
	assert.Equal(t, 1, got[0].Location.Range.Start.Line)
	lines := []string{"tests:", "  - name: the case", "    expect:", "      ran: [a]"}
	line := lines[got[0].Location.Range.Start.Line]
	assert.Equal(t, "the case", line[got[0].Location.Range.Start.Character:got[0].Location.Range.End.Character])
}

// TestTestDocumentSymbolsEmptyOnTestDefaults: a testdefaults.yaml declares no
// `tests:`, so its outline is empty — not the diagnostic-only silence
// [document.speaksFlowfile] used to produce for every feature, but the
// honest answer that there is nothing here to name.
func TestTestDocumentSymbolsEmptyOnTestDefaults(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///testdefaults.yaml", "vars:\n  region: us-east-1\n")
	got := c.symbols("file:///testdefaults.yaml")
	assert.Empty(t, got)
}
