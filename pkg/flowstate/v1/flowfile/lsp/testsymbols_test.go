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

// TestASymbolIsNotMintedFromTheAuthorsData is the negative direction the
// suffix match failed (Codex, #1173), and both halves of its cost: a nested
// mapping whose key spells `cases` minted a phantom runnable case, and —
// worse — set hasRows, which suppressed the real case's own symbol. The
// outline then showed a case flow test cannot run and hid one it can.
func TestASymbolIsNotMintedFromTheAuthorsData(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := "tests:\n" +
		"  - name: real\n" +
		"    inputs:\n" +
		"      cases:\n" +
		"        name: bogus\n"
	c.open("file:///suite.test.yaml", text)
	syms := c.symbols("file:///suite.test.yaml")

	names := make([]string, 0, len(syms))
	for _, s := range syms {
		names = append(names, s.Name)
	}

	assert.Equal(t, []string{"real"}, names,
		"a fixture mapping named cases minted a phantom row, suppressed the real case, or both")

	// And the entry-level collision: a fixture mapping named `tests` must not
	// mint a phantom top-level case either. Separately, because the two
	// suffix checks were two lines and each can regress alone.
	c.open("file:///nested.test.yaml",
		"tests:\n"+
			"  - name: real\n"+
			"    inputs:\n"+
			"      tests:\n"+
			"        name: phantom\n")
	syms = c.symbols("file:///nested.test.yaml")

	names = names[:0]
	for _, s := range syms {
		names = append(names, s.Name)
	}
	assert.Equal(t, []string{"real"}, names,
		"a fixture mapping named tests minted a phantom entry symbol")
}

// TestASymbolRangeIsInUTF16CodeUnits: LSP's Position.Character counts UTF-16
// code units, and a byte count past `café` ends the range beyond the text
// (Codex, #1173). The é is two bytes and one code unit, so byte arithmetic
// puts End one column too far — which is exactly the off-by-one an editor
// rejects or, worse, silently misselects.
func TestASymbolRangeIsInUTF16CodeUnits(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///suite.test.yaml", "tests:\n  - name: café\n")
	syms := c.symbols("file:///suite.test.yaml")

	require.Len(t, syms, 1)
	rng := syms[0].Location.Range
	// "  - name: café" is 14 runes; é is one UTF-16 code unit. Start sits
	// after "  - name: " (10 units), End at the line's end (14 units) — a
	// byte count would say 15.
	assert.Equal(t, 10, rng.Start.Character)
	assert.Equal(t, 14, rng.End.Character,
		"the range end is a byte count, which lands past the text for a non-ASCII name")
}

// TestASymbolNameMatchesTheReportThroughQuotesAndComments: the outline's
// name has to be the report's name, and `name: "smoke" # primary` is a
// perfectly ordinary spelling of it. The old unquote checked the line's
// *last* byte for the closing quote, so a trailing comment left the quotes
// on and the outline showed `"smoke"` while flow test says smoke
// (Codex, #1173).
func TestASymbolNameMatchesTheReportThroughQuotesAndComments(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///suite.test.yaml",
		"tests:\n"+
			"  - name: \"smoke\" # primary\n"+
			"  - name: 'quoted # not a comment'\n")
	syms := c.symbols("file:///suite.test.yaml")

	require.Len(t, syms, 2)
	assert.Equal(t, "smoke", syms[0].Name,
		"a trailing comment left the quotes on the outline's name")
	assert.Equal(t, "quoted # not a comment", syms[1].Name,
		"a # inside a quoted name was stripped as a comment")

	// And each style's own escape, because a bare first-quote scan truncated
	// an escaped quote to the text before it — `say \` for a name the report
	// prints as `say "hi"` (Codex, #1173, second round). The doubled single
	// quote is YAML's own escape for that style, decoded the same way.
	c.open("file:///escaped.test.yaml",
		"tests:\n"+
			"  - name: \"say \\\"hi\\\"\" # greeting\n"+
			"  - name: 'it''s fine'\n")
	syms = c.symbols("file:///escaped.test.yaml")

	require.Len(t, syms, 2)
	assert.Equal(t, `say "hi"`, syms[0].Name,
		"an escaped quote was read as the scalar's close, truncating the name")
	assert.Equal(t, "it's fine", syms[1].Name,
		"a doubled single quote was read as the close instead of the escape it is")
}
