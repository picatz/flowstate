package lsp

import (
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// misspelledKeySource carries one misspelled step key and nothing else wrong, so
// the only quickfix in play is the one under test.
const misspelledKeySource = `edition: v2026.3
name: quickfix
steps:
  - id: a
    timeou: 5s
    log:
      message: hi
`

// TestCodeActionEditMatchesTheDiagnosticsSuggestedEdit is the anti-drift test
// for this feature, and it is the reason the action is derived from the
// validator's diagnostic rather than recomputed here.
//
// Two surfaces answer the same question about the same file: `flow validate
// --output json` hands a program a [v1.SourceRange], and the language server
// hands an editor an [lsp.TextEdit]. They are in different coordinate systems,
// so they cannot be compared as values. What can be compared is what they *do*:
// splice each into the document by its own rules, and the bytes must be
// identical. An editor and an agent repairing the same key must not produce two
// different files.
//
// The range is checked as well as the result, because two ranges can produce the
// same bytes on this fixture and differ on another. Being ASCII, its UTF-16
// columns and its code point columns coincide, which is what lets the expected
// range be written out here without borrowing the conversion under test.
func TestCodeActionEditMatchesTheDiagnosticsSuggestedEdit(t *testing.T) {
	t.Parallel()

	const uri = "file:///quickfix.yaml"
	c := newClient(t)
	c.initialize()
	published := c.open(uri, misspelledKeySource)

	// What the JSON surface carries for this file, read from the same function
	// the CLI and the Validate RPC read.
	schemaEdit, schemaMessage := suggestedEditFor(t, misspelledKeySource, `unknown key "timeou"`)
	change := schemaEdit.GetChanges()[0]

	actions := c.codeAction(uri, wholeOf(misspelledKeySource), []lsp.CodeActionKind{lsp.CAKQuickFix}, published.Diagnostics)
	action := actionTitled(t, actions, schemaEdit.GetTitle())

	require.Len(t, action.Edit.Changes[uri], 1, "a rename is one narrow edit")
	got := action.Edit.Changes[uri][0]

	assert.Equal(t, lsp.Range{
		Start: lsp.Position{
			Line:      int(change.GetRange().GetStartLine()) - 1,
			Character: int(change.GetRange().GetStartColumn()) - 1,
		},
		End: lsp.Position{
			Line:      int(change.GetRange().GetEndLine()) - 1,
			Character: int(change.GetRange().GetEndColumn()) - 1,
		},
	}, got.Range, "the action covers a different region than the schema range names")
	assert.Equal(t, change.GetNewText(), got.NewText)

	assert.Equal(t,
		applySourceRange(t, misspelledKeySource, change),
		applyEdit(t, uri, misspelledKeySource, action.Edit),
		"an editor and an agent repairing the same key produced different files")

	// The action names the problem it answers, which is what hangs the
	// lightbulb off that squiggle rather than only off the menu.
	require.Len(t, action.Diagnostics, 1)
	assert.Equal(t, schemaMessage, action.Diagnostics[0].Message)

	// And the narrow edit is genuinely narrow: it must not be the whole-document
	// migration wearing a different title.
	assert.NotEqual(t, wholeDocumentRange(newDocument(uri, 1, misspelledKeySource, nil)), got.Range)
}

// TestNoCodeActionWhereTheDiagnosticCarriesNoEdit is the same boundary from the
// other side: a problem with no suggested edit draws no quickfix of this kind,
// however fixable it looks to a reader.
//
// The document below has a key the validator refuses to suggest an edit for,
// because the suggestion is already written, and it must not acquire one on the
// way through this package. An action invented here would be exactly the
// recomputation the design refuses.
func TestNoCodeActionWhereTheDiagnosticCarriesNoEdit(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: quickfix
steps:
  - id: a
    timeou: 5s
    timeout: 10s
    log:
      message: hi
`

	const uri = "file:///noedit.yaml"
	c := newClient(t)
	c.initialize()
	published := c.open(uri, src)
	require.NotEmpty(t, published.Diagnostics, "the key is still reported")

	for _, action := range c.codeAction(uri, wholeOf(src), []lsp.CodeActionKind{lsp.CAKQuickFix}, published.Diagnostics) {
		assert.NotContains(t, action.Title, "rename to", "a rename this server invented")
	}
}

// suggestedEditFor returns the edit the schema message carries for the
// diagnostic whose message contains want, alongside that message.
func suggestedEditFor(t *testing.T, src, want string) (*v1.SuggestedEdit, string) {
	t.Helper()

	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		var compiled flowfile.Diagnostics
		require.ErrorAs(t, err, &compiled)
		ds = append(ds, compiled...)
	}

	for _, d := range ds.Report("test.yaml").GetDiagnostics() {
		if !strings.Contains(d.GetMessage(), want) {
			continue
		}
		require.Len(t, d.GetEdits(), 1)
		require.Len(t, d.GetEdits()[0].GetChanges(), 1)
		return d.GetEdits()[0], d.GetMessage()
	}
	t.Fatalf("no diagnostic carrying an edit for %s", want)
	return nil, ""
}

// actionTitled returns the single action with a title, failing when there is not
// exactly one.
func actionTitled(t *testing.T, actions []codeAction, title string) codeAction {
	t.Helper()
	var found []codeAction
	for _, a := range actions {
		if a.Title == title {
			found = append(found, a)
		}
	}
	require.Len(t, found, 1, "expected exactly one action titled %q", title)
	require.Equal(t, lsp.CAKQuickFix, found[0].Kind)
	require.NotNil(t, found[0].Edit)
	return found[0]
}

// applySourceRange splices a change into text using the schema's own units: a
// 1-based line and a 1-based code point column.
//
// Written out rather than borrowing [lineIndex], because the point of the
// comparison is that the two surfaces agree without sharing the conversion.
func applySourceRange(t *testing.T, text string, change *v1.TextChange) string {
	t.Helper()

	r := change.GetRange()
	start := byteOffsetOfYAML(t, text, int(r.GetStartLine()), int(r.GetStartColumn()))
	end := byteOffsetOfYAML(t, text, int(r.GetEndLine()), int(r.GetEndColumn()))
	require.LessOrEqual(t, start, end)
	return text[:start] + change.GetNewText() + text[end:]
}

func byteOffsetOfYAML(t *testing.T, text string, line, column int) int {
	t.Helper()
	require.Positive(t, line)
	require.Positive(t, column)

	offset := 0
	for range line - 1 {
		next := strings.IndexByte(text[offset:], '\n')
		require.GreaterOrEqual(t, next, 0, "line %d is past the end", line)
		offset += next + 1
	}
	seen := 0
	for i := range text[offset:] {
		if seen == column-1 {
			return offset + i
		}
		seen++
	}
	return len(text)
}
