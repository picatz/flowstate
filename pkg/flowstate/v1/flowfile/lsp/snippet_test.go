package lsp

import (
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestSnippetizeTasksAddsRequiredInputsAsTabstops(t *testing.T) {
	t.Parallel()
	tasks := v1.DefaultRegistry()
	replace := lsp.Range{}
	list := &lsp.CompletionList{Items: taskCandidates("", replace, tasks)}
	snippetizeTasks(refsDoc(t, "edition: v2026.4\nname: s\nsteps:\n  - id: a\n    log:\n      message: x\n"), list, tasks)

	var http *lsp.CompletionItem
	for i := range list.Items {
		if list.Items[i].Label == "http" {
			http = &list.Items[i]
		}
	}
	require.NotNil(t, http)
	assert.EqualValues(t, lsp.ITFSnippet, http.InsertTextFormat)
	assert.True(t, strings.HasPrefix(http.TextEdit.NewText, "http:\n  "), http.TextEdit.NewText)
	assert.Contains(t, http.TextEdit.NewText, "  url: $1")
	assert.NotContains(t, http.TextEdit.NewText, "\n\n")

	// Every snippet is well formed: numbered tabstops from 1, one input per line.
	for _, it := range list.Items {
		if it.InsertTextFormat != lsp.ITFSnippet {
			assert.Equal(t, it.Label+": ", it.TextEdit.NewText, "an item without required inputs stays plain")
			continue
		}
		lines := strings.Split(it.TextEdit.NewText, "\n")
		assert.Equal(t, it.Label+":", lines[0])
		for n, line := range lines[1:] {
			assert.Truef(t, strings.HasPrefix(line, "  ") && strings.HasSuffix(line, "$"+string(rune('1'+n))), "%s: %q", it.Label, line)
		}
	}
}

func TestSnippetizeLeavesNonTaskFunctionsAlone(t *testing.T) {
	t.Parallel()
	list := &lsp.CompletionList{Items: []lsp.CompletionItem{
		{Label: "http", Kind: lsp.CIKFunction, TextEdit: &lsp.TextEdit{NewText: "http("}},
		{Label: "id", Kind: lsp.CIKProperty, TextEdit: &lsp.TextEdit{NewText: "id: "}},
	}}
	snippetizeTasks(refsDoc(t, "edition: v2026.4\nname: s\nsteps:\n  - id: a\n    log:\n      message: x\n"), list, v1.DefaultRegistry())
	assert.Equal(t, "http(", list.Items[0].TextEdit.NewText)
	assert.Equal(t, "id: ", list.Items[1].TextEdit.NewText)
	assert.Zero(t, list.Items[0].InsertTextFormat)
}

func TestCompletionOffersSnippetsOnlyToAClientThatSupportsThem(t *testing.T) {
	t.Parallel()

	const src = "edition: v2026.4\nname: s\nsteps:\n  - id: a\n    ht\n"
	for _, supports := range []bool{false, true} {
		c := newClient(t)
		var params lsp.InitializeParams
		params.Capabilities.TextDocument.Completion.CompletionItem.SnippetSupport = supports
		var result initializeResult
		require.NoError(t, c.conn.Call(t.Context(), "initialize", params, &result))
		require.NoError(t, c.conn.Notify(t.Context(), "initialized", struct{}{}))

		uri := "file:///snip.yaml"
		c.open(uri, src)
		got := c.complete(uri, 4, len("    ht"))

		var http *lsp.CompletionItem
		for i := range got.Items {
			if got.Items[i].Label == "http" {
				http = &got.Items[i]
			}
		}
		require.NotNil(t, http, "supports=%v", supports)
		if supports {
			assert.EqualValues(t, lsp.ITFSnippet, http.InsertTextFormat)
			assert.Contains(t, http.TextEdit.NewText, "url: $1")
		} else {
			assert.Equal(t, "http: ", http.TextEdit.NewText)
			assert.Zero(t, http.InsertTextFormat)
		}
	}
}

// A task written as a step's first key sits after the list marker, so its inputs
// must be indented past the dash, not under it. Expanded the way a client does
// (continuation lines get the line's leading whitespace), the result must parse
// as the step the author meant.
func TestSnippetAfterAListMarkerNestsUnderTheTaskKey(t *testing.T) {
	t.Parallel()

	const head = "edition: v2026.4\nname: s\nsteps:\n"
	for name, tc := range map[string]struct{ line, lead string }{
		"first key after the dash": {"  - ht", "  "},
		"after a sibling key":      {"    ht", "    "},
	} {
		src := head + "  - id: a\n    log:\n      message: x\n" + tc.line + "\n"
		if tc.line == "    ht" {
			src = head + "  - id: a\n" + tc.line + "\n"
		}
		doc := refsDoc2(t, src)
		pos := lsp.Position{Line: strings.Count(src, "\n") - 1, Character: len(tc.line)}
		got := completeAt(doc, pos)
		snippetizeTasks(doc, got, v1.DefaultRegistry())

		var http *lsp.CompletionItem
		for i := range got.Items {
			if got.Items[i].Label == "http" {
				http = &got.Items[i]
			}
		}
		require.NotNil(t, http, name)

		// Expand: replace `ht` with the snippet, tabstops emptied, continuation
		// lines prefixed with the line's leading whitespace.
		text := strings.ReplaceAll(http.TextEdit.NewText, "\n", "\n"+tc.lead)
		text = strings.NewReplacer("$1", "https://example.com", "$2", "GET", "$3", "x").Replace(text)
		expanded := src[:len(src)-len("ht\n")] + text + "\n"

		d2 := newDocument("file:///x.yaml", 1, expanded, nil)
		require.NotNil(t, d2.parsed, "%s: %s\n%v", name, expanded, d2.parseErr)
		var step *parsedStep
		for _, s := range d2.parsed.steps {
			if s.taskName == "http" {
				step = s
			}
		}
		require.NotNil(t, step, "%s: http must be the step's task, not a sibling key:\n%s", name, expanded)
		var keys []string
		for _, e := range step.inputs {
			keys = append(keys, e.key)
		}
		assert.Contains(t, keys, "url", name)
	}
}

func refsDoc2(t *testing.T, src string) *document {
	t.Helper()
	// Not required to parse: the buffer being completed is mid-edit.
	return newDocument("file:///snip.yaml", 1, src, nil)
}
