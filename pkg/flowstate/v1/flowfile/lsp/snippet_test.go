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
	snippetizeTasks(list, tasks)

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
	snippetizeTasks(list, v1.DefaultRegistry())
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
