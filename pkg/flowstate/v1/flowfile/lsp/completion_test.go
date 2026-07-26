package lsp

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Completion is tested with the cursor marked in the source by "|", because that
// is how the case reads in an editor and because a document being completed is
// usually mid-edit and therefore invalid — which is exactly the state that must
// work.
func TestCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// src contains a single "|" marking the cursor, which is removed before
		// the document is sent.
		src string
		// want are labels the result must contain, in this relative order.
		want []string
		// notWant are labels the result must not contain.
		notWant []string
		// exact, when set, requires the labels to be exactly this list.
		exact []string
		// detailContains maps a label to a substring its detail must contain.
		detailContains map[string]string
	}{
		{
			name: "task names after name inside a task block",
			src: `name: c
steps:
  - id: a
    task:
      name: |
`,
			exact: v1.TaskNames(),
			detailContains: map[string]string{
				"echo": "Return the given message unchanged.",
				"http": "Perform an HTTP request",
			},
		},
		{
			name: "task names filtered by what is typed",
			src: `name: c
steps:
  - id: a
    task:
      name: ht|
`,
			exact: []string{"http"},
		},
		{
			name: "input keys come from the task's schema, required first",
			src: `name: c
steps:
  - id: a
    task:
      name: http
      inputs:
        |
`,
			// url is the only required input, so it sorts ahead of the rest.
			want: []string{"url", "method", "headers", "body", "outputs"},
			detailContains: map[string]string{
				"url":     "string (required)",
				"headers": "map[string, string]",
				"outputs": "map[string, any]",
			},
		},
		{
			name: "input keys exclude ones already written",
			src: `name: c
steps:
  - id: a
    task:
      name: http
      inputs:
        url: https://example.com
        method: GET
        |
`,
			want:    []string{"headers", "body", "outputs"},
			notWant: []string{"url", "method"},
		},
		{
			name: "input keys of a different task are not offered",
			src: `name: c
steps:
  - id: a
    task:
      name: echo
      inputs:
        |
`,
			exact: []string{"message"},
		},
		{
			name: "no input keys for a task that is not registered",
			src: `name: c
steps:
  - id: a
    task:
      name: shell
      inputs:
        |
`,
			exact: []string{},
		},
		{
			name: "earlier step ids inside an expression",
			src: `name: c
steps:
  - id: first
    task:
      name: echo
      inputs:
        message: one
  - id: second
    task:
      name: echo
      inputs:
        message: two
  - id: third
    task:
      name: echo
      inputs:
        message: ${|}
  - id: fourth
    task:
      name: echo
      inputs:
        message: four
`,
			// Only steps that will have run. Offering `third` or `fourth` would
			// be offering a workflow the engine refuses.
			exact:   []string{"second", "first"},
			notWant: []string{"third", "fourth"},
		},
		{
			name: "step ids are offered nearest first",
			src: `name: c
steps:
  - id: alpha
    task:
      name: echo
      inputs:
        message: one
  - id: beta
    task:
      name: echo
      inputs:
        message: two
  - id: gamma
    task:
      name: echo
      inputs:
        message: ${|}
`,
			exact: []string{"beta", "alpha"},
		},
		{
			name: "step outputs after a dot come from the producing task",
			src: `name: c
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: out
    task:
      name: echo
      inputs:
        message: ${web.|}
`,
			// Derived from the task's Outputs descriptor rather than listed here,
			// so an output added to the schema appears in completion without this
			// test — or the completion code — being touched. Hardcoding the list
			// meant this went red the moment the http task grew a `json` output.
			exact: taskOutputNames(t, "http"),
			detailContains: map[string]string{
				"status_code": "int",
				"body":        "string",
			},
		},
		{
			name: "step outputs filtered by what is typed",
			src: `name: c
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: out
    task:
      name: echo
      inputs:
        message: ${web.st|}
`,
			exact: []string{"status_code"},
		},
		{
			name: "no outputs offered for a later step",
			src: `name: c
steps:
  - id: out
    task:
      name: echo
      inputs:
        message: ${web.|}
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
`,
			exact: []string{},
		},
		{
			name: "step references inside a function call",
			src: `name: c
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: out
    task:
      name: echo
      inputs:
        message: ${string(we|)}
`,
			exact: []string{"web"},
		},
		{
			name: "cel libraries inside a flow list",
			src: `name: c
steps:
  - id: a
    task:
      name: cel
      inputs:
        libs: [|]
        expr: "1"
`,
			exact: v1.ExtensionLibraries(),
			detailContains: map[string]string{
				"json": "Parse a JSON string",
			},
		},
		{
			name: "cel libraries exclude ones already enabled",
			src: `name: c
steps:
  - id: a
    task:
      name: cel
      inputs:
        libs: [json, |]
        expr: "1"
`,
			notWant: []string{"json"},
			want:    []string{"math", "strings"},
		},
		{
			name: "cel libraries in a block list",
			src: `name: c
steps:
  - id: a
    task:
      name: cel
      inputs:
        expr: "1"
        libs:
          - ma|
`,
			exact: []string{"math"},
		},
		{
			name:  "top level document keys",
			src:   `|`,
			exact: []string{"name", "description", "steps"},
		},
		{
			name: "step keys",
			src: `name: c
steps:
  - |
`,
			exact: []string{"id", "task", "for_each", "parallel", "if", "timeout", "retry", "continue_on_error"},
		},
		{
			name: "input keys inside a for_each body",
			// A nested step is where an author is just as likely to want a
			// suggestion, so the line scan descends into nested steps: blocks.
			src: `name: c
steps:
  - id: loop
    for_each:
      items: ${x}
      steps:
        - id: body
          task:
            name: http
            inputs:
              |
`,
			want: []string{"url", "method", "headers"},
		},
		{
			name: "input keys inside a parallel branch",
			src: `name: c
steps:
  - id: branches
    parallel:
      - steps:
          - id: left
            task:
              name: echo
              inputs:
                |
`,
			exact: []string{"message"},
		},
		{
			name: "step references inside a loop body see earlier steps",
			src: `name: c
steps:
  - id: outer
    task:
      name: echo
      inputs:
        message: hi
  - id: loop
    for_each:
      items: ${outer.result}
      steps:
        - id: body
          task:
            name: echo
            inputs:
              message: ${|
`,
			// Nearest first: the loop's iterator, then the step above the loop.
			// The enclosing loop is excluded — it has not finished, so it has no
			// results yet — and so is the body step itself.
			exact:   []string{"item", "outer"},
			notWant: []string{"body", "loop"},
			detailContains: map[string]string{
				"item": "loop item",
			},
		},
		{
			name: "for_each keys",
			src: `name: c
steps:
  - id: a
    for_each:
      |
`,
			exact: []string{"items", "iterator", "max_parallel", "steps"},
		},
		{
			name: "retry keys",
			src: `name: c
steps:
  - id: a
    retry:
      |
`,
			exact: []string{"attempts", "interval", "backoff", "max_interval"},
		},
		{
			name: "task keys",
			src: `name: c
steps:
  - id: a
    task:
      |
`,
			exact: []string{"name", "description", "inputs"},
		},
		{
			name: "nothing to complete in a literal value",
			src: `name: c
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hello |
`,
			exact: []string{},
		},
		{
			name: "nothing to complete for a step id",
			src: `name: c
steps:
  - id: |
`,
			exact: []string{},
		},
	}

	c := newClient(t)
	c.initialize()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, pos := splitCursor(t, tt.src)
			uri := "file:///completion-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
			c.open(uri, src)

			got := c.complete(uri, pos.Line, pos.Character)
			require.NotNil(t, got.Items, "items must be an array, never null")
			gotLabels := labels(got.Items)

			if tt.exact != nil {
				assert.Equal(t, tt.exact, gotLabels)
			}
			for _, want := range tt.want {
				assert.Contains(t, gotLabels, want)
			}
			for _, notWant := range tt.notWant {
				assert.NotContains(t, gotLabels, notWant)
			}
			// Relative order of the wanted labels.
			if len(tt.want) > 1 {
				assert.Equal(t, tt.want, filterLabels(gotLabels, tt.want))
			}
			for label, substr := range tt.detailContains {
				item := findItem(got.Items, label)
				require.NotNil(t, item, "no candidate labeled %q", label)
				assert.Contains(t, item.Detail, substr)
			}
		})
	}
}

// TestCompletionReplacesThePartialWord checks the text edit each candidate carries.
// Without an explicit range an editor guesses at the word boundary, and inserting
// alongside a partial word instead of replacing it produces "htthttp".
func TestCompletionReplacesThePartialWord(t *testing.T) {
	t.Parallel()

	src, pos := splitCursor(t, `name: c
steps:
  - id: a
    task:
      name: ht|
`)
	c := newClient(t)
	c.initialize()
	c.open("file:///replace.yaml", src)

	got := c.complete("file:///replace.yaml", pos.Line, pos.Character)
	require.Len(t, got.Items, 1)
	edit := got.Items[0].TextEdit
	require.NotNil(t, edit, "a candidate must say what it replaces")
	assert.Equal(t, "http", edit.NewText)
	assert.Equal(t, "ht", textInRange(src, edit.Range))
}

// TestCompletionWorksOnUnparseableDocument is the case that matters most in
// practice: a half-typed key is not valid YAML, and that is when completion is
// asked for.
func TestCompletionWorksOnUnparseableDocument(t *testing.T) {
	t.Parallel()

	src, pos := splitCursor(t, `name: c
steps:
  - id: a
    task:
      name: echo
      inputs:
        mes|
`)
	// Confirm the premise: this document does not compile.
	require.NotEmpty(t, diagnose(newDocument("file:///x", 1, src)),
		"premise: the half-typed document should have problems")

	c := newClient(t)
	c.initialize()
	c.open("file:///partial.yaml", src)

	got := c.complete("file:///partial.yaml", pos.Line, pos.Character)
	assert.Equal(t, []string{"message"}, labels(got.Items))
}

// TestCompletionUsesUTF16Columns checks that a partial word after non-ASCII text is
// located and replaced correctly.
func TestCompletionUsesUTF16Columns(t *testing.T) {
	t.Parallel()

	// The partial word sits after an emoji on the same line, so a server counting
	// bytes or code points would replace the wrong span.
	const cursorLine = "          X-🙂: ${fi"
	src := "name: ünïcödé wörkflöw\n" +
		"steps:\n" +
		"  - id: first\n" +
		"    task:\n" +
		"      name: echo\n" +
		"      inputs:\n" +
		"        message: hi\n" +
		"  - id: second\n" +
		"    task:\n" +
		"      name: http\n" +
		"      inputs:\n" +
		"        url: https://example.com\n" +
		"        headers:\n" +
		cursorLine + "\n"

	require.NotEqual(t, len(cursorLine), utf16Len(cursorLine),
		"premise: the cursor's line must contain non-ASCII")

	c := newClient(t)
	c.initialize()
	c.open("file:///unicode-completion.yaml", src)

	got := c.complete("file:///unicode-completion.yaml", 13, utf16Len(cursorLine))
	require.Len(t, got.Items, 1)
	assert.Equal(t, "first", got.Items[0].Label)
	require.NotNil(t, got.Items[0].TextEdit)
	assert.Equal(t, "fi", textInRange(src, got.Items[0].TextEdit.Range))
}

// splitCursor removes the "|" cursor marker from a source and returns the position
// it marked.
func splitCursor(t *testing.T, src string) (string, lsp.Position) {
	t.Helper()
	at := strings.Index(src, "|")
	require.GreaterOrEqual(t, at, 0, "test source has no | cursor marker")
	require.Equal(t, at, strings.LastIndex(src, "|"), "test source has more than one | marker")

	clean := src[:at] + src[at+1:]
	return clean, newLineIndex(clean).positionOfOffset(at)
}

// findItem returns the candidate with the given label.
func findItem(items []lsp.CompletionItem, label string) *lsp.CompletionItem {
	for i := range items {
		if items[i].Label == label {
			return &items[i]
		}
	}
	return nil
}

// filterLabels returns the elements of got that appear in keep, in got's order.
func filterLabels(got, keep []string) []string {
	wanted := make(map[string]bool, len(keep))
	for _, k := range keep {
		wanted[k] = true
	}
	out := make([]string, 0, len(keep))
	for _, g := range got {
		if wanted[g] {
			out = append(out, g)
		}
	}
	return out
}

// taskOutputNames returns the output names a registered task declares, in schema
// order, which is the order completion offers them in.
func taskOutputNames(t *testing.T, task string) []string {
	t.Helper()
	def, ok := v1.LookupTask(task)
	require.True(t, ok, "task %q is not registered", task)
	names := fieldNames(def.Outputs)
	require.NotEmpty(t, names, "task %q declares no outputs", task)
	return names
}
