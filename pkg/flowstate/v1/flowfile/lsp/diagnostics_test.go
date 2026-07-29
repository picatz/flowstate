package lsp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiagnosticsOverProtocol checks the diagnostics an editor actually receives,
// including the range each one covers, by driving the server over a JSON-RPC
// connection rather than calling diagnose directly.
func TestDiagnosticsOverProtocol(t *testing.T) {
	// want describes one expected diagnostic. The range is given as the source
	// text it must cover, which is far easier to read — and to keep correct — than
	// four integers.
	type want struct {
		code     string
		severity lsp.DiagnosticSeverity
		contains string
		// underlines is the exact source text the diagnostic's range covers.
		underlines string
	}

	tests := []struct {
		name string
		src  string
		want []want
	}{
		{
			name: "valid workflow has no diagnostics",
			src: `name: valid
steps:
  - id: first
    echo:
      message: hello
  - id: second
    echo:
      message: ${steps.first.result}
`,
		},
		{
			name: "yaml syntax error lands on the offending token",
			// A tab where the indentation goes. Nothing after it is ever read —
			// the document stops parsing at the tab — but it is spelled in the
			// grammar the DSL has, so nobody reads this fixture as evidence that
			// `task:` is still a step key.
			src: "name: broken\nsteps:\n  - id: a\n  \techo: hi\n",
			want: []want{{
				code:       codeYAMLSyntax,
				severity:   lsp.Error,
				contains:   "cannot start any token",
				underlines: "\t",
			}},
		},
		{
			name: "unterminated flow sequence",
			src:  "name: broken\nsteps: [\n",
			want: []want{{
				code:     codeYAMLSyntax,
				severity: lsp.Error,
				contains: "sequence end token ']' not found",
			}},
		},
		{
			name: "cel syntax error underlines inside the expression",
			src: `name: badcel
steps:
  - id: a
    echo:
      message: ${a b}
`,
			// The compiler cannot produce a workflow from a document with an
			// unparseable expression, so its own validation never runs. The
			// precise CEL error is the entire report — not a second, positionless
			// copy of "invalid expression" at line 1.
			want: []want{{
				code:       codeCELSyntax,
				severity:   lsp.Error,
				contains:   "Syntax error",
				underlines: "b",
			}},
		},
		{
			name: "cel syntax error in a cel step's expr input",
			src: `name: badexpr
steps:
  - id: a
    cel:
      expr: "1 + + 2"
`,
			want: []want{{
				code:       codeCELSyntax,
				severity:   lsp.Error,
				contains:   "Syntax error",
				underlines: "+",
			}},
		},
		{
			name: "unknown cel library underlines the library name",
			src: `name: badlib
steps:
  - id: a
    cel:
      libs: [json, nope]
      expr: "1"
`,
			// Reported by the shared validator, so `flow validate` refuses the file
			// too — a misspelled library used to compile cleanly and fail once the
			// activity ran. The language server's contribution is the range: the
			// validator can only name `libs`, and underlining the whole list would
			// leave the reader to find which of two names is the wrong one.
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   `unknown CEL extension library "nope"`,
				underlines: "nope",
			}},
		},
		{
			name: "unknown task underlines the task name",
			src: `name: badtask
steps:
  - id: a
    shell:
      command: ls
`,
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   `unknown task "shell"`,
				underlines: "shell",
			}},
		},
		{
			name: "misspelled input suggests the declared name",
			src: `name: typo
steps:
  - id: a
    echo:
      mesage: hello
`,
			// Reported by the shared validator, so `flow validate` refuses the
			// workflow too — a misspelled input is silently ignored at run time,
			// which is exactly the mistake that should not reach a run. The
			// language server's contribution is the range: the key is what is
			// misspelled, not the value written under it.
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   `did you mean "message"?`,
				underlines: "mesage",
			}},
		},
		{
			name: "missing required input is reported once",
			src: `name: missing
steps:
  - id: a
    http:
      method: GET
`,
			want: []want{{
				code:     codeFlowfile,
				severity: lsp.Error,
				contains: `task "http" requires input "url" (a string)`,
			}},
		},
		{
			name: "a literal whose type the input cannot hold",
			src: `name: wrong-type
steps:
  - id: a
    echo:
      message: [1, 2]
`,
			// Here the key is fine and the value is not, so the range moves to the
			// value. Which of the two is at fault comes from the schema.
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   "expected a string, but this is a list",
				underlines: "1, 2",
			}},
		},
		{
			name: "step that does nothing",
			src: `name: notask
steps:
  - id: a
`,
			// Reported by the shared validator, not here: a rule about what a
			// step must be belongs with the compiler that enforces it. Only the
			// position is improved.
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   "must have one of for_each, parallel, sleep, wait_until, wait_for_signal, cel, echo, http, or printf",
				underlines: "a",
			}},
		},
		{
			name: "step with two kinds of work",
			src: `name: two-kinds
steps:
  - id: a
    echo:
      message: hi
    parallel:
      - steps:
          - id: b
            echo:
              message: hi
`,
			want: []want{{
				code:     codeFlowfile,
				severity: lsp.Error,
				contains: "a step does exactly one kind of work",
			}},
		},
		{
			name: "a for_each step is not missing a task",
			// A loop is a legal kind of work. Flagging it for carrying no task key
			// would put an error on a working file, which is the failure this
			// package exists to avoid.
			src: `name: loop
steps:
  - id: items
    cel:
      expr: "['a', 'b']"
  - id: each
    for_each:
      items: ${steps.items.result}
      iterator: one
      steps:
        - id: body
          echo:
            message: ${one}
`,
		},
		{
			name: "forward reference underlines the expression",
			src: `name: fwd
steps:
  - id: a
    echo:
      message: ${steps.b.result}
  - id: b
    echo:
      message: hi
`,
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   `references step "b", which runs later`,
				underlines: "${steps.b.result}",
			}},
		},
		{
			name: "duplicate id underlines the id",
			src: `name: dupes
steps:
  - id: a
    echo:
      message: one
  - id: a
    echo:
      message: two
`,
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   "duplicate id",
				underlines: "a",
			}},
		},
		{
			name: "cel task accepts input names its schema does not declare",
			// The compiler flattens vars into the input map, so anything under
			// vars is a legal variable name. Flagging these would make every cel
			// step look broken.
			src: `name: vars
steps:
  - id: a
    echo:
      message: hello
  - id: b
    cel:
      expr: vars.greeting
      vars:
        greeting: ${steps.a.result}
`,
		},
		{
			name: "an expression surrounded by other text is reported",
			// The compiler refuses a partial fence rather than silently keeping it
			// as literal text, because an author who wrote ${...} meant an
			// expression. Its message says how to fix it, so it is used as written
			// and only positioned onto the value.
			src: `name: literal
steps:
  - id: a
    echo:
      message: "cost is ${ ] not cel} dollars"
`,
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   "mixes literal text with an expression",
				underlines: `"cost is ${ ] not cel} dollars"`,
			}},
		},
		{
			name: "a plain string containing no fence is left alone",
			src: `name: literal
steps:
  - id: a
    echo:
      message: "cost is 5 dollars"
`,
		},
		{
			name: "a step with no id is positioned on the step",
			// flowfile addresses this step as steps[0], since it has no id to
			// name it by; the range has to be recovered from the index.
			//
			// Underlining the step's first line is what it did before flattening,
			// and that line is still the one the step opens on — what changed is
			// only which key opens it. Written the old way the fixture no longer
			// tests this at all: `task:` is now an unrecognised key, so the report
			// gained an unknown-task diagnostic and stopped being about the id.
			src: `name: no-id
steps:
  - echo:
      message: hello
`,
			want: []want{{
				code:       codeFlowfile,
				severity:   lsp.Error,
				contains:   "step has no id",
				underlines: "- echo:",
			}},
		},
		{
			name: "workflow with no name",
			src: `steps:
  - id: a
    echo:
      message: hello
`,
			want: []want{{
				code:     codeFlowfile,
				severity: lsp.Error,
				contains: "workflow has no name",
			}},
		},
		{
			name: "workflow with no steps",
			src:  "name: empty\n",
			want: []want{{
				code:     codeFlowfile,
				severity: lsp.Error,
				contains: "workflow has no steps",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newClient(t)
			c.initialize()
			params := c.open("file:///"+tt.name+".yaml", tt.src)

			require.NotNil(t, params.Diagnostics, "diagnostics must be an array, never null")
			assert.Len(t, params.Diagnostics, len(tt.want),
				"unexpected diagnostics: %v", messages(params.Diagnostics))

			for i, w := range tt.want {
				if i >= len(params.Diagnostics) {
					break
				}
				got := params.Diagnostics[i]
				assert.Equal(t, w.code, got.Code)
				assert.Equal(t, w.severity, got.Severity)
				assert.Equal(t, diagnosticSource, got.Source)
				assert.Contains(t, got.Message, w.contains)
				if w.underlines != "" {
					assert.Equal(t, w.underlines, textInRange(tt.src, got.Range),
						"diagnostic %d underlines the wrong text", i)
				}
			}
		})
	}
}

// TestDiagnosticsClearWhenFixed proves that repairing a document retracts the
// problems reported for it. Publishing nothing would leave the editor showing
// errors the author has already fixed.
func TestDiagnosticsClearWhenFixed(t *testing.T) {
	t.Parallel()

	const broken = `name: fix-me
steps:
  - id: a
    shell:
      message: hello
`
	const fixed = `name: fix-me
steps:
  - id: a
    echo:
      message: hello
`

	c := newClient(t)
	c.initialize()

	first := c.open("file:///fix.yaml", broken)
	require.NotEmpty(t, first.Diagnostics)

	second := c.change("file:///fix.yaml", fixed, 2)
	require.NotNil(t, second.Diagnostics)
	assert.Empty(t, second.Diagnostics, "a clean document must publish an empty list")
}

// TestDiagnosticsOnSave checks that saving republishes, which is what a client
// configured to validate on save alone relies on.
func TestDiagnosticsOnSave(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	c.open("file:///save.yaml", "name: x\n")

	before := c.publishCount()
	wait := c.expectPublish()
	require.NoError(t, c.conn.Notify(t.Context(), "textDocument/didSave", lsp.DidSaveTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: "file:///save.yaml"},
	}))
	c.await(wait)
	assert.Greater(t, c.publishCount(), before)
}

// TestDiagnosticsClearOnClose checks that closing a document retracts its
// problems, since they are no longer actionable.
func TestDiagnosticsClearOnClose(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	opened := c.open("file:///close.yaml", "name: x\n")
	require.NotEmpty(t, opened.Diagnostics)

	wait := c.expectPublish()
	require.NoError(t, c.conn.Notify(t.Context(), "textDocument/didClose", lsp.DidCloseTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: "file:///close.yaml"},
	}))
	assert.Empty(t, c.await(wait).Diagnostics)
}

// TestIncrementalChange checks that a range-scoped change is applied, even though
// the server advertises full sync: a client that ignores the advertised kind gets
// correct results rather than a truncated document.
func TestIncrementalChange(t *testing.T) {
	t.Parallel()

	const src = `name: inc
steps:
  - id: a
    shell:
      message: hello
`
	c := newClient(t)
	c.initialize()
	require.NotEmpty(t, c.open("file:///inc.yaml", src).Diagnostics)

	// Replace "shell" with "echo" in place. The task's name is the step's key
	// now, so the word to overwrite opens the task instead of sitting on a
	// `name:` line inside it — one line earlier than it used to be.
	//
	// Both coordinates are read out of the fixture rather than written down. A
	// hard-coded line beside a searched column is what made this fail after the
	// flattening: the search returned -1 on a line that no longer held the word,
	// the edit spliced "echo" across the wrong characters, and the assertion that
	// caught it could only say the document was still broken.
	line, start := -1, -1
	for i, text := range strings.Split(src, "\n") {
		if at := strings.Index(text, "shell"); at >= 0 {
			line, start = i, at
			break
		}
	}
	require.GreaterOrEqual(t, line, 0, "the fixture no longer contains the word this edit replaces")
	params := c.changeRange("file:///inc.yaml", 2, lsp.Range{
		Start: lsp.Position{Line: line, Character: start},
		End:   lsp.Position{Line: line, Character: start + len("shell")},
	}, "echo")
	assert.Empty(t, params.Diagnostics, "the edit should have fixed the document: %v", messages(params.Diagnostics))
}

// TestExamplesAreClean is the regression test against false positives. Every
// example shipped with the repository must validate silently; a check that
// reports a problem in working, documented code is worse than no check.
func TestExamplesAreClean(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the path may have moved")

	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			data, err := os.ReadFile(path)
			require.NoError(t, err)

			doc := newDocument(lsp.DocumentURI("file://"+path), 1, string(data))
			assert.Empty(t, messages(diagnose(doc)))
		})
	}
}

// TestDocumentSizeIsBounded checks that an oversized document is reported rather
// than parsed, so a stray large file cannot make every keystroke slow.
func TestDocumentSizeIsBounded(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	huge := "name: big\nsteps:\n" + strings.Repeat("# padding padding padding padding\n", maxDocumentBytes/34+1)
	require.Greater(t, len(huge), maxDocumentBytes)

	params := c.open("file:///big.yaml", huge)
	require.Len(t, params.Diagnostics, 1)
	assert.Equal(t, codeTooLarge, params.Diagnostics[0].Code)
	assert.Contains(t, params.Diagnostics[0].Message, "not being checked")
}

// TestHostileDocuments checks that pathological input is reported rather than
// crashing or hanging the server. A language server that dies on one bad file
// takes every other open file's diagnostics with it.
func TestHostileDocuments(t *testing.T) {
	t.Parallel()

	sources := map[string]string{
		"empty":                 "",
		"only whitespace":       "   \n\t\n",
		"null document":         "null\n",
		"scalar document":       "just a string\n",
		"list document":         "- a\n- b\n",
		"steps is a scalar":     "name: x\nsteps: nope\n",
		"step is a scalar":      "name: x\nsteps:\n  - nope\n",
		"task is a scalar":      "name: x\nsteps:\n  - id: a\n    echo: scalar\n",
		"inputs is a scalar":    "name: x\nsteps:\n  - id: a\n    echo: nope\n",
		"inputs is a list":      "name: x\nsteps:\n  - id: a\n    echo: [a, b]\n",
		"deeply nested":         "name: x\nsteps:\n  - id: a\n    echo:\n      message:\n" + strings.Repeat("          - ", 1) + "\n",
		"duplicate yaml keys":   "name: x\nname: y\n",
		"tabs everywhere":       "\tname:\tx\n",
		"unterminated quote":    "name: \"unterminated\nsteps: []\n",
		"unclosed expression":   "name: x\nsteps:\n  - id: a\n    echo:\n      message: ${a\n",
		"expression is nothing": "name: x\nsteps:\n  - id: a\n    echo:\n      message: ${}\n",
		"very long line":        "name: " + strings.Repeat("x", 100_000) + "\n",
		"binary-ish":            "name: \x00\x01\x02\nsteps: []\n",
		"crlf":                  "name: x\r\nsteps:\r\n  - id: a\r\n    echo:\r\n      message: hi\r\n",
		"anchors and aliases":   "name: x\nbase: &b\n  message: hi\nsteps:\n  - id: a\n    echo: *b\n",
		"emoji ids":             "name: x\nsteps:\n  - id: 🙂\n    echo:\n      message: hi\n",
	}

	c := newClient(t)
	c.initialize()

	for name, src := range sources {
		t.Run(name, func(t *testing.T) {
			uri := "file:///hostile-" + strings.ReplaceAll(name, " ", "-") + ".yaml"

			// Publishing at all is the assertion: the handler neither panicked
			// (which would reply with an error instead) nor hung.
			params := c.open(uri, src)
			assert.NotNil(t, params.Diagnostics)

			// Every other feature must survive the same document, both at
			// positions inside it and at one well past its end — an editor can
			// send a stale position after an edit the server has not seen.
			for _, pos := range []lsp.Position{
				{Line: 0, Character: 0},
				{Line: 2, Character: 8},
				{Line: strings.Count(src, "\n") + 5, Character: 4000},
				{Line: -1, Character: -1},
			} {
				assert.NotPanics(t, func() {
					c.hover(uri, pos.Line, pos.Character)
					c.complete(uri, pos.Line, pos.Character)
					c.definition(uri, pos.Line, pos.Character)
				})
			}
			assert.NotPanics(t, func() { c.symbols(uri) })
		})
	}
}

// textInRange returns the source text a range covers, so a test can assert on what
// an editor would underline rather than on coordinates.
func textInRange(src string, rng lsp.Range) string {
	ix := newLineIndex(src)
	start := ix.offsetOfPosition(rng.Start)
	end := ix.offsetOfPosition(rng.End)
	if start > end || end > len(src) {
		return fmt.Sprintf("<invalid range %v>", rng)
	}
	return src[start:end]
}
