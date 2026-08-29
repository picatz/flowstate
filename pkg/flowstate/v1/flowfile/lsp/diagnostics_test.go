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

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The classes a validator diagnostic is published under, which this server no
// longer invents: these are read from the registry that assigns them, so a case
// below states which *kind* of problem it expects and cannot drift from what
// `flow validate --output json` says about the same file.
// Only the classes the cases below actually expect are named. The registry has
// more, and listing them here would be a second copy of it that nothing checks.
var (
	codeGeneral             = string(v1.DiagnosticCodeGeneral)
	codeUnknownTask         = string(v1.DiagnosticCodeUnknownTask)
	codeUnresolvedReference = string(v1.DiagnosticCodeUnresolvedReference)
	codeTypeMismatch        = string(v1.DiagnosticCodeTypeMismatch)
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
			// The value a second step used to read out of the first is a `vars:`
			// binding now: `echo` produced a value *and* showed it, and only the
			// showing is a step. What is left of each step is the line a person sees,
			// which is what `log:` is for.
			src: `name: valid
vars:
  greeting: hello
steps:
  - id: first
    log:
      message: ${vars.greeting}
  - id: second
    log:
      message: ${vars.greeting + '!'}
edition: v2026.3
`,
		},
		{
			name: "yaml syntax error lands on the offending token",
			// A tab where the indentation goes. Nothing after it is ever read —
			// the document stops parsing at the tab — but it is spelled in the
			// grammar the DSL has, so nobody reads this fixture as evidence that
			// `echo:` is still a step key.
			src: "name: broken\nsteps:\n  - id: a\n  \tlog: hi\n" + editionSuffix,
			want: []want{{
				code:       codeYAMLSyntax,
				severity:   lsp.Error,
				contains:   "cannot start any token",
				underlines: "\t",
			}},
		},
		{
			name: "unterminated flow sequence",
			src:  "name: broken\nsteps: [\n" + editionSuffix,
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
    log:
      message: ${a b}
edition: v2026.3
`,
			// The compiler cannot produce a workflow from a document with an
			// unparseable expression, so its own validation never runs. The
			// precise CEL error is the entire report — not a second, positionless
			// copy of "invalid expression" at line 1.
			want: []want{{
				code:       codeCELSyntax,
				severity:   lsp.Error,
				contains:   `"b" is not valid here: the expression is already complete before it`,
				underlines: "b",
			}},
		},
		{
			name: "cel syntax error in an input the task evaluates itself",
			// An input listed in the task's DeferredInputs carries expression source
			// with no fence around it, and the validator does not parse those, so this
			// check is the only one they get. `cel:`'s `expr:` was the original case
			// and is retired; `http:`'s `expect:` is the same shape and still here,
			// which is the point — the rule reads DeferredInputs rather than a task
			// name, so it survived the task that motivated it.
			src: `name: badexpr
steps:
  - id: a
    http:
      url: https://example.com
      expect: "1 + + 2"
edition: v2026.3
`,
			want: []want{{
				code:       codeCELSyntax,
				severity:   lsp.Error,
				contains:   `"+" is not valid here, where a value was expected`,
				underlines: "+",
			}},
		},
		{
			name: "unknown task underlines the task name",
			src: `name: badtask
steps:
  - id: a
    shell:
      command: ls
edition: v2026.3
`,
			want: []want{{
				code:       codeUnknownTask,
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
    log:
      mesage: hello
edition: v2026.3
`,
			// Reported by the shared validator, so `flow validate` refuses the
			// workflow too — a misspelled input is silently ignored at run time,
			// which is exactly the mistake that should not reach a run. The
			// language server's contribution is the range: the key is what is
			// misspelled, not the value written under it.
			want: []want{{
				code:       codeGeneral,
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
edition: v2026.3
`,
			want: []want{{
				code:     codeGeneral,
				severity: lsp.Error,
				contains: `task "http" requires input "url" (a string)`,
			}},
		},
		{
			name: "a literal whose type the input cannot hold",
			src: `name: wrong-type
steps:
  - id: a
    log:
      message: [1, 2]
edition: v2026.3
`,
			// Here the key is fine and the value is not, so the range moves to the
			// value. Which of the two is at fault comes from the schema.
			want: []want{{
				code:       codeTypeMismatch,
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
edition: v2026.3
`,
			// Reported by the shared validator, not here: a rule about what a
			// step must be belongs with the compiler that enforces it. Only the
			// position is improved.
			want: []want{{
				code:       codeGeneral,
				severity:   lsp.Error,
				contains:   "must have one of for_each, loop, parallel, sleep, wait_until, wait_for_signal, wait_for_signals, call, value, switch, http, or log",
				underlines: "a",
			}},
		},
		{
			name: "step with two kinds of work",
			src: `name: two-kinds
steps:
  - id: a
    log:
      message: hi
    parallel:
      - steps:
          - id: b
            log:
              message: hi
edition: v2026.3
`,
			want: []want{{
				code:     codeGeneral,
				severity: lsp.Error,
				contains: "a step does exactly one kind of work",
			}},
		},
		{
			name: "a for_each step is not missing a task",
			// A loop is a legal kind of work. Flagging it for carrying no task key
			// would put an error on a working file, which is the failure this
			// package exists to avoid.
			//
			// The list the loop walks is a step-level `vars:` binding rather than a
			// `cel:` step whose result the loop read: a block's vars are in scope for
			// its own `items:` expression, so the value stays beside the loop that
			// needs it instead of becoming a step of its own.
			src: `name: loop
steps:
  - id: each
    vars:
      things: ['a', 'b']
    for_each:
      items: ${things}
      as: one
      steps:
        - id: body
          log:
            message: ${one}
edition: v2026.3
`,
		},
		{
			name: "forward reference underlines the expression",
			// The step being referenced is an `http:` one, because the reference has
			// to name an output that exists: `log:` has none, so `${steps.b.…}` on a
			// log step would be reported for the output rather than for the order,
			// and this case is about the order.
			src: `name: fwd
steps:
  - id: a
    log:
      message: ${steps.b.status_code}
  - id: b
    http:
      url: https://example.com
edition: v2026.3
`,
			want: []want{{
				code:       codeUnresolvedReference,
				severity:   lsp.Error,
				contains:   `references step "b", which runs later`,
				underlines: "${steps.b.status_code}",
			}},
		},
		{
			name: "duplicate id underlines the id",
			src: `name: dupes
steps:
  - id: a
    log:
      message: one
  - id: a
    log:
      message: two
edition: v2026.3
`,
			want: []want{{
				code:       codeGeneral,
				severity:   lsp.Error,
				contains:   "duplicate id",
				underlines: "a",
			}},
		},
		// "cel task accepts input names its schema does not declare" was here, and
		// the exemption it covered is gone rather than merely unreachable. One task
		// took input names its schema did not list, because the compiler emptied its
		// `vars:` mapping into the input map; that task retired at edition v2026.2
		// and the hoist retired with it. An undeclared input is an undeclared input,
		// for every task, which is what the case above this one now asserts.
		{
			name: "a broken expression among other text is reported inside its own fence",
			// Text around a fence is interpolation since #413, so what is wrong
			// with this value is no longer the text — it is the CEL between the
			// braces. The diagnostic says so, and underlines the fence's source
			// rather than the whole scalar: with more than one fence in a value,
			// pointing at the value would leave an author to find which of them
			// the parser meant.
			src: `name: literal
steps:
  - id: a
    log:
      message: "cost is ${ ] not cel} dollars"
edition: v2026.3
`,
			want: []want{{
				code:       codeCELSyntax,
				severity:   lsp.Error,
				contains:   `"]" is not valid here, where a value was expected`,
				underlines: `]`,
			}, {
				code:       codeCELSyntax,
				severity:   lsp.Error,
				contains:   `"cel" is not valid here`,
				underlines: `cel`,
			}},
		},
		{
			name: "text around a whole, valid expression is interpolation and is accepted",
			src: `name: literal
steps:
  - id: a
    log:
      message: "cost is ${1 + 2} dollars, ${'again'}"
edition: v2026.3
`,
			want: nil,
		},
		{
			name: "a plain string containing no fence is left alone",
			src: `name: literal
steps:
  - id: a
    log:
      message: "cost is 5 dollars"
edition: v2026.3
`,
		},
		{
			name: "a step with no id is positioned on the step",
			// flowfile addresses this step as steps[0], since it has no id to
			// name it by; the range has to be recovered from the index.
			//
			// Underlining the step's first line is what it did before flattening,
			// and that line is still the one the step opens on — what changed is
			// only which key opens it. Written with a retired key the fixture no
			// longer tests this at all: `echo:` names no task now, so the report
			// gains a retirement diagnostic and stops being about the id.
			src: `name: no-id
steps:
  - log:
      message: hello
edition: v2026.3
`,
			want: []want{{
				code:       codeGeneral,
				severity:   lsp.Error,
				contains:   "step has no id",
				underlines: "- log:",
			}},
		},
		{
			name: "workflow with no name",
			src: `edition: v2026.3
steps:
  - id: a
    log:
      message: hello
`,
			want: []want{{
				code:     codeGeneral,
				severity: lsp.Error,
				contains: "workflow has no name",
			}},
		},
		{
			name: "workflow with no steps",
			src:  "name: empty\n" + editionSuffix,
			want: []want{{
				code:     codeGeneral,
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
edition: v2026.3
`
	const fixed = `name: fix-me
steps:
  - id: a
    log:
      message: hello
edition: v2026.3
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
edition: v2026.3
`
	c := newClient(t)
	c.initialize()
	require.NotEmpty(t, c.open("file:///inc.yaml", src).Diagnostics)

	// Replace "shell" with "log" in place. The task's name is the step's key
	// now, so the word to overwrite opens the task instead of sitting on a
	// `name:` line inside it — one line earlier than it used to be.
	//
	// Both coordinates are read out of the fixture rather than written down. A
	// hard-coded line beside a searched column is what made this fail after the
	// flattening: the search returned -1 on a line that no longer held the word,
	// the edit spliced the replacement across the wrong characters, and the assertion that
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
	}, "log")
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

			// The real, absolute path rather than the relative one the glob
			// above returns: a real editor always sends an absolute `file://`
			// URI, and `call-a-workflow` names a sibling file relative to its
			// own directory, which only resolves to something real when the
			// URI does too.
			abs, err := filepath.Abs(path)
			require.NoError(t, err)

			doc := newDocument(lsp.DocumentURI("file://"+abs), 1, string(data), nil)
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
		"task is a scalar":      "name: x\nsteps:\n  - id: a\n    log: scalar\n",
		"inputs is a scalar":    "name: x\nsteps:\n  - id: a\n    log: nope\n",
		"inputs is a list":      "name: x\nsteps:\n  - id: a\n    log: [a, b]\n",
		"deeply nested":         "name: x\nsteps:\n  - id: a\n    log:\n      message:\n" + strings.Repeat("          - ", 1) + "\n",
		"duplicate yaml keys":   "name: x\nname: y\n",
		"tabs everywhere":       "\tname:\tx\n",
		"unterminated quote":    "name: \"unterminated\nsteps: []\n",
		"unclosed expression":   "name: x\nsteps:\n  - id: a\n    log:\n      message: ${a\n",
		"expression is nothing": "name: x\nsteps:\n  - id: a\n    log:\n      message: ${}\n",
		"very long line":        "name: " + strings.Repeat("x", 100_000) + "\n",
		"binary-ish":            "name: \x00\x01\x02\nsteps: []\n",
		"crlf":                  "name: x\r\nsteps:\r\n  - id: a\r\n    log:\r\n      message: hi\r\n",
		"anchors and aliases":   "name: x\nbase: &b\n  message: hi\nsteps:\n  - id: a\n    log: *b\n",
		"emoji ids":             "name: x\nsteps:\n  - id: 🙂\n    log:\n      message: hi\n",
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
