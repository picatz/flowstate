package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `vars:` is where an author writes expressions now, and the editor could not see into
// it.
//
// The positional model listed `if`, a loop's `items`, `wait_until` and a task's inputs
// as the places an expression can be — which was the whole list until `echo:` and `cel:`
// retired, and their replacement is `vars:`. So the edit that moved a value out of a
// `cel:` step took hover, go-to-definition and the syntax squiggle away from it in the
// same stroke, in the position the language now steers everyone toward.

// TestASyntaxErrorInVarsLandsOnTheOffendingCharacter is the symptom that matters most.
//
// Without the model, a broken expression here reached only the validator, whose position
// is the *value's* — so the squiggle covered the closing brace rather than the character
// CEL objected to. That is the failure
// TestWaitUntilSyntaxErrorLandsOnTheOffendingCharacter exists to prevent, in the position
// an author is most likely to be mid-edit in.
//
// Three positions, because `vars:` is one rule with several sites and each reaches the
// model by a different path: the document's own block, a task step's, and a loop's.
func TestASyntaxErrorInVarsLandsOnTheOffendingCharacter(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			name: "at the top of the file",
			src: `edition: v2026.2
name: t
vars:
  broken: ${1 + + 2}
steps:
  - id: s
    log:
      message: hi
`,
		},
		{
			name: "on a step",
			src: `edition: v2026.2
name: t
steps:
  - id: s
    vars:
      broken: ${1 + + 2}
    log:
      message: hi
`,
		},
		{
			name: "on a loop, where it is in scope for the body too",
			src: `edition: v2026.2
name: t
steps:
  - id: each
    vars:
      broken: ${1 + + 2}
    for_each:
      items: ${[1]}
      as: n
      steps:
        - id: inner
          log:
            message: hi
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newClient(t)
			c.initialize()
			params := c.open("file:///"+strings.ReplaceAll(test.name, " ", "-")+".yaml", test.src)

			var found bool
			for _, d := range params.Diagnostics {
				if d.Code != codeCELSyntax {
					continue
				}
				found = true

				// The second `+`, which is the character CEL objects to — not the
				// whole value, and not the brace that closes it.
				assert.Equal(t, "+", textInRange(test.src, d.Range),
					"the squiggle does not cover the offending character")
			}
			require.True(t, found,
				"no CEL syntax diagnostic inside `vars:`; the editor is silent where the validator is not: %v",
				messages(params.Diagnostics))
		})
	}
}

// TestAVarNamedLikeADeferredInputIsNotParsedAsCEL is the trap that adding `vars:` to the
// expression list opened, and the reason the check asks about an entry rather than a
// name.
//
// An input a task evaluates itself carries CEL source with no fence, so it is parsed
// even unfenced. That test read the entry's *key*, and a key is a word: once a step's
// `vars:` bindings arrived through the same list, a var called `expect` on an `http`
// step would have had its plain text read as an expression and squiggled.
//
// A false diagnostic on a working file is what this package can least afford, so the
// negative direction is written down rather than assumed.
func TestAVarNamedLikeADeferredInputIsNotParsedAsCEL(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: s
    vars:
      expect: not an expression, just words
    http:
      method: GET
      url: https://example.com
`

	c := newClient(t)
	c.initialize()
	params := c.open("file:///deferred-name.yaml", src)

	for _, d := range params.Diagnostics {
		assert.NotEqual(t, codeCELSyntax, d.Code,
			"plain text under `vars:` was parsed as CEL because a task defers an input of that name: %s",
			d.Message)
	}
}

// TestHoverInsideAWorkflowVarSaysWhatCannotBeReadThere is where silence would have been
// the wrong answer.
//
// A workflow `vars:` block is evaluated before the first step runs and is a mapping with
// no order, so a value in it can reference neither a step nor another var — the two
// things an author reaches for first. Hover cannot go through the step path here,
// because there is no step; and that path's answer for a reference it cannot resolve is
// silence, which reads as "this is fine".
func TestHoverInsideAWorkflowVarSaysWhatCannotBeReadThere(t *testing.T) {
	t.Parallel()

	const uri = "file:///workflow-var-hover.yaml"
	src := `edition: v2026.2
name: t
vars:
  a: hello
  b: ${steps.s.result + vars.a}
steps:
  - id: s
    log:
      message: hi
`

	c := newClient(t)
	c.initialize()
	c.open(uri, src)

	onStep := positionOf(t, src, "steps.s.result", 2)
	got := c.hover(uri, onStep.Line, onStep.Character)
	require.NotNil(t, got, "hovering a step reference inside a workflow var said nothing")
	assert.Contains(t, hoverText(got), "before the first step",
		"the hover does not say why a step cannot be read here")

	onVar := positionOf(t, src, "vars.a}", 1)
	got = c.hover(uri, onVar.Line, onVar.Character)
	require.NotNil(t, got, "hovering a var reference inside a workflow var said nothing")
	assert.Contains(t, hoverText(got), "no order",
		"the hover does not say why one var cannot read another")
}

// TestGoToDefinitionWorksFromAStepsVars keeps the three surfaces together.
//
// [parsedStep.expressionEntries] exists so that hover, go-to-definition and the
// expression diagnostics cover the same places and cannot drift apart. Adding `vars:` to
// it should carry all three; this is what says so rather than assuming it.
func TestGoToDefinitionWorksFromAStepsVars(t *testing.T) {
	t.Parallel()

	const uri = "file:///step-var-definition.yaml"
	src := `edition: v2026.2
name: t
steps:
  - id: web
    http:
      method: GET
      url: https://example.com
  - id: s
    vars:
      code: ${steps.web.status_code}
    log:
      message: hi
`

	c := newClient(t)
	c.initialize()
	c.open(uri, src)

	at := positionOf(t, src, "${steps.web.status_code}", 10)
	locations := c.definition(uri, at.Line, at.Character)
	require.Len(t, locations, 1, "go to definition from inside a step's `vars:` found nothing")

	assert.Equal(t, "  - id: web", strings.Split(src, "\n")[locations[0].Range.Start.Line],
		"go to definition landed somewhere other than the step it names")
}

// TestHoverWorksFromAStepsVars is the same surface from the position that has a step.
//
// Unlike a workflow var, a step's var can read a step, so the answer here is the ordinary
// one — the output's documentation — rather than a refusal. Both directions are worth
// pinning: the same key means different things at the two levels, and a model answering
// the same way at both would be wrong at one of them.
func TestHoverWorksFromAStepsVars(t *testing.T) {
	t.Parallel()

	const uri = "file:///step-var-hover.yaml"
	src := `edition: v2026.2
name: t
steps:
  - id: web
    http:
      method: GET
      url: https://example.com
  - id: s
    vars:
      code: ${steps.web.status_code}
    log:
      message: hi
`

	c := newClient(t)
	c.initialize()
	c.open(uri, src)

	at := positionOf(t, src, "${steps.web.status_code}", 18)
	got := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, got, "hovering a step output inside a step's `vars:` said nothing")

	text := hoverText(got)
	assert.Contains(t, text, "status_code", "the hover does not name the output")
	assert.Contains(t, text, "http", "the hover does not say which task produces it")
}

// TestTheEditorShowsARuleTheSchemaStates is the join of two features that were built
// separately and have to meet.
//
// `flow validate` learned to check the rules a task's inputs declare — a `method` that
// matches a pattern, a `url` that is a URI — by asking protovalidate over the
// descriptor the registry carries. This package converts what the validator reports
// into an editor's diagnostic, positioning it from its own model.
//
// Neither half knows about the other, which is exactly why this is worth a test: the
// new check produces diagnostics carrying a step and a *field*, and whether this
// package can turn that into a range over the offending token is a claim about the two
// together. A range that fell back to the whole step would still "work" and would be
// noticeably worse to read.
func TestTheEditorShowsARuleTheSchemaStates(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: web
    http:
      method: FETCH
      url: https://example.com
`

	c := newClient(t)
	c.initialize()
	params := c.open("file:///schema-rule.yaml", src)

	require.NotEmpty(t, params.Diagnostics,
		"the editor is silent about a value the validator refuses")

	var found bool
	for _, d := range params.Diagnostics {
		if !strings.Contains(d.Message, "regex pattern") {
			continue
		}
		found = true

		// The value at fault, not the line and not the step. This is the part the
		// join buys: the validator names a field, and this package turns that into
		// the token an author has to change.
		assert.Equal(t, "FETCH", textInRange(src, d.Range),
			"the diagnostic does not underline the value the schema refused")
	}
	require.True(t, found,
		"no diagnostic about the declared pattern reached the editor: %v", messages(params.Diagnostics))
}
