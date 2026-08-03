package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` rewriting a valid file is what fixexpr.go's own comment calls the
// worst thing in this package, "because the whole promise of the command is that
// it is safe to run on anything". It happened once, for a macro-bound name, and
// the recorded fix was to parse with the profile's environment so a name CEL binds
// is not free.
//
// That closed the CEL half and left the grammar's. Three names are bound bare by
// the *language* rather than by an expression — a loop's `as:`, a step's own
// `vars:` keys, and `now` inside a wait — and DSL.md made all three legal
// alongside a step of the same id on purpose. Each was silently rewritten into a
// reference to that step.
//
// Silently is the operative word. The result validates, so nothing downstream
// catches it; it just computes something else.

// TestFixLeavesANameTheGrammarBindsAlone is the corruption, in all three spellings.
//
// Byte-for-byte equality rather than a check that the file still validates, which
// is what made this invisible: every one of these came out valid and wrong.
func TestFixLeavesANameTheGrammarBindsAlone(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			// `host` is the loop's item inside the body. Rewritten to `steps.host`
			// it became a reference to a step whose outputs are empty, so
			// `size(host)` went from 5 and 2 to 0 and 0.
			name: "a loop's binding shares a step's id",
			source: `edition: v2026.2
name: shadow
steps:
  - id: host
    log:
      message: a step whose id is host
  - id: each
    for_each:
      items: "${['alpha', 'be']}"
      as: host
      steps:
        - id: inner
          log:
            message: "${'length is ' + string(size(host))}"
`,
		},
		{
			// A step's own vars are bare within that step — deliberately, and
			// unlike the workflow's, which are rooted under `vars.`.
			name: "a step var shares a step's id",
			source: `edition: v2026.2
name: shadow-vars
steps:
  - id: subject
    log:
      message: a step called subject
  - id: greet
    vars:
      subject: "${'world'}"
    log:
      message: "${'hello ' + subject}"
`,
		},
		{
			// A loop with no `as:` still binds one, under `v1.DefaultIterator`.
			// Reading only the explicit spelling left the default corrupted, which
			// is the same defect with the binding written by omission.
			name: "a loop's implicit `item` shares a step's id",
			source: `edition: v2026.2
name: implicit
steps:
  - id: item
    log:
      message: a step called item
  - id: each
    for_each:
      items: "${['a', 'bb']}"
      steps:
        - id: inner
          log:
            message: "${'len ' + string(size(item))}"
`,
		},
		{
			// The same binding, written the two other ways the compiler accepts it
			// (`fields.go`'s `resolve` follows both). The rewriter read only a plain
			// scalar, so an anchored or aliased `as:` looked like a loop with no
			// `as:` at all — and a loop with no `as:` binds `item`, so the name the
			// file actually binds was rooted inside the body of both loops.
			name: "a loop's binding is written through an anchor and an alias",
			source: `edition: v2026.2
name: shadow-anchored
steps:
  - id: host
    log:
      message: a step whose id is host
  - id: first
    for_each:
      items: "${['a']}"
      as: &binding host
      steps:
        - id: one
          log:
            message: "${host}"
  - id: second
    for_each:
      items: "${['b']}"
      as: *binding
      steps:
        - id: two
          log:
            message: "${host}"
`,
		},
		{
			// A step's `vars:` reached the same way. The anchor was unwrapped here
			// already and the alias was not, so `vars: *defaults` read as no vars at
			// all and the name it declares was rooted into the step it shadows.
			name: "a step's vars are written through an alias",
			source: `edition: v2026.2
name: shadow-alias-vars
vars:
  defaults: &defaults
    subject: "${'world'}"
steps:
  - id: subject
    log:
      message: a step called subject
  - id: greet
    vars: *defaults
    log:
      message: "${'hello ' + subject}"
`,
		},
		{
			// The exact file DSL.md claims was verified: "a step called `now`, and
			// a `wait_until: ${now + seconds(1)}` in the same workflow that still
			// reads the clock rather than the step".
			name: "a step is called now, beside a wait that reads the clock",
			source: `edition: v2026.2
name: shadow-now
steps:
  - id: now
    log:
      message: a step called now
  - id: hold
    wait_until: "${now + duration('1s')}"
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(test.source))
			require.NoError(t, err)

			assert.Equal(t, test.source, string(result.Source),
				"a valid file was rewritten; it still validates and means something else")
			assert.Empty(t, result.Changes,
				"a file already in the current edition was reported as changed")

			// `flow fix --check` exits non-zero on a refusal, so a refusal here
			// fails CI on a legitimate file even though nothing was written.
			assert.Empty(t, result.Refusals,
				"a valid file was refused, so `flow fix --check` fails on it in CI")
		})
	}
}

// TestFixStillRootsARealStepReference is the control, and without it the fix above
// is indistinguishable from switching the rewriter off.
//
// Subtracting a name from the candidate set is a blunt instrument: subtract too
// much and `flow fix` stops migrating anything, which no test asserting that files
// are left alone could tell apart from working correctly.
func TestFixStillRootsARealStepReference(t *testing.T) {
	t.Parallel()

	// A loop binding named `item`, and a body that references a genuine step —
	// which is the shape the subtraction must not swallow. The binding shadows one
	// name; every other step is still a step.
	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: roots
steps:
  - id: source
    log:
      message: something
  - id: each
    for_each:
      items: "${['a']}"
      as: item
      steps:
        - id: inner
          log:
            message: "${item + source.said}"
`))
	require.NoError(t, err)

	assert.Contains(t, string(result.Source), "steps.source.said",
		"a real step reference was not rooted, so the subtraction swallowed more than "+
			"the name the grammar binds")
	assert.NotContains(t, string(result.Source), "steps.item",
		"the loop's own binding was rooted alongside the step")
}

// TestFixRemovesAStepWhoseDashIsOnItsOwnLine is a different way of writing the
// same file, and it produced one the validator refuses.
//
// [fixer.deleteStep]'s comment says the range is "bounded by the *dash*, not by
// the first key", and gives the reason — a step's keys all sit at one indent, so a
// range measured from the first key ends at the second. The code then measured
// from the first key anyway, which is the same thing only when the dash shares its
// line. YAML does not require that.
//
// The dash survived, the sequence kept an entry with nothing under it, and `flow
// fix` exited zero:
//
//	steps:
//	  -
//	  -
//	    id: shout
//
// `flow fix . && git commit` therefore succeeded on a file `flow validate`
// refuses, which is the outcome cmd/flow/fix.go says must not happen: "a file that
// looks fixed and is not is worse than one that was never touched".
func TestFixRemovesAStepWhoseDashIsOnItsOwnLine(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: bare-dash
steps:
  -
    id: greet
    echo:
      message: hello
  -
    id: shout
    log:
      message: "${steps.greet.result}"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	// The strongest available statement about the result, and the one that fails
	// without the fix: the rewritten file has to be one the validator accepts.
	// Asserting on the text would pass on a file with an empty sequence entry in
	// it, since that is still the text somebody might expect.
	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)

	assert.NotContains(t, string(result.Source), "greet:\n",
		"the step was moved into vars and its old lines were left behind")
}

// TestFixRootsWhatTheBindingDoesNotReach is the other half of the scoping, and the
// direction an over-broad exemption breaks.
//
// A binding is in scope for some of its siblings and not others. Subtracting a
// name for the whole step is as wrong as not subtracting it at all — it just fails
// the other way: the legacy reference is *left* bare while the edition is stamped,
// so `flow fix` exits zero on a file `flow validate` then rejects.
//
// Both scopes here come from where the engine evaluates the thing. A loop's
// `items:` produces the list before anything is bound, and `runNodes` evaluates a
// step's `if:` before its `vars:` exist — saying so in a comment where it does it.
func TestFixRootsWhatTheBindingDoesNotReach(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		rooted string
		bareIn string
	}{
		{
			name: "a loop's items sees the step, not the iterator it declares",
			source: `edition: 2026.1
name: itemsroot
steps:
  - id: source
    echo:
      message: hello
  - id: each
    for_each:
      items: "${[source.result]}"
      as: source
      steps:
        - id: inner
          log:
            message: "${source}"
`,
			// `source` in `items:` is the step, migrated into `vars:`; `source` in
			// the body is the loop's item and stays bare.
			rooted: "vars.source",
			bareIn: `message: "${source}"`,
		},
		{
			name: "a step's if does not see the step's own vars",
			source: `edition: 2026.1
name: ifscope
steps:
  - id: flag
    echo:
      message: "yes"
  - id: act
    if: "${flag.result == 'yes'}"
    vars:
      flag: "${'shadowed'}"
    log:
      message: "${flag}"
`,
			rooted: "vars.flag",
			bareIn: `message: "${flag}"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(test.source))
			require.NoError(t, err)
			require.Empty(t, result.Refusals)

			assert.Contains(t, string(result.Source), test.rooted,
				"a reference the binding does not reach was left bare, so the file is "+
					"stamped with the new edition and rejected by the validator")
			assert.Contains(t, string(result.Source), test.bareIn,
				"the binding itself was rooted, which changes what the workflow computes")

			// The whole point: whichever way it went, the result has to compile.
			_, err = flowfile.ValidateSource(result.Source)
			assert.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
		})
	}
}

// TestFixDoesNotRewriteProse is the over-read [stepReference]'s comment calls
// harmless.
//
// It claimed both directions were safe — "reading a reference that is not one
// produces an unused `vars:` entry, and missing one produces a refusal. Neither
// writes something that means something else" — and the first half was wrong twice
// over. The scan decides whether a retired step is *migrated or deleted*, so a
// sentence in a `message:` was enough to delete an `echo` an author meant a person
// to see; and the rewriter then edited the sentence itself:
//
//   - message: "to read the greeting write steps.greet.result in your expression"
//   - message: "to read the greeting write vars.greet in your expression"
//
// The file validated and ran, printing prose nobody wrote.
//
// A reference is a reference only inside a `${...}`. Outside one it is text, and
// under-reading it produces a refusal — which is the direction the comment was
// right about.
func TestFixDoesNotRewriteProse(t *testing.T) {
	t.Parallel()

	const prose = `to read the greeting write steps.greet.result in your expression`

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: literal
steps:
  - id: greet
    echo:
      message: hello
  - id: doc
    log:
      message: "` + prose + `"
`))
	require.NoError(t, err)

	assert.Contains(t, string(result.Source), prose,
		"an author's sentence was rewritten because it mentioned a step")

	// And the step survives, because nothing actually reads it: the migration
	// cannot tell what an `echo` nobody reads was for, and says so rather than
	// guessing. A refusal is the safe direction — it leaves work to do rather than
	// removing work already done.
	assert.NotEmpty(t, result.Refusals,
		"a step mentioned only in prose was migrated as though something read it")
	assert.Contains(t, string(result.Source), "id: greet",
		"a step nothing reads was deleted on the strength of a sentence")
}

// TestFixStillFindsAReferenceInsideAFence is the control for that narrowing.
//
// Scanning only inside fences is a real reduction in what the migration sees, and
// a version that saw nothing would pass the test above while refusing every file
// it used to rewrite.
func TestFixStillFindsAReferenceInsideAFence(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: fenced
steps:
  - id: greet
    echo:
      message: hello
  - id: show
    log:
      message: "${steps.greet.result}"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "a reference written inside a fence was not seen")

	assert.Contains(t, string(result.Source), "vars.greet",
		"the reference was found but not rewritten")

	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
}

// TestAnExpressionMayContainBraces covers the reason the scan counts them.
//
// `${ {'a': steps.x.result} }` closes at the last brace and not the first, and a
// scanner that stopped at the first would read half an expression — finding the
// reference or not depending on where the author put a map.
func TestAnExpressionMayContainBraces(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: braces
steps:
  - id: greet
    echo:
      message: hello
  - id: show
    log:
      message: "${ {'said': steps.greet.result}['said'] }"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Contains(t, string(result.Source), "vars.greet",
		"a reference after a map literal inside the same expression was not seen")
}

// TestFixMigratesAPrintfWithANumericArgument covers the migration DSL.md
// documents and `flow fix` refused.
//
// `celSourceOf` asked [scalarText] first, which answers only for text — so an
// integer, float or boolean argument took the refusal at the top, and the branch
// written directly below it for exactly those three node kinds could never run.
// `parse.go` tells an author to "Run `flow fix` to rewrite it", and running it
// stamped the new edition and left the step, producing a file that was neither the
// old spelling nor a valid one.
func TestFixMigratesAPrintfWithANumericArgument(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: printf-numeric
steps:
  - id: greet
    printf:
      format: "hello %s (%d)"
      args:
        - "world"
        - 0
  - id: show
    log:
      message: "${steps.greet.result}"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals,
		"the migration this repo tells authors to run refused the example its own docs give")

	// The number stays a number. Quoting it — which is what text needs, and what
	// the refusal was standing in front of — would make `%d` a string and the
	// format fail at run time rather than here.
	assert.Contains(t, string(result.Source), `"hello %s (%d)".format(["world", 0])`,
		"the rewritten expression is not the one the docs show")

	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
}

// TestABraceInsideAStringDoesNotEndTheExpression is the counter's second rule.
//
// `${a + '}' + b}` closes at the *last* brace. A counter that stopped at the
// quoted one rewrote the first reference and left the second naming a step the
// same pass had just deleted — a current-edition file the validator rejects, from
// a command that exited zero.
func TestABraceInsideAStringDoesNotEndTheExpression(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: braced-string
steps:
  - id: greet
    echo:
      message: hello
  - id: show
    log:
      message: "${steps.greet.result + '}' + steps.greet.result}"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.NotContains(t, string(result.Source), "steps.greet",
		"a reference after a brace inside a string outlived the step it names")
	assert.Contains(t, string(result.Source), `'}'`,
		"the string literal itself was disturbed")

	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
}

// TestADeferredInputIsExpressionSourceWhole is the one place the fence rule is
// wrong, and the reason it is a rule about *prose* rather than about fences.
//
// An input the task evaluates itself is written bare, since a fence there would be
// a fence around a fence. Rewriting only inside `${...}` therefore skipped it: a
// rooted reference in an `outputs:` survived `greet` being migrated and went on
// naming a step that no longer existed.
//
// And it validated, because deferred inputs are deliberately not reference-checked
// — the task evaluates them in a scope the validator cannot see. So the only place
// this could surface was a run.
func TestADeferredInputIsExpressionSourceWhole(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: deferred-bare
steps:
  - id: greet
    echo:
      message: hello
  - id: fetch
    http:
      url: https://example.com/
      outputs: "{'said': steps.greet.result}"
  - id: show
    log:
      message: "${steps.greet.result}"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.NotContains(t, string(result.Source), "steps.greet",
		"a bare deferred input kept a reference to a step this pass deleted, and nothing "+
			"downstream checks a deferred input, so the run is the first thing that would say so")
	assert.Contains(t, string(result.Source), `outputs: "{'said': vars.greet}"`,
		"the deferred input was not rewritten to the value the step moved to")
}

// TestFixRootsAStepShadowedByNothingWhenTheBindingIsAnchored is the other
// direction of the anchored `as:`, and the one an over-broad exemption hides.
//
// Reading a binding it cannot spell wrong in *two* ways at once is what made this
// worth its own test. A loop whose `as:` was an anchor fell back to
// [v1.DefaultIterator], so the rewriter subtracted `item` — a name the file never
// binds — while leaving `host`, the name it does, in the candidate set. So the
// legacy reference to a step genuinely called `item` was left bare in a file
// stamped with the new edition, which is `flow fix` exiting zero on a file `flow
// validate` then rejects; and a body reference to `host` would have been rooted
// into the step of that name, which is the corruption.
//
// Byte-for-byte, because the interesting property is which of the two names moved.
func TestFixRootsAStepShadowedByNothingWhenTheBindingIsAnchored(t *testing.T) {
	t.Parallel()

	const want = `edition: v2026.2
name: itemroot
steps:
  - id: item
    log:
      message: something
  - id: each
    for_each:
      items: "${['a']}"
      as: &binding host
      steps:
        - id: inner
          log:
            message: "${host + steps.item.said}"
`

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: itemroot
steps:
  - id: item
    log:
      message: something
  - id: each
    for_each:
      items: "${['a']}"
      as: &binding host
      steps:
        - id: inner
          log:
            message: "${host + item.said}"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Equal(t, want, string(result.Source),
		"the loop's `as:` was read as absent, so `item` was subtracted as a binding "+
			"the file never made and the step of that name was left unrooted")

	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
}

// TestFixRootsAStepSharingTheWorkflowsVarName is the third scope, and the one
// [boundBareNames] was applied to by accident.
//
// The walk reads the bindings off every mapping, and the document's own body is a
// mapping — so the *workflow's* `vars:` were subtracted as though they were bound
// bare. They are not: a workflow var is reached as `vars.<name>`, which is what
// makes a top-level var legal beside a step of the same id in the first place.
//
// The cost is the leave-it-bare direction. `greet` was subtracted, the legacy
// `greet.said` was left as written, and the edition was stamped anyway — so `flow
// fix` reported success on a file `flow validate` refuses.
func TestFixRootsAStepSharingTheWorkflowsVarName(t *testing.T) {
	t.Parallel()

	const want = `edition: v2026.2
name: topvars
vars:
  greet: "${'hi'}"
steps:
  - id: greet
    log:
      message: something
  - id: show
    log:
      message: "${steps.greet.said}"
`

	result, err := flowfile.Fix([]byte(`edition: 2026.1
name: topvars
vars:
  greet: "${'hi'}"
steps:
  - id: greet
    log:
      message: something
  - id: show
    log:
      message: "${greet.said}"
`))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)

	assert.Equal(t, want, string(result.Source),
		"a workflow var was read as a bare binding, so the step sharing its name was "+
			"left unrooted in a file stamped with the new edition")

	_, err = flowfile.ValidateSource(result.Source)
	require.NoError(t, err, "the rewritten file does not compile:\n%s", result.Source)
}
