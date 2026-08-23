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
			source: `edition: v2026.3
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
			source: `edition: v2026.3
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
			source: `edition: v2026.3
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
		// The same bindings written through an anchor and an alias used to live here,
		// because `fields.go`'s `resolve` followed both and the rewriter read only a
		// plain scalar. The grammar is now a strict subset of YAML that refuses
		// anchors and aliases outright (#653), so those two cases are gone with the
		// spellings they exercised: a name the rewriter can no longer reach through a
		// construct the compiler no longer accepts.
		{
			// A `loop:`'s carried state is bound bare, the same standing as a
			// `for_each` item, so a state named for a step is the same corruption in
			// the new primitive: `cursor` is a step id *and* the loop's state, and
			// `${cursor}` in the body, `until:` and `update:` is the state — rooted to
			// `steps.cursor` it becomes a reference to a `log:` step's empty outputs.
			// Unlike a `for_each` a loop has no default binding, and it is recognised by
			// `until:`+`steps:` rather than `items:`+`steps:`, so this is the path
			// [fixer.boundBareNames] and [sees] grew for the loop.
			name: "a loop's carried state shares a step's id",
			source: `edition: v2026.3
name: shadow-loop-state
steps:
  - id: cursor
    log:
      message: a step whose id is cursor
  - id: pages
    loop:
      as: cursor
      init: "${''}"
      update: "${cursor + 'x'}"
      until: "${size(cursor) >= 3}"
      steps:
        - id: inner
          log:
            message: "${'cursor is now ' + cursor}"
`,
		},
		{
			// The exact file DSL.md claims was verified: "a step called `now`, and
			// a `wait_until: ${now + seconds(1)}` in the same workflow that still
			// reads the clock rather than the step".
			name: "a step is called now, beside a wait that reads the clock",
			source: `edition: v2026.3
name: shadow-now
steps:
  - id: now
    log:
      message: a step called now
  - id: hold
    wait_until: "${now + duration('1s')}"
`,
		},
		{
			// The same clock in the position that grew an expression second.
			// `sleep:` took a literal duration and nothing else until computed
			// durations landed, so this file could not be written at all — and the
			// day it could, `now` was bound in it while the rewriter knew about
			// `wait_until:` alone. A `sleep:` whose value is a scalar expression is
			// the same node shape the case above covers, so it is the cheap half of
			// the extent; the mapping below is the other one.
			name: "a step is called now, beside a computed sleep that reads the clock",
			source: `edition: v2026.3
name: shadow-now-sleep
steps:
  - id: now
    log:
      message: a step called now
  - id: hold
    sleep: "${(now + duration('1s')) - now}"
`,
		},
		{
			// The extent, in the shape that is genuinely different. A `wait_until:`
			// and a computed `sleep:` are scalars hanging off their own key;
			// `wait_for_signal:` is a *mapping*, and the expression that sees `now`
			// is a key inside it. So the subtraction has to survive one more level of
			// descent, and a rewriter that subtracted at the key holding the
			// expression rather than at the key opening the wait would be right on
			// both scalars and wrong here.
			//
			// It cannot be subtracted at `timeout:` either, which is the other half
			// of why this case exists: a bare `timeout:` on an ordinary step is that
			// step's activity timeout, an ordinary duration evaluated where there is
			// no clock at all.
			name: "a step is called now, beside a signal timeout computed from the clock",
			source: `edition: v2026.3
name: shadow-now-timeout
steps:
  - id: now
    log:
      message: a step called now
  - id: gate
    wait_for_signal:
      name: sign-off
      timeout: "${(now + duration('1h')) - now}"
`,
		},
		{
			// The fourth binding position, and the one that binds three names
			// rather than one. A `wait_for_signal:`'s `outputs:` sees the wait's
			// own result — `payload`, `sender`, `timed_out` — bound bare, and all
			// three are ordinary words a step may legitimately be called. Rooted,
			// `payload.approved` becomes `steps.payload.approved`: still valid,
			// still passing `flow validate`, and computing the outputs of a
			// logging step instead of the gate.
			//
			// All three in one file on purpose. A rewriter that learned two of
			// them would corrupt exactly the file it did not know about, which is
			// the failure `bindsNow` was widened to fix a release ago.
			name: "steps share the names a gate's outputs shaping binds",
			source: `edition: v2026.3
name: shadow-shaping
steps:
  - id: payload
    log:
      message: a step called payload
  - id: sender
    log:
      message: a step called sender
  - id: timed_out
    log:
      message: a step called timed_out
  - id: gate
    wait_for_signal:
      name: sign-off
      timeout: 1h
      outputs:
        approved: "${has(payload.approved) && payload.approved}"
        who: "${sender.identity.subject}"
        lapsed: "${timed_out}"
`,
		},
		{
			// `now` is bound in the shaping block too — it is a wait, and the
			// clock follows the node kind — so the subtraction that covers the
			// whole `wait_for_signal:` subtree has to survive descending one
			// further level into `outputs:`, where a second subtraction has
			// already narrowed the same map.
			name: "a step is called now, beside a gate's outputs shaping that reads the clock",
			source: `edition: v2026.3
name: shadow-now-shaping
steps:
  - id: now
    log:
      message: a step called now
  - id: gate
    wait_for_signal:
      name: sign-off
      outputs:
        answered_before: "${now < now + duration('1h')}"
`,
		},
		{
			// `run` is a root (#206), not a name the grammar binds bare the way a
			// loop's `as:` or `now` is — but the risk is the identical shape: a bare
			// identifier that means something other than a step, sitting in a file
			// that also has steps, where a rewriter that only asks "is this a step
			// id anywhere in the document" would still get it right by construction.
			// No step here is called `run` — `flow validate` refuses that collision
			// outright, see declarations.go — so this is the case that must never be
			// touched at all: nothing here is a step reference to root.
			name: "a bare reference to the run root, beside ordinary steps",
			source: `edition: v2026.3
name: shadow-run-root
steps:
  - id: approval
    log:
      message: "${'requested by ' + run.identity.subject}"
  - id: deploy
    if: "${!run.local && run.identity.subject != ''}"
    log:
      message: deploying
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
		{
			// Both directions of the clock's own scope, in the position the
			// byte-comparison table above cannot reach: that table asserts a file is
			// left alone, so it can only ever prove the subtraction happened, never
			// that it stopped where it should.
			//
			// Here a step is called `now` *and* referenced the legacy way outside the
			// wait. The reference has to be rooted — it is a step — while the `now`
			// inside the computed `sleep:` has to stay bare, because there it is the
			// clock. Subtracting for the step rather than for the wait's own value
			// would leave the first bare in a file stamped with the new edition,
			// which is the failure this whole test is named for.
			name: "a computed sleep sees the clock, and the rest of the step sees the step called now",
			source: `edition: 2026.1
name: nowsleepscope
steps:
  - id: now
    echo:
      message: "5s"
  - id: hold
    if: "${now.result != ''}"
    sleep: "${duration(now.result)}"
`,
			// The `if:` is outside the wait's own value, so `now.result` there is the
			// step and migrates with it.
			rooted: "vars.now",
			bareIn: `sleep: "${duration(now.result)}"`,
		},
		{
			// The same pair one level deeper, through the mapping shape. `timeout:`
			// is a key inside `wait_for_signal:`, so the clock is bound across a
			// descent the two scalar positions never make — and the step called `now`
			// is still an ordinary step everywhere outside that mapping.
			name: "a signal's timeout sees the clock, and the rest of the step sees the step called now",
			source: `edition: 2026.1
name: nowtimeoutscope
steps:
  - id: now
    echo:
      message: "1h"
  - id: gate
    if: "${now.result != ''}"
    wait_for_signal:
      name: sign-off
      timeout: "${(now + duration('1h')) - now}"
`,
			rooted: "vars.now",
			bareIn: `timeout: "${(now + duration('1h')) - now}"`,
		},
		{
			// The extent of the shaping binding, from the side the byte-comparison
			// table cannot see. `payload` is a step *and* the wait's own result, and
			// which one a reference means is decided by whether it is inside
			// `outputs:` — so the `if:` on the same step migrates and the shaping
			// expression does not.
			//
			// Subtracting the three names for the whole `wait_for_signal:` subtree
			// instead of for `outputs:` alone would leave `${payload.result}` in the
			// `timeout:` below bare, in a file stamped with the new edition, which
			// the validator then rejects — the "too wide" half of this failure.
			name: "a gate's outputs see the wait's result, and its timeout sees the step called payload",
			source: `edition: v2026.3
name: payloadscope
steps:
  - id: payload
    sleep: 1s
  - id: gate
    wait_for_signal:
      name: sign-off
      timeout: "${payload.timed_out ? duration('1h') : duration('2h')}"
      outputs:
        approved: "${has(payload.approved) && payload.approved}"
`,
			// Two keys of one mapping, and the same word means different things in
			// each. Only `outputs:` binds the wait's result, so the `timeout:` beside
			// it is naming the step and has to be rooted — subtracting the three
			// names for the whole `wait_for_signal:` subtree instead would leave this
			// bare in a file the validator then rejects, which is the "too wide" half
			// of the failure and the half no byte-comparison case can see.
			rooted: `timeout: "${steps.payload.timed_out ? duration('1h') : duration('2h')}"`,
			bareIn: `approved: "${has(payload.approved) && payload.approved}"`,
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

	const want = `edition: v2026.3
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
