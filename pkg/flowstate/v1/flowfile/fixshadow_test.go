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
