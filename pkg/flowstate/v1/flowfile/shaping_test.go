package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// One spelling for output shaping, and what it buys.
//
// A `wait_for_signal:` has always shaped its outputs as a mapping of name to
// value. A task shaping its own outputs was written as a CEL map literal
// smuggled through a quoted string, because a `: ` in a plain YAML scalar is
// mapping syntax — four files in the corpus stopped to apologise for the quoting
// in a comment, which is a language telling you the spelling is wrong.
//
// The mapping form is now the contract for both. The tests below are in three
// groups, one per thing that can go wrong:
//
//   - the compiler keeps the names, so the shaped set survives into the
//     specification;
//   - the validator reports against that set, and refuses the shapes whose names
//     could never be known;
//   - `flow fix` promotes the old spelling into the new one without changing what
//     the file computes, which is asserted by comparing bytes.

const shapedPrelude = `edition: v2026.3
name: t
steps:
  - id: fetch
    http:
      url: https://example.com
`

// TestShapedOutputsCompileToNames is the whole argument for the mapping form in
// one assertion: the names an author wrote are in the compiled workflow.
//
// The older spelling compiles to one expression that happens to build a map, so
// nothing downstream can say what the step produces until it has run. Everything
// else in this file rests on the difference.
func TestShapedOutputsCompileToNames(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(shapedPrelude +
		"      outputs:\n" +
		"        code: ${response.status_code}\n" +
		"        said: hello\n"))
	require.NoError(t, err)

	shaping := wf.GetSteps()[0].GetTask().GetInputs()[v1.ShapingInput]
	names, knowable := v1.ShapedOutputNames(shaping)
	require.True(t, knowable, "a mapping's keys are written down, so they are knowable")
	assert.Equal(t, []string{"code", "said"}, names)

	entries := shaping.GetStructure().GetMap().GetEntries()
	require.Len(t, entries, 2)
	assert.NotNil(t, entries["code"].GetExpr(), "a fenced entry is an expression")
	assert.Equal(t, "hello", entries["said"].GetLiteral().GetStringValue(),
		"an unfenced entry is a literal, exactly as it is in every other input position")
}

// TestShapedOutputsKeepTheStringFormLegal pins the other half of the decision.
//
// Nothing retires here. A map built by an expression is the spelling for a shape
// only the run can know, and it stays legal, stays compiling, and stays
// deliberately unchecked.
func TestShapedOutputsKeepTheStringFormLegal(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(shapedPrelude +
		"      outputs: '${ {\"code\": response.status_code} }'\n"))
	require.NoError(t, err)

	shaping := wf.GetSteps()[0].GetTask().GetInputs()[v1.ShapingInput]
	require.NotNil(t, shaping.GetExpr(), "the string form is still one expression")

	names, knowable := v1.ShapedOutputNames(shaping)
	assert.True(t, knowable, "a literal-keyed map literal is still readable")
	assert.Equal(t, []string{"code"}, names)
}

// TestShapedOutputsRefuseWhatCannotBeANameCovers the two shapes a mapping must
// not take, each with the position and the sentence that says what to write.
func TestShapedOutputsRefuseWhatCannotBeAName(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		src      string
		contains string
	}{
		{
			// A computed key is a shaped set only the run knows, written in the
			// one spelling that promises the opposite. Refused rather than
			// silently compiled, and pointed at the older spelling, which is
			// where a genuinely dynamic shape belongs.
			name:     "a computed key",
			src:      "      outputs:\n        ${vars.name}: ${response.status_code}\n",
			contains: "computes an output name",
		},
		{
			// `outputs:` replaces, so an empty one is a step that deliberately
			// produces nothing. The wait's own diagnostic, said about a task.
			name:     "an empty mapping",
			src:      "      outputs: {}\n",
			contains: "would have no outputs at all",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			out := diagnose(t, shapedPrelude+test.src)
			assert.Contains(t, out, test.contains)
			assert.Regexp(t, `\d+:\d+`, out, "a refusal names the position")
		})
	}
}

// TestShapedOutputsAreCheckedByName is the wait's diagnostic, ported.
//
// This is what the mapping form is *for*: `outputs:` drops what the task
// produces, so a later reference to a dropped name reads nothing at all and
// every branch built on it quietly takes the other arm.
func TestShapedOutputsAreCheckedByName(t *testing.T) {
	t.Parallel()

	src := shapedPrelude +
		"      outputs:\n" +
		"        code: ${response.status_code}\n" +
		"  - id: after\n    log:\n      message: ${steps.fetch.codee}\n"

	out := diagnose(t, src)
	assert.Contains(t, out, `has no output "codee"`)
	assert.Contains(t, out, `did you mean "code"?`)
}

// TestShapedOutputsSayWhatShapingDropped covers the re-exposure sentence: a name
// the task declares, gone because shaping replaced it.
func TestShapedOutputsSayWhatShapingDropped(t *testing.T) {
	t.Parallel()

	src := shapedPrelude +
		"      outputs:\n" +
		"        code: ${response.status_code}\n" +
		"  - id: after\n    log:\n      message: ${steps.fetch.body}\n"

	out := diagnose(t, src)
	assert.Contains(t, out, "which shaping dropped")
	assert.Contains(t, out, "re-expose it")
}

// TestUncheckableShapingIsSilent is the negative direction, and the one a false
// diagnostic would live in.
//
// A map whose keys the run decides produces names nothing here can enumerate, so
// every reference to that step is left alone. Reporting one would be a validator
// telling an author a working file is wrong.
func TestUncheckableShapingIsSilent(t *testing.T) {
	t.Parallel()

	src := shapedPrelude +
		"      outputs: '${ {vars.name: response.status_code} }'\n" +
		"  - id: after\n    log:\n      message: ${steps.fetch.anything}\n"

	assert.Empty(t, diagnose(t, src))
}

// TestFixPromotesAMapLiteralToTheMappingForm compares bytes, per CLAUDE.md: a
// rewriter asserted only to produce something that still validates is how this
// command corrupted two files.
func TestFixPromotesAMapLiteralToTheMappingForm(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a single-line map literal",
			src:  "      outputs: '${ {\"id\": response.json.id} }'\n",
			want: "      outputs:\n        id: ${response.json.id}\n",
		},
		{
			// The block-scalar spelling the corpus reached for once a shaping
			// grew past one line. Its lines are the value's, so the whole run is
			// replaced.
			name: "a folded block scalar",
			src: "      outputs: >-\n" +
				"        ${ {\"id\": response.json.id,\n" +
				"            \"name\": response.json.name} }\n",
			want: "      outputs:\n        id: ${response.json.id}\n        name: ${response.json.name}\n",
		},
		{
			// The entry that still needs quoting, and only that entry. A `: `
			// inside a ternary is YAML mapping syntax, so the apology survives
			// on one line instead of covering the whole shaping.
			name: "an entry whose expression holds a colon",
			src:  "      outputs: '${ {\"n\": has(response.json.n) ? response.json.n : -1} }'\n",
			want: "      outputs:\n        n: '${has(response.json.n) ? response.json.n : -1}'\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(shapedPrelude + test.src))
			require.NoError(t, err)
			require.Empty(t, result.Refusals)
			require.Equal(t, shapedPrelude+test.want, string(result.Source))

			// And the rewritten file compiles, which is the other half of the
			// pair: bytes say it was not mangled, compiling says it still means
			// something.
			require.Empty(t, diagnose(t, string(result.Source)))
		})
	}
}

// TestFixPromotesALoopsCarriedState covers the other two positions, where the
// gain is a constant that no longer needs a fence.
func TestFixPromotesALoopsCarriedState(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: count
    loop:
      as: acc
      init: "${ {'n': 1, 'sum': 0} }"
      update: "${ {'n': acc.n + 1, 'sum': acc.sum + acc.n} }"
      until: ${acc.n >= 3}
      max_iterations: 10
      steps:
        - id: body
          log:
            message: ${string(acc.n)}
`
	want := `edition: v2026.3
name: t
steps:
  - id: count
    loop:
      as: acc
      init:
        n: 1
        sum: 0
      update:
        n: ${acc.n + 1}
        sum: ${acc.sum + acc.n}
      until: ${acc.n >= 3}
      max_iterations: 10
      steps:
        - id: body
          log:
            message: ${string(acc.n)}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)
	require.Equal(t, want, string(result.Source))
	require.Empty(t, diagnose(t, string(result.Source)))
}

// TestFixDoesNotRootANameTheGrammarBindsWhilePromoting is the adversarial case.
//
// A loop's `as:` binds a name bare for its body, its `until:` and its `update:` —
// and a step of the same id is deliberately legal beside it. Promoting `update:`
// into the mapping form moves that bare name onto a new line, which is precisely
// where the reference rewriter would next see it. If the two passes disagree
// about what `acc` is, the file comes back computing the *step's* outputs
// instead, still valid, still silent.
//
// Compared as bytes for that reason: this file validates either way.
func TestFixDoesNotRootANameTheGrammarBindsWhilePromoting(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: acc
    value: ${"a step deliberately named like the loop's binding"}
  - id: count
    loop:
      as: acc
      init: "${ {'n': 1} }"
      update: "${ {'n': acc.n + 1} }"
      until: ${acc.n >= 3}
      max_iterations: 10
      steps:
        - id: body
          log:
            message: ${string(acc.n)}
`
	want := `edition: v2026.3
name: t
steps:
  - id: acc
    value: ${"a step deliberately named like the loop's binding"}
  - id: count
    loop:
      as: acc
      init:
        n: 1
      update:
        n: ${acc.n + 1}
      until: ${acc.n >= 3}
      max_iterations: 10
      steps:
        - id: body
          log:
            message: ${string(acc.n)}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)
	require.Equal(t, want, string(result.Source),
		"`acc` in `update:` is the loop's binding, not the step of that name")
	require.NotContains(t, string(result.Source), "steps.acc")
}

// TestFixLeavesShapingItCannotPromoteAlone is the silence half.
//
// Each of these is a working file. A rewriter that acted on any of them would be
// changing what an author wrote for its own reasons, which is the failure this
// command cannot afford.
func TestFixLeavesShapingItCannotPromoteAlone(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			// Keys the run decides. The whole reason the string form stays.
			name: "a computed key",
			src:  "      outputs: '${ {vars.name: response.json.id} }'\n",
		},
		{
			// A key that is not a plain YAML name would have to be quoted, which
			// changes how the key is written.
			name: "a key needing quotes",
			src:  "      outputs: '${ {\"not a name\": response.json.id} }'\n",
		},
		{
			// Replacing the run would delete the comment in it.
			name: "a comment inside the value",
			src:  "      outputs: # the shape this step produces\n        '${ {\"id\": response.json.id} }'\n",
		},
		{
			// Not a map at all: whatever this evaluates to, this rewrite has
			// nothing to say about it.
			name: "an expression that is not a map literal",
			src:  "      outputs: ${vars.shape}\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			src := shapedPrelude + test.src
			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.Empty(t, result.Refusals)
			assert.Equal(t, src, string(result.Source), "left byte for byte alone")
		})
	}
}
