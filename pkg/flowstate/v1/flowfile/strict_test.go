package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The Flowfile grammar is a strict subset of YAML: anchors, aliases, and merge
// keys are refused. These cover the three refusals in both directions — the
// construct is rejected with a positioned diagnostic, and the equivalent value
// spelled out is accepted — plus the billion-laughs shape the refusal exists to
// stop, which must be refused without ever expanding. See #653 and strict.go.

// firstDiagnostic pulls the earliest-positioned diagnostic out of a parse error,
// so a test can assert the exact line, column, and text a construct is refused
// at rather than only that something failed.
func firstDiagnostic(t *testing.T, err error) flowfile.Diagnostic {
	t.Helper()
	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.NotEmpty(t, ds)
	return ds[0]
}

func TestStrictYAMLRefusesAnchor(t *testing.T) {
	t.Parallel()

	// The anchor sits on the first step. `&shared` is at column 5, under the
	// two-space list indent and the `- ` marker.
	src := `edition: v2026.3
name: t
steps:
  - &shared
    id: a
    log:
      message: hi
`
	_, _, err := flowfile.Parse([]byte(src))
	d := firstDiagnostic(t, err)
	assert.Equal(t, 4, d.Line)
	assert.Equal(t, 5, d.Column)
	assert.Contains(t, d.Message, "an anchor (`&shared`) is not part of the Flowfile grammar")
	assert.Contains(t, d.Message, "write the value out", "the diagnostic must say what to do instead")
}

func TestStrictYAMLRefusesAlias(t *testing.T) {
	t.Parallel()

	// `*base` is the value of `message:`, at column 16.
	src := `edition: v2026.3
name: t
vars:
  base: hi
steps:
  - id: a
    log:
      message: *base
`
	_, _, err := flowfile.Parse([]byte(src))
	d := firstDiagnostic(t, err)
	assert.Equal(t, 8, d.Line)
	assert.Equal(t, 16, d.Column)
	assert.Contains(t, d.Message, "an alias (`*base`) is not part of the Flowfile grammar")
	assert.Contains(t, d.Message, "write the value out here", "the diagnostic must say what to do instead")
}

func TestStrictYAMLRefusesMergeKey(t *testing.T) {
	t.Parallel()

	// The `<<:` is on the second step, at column 5.
	src := `edition: v2026.3
name: t
steps:
  - &policy
    id: a
    timeout: 30s
    log:
      message: one
  - id: b
    <<: *policy
    log:
      message: two
`
	_, _, err := flowfile.Parse([]byte(src))
	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)

	// All three constructs in this file are named, so an author is not sent back
	// one recompile at a time.
	joined := ds.Error()
	assert.Contains(t, joined, "an anchor (`&policy`)")
	assert.Contains(t, joined, "an alias (`*policy`)")

	// The merge key itself, positioned.
	var merge *flowfile.Diagnostic
	for i := range ds {
		if strings.Contains(ds[i].Message, "a merge key (`<<:`)") {
			merge = &ds[i]
			break
		}
	}
	require.NotNil(t, merge, "the merge key must be reported")
	assert.Equal(t, 10, merge.Line)
	assert.Equal(t, 5, merge.Column)
	assert.Contains(t, merge.Message, "write each key it would merge in directly")
}

// TestStrictYAMLAcceptsTheSpelledOutEquivalent is the other direction: the value
// a merge key would have shared, written directly on each step, compiles and
// means what the merge would have meant.
func TestStrictYAMLAcceptsTheSpelledOutEquivalent(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: shared
steps:
  - id: a
    timeout: 30s
    continue_on_error: true
    log:
      message: one
  - id: b
    timeout: 30s
    continue_on_error: true
    log:
      message: two
`
	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Len(t, wf.GetSteps(), 2)
	for _, step := range wf.GetSteps() {
		assert.True(t, step.GetPolicy().GetContinueOnError(), "step %q", step.GetId())
	}
}

// TestStrictYAMLRefusesBillionLaughsWithoutExpanding is the security property.
//
// A billion-laughs document has depth one per alias and multiplies breadth at
// every level, so a bound that expanded first would allocate an enormous tree
// before it could refuse. The refusal reads only the nodes the author wrote — a
// handful of anchors and aliases — and never follows one, so the document is
// rejected in the time it takes to walk what is on disk. Run under a tight
// memory limit by `go test`, this proves the refusal precedes expansion: were an
// alias ever followed, the nested lists would exhaust the budget rather than
// return a diagnostic.
func TestStrictYAMLRefusesBillionLaughsWithoutExpanding(t *testing.T) {
	t.Parallel()

	// Nine levels, nine references each: were this expanded it would be 9^9 ≈ 387
	// million leaf nodes. It is a few hundred bytes on disk.
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: boom\n")
	b.WriteString("l0: &l0 \"lol\"\n")
	for i := 1; i <= 9; i++ {
		b.WriteString("l" + strconv.Itoa(i) + ": &l" + strconv.Itoa(i) + " [")
		for j := 0; j < 9; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString("*l" + strconv.Itoa(i-1))
		}
		b.WriteString("]\n")
	}
	b.WriteString("steps:\n  - id: s\n    log:\n      message: hi\n")

	src := []byte(b.String())
	require.Less(t, len(src), 4096, "premise: the bomb is tiny on disk; only expansion makes it large")

	_, _, err := flowfile.Parse(src)
	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	// Reached the refusal, not some unrelated failure: the anchors and aliases
	// themselves are what is named.
	assert.Contains(t, ds.Error(), "not part of the Flowfile grammar")
}

// TestFixRefusesStrictYAML is the fixer's half, on the construct it still refuses.
//
// `flow fix` now carries an anchor and a whole-value alias across mechanically
// (fixalias.go, fixalias_test.go). A merge key it does not: `<<: *policy` beside a
// sibling key is a *precedence* rule, so writing the merged keys out means deciding
// which spelling of a colliding key the author meant — judgment, which this command
// does not exercise (#653). So the file is left byte for byte alone and the refusal
// is the compiler's own sentence, which is what keeps `flow fix` from ever emitting
// a file the compiler then rejects.
func TestFixRefusesStrictYAML(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
defaults: &shared
  timeout: 30s
steps:
  - <<: *shared
    id: a
    log:
      message: hi
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, result.Refusals, "a merge key must be refused, not silently passed through")
	assert.False(t, result.Complete())
	assert.Equal(t, src, string(result.Source), "a refused file is left byte for byte alone")
	assert.Contains(t, result.Refusals[0].Message, "a merge key (`<<:`) is not part of the Flowfile grammar")
}

// TestFixRefusesEachStrictConstructWhereItIsWritten covers the fixer's refusals
// the way a diagnostic is meant to be read: at a line and column an author can go
// to.
//
// The two cases are the two sources those sentences come from, which is worth
// keeping distinct. A merge key is refused in the *compiler's* words, built by the
// one collector both share (strict.go's strictYAMLRefusalsIn) — "same collector"
// is a claim only a test comparing the two can keep true. An alias the rewrite
// tried to inline and could not is refused in the rewrite's own words, naming what
// stopped it rather than repeating that aliases are refused; here the anchor the
// alias names is never declared, so there are no bytes to write in its place.
func TestFixRefusesEachStrictConstructWhereItIsWritten(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		src          string
		line, column int
		message      string
	}{
		"an alias": {
			src: `edition: v2026.3
name: t
vars:
  base: hi
steps:
  - id: a
    log:
      message: *base
`,
			line: 8, column: 16,
			message: "this alias names an anchor (`&base`) this document does not declare",
		},
		"a merge key": {
			src: `edition: v2026.3
name: t
defaults: &policy
  timeout: 30s
steps:
  - id: a
    <<: *policy
    log:
      message: hi
`,
			line: 7, column: 5,
			message: "a merge key (`<<:`) is not part of the Flowfile grammar",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// The premise both halves of this rest on: the compiler refuses the
			// file, so a rewrite of it could only ever be a file `flow validate`
			// then rejects.
			_, _, err := flowfile.Parse([]byte(test.src))
			require.Error(t, err, "premise: the construct must not compile")

			result, err := flowfile.Fix([]byte(test.src))
			require.NoError(t, err)
			require.NotEmpty(t, result.Refusals)
			assert.False(t, result.Complete())
			assert.Equal(t, test.src, string(result.Source), "a refused file is left byte for byte alone")

			var found *flowfile.Diagnostic
			for i := range result.Refusals {
				if strings.Contains(result.Refusals[i].Message, test.message) {
					found = &result.Refusals[i]
					break
				}
			}
			require.NotNil(t, found, "the construct itself must be named: %v", result.Refusals)
			assert.Equal(t, test.line, found.Line)
			assert.Equal(t, test.column, found.Column)
		})
	}
}

// TestFixRefusesBeforeItRewritesAnything is the ordering the refusal is only
// safe because of, and the one a test can hold in place.
//
// The fixture is a file `flow fix` would otherwise certainly change: it declares
// no edition, so the stamp applies (TestFixStampsAnEditionOntoAFileWithoutOne),
// and it writes the retired `task:` block. Adding one construct the rewriter
// refuses to it must turn the whole rewrite off — not narrow it — because a file
// that came back stamped and modernized *and* still holding a merge key is `flow
// fix . && git commit` succeeding on a file `flow validate` rejects, which is the
// outcome the fixer's refusal exists to prevent.
//
// The construct is a merge key rather than an anchor because an anchor is now
// carried across rather than refused (fixalias.go). What that changes is which
// construct demonstrates the property, not the property: [flowfile.FixResult]'s
// all-or-nothing rule is about any refusal, and this is the one that stays.
//
// It is also what keeps the fixer's own alias walks ([fixer.resolved],
// [fixer.collectAnchors]) out of reach of hostile input: they run in the
// expression pass, which is downstream of the strict check, and the inlining pass
// that replaced the refusal there hands them a document with no alias left in it.
// Should that order ever change, this fails rather than the bound quietly going
// live untested (#841).
func TestFixRefusesBeforeItRewritesAnything(t *testing.T) {
	t.Parallel()

	rewritable := `name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: hi
`
	// The premise: without the merge key this file is rewritten.
	result, err := flowfile.Fix([]byte(rewritable))
	require.NoError(t, err)
	require.True(t, result.Changed(), "premise: this file is one flow fix rewrites")
	require.Contains(t, string(result.Source), "edition: "+flowfile.CurrentEdition)

	withMerge := `name: t
defaults: &policy
  timeout: 30s
steps:
  - <<: *policy
    id: a
    task:
      name: log
      inputs:
        message: hi
`
	result, err = flowfile.Fix([]byte(withMerge))
	require.NoError(t, err)
	require.NotEmpty(t, result.Refusals)
	assert.False(t, result.Changed(), "no part of the rewrite may run on a refused file")
	assert.Equal(t, withMerge, string(result.Source))
	assert.NotContains(t, string(result.Source), "edition: ",
		"the edition stamp in particular: stamping a file the compiler refuses is the worst version of this")
}

// TestStrictRefusalIsNotGatedOnEdition pins the decision #840 made and #841 asked
// to have ratified rather than left implicit: the refusal is unconditional, not
// a property of the edition a file declares.
//
// The staged scope on #653 described an edition boundary — a newer edition
// refuses, the edition a file declares still parses — so an upgrade in place
// breaks nothing. #840 shipped unconditional refusal instead, and this is the
// argument for keeping it, written where it can fail: an edition in this
// repository is deliberately *not* a compatibility mechanism. "Declaring an
// older edition does not make an older grammar compile. There is one grammar in
// a build" (edition.go, the type's own doc comment) — so a boundary that let
// v2026.3 refuse an alias while v2026.2 still resolved one would be two grammars
// in one build, which is the exact cost the no-deprecation decision recorded in
// docs/DSL.md was made to avoid. The narrower rule wins because the wider one
// contradicts a rule already written down.
//
// The cost the refusal used to carry — for an edition `flow fix` exists to
// migrate, an anchor-bearing file got no migration at all — has since been bought
// back for the shapes that can be rewritten mechanically: `flow fix` writes a
// whole-value alias out and drops the anchor marker, then goes on to stamp the
// edition in the same run (fixalias.go, #653). What is unchanged is the compiler's
// answer, which is what this test is about, and what remains refused is the merge
// key — asserted below, because "the refusal is not gated on the edition" has to
// stay checkable on a construct that is still refused.
func TestStrictRefusalIsNotGatedOnEdition(t *testing.T) {
	t.Parallel()

	// Every edition this build recognises, so a new one added tomorrow is
	// covered by this test on the day it is added rather than whenever someone
	// remembers. The list itself is the source of truth (edition.go).
	editions := flowfile.KnownEditions()
	require.Contains(t, editions, flowfile.CurrentEdition, "premise: the current edition is a known one")
	require.Greater(t, len(editions), 1, "premise: there is more than one edition to be gated on")

	for _, edition := range editions {
		t.Run(edition, func(t *testing.T) {
			t.Parallel()

			withAnchor := "edition: " + edition + `
name: t
defaults: &policy
  timeout: 30s
steps:
  - id: a
    log:
      message: hi
`
			// The compiler refuses, in the same words, at the same position, no
			// matter which edition the file claims to be written in.
			_, _, err := flowfile.Parse([]byte(withAnchor))
			require.Error(t, err, "an anchor is refused whatever edition the file declares")
			d := firstDiagnostic(t, err)
			assert.Equal(t, 3, d.Line)
			assert.Contains(t, d.Message, "an anchor (`&policy`) is not part of the Flowfile grammar",
				"the anchor is what is reported — not the edition, which is not what is wrong")

			// `flow fix` carries that same file across, whatever edition it
			// declares: the marker goes and the edition is brought forward in the
			// one run, so the migration is not gated on the edition either.
			// Compared as bytes rather than by re-validating the output: a
			// rewrite that still validates can still have changed what the file
			// means, which is how this repository's two `flow fix` corruptions
			// got through.
			carried, err := flowfile.Fix([]byte(withAnchor))
			require.NoError(t, err)
			require.Empty(t, carried.Refusals)
			assert.Equal(t, "edition: "+flowfile.CurrentEdition+`
name: t
defaults:
  timeout: 30s
steps:
  - id: a
    log:
      message: hi
`, string(carried.Source))

			// And the construct that is still refused is refused whatever the
			// edition says too, byte for byte and un-stamped — which is the half
			// of this claim that needs a construct `flow fix` will not rewrite.
			withMerge := "edition: " + edition + `
name: t
defaults: &policy
  timeout: 30s
steps:
  - <<: *policy
    id: a
    log:
      message: hi
`
			result, err := flowfile.Fix([]byte(withMerge))
			require.NoError(t, err)
			require.NotEmpty(t, result.Refusals)
			assert.Equal(t, withMerge, string(result.Source), "a refused file is left byte for byte alone")
			assert.Contains(t, string(result.Source), "edition: "+edition,
				"including its declared edition: no file is stamped forward on the way to being refused")

			// The premise that makes the assertion above cost something. Without
			// the anchor, an older edition is a file `flow fix` migrates — so for
			// those editions the refusal is withholding a rewrite the author
			// would otherwise get, which is the price of the decision.
			withoutAnchor := "edition: " + edition + `
name: t
steps:
  - id: a
    log:
      message: hi
`
			clean, err := flowfile.Fix([]byte(withoutAnchor))
			require.NoError(t, err)
			require.Empty(t, clean.Refusals)
			if edition != flowfile.CurrentEdition {
				assert.True(t, clean.Changed(),
					"premise: an older edition is one flow fix brings forward, so the refusal above costs a migration")
				assert.Contains(t, string(clean.Source), "edition: "+flowfile.CurrentEdition)
			}
		})
	}
}
