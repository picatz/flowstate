package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every bound in this package is against input an outside party chooses — the
// language server compiles whatever an editor opens, and a server compiles
// whatever is submitted — so each one needs a test that reaches it.
//
// The rule these are written to is that a bound has to match the shape of the
// attack. Depth bounds do not stop breadth explosions; a bound on the values a
// walk descends into does not stop values produced without descending.

// The *compiler's* anchor, alias, and merge-key expansion bounds are no longer
// reachable from [flowfile.Parse]: the grammar is a strict subset of YAML that
// refuses all three at the document tree before [compiler.collectAnchors] or any
// call to [compiler.entries] runs (parse.go, strict.go), so the billion-laughs
// shape their tests fed is refused on the *presence* of the construct, before any
// expansion — strict_test.go's TestStrictYAMLRefusesBillionLaughsWithoutExpanding.
//
// The bounds themselves did not go away with those tests, and neither did every
// path to them. [flowfile.CallPins] and [flowfile.Format] read a document that
// need not compile — `flow fix` reads the pins of every file in a tree to report
// staleness it caused (cmd/flow/fixstale.go), including files its own strict
// refusal left alone — and the walk they share resolves anchors, aliases and
// merge keys (callpins.go's pinCollector) under exactly the two bounds the
// compiler used to enforce: maxNodes on total expansion, maxAliasDepth on chain
// length. That is hostile input reaching a live bound with no refusal in front of
// it, so the two below drive each bound to the point it fires. See #653, #841.
//
// One more correction to that first paragraph, because reading it as "the
// compiler's node budget is dead" is how a live bound loses its last test. What
// #840 made unreachable is the *alias-driven* half — the merge branch in
// fields.go and [compiler.resolve]. The budget on the walk itself
// ([compiler.enter], parse.go) is untouched by the strict profile, because a
// document needs no anchor, alias or merge key to hold more values than the
// budget allows: two bytes per value in flow style puts 120,000 of them well
// inside the byte limit. TestCompilingRefusesADocumentPastTheValueBudget drives
// that one, on the surface every compile goes through.

// TestCallPinsRefusesABillionLaughsAtTheNodeBudget drives [maxNodes] with the
// document it exists for.
//
// Aliases multiply breadth rather than depth: nine levels of nine references
// each is 9^9 ≈ 387 million nodes expanded, out of a few hundred bytes on disk.
// The pin collector follows aliases — it has to, because a `digest:` may arrive
// through one (#640) — so nothing about the strict profile saves this walk. The
// budget is what does, and this asserts the budget is *reached*: the answer is
// the node-count refusal naming the limit, not some other failure and not a
// listing of pins.
func TestCallPinsRefusesABillionLaughsAtTheNodeBudget(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: boom\n")
	b.WriteString("l0: &l0 \"lol\"\n")
	for i := 1; i <= 9; i++ {
		level := strconv.Itoa(i)
		b.WriteString("l" + level + ": &l" + level + " [")
		for j := 0; j < 9; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString("*l" + strconv.Itoa(i-1))
		}
		b.WriteString("]\n")
	}
	// A real pin, so the walk has every reason to run to completion rather than
	// stopping early on a document with nothing in it to find.
	b.WriteString("steps:\n  - id: s\n    call: ./callee.yaml\n    digest: sha256:abc\n")

	src := []byte(b.String())
	require.Less(t, len(src), 4096, "premise: the bomb is tiny on disk; only expansion makes it large")

	pins, err := flowfile.CallPins(src)
	require.Error(t, err, "an alias bomb must be refused, not walked to the end")
	assert.Contains(t, err.Error(), "holds more than 100000 values once aliases are expanded",
		"the refusal must be the node budget, so the test fails if some other limit starts catching this first")
	assert.Empty(t, pins)
}

// TestCallPinsStopsFollowingAnAliasChainAtTheDepthBound drives [maxAliasDepth],
// the other half, and asserts the bound both from below and from above: a chain
// one link short of the limit resolves to the digest it names, and a chain past
// it is refused. A bound only ever tested from above is also satisfied by a walk
// that gave up immediately (CLAUDE.md: assert the bound was reached).
//
// Refusal here is fail-closed rather than a dropped pin, which is the property
// worth pinning down: a `digest:` is a security check, so a collector that
// cannot read one says so instead of reporting a file as holding no pins.
func TestCallPinsStopsFollowingAnAliasChainAtTheDepthBound(t *testing.T) {
	t.Parallel()

	// Each link is an anchor whose value is an alias of the link before it. YAML
	// refuses that inline (`&a1 *a0`), so it is written in block form, which is
	// the spelling a hostile document would have to use too.
	chain := func(links int) []byte {
		var b strings.Builder
		b.WriteString("edition: v2026.3\nname: chain\na0: &a0 sha256:abc\n")
		for i := 1; i <= links; i++ {
			b.WriteString("a" + strconv.Itoa(i) + ": &a" + strconv.Itoa(i) + "\n  *a" + strconv.Itoa(i-1) + "\n")
		}
		b.WriteString("steps:\n  - id: s\n    call: ./callee.yaml\n    digest: *a" + strconv.Itoa(links) + "\n")
		return []byte(b.String())
	}

	// One link short of the bound: followed all the way to the anchor at the end.
	pins, err := flowfile.CallPins(chain(31))
	require.NoError(t, err, "a chain within the bound must be followed, or the bound is untested from below")
	require.Len(t, pins, 1)
	assert.Equal(t, "sha256:abc", pins[0].Digest)

	// One link past it: refused rather than followed, and refused out loud.
	pins, err = flowfile.CallPins(chain(32))
	require.Error(t, err, "a chain past the bound must be refused, not followed")
	assert.Contains(t, err.Error(), "could not be read as text",
		"a pin the collector cannot resolve is reported, not treated as absent")
	assert.Empty(t, pins)

	// Far past it, to show the answer is the bound firing rather than an
	// accident of that one length.
	_, err = flowfile.CallPins(chain(256))
	require.Error(t, err)
}

// valueBudgetDocument is a Flowfile whose one `value:` step holds a flow
// sequence of n values, and nothing else out of the ordinary.
//
// Flow style rather than block, because the interesting property is the ratio
// between what is on disk and what is walked: `1,` is two bytes per value, so a
// document past the 100,000-value budget still fits well inside the byte limit
// that would otherwise catch it first. The same document written as a block
// sequence is over 2 MiB and never reaches the budget at all — [maxBytes]
// refuses it on sight, which is a different bound answering a different question.
func valueBudgetDocument(n int) []byte {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: budget\nsteps:\n  - id: a\n    value: [")
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('1')
	}
	b.WriteString("]\n")
	return []byte(b.String())
}

// TestCompilingRefusesADocumentPastTheValueBudget drives the *compiler's* copy
// of [maxNodes] — parse.go's [compiler.enter], the one every compile walks
// through — with the input that still reaches it.
//
// The bound's alias-driven tests went with #840, and the merge branch beside it
// (fields.go) is genuinely unreachable now: the strict profile refuses an alias
// or a merge key at the document tree before anything resolves or expands. The
// budget on the *walk* is not, and never was, about aliases alone. A document
// with no anchor, alias or merge key in it can hold as many values as its bytes
// allow, and the language server compiles whatever an editor opens — so this is
// hostile input reaching a live bound with no refusal in front of it, which
// after #840 had no test at all. See #841.
//
// The assertion is on the message naming the value limit, not merely on some
// error, so the test fails rather than passes if [maxBytes] or [maxDepth] starts
// catching this shape first — three bounds guarding three different resources,
// and a test that cannot tell them apart proves none of them.
func TestCompilingRefusesADocumentPastTheValueBudget(t *testing.T) {
	t.Parallel()

	src := valueBudgetDocument(120_000)
	require.Less(t, len(src), 1<<20,
		"premise: the document is inside the byte limit, so the value budget is what answers")

	ds, err := flowfile.ValidateSource(src)
	require.Error(t, err, "a document past the value budget must be refused")
	assert.Contains(t, err.Error(), "holds more than 100000 values",
		"the refusal must be the value budget, named, rather than any other limit")
	assert.NotContains(t, err.Error(), "larger than the",
		"the byte limit answering instead would make this test prove nothing about the value budget")
	assert.Empty(t, ds, "the refusal is reported as the compile error, not as a diagnostic list")
}

// TestCompilingAcceptsADocumentInsideTheValueBudget is the other side of it.
//
// A bound only ever tested from above is also satisfied by a compiler that
// refuses everything: `nodes <= maxNodes` holds trivially for a walk that gave
// up on the first value. This document is the same shape, one order of magnitude
// under the budget, and it has to compile clean — which is also what makes the
// test above evidence that the budget is what fired rather than the document
// being malformed in some way the size hid.
func TestCompilingAcceptsADocumentInsideTheValueBudget(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource(valueBudgetDocument(10_000))
	require.NoError(t, err, "a document well inside the budget must compile")
	assert.Empty(t, ds, "and hold no diagnostics: the shape is legitimate, only large")
}

// The root is the one name rooting *creates* a collision for, which is worth
// stating plainly in a change that is otherwise about deleting collision rules.
//
// It has to be refused at compile time rather than left to resolve, because the
// runtime deliberately lets a step of this name win: a spec compiled before the
// root existed may contain one, and a worker replaying it must resolve the way it
// always did. That compatibility is only safe while no *new* file can create the
// situation.

// TestNothingMayBeCalledSteps covers every route a name reaches an expression's
// scope by, because closing one and leaving the others is how this kind of hole
// survives.
func TestNothingMayBeCalledSteps(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"a top-level step": `edition: v2026.3
name: t
steps:
  - id: steps
    log:
      message: hi
`,
		"a step inside a loop body": `edition: v2026.3
name: t
steps:
  - id: a
    for_each:
      items: ${[1]}
      steps:
        - id: steps
          log:
            message: hi
`,
		"a step inside a parallel branch": `edition: v2026.3
name: t
steps:
  - id: a
    parallel:
      - steps:
          - id: steps
            log:
              message: hi
`,
		// The other route into a body's scope. A bound name wins over the scope it
		// is bound into, so this hides every step from exactly the place rooted
		// references are written.
		"a loop iterator": `edition: v2026.3
name: t
steps:
  - id: a
    for_each:
      items: ${[1]}
      as: steps
      steps:
        - id: b
          log:
            message: hi
`,
	}

	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err, "the document is valid YAML; the name is a semantic problem")
			require.NotEmpty(t, ds, "%s called `steps` was accepted", name)
			assert.Contains(t, ds.Error(), "hide all",
				"the diagnostic has to say what goes wrong, not only that the name is taken")
		})
	}
}

// TestAStepCalledStepsWouldHaveFailedAtRunTime is the evidence the rule above is
// worth having, rather than a name reserved out of tidiness.
//
// Without the refusal this document validates clean and then dies on its third
// step with `no such key: other` — the shape of failure this repo cares most
// about, because nothing an author can see says why.
func TestAStepCalledStepsWouldHaveFailedAtRunTime(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: shadowed-root
steps:
  - id: steps
    log:
      message: i am a step called steps
  - id: other
    http:
      url: https://example.com
  - id: read
    log:
      message: ${steps.other.body}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, ds)

	// The reference is *not* what is reported. It is correct; the id is the
	// problem, and a diagnostic on the reference would send an author to fix the
	// wrong line.
	assert.NotContains(t, ds.Error(), "unknown step",
		"the reference is fine; the id is what has to change")
}
