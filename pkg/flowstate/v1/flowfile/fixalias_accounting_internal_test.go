package flowfile

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/require"
)

// nonNegative folds an arbitrary int — including math.MinInt, which the
// fuzzer tries and which negating would overflow — into a non-negative one,
// so %-clamping [oracleFlowfile]'s randomized dimensions can never panic on
// a negative operand.
func nonNegative(n int) int {
	if n == math.MinInt {
		return 0
	}
	if n < 0 {
		return -n
	}

	return n
}

// #2045: the four review rounds a per-site charge lost to each found a
// different copy site the charge did not cover, and the corpus of shapes the
// last attempt wrote to close them still could not have caught the fourth —
// it varied bytes per line, and the fourth varies lines per byte. What
// closes the class rather than the instance is an oracle: run the rewrite
// and check that what it charged dominates what it actually built, over
// every shape a generator can vary independently, rather than over the
// shapes review happened to imagine.
//
// This file is that oracle, plus the adversarial input the issue also asks
// for — an alias chain refused before its expansion is materialized, proved
// by a bounded allocation rather than by the error string alone.

// oracleFlowfile builds a small, syntactically valid Flowfile holding one
// anchored leaf block and a chain of aliases above it, varying every
// dimension #2045 names as a distinct amplifier: how many lines the leaf
// holds, how long each one is, how many of them are blank, how deep the leaf
// is nested under (which is also how wide [aliasInliner.spliceBlock]'s
// indent shift is), how long a trailing comment on a leaf line runs, and how
// many chain levels sit above the leaf, each referencing the level below it
// twice.
//
// The chain matters for what a single reference cannot show. Every draw
// this generator makes starts [aliasInliner.bytes] seeded at the source's
// own length — the same convention [aliasInliner.nodes] already uses — so
// an accounting gap on a leaf referenced once is smaller than that seed and
// invisible to the oracle below no matter how the leaf is shaped. Doubling
// the reference at every chain level doubles the gap too, the same way it
// doubles the true cost, until it is large enough to be seen against the
// seed rather than lost in it — which is the property an adversary actually
// needs (#2045's own findings all depend on more than one copy) and the one
// a single-reference generator cannot exercise no matter how many shapes it
// tries.
//
// Kept small enough that most draws stay under this package's own budgets —
// a generator whose every draw is refused before [aliasInliner.appendLine]
// is ever reached checks nothing, since the property under test is what
// that accounting says about what it actually built, not that a refusal
// happened.
func oracleFlowfile(lines, lineLen, nestDepth, commentLen, blankEvery, chainLevels int) string {
	lines = 1 + nonNegative(lines)%12
	lineLen = nonNegative(lineLen) % 300
	nestDepth = nonNegative(nestDepth) % 6
	commentLen = nonNegative(commentLen) % 200
	blankEvery = 2 + nonNegative(blankEvery)%10
	chainLevels = nonNegative(chainLevels) % 6

	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n")

	// nestDepth extra mapping levels between `vars:` and the leaf anchor,
	// each one wider than the last — this is what makes spliceBlock's
	// indent shift (finding 1) something a draw can actually exercise,
	// rather than a fixed one-level nesting every draw shares.
	indent := "  "
	for level := range nestDepth {
		fmt.Fprintf(&b, "%sparent%d:\n", indent, level)
		indent += "  "
	}

	fmt.Fprintf(&b, "%slevel0: &level0\n", indent)
	blockIndent := indent + "  "
	for i := range lines {
		if blankEvery > 0 && i%blankEvery == 0 {
			b.WriteString("\n")

			continue
		}

		fmt.Fprintf(&b, "%sk%d: %s", blockIndent, i, strings.Repeat("x", lineLen))
		if commentLen > 0 && i%3 == 0 {
			fmt.Fprintf(&b, " # %s", strings.Repeat("c", commentLen))
		}
		b.WriteString("\n")
	}

	// Each chain level doubles the leaf's own reach, written under the same
	// nestDepth indent as the leaf so the shift width stays comparable at
	// every level rather than growing with the chain itself.
	for level := 1; level <= chainLevels; level++ {
		fmt.Fprintf(&b, "%slevel%d: &level%d\n", indent, level, level)
		fmt.Fprintf(&b, "%s  a: *level%d\n", indent, level-1)
		fmt.Fprintf(&b, "%s  b: *level%d\n", indent, level-1)
	}

	// The alias, at the top level rather than nested, so the shift width
	// [aliasInliner.spliceBlock] computes is never zero.
	fmt.Fprintf(&b, "  use: *level%d\n", chainLevels)
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// checkAliasInlinerAccountingOracle is the invariant every draw checks:
// what [aliasInliner.bytes] charged has to be at least what the rewrite
// actually wrote into every recorded edit — in.bytes >=
// Σ(len(line)+len(terminator)) over every [lineEdit.replacement] line. A
// charge that fell short of this on any draw is exactly #2045's shape of
// defect: a copy site review did not think to cover.
func checkAliasInlinerAccountingOracle(t *testing.T, src string) {
	t.Helper()

	data := []byte(src)
	file, err := parser.ParseBytes(data, parser.ParseComments)
	if err != nil {
		return
	}

	in, _, ok := runAliasInliner(data, file)
	if !ok {
		// Refused — by either budget, or by a shape this rewrite declines for
		// a reason unrelated to size. Nothing was built, so there is nothing
		// for the oracle to check the charge against.
		return
	}

	var produced int
	for _, edit := range in.f.edits {
		for _, line := range edit.replacement {
			produced += len(line) + len(in.f.terminator)
		}
	}

	require.GreaterOrEqual(t, in.bytes, produced,
		"the byte charge (%d) undercounted what the rewrite actually produced (%d) for:\n%s",
		in.bytes, produced, src)
}

// FuzzAliasInlinerChargeDominatesWhatItBuilds is the oracle, driven by Go's
// fuzzer over the dimensions [oracleFlowfile] varies independently,
// including the chain level that is what makes a per-line undercount large
// enough to see against the seed [aliasInliner.bytes] starts seeded at.
//
// This is the general net, not the proof for any one finding: the charge
// this rewrite makes at every level is itself conservative (a block
// re-indented by an outer chain level is priced again at the outer level
// rather than only at the difference the re-indent made), and that slack is
// large enough across a chained draw that reverting the blank-line floor
// alone does not reliably surface here — [TestFixRefusesABlankLineBombBeforeMaterializingIt]
// is the test mutation-tested against that specific finding, on allocation
// rather than on this oracle's own arithmetic. What this oracle catches
// reliably is a charge that stops tracking production altogether — nil
// pointer arithmetic gone wrong, a charge dropped rather than merely
// generous — over a much wider set of shapes than any hand-written corpus.
func FuzzAliasInlinerChargeDominatesWhatItBuilds(f *testing.F) {
	f.Add(8, 5, 3, 0, 2, 4)    // ordinary: short lines, shallow nesting, a real chain
	f.Add(1, 250, 5, 0, 2, 3)  // finding 2's shape: one long line, deep nesting
	f.Add(8, 0, 1, 0, 2, 5)    // finding 4's shape: every line blank
	f.Add(6, 10, 2, 180, 3, 4) // a trailing comment on every third line

	f.Fuzz(func(t *testing.T, lines, lineLen, nestDepth, commentLen, blankEvery, chainLevels int) {
		checkAliasInlinerAccountingOracle(t,
			oracleFlowfile(lines, lineLen, nestDepth, commentLen, blankEvery, chainLevels))
	})
}

// TestAliasInlinerChargeDominatesWhatItBuilds runs the same oracle over a
// deterministic sweep, so the property is checked on every ordinary test run
// rather than only when someone remembers to run this package's fuzz
// targets — [oracleFlowfile]'s parameters are cheap enough to try many
// combinations directly.
func TestAliasInlinerChargeDominatesWhatItBuilds(t *testing.T) {
	t.Parallel()

	for lines := 0; lines < 12; lines += 5 {
		for lineLen := 0; lineLen < 300; lineLen += 97 {
			for nestDepth := 0; nestDepth < 6; nestDepth += 2 {
				for commentLen := 0; commentLen < 200; commentLen += 97 {
					for blankEvery := 0; blankEvery < 11; blankEvery += 4 {
						for chainLevels := 0; chainLevels < 6; chainLevels++ {
							checkAliasInlinerAccountingOracle(t,
								oracleFlowfile(lines, lineLen, nestDepth, commentLen, blankEvery, chainLevels))
						}
					}
				}
			}
		}
	}
}

// aliasChainBomb builds a Flowfile a few hundred bytes long whose alias chain
// would copy orders of magnitude more than [maxBytes] were every level
// actually materialized: depth levels, two references each, a modest scalar
// at the leaf. Two-way branching is enough once appendLine charges the whole
// line a splice rebuilds rather than one ingredient of it (#2045): maxBytes
// is small enough that this chain's byte growth crosses it several levels
// before maxNodes would.
func aliasChainBomb(depth int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  level0: &level0\n    leaf: " +
		strings.Repeat("x", 64) + "\n")
	for level := 1; level <= depth; level++ {
		fmt.Fprintf(&b, "  level%d: &level%d\n    a: *level%d\n    b: *level%d\n",
			level, level, level-1, level-1)
	}
	fmt.Fprintf(&b, "  use: *level%d\n", depth)
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// TestFixRefusesAnAliasBombBeforeMaterializingIt is #2045's acceptance
// criterion: an adversarial alias chain is refused before its expansion is
// built, not after — checked on allocation, the resource the attack
// actually spends, the same way TestFormatOfAStringHoldingAMergeKeyBombStaysBounded
// (marshalbomb_test.go) checks the sibling defect in the scalar chooser.
//
// The ceiling is generous on purpose. What corrupted the previous attempt at
// this fix was review imagining a shape and missing one; a tight ceiling
// tuned to this one bomb's exact cost would be the identical mistake one
// level up — passing because the number happened to fit rather than because
// the accounting is sound. A ceiling an order of magnitude under what the
// unbounded expansion would cost, and comfortably over [maxBytes] itself, is
// the bound that actually follows from appendLine's own accounting: nothing
// this rewrite charges should ever need to allocate much past [maxBytes]
// before refusing.
func TestFixRefusesAnAliasBombBeforeMaterializingIt(t *testing.T) {
	// Not parallel: it reads process-wide allocation counters, and a sibling
	// test allocating alongside it would be charged to this one.

	const depth = 40 // two-way branching: 2^40 levels of "would be", none built
	src := aliasChainBomb(depth)
	require.Less(t, len(src), 4096, "the whole point is that the input is small")

	data := []byte(src)
	file, err := parser.ParseBytes(data, parser.ParseComments)
	require.NoError(t, err)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()

	in, _, ok := runAliasInliner(data, file)

	runtime.ReadMemStats(&after)
	elapsed := time.Since(start)
	allocated := after.TotalAlloc - before.TotalAlloc

	require.False(t, ok, "a %d-level, two-way alias chain was accepted rather than refused", depth)
	require.NotEmpty(t, in.refusals)

	const ceiling = 8 << 20
	require.Less(t, allocated, uint64(ceiling),
		"refusing a %d-byte alias bomb allocated %.1f MiB in %s: the expansion is being materialized before "+
			"the byte budget ever sees it, which is #2045 itself",
		len(src), float64(allocated)/(1<<20), elapsed)
}

// TestEveryDepthOfTheAliasBombCostsTheSame is the bound asserted as *reached*
// rather than merely not exceeded — the same shape as
// TestEveryDepthOfTheBombCostsTheSame (marshalbomb_test.go) for the sibling
// defect. A ceiling alone is also satisfied by a check that happens to fire
// at whichever depth a test picked; comparing a shallow draw against a much
// deeper one is what says the refusal is reached early rather than by luck.
func TestEveryDepthOfTheAliasBombCostsTheSame(t *testing.T) {
	costs := map[int]uint64{}
	for _, depth := range []int{20, 30, 40, 50} {
		src := aliasChainBomb(depth)
		data := []byte(src)
		file, err := parser.ParseBytes(data, parser.ParseComments)
		require.NoError(t, err)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, _, ok := runAliasInliner(data, file)
		runtime.ReadMemStats(&after)
		require.False(t, ok)

		costs[depth] = after.TotalAlloc - before.TotalAlloc
	}

	// Unbounded, two-way branching would differ by roughly 2^30 between depth
	// 20 and depth 50 — thirty orders of magnitude doubled. A refusal reached
	// at a cost that does not grow with depth differs by a small constant
	// factor instead; ten is far outside what an early, bounded refusal costs
	// and far inside what an unbounded expansion would.
	require.Less(t, costs[50], costs[20]*10,
		"refusal cost grows with the bomb's depth (%d bytes at depth 20, %d at depth 50), so the expansion "+
			"is still being materialized before it is refused", costs[20], costs[50])
}

// aliasChainBlankBomb is [aliasChainBomb]'s shape with the one dimension it
// does not vary: blank lines, sandwiched inside the leaf so they sit inside
// the copied span rather than before it (a blank line the parser reads as
// preceding the anchor's value is not part of that value's span at all, and
// never reaches [aliasInliner.expandRange] to be charged or not — the actual
// finding needs a blank line the rewrite would otherwise really copy). Two
// non-blank lines pin the leaf's actual, non-zero content; blanks is
// everything between them.
//
// This is #2045's finding 4 specifically, isolated from the other three: a
// chain built entirely from [aliasChainBomb]'s own shape, differing only in
// what the leaf holds, targets the one accounting gap a byte charge that
// prices *content* but not *lines* would still miss.
func aliasChainBlankBomb(depth, blanks int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  level0: &level0\n    head: 1\n")
	b.WriteString(strings.Repeat("\n", blanks))
	b.WriteString("    tail: 1\n")
	for level := 1; level <= depth; level++ {
		fmt.Fprintf(&b, "  level%d: &level%d\n    a: *level%d\n    b: *level%d\n",
			level, level, level-1, level-1)
	}
	fmt.Fprintf(&b, "  use: *level%d\n", depth)
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// TestFixRefusesABlankLineBombBeforeMaterializingIt is
// [TestFixRefusesAnAliasBombBeforeMaterializingIt] for #2045's finding 4 by
// name: a leaf whose only real content is two short lines, wrapped around
// three thousand blank ones, doubled through the same chain shape.
//
// Depth 10, not a shallower one, because the refusal has to be deserved on
// its own terms and not only relative to a mutant: measured with the byte
// charge disabled entirely (so [fixer.apply] runs to completion and reports
// what the correctly-inlined document would actually be), this chain's true
// size is 587,042 bytes at depth 6 — under maxBytes, so a byte-perfect
// accounting would *accept* it, and refusing it there would only be proving
// this rewrite's own conservative over-charge (an outer chain level pricing
// an inner level's already-priced content again, documented on
// [aliasInliner.appendLine]) rather than a defect. The true size crosses
// maxBytes at depth 7 and is 9,534,643 bytes — nine times over — by depth
// 10, which is where this test measures: unambiguously a bomb, not an edge
// case of this rewrite's own conservatism.
//
// The ceiling is tuned against a measurement, not guessed, and against both
// shapes finding 4 could be reintroduced in — not only the one this
// rewrite's own history happened to take. Fixed code allocates a stable
// 51.85 MiB at this depth (-count=5, with and without -race; the flowfile
// package's tests run under -race in CI's "rest" lane, ci.yml). Reverting
// only the blank-line floor inside [aliasInliner.appendLine] itself (`if
// line == "" { return append(out, line), true }`, skipping the charge for
// every caller at once) allocates a stable 2627 MiB before the *same*
// charge finally catches it once enough non-blank lines alone cross
// maxBytes. A narrower reintroduction at the one call site that charges a
// blank line on its own — [aliasInliner.spliceBlock]'s blank-line arm
// appending "" directly instead of going through appendLine — allocates
// less, since it only un-charges that one path, but still a stable 337
// MiB, caught the same way. [aliasInliner.expandRange]'s plain-copy charge
// is not load-bearing in the same way: spliceBlock charges every line of
// the range again as it shifts it, so dropping expandRange's charge
// measures about 64 MiB, flat, and is still refused by spliceBlock's
// charge; this test does not, and need not, catch it. The ceiling sits at
// roughly 3x the fixed figure, below both measured mutants, so either of
// those reintroductions of finding 4 fails this test on allocation rather
// than merely costing more.
func TestFixRefusesABlankLineBombBeforeMaterializingIt(t *testing.T) {
	// Not parallel: it reads process-wide allocation counters, and a sibling
	// test allocating alongside it would be charged to this one.

	const depth = 10
	const blanks = 3000
	src := aliasChainBlankBomb(depth, blanks)
	require.Less(t, len(src), 4096, "the whole point is that the input is small")

	data := []byte(src)
	file, err := parser.ParseBytes(data, parser.ParseComments)
	require.NoError(t, err)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()

	in, _, ok := runAliasInliner(data, file)

	runtime.ReadMemStats(&after)
	elapsed := time.Since(start)
	allocated := after.TotalAlloc - before.TotalAlloc

	require.False(t, ok, "a %d-level chain over a %d-blank-line leaf was accepted rather than refused", depth, blanks)
	require.NotEmpty(t, in.refusals)
	require.Contains(t, in.refusals[0].Message, "would copy more than",
		"refused by the trailing size check rather than the charge itself: the charge let the expansion "+
			"materialize before anything bounded it, which is #2045 itself")

	const ceiling = 160 << 20
	require.Less(t, allocated, uint64(ceiling),
		"refusing a %d-byte blank-line alias bomb allocated %.1f MiB in %s: a blank line is being copied "+
			"without being charged, which is #2045's finding 4",
		len(src), float64(allocated)/(1<<20), elapsed)
}

// TestEveryDepthOfTheBlankLineBombCostsTheSame is
// [TestEveryDepthOfTheAliasBombCostsTheSame] for the blank-line bomb: a
// refusal reached this early has to cost about the same at every depth past
// it, or it is not actually early. Every depth here is past the crossover
// [TestFixRefusesABlankLineBombBeforeMaterializingIt] documents (true size
// exceeds maxBytes from depth 7), so a flat cost across them is the claim
// and not an artifact of depths that were all still under budget anyway.
func TestEveryDepthOfTheBlankLineBombCostsTheSame(t *testing.T) {
	costs := map[int]uint64{}
	for _, depth := range []int{10, 12, 14, 16} {
		src := aliasChainBlankBomb(depth, 3000)
		data := []byte(src)
		file, err := parser.ParseBytes(data, parser.ParseComments)
		require.NoError(t, err)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, _, ok := runAliasInliner(data, file)
		runtime.ReadMemStats(&after)
		require.False(t, ok)

		costs[depth] = after.TotalAlloc - before.TotalAlloc
	}

	require.Less(t, costs[16], costs[10]*10,
		"refusal cost grows with the bomb's depth (%d bytes at depth 10, %d at depth 16), so the expansion "+
			"is still being materialized before it is refused", costs[10], costs[16])
}

// #2075's independent review, round two: caching what [fixer.blockEnd],
// [byteOffsetOfColumn], and [spanOfNode]/[tokenText] answer for an anchor or
// alias closes the *repeated* half of the shape (once per site instead of
// once per anchor), but round one's own [aliasInliner.spliceScalar] and
// [aliasInliner.spliceBlock] still called [spanOfNode] on an anchor's whole
// value — which walks every token in it and scans each one's `Origin` with
// [strings.TrimSpace] — *before* ever reaching those caches, and
// [aliasInliner.spliceBlock] still recomputed `strings.TrimRight(prefix, "
// ")` and `strings.TrimSpace(suffix)` on every visit even once prefix and
// suffix themselves were cached. Both are uncharged, unbounded scans that
// do not need a second site to matter — one anchor whose value is preceded
// by attacker-sized padding is enough. Auditing every other read of a site's
// or an anchor's own line for the same shape found two more: [split] itself
// called [spanOfNode] on a site's own *key*, uncharged, and
// [aliasInliner.spliceBlock] measured [indentWidth] of the anchor's block's
// first line and the anchor's own line fresh on every visit — YAML
// indentation is a run of spaces an author chooses the width of, not bounded
// by nesting depth, so that scan has the same shape as the rest even though
// nothing here nests any deeper for it. The fix routes every one of them
// through [aliasInliner.chargeScan] (charging before the scan wherever the
// size is cheaply known in advance, immediately after where it is not) and
// caches whatever [aliasInliner.chargeScan] alone cannot amortize, rather
// than adding a fifth ad hoc cache for a fifth ad hoc finding.
//
// The four builders below are the shapes the finding named explicitly: a
// scalar anchor's value preceded by padding ([scalarPaddingFlowfile] — the
// reviewer's own probe, reproduced at its own scale in
// [TestAliasInlinerScalarPaddingProbeIsRefusedOrCheap]), a block anchor
// whose sole entry's alias is preceded by padding
// ([keyFormPaddingFlowfile], exercising [aliasInliner.spliceBlock]'s mapping
// form and its `TrimRight`), a block anchor whose sole sequence element's
// alias is followed by padding ([sequenceFormPaddingFlowfile], exercising
// the sequence form and its `TrimSpace`), and the original block-with-a-
// blank-tail shape from round one ([blockTailFlowfile], re-checked here on
// the same counter rather than [fixer.blockEndBytesScanned] directly, so one
// style of assertion covers every finding this issue has accumulated). The
// two found on the way ([split]'s own key span, [indentWidth]'s two reads)
// are exercised incidentally by these same four shapes — every one of them
// has outer sites whose own key [split] spans, and every anchor in them has
// an opening line and a block-first line [aliasInliner.spliceBlock] measures
// the indentation of — rather than by builders of their own, since none of
// this file's probes found a shape that isolates either one from the costs
// the other caches already bound.

// scalarPaddingFlowfile builds a Flowfile whose anchor `a` names a one-line
// scalar value preceded by padding spaces, aliased by sites sites as
// `u0: *a`, `u1: *a`, and so on.
func scalarPaddingFlowfile(sites, padding int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "edition: v2026.3\nname: t\nvars:\n  a: &a%s1\n", strings.Repeat(" ", padding))
	for i := range sites {
		fmt.Fprintf(&b, "  u%d: *a\n", i)
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// keyFormPaddingFlowfile builds a Flowfile whose anchor `a` opens a
// one-line block — a single key `k` whose value is an alias `*b` to a short
// anchor `b`, separated from the colon by padding spaces — aliased by
// sites outer sites as `u0: *a`, `u1: *a`, and so on.
func keyFormPaddingFlowfile(sites, padding int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  b: &b\n    leaf: 1\n")
	fmt.Fprintf(&b, "  a: &a\n    k:%s*b\n", strings.Repeat(" ", padding))
	for i := range sites {
		fmt.Fprintf(&b, "  u%d: *a\n", i)
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// sequenceFormPaddingFlowfile is [keyFormPaddingFlowfile]'s shape with the
// inner alias written as a sequence element (`- *b`) followed by padding
// spaces, rather than as a mapping value preceded by them — the shape
// [aliasInliner.spliceBlock]'s sequence form (`strings.TrimSpace(suffix)`)
// reads instead of its mapping form (`strings.TrimRight(prefix, " ")`).
func sequenceFormPaddingFlowfile(sites, padding int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  b: &b\n    leaf: 1\n")
	fmt.Fprintf(&b, "  a: &a\n    - *b%s\n", strings.Repeat(" ", padding))
	for i := range sites {
		fmt.Fprintf(&b, "  u%d: *a\n", i)
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// blockTailFlowfile is round one's shape (renamed from blockEndBombFlowfile
// now that this file checks it the same way as the other three): anchor `l`
// opens a block holding one leaf line, then blankTail blank lines with no
// further content under it, aliased by sites outer sites.
func blockTailFlowfile(sites, blankTail int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  l: &l\n    leaf: 1\n")
	b.WriteString(strings.Repeat("\n", blankTail))
	for i := range sites {
		fmt.Fprintf(&b, "  u%d: *l\n", i)
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// #2075's independent review, round three: folding [aliasInliner.chargeScan]
// into [aliasInliner.bytes] — the fix the round above this one made — priced
// the same bytes twice for any legitimate document large enough to notice.
// [aliasInliner.bytes] starts seeded at the document's own length; a scan of
// an anchor's value or a block's extent then charged largely those same
// bytes again, before the eventual output priced them a third time. A
// 5,000-line block aliased once (259 KB in, ~518 KB correctly out) and a
// 3,000-line block aliased twice (155 KB in, ~465 KB out) — both ordinary,
// both accepted before that round — were refused with "would copy more
// than 1048576 bytes", a message about output size that was not actually
// about output at all.
//
// Separately, this file's own tests read [aliasInliner.scanBytes] (that
// round's name for what [aliasInliner.scanned] is now), which only
// [aliasInliner.chargeScan] itself incremented. Three mutants that each
// reverted one round's original defect — spanOf uncached and uncharged,
// blockEnd's charge point removed, split uncached with no charge — all
// still passed every test in this file, because an uncharged scan never
// touched the one counter being read.
//
// Both are fixed the same way this issue's every other round has been:
// [aliasInliner.chargeScan] now charges against [aliasInliner.scanned] and
// [maxScanned], a budget separate from [aliasInliner.bytes] and [maxBytes]
// (so a scan can no longer inflate what a document's *output* is judged
// against), and [aliasInliner.rawScannedBytes] is incremented at the actual
// scanning primitives themselves — [spanOfNode]/[tokenText] via
// [aliasInliner.spanOf], [byteOffsetOfColumn] via [aliasInliner.scalarValueOf]
// and [split], [indentWidth] in [aliasInliner.spliceBlock], and
// [fixer.blockEndBytesScanned] for [fixer.blockEnd] — never by
// [aliasInliner.chargeScan], so a mutant that keeps the charge but skips the
// underlying scan (impossible, since the charge's own size comes from
// measuring what is about to be scanned) is not what this catches; a mutant
// that skips the *cache* and calls the primitive again is.

// largeBlockOneAliasFlowfile and largeBlockTwoAliasFlowfile are the two
// documents review measured directly: an ordinary, large block aliased
// once or twice, with no padding and no attacker-shaped anything — the
// legitimate side of the class this issue's fix has to stay usable for.
func largeBlockOneAliasFlowfile(lines int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  a: &a\n")
	for i := range lines {
		fmt.Fprintf(&b, "    k%d: %s\n", i, strings.Repeat("x", 40))
	}
	b.WriteString("  u0: *a\n")
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

func largeBlockTwoAliasFlowfile(lines int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  a: &a\n")
	for i := range lines {
		fmt.Fprintf(&b, "    k%d: %s\n", i, strings.Repeat("x", 40))
	}
	b.WriteString("  u0: *a\n  u1: *a\n")
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// TestAliasInlinerAcceptsLargeLegitimateDocuments is #2075's round-three
// regression: both documents were accepted before [aliasInliner.chargeScan]
// existed, and both must be accepted with it too, since neither is anywhere
// near [maxBytes] on its own — 259 KB and 155 KB in, a few hundred KB out.
// Fails against a mutant that folds scan charges back into
// [aliasInliner.bytes] (this file's own history, aa1d3302): both were
// refused there with "would copy more than 1048576 bytes", a message about
// output that was never actually about output.
func TestAliasInlinerAcceptsLargeLegitimateDocuments(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		src  string
	}{
		{"5,000-line block, 1 alias", largeBlockOneAliasFlowfile(5000)},
		{"3,000-line block, 2 aliases", largeBlockTwoAliasFlowfile(3000)},
	} {
		data := []byte(tc.src)
		file, err := parser.ParseBytes(data, parser.ParseComments)
		require.NoError(t, err)

		in, _, ok := runAliasInliner(data, file)
		require.True(t, ok, "%s (%d bytes in): expected to be accepted; refusals: %v", tc.name, len(data), in.refusals)
	}
}

// aliasInlinerScanReport is what each shape's test checks: how much this
// rewrite actually scanned against how much it charged for output and how
// large the input was, plus the outcome.
type aliasInlinerScanReport struct {
	ok bool

	// trueScanned is [aliasInliner.rawScannedBytes] plus
	// [fixer.blockEndBytesScanned] — every scan this rewrite actually ran,
	// counted at the scan itself rather than at
	// [aliasInliner.chargeScan] (#2075).
	trueScanned int

	// scanned is [aliasInliner.scanned]: what was charged against
	// [maxScanned], the value a refusal is actually decided on.
	scanned int

	// outputBytes is [aliasInliner.bytes]: seeded at the input's own
	// length, grown only by what [aliasInliner.appendLine] charges for
	// output — no longer inflated by scanning (#2075).
	outputBytes int

	inputLen int
}

// scanReportOf runs the inliner over src and reports what it charged and
// scanned, without requiring either outcome: a probe shape may be refused
// rather than accepted, and either can be healthy depending on which shape
// it is — [assertScanBounded] requires acceptance, but a caller checking a
// shape that is expected to sit near a budget reads the report directly.
func scanReportOf(t *testing.T, src string) aliasInlinerScanReport {
	t.Helper()

	data := []byte(src)
	file, err := parser.ParseBytes(data, parser.ParseComments)
	require.NoError(t, err)

	in, _, ok := runAliasInliner(data, file)

	return aliasInlinerScanReport{
		ok:          ok,
		trueScanned: in.rawScannedBytes + in.f.blockEndBytesScanned,
		scanned:     in.scanned,
		outputBytes: in.bytes,
		inputLen:    len(src),
	}
}

// assertScanBounded is the shape every one of this issue's shapes checks:
// true scanning grows with what this rewrite actually charged for output
// (which is seeded at the input's own length, so that is counted once, not
// added again), not disproportionately past it — k is small and fixed
// across every shape, not tuned per shape, because the property under test
// is that one mechanism ([aliasInliner.chargeScan] plus caching) bounds all
// of them alike. A rewrite whose true scan cost is many times its own
// charged output (an unbounded per-site rescan, uncached) fails this on any
// k a legitimate document would never approach.
func assertScanBounded(t *testing.T, name string, r aliasInlinerScanReport) {
	t.Helper()

	require.True(t, r.ok, "%s: expected the rewrite to succeed", name)

	const k = 3
	bound := k * r.outputBytes
	require.LessOrEqual(t, r.trueScanned, bound,
		"%s: truly scanned %d bytes against %d bytes charged for output (already seeded at the "+
			"input's own length) — expected scanning within %dx of that, not disproportionately "+
			"more (#2075)", name, r.trueScanned, r.outputBytes, k)
}

// TestAliasInlinerScanStaysProportionalAcrossFindingShapes runs
// [assertScanBounded] over all four shapes #2075's review has accumulated,
// each at a site count (100) large enough that an unbounded per-site rescan
// would fail it by orders of magnitude rather than by chance. Checking
// [aliasInlinerScanReport.trueScanned] — [aliasInliner.rawScannedBytes] plus
// [fixer.blockEndBytesScanned], incremented at each scan itself — rather
// than [aliasInlinerScanReport.scanned] (fed only by
// [aliasInliner.chargeScan]) is what makes this catch an uncached scan even
// where nothing charged it: a mutant that keeps every charge but skips a
// cache still shows the true count growing per site here, which reading
// only what was charged cannot (#2075's own review: three such mutants each
// passed every test in an earlier push, because that push's tests read only
// the charge).
func TestAliasInlinerScanStaysProportionalAcrossFindingShapes(t *testing.T) {
	t.Parallel()

	const sites = 100
	const padding = 20000
	const blankTail = 6000

	assertScanBounded(t, "scalar padding", scanReportOf(t, scalarPaddingFlowfile(sites, padding)))
	assertScanBounded(t, "key-form padding", scanReportOf(t, keyFormPaddingFlowfile(sites, padding)))
	assertScanBounded(t, "sequence-form padding", scanReportOf(t, sequenceFormPaddingFlowfile(sites, padding)))
	assertScanBounded(t, "block blank tail", scanReportOf(t, blockTailFlowfile(sites, blankTail)))
}

// TestAliasInlinerScalarPaddingProbeIsRefusedOrCheap reproduces the
// independent review's own probe at its own scale: `vars:` holds one
// scalar anchor whose value is preceded by 500,000 padding columns,
// aliased by 10,000 sites. Before any of this issue's caching existed, this
// was *accepted* — correct output, but 3.70s, because
// [aliasInliner.spliceScalar] read that padding through [spanOfNode] before
// ever reaching its own value cache, once per site rather than once for the
// anchor. Caching closes the repeat; charging what is scanned is what makes
// that closure provable rather than merely observed. Named explicitly
// because it is the shape the independent review's own probe used, not
// because its outcome is expected to differ from
// [TestAliasInlinerScanStaysProportionalAcrossFindingShapes]'s other three
// shapes — at this scale it does not: cached, the anchor's value is scanned
// once regardless of site count, comfortably inside both budgets, so this
// is accepted and cheap, the same as the others.
func TestAliasInlinerScalarPaddingProbeIsRefusedOrCheap(t *testing.T) {
	// Not t.Parallel(): 10,000 sites over 500,000 columns of padding is the
	// one shape in this file sized to match the reviewer's own probe rather
	// than trimmed for speed, and it is not so slow that it needs to run
	// alongside nothing else to time out safely.

	r := scanReportOf(t, scalarPaddingFlowfile(10000, 500000))
	if !r.ok {
		// Refused: healthy on its own terms too, and was this file's own
		// prior expectation — kept as a branch rather than removed, in
		// case a future, tighter choice of [scanBudgetMultiple] revisits
		// where this shape sits relative to [maxScanned].
		return
	}

	assertScanBounded(t, "scalar padding (reviewer's own scale)", r)
}
