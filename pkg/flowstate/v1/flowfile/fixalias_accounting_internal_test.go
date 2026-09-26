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

// #2075: [fixer.blockEnd] walks forward from a block's key past every
// trailing blank line and dedented comment to find where the block ends.
// [aliasInliner.spliceBlock] asked it for the same answer on every site that
// aliases one anchor, before this file's own fix cached it — so an anchor
// with a long blank tail, referenced by many sites, paid for that walk
// sites × lines rather than sites + lines. Neither of #2072's budgets sees
// it: the byte charge prices what a site's replacement actually contains
// (here, one short leaf line — a blank tail past the block's last piece of
// real content extends nothing to charge for), and the node budget never
// looks at blank lines at all. The scan itself is the whole uncharged cost,
// and it allocates nothing a MemStats comparison (this file's alias-bomb
// tests) could see, so the tests below check it on a plain counter instead —
// see [fixer.blockEndScans].

// blockEndBombFlowfile builds a Flowfile whose anchor `l` opens a block
// holding one leaf line, then blankTail blank lines with no further content
// under it, then sites aliases of `l` as `u0: *l`, `u1: *l`, and so on. Every
// one of those sites expands the same anchor, so [aliasInliner.spliceBlock]
// asks [fixer.blockEnd] for that anchor's block extent once per site.
func blockEndBombFlowfile(sites, blankTail int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  l: &l\n    leaf: 1\n")
	b.WriteString(strings.Repeat("\n", blankTail))
	for i := range sites {
		fmt.Fprintf(&b, "  u%d: *l\n", i)
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// blockEndScansOf runs the inliner over src and returns how many lines
// [fixer.blockEnd] scanned across the whole rewrite, requiring the rewrite to
// succeed: a refused rewrite stops expanding sites partway through, which
// would understate the count on either side of the fix and make the two
// harder to tell apart rather than easier.
func blockEndScansOf(t *testing.T, src string) int {
	t.Helper()

	data := []byte(src)
	file, err := parser.ParseBytes(data, parser.ParseComments)
	require.NoError(t, err)

	in, _, ok := runAliasInliner(data, file)
	require.True(t, ok, "expected the rewrite to succeed; refusals: %v", in.refusals)

	return in.f.blockEndScans
}

// TestAliasInlinerBlockEndScansGrowWithSitesPlusLinesNotTheirProduct is
// #2075's acceptance criterion: scanning one anchor's block extent for many
// sites costs about what scanning it once does, not what scanning it once
// per site does.
//
// few and many alias the same anchor over the same blank tail, differing
// only in how many sites there are (5 against 100 — a 20x difference).
// Cached per anchor (fixed), both scan the tail exactly once, so many costs
// about what few does. Recomputed per site (unfixed), blockEnd runs once per
// site, so many costs about 20x what few does — sites × lines, not
// sites + lines.
func TestAliasInlinerBlockEndScansGrowWithSitesPlusLinesNotTheirProduct(t *testing.T) {
	t.Parallel()

	const blankTail = 6000

	few := blockEndScansOf(t, blockEndBombFlowfile(5, blankTail))
	many := blockEndScansOf(t, blockEndBombFlowfile(100, blankTail))

	require.Less(t, many, few*3,
		"scanning grew from %d to %d lines scanned as the site count went from 5 to 100 over the same "+
			"%d-line blank tail; blockEnd's block-extent scan is being repeated per site rather than "+
			"cached per anchor, which is #2075", few, many, blankTail)

	// Not just bounded — actually tracking the tail, so a cache keyed wrong
	// (or not keyed at all) does not pass the ratio check above for the
	// wrong reason.
	require.InDelta(t, blankTail, many, float64(blankTail)/2,
		"expanding 100 sites over a %d-line blank tail scanned %d lines; expected roughly one scan of "+
			"the tail (#2075)", blankTail, many)
}

// TestAliasInlinerBlockEndScansScaleWithTheBlankTail is the other half of
// [TestAliasInlinerBlockEndScansGrowWithSitesPlusLinesNotTheirProduct]: fixed
// code still has to scan a longer tail more, or the cache would be hiding
// the cost rather than paying it once. Held at a fixed, generous site count
// so any per-site sliver the cache leaves uncovered does not mask a tail
// that scaled wrong.
func TestAliasInlinerBlockEndScansScaleWithTheBlankTail(t *testing.T) {
	t.Parallel()

	const sites = 50

	short := blockEndScansOf(t, blockEndBombFlowfile(sites, 3000))
	long := blockEndScansOf(t, blockEndBombFlowfile(sites, 30000))

	require.InDelta(t, 10*short, long, float64(short),
		"a 10x longer blank tail scanned %d -> %d lines at the same %d sites; expected roughly a 10x "+
			"increase, not a flat cost that would mean the tail is never actually scanned", short, long, sites)
}

// longScalarBomb builds a Flowfile whose anchor `l` names a single-line
// scalar valueLen bytes long, aliased by sites sites as `u0: *l`, `u1: *l`,
// and so on. [aliasInliner.spliceScalar] locates that value with
// [byteOffsetOfColumn], which rescans the anchor's own line from its start —
// the issue's own note that this "looks like the same shape" as blockEnd's
// scan (#2075), here isolated to just that shape rather than a block's.
func longScalarBomb(sites, valueLen int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "edition: v2026.3\nname: t\nvars:\n  l: &l %s\n", strings.Repeat("x", valueLen))
	for i := range sites {
		fmt.Fprintf(&b, "  u%d: *l\n", i)
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// TestAliasInlinerScalarValueComputedOncePerAnchor is
// [TestAliasInlinerBlockEndScansGrowWithSitesPlusLinesNotTheirProduct]'s
// counterpart for [aliasInliner.spliceScalar]: a long one-line scalar
// anchor's value, aliased by many sites, is located once and reused rather
// than rescanned at every site. [aliasInliner.scalarValueComputations]
// counts [aliasInliner.scalarValueOf]'s own calls, which only happen on a
// [aliasInliner.scalarValues] cache miss — one per anchor, not one per site.
func TestAliasInlinerScalarValueComputedOncePerAnchor(t *testing.T) {
	t.Parallel()

	const sites = 40
	src := longScalarBomb(sites, 2000)

	data := []byte(src)
	file, err := parser.ParseBytes(data, parser.ParseComments)
	require.NoError(t, err)

	in, out, ok := runAliasInliner(data, file)
	require.True(t, ok, "expected the rewrite to succeed; refusals: %v", in.refusals)

	require.Equal(t, 1, in.scalarValueComputations,
		"expanding %d sites that alias one scalar anchor computed its value %d times; expected once, "+
			"reused from every other site (#2075)", sites, in.scalarValueComputations)

	value := strings.Repeat("x", 2000)
	for i := range sites {
		require.Contains(t, string(out), fmt.Sprintf("u%d: %s", i, value),
			"site u%d was not correctly rewritten with the anchor's value", i)
	}
}
