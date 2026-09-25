package nearest_test

// The randomized half of this package's tests: the claims that are true of every
// pair of names rather than of the hand-checked pairs in nearest_test.go.
//
// [testing/quick] rather than a fuzz target, and the distinction is the one
// `tools/fuzztargets` draws. A fuzz target covers a parser that takes bytes
// across a trust boundary, where the corpus is worth keeping because a crash is
// worth reproducing forever. This package has no boundary and cannot crash: it
// is arithmetic over two strings, and what needs checking is that two ways of
// computing the same answer agree. [quick.Check] says that in a sentence, and
// the seed below makes a disagreement replayable without a corpus directory.
//
// The package is frozen (no new features), which is a reason not to build on it
// and not a reason to avoid what it already does well.

import (
	// math/rand, not math/rand/v2, which is the spelling everywhere else in this
	// repository ([v1.SeededScheduler], the plugin backoff). [quick.Config]'s
	// Rand field is a *math/rand.Rand and the package is frozen, so v1 is what
	// seeding a check costs. The alternative is leaving Rand nil, which seeds
	// from the clock and turns any disagreement into a failure nobody can replay.
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// propertySeed fixes every check in this file, so a failure names inputs that
// reproduce on the next run rather than a shape that was there once. Any value
// works; this one is arbitrary and should stay put, because changing it is
// changing which inputs are covered.
const propertySeed = 20260913

// propertyChecks is how many cases each check draws. Two thousand rather than
// [quick.Config]'s default hundred because the interesting inputs here are the
// ones where a candidate is *close*, and a hundred cases finds too few of them
// to stand behind — [TestTheLengthFilterAgreesWithAnUnfilteredScan] asserts how
// many it actually found rather than assuming.
const propertyChecks = 2000

// checkConfig returns a fresh configuration, because [quick.Config] holds the
// generator: two checks sharing one would draw from the same stream and neither
// would be replayable on its own.
func checkConfig() *quick.Config {
	return &quick.Config{
		MaxCount: propertyChecks,
		Rand:     rand.New(rand.NewSource(propertySeed)), //nolint:gosec // not cryptographic; see propertySeed
	}
}

// nameAlphabet is what generated names are spelled from, and it is the whole
// reason these checks prove anything.
//
// Two properties are needed of it. It has to be small, because quick's own
// string generator draws from the entire rune space, where two generated names
// are never within [nearest.MaxDistance] edits of each other: every case would
// answer "no suggestion" on both sides and the check would pass while testing
// nothing. And it has to mix rune widths, because the rule under test counts
// runes — `Limit` is a third of the name *in runes*, and the length filter
// compares rune counts — so an implementation that reached for `len` instead
// would agree with itself on ASCII and only diverge here. `é` is two bytes and
// `字` is three.
var nameAlphabet = []rune{'a', 'b', 'é', '字'}

// maxGeneratedName is the longest name generated. Long enough that a pair can
// differ by more than any candidate's limit — which is the case the length
// filter exists to refuse — and short enough that a pair is often within one
// edit, which is the case it must not refuse.
const maxGeneratedName = 8

// generatedName is a name drawn from [nameAlphabet].
//
// It exists so that [quick.Value] reaches this generator rather than its own
// string generator, for [nameAlphabet]'s reason. The empty name is in range
// deliberately: it is the one candidate that can never be suggested however
// close it is, because [nearest.Name] spells "nothing found yet" as an empty
// best, and both implementations below have to agree about that.
type generatedName string

// Generate implements [quick.Generator].
//
// The size hint is ignored. It bounds how many elements a generated slice holds,
// which is the right knob for a candidate list and the wrong one for the length
// of a name: that length is the property being varied, and [maxGeneratedName]
// says what it has to cover.
func (generatedName) Generate(rng *rand.Rand, _ int) reflect.Value {
	name := make([]rune, rng.Intn(maxGeneratedName+1))
	for i := range name {
		name[i] = nameAlphabet[rng.Intn(len(nameAlphabet))]
	}

	return reflect.ValueOf(generatedName(name))
}

// generatedNames is a candidate list, in the shape [nearest.Name] takes it.
type generatedNames []generatedName

// strings renders the list as [nearest.Name]'s argument.
func (names generatedNames) strings() []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = string(name)
	}

	return out
}

// nameUnfiltered is [nearest.Name] with the length filter removed: every
// candidate measured in full, no matter how far its length puts it out of reach.
//
// It is the reference the real implementation has to agree with, and it is a
// deliberate second copy of a rule this repository otherwise keeps in one place.
// The duplication is the test: the filter is an optimization, its own doc
// comment claims it is "the same answer arrived at for less work", and a claim
// that two computations agree cannot be checked against one computation.
func nameUnfiltered(got string, known []string) (string, bool) {
	best, bestDistance := "", 0

	for _, name := range known {
		distance := nearest.Distance(got, name)
		if !nearest.Within(name, distance) {
			continue
		}
		if best == "" || distance < bestDistance {
			best, bestDistance = name, distance
		}
	}

	return best, best != ""
}

// TestTheLengthFilterAgreesWithAnUnfilteredScan is the claim [nearest.Name]'s
// doc comment makes about its own length check and nothing proved: that refusing
// a candidate on its length alone is "the same answer arrived at for less work,
// not a bound".
//
// The existing test for that filter
// ([TestNameRefusesAnImpossibleLengthBeforeMeasuringIt]) asserts it in
// allocations, which is the half that says the work was skipped, plus three
// hand-picked names that say a reachable candidate survived. This is the other
// half stated for every input rather than for three: the filtered and unfiltered
// scans return the same name and the same found-or-not, including the tie rule,
// so a future edit to the filter's arithmetic — an off-by-one in the length
// difference, `len` where `utf8.RuneCountInString` belongs — fails here rather
// than silently costing a suggestion somebody was being offered.
//
// Two counters keep the check honest about itself. A property over random
// strings can be true because both sides always answered "nothing", so this
// records how often a suggestion was actually produced and how often the filter
// actually skipped a candidate, and requires both to have happened often enough
// that the agreement above means something.
func TestTheLengthFilterAgreesWithAnUnfilteredScan(t *testing.T) {
	t.Parallel()

	var suggested, filtered int

	property := func(typed generatedName, candidates generatedNames) bool {
		known := candidates.strings()

		gotName, gotOK := nearest.Name(string(typed), known)
		wantName, wantOK := nameUnfiltered(string(typed), known)

		if gotOK {
			suggested++
		}

		// The filter's own condition, restated: a candidate whose length differs
		// from the typed name's by more than its limit is refused before
		// [nearest.Distance] is called. Counted here so the assertions below can
		// say the fast path was reached, which no observation of the answer can.
		length := utf8.RuneCountInString(string(typed))
		for _, name := range known {
			if difference := length - utf8.RuneCountInString(name); max(difference, -difference) > nearest.Limit(name) {
				filtered++
				break
			}
		}

		return gotName == wantName && gotOK == wantOK
	}

	require.NoError(t, quick.Check(property, checkConfig()),
		"the length filter changed an answer: it is an optimization, not a threshold")

	assert.Greater(t, suggested, propertyChecks/100,
		"a check where nothing was ever suggested would agree about nothing; see nameAlphabet")
	assert.Greater(t, filtered, propertyChecks/100,
		"a check that never reached the length filter would agree about the slow path only")
}

// TestDistanceObeysTheMetricLaws pins the three properties every caller reads
// into the name "distance" and none of the hand-checked pairs can state.
//
// Symmetry is the one a two-row dynamic program can plausibly lose: the table is
// rows over one string and columns over the other, and the two strings are not
// interchangeable in the code even though they are in the definition. The other
// two are cheap alongside it and catch the shapes symmetry would not — a
// distance that is zero between different names, or one that reports a detour as
// shorter than the direct route.
func TestDistanceObeysTheMetricLaws(t *testing.T) {
	t.Parallel()

	symmetric := func(a, b generatedName) bool {
		return nearest.Distance(string(a), string(b)) == nearest.Distance(string(b), string(a))
	}
	require.NoError(t, quick.Check(symmetric, checkConfig()),
		"the distance between two names must not depend on which one was typed")

	// Both directions, and the identity half is asked of every draw rather than
	// only of the pairs that happen to come up equal — two independent draws from
	// [nameAlphabet] agree outright about one time in eighty, so left to the
	// biconditional alone this law would be almost entirely the "different names
	// cost an edit" direction.
	//
	// Scoped to valid UTF-8, which is what the generator emits: two distinct
	// strings of invalid bytes both decode to one [utf8.RuneError] and are zero
	// apart by this function's rune-wise definition. That is [nearest.Distance]'s
	// documented behaviour rather than a gap, since its callers compare names a
	// parser already accepted.
	zeroOnlyWhenEqual := func(a, b generatedName) bool {
		if nearest.Distance(string(a), string(a)) != 0 {
			return false
		}

		return (nearest.Distance(string(a), string(b)) == 0) == (a == b)
	}
	require.NoError(t, quick.Check(zeroOnlyWhenEqual, checkConfig()),
		"no edits must mean the same name, and the same name must cost no edits")

	triangle := func(a, b, c generatedName) bool {
		direct := nearest.Distance(string(a), string(c))
		via := nearest.Distance(string(a), string(b)) + nearest.Distance(string(b), string(c))

		return direct <= via
	}
	require.NoError(t, quick.Check(triangle, checkConfig()),
		"turning one name into another by way of a third cannot be cheaper than doing it directly")
}
