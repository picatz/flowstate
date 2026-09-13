package textbound

// The randomized half of this package's tests.
//
// The package doc states its guarantee universally — "Every result is valid
// UTF-8" — and textbound_test.go proves it on twenty-six rows the author chose.
// Those rows are the right way to pin the cases that are hard to reason about
// (a lead byte whose continuation lies past the limit; a rune ending exactly on
// it). They are the wrong way to state a claim about every input, which is what
// the doc claims and what the callers rely on: this package exists for text a
// plugin, a relying party, or a review thread wrote, so "every input" means
// every byte string some other process can choose.
//
// [testing/quick] rather than a fuzz target, on the distinction
// tools/fuzztargets/boundaries.txt draws: a target belongs to a parser that
// takes bytes across a trust boundary and can crash or misread them. These two
// functions parse nothing and return a string; what needs stating is that two
// postconditions hold for all inputs, which is a property, not a search. The
// seed is pinned so a counterexample replays without a corpus directory.

import (
	// math/rand rather than math/rand/v2: [quick.Config]'s Rand field is a
	// *math/rand.Rand and the package is frozen. Leaving it nil would seed from
	// the clock and make a counterexample unreplayable.
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"unicode/utf8"
)

// propertySeed fixes every check here. Any value works; changing it changes
// which inputs are covered, so it should stay put.
const propertySeed = 20260913

// propertyChecks is how many cases each check draws.
const propertyChecks = 5000

// checkConfig returns a fresh configuration, because [quick.Config] holds the
// generator: two checks sharing one would draw from the same stream and neither
// would replay on its own.
func checkConfig() *quick.Config {
	return &quick.Config{
		MaxCount: propertyChecks,
		Rand:     rand.New(rand.NewSource(propertySeed)), //nolint:gosec // not cryptographic; see propertySeed
	}
}

// generatedText is a byte string mixing the three kinds of content these two
// functions treat differently.
//
// It exists because quick's own string generator is the wrong shape for this
// claim in both directions. Random bytes are almost never valid UTF-8, so a
// check over them would exercise the drop-invalid-bytes path and essentially
// never the one where a *valid* multi-byte rune straddles the limit — the case
// the whole package is about. Random runes are the opposite: always valid, so
// the repair path never runs. A generated value therefore draws each position
// from one of three sources: an ASCII letter, a valid rune of two, three or
// four bytes, or an arbitrary byte that may be a lone continuation or a lead
// byte with nothing after it.
type generatedText string

// multiByteRunes are the valid non-ASCII runes generated text is built from, one
// of each width, so a cut can land before, inside, or after a sequence of every
// length the encoding has.
var multiByteRunes = []rune{'é', '中', '\U0001F600'}

// Generate implements [quick.Generator].
//
// The size hint is ignored: it bounds a generated collection's element count,
// while what has to vary here is the length in bytes against the limit the
// functions are given, which [generatedLimit] draws independently.
func (generatedText) Generate(rng *rand.Rand, _ int) reflect.Value {
	var text []byte
	for range rng.Intn(12) {
		switch rng.Intn(3) {
		case 0:
			text = append(text, byte('a'+rng.Intn(26)))
		case 1:
			text = utf8.AppendRune(text, multiByteRunes[rng.Intn(len(multiByteRunes))])
		default:
			text = append(text, byte(rng.Intn(256)))
		}
	}

	return reflect.ValueOf(generatedText(text))
}

// generatedLimit is a byte limit, drawn to straddle the interesting region
// rather than uniformly: most of the range is inside or just past a generated
// text's length, where a cut has to decide something, and zero and negative
// limits are in range because both functions document an answer for them.
type generatedLimit int

// Generate implements [quick.Generator].
func (generatedLimit) Generate(rng *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(generatedLimit(rng.Intn(36) - 2))
}

// marker is the ellipsis [Truncate] appends, named here so the bound below reads
// as the function's contract rather than as three characters.
const marker = "..."

// TestCutHoldsItsTwoPostconditionsForEveryInput states what
// textbound_test.go's table asserts per row, for every input instead: the answer
// is valid UTF-8 and it is within the limit.
//
// Both directions matter and they pull against each other. Returning "" always
// would satisfy them and answer nothing, so the check also records how often a
// non-empty answer came back, how often the input was already invalid UTF-8, and
// how often a valid multi-byte rune straddled the limit — the case the package
// exists for — and fails if any of the three is too rare to stand behind.
func TestCutHoldsItsTwoPostconditionsForEveryInput(t *testing.T) {
	t.Parallel()

	var kept, wasInvalid, straddled int

	property := func(text generatedText, limit generatedLimit) bool {
		in, n := string(text), int(limit)
		got := Cut(in, n)

		if got != "" {
			kept++
		}
		if !utf8.ValidString(in) {
			wasInvalid++
		}
		if straddlesLimit(in, n) {
			straddled++
		}

		return utf8.ValidString(got) && len(got) <= max(n, 0)
	}

	if err := quick.Check(property, checkConfig()); err != nil {
		t.Fatalf("Cut broke a postcondition: %v", err)
	}

	assertReached(t, "a non-empty answer", kept)
	assertReached(t, "input that was already invalid UTF-8", wasInvalid)
	assertReached(t, "a valid multi-byte rune straddling the limit", straddled)
}

// TestTruncateHoldsItsTwoPostconditionsForEveryInput is [Cut]'s check for the
// marker-carrying half.
//
// The bound is the limit plus the marker, which is [Truncate]'s documented
// answer and not a looser version of Cut's: the marker is this function's own
// text rather than the other party's, so it is not what the limit bounds. What
// must not happen is input bytes past the limit, which is the difference the
// subtraction states.
func TestTruncateHoldsItsTwoPostconditionsForEveryInput(t *testing.T) {
	t.Parallel()

	var marked int

	property := func(text generatedText, limit generatedLimit) bool {
		in, n := string(text), int(limit)
		got := Truncate(in, n)

		if len(got) >= len(marker) && got[len(got)-len(marker):] == marker {
			marked++
		}

		return utf8.ValidString(got) && len(got) <= max(n, 0)+len(marker)
	}

	if err := quick.Check(property, checkConfig()); err != nil {
		t.Fatalf("Truncate broke a postcondition: %v", err)
	}

	assertReached(t, "an answer carrying the marker", marked)
}

// TestCutIsIdempotentForEveryInput is the claim that makes the two above worth
// having: an answer already within the limit and already valid must come back
// unchanged.
//
// Without it, a function that dropped one more byte on every call would satisfy
// both postconditions forever. It is also the property a caller relies on
// without writing down, since bounded text passes through more than one layer
// on its way to a log line or a response, and each layer bounds what it was
// handed.
func TestCutIsIdempotentForEveryInput(t *testing.T) {
	t.Parallel()

	property := func(text generatedText, limit generatedLimit) bool {
		once := Cut(string(text), int(limit))

		return Cut(once, int(limit)) == once
	}

	if err := quick.Check(property, checkConfig()); err != nil {
		t.Fatalf("Cut is not idempotent: %v", err)
	}
}

// straddlesLimit reports whether a valid multi-byte rune in s begins before
// limit and ends at or after it: the case a byte-wise cut would break and this
// package exists to handle.
//
// Written out here rather than derived from [Cut]'s answer, because a counter
// that asked the function under test whether it reached its own interesting case
// would agree with any implementation, including a broken one.
func straddlesLimit(s string, limit int) bool {
	if limit <= 0 {
		return false
	}

	for at, r := range s {
		width := utf8.RuneLen(r)
		if r == utf8.RuneError && width <= 1 {
			continue // an invalid byte, not a rune the input holds
		}
		if at < limit && at+width > limit {
			return true
		}
	}

	return false
}

// assertReached fails when a check reached one of its interesting cases too
// rarely for its agreement about that case to mean anything. One percent of the
// draws is a low floor on purpose: it is here to catch a generator that stopped
// producing a shape entirely, not to pin a distribution.
func assertReached(t *testing.T, what string, count int) {
	t.Helper()

	if floor := propertyChecks / 100; count <= floor {
		t.Errorf("only %d of %d cases reached %s, want more than %d: the property agreed about nothing there",
			count, propertyChecks, what, floor)
	}
}
