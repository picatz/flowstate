package flowdebug

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The bound tested where it is made.
//
// An answer-level test cannot see this one: celcomplete's own bound() cuts the
// answer at MaxCandidates and reports Truncated either way, so a completer
// that sorted every key of a hostile map and then cut looks identical from
// outside. What differs is the work, and the work is here (Codex, #1114).

func TestBoundedNamesNeverHoldsMoreThanItsBound(t *testing.T) {
	t.Parallel()

	values := make(map[string]int, 5000)
	for i := range 5000 {
		values[fmt.Sprintf("k%05d", i)] = i
	}

	names, more := boundedNames(values, 512)

	require.Len(t, names, 512, "it must never accumulate past the bound, not even to cut afterwards")
	assert.True(t, more, "and it has to say the map went further")
	assert.True(t, slices.IsSorted(names), "what is kept stays in order")

	// The first N in order rather than an arbitrary N: a completer whose
	// answer changed between two identical tab presses would be worse than a
	// slow one.
	assert.Equal(t, "k00000", names[0])
	assert.Equal(t, "k00511", names[511])
}

func TestBoundedNamesKeepsTheSmallestWhateverOrderItMeetsThem(t *testing.T) {
	t.Parallel()

	// Go randomises map iteration, so this is already adversarial in the way
	// that matters — but the property is stated rather than left to luck: the
	// answer is the same whichever order the keys arrive in.
	values := map[string]int{"e": 5, "b": 2, "d": 4, "a": 1, "c": 3}

	for range 32 {
		names, more := boundedNames(values, 3)

		require.Equal(t, []string{"a", "b", "c"}, names)
		require.True(t, more)
	}
}

func TestBoundedNamesOnMapsThatFitAndBoundsThatDoNot(t *testing.T) {
	t.Parallel()

	names, more := boundedNames(map[string]int{"b": 1, "a": 2}, 512)
	assert.Equal(t, []string{"a", "b"}, names)
	assert.False(t, more, "a map inside the bound is not a truncated one")

	none, more := boundedNames(map[string]int{"a": 1}, 0)
	assert.Empty(t, none)
	assert.True(t, more, "a bound of zero still has to admit there was something")

	empty, more := boundedNames(map[string]int{}, 0)
	assert.Empty(t, empty)
	assert.False(t, more, "and nothing withheld is not a truncation")
}

// TestBoundedNamesDoesNotAllocateTheWholeMap is the finding itself, and it
// needs a different kind of assertion than its neighbours (Codex, #1114).
//
// Collecting every key and cutting to the bound afterwards produces the *same
// answer* as bounding during collection — identical names, identical `more`.
// No behavioural test can separate them, which is exactly why the unbounded
// shape survived review and a green suite. What differs is the peak memory a
// single tab press costs, and the map whose size decides it belongs to the
// workload: a plugin's return, or a stubbed `returns:` in a document submitted
// to flowstate_debug.
//
// So this measures. A slice of 100k string headers is about 1.6 MB before a
// single candidate is built; the bounded form holds 512. The threshold sits
// two orders of magnitude below the unbounded cost and one above the bounded
// one, so it is a real signal rather than a number tuned to today's allocator.
func TestBoundedNamesDoesNotAllocateTheWholeMap(t *testing.T) {
	const (
		keys  = 100_000
		limit = 512
	)

	values := make(map[string]int, keys)
	for i := range keys {
		values[fmt.Sprintf("k%06d", i)] = i
	}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			names, more := boundedNames(values, limit)
			if len(names) != limit || !more {
				b.Fatalf("fixture stopped exercising the bound: %d names, more=%v", len(names), more)
			}
		}
	})

	assert.Less(t, result.AllocedBytesPerOp(), int64(256<<10),
		"one tab press over a %d-key map allocated %d bytes — the bound is on the answer, "+
			"not on the work the far side can ask for", keys, result.AllocedBytesPerOp())
}
