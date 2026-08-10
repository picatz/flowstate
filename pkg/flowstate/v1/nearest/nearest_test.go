package nearest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// TestDistanceKnownPairs pins the edit-distance function against hand-checked
// pairs, including the empty-string edges the DP table has to get right at its
// boundary, and one pair that separates a rune comparison from a byte one:
// `naive` for `naïve` is a single substitution on screen and two bytes of
// difference underneath.
func TestDistanceKnownPairs(t *testing.T) {
	for _, test := range []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"", "abc", 3},
		{"abc", "", 3},
		{"list", "list", 0},
		{"lst", "list", 1},
		{"validte", "validate", 1},
		{"kitten", "sitting", 3},
		{"adress", "address", 1},
		{"naive", "naïve", 1},
	} {
		assert.Equal(t, test.want, nearest.Distance(test.a, test.b), "Distance(%q, %q)", test.a, test.b)
	}
}

// TestLimitIsReachedAndNotExceeded pins the acceptance threshold at both of its
// edges, for every name length that changes the answer.
//
// The bound-was-reached half matters as much as the bound-was-not-exceeded half
// (CLAUDE.md, "test the traversal, not just the step"): a threshold nothing ever
// reaches is indistinguishable from a stricter one, so each row asserts the name
// exactly at its limit is accepted *and* the one edit past it is refused. Read
// down the want column and the shape of the rule is visible: a third of the name,
// capped at [nearest.MaxDistance].
func TestLimitIsReachedAndNotExceeded(t *testing.T) {
	for _, test := range []struct {
		name string
		want int
	}{
		{"a", 1},
		{"if", 1},
		{"run", 2},
		{"step", 2},
		{"steps", 2},
		{"vars", 2},
		{"timeout", 2},
		{"continue_on_error", 2},
		{strings.Repeat("x", 300), 2},
	} {
		limit := nearest.Limit(test.name)
		require.Equal(t, test.want, limit, "Limit(%q)", test.name)

		assert.True(t, nearest.Within(test.name, limit),
			"a name at exactly its limit of %d edits was refused, so the bound is never reached", limit)
		assert.False(t, nearest.Within(test.name, limit+1),
			"a name one edit past its limit of %d was accepted, so the bound does not hold", limit)
	}
}

// TestLimitNeverExceedsMaxDistance is the same claim stated once for every
// length rather than for the sampled ones: however long a name gets, the
// proportion never buys it a third edit.
func TestLimitNeverExceedsMaxDistance(t *testing.T) {
	for length := range 200 {
		name := strings.Repeat("x", length)
		assert.LessOrEqual(t, nearest.Limit(name), nearest.MaxDistance,
			"a name of %d characters was allowed more than MaxDistance edits", length)
		assert.GreaterOrEqual(t, nearest.Limit(name), 1,
			"a name of %d characters was allowed no edits at all, so nothing is ever suggested for it", length)
	}
}

// TestNamePicksTheClosestAndRefusesTheFar covers the whole-list entry point in
// both directions: the near miss is answered with the name that was meant, and a
// word sharing nothing with the list is answered with nothing rather than with an
// invention.
func TestNamePicksTheClosestAndRefusesTheFar(t *testing.T) {
	known := []string{"steps", "vars", "timeout", "retry"}

	got, ok := nearest.Name("stpes", known)
	require.True(t, ok, "expected a suggestion for a transposition of a known name")
	assert.Equal(t, "steps", got)

	got, ok = nearest.Name("tiemout", known)
	require.True(t, ok, "expected `timeout` for a transposition exactly at that name's limit of two edits")
	assert.Equal(t, "timeout", got)

	_, ok = nearest.Name("zzzzzqqqq123", known)
	assert.False(t, ok, "a name sharing nothing with the list should suggest nothing")

	_, ok = nearest.Name("var", []string{"a"})
	assert.False(t, ok, "a one letter candidate should not answer for a three letter word")
}

// TestNameTieGoesToTheEarlierCandidate pins the ordering guarantee callers rely
// on to render a stable message: two candidates equally close, and the answer is
// the one the caller listed first rather than whichever the loop happened to see
// last.
func TestNameTieGoesToTheEarlierCandidate(t *testing.T) {
	got, ok := nearest.Name("cat", []string{"bat", "hat"})
	require.True(t, ok)
	assert.Equal(t, "bat", got)

	got, ok = nearest.Name("cat", []string{"hat", "bat"})
	require.True(t, ok)
	assert.Equal(t, "hat", got)
}
