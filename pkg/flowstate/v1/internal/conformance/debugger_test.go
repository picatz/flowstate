package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The data invariant [DebuggerCase.Held] states about itself, checked where the
// data is rather than at either driver.
//
// The chain the three tests make together: this one says every id a durable
// lease claims it can hold at is one the corpus also declares as offered, in
// order; `v1.TestRunWorkflowDebuggerBoundaries` says the offered list is what
// the local driver really does; and `engine.TestALeaseHoldsTheDurableCorpus…`
// says a lease really takes effect at the first held id. Break any link and the
// corpus is describing a run neither driver has.

// TestEveryHeldBoundaryIsOneTheCorpusOffers is the invariant, over the real
// corpus.
func TestEveryHeldBoundaryIsOneTheCorpusOffers(t *testing.T) {
	cases := DebuggerCases()
	require.NotEmpty(t, cases, "the debugger corpus is empty, so every claim below is vacuous")

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			require.NotEmpty(t, test.Held,
				"a case with no holdable boundary states nothing about the durable driver")

			assert.True(t, isSubsequence(test.Held, test.Offered),
				"the durable driver claims it can hold at boundaries %v, which is not an in-order "+
					"subsequence of the %v this run actually reaches — so the two drivers disagree "+
					"about which boundaries a run has", test.Held, test.Offered)
		})
	}
}

// TestASubsequenceIsNotMerelyASubset is why the check above is written the way
// it is, and it is the whole reason this file exists.
//
// `assert.Subset` — the obvious spelling, and the one this started as — is
// satisfied by held boundaries in the wrong order and by a held boundary
// claimed more times than the run reaches it. Both are real ways for the corpus
// to be wrong: a lease that held `after` before `before` would be describing a
// run neither driver has, and a body visited once cannot be held twice.
//
// The real corpus agrees with itself on every one of these, which is exactly
// why the difference has to be driven by a fixture: a check written against
// data that always agrees is a check no test can reach (CLAUDE.md, "assert
// where the answers differ").
func TestASubsequenceIsNotMerelyASubset(t *testing.T) {
	offered := []string{"before", "each", "touch", "touch", "after"}

	for name, held := range map[string][]string{
		"the whole list":         {"before", "each", "touch", "touch", "after"},
		"a prefix":               {"before", "each"},
		"a gap in the middle":    {"before", "after"},
		"one repeat of two":      {"before", "touch", "after"},
		"nothing at all":         {},
		"both repeats, in place": {"touch", "touch"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.True(t, isSubsequence(held, offered), "%v is a subsequence of %v", held, offered)
		})
	}

	for name, held := range map[string][]string{
		"out of order":            {"each", "before"},
		"one more repeat than is": {"touch", "touch", "touch"},
		"a boundary never seen":   {"before", "elsewhere"},
		"longer than the whole":   {"before", "each", "touch", "touch", "after", "before"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, isSubsequence(held, offered), "%v is not a subsequence of %v", held, offered)
		})
	}

	// The two the loops above are told apart from, stated rather than implied:
	// every rejected list is a *subset* of what is offered by name, so a check
	// written as a set membership passes all four.
	assert.Subset(t, offered, []string{"each", "before"},
		"the rejected cases have to be ones a set check would accept, or this file proves nothing")
}

// isSubsequence reports whether held appears inside offered in order, each
// element consumed at most once.
//
// A function rather than an inline loop for the reason its own test gives: the
// real corpus never distinguishes this from a subset, so the difference can
// only be shown against data built to show it.
func isSubsequence(held, offered []string) bool {
	next := 0
	for _, want := range held {
		found := false
		for ; next < len(offered); next++ {
			if offered[next] == want {
				next++
				found = true

				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}
