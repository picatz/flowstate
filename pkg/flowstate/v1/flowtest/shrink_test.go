package flowtest

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// firings lists every (fault, n) a set of pins holds, so a test can state a
// violation as "needs these firings" without caring how they are grouped.
func firings(pins []Fault) []faultAtom {
	return atomsOf(pins, make([]bool, len(pins)))
}

func pinsWith(on ...[]int) ([]Fault, []bool) {
	pins := make([]Fault, len(on))
	for i, o := range on {
		pins[i] = Fault{Step: "s", On: o}
	}

	return pins, make([]bool, len(pins))
}

// needing is a violation that holds exactly when every firing in need is
// present, which is how a real violation behaves when two failures together
// are what the workflow cannot absorb.
func needing(need ...faultAtom) func([]Fault) (bool, bool) {
	return func(candidate []Fault) (bool, bool) {
		have := firings(candidate)

		return !slices.ContainsFunc(need, func(a faultAtom) bool { return !slices.Contains(have, a) }), true
	}
}

func TestShrinkFindsTheOneFiringThatMatters(t *testing.T) {
	t.Parallel()

	pins, authored := pinsWith([]int{1, 2, 3, 4}, []int{1, 2})
	got := shrinkFaults(pins, authored, MaxShrinkRuns, needing(faultAtom{0, 3}))

	require.True(t, got.Reproduced)
	assert.True(t, got.Minimal)
	assert.Equal(t, 6, got.From)
	assert.Equal(t, []faultAtom{{0, 3}}, firings(got.Pins))
	assert.Less(t, got.Runs, 6+1, "delta debugging spends fewer probes than trying every firing")
}

func TestShrinkKeepsEveryFiringTheViolationNeeds(t *testing.T) {
	t.Parallel()

	pins, authored := pinsWith([]int{1, 2, 3}, []int{5, 6})
	need := []faultAtom{{0, 2}, {1, 6}}
	got := shrinkFaults(pins, authored, MaxShrinkRuns, needing(need...))

	require.True(t, got.Reproduced)
	assert.True(t, got.Minimal)
	assert.ElementsMatch(t, need, firings(got.Pins))
	assert.Len(t, got.Pins, 2, "a fault whose firings were all dropped leaves the list")
}

// 1-minimality is the promise, so it is checked directly: dropping any single
// firing from the result stops the violation.
func TestShrinkResultIsOneMinimal(t *testing.T) {
	t.Parallel()

	pins, authored := pinsWith([]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{1, 2, 3, 4})
	violates := func(candidate []Fault) (bool, bool) {
		// Violates when any two firings of fault 0 are consecutive.
		have := firings(candidate)
		for _, a := range have {
			if a.fault == 0 && slices.Contains(have, faultAtom{0, a.n + 1}) {
				return true, true
			}
		}

		return false, true
	}
	got := shrinkFaults(pins, authored, MaxShrinkRuns, violates)

	require.True(t, got.Reproduced)
	require.True(t, got.Minimal)
	atoms := firings(got.Pins)
	v, _ := violates(got.Pins)
	require.True(t, v)
	for i := range atoms {
		without := pinsOf(got.Pins, make([]bool, len(got.Pins)), slices.Delete(slices.Clone(atoms), i, i+1))
		v, _ = violates(without)
		assert.False(t, v, "removing %v should stop the violation", atoms[i])
	}
	assert.Len(t, atoms, 2)
}

func TestShrinkDoesNotTouchAPinTheCaseDeclared(t *testing.T) {
	t.Parallel()

	pins, _ := pinsWith([]int{1}, []int{1, 2, 3})
	authored := []bool{true, false}
	got := shrinkFaults(pins, authored, MaxShrinkRuns, needing(faultAtom{1, 2}))

	require.True(t, got.Reproduced)
	require.Len(t, got.Pins, 2)
	assert.Equal(t, []int{1}, got.Pins[0].On, "the case's own pin stays, needed or not")
	assert.Equal(t, []int{2}, got.Pins[1].On)
	assert.Equal(t, 3, got.From, "only drawn firings count as the size being shrunk")
}

// A set that does not violate when replayed alone was not found by shrinking;
// it is returned as given and says so, rather than being "reduced" from a
// starting point that proves nothing.
func TestShrinkRefusesAnInputThatDoesNotReproduce(t *testing.T) {
	t.Parallel()

	pins, authored := pinsWith([]int{1, 2, 3})
	got := shrinkFaults(pins, authored, MaxShrinkRuns, func([]Fault) (bool, bool) { return false, true })

	assert.False(t, got.Reproduced)
	assert.False(t, got.Minimal)
	assert.Equal(t, 1, got.Runs)
	assert.Equal(t, pins, got.Pins)
}

func TestShrinkSpendsAtMostItsBudgetAndSaysItIsNotMinimal(t *testing.T) {
	t.Parallel()

	on := make([]int, 64)
	for i := range on {
		on[i] = i + 1
	}
	pins, authored := pinsWith(on)
	probes := 0
	got := shrinkFaults(pins, authored, 5, func(candidate []Fault) (bool, bool) {
		probes++

		return needing(faultAtom{0, 40})(candidate)
	})

	assert.LessOrEqual(t, probes, 5)
	assert.Equal(t, probes, got.Runs)
	assert.True(t, got.Reproduced)
	assert.False(t, got.Minimal, "a search the budget ended cannot claim minimality")
	v, _ := needing(faultAtom{0, 40})(got.Pins)
	assert.True(t, v, "and what it returns still violates")
}

func TestShrinkOfASingleFiringIsMinimalAfterOneProbe(t *testing.T) {
	t.Parallel()

	pins, authored := pinsWith([]int{4})
	got := shrinkFaults(pins, authored, MaxShrinkRuns, needing(faultAtom{0, 4}))

	assert.True(t, got.Minimal)
	assert.Equal(t, 1, got.Runs)
}

// A probe cut off by a cancelled context answers nothing. Reading its "no" as a
// removal that stopped the violation would let a cancelled search claim a
// minimality it never established.
func TestShrinkCutOffMidSearchIsNotMinimal(t *testing.T) {
	t.Parallel()

	pins, authored := pinsWith([]int{1, 2, 3, 4, 5, 6})
	calls := 0
	got := shrinkFaults(pins, authored, MaxShrinkRuns, func(candidate []Fault) (bool, bool) {
		calls++
		if calls > 2 {
			return false, false
		}

		return needing(faultAtom{0, 5})(candidate)
	})

	assert.True(t, got.Reproduced)
	assert.False(t, got.Minimal)
	assert.Equal(t, 3, calls, "the search stops at the first cut-off probe")
	v, _ := needing(faultAtom{0, 5})(got.Pins)
	assert.True(t, v, "and what it returns still violates")
}

// A first replay that was cut off says so, rather than looking like a replay
// that completed and did not reproduce.
func TestShrinkCutOffBeforeTheFirstReplayIsInconclusive(t *testing.T) {
	t.Parallel()

	pins, authored := pinsWith([]int{1, 2, 3})
	got := shrinkFaults(pins, authored, MaxShrinkRuns, func([]Fault) (bool, bool) { return false, false })

	assert.False(t, got.Reproduced)
	assert.True(t, got.Inconclusive)
	assert.False(t, got.Minimal)
	assert.Equal(t, pins, got.Pins)

	done := shrinkFaults(pins, authored, MaxShrinkRuns, func([]Fault) (bool, bool) { return false, true })
	assert.False(t, done.Inconclusive, "a completed replay that does not reproduce is a definite answer")
}
