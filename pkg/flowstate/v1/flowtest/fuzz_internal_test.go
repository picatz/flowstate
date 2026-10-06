package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The run bound lives in the library as well as the command: a caller of
// RunOptions cannot ask for more than the command would generate.
func TestNewFuzzerClampsRunsToTheBound(t *testing.T) {
	t.Parallel()

	assert.Equal(t, MaxFuzzRuns, newFuzzer(FuzzOptions{Runs: MaxFuzzRuns * 1000}).opts.Runs)
	assert.Equal(t, 7, newFuzzer(FuzzOptions{Runs: 7}).opts.Runs)
	assert.Nil(t, newFuzzer(FuzzOptions{}))
}

// The first seeds walk every boundary value of every input, one at a time with
// the rest as the case wrote them, and every optional input absent, so a small
// run still exercises each of them and a failure is localized to one input.
func TestCorpusWalksEachBoundaryOneInputAtATime(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
		{Name: "flag", Type: v1.InputDeclaration_TYPE_BOOL, Required: true},
		{Name: "count", Type: v1.InputDeclaration_TYPE_BOOL},
	}}
	base := map[string]any{"flag": true, "count": true}
	slots, _ := inputSlots(spec)

	var seen []map[string]any
	for n := range uint64(8) {
		inputs, ok := corpusInputs(spec, base, slots, n)
		if !ok {
			break
		}
		seen = append(seen, inputs)
	}
	assert.Equal(t, []map[string]any{
		{"flag": true, "count": true},
		{"flag": false, "count": true},
		{"flag": true},
		{"flag": true, "count": true},
		{"flag": true, "count": false},
	}, seen)
}

// A failing generated set is reduced to the inputs that cause the failure:
// the seed changed four, only one of them matters, and the rest go back to the
// case's own values.
func TestShrinkInputsKeepsOnlyTheInputsTheFailureNeeds(t *testing.T) {
	t.Parallel()

	base := map[string]any{"count": int64(4), "label": "hello", "region": "us"}
	generated := map[string]any{"count": int64(0), "label": "", "region": "eu", "extra": int64(7)}

	probes := 0
	shrunk := shrinkInputs(base, generated, nil, func(candidate map[string]any) (bool, bool, bool) {
		probes++

		return candidate["count"] == int64(0), true, true
	})
	require.True(t, shrunk.Reproduced)
	assert.True(t, shrunk.Minimal)
	assert.Equal(t, 4, shrunk.From)
	assert.Equal(t, probes, shrunk.Runs)
	assert.Equal(t, map[string]any{"count": int64(0), "label": "hello", "region": "us"}, shrunk.Inputs)
	assert.Equal(t, map[string]any{"count": int64(0)}, overlayOf(base, shrunk.Inputs))
}

// An input the case supplies and the run left out is a change too, and an
// overlay cannot say it: it is named separately, and putting it back is one of
// the reductions the search tries.
func TestShrinkInputsNamesAnInputTheRunLeftOut(t *testing.T) {
	t.Parallel()

	base := map[string]any{"count": int64(4), "note": "x"}
	generated := map[string]any{"count": int64(4)}

	shrunk := shrinkInputs(base, generated, nil, func(candidate map[string]any) (bool, bool, bool) {
		_, has := candidate["note"]

		return !has, true, true
	})
	require.True(t, shrunk.Reproduced)
	assert.Equal(t, 1, shrunk.From)
	assert.Equal(t, []string{"note"}, absentInputs(base, shrunk.Inputs, nil))
	assert.Empty(t, overlayOf(base, shrunk.Inputs))
}

// A sensitive input is never generated, so it is never among the changed ones
// and never named as absent, whatever the case holds for it.
func TestShrinkInputsNeverTouchesASensitiveInput(t *testing.T) {
	t.Parallel()

	base := map[string]any{"token": "s3cret", "count": int64(4)}
	generated := map[string]any{"count": int64(0)}
	sensitive := map[string]bool{"token": true}

	assert.Equal(t, []string{"count"}, changedInputs(base, generated, sensitive))
	assert.NotContains(t, absentInputs(base, generated, sensitive), "token")
}

// A set that does not fail again on its own is returned as it was: a smaller
// set found from a start that does not reproduce would shrink nothing.
func TestShrinkInputsReturnsTheInputWhenItDoesNotReproduce(t *testing.T) {
	t.Parallel()

	base := map[string]any{"count": int64(4)}
	generated := map[string]any{"count": int64(0)}

	shrunk := shrinkInputs(base, generated, nil, func(map[string]any) (bool, bool, bool) { return false, true, true })
	assert.False(t, shrunk.Reproduced)
	assert.False(t, shrunk.Minimal)
	assert.Equal(t, generated, shrunk.Inputs)
}

// A failure the case's own inputs already have is not caused by any generated
// input: with everything put back it still fails, so the shrunk set is empty.
func TestShrinkInputsFindsAFailureNoGeneratedInputCauses(t *testing.T) {
	t.Parallel()

	base := map[string]any{"count": int64(4)}
	generated := map[string]any{"count": int64(0)}

	shrunk := shrinkInputs(base, generated, nil, func(map[string]any) (bool, bool, bool) { return true, true, true })
	require.True(t, shrunk.Reproduced)
	assert.True(t, shrunk.Minimal)
	assert.Equal(t, base, shrunk.Inputs)
	assert.Empty(t, overlayOf(base, shrunk.Inputs))
	assert.Equal(t, 2, shrunk.Runs, "the whole set, then the empty one")
}

// A candidate the run could not judge (refused at submit, or errored before the
// run) does not reproduce the failure, but it does not prove the inputs it put
// back matter either, so the result is not claimed minimal.
func TestShrinkInputsDoesNotClaimMinimalFromAnUnjudgedProbe(t *testing.T) {
	t.Parallel()

	base := map[string]any{"a": int64(1), "b": int64(1)}
	generated := map[string]any{"a": int64(2), "b": int64(2)}

	shrunk := shrinkInputs(base, generated, nil, func(candidate map[string]any) (bool, bool, bool) {
		both := candidate["a"] == int64(2) && candidate["b"] == int64(2)

		return both, true, both // every smaller candidate is unjudged
	})
	require.True(t, shrunk.Reproduced)
	assert.False(t, shrunk.Minimal)
	assert.Equal(t, generated, shrunk.Inputs)
}

// A cancelled probe ends the search and leaves minimality unclaimed.
func TestShrinkInputsStopsWhenAProbeIsCancelled(t *testing.T) {
	t.Parallel()

	base := map[string]any{"a": int64(1), "b": int64(1)}
	generated := map[string]any{"a": int64(2), "b": int64(2)}

	calls := 0
	shrunk := shrinkInputs(base, generated, nil, func(map[string]any) (bool, bool, bool) {
		calls++
		if calls == 1 {
			return true, true, true
		}

		return false, false, false
	})
	assert.False(t, shrunk.Minimal)
	assert.Equal(t, 2, calls, "the search ends at the cancelled probe")
}
