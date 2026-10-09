package flowtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
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

// messageType is the structural type that names the record name.
func messageType(name string) *v1.Type {
	return &v1.Type{Kind: &v1.Type_Message{Message: name}}
}

// binds reports whether the declaration accepts value for the input name.
func binds(spec *v1.Workflow, name string, value any) bool {
	_, err := v1.BindRunInputs(spec, v1.NewNamedValues(map[string]any{name: value}))

	return err == nil
}

// A record input is generated whole, from its declared type: one object with
// every field, including a nested list of another record, and one with only the
// required fields, and the declaration accepts each of them.
func TestFuzzGeneratesRecordInputsFromTheDeclaredType(t *testing.T) {
	t.Parallel()

	spec, _, err := flowfile.Parse([]byte(`
edition: v2026.4
name: orders
types:
  Line:
    fields:
      sku: {type: string, required: true}
      quantity: {type: int, required: true}
  Order:
    fields:
      id: {type: string, required: true}
      status: {type: enum, values: [open, paid]}
      lines: {type: "list(Line)"}
inputs:
  order: {type: Order, required: true}
steps:
  - id: noop
    log: {message: hi}
`))
	require.NoError(t, err)

	slots, skipped := inputSlots(spec)
	require.Empty(t, skipped)
	require.Len(t, slots, 1)
	assert.LessOrEqual(t, len(slots[0].candidates), maxTypeCandidates)

	var withLines, requiredOnly bool
	for _, candidate := range slots[0].candidates {
		object, ok := candidate.(map[string]any)
		require.True(t, ok, "%v", candidate)
		assert.True(t, binds(spec, "order", candidate), "%v", candidate)
		_, hasLines := object["lines"]
		_, hasStatus := object["status"]
		withLines = withLines || hasLines
		requiredOnly = requiredOnly || (!hasLines && !hasStatus && len(object) == 1)
	}
	assert.True(t, withLines, "a candidate carries the nested list of Line")
	assert.True(t, requiredOnly, "a candidate carries only the required fields")

	inputs, _, ok := generateInputs(spec, nil, DefaultFuzzSeed0)
	require.True(t, ok)
	assert.Contains(t, inputs, "order")
}

// Each type the generator now reaches yields a value the declaration binds: a
// list, a map, and the timestamp, duration and bytes spellings.
func TestFuzzGeneratesListMapAndDataKindInputs(t *testing.T) {
	t.Parallel()

	spec, _, err := flowfile.Parse([]byte(`
edition: v2026.4
name: kinds
inputs:
  tags: {type: "list(string)"}
  limits: {type: "map(string, int)"}
  at: {type: timestamp}
  wait: {type: duration}
  blob: {type: bytes}
steps:
  - id: noop
    log: {message: hi}
`))
	require.NoError(t, err)

	slots, skipped := inputSlots(spec)
	require.Empty(t, skipped)
	require.Len(t, slots, 5)
	for _, slot := range slots {
		var accepted int
		for _, candidate := range slot.candidates {
			if binds(spec, slot.name, candidate) {
				accepted++
			}
		}
		assert.Equal(t, len(slot.candidates), accepted, "%s: every candidate binds", slot.name)
		assert.Positive(t, accepted, slot.name)
	}
	assert.Contains(t, slots[0].candidates, []any{})
	assert.Contains(t, slots[1].candidates, map[string]any{})
}

// A record that reaches itself ends: the cycle is cut where it closes. An
// optional field that would close it is left out, a required one leaves the
// record ungenerated and says why.
func TestFuzzSelfReferentialRecordTerminates(t *testing.T) {
	t.Parallel()

	node := func(required bool) *v1.Workflow {
		return &v1.Workflow{
			DeclaredTypes: []*v1.TypeDeclaration{{Name: "Node", Fields: []*v1.InputDeclaration{
				{Name: "id", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
				{Name: "next", ValueType: messageType("Node"), Required: required},
			}}},
			DeclaredInputs: []*v1.InputDeclaration{{Name: "head", ValueType: messageType("Node")}},
		}
	}

	slots, skipped := inputSlots(node(false))
	require.Empty(t, skipped)
	require.Len(t, slots, 1)
	for _, candidate := range slots[0].candidates {
		assert.NotContains(t, candidate, "next")
	}

	slots, skipped = inputSlots(node(true))
	assert.Empty(t, slots)
	require.Len(t, skipped, 1)
	assert.Contains(t, skipped[0], "refers to itself")
}

// A value that holds a sensitive field is never generated, even when the input
// was not marked sensitive itself.
func TestFuzzSkipsARecordThatHoldsASensitiveField(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		DeclaredTypes: []*v1.TypeDeclaration{{Name: "Login", Fields: []*v1.InputDeclaration{
			{Name: "user", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
			{Name: "password", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true},
		}}},
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "login", ValueType: messageType("Login")},
			{Name: "logins", ValueType: &v1.Type{Kind: &v1.Type_List{List: messageType("Login")}}},
		},
	}

	slots, skipped := inputSlots(spec)
	assert.Empty(t, slots)
	require.Len(t, skipped, 2)
	for _, s := range skipped {
		assert.Contains(t, s, "sensitive")
	}
}

// What has no declared shape to draw from is still named, not silently left out:
// an untyped struct, and a record nothing under `types:` declares.
func TestFuzzStillReportsInputsWithNoShapeToDrawFrom(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
		{Name: "blob", Type: v1.InputDeclaration_TYPE_STRUCT},
		{Name: "ext", ValueType: messageType("acme.v1.Thing")},
	}}

	slots, skipped := inputSlots(spec)
	assert.Empty(t, slots)
	require.Len(t, skipped, 2)
	assert.Contains(t, skipped[0], "blob: ")
	assert.Contains(t, skipped[0], "dyn")
	assert.Contains(t, skipped[1], "ext: record acme.v1.Thing is not declared")
}

// Nested lists do not multiply: each level picks from its parts' candidates,
// so the set stays within the per-type cap and the size bound.
func TestFuzzNestedCandidatesStayBounded(t *testing.T) {
	t.Parallel()

	typ := &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}
	for range v1.MaxStructureDepth - 1 {
		typ = &v1.Type{Kind: &v1.Type_List{List: typ}}
	}

	candidates, _ := inputCandidates(nil, &v1.InputDeclaration{Name: "deep", ValueType: typ})
	require.NotEmpty(t, candidates)
	assert.LessOrEqual(t, len(candidates), maxTypeCandidates)
	for _, c := range candidates {
		assert.LessOrEqual(t, nodes(c), maxCandidateNodes)
	}
}

// TestFuzzRecordFanOutIsBoundedByWork: a record is expanded once per field that
// names it, so records that each name the next twice cost double per level. The
// candidate caps bound what is kept, not the work, so a chain this long would
// hang planning without the step budget.
func TestFuzzRecordFanOutIsBoundedByWork(t *testing.T) {
	t.Parallel()

	record := func(name string) *v1.Type { return &v1.Type{Kind: &v1.Type_Message{Message: name}} }
	const length = 24

	wf := &v1.Workflow{Name: "fan"}
	for i := range length {
		fields := []*v1.InputDeclaration{
			{Name: "a", ValueType: record(fmt.Sprintf("R%d", i+1))},
			{Name: "b", ValueType: record(fmt.Sprintf("R%d", i+1))},
		}
		wf.DeclaredTypes = append(wf.DeclaredTypes, &v1.TypeDeclaration{Name: fmt.Sprintf("R%d", i), Fields: fields})
	}
	wf.DeclaredTypes = append(wf.DeclaredTypes, &v1.TypeDeclaration{
		Name:   fmt.Sprintf("R%d", length),
		Fields: []*v1.InputDeclaration{{Name: "s", Required: true, ValueType: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}}},
	})
	wf.DeclaredInputs = []*v1.InputDeclaration{{Name: "root", Required: true, ValueType: record("R0")}}

	start := time.Now()
	slots, skipped := inputSlots(wf)
	assert.Less(t, time.Since(start), 5*time.Second, "planning the inputs took time exponential in the chain")
	assert.Empty(t, slots)
	require.Len(t, skipped, 1)
	assert.Contains(t, skipped[0], "root: its type nests more than")
}
