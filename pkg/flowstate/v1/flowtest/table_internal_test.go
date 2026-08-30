package flowtest

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEveryExpectationFieldIsMerged is the drift guard on [mergeExpectation],
// and it is here rather than as a comment because a hand-kept list of fields
// is exactly the thing that silently stops being complete (the lesson
// tools/fuzztargets' own bijection test exists for).
//
// It works by setting every field of an entry's expectation to a non-zero
// value, merging an empty row over it, and requiring the result to equal the
// entry. A field [mergeExpectation] forgets stays zero in the merge and fails
// here, naming itself.
func TestEveryExpectationFieldIsMerged(t *testing.T) {
	t.Parallel()

	entry := nonZeroExpectation()

	// Sanity: the fixture really does set every field, or the guard would
	// pass by having nothing to check — the vacuous-green shape this
	// repository legislates against.
	value := reflect.ValueOf(entry)
	for i := range value.NumField() {
		if value.Type().Field(i).Name == "fromEntry" {
			continue
		}
		require.False(t, value.Field(i).IsZero(),
			"the fixture leaves %s zero, so this guard would not notice it going unmerged",
			value.Type().Field(i).Name)
	}

	merged := mergeExpectation(entry, Expectation{})

	mergedValue := reflect.ValueOf(merged)
	for i := range mergedValue.NumField() {
		name := mergedValue.Type().Field(i).Name
		if name == "fromEntry" {
			continue
		}
		assert.False(t, mergedValue.Field(i).IsZero(),
			"Expectation.%s is not inherited by a row that states none; add it to mergeExpectation", name)
	}
	assert.Equal(t, expectationProvenance{
		outputs: true, inputs: true, refused: true, idempotencyKey: true, failed: true,
		errorContains: true, compensated: true, ran: true, skipped: true, others: true,
	}, merged.fromEntry, "every inherited value field must retain the entry as its writer")
	for i := range merged.Check {
		assert.True(t, merged.Check[i].fromEntry, "an accumulated entry claim lost its writer")
		merged.Check[i].fromEntry = false
	}
	merged.fromEntry = expectationProvenance{}
	assert.Equal(t, entry, merged, "a row that states nothing inherits its entry's expectation values entirely")
}

// TestAStatedFieldBeatsAnInheritedOne is the other direction, over every
// field at once: a row that states everything inherits nothing.
func TestAStatedFieldBeatsAnInheritedOne(t *testing.T) {
	t.Parallel()

	entry := nonZeroExpectation()
	row := Expectation{
		Outputs:        map[string]any{"row": true},
		Inputs:         map[string]any{"row": true},
		Refused:        ptr(false),
		IdempotencyKey: "row",
		Failed:         ptr(false),
		ErrorContains:  "row",
		Compensated:    []string{"row"},
		Ran:            []string{"row"},
		Skipped:        []string{"row"},
		Others:         "ran",
		Check:          []CheckClaim{{That: "1 == 1"}},
	}

	// Check is the deliberate exception to "inherits nothing": claims
	// accumulate, entry's first, because every level's predicates all hold.
	want := row
	inherited := entry.Check[0]
	inherited.fromEntry = true
	want.Check = append([]CheckClaim{inherited}, row.Check...)
	assert.Equal(t, want, mergeExpectation(entry, row))
}

// TestAnEmptyListIsAStatementNotAnAbsence: `ran: []` asserts that no step
// ran, which an author writes on purpose. Only a nil slice inherits.
func TestAnEmptyListIsAStatementNotAnAbsence(t *testing.T) {
	t.Parallel()

	entry := Expectation{Ran: []string{"a", "b"}, Compensated: []string{"c"}, Skipped: []string{"d"}}
	row := Expectation{Ran: []string{}, Compensated: []string{}, Skipped: []string{}}

	merged := mergeExpectation(entry, row)
	assert.Empty(t, merged.Ran, "an empty `ran:` is the claim that nothing ran, not an absent claim")
	assert.NotNil(t, merged.Ran)
	assert.Empty(t, merged.Compensated)
	assert.Empty(t, merged.Skipped)
}

func nonZeroExpectation() Expectation {
	return Expectation{
		Outputs:        map[string]any{"entry": true},
		Inputs:         map[string]any{"entry": true},
		Refused:        ptr(true),
		IdempotencyKey: "entry",
		Failed:         ptr(true),
		ErrorContains:  "entry",
		Compensated:    []string{"entry"},
		Ran:            []string{"entry"},
		Skipped:        []string{"entry"},
		Others:         "skipped",
		Check:          []CheckClaim{{That: "true"}},
	}
}

func ptr[T any](v T) *T { return &v }
