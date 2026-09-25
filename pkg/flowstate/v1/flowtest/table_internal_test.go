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
		Refused:        new(false),
		IdempotencyKey: "row",
		Failed:         new(false),
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

// TestExpandingATableDoesNotNormalizeTheCallersClaims: [Run] expands a
// Go-built File before making its own shallow copy. A tolerated fence is
// stripped during entry validation, so that validation must own its claim
// slice rather than mutate the caller's File — or two concurrent runs race on
// the same CheckClaim.That.
func TestExpandingATableDoesNotNormalizeTheCallersClaims(t *testing.T) {
	t.Parallel()

	tests := []Test{{
		Name:   "table",
		Expect: Expectation{Check: []CheckClaim{{That: "${true}"}}},
		Cases:  []Test{{Name: "row"}},
	}}
	p := newProblems(nil)
	expanded, _ := expandTableEntries(p, tests)

	require.Nil(t, p.err())
	require.Len(t, expanded, 1)
	require.Len(t, expanded[0].Expect.Check, 1)
	assert.Equal(t, "${true}", tests[0].Expect.Check[0].That,
		"expansion normalized a claim through the caller's slice")
	assert.Equal(t, "true", expanded[0].Expect.Check[0].That,
		"the effective row did not retain the normalized claim")
}

// TestMergeRowKeepsSecretsWholeOrNothing pins docs/CLI.md's documented
// contract unchanged: a row's own `secrets:` replaces the entry's binding
// entirely, which is how a row exercises the "no matching secrets entry"
// refusal for a secret its entry declares. [expandTableEntries]'s
// entrySecretMaterial is the answer to what that row's *redaction* posture
// still owes the entry's plaintext (#2041); it is not a reason to change what
// [mergeRow] binds.
func TestMergeRowKeepsSecretsWholeOrNothing(t *testing.T) {
	t.Parallel()

	entry := Test{Name: "entry", Secrets: map[string]string{"env:VENDOR_TOKEN": "entry-material"}}
	row := Test{Name: "row", Secrets: map[string]string{"env:ROW_TOKEN": "row-material"}}

	merged := mergeRow(entry, row)
	assert.Equal(t, map[string]string{"env:ROW_TOKEN": "row-material"}, merged.Secrets,
		"a row naming its own secret does not bind the entry's — that is the row choosing not to")

	// And the ordinary inheritance direction: a row that states none of its
	// own inherits the entry's whole map.
	merged = mergeRow(entry, Test{Name: "row"})
	assert.Equal(t, entry.Secrets, merged.Secrets)
}

// TestExpandTableEntriesCarriesTheEntrysSecretMaterialToEveryRow drives
// #2041's table route: a row's redaction posture must still withhold its
// entry's secret plaintext even when the row's own `secrets:` replaces the
// entry's binding — see [Test.entrySecretMaterial]'s doc for why that is a
// separate concern from what [mergeRow] binds. Every row under the same
// entry shares the identical slice, proving the entry's material is read
// once rather than copied per row.
func TestExpandTableEntriesCarriesTheEntrysSecretMaterialToEveryRow(t *testing.T) {
	t.Parallel()

	tests := []Test{{
		Name:    "entry",
		Secrets: map[string]string{"env:VENDOR_TOKEN": "entry-material"},
		Cases: []Test{
			{Name: "inherits", Expect: Expectation{Outputs: map[string]any{}}},
			{Name: "overrides", Secrets: map[string]string{"env:ROW_TOKEN": "row-material"},
				Expect: Expectation{Outputs: map[string]any{}}},
		},
	}}

	p := newProblems(nil)
	expanded, _ := expandTableEntries(p, tests)
	require.Nil(t, p.err())
	require.Len(t, expanded, 2)

	for _, test := range expanded {
		assert.Equal(t, []string{"entry-material"}, test.entrySecretMaterial,
			"row %q must carry its entry's secret material for redaction regardless of its own Secrets", test.Name)
	}
	assert.Equal(t, &expanded[0].entrySecretMaterial[0], &expanded[1].entrySecretMaterial[0],
		"every row under one entry shares the same slice rather than a copy each")
}

func nonZeroExpectation() Expectation {
	return Expectation{
		Outputs:        map[string]any{"entry": true},
		Inputs:         map[string]any{"entry": true},
		Refused:        new(true),
		IdempotencyKey: "entry",
		Failed:         new(true),
		ErrorContains:  "entry",
		Compensated:    []string{"entry"},
		Ran:            []string{"entry"},
		Skipped:        []string{"entry"},
		Others:         "skipped",
		Check:          []CheckClaim{{That: "true"}},
	}
}
