package auth

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// stringSliceType is the type every list inside a [ClaimRule] has, and what the
// walks below select fields by.
var stringSliceType = reflect.TypeOf([]string(nil))

// TestCloneSharesNoClaimRuleSliceWithItsSource is the guard for the defect that
// arrived with [ClaimRule.NoneOf]: [TrustedIssuer.clone] deep-copied AnyOf and
// left NoneOf aliased, so both verifiers held a slice their caller could still
// write to. That is the fail-open direction — emptying or rewriting an
// exclusion after construction widens a running entry — and it races
// verification besides.
//
// It walks [ClaimRule]'s fields by reflection rather than naming them, which is
// the whole point. A test that asserted "AnyOf is copied and NoneOf is copied"
// would have been written the day NoneOf landed and would say nothing about the
// third list somebody adds later; this one fails on that field the moment it
// exists. It is the same reasoning as
// TestClaimRuleFieldsAreAccountedFor next door, enforced rather than
// recorded, because here the right answer is known in advance: every list gets
// copied, with no per-field decision to make.
func TestCloneSharesNoClaimRuleSliceWithItsSource(t *testing.T) {
	rule := ClaimRule{Claim: "ref"}

	populated := 0
	fields := reflect.ValueOf(&rule).Elem()
	for i := range fields.NumField() {
		if fields.Field(i).Type() != stringSliceType {
			continue
		}
		fields.Field(i).Set(reflect.ValueOf([]string{"original"}))
		populated++
	}
	require.Positive(t, populated,
		"no []string field found on ClaimRule, so this test walked nothing and proved nothing")

	source := TrustedIssuer{
		Name:      "corp",
		Issuer:    "https://issuer.example",
		Audiences: []string{"flowstate"},
		Require:   []ClaimRule{rule},
	}
	clone := source.clone()

	// Write through the caller's own slices, exactly as a caller holding the
	// policy after building a verifier could.
	mutated := 0
	sourceRule := reflect.ValueOf(&source.Require[0]).Elem()
	for i := range sourceRule.NumField() {
		field := sourceRule.Field(i)
		if field.Type() != stringSliceType || field.Len() == 0 {
			continue
		}
		field.Index(0).SetString("mutated")
		mutated++
	}
	require.Equal(t, populated, mutated, "every list populated above must have been written through")

	clonedRule := reflect.ValueOf(&clone.Require[0]).Elem()
	for i := range clonedRule.NumField() {
		field := clonedRule.Field(i)
		if field.Type() != stringSliceType || field.Len() == 0 {
			continue
		}
		require.Equalf(t, "original", field.Index(0).String(),
			"TrustedIssuer.clone left ClaimRule.%s aliased to the caller's slice: a verifier built from this "+
				"policy reads whatever the caller writes next", clonedRule.Type().Field(i).Name)
	}
}
