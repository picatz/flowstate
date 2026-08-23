package flowstatev1_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// enumDecl declares an environment-shaped enum input over the given values,
// the shape every test below binds against.
func enumDecl(values ...string) *v1.InputDeclaration {
	return &v1.InputDeclaration{
		Name:   "environment",
		Type:   v1.InputDeclaration_TYPE_ENUM,
		Values: values,
	}
}

// TestBindRunInputsRefusesNonMemberEnumValue is the negative direction of
// membership: a submitted value that is not one of the declaration's own
// `values` is refused at BindRunInputs, the one choke point every submit path
// shares.
func TestBindRunInputsRefusesNonMemberEnumValue(t *testing.T) {
	t.Parallel()

	decl := enumDecl("staging", "production")
	wf := constrainedWorkflow(decl)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"environment": v1.NewLiteral("prod")})
	require.Error(t, err, "a non-member enum value was accepted")

	// Anti-vacuity: the refusal must name the declaration's own values
	// verbatim, which proves enforcement consulted the declared list rather
	// than emitting a generic error.
	assert.Contains(t, err.Error(), `"staging"`)
	assert.Contains(t, err.Error(), `"production"`)
	assert.Contains(t, err.Error(), "environment")
	// "prod" is one edit from "production"? No - it's a prefix, distance is
	// large; check the nearer one instead: "prod" is closer to nothing here,
	// so just assert the message names what was given.
	assert.Contains(t, err.Error(), `"prod"`)
}

// TestBindRunInputsBoundsEnumSuggestionWork proves that an outside-controlled
// value too long to be a typo is refused without sending it through the
// quadratic edit-distance scan. The diagnostic still names the submitted
// value and the declaration's choices; only the inapplicable suggestion is
// skipped.
func TestBindRunInputsBoundsEnumSuggestionWork(t *testing.T) {
	t.Parallel()

	values := make([]string, 64)
	for i := range values {
		values[i] = fmt.Sprintf("%03d-%s", i, strings.Repeat("x", 124))
	}
	const submittedBytes = 256 << 10
	submitted := strings.Repeat("z", submittedBytes)
	wf := constrainedWorkflow(enumDecl(values...))

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"environment": v1.NewLiteral(submitted)})
	require.Error(t, err, "a non-member enum value was accepted")
	assert.Contains(t, err.Error(), strconv.Quote(submitted))
	assert.Contains(t, err.Error(), strconv.Quote(values[0]))
	assert.NotContains(t, err.Error(), "did you mean")
}

// TestBindRunInputsSuggestsNearbyMultibyteEnumValue keeps the bound in rune
// units, as nearest.Distance is: byte length must not suppress a legitimate
// suggestion in a multibyte script.
func TestBindRunInputsSuggestsNearbyMultibyteEnumValue(t *testing.T) {
	t.Parallel()

	wf := constrainedWorkflow(enumDecl("環境本番"))

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"environment": v1.NewLiteral("環境本番版")})
	require.Error(t, err, "a non-member enum value was accepted")
	assert.Contains(t, err.Error(), `did you mean "環境本番"?`)
}

// TestBindRunInputsRefusesNonMemberDefault is the same negative direction
// against a declaration's own default rather than a caller's submission.
// BindRunInputs already checks a defaulted value through the identical path
// a submitted one takes (CheckInputValue then CheckInputConstraints), so this
// works without new plumbing — this test verifies that claim rather than
// assuming it.
func TestBindRunInputsRefusesNonMemberDefault(t *testing.T) {
	t.Parallel()

	decl := enumDecl("staging", "production")
	decl.Default = v1.NewLiteral("prod")
	wf := constrainedWorkflow(decl)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{})
	require.Error(t, err, "a non-member default was accepted")
	assert.Contains(t, err.Error(), `"staging"`)
	assert.Contains(t, err.Error(), `"production"`)
	assert.Contains(t, err.Error(), "environment")
}

// TestBindRunInputsRefusesValuesOnNonEnumType is the shape check: `values:`
// declared on a TYPE_STRING input is refused, because a set-fact like "which
// type may declare `values`" belongs to CheckInputConstraintShape.
func TestBindRunInputsRefusesValuesOnNonEnumType(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name:   "environment",
		Type:   v1.InputDeclaration_TYPE_STRING,
		Values: []string{"staging", "production"},
	}
	wf := constrainedWorkflow(decl)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"environment": v1.NewLiteral("staging")})
	require.Error(t, err, "values: declared on a non-enum type was accepted")
	assert.Contains(t, err.Error(), "environment")
	assert.Contains(t, err.Error(), "values")
}

// TestBindRunInputsRefusesEnumWithNoValues is the other shape check: an
// enum-typed declaration with no `values` at all is refused, since an empty
// domain describes nothing a submitted value could ever be a member of.
func TestBindRunInputsRefusesEnumWithNoValues(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "environment", Type: v1.InputDeclaration_TYPE_ENUM}
	wf := constrainedWorkflow(decl)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"environment": v1.NewLiteral("anything")})
	require.Error(t, err, "an enum declared with no values was accepted")
	assert.Contains(t, err.Error(), "environment")
}

// TestBindRunInputsRefusesEnumOver64Values is the bound: 65 declared members
// is refused by protovalidate's `max_items: 64` on [InputDeclaration.values]
// — the schema rule this slice added, enforced by [v1.Validate] wherever a
// submitted [Workflow] is validated at the RPC boundary. This asserts the
// bound is *reached*, not merely that 65 fails: 64 members must both validate
// cleanly and bind successfully, member by member, through [BindRunInputs] —
// a check that only exercised the 65-member boundary case would also pass a
// bound that gave up after the first member.
func TestBindRunInputsRefusesEnumOver64Values(t *testing.T) {
	t.Parallel()

	values64 := make([]string, 64)
	for i := range values64 {
		values64[i] = fmt.Sprintf("v%d", i)
	}
	decl64 := enumDecl(values64...)
	wf64 := constrainedWorkflow(decl64)

	require.NoError(t, v1.Validate(wf64), "a 64-value enum declaration was refused")

	// 64 is accepted: bind every declared member successfully (anti-vacuity:
	// enforcement actually consults the declared list, member by member,
	// rather than a check that only exercises the boundary case).
	for _, want := range values64 {
		_, err := v1.BindRunInputs(wf64, map[string]*v1.Value{"environment": v1.NewLiteral(want)})
		require.NoErrorf(t, err, "member %q of a 64-value enum was refused", want)
	}

	values65 := append(append([]string{}, values64...), "v64")
	decl65 := enumDecl(values65...)
	wf65 := constrainedWorkflow(decl65)

	err := v1.Validate(wf65)
	require.Error(t, err, "a 65-value enum declaration was accepted")
	assert.Contains(t, err.Error(), "values")
	assert.Contains(t, err.Error(), "64")

	// The disagreement PR #621's review finding named: BindRunInputs used to
	// accept this same 65-value declaration, because CheckInputConstraintShape
	// checked only membership (type mismatch, empty domain), never the
	// per-member and list-size rules the schema itself declares on `values`.
	// Both execution drivers reach BindRunInputs before running anything, so
	// this is the assertion that closes the gap for both at once rather than
	// for whichever driver a test happens to exercise.
	_, err = v1.BindRunInputs(wf65, map[string]*v1.Value{"environment": v1.NewLiteral("v0")})
	require.Error(t, err, "BindRunInputs accepted a 65-value enum declaration")
	assert.Contains(t, err.Error(), "64")
}

// distinctEnumValues returns n values, each exactly length characters and
// distinct from every other, for building a values: list that sits at a
// chosen point relative to the schema's bounds without also tripping a
// different rule than the one under test.
func distinctEnumValues(n, length int) []string {
	vals := make([]string, n)
	for i := range vals {
		vals[i] = fmt.Sprintf("v%0*d", length-1, i)
	}
	return vals
}

// TestEnumValuesShapeAgreesWithValidate is the guard PR #621's review finding
// asked for, and the actual deliverable: [v1.CheckInputConstraintShape] —
// what [v1.BindRunInputs] enforces on both execution drivers, and what the
// flowfile compiler's author-time diagnostic now delegates to — and
// [v1.Validate] — what durable submission's complete-request check applies,
// through the server's own validation of a [v1.RunRequest] — must reach the
// identical verdict on a declared enum's `values:`, at and past each of the
// three bounds the schema itself declares on [v1.InputDeclaration_values]:
// 64 items, 128 characters per item, and distinctness.
//
// This is deliberately not a test of either function's behavior in
// isolation — both are covered elsewhere — but of the join: that
// checkEnumValuesShape's choice to derive its answer from [v1.Validate]
// rather than restate "64, 128, unique" a second time actually keeps the two
// from disagreeing, which is the property CLAUDE.md's "one rule, not two"
// exists to protect and the property that would silently break if that seam
// were ever swapped back out for a hand-copied bound.
func TestEnumValuesShapeAgreesWithValidate(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		values  []string
		wantErr bool
	}{
		{name: "63 distinct, 128 chars: under every bound", values: distinctEnumValues(63, 128), wantErr: false},
		{name: "64 distinct, 128 chars: exactly at every bound", values: distinctEnumValues(64, 128), wantErr: false},
		{name: "65 distinct, 128 chars: one past the count bound", values: distinctEnumValues(65, 128), wantErr: true},
		{
			name:    "64 members, one at 129 chars: one past the length bound",
			values:  append(distinctEnumValues(63, 128), strings.Repeat("z", 129)),
			wantErr: true,
		},
		{
			name:    "64 members, one at exactly 128 chars: still at the length bound",
			values:  append(distinctEnumValues(63, 128), strings.Repeat("z", 128)),
			wantErr: false,
		},
		{
			name:    "64 members, one duplicated: fails distinctness at the count bound",
			values:  append(distinctEnumValues(63, 128), distinctEnumValues(1, 128)[0]),
			wantErr: true,
		},
		{
			name:    "64 members, one empty: fails the length floor",
			values:  append(distinctEnumValues(63, 128), ""),
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			decl := enumDecl(test.values...)
			shapeErr := v1.CheckInputConstraintShape(decl)

			wf := constrainedWorkflow(decl)
			validateErr := v1.Validate(wf)

			if test.wantErr {
				assert.Errorf(t, shapeErr, "CheckInputConstraintShape accepted: %s", test.name)
				assert.Errorf(t, validateErr, "Validate accepted: %s", test.name)
			} else {
				assert.NoErrorf(t, shapeErr, "CheckInputConstraintShape refused: %s", test.name)
				assert.NoErrorf(t, validateErr, "Validate refused: %s", test.name)
			}

			// The join itself: not merely that each independently matches
			// wantErr, but that neither one accepts what the other refuses.
			assert.Equalf(t, validateErr == nil, shapeErr == nil,
				"CheckInputConstraintShape and Validate disagreed for %s: shape=%v validate=%v",
				test.name, shapeErr, validateErr)
		})
	}
}
