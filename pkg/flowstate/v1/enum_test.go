package flowstatev1_test

import (
	"fmt"
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
}
