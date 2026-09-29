package flowstatev1_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAFailureCarriesWhatEveryCalleeOnItsWayWithheld: each call boundary a
// failure leaves wraps it with that callee's set, and the outermost carrier
// answers for the whole chain, so a middle workflow's own set does not hide
// what its callee's failure carried (#2210).
func TestAFailureCarriesWhatEveryCalleeOnItsWayWithheld(t *testing.T) {
	t.Parallel()

	sensitive := func(value string) v1.SensitiveValues {
		return v1.SensitiveInputValues(map[string]*v1.Value{"x": v1.NewLiteral(value)}, map[string]bool{"x": true})
	}
	cause := errors.New("no such key: leaf-secret")
	leaf := fmt.Errorf("workflow %q: %w", "leaf", v1.WithFailureSensitiveValues(cause, sensitive("leaf-secret")))
	middle := fmt.Errorf("workflow %q: %w", "middle", v1.WithFailureSensitiveValues(fmt.Errorf("step %q: %w", "inner", leaf), sensitive("middle-secret")))

	carried := v1.FailureSensitiveValues(middle)
	assert.True(t, carried.IsSensitive("leaf-secret"), "the leaf's set was lost behind the middle's")
	assert.True(t, carried.IsSensitive("middle-secret"), "the middle's set was not carried")

	// The failure itself is unchanged: its text, and what errors.Is finds.
	assert.Equal(t, `workflow "middle": step "inner": workflow "leaf": no such key: leaf-secret`, middle.Error())
	assert.ErrorIs(t, middle, cause)
}

// TestAFailureCarryingNothingIsItself: an empty set, which is every run with
// no debugger, returns the failure as it was, and a failure that was never
// wrapped carries nothing.
func TestAFailureCarryingNothingIsItself(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	require.Same(t, cause, v1.WithFailureSensitiveValues(cause, v1.SensitiveValues{}))
	assert.True(t, v1.FailureSensitiveValues(cause).Empty())
	assert.True(t, v1.FailureSensitiveValues(nil).Empty())
	assert.NoError(t, v1.WithFailureSensitiveValues(nil, v1.WithheldSensitiveValues()))
}
