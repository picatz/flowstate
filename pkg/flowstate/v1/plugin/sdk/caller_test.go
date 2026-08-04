package sdk

import (
	"context"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCallerFromContextReadsWhatWasInstalled checks the additive half of Gap
// B: the wire already carries identity and namespace in ExecuteRequest, and
// this is the accessor that lets a task read them without [Task.Fn]'s
// signature changing to carry them explicitly.
func TestCallerFromContextReadsWhatWasInstalled(t *testing.T) {
	t.Parallel()

	identity := &flowstatev1.WorkloadIdentity{Subject: "ci", Namespace: "team-a"}
	ctx := contextWithCaller(t.Context(), identity, "team-a")

	caller, ok := CallerFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, "ci", caller.Identity.GetSubject())
	assert.Equal(t, "team-a", caller.Namespace)
}

// TestCallerFromContextReportsAnAbsentIdentity checks the case a single-tenant
// deployment with no identity provider actually has: a caller is still found,
// its namespace still readable, and Identity is simply nil rather than the
// bool reporting nothing was installed at all — the request still reached
// [taskService.Execute], which always installs one.
func TestCallerFromContextReportsAnAbsentIdentity(t *testing.T) {
	t.Parallel()

	ctx := contextWithCaller(t.Context(), nil, "")

	caller, ok := CallerFromContext(ctx)
	require.True(t, ok)
	assert.Nil(t, caller.Identity)
	assert.Equal(t, "", caller.Namespace)
}

// TestCallerFromContextIsAbsentWhenNothingInstalledIt checks the case that
// matters for a test calling a task's Fn directly rather than through the
// engine's dispatch: no caller was ever installed, and the bool says so.
func TestCallerFromContextIsAbsentWhenNothingInstalledIt(t *testing.T) {
	t.Parallel()

	_, ok := CallerFromContext(context.Background())
	assert.False(t, ok)
}
