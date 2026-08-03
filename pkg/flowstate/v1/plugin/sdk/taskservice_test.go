package sdk

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTaskServiceExecuteInstallsCallerBeforeFn checks the wiring
// [CallerFromContext] depends on: taskService.Execute installs the request's
// identity and namespace on the context before calling Fn, so a task that
// reads them sees what the wire actually carried rather than nothing.
func TestTaskServiceExecuteInstallsCallerBeforeFn(t *testing.T) {
	t.Parallel()

	var gotCaller Caller
	var gotOK bool

	svc := &taskService{tasks: map[string]Task{
		"whoami": {
			Name: "whoami",
			Fn: func(ctx context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				gotCaller, gotOK = CallerFromContext(ctx)
				return &flowstatev1.Node_Outputs{}, nil
			},
		},
	}}

	_, err := svc.Execute(t.Context(), connect.NewRequest(&pluginv1.ExecuteRequest{
		Task:      &flowstatev1.Task{Name: "whoami"},
		Identity:  &flowstatev1.WorkloadIdentity{Subject: "ci", Namespace: "team-a"},
		Namespace: "team-a",
	}))
	require.NoError(t, err)

	require.True(t, gotOK, "Fn ran without a caller installed on its context")
	assert.Equal(t, "ci", gotCaller.Identity.GetSubject())
	assert.Equal(t, "team-a", gotCaller.Namespace)
}

// TestTaskServiceExecuteInstallsCallerEvenWithNoIdentity checks the
// single-tenant, no-identity-provider case: the caller is still installed,
// carrying a nil Identity and the empty namespace, rather than the request
// leaving nothing for [CallerFromContext] to find.
func TestTaskServiceExecuteInstallsCallerEvenWithNoIdentity(t *testing.T) {
	t.Parallel()

	var gotOK bool

	svc := &taskService{tasks: map[string]Task{
		"whoami": {
			Name: "whoami",
			Fn: func(ctx context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				_, gotOK = CallerFromContext(ctx)
				return &flowstatev1.Node_Outputs{}, nil
			},
		},
	}}

	_, err := svc.Execute(t.Context(), connect.NewRequest(&pluginv1.ExecuteRequest{
		Task: &flowstatev1.Task{Name: "whoami"},
	}))
	require.NoError(t, err)
	assert.True(t, gotOK)
}
