package plugin

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// runCallerModePlugin is a real SDK fixture. It reads mode only through the
// public SDK accessor, after the request crossed the subprocess boundary.
func runCallerModePlugin() int {
	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "caller-mode",
		Version:     "0.0.1",
		Description: "reports the host-established caller mode",
		Tasks: []sdk.Task{{
			Name:   "read",
			Input:  &flowstatev1.Task_Log_Inputs{},
			Output: &flowstatev1.Task_Log_Outputs{},
			Fn: func(ctx context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				caller, ok := sdk.CallerFromContext(ctx)
				if !ok {
					return nil, sdk.InvalidInput("caller missing")
				}
				return &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
					"mode": flowstatev1.NewLiteral(int64(caller.Mode())),
				}}, nil
			},
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "caller-mode fixture: %v\n", err)
		return 1
	}
	return 0
}

// TestCallerModeHostSDKConformance proves the host request, protocol wire, SDK
// request handler, and public accessor agree. Unspecified and unknown future
// enum values remain unknown rather than falling through to production.
func TestCallerModeHostSDKConformance(t *testing.T) {
	t.Parallel()
	host := openHost(t, testConfig(t, pluginDir(t, "caller-mode")))
	require.Len(t, host.TaskDefs(), 1)
	def := host.TaskDefs()[0]

	for _, test := range []struct {
		name string
		sent flowstatev1.WorkloadIdentityMode
		want flowstatev1.WorkloadIdentityMode
	}{
		{"production", flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION, flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION},
		{"rehearsal", flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL, flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL},
		{"old host absence", flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_UNSPECIFIED, flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_UNSPECIFIED},
		{"unknown future enum", flowstatev1.WorkloadIdentityMode(99), flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_UNSPECIFIED},
	} {
		t.Run(test.name, func(t *testing.T) {
			const privateClaim = "claim-value-must-not-become-output"
			ctx := NewContextWithIdentity(t.Context(), &flowstatev1.WorkloadIdentity{
				Mode:   test.sent,
				Claims: map[string]string{"private": privateClaim},
			})
			outputs, err := def.Fn(ctx, nil, nil)
			require.NoError(t, err)
			got := flowstatev1.WorkloadIdentityMode(outputs.GetNamedValues()["mode"].GetLiteral().GetInt64Value())
			require.Equal(t, test.want, got)
			require.NotContains(t, fmt.Sprintf("%v", outputs), privateClaim,
				"reading mode must not copy identity claims into task outputs")
			if test.want == flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_UNSPECIFIED {
				require.NotEqual(t, flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION, got)
			}
		})
	}
}

// TestLocalDriverOverridesAnOrdinaryIdentityMode is the public-embedder case:
// Run marks its scope local regardless of how the optional context identity was
// constructed. The real host adapter must turn that host-owned fact into
// REHEARSAL before the real SDK plugin sees it, rather than trusting the
// ordinary identity's production value.
func TestLocalDriverOverridesAnOrdinaryIdentityMode(t *testing.T) {
	host := openHost(t, testConfig(t, pluginDir(t, "caller-mode")))
	require.Len(t, host.TaskDefs(), 1)

	registry := flowstatev1.NewRegistry()
	require.NoError(t, registry.Register(host.TaskDefs()[0]))
	ctx := flowstatev1.NewContextWithRegistry(t.Context(), registry)
	ctx = NewContextWithIdentity(ctx, &flowstatev1.WorkloadIdentity{
		Subject: "embedder",
		Mode:    flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION,
	})

	outputs, err := flowstatev1.Run(ctx, &flowstatev1.Workflow{
		Name: "local-embedder-mode",
		Steps: []*flowstatev1.Node{{
			Id: "read",
			Kind: &flowstatev1.Node_Task{Task: &flowstatev1.Task{
				Name: "caller-mode.read",
			}},
		}},
	})
	require.NoError(t, err)

	got := outputs.GetStepValues()["read"].GetNamedValues()["mode"].GetLiteral().GetInt64Value()
	require.Equal(t, int64(flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL), got)
}
