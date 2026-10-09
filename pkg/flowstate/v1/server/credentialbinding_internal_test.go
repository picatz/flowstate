package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// Admission of a `plugins:` credential binding, on specifications built by hand.
//
// A hand-built specification reaches Run, SignalWithStart and CreateSchedule with
// no compiler in front of it, so the server's own expansion and checks are the
// only thing between a spec that omits or mis-binds a credential and workflow
// history. Internal, like plugins_internal_test.go, because the refusal has to
// come before the handler has spoken to Temporal, which is what lets this run
// without one.

// credentialBindingServer is a server whose catalog holds the fixture plugin, with
// the fixture task registered for the test.
func credentialBindingServer(t *testing.T) *FlowstateServer {
	t.Helper()

	require.NoError(t, v1.DefaultRegistry().Register(conformance.BoundCredentialTaskDef()))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(conformance.BoundCredentialTaskName) })

	return &FlowstateServer{pluginCatalog: testCatalog(installedPlugin(conformance.BoundCredentialPlugin, "v0.1.0"))}
}

// TestSubmissionRefusesWhatOmitsOrMisbindsACredential runs the shared refusal
// corpus through the path Run and SignalWithStart share. The local driver runs
// the same cases in TestRunWorkflowCredentialBindingsRefused.
func TestSubmissionRefusesWhatOmitsOrMisbindsACredential(t *testing.T) {
	s := credentialBindingServer(t)

	for _, refusal := range conformance.CredentialBindingRefusalCases() {
		t.Run(refusal.Name, func(t *testing.T) {
			_, err := s.validateSubmission(refusal.Workflow, refusal.Inputs)
			require.Error(t, err, "the submission was accepted")
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "%v", err)
			require.Contains(t, err.Error(), refusal.Contains)
			if refusal.Omits != "" {
				require.NotContains(t, err.Error(), refusal.Omits)
			}
		})
	}
}

// TestSubmissionExpandsABindingBeforeAnythingIsDurable is the positive direction
// the refusals above need to mean anything: the same specification, bound
// correctly, is admitted, and what the server would start is the expanded one.
func TestSubmissionExpandsABindingBeforeAnythingIsDurable(t *testing.T) {
	s := credentialBindingServer(t)

	for _, test := range conformance.CredentialBindingCases() {
		t.Run(test.Name, func(t *testing.T) {
			wf := test.Workflow
			_, err := s.validateSubmission(wf, nil)
			require.NoError(t, err)

			bound, overridden := wf.GetSteps()[0].GetTask().GetInputs()["token"], wf.GetSteps()[1].GetTask().GetInputs()["token"]
			require.Equal(t, "BOUND_TOKEN", bound.GetSecretRef().GetName(), "the omitting step did not receive the binding")
			require.Equal(t, "OVERRIDE_TOKEN", overridden.GetSecretRef().GetName(), "the binding replaced a step's own reference")
			require.Len(t, wf.GetResolvedPlugins(), 1, "the plugin was not pinned")
			require.NotSame(t, bound, wf.GetPluginRequirements()[0].GetCredentials()[conformance.BoundCredentialName],
				"a step shares the binding's message, so mutating one would change the other")
		})
	}
}

// TestCreateScheduleRefusesAnUnboundCredentialBeforeTemporal is the third
// creation path: a schedule that persisted a spec with a credential input bound
// nowhere would fail at three in the morning, after the spec was durable.
func TestCreateScheduleRefusesAnUnboundCredentialBeforeTemporal(t *testing.T) {
	s := credentialBindingServer(t)

	wf := conformance.CredentialBindingRefusalCases()[0].Workflow
	wf.Triggers = &v1.Triggers{Schedule: &v1.ScheduleTrigger{Cron: []string{"0 * * * *"}}}

	_, err := s.CreateSchedule(context.Background(), connect.NewRequest(&v1.CreateScheduleRequest{Name: "nightly", Workflow: wf}))

	require.ErrorContains(t, err, `receives the plugin's credential "api_token"`)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}
