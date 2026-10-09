package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// registerBoundCredentialTask installs the fixture plugin task a `plugins:`
// credential binding is expanded against, for the test.
func registerBoundCredentialTask(t *testing.T) {
	t.Helper()

	require.NoError(t, v1.DefaultRegistry().Register(conformance.BoundCredentialTaskDef()))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(conformance.BoundCredentialTaskName) })
}

// TestCredentialBindingLocal is the local driver's half of the shared
// credential binding cases: a workflow whose steps omit a credential input runs
// with the `plugins:` binding, and a step that writes the input keeps its own.
// TestCredentialBindingDurable in engine/credentialbinding_test.go runs the
// identical [conformance.CredentialBindingCases] through the server's expansion
// and worker registration.
func TestCredentialBindingLocal(t *testing.T) {
	registerBoundCredentialTask(t)

	for _, test := range conformance.CredentialBindingCases() {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
			require.NotContains(t, test.Workflow.GetSteps()[0].GetTask().GetInputs(), "token",
				"the run expanded the caller's own workflow instead of a copy")
		})
	}
}

// TestRunWorkflowCredentialBindingsRefused is the local driver's half of the
// credential binding refusals: the server refuses these specifications at
// admission, so a rehearsal that ran them would say yes where production says
// no.
func TestRunWorkflowCredentialBindingsRefused(t *testing.T) {
	registerBoundCredentialTask(t)

	for _, test := range conformance.CredentialBindingRefusalCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			require.Error(t, err, "the submission was accepted")
			require.Contains(t, err.Error(), test.Contains)
			if test.Omits != "" {
				require.NotContains(t, err.Error(), test.Omits)
			}
			require.Empty(t, out.GetStepValues(), "a step ran before the refusal")
		})
	}
}
