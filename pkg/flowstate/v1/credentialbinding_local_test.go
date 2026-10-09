package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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

// TestRunWorkflowRefusesABindingThatExpandsPastTheSizeLimit is the boundary the
// size check has to be asked at twice: the specification as written fits, and the
// binding copied into its step does not. The server asks again after expanding,
// and a rehearsal that did not would run what production refuses.
func TestRunWorkflowRefusesABindingThatExpandsPastTheSizeLimit(t *testing.T) {
	registerBoundCredentialTask(t)

	build := func(padding int) *v1.Workflow {
		wf := boundWorkflow(useStep("a", map[string]*v1.Value{"note": v1.NewLiteral(strings.Repeat("x", padding))}))

		return wf
	}
	size := func(wf *v1.Workflow) int { return proto.Size(&v1.RunState{Workflow: wf}) }

	// Pad until the workflow sits just under the limit with room for less than
	// the binding's expansion. Stepped, because the length prefix changes width.
	padding := v1.MaxSpecBytes - 1000
	for size(build(padding)) > v1.MaxSpecBytes-10 {
		padding--
	}
	for size(build(padding+1)) <= v1.MaxSpecBytes-10 {
		padding++
	}
	wf := build(padding)
	require.LessOrEqual(t, size(wf), v1.MaxSpecBytes, "the case must fit as written")

	_, err := v1.RunWithInputs(t.Context(), wf, nil)
	require.ErrorContains(t, err, "bytes together, over the")
	require.NotContains(t, wf.GetSteps()[0].GetTask().GetInputs(), "token", "the caller's workflow was expanded")

	// And the same workflow without the binding to expand is not refused for size.
	wf.PluginRequirements[0].Credentials = nil
	wf.GetSteps()[0].GetTask().Inputs["token"] = &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "T"}}}
	_, err = v1.RunWithInputs(t.Context(), wf, nil)
	require.NotContains(t, fmt.Sprint(err), "bytes together")
}
