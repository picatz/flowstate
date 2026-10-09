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

func boundWorkflow(steps ...*v1.Node) *v1.Workflow {
	return &v1.Workflow{
		Name:    "bind",
		Profile: v1.CurrentProfile,
		PluginRequirements: []*v1.PluginRequirement{conformance.BoundCredentialRequirement(map[string]*v1.Value{
			conformance.BoundCredentialName: {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "BOUND"}}},
		})},
		Steps: steps,
	}
}

func useStep(id string, inputs map[string]*v1.Value) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: conformance.BoundCredentialTaskName, Inputs: inputs}}}
}

func tokenOf(node *v1.Node) *v1.SecretRef {
	return node.GetTask().GetInputs()["token"].GetSecretRef()
}

func TestBindPluginCredentialsReachesEveryTaskPosition(t *testing.T) {
	registerBoundCredentialTask(t)

	undone := useStep("deploy", map[string]*v1.Value{"note": v1.NewLiteral("a")})
	undone.Undo = &v1.Compensation{Task: useStep("x", map[string]*v1.Value{"note": v1.NewLiteral("b")}).GetTask()}
	loop := &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
		Items: v1.NewExpr(`["one"]`),
		Body:  []*v1.Node{useStep("in_loop", map[string]*v1.Value{"note": v1.NewLiteral("c")})},
	}}}
	parallel := &v1.Node{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
		{Steps: []*v1.Node{useStep("in_branch", map[string]*v1.Value{"note": v1.NewLiteral("d")})}},
	}}}}
	wf := boundWorkflow(undone, loop, parallel)

	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))

	require.Equal(t, "BOUND", tokenOf(undone).GetName(), "the step")
	require.Equal(t, "BOUND", undone.GetUndo().GetTask().GetInputs()["token"].GetSecretRef().GetName(), "the compensation")
	require.Equal(t, "BOUND", tokenOf(loop.GetForEach().GetBody()[0]).GetName(), "a loop body")
	require.Equal(t, "BOUND", tokenOf(parallel.GetParallel().GetBranches()[0].GetSteps()[0]).GetName(), "a parallel branch")

	// Each step holds its own copy: one step's input changing must not change
	// another's or the binding the file wrote.
	tokenOf(undone).Name = "CHANGED"
	require.Equal(t, "BOUND", tokenOf(loop.GetForEach().GetBody()[0]).GetName())
	require.Equal(t, "BOUND", wf.GetPluginRequirements()[0].GetCredentials()[conformance.BoundCredentialName].GetSecretRef().GetName())
}

func TestBindPluginCredentialsIsIdempotentAndKeepsAnOverride(t *testing.T) {
	registerBoundCredentialTask(t)

	own := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "OWN"}}}
	wf := boundWorkflow(useStep("a", nil), useStep("b", map[string]*v1.Value{"token": own}))

	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))
	once := proto.Clone(wf)
	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))

	require.True(t, proto.Equal(once, wf), "binding twice changed the specification")
	require.Equal(t, "BOUND", tokenOf(wf.GetSteps()[0]).GetName())
	require.Equal(t, "OWN", tokenOf(wf.GetSteps()[1]).GetName(), "the binding replaced a step's own reference")
}

func TestBindPluginCredentialsLeavesOtherPluginsAndTasksAlone(t *testing.T) {
	registerBoundCredentialTask(t)

	other := &v1.Node{Id: "log", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")}}}}
	wf := boundWorkflow(other)
	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))
	require.Len(t, other.GetTask().GetInputs(), 1, "a task of no plugin received an input")

	// Binding nothing is not an error, for a plugin the registry does not hold.
	wf = &v1.Workflow{
		Name:               "unknown-plugin",
		PluginRequirements: []*v1.PluginRequirement{{Name: "elsewhere", MinimumVersion: "v1.0.0", Credentials: wf.GetPluginRequirements()[0].GetCredentials()}},
		Steps:              []*v1.Node{other},
	}
	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))
}

func TestBindPluginCredentialsIsBoundedByTheExpansion(t *testing.T) {
	registerBoundCredentialTask(t)

	// A binding as large as a reference may be, over enough steps to pass the
	// byte budget: refused, not expanded.
	huge := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: strings.Repeat("A", 1024)}}}
	steps := make([]*v1.Node, 0, 1200)
	for i := range 1200 {
		steps = append(steps, useStep(fmt.Sprintf("s%d", i), nil))
	}
	wf := boundWorkflow(steps...)
	wf.PluginRequirements[0].Credentials[conformance.BoundCredentialName] = huge

	err := v1.BindPluginCredentials(wf, v1.DefaultRegistry())
	require.ErrorContains(t, err, "would add more than")
	require.NotContains(t, err.Error(), strings.Repeat("A", 64), "the refusal echoed the reference")
}

func TestBindPluginCredentialsNeedsARegistry(t *testing.T) {
	require.ErrorContains(t, v1.BindPluginCredentials(boundWorkflow(useStep("a", nil)), nil), "no task registry")
	require.ErrorContains(t, v1.ElideBoundCredentials(boundWorkflow(useStep("a", nil)), nil), "no task registry")
}

func TestElideBoundCredentialsIsTheInverse(t *testing.T) {
	registerBoundCredentialTask(t)

	own := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "OWN"}}}
	wf := boundWorkflow(useStep("a", map[string]*v1.Value{"note": v1.NewLiteral("x")}), useStep("b", map[string]*v1.Value{"note": v1.NewLiteral("y"), "token": own}))
	authored := proto.Clone(wf)
	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))
	bound := proto.Clone(wf)

	require.NoError(t, v1.ElideBoundCredentials(wf, v1.DefaultRegistry()))
	require.True(t, proto.Equal(authored, wf), "eliding did not give back what was authored")

	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))
	require.True(t, proto.Equal(bound, wf), "expanding the elided form did not give back the expansion")
}
