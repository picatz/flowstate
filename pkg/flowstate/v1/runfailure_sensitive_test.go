package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func sensitiveInput(name string) *v1.InputDeclaration {
	return &v1.InputDeclaration{Name: name, Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}
}

func callOf(callee *v1.Workflow) *v1.Node {
	return &v1.Node{Id: "sub", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}
}

// TestRunFailureSensitiveValuesCoverCallees: a callee's sensitive input is
// bound from an expression at the call, so its value cannot be enumerated
// from the run's own inputs, and failure text is withheld whole.
func TestRunFailureSensitiveValuesCoverCallees(t *testing.T) {
	t.Parallel()

	const token = "synthetic-token-7a1e"
	inputs := map[string]*v1.Value{"token": v1.NewLiteral(token)}

	root := &v1.Workflow{Name: "root", DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("token")}}
	own := v1.RunFailureSensitiveValues(root, inputs)
	require.False(t, own.WithholdAll(), "the run's own inputs can be enumerated")
	require.NotContains(t, own.RedactText("GET /"+token+" failed", "[withheld]"), token)

	plain := &v1.Workflow{Name: "plain", Steps: []*v1.Node{callOf(&v1.Workflow{Name: "callee"})}}
	require.True(t, v1.RunFailureSensitiveValues(plain, nil).Empty(), "nothing is declared anywhere")

	nested := &v1.Workflow{Name: "caller", Steps: []*v1.Node{callOf(&v1.Workflow{
		Name:           "callee",
		DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("password")},
	})}}
	require.True(t, v1.RunFailureSensitiveValues(nested, nil).WithholdAll(),
		"a callee's sensitive input cannot be enumerated, so failure text must be withheld whole")
}

// TestACalleesDeclarationsAreSeenFromTheCaller: a caller reads a callee's
// outputs by expression and passes it values the same way, so a declaration
// anywhere below the root is one the root's own names cannot redact by.
func TestACalleesDeclarationsAreSeenFromTheCaller(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		wf   *v1.Workflow
		want bool
	}{
		"nothing embedded": {wf: &v1.Workflow{Name: "root", DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("token")}}},
		"a plain callee":   {wf: &v1.Workflow{Name: "root", Steps: []*v1.Node{callOf(&v1.Workflow{Name: "callee"})}}},
		"a sensitive input below": {wf: &v1.Workflow{Name: "root", Steps: []*v1.Node{callOf(&v1.Workflow{
			Name: "callee", DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("password")},
		})}}, want: true},
		"a sensitive output below": {wf: &v1.Workflow{Name: "root", Steps: []*v1.Node{callOf(&v1.Workflow{
			Name: "callee", DeclaredOutputs: []*v1.OutputDeclaration{{Name: "token", Sensitive: true}},
		})}}, want: true},
	} {
		got, err := v1.CalleeDeclaresSensitiveValues(tc.wf)
		require.NoError(t, err, name)
		require.Equal(t, tc.want, got, name)
	}
}

// TestAValueSetIsCostedByWhatItHolds: a cache of value sets is bounded by
// RetainedBytes, so it must count a sensitive default the run's inputs never
// named as surely as an input the caller passed.
func TestAValueSetIsCostedByWhatItHolds(t *testing.T) {
	t.Parallel()

	large := strings.Repeat("d", 32<<10)
	declared := sensitiveInput("token")
	declared.Default = v1.NewLiteral(large)
	wf := &v1.Workflow{Name: "root", DeclaredInputs: []*v1.InputDeclaration{declared}}

	fromDefault := v1.RunFailureSensitiveValues(wf, nil)
	require.False(t, fromDefault.WithholdAll())
	require.GreaterOrEqual(t, fromDefault.RetainedBytes(), len(large), "a bound default was not counted")

	fromInput := v1.RunFailureSensitiveValues(wf, map[string]*v1.Value{"token": v1.NewLiteral(large)})
	require.GreaterOrEqual(t, fromInput.RetainedBytes(), len(large), "a passed input was not counted")

	require.Zero(t, v1.SensitiveValues{}.RetainedBytes())
}
