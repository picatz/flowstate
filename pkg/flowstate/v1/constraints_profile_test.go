package flowstatev1

import (
	"context"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
)

// TestMustUsesTheWorkflowProfile makes the latent #1465 bug observable before
// a second production profile ships. The test-only profile deliberately omits
// the strings library that CurrentProfile includes; both an ordinary value
// expression and a must: constraint pinned to it must therefore refuse trim.
func TestMustUsesTheWorkflowProfile(t *testing.T) {
	const profile = "test.restricted"
	profiles[profile] = nil
	t.Cleanup(func() { delete(profiles, profile) })

	current, err := DefaultEvaluator().ProfileEnv(CurrentProfile)
	require.NoError(t, err)
	parsed, issues := current.Parse(`" value ".trim()`)
	require.NoError(t, issues.Err())
	stored, err := cel.AstToParsedExpr(parsed)
	require.NoError(t, err)
	_, err = DefaultEvaluator().EvalParsedBase(context.Background(), profile, stored, map[string]any{})
	require.Error(t, err, "an ordinary expression used a library outside its recorded profile")

	must := `this.trim() == this`
	wf := &Workflow{
		Name:    "profile-pinned-must",
		Profile: profile,
		DeclaredInputs: []*InputDeclaration{{
			Name: "value", Type: InputDeclaration_TYPE_STRING, Must: &must,
		}},
		Steps: []*Node{{
			Id: "noop",
			Kind: &Node_Task{Task: &Task{
				Name: "log", Inputs: map[string]*Value{"message": NewLiteral("unused")},
			}},
		}},
	}
	_, err = BindRunInputs(wf, map[string]*Value{"value": NewLiteral("value")})
	require.ErrorContains(t, err, "undeclared reference to 'trim'")
}
