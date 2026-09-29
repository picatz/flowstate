package flowstatev1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type identityObserver struct {
	identities []*sensitiveIdentity
}

func (*identityObserver) StepFinished(string, *Node_Outputs, error, bool) {}
func (*identityObserver) StepSkipped(string)                              {}
func (*identityObserver) WaitStarted(string, string, time.Duration, bool) {}

func (o *identityObserver) StepFinishedWithholding(_ string, _ *Node_Outputs, _ error, _ bool, withhold SensitiveValues) {
	o.identities = append(o.identities, withhold.identity)
}

// TestEveryStepOfAPositionReportsOneSet: the steps of one workflow position
// report the set built once for it, not one each, so a reader gathering them
// recognizes it by identity rather than reading it again (Copilot, #2215).
func TestEveryStepOfAPositionReportsOneSet(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	steps := make([]*Node, 0, 3)
	for _, id := range []string{"one", "two", "three"} {
		steps = append(steps, &Node{Id: id, Kind: &Node_Value{Value: NewExpr("1")}})
	}
	spec := &Workflow{Name: "parent", Profile: CurrentProfile, Steps: []*Node{{
		Id: "nested", Kind: &Node_Call{Call: &Call{
			Workflow: &Workflow{
				Name:           "child",
				Profile:        CurrentProfile,
				DeclaredInputs: []*InputDeclaration{{Name: "api_key", Type: InputDeclaration_TYPE_STRING, Sensitive: true}},
				Steps:          steps,
			},
			Arguments: map[string]*Value{"api_key": NewLiteral(secret)},
		}},
	}}}

	observer := &identityObserver{}
	_, err := RunWithInputs(NewContextWithRunObserver(t.Context(), observer), spec, nil)
	require.NoError(t, err)
	require.Len(t, observer.identities, 4, "three callee steps and the call")
	require.NotNil(t, observer.identities[0], "the callee's steps were told no set")
	assert.Same(t, observer.identities[0], observer.identities[1])
	assert.Same(t, observer.identities[0], observer.identities[2])
}
