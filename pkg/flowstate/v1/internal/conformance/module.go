package conformance

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// NoStepsCase is one spec with no steps and what both drivers must say when
// asked to run it.
type NoStepsCase struct {
	Name     string
	Workflow *v1.Workflow
	// Module is whether the refusal must name the spec a module.
	Module bool
}

// NoStepsCases proves a spec with no steps is refused rather than completed
// empty, and that a module is told it is one. A durable error crosses
// Temporal's wire as text, so the assertion is on the message both drivers
// share rather than on the sentinel.
func NoStepsCases() []NoStepsCase {
	return []NoStepsCase{
		{
			Name: "a module is refused and named",
			Workflow: &v1.Workflow{
				Name:           "ids",
				DeclaredErrors: []*v1.ErrorDeclaration{{Name: "NotFound"}},
			},
			Module: true,
		},
		{
			Name:     "an empty workflow is refused and not called a module",
			Workflow: &v1.Workflow{Name: "empty"},
		},
	}
}

// AssertNoStepsCases runs the shared cases through one driver's own entry.
func AssertNoStepsCases(t *testing.T, run func(*v1.Workflow) error) {
	t.Helper()

	for _, c := range NoStepsCases() {
		t.Run(c.Name, func(t *testing.T) {
			err := run(c.Workflow)
			require.Error(t, err)
			if c.Module {
				require.ErrorContains(t, err, `workflow "ids" `+v1.ErrModule.Error())
			} else {
				require.NotContains(t, err.Error(), v1.ErrModule.Error())
			}
		})
	}
}
