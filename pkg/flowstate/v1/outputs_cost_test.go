package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEvalRunOutputsWithCostChargesTheMustPredicate pins #1970's fourth level:
// a call inside a loop re-evaluates the callee's `outputs:` block every
// iteration, and an output's `must:` is CEL evaluated beneath that, so its cost
// has to land in the total the workflow-side budget reads. A satisfied
// predicate is the case that was invisible: nothing failed to draw attention
// to it.
func TestEvalRunOutputsWithCostChargesTheMustPredicate(t *testing.T) {
	t.Parallel()

	output := func(must *string) *v1.Workflow {
		return &v1.Workflow{Name: "wf", DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "channel", Value: v1.NewLiteral("stable"), Must: must},
		}}
	}
	scope := func() *v1.Scope { return v1.NewScope("", &v1.Workflow_StepOutputs{}) }

	_, without, err := v1.EvalRunOutputsWithCost(t.Context(), output(nil), scope())
	require.NoError(t, err)
	assert.Zero(t, without, "a literal output with no rule spends nothing")

	_, with, err := v1.EvalRunOutputsWithCost(t.Context(), output(new(`this.size() > 0 && this == "stable"`)), scope())
	require.NoError(t, err)
	assert.Positive(t, with, "a satisfied must: predicate is CEL work and has to be charged")

	_, refused, err := v1.EvalRunOutputsWithCost(t.Context(), output(new(`this == "beta"`)), scope())
	require.Error(t, err)
	assert.Positive(t, refused, "a refused predicate still did the work it was priced for")
}
