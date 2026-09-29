package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// parseConditioned parses one breakpoint on spec inside a workflow, as a
// segment does, answering [conditionScopeChange] as a history recorded before
// it when before is set.
func parseConditioned(t *testing.T, spec *v1.Workflow, bp *v1.DebugBreakpoint, before bool) parsedBreakpoint {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: conformance.BoundaryDeadlockDetectionTimeout})
	if before {
		env.OnGetVersion(conditionScopeChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	}
	var parsed []parsedBreakpoint
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		e := &executor{ctx: ctx, spec: spec, debug: &debugControl{carry: &v1.DebugCarry{
			Breakpoints: []*v1.DebugBreakpoint{bp},
		}}}
		e.parseDebugBreakpoints()
		parsed = e.debug.parsed

		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, parsed, 1)

	return parsed[0]
}

// TestADurableConditionReadingANameNothingBindsIsRefused is #2194 on the
// durable driver: the same refusal the local one gives, from the same
// [v1.CheckDebugConditionScope]. A history recorded before
// [conditionScopeChange] armed it, and replays arming it.
func TestADurableConditionReadingANameNothingBindsIsRefused(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{Name: "scoped", Profile: v1.CurrentProfile, Steps: []*v1.Node{
		{Id: "compose", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(1), Iterator: "amount",
			Body: []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("amount")}}},
		}}},
	}}

	for condition, refusal := range map[string]string{
		"nosuch > 1": "`nosuch` is not bound where this breakpoint fires",
		"amount > 1": "`amount` is bound only inside the loops and steps that declare it",
	} {
		bp := &v1.DebugBreakpoint{Id: "compose", Step: "compose", Condition: condition}
		refused := parseConditioned(t, spec, bp, false)
		assert.False(t, refused.state.GetVerified(), "%s: armed a condition nothing can bind", condition)
		assert.Contains(t, refused.state.GetMessage(), refusal, condition)
		assert.Nil(t, refused.condition, condition)

		replayed := parseConditioned(t, spec, bp, true)
		assert.True(t, replayed.state.GetVerified(), "%s: a history from before the change replays into a refusal it never had: %s",
			condition, replayed.state.GetMessage())
		assert.NotNil(t, replayed.condition, condition)
	}

	armed := parseConditioned(t, spec, &v1.DebugBreakpoint{Id: "compose", Step: "compose", Condition: "steps.compose == 1"}, false)
	assert.True(t, armed.state.GetVerified(), "refused a condition reading a root: %s", armed.state.GetMessage())
	assert.Empty(t, armed.state.GetMessage())
}

// TestATruncatedProgramSaysItsConditionIsUnchecked: past a cut enumeration
// the sites a breakpoint fires at are not known, so its condition's names
// cannot be judged. It is armed, as before the check, and says so rather than
// reading as checked.
func TestATruncatedProgramSaysItsConditionIsUnchecked(t *testing.T) {
	t.Parallel()

	parsed := parseConditioned(t, truncatedProgram(t), &v1.DebugBreakpoint{Id: "last", Step: "last", Condition: "nosuch > 1"}, false)
	assert.True(t, parsed.state.GetVerified(), "refused past the cut: %s", parsed.state.GetMessage())
	assert.Contains(t, parsed.state.GetMessage(), "the condition's names are not checked")
}
