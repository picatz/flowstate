package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// truncatedProgram is a run whose site enumeration stops at
// [v1.MaxDebugStaticSites] before its last top-level step, `last`: a loop
// whose body is a `touch`, enough calls of a wide callee to pass the cap, then
// a top-level `touch` and `last` past the cut.
func truncatedProgram(t *testing.T) *v1.Workflow {
	t.Helper()

	log := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral(id)},
		}}}
	}
	// A callee called often enough that its sites alone pass the cap.
	callee := &v1.Workflow{Name: "wide"}
	for i := range 512 {
		callee.Steps = append(callee.Steps, log(fmt.Sprintf("s%d", i)))
	}
	spec := &v1.Workflow{Name: "fanout", Profile: v1.CurrentProfile, Steps: []*v1.Node{
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(v1.NewLiteral("a")), Iterator: "item", Body: []*v1.Node{log("touch")},
		}}},
	}}
	for i := range v1.MaxDebugStaticSites/len(callee.Steps) + 1 {
		spec.Steps = append(spec.Steps, &v1.Node{Id: fmt.Sprintf("call%d", i), Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}})
	}
	spec.Steps = append(spec.Steps, log("touch"), log("last"))
	sites, truncated := v1.DebugStaticSites(spec)
	require.True(t, truncated, "the program did not pass the cap, so this proves nothing")
	target, err := v1.ParseDebugTarget("last")
	require.NoError(t, err)
	require.Empty(t, target.Resolve(sites), "`last` was enumerated, so it is not past the cut")

	return spec
}

// TestATruncatedProgramJudgesBreakpointsByResolutionAlone: when the site
// enumeration stops at [v1.MaxDebugStaticSites], a step past the cut can still
// be an arrival a breakpoint matches. Here the only enumerated match for
// `touch` is inside a loop body, and the top-level `touch` past the cut is one
// the run holds at, so refusing the breakpoint as unholdable would be false —
// and would diverge from a history the engine recorded before the check, where
// it held there.
func TestATruncatedProgramJudgesBreakpointsByResolutionAlone(t *testing.T) {
	t.Parallel()

	spec := truncatedProgram(t)
	e := &executor{spec: spec, debug: &debugControl{carry: &v1.DebugCarry{
		Breakpoints: []*v1.DebugBreakpoint{{Id: "touch", Step: "touch"}},
	}}}
	e.parseDebugBreakpoints()

	require.Len(t, e.debug.parsed, 1)
	state := e.debug.parsed[0].state
	assert.True(t, state.GetVerified(), "refused as unholdable on a truncated enumeration: %s", state.GetMessage())
	assert.True(t, e.debug.parsed[0].target.Matches(v1.NewDebugOccurrence("fanout", nil, "touch", "task")),
		"the breakpoint does not match the top-level step past the cut")
}

// TestATruncatedProgramArmsABreakpointPastTheCut: a breakpoint whose only
// match is past the cut matches nothing enumerated, and a truncated
// enumeration cannot call that absent, so it is armed. A history recorded
// before [truncatedArmChange] refused it, and replays refusing it.
func TestATruncatedProgramArmsABreakpointPastTheCut(t *testing.T) {
	t.Parallel()

	spec := truncatedProgram(t)
	parse := func(t *testing.T, before bool) parsedBreakpoint {
		t.Helper()

		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		if before {
			env.OnGetVersion(truncatedArmChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
		}
		var parsed []parsedBreakpoint
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			e := &executor{ctx: ctx, spec: spec, debug: &debugControl{carry: &v1.DebugCarry{
				Breakpoints: []*v1.DebugBreakpoint{{Id: "last", Step: "last"}},
			}}}
			e.parseDebugBreakpoints()
			parsed = e.debug.parsed

			return nil
		})
		require.NoError(t, env.GetWorkflowError())
		require.Len(t, parsed, 1)

		return parsed[0]
	}

	armed := parse(t, false)
	assert.True(t, armed.state.GetVerified(), "refused a step past the cut: %s", armed.state.GetMessage())
	assert.Empty(t, armed.state.GetSites(), "listed sites the enumeration never reached")
	assert.True(t, armed.target.Matches(v1.NewDebugOccurrence("fanout", nil, "last", "task")),
		"the breakpoint does not match the step past the cut")

	replayed := parse(t, true)
	assert.False(t, replayed.state.GetVerified(), "a history from before the change replays into a hold it never had")
	assert.Contains(t, replayed.state.GetMessage(), `no step matches "last"`)
}
