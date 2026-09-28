package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestATruncatedProgramJudgesBreakpointsByResolutionAlone: when the site
// enumeration stops at [v1.MaxDebugStaticSites], a step past the cut can still
// be an arrival a breakpoint matches. Here the only enumerated match for
// `touch` is inside a loop body, and the top-level `touch` past the cut is one
// the run holds at, so refusing the breakpoint as unholdable would be false —
// and would diverge from a history the engine recorded before the check, where
// it held there.
func TestATruncatedProgramJudgesBreakpointsByResolutionAlone(t *testing.T) {
	t.Parallel()

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
	spec.Steps = append(spec.Steps, log("touch"))
	_, truncated := v1.DebugStaticSites(spec)
	require.True(t, truncated, "the program did not pass the cap, so this proves nothing")

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
