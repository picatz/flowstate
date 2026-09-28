package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestBreakpointExpressionsNeedTheInspectAction: setting a condition or a log
// message needs workload.debug_inspect, so reading one back does too. A
// caller whose action list lacks it reads every expression-bearing breakpoint
// without its definition or its message; a plain breakpoint, and a caller
// with the action or with no action list, read the snapshot unchanged.
func TestBreakpointExpressionsNeedTheInspectAction(t *testing.T) {
	t.Parallel()

	snapshot := &v1.DebugSnapshot{Revision: 3, Breakpoints: []*v1.DebugBreakpointState{
		{Id: "plain", Verified: true, Definition: &v1.DebugBreakpoint{Id: "plain", Step: "after"}},
		{Id: "peek", Verified: true, Definition: &v1.DebugBreakpoint{Id: "peek", Step: "after", Condition: `secret == "hunter2"`}},
		{Id: "said", Message: `condition: ERROR: <input>:1:9: Syntax error | secret == "hunter2`, Definition: &v1.DebugBreakpoint{
			Id: "said", Step: "after", LogMessage: `saw {secret}`,
		}},
	}}
	original := proto.CloneOf(snapshot)
	as := func(actions ...string) context.Context {
		return auth.ContextWithPrincipal(t.Context(), auth.Principal{Issuer: "i", Subject: "s", Actions: actions})
	}

	for name, ctx := range map[string]context.Context{
		"no principal":       t.Context(),
		"no action list":     as(),
		"the inspect action": as("workload.debug", "workload.debug_inspect"),
	} {
		assert.True(t, proto.Equal(original, expressionsFor(ctx, snapshot)), "%s: the snapshot was changed", name)
	}

	withheld := expressionsFor(as("workload.debug"), snapshot)
	require.Len(t, withheld.GetBreakpoints(), 3)
	assert.True(t, proto.Equal(original.GetBreakpoints()[0], withheld.GetBreakpoints()[0]), "a plain breakpoint was withheld")
	for _, state := range withheld.GetBreakpoints()[1:] {
		assert.Nil(t, state.GetDefinition(), "%s kept its definition", state.GetId())
		assert.NotContains(t, state.GetMessage(), "hunter2", "%s kept a message quoting its expression", state.GetId())
		assert.Contains(t, state.GetMessage(), "workload.debug_inspect")
	}
	assert.True(t, withheld.GetBreakpoints()[1].GetVerified(), "withholding a definition changed whether it is armed")
	assert.True(t, proto.Equal(original, snapshot), "the run's own snapshot was changed")
}
