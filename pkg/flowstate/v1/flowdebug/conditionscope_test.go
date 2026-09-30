package flowdebug_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A condition reading a bare name no site its breakpoint fires at can bind is
// refused when it is set (#2194). These are the local driver's fronts: the
// prompt's `break` and `until`, and the typed contract DAP and the Driver
// reach. The durable driver's refusal is the engine's own test; the words are
// one [v1.CheckDebugConditionScope].

// TestThePromptRefusesAConditionNothingCanBind: typed at the first step, before
// the loop binds `n`, a condition on the loop's body that reads `n` is
// accepted and fires, while one naming a name nothing binds, or naming `n` at
// a step outside the loop, is refused and never armed.
func TestThePromptRefusesAConditionNothingCanBind(t *testing.T) {
	t.Parallel()

	script := strings.Join([]string{
		"break body if nosuch > 1",
		"break compose if n > 1",
		"until compose if nosuch",
		"break body if n == 1",
		"break compose if type(1) == string",
		"breakpoints",
		"continue",
		"continue",
		"",
	}, "\n")
	var console strings.Builder
	program := &v1.Workflow{Name: "looping", Steps: []*v1.Node{
		{Id: "first", Kind: &v1.Node_Value{Value: v1.NewExpr("0")}},
		{Id: "compose", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:    v1.NewLiteralList(0, 1, 2),
			Iterator: "n",
			Body:     []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("n")}}},
		}}},
	}}
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader(script), Out: &console, Workflow: program})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	_, err = v1.Run(v1.NewContextWithDebugger(t.Context(), session), program)
	require.NoError(t, err)
	out := console.String()

	assert.Contains(t, out, "break body: `nosuch` is not bound where this breakpoint fires")
	assert.Contains(t, out, "and here `n`", "the refusal did not say what is bound at the loop's body")
	assert.Contains(t, out, "break compose: `n` is bound only inside the loops and steps that declare it")
	assert.Contains(t, out, "until compose: `nosuch` is not bound")
	assert.NotContains(t, out, "break at compose", "a refused `until` or breakpoint still stopped at compose")
	assert.Equal(t, 1, strings.Count(out, "break at each[1]/body ("), "the condition set before the loop bound `n` did not fire")
	assert.NotContains(t, out, "could not be evaluated", "a refused condition was armed and declined at an arrival")
	assert.Contains(t, out, "breakpoint at compose if type(1) == string", "a type value was refused as a name nothing binds")
	assert.NotContains(t, out, "did you mean `run`", "a binding that exists elsewhere was offered an unrelated near name")
}

// TestTheTypedContractRefusesAConditionNothingCanBind: the contract DAP, the
// Driver and the MCP session tools reach says so on the breakpoint's state, and
// suggests the near name.
func TestTheTypedContractRefusesAConditionNothingCanBind(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)

	response, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		RequestId: "scope",
		Breakpoints: []*v1.DebugBreakpoint{
			{Id: "typo", Step: "touch", Condition: "itme == 2"},
			{Id: "outside", Step: "done", Condition: "item == 2"},
			{Id: "callee", Step: "nested/greet", Condition: "item == 2"},
			{Id: "inside", Step: "touch", Condition: "item == 2 && vars.items[0] == 1"},
		},
	})
	require.NoError(t, err)
	states := map[string]*v1.DebugBreakpointState{}
	for _, state := range response.GetBreakpoints() {
		states[state.GetId()] = state
	}
	assert.False(t, states["typo"].GetVerified())
	assert.Contains(t, states["typo"].GetMessage(), "condition: `itme` is not bound where this breakpoint fires")
	assert.True(t, strings.HasSuffix(states["typo"].GetMessage(), "; did you mean `item`?"), states["typo"].GetMessage())
	assert.False(t, states["outside"].GetVerified())
	assert.Contains(t, states["outside"].GetMessage(), "`item` is bound only inside")
	assert.False(t, states["callee"].GetVerified(), "a callee's step sees none of its caller's bare names")
	assert.True(t, states["inside"].GetVerified(), states["inside"].GetMessage())

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, "each[1]/touch", at.GetOccurrence().GetAddress())
	assert.Equal(t, []string{"inside"}, at.GetBreakpointIds())
}

// TestEachCasesProgramReplacesTheLast: one session may hold several cases'
// runs (`flowtest.RunOptions.Debugger`), and each case may run a different
// program. Each [flowdebug.Session.Program] replaces the last, so a breakpoint
// set while the second case is held is judged against the second program, and
// one set against the first that the second refuses is removed with a notice
// saying why (Codex, #2202). A session given its program keeps it.
func TestEachCasesProgramReplacesTheLast(t *testing.T) {
	t.Parallel()

	looping := &v1.Workflow{Name: "looping", Steps: []*v1.Node{
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(1, 2), Iterator: "n",
			Body: []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("n")}}},
		}}},
	}}
	flat := &v1.Workflow{Name: "flat", Steps: []*v1.Node{
		{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
		{Id: "later", Kind: &v1.Node_Value{Value: v1.NewExpr("2")}},
	}}
	set := func(session *flowdebug.Session, breakpoints ...*v1.DebugBreakpoint) map[string]*v1.DebugBreakpointState {
		t.Helper()
		response, err := session.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{Breakpoints: breakpoints})
		require.NoError(t, err)
		states := map[string]*v1.DebugBreakpointState{}
		for _, state := range response.GetBreakpoints() {
			states[state.GetId()] = state
		}

		return states
	}
	armed := func(session *flowdebug.Session) []string {
		t.Helper()
		snapshot, err := session.Snapshot(t.Context())
		require.NoError(t, err)
		var ids []string
		for _, state := range snapshot.GetBreakpoints() {
			ids = append(ids, state.GetId())
		}

		return ids
	}

	var printed strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Emit: func(text string, _ flowdebug.Tone) { printed.WriteString(text) }})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	session.Program(looping)
	states := set(session,
		&v1.DebugBreakpoint{Id: "bound", Step: "body", Condition: "n > 1"},
		&v1.DebugBreakpoint{Id: "plain", Step: "body"},
		&v1.DebugBreakpoint{Id: "later", Step: "later"},
	)
	require.True(t, states["bound"].GetVerified(), states["bound"].GetMessage())
	require.False(t, states["later"].GetVerified(), "a step the first program lacks was armed")

	session.Program(flat)
	assert.Equal(t, []string{"plain"}, armed(session), "a breakpoint the new program cannot answer is still armed")
	assert.Contains(t, printed.String(), "breakpoint body if n > 1 no longer applies to this program: condition: `n` is not bound")
	snapshot, err := session.Snapshot(t.Context())
	require.NoError(t, err)
	var notices []string
	for _, observation := range snapshot.GetObservations() {
		notices = append(notices, observation.GetText())
	}
	assert.Contains(t, strings.Join(notices, "\n"), "breakpoint body if n > 1 no longer applies to this program")

	states = set(session, &v1.DebugBreakpoint{Id: "later", Step: "later"})
	assert.True(t, states["later"].GetVerified(), "a step of the second program was judged against the first: %s", states["later"].GetMessage())

	given, err := flowdebug.New(flowdebug.Options{Workflow: looping})
	require.NoError(t, err)
	t.Cleanup(func() { _ = given.Close() })
	given.Program(flat)
	states = set(given, &v1.DebugBreakpoint{Id: "later", Step: "later"})
	assert.False(t, states["later"].GetVerified(), "a session given its program took another")
}
