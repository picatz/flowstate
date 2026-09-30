package engine_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheDurableDriverSteppedThroughTheCorpusStopsWhereItSays is the durable
// half of [conformance.SteppedCase]: a session that steps in at every stop is
// held at exactly the addresses the corpus lists, which are the ones the local
// driver stops at, from the first boundary to the end of the run.
func TestTheDurableDriverSteppedThroughTheCorpusStopsWhereItSays(t *testing.T) {
	t.Parallel()

	cases := conformance.SteppedCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			spec := proto.CloneOf(test.Workflow)
			spec.Debug = debugSpec(spec.GetName()).GetDebug()

			stops := stepThroughDurably(t, spec, len(test.Stops))
			assert.Equal(t, test.Stops, stops)
		})
	}
}

// durableMove is one command a session sends while the run is held.
type durableMove struct {
	action v1.DebugResumeAction
	until  string
}

func stepIn() durableMove {
	return durableMove{action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN}
}
func stepOver() durableMove {
	return durableMove{action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER}
}
func stepOut() durableMove {
	return durableMove{action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT}
}
func proceed() durableMove {
	return durableMove{action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE}
}
func runUntil(target string) durableMove {
	return durableMove{action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, until: target}
}

// stepThroughDurably attaches a session to spec and steps in n times, reading
// the address the run is held at before each step, and returns those
// addresses. The run must end after the last step.
func stepThroughDurably(t *testing.T, spec *v1.Workflow, n int) []string {
	t.Helper()

	moves := make([]durableMove, n)
	for k := range moves {
		moves[k] = stepIn()
	}

	return holdsBefore(t, spec, moves)
}

// holdsBefore attaches a session to spec and sends moves in order, reading the
// address the run is held at before each, and returns those addresses. The
// last move must let the run end.
func holdsBefore(t *testing.T, spec *v1.Workflow, moves []durableMove) []string {
	t.Helper()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	for k, move := range moves {
		at := time.Duration(k+1) * 10 * time.Second
		tl.read(at, fmt.Sprintf("stop-%d", k), "")
		tl.ask(at+time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: fmt.Sprintf("move-%d", k),
			Action: move.action, Until: move.until})
	}

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted(), "the run did not end after the last move")
	require.NoError(t, tl.env.GetWorkflowError())

	stops := make([]string, 0, len(moves))
	for k := range moves {
		snapshot := tl.reads[fmt.Sprintf("stop-%d", k)]
		require.NotNil(t, snapshot, "stop %d was not read", k)
		require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, snapshot.GetState(), "the run was not held at stop %d", k)
		stops = append(stops, snapshot.GetOccurrence().GetAddress())
	}

	return stops
}

// bodySpec is `first`, a sequential `for_each:` over two items whose body is
// `touch`, and `last`, declaring `debug:`.
func bodySpec(name string) *v1.Workflow {
	spec := &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			logStep("first", "one"),
			{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewExpr(`["a", "b"]`), MaxParallel: 1, Body: []*v1.Node{logStep("touch", "visited")},
			}}},
			logStep("last", "two"),
		},
	}
	spec.Debug = debugSpec(name).GetDebug()

	return spec
}

// `next` at a loop runs it whole and `finish` inside its body leaves it, as
// they do at a call: the nesting they measure counts the iterations, branches
// and arms a step is in as well as the calls.
func TestNextRunsALoopWholeAndFinishLeavesIt(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"first", "each", "last"},
		holdsBefore(t, bodySpec("next-over"), []durableMove{stepIn(), stepOver(), proceed()}),
		"`next` at the loop stopped inside its body")
	assert.Equal(t, []string{"first", "each", "each[0]/touch", "last"},
		holdsBefore(t, bodySpec("finish-out"), []durableMove{stepIn(), stepIn(), stepOut(), proceed()}),
		"`finish` inside the body did not leave the loop")
}

// `until` names one iteration of a body, as it does on the local driver.
func TestUntilNamesAnIterationOfALoopBody(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"first", "each[1]/touch"},
		holdsBefore(t, bodySpec("until-iteration"), []durableMove{runUntil("each[1]/touch"), proceed()}))
}

// A breakpoint on a step in a body is armed and hit at every iteration, with
// the body's address, and stops the run nowhere else.
func TestABreakpointInALoopBodyIsHitOnEveryIteration(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.ask(5*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbBreakpoints, Session: "s1", Request: "bp",
		Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{{Id: "body", Step: "each/touch"}}}})
	tl.read(6*time.Second, "set", "bp")
	tl.ask(7*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "go-1", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	tl.read(20*time.Second, "first", "")
	tl.ask(21*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "go-2", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	tl.read(30*time.Second, "second", "")
	tl.ask(31*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "go-3", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: bodySpec("break-in-body")})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())

	require.Len(t, tl.reads["set"].GetBreakpoints(), 1)
	armed := tl.reads["set"].GetBreakpoints()[0]
	assert.True(t, armed.GetVerified(), "a breakpoint in a sequential body was refused: %s", armed.GetMessage())
	assert.NotEmpty(t, armed.GetSites())
	for name, want := range map[string]string{"first": "each[0]/touch", "second": "each[1]/touch"} {
		stop := tl.reads[name]
		assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, stop.GetReason(), name)
		assert.Equal(t, want, stop.GetOccurrence().GetAddress(), name)
	}
}

// Where a run is in several places at once it holds in none of them: stepping
// through a `parallel:` block and a `for_each:` running iterations together
// stops at the containers and never inside them.
func TestSteppingNeverStopsWhereARunIsInSeveralPlaces(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name:    "several-places",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
				{Steps: []*v1.Node{logStep("left", "l")}},
				{Steps: []*v1.Node{logStep("right", "r")}},
			}}}},
			{Id: "wide", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewExpr(`["x", "y"]`), MaxParallel: 2, Body: []*v1.Node{logStep("touch", "t")},
			}}},
			logStep("last", "end"),
		},
	}
	spec.Debug = debugSpec(spec.GetName()).GetDebug()

	assert.Equal(t, []string{"fan", "wide", "last"}, stepThroughDurably(t, spec, 3))
}

// TestSteppingCrossesAContinueAsNewInsideALoop: a run that continues as new
// between iterations of a loop, with a session stepping through it, is held at
// the same addresses the run makes in one segment. The seam falls between
// iterations, never inside a body, so no hold spans it, and the session's
// stepping mode rides the carry into the next segment.
func TestSteppingCrossesAContinueAsNewInsideALoop(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name:    "stepped-across-a-seam",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:       v1.NewExpr(`["a", "b", "c"]`),
				MaxParallel: 1,
				Body:        []*v1.Node{logStep("touch", "visited")},
			}},
		}},
	}
	spec.Debug = debugSpec(spec.GetName()).GetDebug()

	want := []string{"each", "each[0]/touch", "each[1]/touch", "each[2]/touch"}

	var (
		stops    []string
		segments int
	)
	state := &v1.RunState{Workflow: spec, StepsBudget: 1}
	for {
		segments++
		require.LessOrEqual(t, segments, 8, "the run kept continuing as new")

		tl := newTimeline(t)
		const sre = "sre-1@example.com"
		if segments == 1 {
			tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
		}
		for k := range len(want) {
			at := time.Duration(k+1) * 10 * time.Second
			tl.read(at, fmt.Sprintf("stop-%d", k), "")
			tl.ask(at+time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1",
				Request: fmt.Sprintf("step-%d-%d", segments, k), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN})
		}
		tl.env.ExecuteWorkflow(engine.Run, state)
		require.True(t, tl.env.IsWorkflowCompleted())

		for k := range len(want) {
			if snapshot := tl.reads[fmt.Sprintf("stop-%d", k)]; snapshot != nil && snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
				stops = append(stops, snapshot.GetOccurrence().GetAddress())
			}
		}

		var continued *workflow.ContinueAsNewError
		if !errors.As(tl.env.GetWorkflowError(), &continued) {
			require.NoError(t, tl.env.GetWorkflowError())

			break
		}
		next := &v1.RunState{}
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, next))
		next.StepsBudget = 1
		state = next
	}

	assert.Greater(t, segments, 1, "the run never continued as new, so this proves nothing about a seam")
	assert.Equal(t, want, stops)
}

// A hold inside a body while an `async:` step started above it is still
// outstanding changes nothing the run computes. The body's scope cannot join
// its enclosing scope's async work, so the step keeps running behind the
// hold, and is joined where it always was.
func TestAHoldInABodyLeavesAnOutstandingAsyncStepAlone(t *testing.T) {
	t.Parallel()

	spec := bodySpec("async-behind-a-hold")
	spec.Steps = append([]*v1.Node{{
		Id: "background", Async: true,
		Kind: logStep("background", "ticking").GetKind(),
	}}, spec.Steps...)

	plain := newWaitEnv(t)
	plain.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: proto.CloneOf(spec)})
	require.True(t, plain.IsWorkflowCompleted())
	require.NoError(t, plain.GetWorkflowError())
	var want v1.Workflow_StepOutputs
	require.NoError(t, plain.GetWorkflowResult(&want))

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	for k, move := range []durableMove{stepIn(), stepIn(), stepIn(), stepIn(), stepIn(), proceed()} {
		at := time.Duration(k+1) * 10 * time.Second
		tl.read(at, fmt.Sprintf("stop-%d", k), "")
		tl.ask(at+time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: fmt.Sprintf("move-%d", k),
			Action: move.action})
	}
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	var got v1.Workflow_StepOutputs
	require.NoError(t, tl.env.GetWorkflowResult(&got))

	assert.Empty(t, cmp.Diff(&want, &got, protocmp.Transform()),
		"a hold inside the body changed what the run computed")
	assert.Equal(t, "each[1]/touch", tl.reads["stop-4"].GetOccurrence().GetAddress(),
		"the run was not held inside the body with the async step outstanding, so this proves nothing")
}
