package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The reachability half of #289, which no unit test can see.
//
// [progress.setLoopState]'s entry bound is exercised at the struct level in
// progress_internal_test.go by calling it in a loop, which proves the bound
// holds and says nothing about how many entries a *run* can put there. That
// second question is the one #289 is about, and it was answered wrongly:
// the issue's premise was that at most one loop is ever tracked at a time,
// on the reasoning that a sequential loop blocks everything after it and a
// `parallel:` branch records nothing. The reasoning missed [executor.runCall],
// which carries the run's progress into the callee — so a `loop:` whose body
// calls a workflow that itself loops has both loops tracked at once, and the
// shape is not exotic: it is exactly what the compiler's refusal of a
// directly-nested loop tells an author to write instead.
//
// These tests drive that path through the real durable driver and pin what is
// actually reachable: two entries for the simplest caller/callee pair, and
// [v1.MaxCallDepth] + 1 entries at the architectural ceiling, which is where
// the count stops. Written as internal tests so they can name
// [entityStateMaxLoopEntries] rather than restate its value — a test that
// hard-codes 64 is a second copy of the constant.

// newLoopStateEnv is [newWaitEnv] for this file's package: the engine's
// workflow and the activities a loop's body reaches, registered on a fresh
// test environment.
//
// A local copy rather than the one in wait_test.go because that one lives in
// `package engine_test`, which cannot hand anything to `package engine`.
func newLoopStateEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(Run)
	env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(Task)
	env.OnActivity(TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(TaskWithPrev)
	env.OnActivity(TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(TaskInScope)
	env.RegisterActivity(WorkflowVars)

	return env
}

// askStateDuring is progress_test.go's askDuring pointed at [StateQuery]: it
// registers a query that lands while the run is still going, and returns where
// the answer will be written.
//
// Asking during rather than after is the whole point here, exactly as it is
// for the position query. `loopState` is emptied by [progress.clearLoopState]
// as each loop finishes, so a query sent to a completed run answers with an
// empty map no matter how many loops the run had live at its deepest moment.
func askStateDuring(t *testing.T, env *testsuite.TestWorkflowEnvironment, after time.Duration) (*v1.EntityState, *error) {
	t.Helper()

	got := &v1.EntityState{}
	var asked bool
	var queryErr error

	env.RegisterDelayedCallback(func() {
		encoded, err := env.QueryWorkflow(StateQuery)
		if queryErr = err; err != nil {
			return
		}
		if queryErr = encoded.Get(got); queryErr != nil {
			return
		}
		asked = true
	}, after)

	t.Cleanup(func() {
		if queryErr == nil && !asked {
			t.Error("the query never ran, so this test asserted on an empty answer")
		}
	})

	return got, &queryErr
}

// askPositionDuring is [askStateDuring] for [ProgressQuery], so a test can say
// where the run was when it read the state — the two queries answer from the
// same [progress], and a claim about a loop being live is only readable beside
// the position that shows it.
func askPositionDuring(t *testing.T, env *testsuite.TestWorkflowEnvironment, after time.Duration) (*v1.RunProgress, *error) {
	t.Helper()

	got := &v1.RunProgress{}
	var asked bool
	var queryErr error

	env.RegisterDelayedCallback(func() {
		encoded, err := env.QueryWorkflow(ProgressQuery)
		if queryErr = err; err != nil {
			return
		}
		if queryErr = encoded.Get(got); queryErr != nil {
			return
		}
		asked = true
	}, after)

	t.Cleanup(func() {
		if queryErr == nil && !asked {
			t.Error("the query never ran, so this test asserted on an empty answer")
		}
	})

	return got, &queryErr
}

// parkStep is a step that parks the run for a duration, so a query has
// something to land in the middle of.
func parkStep(id string, d time.Duration) *v1.Node {
	return &v1.Node{
		Id:   id,
		Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(d)}}},
	}
}

// carryingLoop is a `loop:` that carries state — which is the only kind
// [progress.setLoopState] records at all, since it refuses a nil value — and
// stops after one iteration.
//
// The carried value is `initial` and nothing else: `until:` holds immediately,
// so the value a query sees for this loop is the one passed here, which is what
// lets a test tell two loops' entries apart by their contents.
func carryingLoop(id, name string, initial int64, body ...*v1.Node) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Loop{Loop: &v1.Loop{
			State:         name,
			Initial:       v1.NewLiteral(initial),
			Update:        v1.NewExpr(name + " + 1"),
			Until:         v1.NewExpr("true"),
			MaxIterations: 2,
			Body:          body,
		}},
	}
}

// callTo wraps a callee workflow in a step that calls it.
func callTo(id string, callee *v1.Workflow) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}
}

// nestedLoopCalls builds `levels` workflows, each one a single `loop:` whose
// body calls the next, with the innermost loop's body parking on `park`.
//
// The returned workflow is the outermost. Every loop carries state, so every
// level puts one entry in `loopState`, and all of them are live at once while
// the innermost is parked: a call does not return until its callee finishes,
// so the caller's loop is still on the iteration that made the call.
//
// `levels` loops means `levels - 1` calls, which is what makes
// [v1.MaxCallDepth] + 1 the largest value that runs at all.
func nestedLoopCalls(levels int, park *v1.Node) *v1.Workflow {
	var wf *v1.Workflow

	for i := levels - 1; i >= 0; i-- {
		body := []*v1.Node{park}
		if wf != nil {
			body = []*v1.Node{callTo(fmt.Sprintf("call-%d", i), wf)}
		}

		wf = &v1.Workflow{
			Name:    fmt.Sprintf("level-%d", i),
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				carryingLoop(fmt.Sprintf("loop-%d", i), fmt.Sprintf("n%d", i), int64(i), body...),
			},
		}
	}

	return wf
}

// TestLoopStateTracksACallerAndItsCalleeAtOnce is #289's premise, disproved
// with the smallest workflow that does it.
//
// A `loop:` whose body calls a workflow that itself loops has two loops live at
// the moment the callee parks: the caller's iteration is blocked inside the
// call, and the callee's iteration is blocked inside the wait. [executor.runCall]
// carries `progress: e.progress` into the callee's executor (execute.go, the
// nested executor's field list), unlike [executor.runForEach]'s concurrent
// workers and [executor.runParallel]'s branches, which pass `progress: nil` —
// so both loops write their carried state into the same map, under their own
// step ids.
func TestLoopStateTracksACallerAndItsCalleeAtOnce(t *testing.T) {
	t.Parallel()

	env := newLoopStateEnv(t)
	during, queryErr := askStateDuring(t, env, 30*time.Second)

	callee := &v1.Workflow{
		Name:    "callee-that-loops",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{carryingLoop("inner", "i", 2, parkStep("settle", time.Hour))},
	}
	caller := &v1.Workflow{
		Name:    "caller-that-loops",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{carryingLoop("outer", "o", 1, callTo("invoke", callee))},
	}

	env.ExecuteWorkflow(Run, &v1.RunState{Workflow: caller})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *queryErr, "the run did not answer what it was carrying")

	assert.False(t, during.GetTruncated(),
		"two concurrently tracked loops tripped a bound sized for sixty-four")
	require.Len(t, during.GetLoopState(), 2,
		"a loop calling a workflow that loops tracked %d loops at once, and #289's "+
			"premise was that no live path tracks more than one",
		len(during.GetLoopState()))

	assert.Equal(t, int64(1), during.GetLoopState()["outer"].GetLiteral().GetInt64Value(),
		"the caller's loop was not carrying what it started with")
	assert.Equal(t, int64(2), during.GetLoopState()["inner"].GetLiteral().GetInt64Value(),
		"the callee's loop was not carrying what it started with")
}

// TestLoopStateReachesTheCallDepthCeiling asserts the bound is approached as
// designed, which is the habit the count bound's own unit test already keeps
// ([TestSetLoopStateRefusesPastTheEntryBound]) and the one #289 asks for here:
// showing two entries and stopping proves the premise wrong without measuring
// how wrong.
//
// [v1.MaxCallDepth] calls stacked on the outermost loop is the deepest a run
// goes, so [v1.MaxCallDepth] + 1 concurrently tracked loops is the ceiling —
// far below [entityStateMaxLoopEntries], which is the fact the constant's
// comment now records and #289 has to decide about.
func TestLoopStateReachesTheCallDepthCeiling(t *testing.T) {
	t.Parallel()

	const levels = v1.MaxCallDepth + 1

	env := newLoopStateEnv(t)
	during, queryErr := askStateDuring(t, env, 30*time.Second)

	env.ExecuteWorkflow(Run, &v1.RunState{
		Workflow: nestedLoopCalls(levels, parkStep("settle", time.Hour)),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"a run nesting calls to exactly MaxCallDepth was refused")
	require.NoError(t, *queryErr, "the run did not answer what it was carrying")

	got := during.GetLoopState()

	assert.False(t, during.GetTruncated(),
		"the deepest run this engine can execute tripped the entry bound")
	assert.GreaterOrEqual(t, len(got), 2,
		"the reachable maximum is not even two, which would make #289's premise right")
	assert.Equal(t, levels, len(got),
		"every nested level's loop is live while the innermost parks, so all %d "+
			"should be tracked at once", levels)
	assert.Less(t, len(got), entityStateMaxLoopEntries,
		"the architectural ceiling met the entry bound, which would make the bound "+
			"reachable and this file's premise wrong")

	for i := 0; i < levels; i++ {
		id := fmt.Sprintf("loop-%d", i)
		assert.Equal(t, int64(i), got[id].GetLiteral().GetInt64Value(),
			"level %d's loop was not tracked under its own step id, or was carrying "+
				"another level's value", i)
	}
}

// TestNestingPastTheCallDepthCeilingIsRefused is the other half of "reached and
// stops there": one level more than the test above is not a deeper answer, it is
// a failed run.
//
// Without this, [TestLoopStateReachesTheCallDepthCeiling] asserts a number
// without showing it is a ceiling — a reader could reasonably wonder whether
// nine was simply the number that test happened to ask for.
func TestNestingPastTheCallDepthCeilingIsRefused(t *testing.T) {
	t.Parallel()

	env := newLoopStateEnv(t)

	env.ExecuteWorkflow(Run, &v1.RunState{
		Workflow: nestedLoopCalls(v1.MaxCallDepth+2, parkStep("settle", time.Hour)),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(),
		"a run nesting calls one past MaxCallDepth was allowed to run")
}

// TestLoopStateKeysCollideAcrossACallBoundary is the question #289's staging
// comment flagged and left open, answered: **yes, they collide.**
//
// [progress.setLoopState] is keyed by `node.GetId()` alone, and a callee's step
// ids live in the callee's own namespace — nothing requires them to differ from
// the caller's, and `flow validate` does not (a callee is compiled on its own).
// So a caller whose loop step is named `loop`, calling a workflow whose loop
// step is also named `loop`, has two live loops and one map key.
//
// Two observable consequences, both asserted below, because they are different
// wrong answers:
//
//  1. While both are live the map holds one entry, carrying the *callee's*
//     value. A reader of [StateQuery] sees one loop where there are two, and
//     sees the caller's carried state replaced by the callee's rather than
//     missing — the shape that reads as an answer instead of as a gap.
//  2. When the callee's loop finishes, [progress.clearLoopState] deletes that
//     one key, so the caller's loop — still live, still carrying — vanishes
//     from the query entirely.
//
// This is a wrong answer in a read-only query rather than anything the run
// itself acts on: `loopState` feeds [StateQuery] and nothing else, and each
// loop's real carried state travels in its own frame ([executor.setLoopStateFrame],
// keyed by depth), so execution and resumption are unaffected. It is recorded
// here rather than fixed: keying by something other than the step id is a
// schema-visible change to what [v1.EntityState.LoopState]'s keys mean, and
// #289 is the open decision about this map.
func TestLoopStateKeysCollideAcrossACallBoundary(t *testing.T) {
	t.Parallel()

	env := newLoopStateEnv(t)

	// While the callee's loop is parked: both loops live, one key.
	nested, nestedErr := askStateDuring(t, env, 30*time.Second)
	// After the callee's loop has finished and the caller's has not: the
	// caller's loop is parked on the step after the call, still carrying.
	after, afterErr := askStateDuring(t, env, time.Hour+30*time.Second)
	// Asked at the same moment, so the emptiness above is read as "the entry
	// was deleted out from under a live loop" rather than as "the loop had
	// finished, and clearing it was right".
	where, whereErr := askPositionDuring(t, env, time.Hour+30*time.Second)

	callee := &v1.Workflow{
		Name:    "callee-naming-its-loop-loop",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{carryingLoop("loop", "i", 2, parkStep("settle", time.Hour))},
	}
	caller := &v1.Workflow{
		Name:    "caller-naming-its-loop-loop",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{carryingLoop("loop", "o", 1,
			callTo("invoke", callee),
			parkStep("after", 2*time.Hour),
		)},
	}

	env.ExecuteWorkflow(Run, &v1.RunState{Workflow: caller})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *nestedErr, "the run did not answer what it was carrying")
	require.NoError(t, *afterErr, "the run did not answer what it was carrying")

	require.Len(t, nested.GetLoopState(), 1,
		"two loops sharing a step id across a call boundary were tracked separately, "+
			"which would mean the key encoding distinguishes them after all")
	assert.Equal(t, int64(2), nested.GetLoopState()["loop"].GetLiteral().GetInt64Value(),
		"the surviving entry was not the callee's, so the collision resolves the "+
			"other way and this comment describes the wrong one")
	assert.False(t, nested.GetTruncated(),
		"a collision is a silently dropped loop, not a reported truncation — "+
			"[progress.loopStateTruncated] only fires on the count bound")

	require.NoError(t, *whereErr, "the run did not answer where it had got to")
	require.Equal(t, "loop", where.GetStepId(),
		"the second query did not land inside the caller's loop, so the state it "+
			"read says nothing about a live loop")
	require.Equal(t, []string{"after"}, where.GetPath(),
		"the second query did not land after the call returned")

	assert.Empty(t, after.GetLoopState(),
		"the caller's loop survived its callee's clearLoopState of the shared key, "+
			"which would mean the collision is harmless on the way out")
}
