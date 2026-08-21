package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The reachability half of #289, driven through both the durable driver and
// the submission path — because the two together are what decide how many
// loops can actually be tracked at once, and neither alone can see it.
//
// #289's observation is that [entityStateMaxLoopEntries] (64) counts
// *concurrently active* loops and that no spec an author can submit produces
// more than one, so the bound cannot fire. That observation is correct, and it
// rests on facts this file pins:
//
//   - An accepted single loop records exactly one entry while it is live — the
//     reachable maximum ([TestAnAcceptedLoopRecordsExactlyOneLoopStateEntry]).
//   - The only shape that would place a *second* loop concurrently live is a
//     loop whose body reaches another loop — and every spelling of that, direct
//     or through a `call:`, is refused by the submission path before it can run
//     ([TestTheSubmissionPathRefusesEveryShapeThatWouldStackLoopState]).
//   - Concurrent constructs, the other way two loops could appear at once,
//     record no loop state at all, because their branch executors carry
//     `progress: nil` ([TestConcurrentLoopsRecordNoLoopState]).
//
// # A correction this file used to get wrong
//
// An earlier revision asserted the opposite: that a `loop:` whose body `call:`s
// a workflow that itself loops stacks up to [v1.MaxCallDepth] + 1 concurrent
// entries, run through [Run] directly. That measured the engine's data
// structure, not anything reachable. Running [Run] on a hand-built
// [v1.RunState] bypasses `FlowstateServer.validateSpecification`, and the shape
// is *unsubmittable*: [v1.CheckLoopNesting] on the RPC path and
// `bodyHasNestedLoop` in the flowfile compiler both refuse a loop beneath a
// loop *through a call* (#727/#680) — the engine does not suspend inside a loop
// body, so an inner loop would run atomically inside each outer iteration with
// no Continue-As-New between them, a shape nothing exercises and that wedges
// rather than fails. The nine-entry claim was verified against a tree from
// before that refusal existed and did not survive contact with the real
// submission path.

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
// empty map no matter what the run held at its deepest moment.
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

// parkStep parks the run for a duration, so a query has something to land in
// the middle of.
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
// so the value a query sees for this loop is the one passed here.
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

// TestAnAcceptedLoopRecordsExactlyOneLoopStateEntry pins the reachable maximum:
// one.
//
// A single top-level carrying loop, parked inside its body, is the deepest an
// author can push `loopState` —
// [TestTheSubmissionPathRefusesEveryShapeThatWouldStackLoopState] below is why
// nothing accepted stacks a second entry beside it. The assertion is `== 1`,
// not `>= 1`: the bound of [entityStateMaxLoopEntries] is sixty-four above a
// maximum of one, which is the fact #289 records and asks a decision about.
func TestAnAcceptedLoopRecordsExactlyOneLoopStateEntry(t *testing.T) {
	t.Parallel()

	env := newLoopStateEnv(t)
	during, queryErr := askStateDuring(t, env, 30*time.Second)

	wf := &v1.Workflow{
		Name:    "one-loop",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{carryingLoop("outer", "o", 7, parkStep("settle", time.Hour))},
	}

	// The same guard the server applies before a run starts, asserted here so
	// this test's own workflow is one an author could actually submit — a test
	// that drives [Run] on a spec `validateSpecification` would reject is
	// exactly the mistake this file is correcting.
	require.NoError(t, v1.CheckLoopNesting(wf),
		"this test's own workflow would be refused at submission")

	env.ExecuteWorkflow(Run, &v1.RunState{Workflow: wf})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *queryErr, "the run did not answer what it was carrying")

	assert.False(t, during.GetTruncated(), "one loop tripped a bound sized for sixty-four")
	require.Len(t, during.GetLoopState(), 1,
		"a single accepted loop recorded %d entries", len(during.GetLoopState()))
	assert.Equal(t, int64(7), during.GetLoopState()["outer"].GetLiteral().GetInt64Value(),
		"the loop was not carrying what it started with")
}

// TestTheSubmissionPathRefusesEveryShapeThatWouldStackLoopState is the guard
// that keeps the count at one, and the direct answer to #289's premise: the
// only way a second loop is live while a first still is, is a loop reached from
// inside a loop's body — and every route to that is refused before it runs.
//
// [v1.CheckLoopNesting] is the RPC-boundary half of the refusal (the flowfile
// compiler's `bodyHasNestedLoop` is the other, for files that come through the
// parser); it descends through `call:`, `switch:`, `for_each:` and `parallel:`
// carrying the enclosing loop across each, so no construct launders a nested
// loop. These are the shapes an author might reach for to stack `loopState`,
// and each is refused:
//
//   - the shape #289's staging comment believed was accepted — a loop whose
//     body calls a workflow that loops;
//   - the same through a chain of calls with no intermediate loop;
//   - the same with a `switch:` between the loop and the call, the transparency
//     the staging comment leaned on;
//   - the same with both loop steps sharing an id, which is the collision the
//     PR flagged: it is refused here, so it is latent (see the constant's
//     comment) rather than a live wrong answer.
func TestTheSubmissionPathRefusesEveryShapeThatWouldStackLoopState(t *testing.T) {
	t.Parallel()

	looper := func(name, stepID string) *v1.Workflow {
		return &v1.Workflow{
			Name:    name,
			Profile: v1.CurrentProfile,
			Steps:   []*v1.Node{carryingLoop(stepID, "i", 1, parkStep("settle", time.Hour))},
		}
	}
	loopOver := func(name, loopID string, body ...*v1.Node) *v1.Workflow {
		return &v1.Workflow{
			Name:    name,
			Profile: v1.CurrentProfile,
			Steps:   []*v1.Node{carryingLoop(loopID, "o", 0, body...)},
		}
	}

	// loop -> call -> loop
	loopCallLoop := loopOver("loop-call-loop", "outer", callTo("invoke", looper("callee", "inner")))

	// loop -> call -> call -> loop (no intermediate loop)
	mid := &v1.Workflow{Name: "mid", Profile: v1.CurrentProfile,
		Steps: []*v1.Node{callTo("c2", looper("grandchild", "inner"))}}
	loopCallCallLoop := loopOver("loop-call-call-loop", "outer", callTo("c1", mid))

	// loop -> switch -> call -> loop
	sw := &v1.Node{Id: "sw", Kind: &v1.Node_Switch{Switch: &v1.Switch{
		Value: v1.NewLiteral("k"),
		Cases: []*v1.Switch_Case{{
			Values: []*v1.Value{v1.NewLiteral("k")},
			Steps:  []*v1.Node{callTo("invoke", looper("callee", "inner"))},
		}},
	}}}
	loopSwitchCallLoop := loopOver("loop-switch-call-loop", "outer", sw)

	// loop(id=loop) -> call -> loop(id=loop): the flagged key collision.
	collision := loopOver("same-id-collision", "loop",
		callTo("invoke", looper("callee", "loop")))

	for _, tc := range []struct {
		name string
		wf   *v1.Workflow
	}{
		{"loop calls a workflow that loops", loopCallLoop},
		{"loop calls through a chain into a loop", loopCallCallLoop},
		{"loop reaches a looping call through a switch", loopSwitchCallLoop},
		{"loop and callee loop share an id", collision},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := v1.CheckLoopNesting(tc.wf)
			require.Error(t, err,
				"the submission path accepted a loop-beneath-a-loop shape, so a "+
					"second loop_state entry is reachable after all and #289's "+
					"premise is wrong")
			assert.Contains(t, err.Error(), "loop",
				"the refusal did not name the nested loop it caught")
		})
	}
}

// TestConcurrentLoopsRecordNoLoopState is the other reason nothing accepted
// stacks entries: the constructs that *do* run more than one loop at once run
// each in an executor built with `progress: nil` ([executor.runParallel]'s
// branch worker), so a `loop:` inside a branch calls [progress.setLoopState] on
// a nil progress and records nothing.
//
// Two loops genuinely live at once here — one per branch, both parked — and the
// state query still answers empty. This is the shape #289's body named as the
// reason the bound was unreachable; it is a real reason, just not the whole of
// it (the call path is the other half, closed by the test above rather than by
// nil progress).
func TestConcurrentLoopsRecordNoLoopState(t *testing.T) {
	t.Parallel()

	env := newLoopStateEnv(t)
	during, queryErr := askStateDuring(t, env, 30*time.Second)

	wf := &v1.Workflow{
		Name:    "two-loops-at-once",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "branches",
			Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
				Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{carryingLoop("left", "a", 1, parkStep("l", time.Hour))}},
					{Steps: []*v1.Node{carryingLoop("right", "b", 2, parkStep("r", time.Hour))}},
				},
			}},
		}},
	}

	require.NoError(t, v1.CheckLoopNesting(wf),
		"two loops in separate parallel branches is an accepted shape")

	env.ExecuteWorkflow(Run, &v1.RunState{Workflow: wf})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *queryErr, "the run did not answer what it was carrying")

	assert.False(t, during.GetTruncated())
	assert.Empty(t, during.GetLoopState(),
		"a loop inside a parallel branch recorded state, but a branch executor "+
			"carries nil progress precisely so it cannot")
}
