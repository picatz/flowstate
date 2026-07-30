package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// `RUNNING` is the same word for a workload three seconds in and one wedged on a
// step that will never return, and a run could say how long it had been going but
// not what it was doing. Those two situations want opposite responses from whoever
// is looking.
//
// The position is not derivable from outside: a listing and a Describe both know the
// run is RUNNING, and neither knows what it is running, because the position is in
// the interpreter's own call stack. So it is a Temporal query — live state, no
// history event, and no way for the asking to change the run.

// askDuring registers a query that lands while the run is still going, and returns
// where the answer will be written.
//
// A delayed callback rather than a query after ExecuteWorkflow returns, because a
// finished run answers from its final state and would prove nothing about a workload
// somebody is actually waiting on. The error is captured rather than asserted here so
// that a refused query fails the test that asked, with that test's own reason.
func askDuring(t *testing.T, env *testsuite.TestWorkflowEnvironment, after time.Duration) (*v1.RunProgress, *error) {
	t.Helper()

	got := &v1.RunProgress{}
	var asked bool
	var queryErr error

	env.RegisterDelayedCallback(func() {
		encoded, err := env.QueryWorkflow(engine.ProgressQuery)
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

// TestARunningRunSaysWhichStepItIsOn is the whole point, asked while a step is in
// flight.
//
// The query is sent from a delayed callback so that it lands *during* the run rather
// than after it: a query against a finished run would be answered from a final state
// and would prove nothing about a workload somebody is waiting on.
func TestARunningRunSaysWhichStepItIsOn(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	during, queryErr := askDuring(t, env, 30*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "one-step-at-a-time",
		Steps: []*v1.Node{
			logStep("first", "1"),
			sleepStep("pause", time.Hour),
			logStep("last", "2"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, *queryErr, "the run did not answer where it had got to")
	assert.Equal(t, "pause", during.GetStepId(),
		"a run waiting on a timer did not say which step it was waiting in")
	assert.Equal(t, int32(1), during.GetCompletedSteps(),
		"the run had finished one step and did not say so")
}

// TestProgressIsAnsweredWhileTheRunIsStillSettingUp is why the handler is registered
// before the vars activity rather than beside the executor.
//
// Temporal fails a query whose handler is not installed, and the first moments of a
// run are exactly when somebody asks what it is doing. A workflow with a `vars:`
// block spends those moments inside an activity, so registering after it would leave
// a window answering with an error that reads like a broken worker rather than like a
// run that has not got anywhere.
//
// The window is real but small, so this asks during it: the query lands while the run
// is between starting and reaching its first step.
func TestProgressIsAnsweredWhileTheRunIsStillSettingUp(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	atStart, queryErr := askDuring(t, env, time.Millisecond)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name:  "asked-immediately",
		Vars:  map[string]*v1.Value{"greeting": v1.NewLiteral("hi")},
		Steps: []*v1.Node{sleepStep("settle", time.Hour)},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, *queryErr,
		"a query in the first moments of a run was refused, which reads as a broken worker")
	assert.Zero(t, atStart.GetCompletedSteps(),
		"a run that had finished nothing reported finished steps")
}

// TestProgressInsideALoopNamesTheLoopAndTheBodyStep is the nesting case, and the
// reason the position is a path rather than a name.
//
// "the third item of the loop that is the second step" is where a workload actually
// spends its time, and a report that could only name `each` would be true and
// useless.
func TestProgressInsideALoopNamesTheLoopAndTheBodyStep(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	during, queryErr := askDuring(t, env, 30*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "inside-a-loop",
		Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteralList("only"),
				Iterator: "item",
				Body:     []*v1.Node{sleepStep("settle", time.Hour)},
			}},
		}},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, *queryErr, "the run did not answer where it had got to")
	assert.Equal(t, "each", during.GetStepId(),
		"the top-level position was lost once the run descended into a loop")
	assert.Equal(t, []string{"settle"}, during.GetPath(),
		"a run inside a loop body did not say which body step it was on")
}

// TestProgressStopsAtConcurrentWork is the negative direction, and it is the one that
// would be tempting to get wrong by reporting something.
//
// Several branches are current at once inside a parallel block, so no single one is
// *the* position — reporting whichever coroutine last wrote would be a lie that
// changes between two identical queries. The engine already refuses to suspend inside
// concurrent work for exactly this reason, and progress reports the outermost thing
// that is true and stops.
func TestProgressStopsAtConcurrentWork(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	during, queryErr := askDuring(t, env, 30*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "two-at-once",
		Steps: []*v1.Node{{
			Id: "branches",
			Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
				Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{sleepStep("left", time.Hour)}},
					{Steps: []*v1.Node{sleepStep("right", time.Hour)}},
				},
			}},
		}},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, *queryErr, "the run did not answer where it had got to")
	assert.Equal(t, "branches", during.GetStepId(),
		"a run inside a parallel block did not report the step that contains it")
	assert.Empty(t, during.GetPath(),
		"a parallel block reported one branch as though it were the run's position, "+
			"which two identical queries could disagree about")
}

// TestProgressKeepsTheStepThatContainsConcurrentWork is the case that showed the
// first version of this clearing too much.
//
// A parallel block nested in a sequential loop body has an unambiguous path down to
// it — every branch is inside `fanout`, whichever one is running — and an earlier
// version reset the whole path on entering concurrent work, so a query reported only
// `outer` and lost the step actually holding the work. That is the one part of the
// position somebody looking at a stuck run most needs.
//
// Nothing has to be cleared: the branch executors are given a nil progress, so no
// deeper entry can be written, and [progress.enter] truncates to the depth it is
// entering, so a stale one cannot survive either.
func TestProgressKeepsTheStepThatContainsConcurrentWork(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	during, queryErr := askDuring(t, env, 30*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "fanout-inside-a-loop",
		Steps: []*v1.Node{{
			Id: "outer",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteralList("only"),
				Iterator: "item",
				Body: []*v1.Node{{
					Id: "fanout",
					Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
						Branches: []*v1.Parallel_Branch{
							{Steps: []*v1.Node{sleepStep("left", time.Hour)}},
							{Steps: []*v1.Node{sleepStep("right", time.Hour)}},
						},
					}},
				}},
			}},
		}},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *queryErr, "the run did not answer where it had got to")

	assert.Equal(t, "outer", during.GetStepId())
	assert.Equal(t, []string{"fanout"}, during.GetPath(),
		"the step containing the concurrent work was dropped from the position, "+
			"leaving only the loop around it")
}
