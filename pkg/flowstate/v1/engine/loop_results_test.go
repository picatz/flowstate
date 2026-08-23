package engine_test

import (
	"errors"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// loopSpanningCAN builds a workflow whose only step is a loop that counts from
// 0 to 5 — six iterations — optionally with a declared output that reads the
// loop's own `results`. That one difference is the entire distinction between
// the "read" and "unread" halves of TestRunWorkflowLoopResultsAcrossCAN below;
// everything else about the two workflows is identical.
func loopSpanningCAN(read bool) *v1.Workflow {
	wf := &v1.Workflow{
		Name:    "loop-can-suppression",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "loop",
				Kind: &v1.Node_Loop{Loop: &v1.Loop{
					State:         "n",
					Initial:       v1.NewLiteral(int64(0)),
					Update:        v1.NewExpr("n + 1"),
					Until:         v1.NewExpr("n >= 5"),
					MaxIterations: 100,
					Body: []*v1.Node{{
						Id: "tick",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("tick")},
						}},
					}},
				}},
			},
		},
	}
	if read {
		wf.DeclaredOutputs = []*v1.OutputDeclaration{
			{Name: "count", Value: v1.NewExpr("size(steps.loop.results)")},
		}
	}
	return wf
}

// TestRunWorkflowLoopResultsAcrossCAN is #229's central proof: a loop nothing
// downstream reads must not carry its accumulated `results` across every
// segment of a Continue-As-New, while a loop something reads must carry them
// in full — both driven through a real, multi-segment durable run rather than
// a fabricated RunState.
//
// It also pins the honest-contract follow-up: what env.GetWorkflowResult
// returns here is exactly what a completed run's `Get` answer, `flow get`
// output, and `flowstate_get` MCP answer all carry verbatim (see
// FlowstateServer.Get's COMPLETED branch and cmd/flow/mcp.go's
// runLocalResult) — so this is the caller-visible surface, not an internal
// implementation detail. An unread loop's `results` must therefore come back
// *absent*, never a plausible-looking partial list.
//
// The test environment does not continue a workflow as new for real: it
// reports the [workflow.ContinueAsNewError], and the next segment has to be
// started by hand, feeding back the state it carried — see
// TestPendingCompensationsSurviveContinueAsNew, the same technique. StepsBudget:
// 1 forces a Continue-As-New after essentially every processed node, so the
// six-iteration loop below is guaranteed to span more than one segment. That is
// exactly the shape this test needs: suppression ([v1.LoopResumeResults]) only
// has anything to decide at a resume boundary, and a loop that finishes within
// its first segment — every existing example and shared case — never reaches
// that code at all, which is why none of them needed to change for #229's fix
// (see TestRunWorkflowLoop's unchanged expectations).
func TestRunWorkflowLoopResultsAcrossCAN(t *testing.T) {
	for _, tc := range []struct {
		name string
		read bool
	}{
		{name: "unread loop drops history it does not need across a Continue-As-New", read: false},
		{name: "read loop keeps its full history across a Continue-As-New", read: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &v1.RunState{Workflow: loopSpanningCAN(tc.read), StepsBudget: 1}

			var (
				out      v1.Workflow_StepOutputs
				got      bool
				segments int
			)
			for segments = 0; segments < 20; segments++ {
				testSuite := &testsuite.WorkflowTestSuite{}
				env := testSuite.NewTestWorkflowEnvironment()
				env.RegisterWorkflow(engine.Run)
				env.OnActivity(engine.TaskV2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskV2)
				env.OnActivity(engine.TaskInScopeV2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScopeV2)
				env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

				env.ExecuteWorkflow(engine.Run, state)
				require.True(t, env.IsWorkflowCompleted())

				err := env.GetWorkflowError()
				var continued *workflow.ContinueAsNewError
				if errors.As(err, &continued) {
					next := &v1.RunState{}
					require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
					state = next
					continue
				}

				require.NoError(t, err)
				require.NoError(t, env.GetWorkflowResult(&out))
				got = true
				break
			}

			require.True(t, got, "the run never completed within %d segments", segments)
			require.Greater(t, segments, 0,
				"the run completed within its first segment, so this proves nothing "+
					"the single-segment shared cases did not already cover")

			// The carried state (n) must survive every Continue-As-New intact
			// regardless of whether results are suppressed — the two travel in
			// different frame fields ([v1.Frame.LoopState] versus
			// [v1.Frame.Results]) and suppressing one must never touch the other.
			n := out.GetStepValues()["loop"].GetNamedValues()["state"].GetLiteral().GetInt64Value()
			require.EqualValues(t, 5, n,
				"carried state must survive Continue-As-New regardless of results suppression")

			loopOutputs := out.GetStepValues()["loop"]
			if tc.read {
				results := loopOutputs.GetNamedValues()["results"].GetLiteral().GetListValue().GetValues()
				require.Len(t, results, 6,
					"a read loop's results must survive every Continue-As-New segment in full")
			} else {
				// The honest contract (#229's P1 follow-up): a loop that dropped
				// earlier segments' history must not report the finishing
				// segment's own iterations as though they were the whole run —
				// that would be a partial list on a surface (Get, `flow get`,
				// `flowstate_get`) that gives a reader no way to tell it apart
				// from a short but complete one. The key must be absent
				// entirely, not merely short.
				_, present := loopOutputs.GetNamedValues()["results"]
				require.False(t, present,
					"an unread loop that dropped history across a Continue-As-New must omit `results` "+
						"entirely rather than publish a partial list nothing marks as partial")
			}
		})
	}
}
