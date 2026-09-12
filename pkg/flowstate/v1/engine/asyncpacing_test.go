package engine_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A sequential top-level `for_each` is exempt from [v1.MaxAtomicBlockActivities]
// because it is paced: every body step counts against the run's step budget, and
// the loop offers a Continue-As-New seam at every iteration boundary, where the
// engine also consults Temporal's own history-pressure hint. `atomicblock.go`
// says so, and sizes the exemption against the alternative — 1,000 items over a
// 60-step body is roughly 180,000 history events against the 51,200-event cap at
// which Temporal force-terminates the run, taking the compensation log with it.
//
// An `async:` step earlier in the same scope stays in the outstanding set until
// a later node references it or the scope ends, which is the whole of the loop.
// A suspension rule that refused to suspend while anything was outstanding would
// therefore switch that pacing off for exactly the workloads the exemption was
// written for, and nothing else would notice: the step budget has no other
// consumer, so exceeding it does not fail the run.
//
// So the seam has to survive an unjoined `async:` step.

// TestALoopStillPacesWhileAnAsyncStepIsOutstanding is that claim.
//
// The async step's failure is tolerated, so it stays outstanding without ending
// the run, and the budget is spent before the loop is half done — so a segment
// that reaches the end of the list is one that never offered a seam.
func TestALoopStillPacesWhileAnAsyncStepIsOutstanding(t *testing.T) {
	t.Parallel()

	const items = 8

	list := make([]any, items)
	for i := range list {
		list[i] = fmt.Sprintf("item-%d", i)
	}

	env := newWaitEnv(t)
	env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

	carried := carriedState(t, env, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "async-then-loop",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					// Outstanding for the whole loop: nothing after it mentions
					// it, so it is joined at the scope's end. Tolerated, so the
					// run's outcome does not depend on it.
					Id:     "notify",
					Async:  true,
					Policy: &v1.StepPolicy{ContinueOnError: true, Retry: &v1.RetryPolicy{MaxAttempts: 1}},
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url":    v1.NewLiteral("http://127.0.0.1:1/"),
							"method": v1.NewLiteral("GET"),
						},
					}},
				},
				{
					Id: "process",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items:    v1.NewLiteralList(list...),
						Iterator: "item",
						Body: []*v1.Node{
							{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("item")}},
						},
					}},
				},
			},
		},
		// Spent within the first iterations, so the loop's own boundary is the
		// one that has to answer.
		StepsBudget: 2,
	})

	require.NotEmpty(t, carried.GetFrames(),
		"the loop ran to the end of its items in one segment: the Continue-As-New seam "+
			"an unjoined async step must not close was closed")

	// And it suspended *inside* the loop rather than after it, which is what
	// makes the frame evidence about the iteration boundary.
	require.Len(t, carried.GetFrames(), 2,
		"the run suspended somewhere other than inside the loop")
	next := carried.GetFrames()[1].GetNextIteration()
	assert.Positive(t, next, "the loop suspended before running an iteration")
	assert.Less(t, int(next), items, "the loop suspended after its last iteration")
}
