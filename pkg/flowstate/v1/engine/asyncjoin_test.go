package engine_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// An `async:` step is joined where a later node reads it or where its scope
// ends. A continuation emitted beneath the scope that started it — a `for_each`
// or `loop:` iteration, a called workflow's own boundary — leaves through that
// scope, and used to take the unjoined step with it: its outputs and its
// failure then existed in neither segment (#1968).

// TestAnOutstandingAsyncFailureCrossesALoopSeam is that claim for a failure.
//
// The step is not tolerated, so it must fail the run. It is never read by a
// later node, which makes the loop's seam the only place the run could stop
// between starting it and joining it.
func TestAnOutstandingAsyncFailureCrossesALoopSeam(t *testing.T) {
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
					Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
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

	require.Len(t, carried.GetFrames(), 2, "the loop did not suspend inside its iterations")

	// The step was started in the top-level scope and the continuation left
	// through it, so the failure is that scope's to carry: without the join
	// the next segment starts with no `notify` outstanding and no failure to
	// raise, and the run succeeds having dropped a step that failed.
	held := carried.GetFrames()[0].GetHeldFailures()
	require.Len(t, held, 1, "the failure of an async step the continuation outran did not cross")
	assert.Equal(t, "notify", held[0].GetStepId())
}
