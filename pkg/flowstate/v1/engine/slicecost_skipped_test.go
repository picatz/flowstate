package engine_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// A step whose `if:` is false is the one shape that spends workflow-side CEL
// and leaves nothing behind: no output, no history event, and no step counted
// against the budget. Before #1119 that was the whole of it — the condition's
// cost was discarded by the evaluating API, and the skipped path returned to
// the top of the loop without ever reaching the continuation check — so a
// segment made of skipped steps ran unbounded deterministic work that neither
// of the two suspension thresholds could see. The in-memory yield on that path
// then kept the worker's deadlock detector from noticing, which is what turned
// a bounded expression repeated enough times into a workflow task that never
// ends and is retried from its own history forever.
//
// Both halves are needed and neither is sufficient. Charging with no check
// spends a budget nothing reads; checking with no charge reads a budget nothing
// spends.

// skippedHeavyConditions is a workflow whose every step is skipped by an
// individually bounded but expensive condition.
//
// The expression is the conformance corpus's own `heavy`, and the count is
// chosen so the total is comfortably past [v1.DefaultWorkflowSliceCost] while
// each evaluation stays far inside [v1.DefaultCostLimit]: that gap is the whole
// attack, an expression every existing bound admits, repeated.
//
// A list of 10,000 elements is the largest input the element bound admits, so
// every environment below is built with [atABound]: a single evaluation of this
// expression is exactly the at-a-bound work that budget exists for, and holding
// it to the SDK's one-second default under the race detector is stricter than
// production rather than equal to it. See
// [conformance.BoundaryDeadlockDetectionTimeout].
func skippedHeavyConditions(steps int) *v1.Workflow {
	const heavy = "lists.range(10000).map(i, i + 1).size()"

	nodes := make([]*v1.Node, steps)
	for i := range nodes {
		nodes[i] = &v1.Node{
			Id:        fmt.Sprintf("skipped-%03d", i),
			Condition: v1.NewExpr(heavy + " == 0"),
			Kind:      &v1.Node_Value{Value: v1.NewLiteral(int64(1))},
		}
	}

	return &v1.Workflow{Name: "skipped", Profile: v1.CurrentProfile, Steps: nodes}
}

// TestASegmentOfSkippedStepsSuspendsOnTheCostItSpent is the claim: a run whose
// steps are all skipped still continues as new, because the conditions it
// evaluated were charged to the segment and the skipped path now checks.
//
// The steps budget is deliberately far above the step count, so nothing here
// can suspend on steps processed — a skipped step increments nothing anyway.
// Cost is the only threshold that can end this segment, which is what makes the
// continuation evidence that cost was accounted.
func TestASegmentOfSkippedStepsSuspendsOnTheCostItSpent(t *testing.T) {
	t.Parallel()

	env := atABound(newWaitEnv(t))
	env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

	carried := carriedState(t, env, &v1.RunState{
		Workflow:    skippedHeavyConditions(64),
		StepsBudget: 10_000,
	})

	require.NotEmpty(t, carried.GetFrames(),
		"the segment suspended without recording where to resume")

	next := carried.GetFrames()[0].GetNextNode()
	assert.Positive(t, next,
		"the resume position is the start of the list, so the segment suspended before doing anything")
	assert.Less(t, int(next), 64,
		"the segment reached the end of the list, so something other than the cost budget ended it")
}

// TestASegmentOfCheapSkippedStepsDoesNotSuspend is the half that keeps the
// assertion above honest. The same shape with a condition that costs nothing to
// evaluate must run to completion in one segment: a checker that continued as
// new on every skipped step would satisfy the test above while turning an
// ordinary `if: false` into a history event.
func TestASegmentOfCheapSkippedStepsDoesNotSuspend(t *testing.T) {
	t.Parallel()

	nodes := make([]*v1.Node, 64)
	for i := range nodes {
		nodes[i] = &v1.Node{
			Id:        fmt.Sprintf("skipped-%03d", i),
			Condition: v1.NewExpr("1 == 2"),
			Kind:      &v1.Node_Value{Value: v1.NewLiteral(int64(1))},
		}
	}

	// Not [atABound]: this fixture's conditions are free, so a goroutine that
	// does not yield here is a finding rather than a bound being exercised.
	env := newWaitEnv(t)
	env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow:    &v1.Workflow{Name: "cheap", Profile: v1.CurrentProfile, Steps: nodes},
		StepsBudget: 10_000,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"a run of cheap skipped steps must finish in one segment")
}
