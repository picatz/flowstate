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

// A wait that returns without parking leaves no history event to pace the
// expression that decided it: `sleep:` computing zero schedules no timer, a past
// `wait_until:` resolves at once, and a zero signal `timeout:` lapses at once. A
// loop of them is workflow-side work that a segment must charge (#2629), and a
// stored output expression resolved lazily under another evaluation spends cost
// the evaluation that owns it never counted (#2627).
//
// Each fixture's only expense is the expression under test, and the cheap twin of
// each proves the charge is not a refusal: the same shape with a free expression
// runs to completion in one segment.

const waitSteps = 64

// waitFixtures builds each non-parking wait, deciding on the given expression.
func waitFixtures(cost string) map[string]func(id string) *v1.Node {
	return map[string]func(id string) *v1.Node{
		"a zero sleep": func(id string) *v1.Node {
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_DurationExpr{DurationExpr: v1.NewExpr(cost + " > 0 ? duration('0s') : duration('1s')")},
			}}}
		},
		"a past wait_until": func(id string) *v1.Node {
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Until{Until: v1.NewExpr(cost + " > 0 ? timestamp('2000-01-01T00:00:00Z') : now")},
			}}}
		},
		"a zero signal timeout": func(id string) *v1.Node {
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind:        &v1.Wait_Signal{Signal: &v1.Signal{Name: "never-sent"}},
				TimeoutExpr: v1.NewExpr(cost + " > 0 ? duration('0s') : duration('1s')"),
			}}}
		},
	}
}

func TestASegmentSuspendsOnTheCostOfAWaitThatDoesNotPark(t *testing.T) {
	t.Parallel()

	for name, build := range waitFixtures(heavySliceExpr) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nodes := make([]*v1.Node, waitSteps)
			for i := range nodes {
				nodes[i] = build(fmt.Sprintf("wait-%03d", i))
			}

			env := atABound(newWaitEnv(t))
			env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

			carried := carriedState(t, env, &v1.RunState{
				Workflow:    &v1.Workflow{Name: "wait-cost", Profile: v1.CurrentProfile, Steps: nodes},
				StepsBudget: 10_000,
			})

			require.NotEmpty(t, carried.GetFrames(), "the segment suspended without recording where to resume")
			next := carried.GetFrames()[0].GetNextNode()
			assert.Positive(t, next, "the segment suspended before doing anything")
			assert.Less(t, int(next), waitSteps,
				"the segment reached the end of the list, so something other than the cost budget ended it")
		})
	}
}

func TestASegmentOfCheapWaitsThatDoNotParkDoesNotSuspend(t *testing.T) {
	t.Parallel()

	for name, build := range waitFixtures("1") {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nodes := make([]*v1.Node, waitSteps)
			for i := range nodes {
				nodes[i] = build(fmt.Sprintf("wait-%03d", i))
			}

			env := newWaitEnv(t)
			env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

			env.ExecuteWorkflow(engine.Run, &v1.RunState{
				Workflow:    &v1.Workflow{Name: "cheap-waits", Profile: v1.CurrentProfile, Steps: nodes},
				StepsBudget: 10_000,
			})

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError(), "cheap waits must finish in one segment")
		})
	}
}

// lazyOutputState reads a stored output expression from every step's
// condition. The condition itself is free, so only the lazy resolution costs.
func lazyOutputState(stored string) *v1.RunState {
	nodes := make([]*v1.Node, waitSteps)
	for i := range nodes {
		nodes[i] = &v1.Node{
			Id:        fmt.Sprintf("skipped-%03d", i),
			Condition: v1.NewExpr("steps.seed.x == 0"),
			Kind:      &v1.Node_Value{Value: v1.NewLiteral(int64(1))},
		}
	}

	return &v1.RunState{
		Workflow: &v1.Workflow{Name: "lazy-output", Profile: v1.CurrentProfile, Steps: nodes},
		Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"seed": {NamedValues: map[string]*v1.Value{"x": v1.NewExpr(stored)}},
		}},
		StepsBudget: 10_000,
	}
}

func TestASegmentSuspendsOnTheCostOfALazilyResolvedOutput(t *testing.T) {
	t.Parallel()

	env := atABound(newWaitEnv(t))
	env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

	carried := carriedState(t, env, lazyOutputState(heavySliceExpr))

	require.NotEmpty(t, carried.GetFrames(), "the segment suspended without recording where to resume")
	next := carried.GetFrames()[0].GetNextNode()
	assert.Positive(t, next, "the segment suspended before doing anything")
	assert.Less(t, int(next), waitSteps,
		"the segment reached the end of the list, so something other than the cost budget ended it")
}

func TestASegmentOfCheapLazilyResolvedOutputsDoesNotSuspend(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(engine.Run, lazyOutputState("1"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(), "cheap stored outputs must resolve within one segment")
}
