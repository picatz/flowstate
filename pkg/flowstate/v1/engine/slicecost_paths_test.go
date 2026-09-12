package engine_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A step's condition is not the only expression a run evaluates in workflow code
// and can repeat without scheduling anything. A step's `vars:`, a `switch:`'s
// subject and a `for_each`'s `items:` are evaluated the same way at the same
// place, and each of them can sit in a loop body that schedules no activity at
// all — so a segment made of them spends unbounded deterministic work that
// neither suspension threshold can see, which is exactly the shape #1119
// describes for a condition.
//
// The bound is only as good as its narrowest path, so each of these is charged
// through [engine.executor.chargeWorkflowCost] and each is asserted here.

// heavySliceExpr is an expression every existing bound admits — comfortably
// inside [v1.DefaultCostLimit] — repeated enough times to pass
// [v1.DefaultWorkflowSliceCost]. It is the conformance corpus's own `heavy`.
//
// Its 10,000 elements are the largest input the element bound admits, which is
// why the environment below is built with [atABound]: see
// [TestASegmentOfSkippedStepsSuspendsOnTheCostItSpent]'s fixture for the whole
// reasoning.
const heavySliceExpr = "lists.range(10000).map(i, i + 1).size()"

// TestASegmentSuspendsOnTheCostOfEachWorkflowSidePath is the claim, once per
// path: a run whose only expensive work is a step's `vars:`, a `switch:`'s
// subject, or a `for_each`'s `items:` still continues as new.
//
// Every fixture's steps are otherwise free — literal kinds, an empty loop body,
// a matching case that does nothing — so the cost under test is the only cost
// there is. The steps budget is far above the step count for
// [TestASegmentOfSkippedStepsSuspendsOnTheCostItSpent]'s reason: cost is then
// the only threshold that can end the segment, which is what makes the
// continuation evidence that this path was charged.
func TestASegmentSuspendsOnTheCostOfEachWorkflowSidePath(t *testing.T) {
	t.Parallel()

	const steps = 64

	for name, build := range map[string]func(id string) *v1.Node{
		"a step's vars": func(id string) *v1.Node {
			return &v1.Node{
				Id:   id,
				Vars: map[string]*v1.Value{"size": v1.NewExpr(heavySliceExpr)},
				Kind: &v1.Node_Value{Value: v1.NewLiteral(int64(1))},
			}
		},
		"a switch's subject": func(id string) *v1.Node {
			return &v1.Node{
				Id: id,
				Kind: &v1.Node_Switch{Switch: &v1.Switch{
					Value: v1.NewExpr(heavySliceExpr),
					Cases: []*v1.Switch_Case{{
						Values: []*v1.Value{v1.NewLiteral(int64(10000))},
						Steps:  []*v1.Node{{Id: id + "-arm", Kind: &v1.Node_Value{Value: v1.NewLiteral(int64(1))}}},
					}},
				}},
			}
		},
		"a call's arguments": func(id string) *v1.Node {
			return &v1.Node{
				Id: id,
				Kind: &v1.Node_Call{Call: &v1.Call{
					Arguments: map[string]*v1.Value{"size": v1.NewExpr(heavySliceExpr)},
					// A callee of one literal `value:` step, which is a call
					// whose whole body writes no history and costs nothing:
					// the arguments are then the only work the step does.
					Workflow: &v1.Workflow{
						Name:    "callee",
						Profile: v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{
							{Name: "size", Type: v1.InputDeclaration_TYPE_INT, Required: true},
						},
						Steps: []*v1.Node{{Id: "inner", Kind: &v1.Node_Value{Value: v1.NewLiteral(int64(1))}}},
					},
				}},
			}
		},
		"a for_each's items": func(id string) *v1.Node {
			return &v1.Node{
				Id: id,
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					// Expensive to produce and empty when produced, so the loop
					// body cannot be what ends the segment.
					Items:    v1.NewExpr("lists.range(10000).map(i, i + 1).filter(i, i < 0)"),
					Iterator: "item",
					Body:     []*v1.Node{{Id: id + "-body", Kind: &v1.Node_Value{Value: v1.NewExpr("item")}}},
				}},
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nodes := make([]*v1.Node, steps)
			for i := range nodes {
				nodes[i] = build(fmt.Sprintf("step-%03d", i))
			}

			env := atABound(newWaitEnv(t))
			env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

			carried := carriedState(t, env, &v1.RunState{
				Workflow:    &v1.Workflow{Name: "workflow-side-cost", Profile: v1.CurrentProfile, Steps: nodes},
				StepsBudget: 10_000,
			})

			require.NotEmpty(t, carried.GetFrames(),
				"the segment suspended without recording where to resume")

			next := carried.GetFrames()[0].GetNextNode()
			assert.Positive(t, next,
				"the resume position is the start of the list, so the segment suspended before doing anything")
			assert.Less(t, int(next), steps,
				"the segment reached the end of the list, so something other than the cost budget ended it")
		})
	}
}
