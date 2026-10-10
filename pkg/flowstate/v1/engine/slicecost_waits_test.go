package engine_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

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

// rangeSliceExpr spends 100,000 cost units in `lists.range`, which CEL prices by
// the elements it produces and which allocates them without evaluating anything
// per element. A comprehension spending the same units
// (`lists.range(10000).map(i, i + 1).size()`) costs several times
// more wall time for the same units under -race: a quorum draining its deliveries without a yield then ran past the 15s deadlock
// detector. Same threshold crossing, a fraction of the time.
var rangeSliceExpr = "(" + strings.Repeat("lists.range(10000).size() + ", 9) + "lists.range(10000).size())"

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
		"a shaped signal's outputs": func(id string) *v1.Node {
			// Shaping runs when the wait resolves, and a zero timeout resolves it
			// before it parks.
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{
					Name:    "never-sent",
					Outputs: map[string]*v1.Value{"size": v1.NewExpr(cost)},
				}},
				Timeout: durationpb.New(0),
			}}}
		},
		"a shaped batch's outputs": func(id string) *v1.Node {
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
					Name:    "never-sent",
					Outputs: map[string]*v1.Value{"size": v1.NewExpr(cost)},
				}},
				Timeout: durationpb.New(0),
			}}}
		},
		"a zero batch timeout": func(id string) *v1.Node {
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind:        &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{Name: "never-sent"}},
				TimeoutExpr: v1.NewExpr(cost + " > 0 ? duration('0s') : duration('1s')"),
			}}}
		},
		"a signal prompt": func(id string) *v1.Node {
			// Evaluated after the wait has been found unable to resolve early,
			// so the wait parks on a one-second timer the test clock skips.
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "never-sent", Prompt: v1.NewExpr("string(" + cost + ")")}},
				Timeout: durationpb.New(time.Second),
			}}}
		},
		"a batch prompt": func(id string) *v1.Node {
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind:    &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{Name: "never-sent", Prompt: v1.NewExpr("string(" + cost + ")")}},
				Timeout: durationpb.New(time.Second),
			}}}
		},
		"a quorum prompt": func(id string) *v1.Node {
			return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
					Name:   "never-sent",
					Prompt: v1.NewExpr("string(" + cost + ")"),
					Quorum: &v1.SignalQuorum{Approve: 1},
				}},
				Timeout: durationpb.New(time.Second),
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

	for name, build := range waitFixtures(rangeSliceExpr) {
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

	carried := carriedState(t, env, lazyOutputState(rangeSliceExpr))

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

// quorumDeliveries is few on purpose: each heavy evaluation runs inside one step
// with no yield, so more of them than the slice threshold needs only risks the
// deadlock detector.
const quorumDeliveries = 8

// quorumState is a quorum wait over quorumDeliveries carried deliveries followed by a
// free step. The deliveries are already carried, so the wait drains them without
// parking and the zero timeout ends it; a segment boundary falls between the two
// steps, which is where the spent cost is read. Approve is above the delivery
// count so the tally never decides early and every delivery is evaluated.
func quorumState(approved bool, veto *v1.Value, exclude []*v1.Value) *v1.RunState {
	pending := make([]*v1.PendingSignal, quorumDeliveries)
	for i := range pending {
		pending[i] = &v1.PendingSignal{
			Name: "votes",
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(approved),
			}},
		}
	}

	return &v1.RunState{
		Workflow: &v1.Workflow{Name: "quorum-cost", Profile: v1.CurrentProfile, Steps: []*v1.Node{
			{Id: "gate", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
					Name:   "votes",
					Quorum: &v1.SignalQuorum{Approve: 100, Distinct: proto.Bool(false), Veto: veto, Exclude: exclude},
				}},
				Timeout: durationpb.New(0),
			}}},
			{Id: "after", Kind: &v1.Node_Value{Value: v1.NewLiteral(int64(1))}},
		}},
		PendingSignals: pending,
		StepsBudget:    10_000,
	}
}

func quorumFixtures(cost string) map[string]*v1.RunState {
	return map[string]*v1.RunState{
		"a quorum veto":    quorumState(true, v1.NewExpr(cost+" < 0"), nil),
		"a quorum exclude": quorumState(true, nil, []*v1.Value{v1.NewExpr(cost + " < 0 ? ['nobody'] : []")}),
	}
}

func TestASegmentSuspendsOnTheCostOfQuorumExpressions(t *testing.T) {
	t.Parallel()

	for name, state := range quorumFixtures(rangeSliceExpr) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			env := atABound(newWaitEnv(t))
			env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

			carried := carriedState(t, env, state)

			require.NotEmpty(t, carried.GetFrames(), "the segment suspended without recording where to resume")
			assert.Equal(t, int32(1), carried.GetFrames()[0].GetNextNode(),
				"the segment did not suspend between the gate and the step after it")
		})
	}
}

func TestASegmentOfCheapQuorumExpressionsDoesNotSuspend(t *testing.T) {
	t.Parallel()

	for name, state := range quorumFixtures("1") {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)
			env.OnUpsertMemo(mock.Anything).Return(nil).Maybe()

			env.ExecuteWorkflow(engine.Run, state)

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError(), "a cheap quorum must finish in one segment")
		})
	}
}
