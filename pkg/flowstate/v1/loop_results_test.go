package flowstatev1_test

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

// loopWorkflow builds a minimal workflow with one `loop:` step, optionally
// followed by a sibling step whose condition references the loop's `results`
// (whole or by field) or the workflow's own declared outputs doing the same —
// exactly the shapes [v1.LoopResultsReferenced] has to tell apart.
func loopWorkflow(loopID string, after []*v1.Node, declared []*v1.OutputDeclaration) *v1.Workflow {
	steps := append([]*v1.Node{{
		Id: loopID,
		Kind: &v1.Node_Loop{Loop: &v1.Loop{
			Until: v1.NewExpr("true"),
			Body: []*v1.Node{{
				Id:   "body",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
			}},
		}},
	}}, after...)

	return &v1.Workflow{
		Name:            "loop-results-referenced",
		Profile:         v1.CurrentProfile,
		Steps:           steps,
		DeclaredOutputs: declared,
	}
}

func condStep(id, condition string) *v1.Node {
	return &v1.Node{
		Id:        id,
		Condition: v1.NewExpr(condition),
		Kind:      &v1.Node_Task{Task: &v1.Task{Name: "log"}},
	}
}

// TestLoopResultsReferenced covers exactly what #229's suppression treats as
// "provably unread": every reachable site the language has, and the two sites
// that look reachable but are not — a loop's own body, and a call's isolated
// callee.
func TestLoopResultsReferenced(t *testing.T) {
	t.Run("nothing anywhere references the loop", func(t *testing.T) {
		wf := loopWorkflow("loop", nil, nil)
		require.False(t, v1.LoopResultsReferenced(wf, "loop"))
	})

	t.Run("a sibling step's condition reads results by field", func(t *testing.T) {
		wf := loopWorkflow("loop", []*v1.Node{
			condStep("after", "size(steps.loop.results) > 0"),
		}, nil)
		require.True(t, v1.LoopResultsReferenced(wf, "loop"))
	})

	t.Run("a sibling step's condition reads the whole step", func(t *testing.T) {
		wf := loopWorkflow("loop", []*v1.Node{
			condStep("after", "has(steps.loop)"),
		}, nil)
		require.True(t, v1.LoopResultsReferenced(wf, "loop"),
			"a whole-step reference reaches results too — it is a superset of naming the field")
	})

	t.Run("a sibling references a different field, not results", func(t *testing.T) {
		// A loop that also carries state (`state:`) reports both `results` and
		// `state`; a sibling reading only `state` must not keep `results` alive.
		wf := &v1.Workflow{
			Name:    "loop-state-only-read",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					Id: "loop",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						State:   "n",
						Initial: v1.NewLiteral(int64(0)),
						Update:  v1.NewExpr("n + 1"),
						Until:   v1.NewExpr("true"),
						Body:    []*v1.Node{{Id: "body", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}},
					}},
				},
				condStep("after", "steps.loop.state == 0"),
			},
		}
		require.False(t, v1.LoopResultsReferenced(wf, "loop"),
			"a reference to a loop's `state` output must not keep `results` alive")
	})

	t.Run("the workflow's own declared outputs read results", func(t *testing.T) {
		wf := loopWorkflow("loop", nil, []*v1.OutputDeclaration{
			{Name: "count", Value: v1.NewExpr("size(steps.loop.results)")},
		})
		require.True(t, v1.LoopResultsReferenced(wf, "loop"))
	})

	t.Run("a declared output naming an unrelated step does not count", func(t *testing.T) {
		wf := loopWorkflow("loop", []*v1.Node{condStep("after", "true")}, []*v1.OutputDeclaration{
			{Name: "ok", Value: v1.NewExpr("steps.after")},
		})
		require.False(t, v1.LoopResultsReferenced(wf, "loop"))
	})

	t.Run("a different loop's body reading an earlier loop's results counts", func(t *testing.T) {
		wf := &v1.Workflow{
			Name:    "loop-cross-reference",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					Id: "first",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						Until: v1.NewExpr("true"),
						Body:  []*v1.Node{{Id: "body", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}},
					}},
				},
				{
					Id: "second",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						Until: v1.NewExpr("true"),
						// A step *inside* second's own body reading an earlier,
						// already-completed sibling loop's results — ordinary
						// outer-scope access, nothing to do with self-reference.
						Body: []*v1.Node{condStep("body", "size(steps.first.results) >= 0")},
					}},
				},
			},
		}
		require.True(t, v1.LoopResultsReferenced(wf, "first"),
			"a later loop's body reading an earlier, already-completed loop's results is an ordinary reference")
	})

	t.Run("a loop's own body cannot make its own results reachable", func(t *testing.T) {
		// The per-iteration scope both drivers build is seeded from the outputs
		// visible *before* the loop ran, so `steps.loop.results` inside loop's
		// own body can never resolve at runtime. This spec is nonsensical (and
		// `flow validate` would likely refuse it), but if it somehow reaches
		// here the walk still records the reference — the safe direction to be
		// wrong in is "accumulate", never "suppress".
		wf := &v1.Workflow{
			Name:    "loop-self-body-reference",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					Id: "loop",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						Until: v1.NewExpr("true"),
						Body:  []*v1.Node{condStep("body", "size(steps.loop.results) >= 0")},
					}},
				},
			},
		}
		require.True(t, v1.LoopResultsReferenced(wf, "loop"),
			"a syntactic self-reference is still counted as a reference — conservative, never incorrectly suppressed")
	})

	t.Run("a call's own arguments referencing the loop count", func(t *testing.T) {
		callee := &v1.Workflow{
			Name:    "callee",
			Profile: v1.CurrentProfile,
			DeclaredInputs: []*v1.InputDeclaration{
				{Name: "n", Type: v1.InputDeclaration_TYPE_INT},
			},
			Steps: []*v1.Node{{Id: "noop", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}},
		}
		wf := loopWorkflow("loop", []*v1.Node{{
			Id: "invoke",
			Kind: &v1.Node_Call{Call: &v1.Call{
				Workflow:  callee,
				Arguments: map[string]*v1.Value{"n": v1.NewExpr("size(steps.loop.results)")},
			}},
		}}, nil)
		require.True(t, v1.LoopResultsReferenced(wf, "loop"),
			"an argument is resolved in the caller's own scope, exactly like a task's inputs")
	})

	t.Run("a callee's own steps cannot reach a caller's loop", func(t *testing.T) {
		// The callee's steps run in CallScope's isolated namespace — a
		// different node tree entirely — and a caller's loop must not be kept
		// alive by anything written inside a callee, however the id happens to
		// be spelled there.
		callee := &v1.Workflow{
			Name:    "callee",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				condStep("inside", "size(steps.loop.results) >= 0"),
			},
		}
		wf := loopWorkflow("loop", []*v1.Node{{
			Id:   "invoke",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
		}}, nil)
		require.False(t, v1.LoopResultsReferenced(wf, "loop"),
			"a reference written inside a callee's own steps must not reach the caller's loop")
	})

	t.Run("a later step's undo inputs read results", func(t *testing.T) {
		// The neighbouring hole behind #418 slice 1's compensation-join finding.
		// A compensation's inputs are resolved when its step succeeds, so an
		// output only they name is as live as one a task's own inputs name — and
		// suppressing a loop's `results` because nothing *but* an `undo:` read
		// them would fail the run at the moment it registered that compensation,
		// with the effect already performed.
		wf := loopWorkflow("loop", []*v1.Node{{
			Id:   "after",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
			Undo: &v1.Compensation{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewExpr("string(size(steps.loop.results))")},
			}},
		}}, nil)
		require.True(t, v1.LoopResultsReferenced(wf, "loop"),
			"a loop's results read only by a later step's `undo:` must survive")
	})

	t.Run("nil spec is conservative", func(t *testing.T) {
		require.True(t, v1.LoopResultsReferenced(nil, "loop"),
			"an unknown tree cannot be proven unreachable")
	})
}

// TestLoopResumeResults covers the one place suppression actually happens: a
// Continue-As-New resume boundary.
func TestLoopResumeResults(t *testing.T) {
	carried := []*v1.Workflow_StepOutputs{
		{StepValues: map[string]*v1.Node_Outputs{"body": {}}},
		{StepValues: map[string]*v1.Node_Outputs{"body": {}}},
	}

	t.Run("referenced loop keeps what was carried", func(t *testing.T) {
		wf := loopWorkflow("loop", []*v1.Node{
			condStep("after", "size(steps.loop.results) > 0"),
		}, nil)
		got := v1.LoopResumeResults(wf, "loop", carried)
		require.Equal(t, carried, got)
	})

	t.Run("unreferenced loop drops what was carried", func(t *testing.T) {
		wf := loopWorkflow("loop", nil, nil)
		got := v1.LoopResumeResults(wf, "loop", carried)
		require.Nil(t, got, "an unread loop must start its new segment fresh, not inherit prior segments' history")
	})
}

// TestAccumulateLoopResult covers #229's byte bound: reached, not merely
// respected — a just-under case must still pass, and the failure must name
// the bound and a remedy.
func TestAccumulateLoopResult(t *testing.T) {
	bigIteration := func(n int) *v1.Workflow_StepOutputs {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"body": {NamedValues: map[string]*v1.Value{
				"blob": v1.NewLiteral(strings.Repeat("x", n)),
			}},
		}}
	}

	t.Run("well under the bound accumulates without error", func(t *testing.T) {
		var results []*v1.Workflow_StepOutputs
		bytes := 0
		var err error
		for i := 0; i < 3; i++ {
			results, bytes, err = v1.AccumulateLoopResult(results, bytes, bigIteration(100))
			require.NoError(t, err)
		}
		require.Len(t, results, 3)
		require.Positive(t, bytes)
	})

	t.Run("the bound is reached, not merely respected", func(t *testing.T) {
		// One iteration sized to sit just on either side of the bound, so both
		// "just under" and "just over" are exercised against the real
		// constant rather than an approximation of it.
		var results []*v1.Workflow_StepOutputs
		bytes := 0
		var err error

		// Consume all but a small margin of the bound in one iteration that
		// must still succeed.
		results, bytes, err = v1.AccumulateLoopResult(results, bytes, bigIteration(v1.MaxLoopResultsBytes-1024))
		require.NoError(t, err, "an iteration that lands just under the bound must be accepted")
		require.LessOrEqual(t, bytes, v1.MaxLoopResultsBytes)

		// A further iteration that pushes the running total over the bound
		// must fail, naming the bound and a remedy.
		_, bytes, err = v1.AccumulateLoopResult(results, bytes, bigIteration(4096))
		require.Error(t, err, "an iteration that crosses the bound must be refused")
		require.Greater(t, bytes, v1.MaxLoopResultsBytes,
			"the failure must correspond to the bound actually having been exceeded, not merely approached")
		require.Contains(t, err.Error(), "byte limit")
		require.Contains(t, err.Error(), "max_iterations",
			"the diagnostic must name a remedy, not just the number that was exceeded")
	})
}

// TestAccumulateForEachResult is TestAccumulateLoopResult's `for_each` sibling:
// the same byte bound, reached rather than merely respected, but reporting the
// `for_each`-appropriate diagnostic. A `for_each` has no `max_iterations:` or
// `state:` to offer as a remedy, so its message names neither — a loop's remedy
// text on a `for_each` failure would be the false diagnostic CLAUDE.md's
// "Diagnostics are a feature" warns against.
func TestAccumulateForEachResult(t *testing.T) {
	bigIteration := func(n int) *v1.Workflow_StepOutputs {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"body": {NamedValues: map[string]*v1.Value{
				"blob": v1.NewLiteral(strings.Repeat("x", n)),
			}},
		}}
	}

	t.Run("well under the bound accumulates without error", func(t *testing.T) {
		var results []*v1.Workflow_StepOutputs
		bytes := 0
		var err error
		for i := 0; i < 3; i++ {
			results, bytes, err = v1.AccumulateForEachResult(results, bytes, bigIteration(100))
			require.NoError(t, err)
		}
		require.Len(t, results, 3)
		require.Positive(t, bytes)
	})

	t.Run("the bound is reached, not merely respected", func(t *testing.T) {
		var results []*v1.Workflow_StepOutputs
		bytes := 0
		var err error

		// An iteration landing just under the shared bound must be accepted — the
		// same constant a `loop:` is weighed against, since the two share the field.
		results, bytes, err = v1.AccumulateForEachResult(results, bytes, bigIteration(v1.MaxLoopResultsBytes-1024))
		require.NoError(t, err, "an iteration that lands just under the bound must be accepted")
		require.LessOrEqual(t, bytes, v1.MaxLoopResultsBytes)

		// A further iteration crossing the bound must fail, naming the bound and a
		// `for_each`-appropriate remedy.
		_, bytes, err = v1.AccumulateForEachResult(results, bytes, bigIteration(4096))
		require.Error(t, err, "an iteration that crosses the bound must be refused")
		require.Greater(t, bytes, v1.MaxLoopResultsBytes,
			"the failure must correspond to the bound actually having been exceeded, not merely approached")
		require.Contains(t, err.Error(), "byte limit")
		require.Contains(t, err.Error(), "for_each",
			"the diagnostic must name the construct that overflowed")
		require.Contains(t, err.Error(), "fewer items",
			"the remedy must be one a for_each actually has, not a loop's max_iterations:")
		require.NotContains(t, err.Error(), "max_iterations",
			"a for_each has no max_iterations: to lower — naming it would be a false diagnostic")
	})
}

// TestLoopResultsSize covers the seed a resumed segment starts its running
// byte count from — results arriving from a prior segment were accumulated
// there, not here, so their size has to be recomputed rather than assumed
// zero.
func TestLoopResultsSize(t *testing.T) {
	require.Equal(t, 0, v1.LoopResultsSize(nil))

	one := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"body": {NamedValues: map[string]*v1.Value{"v": v1.NewLiteral("hello")}},
	}}
	require.Positive(t, v1.LoopResultsSize([]*v1.Workflow_StepOutputs{one, one}))
	require.Equal(t,
		v1.LoopResultsSize([]*v1.Workflow_StepOutputs{one})*2,
		v1.LoopResultsSize([]*v1.Workflow_StepOutputs{one, one}),
		"size is additive across entries, the same way proto.Size sums what it measures",
	)
}

// TestLoopStateOutputsHonest covers #229's follow-up honest contract: a loop
// that dropped history at a Continue-As-New resume must not report the
// finishing segment's own iterations as though they were the whole run —
// this is what a caller reading a completed run's `Get` answer, `flow get`
// output, or `flowstate_get` MCP answer actually sees, so the distinction has
// to be caller-visible, not just an internal bookkeeping detail.
func TestLoopStateOutputsHonest(t *testing.T) {
	iterations := []*v1.Workflow_StepOutputs{
		{StepValues: map[string]*v1.Node_Outputs{"body": {}}},
	}
	state := v1.NewLiteral(int64(3))

	t.Run("not truncated reports results in full, identical to LoopStateOutputs", func(t *testing.T) {
		got := v1.LoopStateOutputsHonest(iterations, state, false)
		want := v1.LoopStateOutputs(iterations, state)
		require.Equal(t, want.GetNamedValues()["results"], got.GetNamedValues()["results"])
		require.Equal(t, want.GetNamedValues()["state"], got.GetNamedValues()["state"])
	})

	t.Run("truncated omits results entirely rather than reporting a partial list", func(t *testing.T) {
		got := v1.LoopStateOutputsHonest(iterations, state, true)
		_, present := got.GetNamedValues()["results"]
		require.False(t, present,
			"a truncated loop's results must be an absent key, not an empty or partial list — "+
				"either of those would still read as a claim about what the loop's whole history was")
	})

	t.Run("truncated still reports the true final state", func(t *testing.T) {
		got := v1.LoopStateOutputsHonest(iterations, state, true)
		require.Equal(t, state, got.GetNamedValues()["state"],
			"state travels in Frame.LoopState, never subject to results suppression — "+
				"truncating results must never touch it")
	})

	t.Run("truncated with no carried state reports neither key", func(t *testing.T) {
		got := v1.LoopStateOutputsHonest(iterations, nil, true)
		require.Empty(t, got.GetNamedValues())
	})
}
