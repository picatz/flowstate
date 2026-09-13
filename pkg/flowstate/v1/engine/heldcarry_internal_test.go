package engine

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.temporal.io/sdk/temporal"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A failure a debug ask made a scope hear early is an obligation that outlives
// the segment holding it, and [heldFailureCarryChange] is what lets the segment
// end. These are the four mechanical claims that stand behind the end-to-end
// round trips in debugasyncorder_test.go, each stated where a single wrong line
// shows up as itself rather than as a run that finished differently:
//
//   - what crosses is what comes back ([heldAcross], [heldFrom]),
//   - a history recorded before the marker still refuses the seam
//     ([executor.canSuspendHolding]),
//   - and the record of a held step survives the compaction the seam performs
//     ([keepHeldOutputs]).

// TestAHeldFailureCrossesWithEverythingItsRaiseNeeds pins the round trip field
// by field.
//
// Message, Recorded and recordedFromTask are three different sentences about
// one failure and the resumed segment needs all three: Message is what a person
// reads at the run's failure, Recorded is what an author's expression compares,
// and recordedFromTask decides whether an enclosing level prepends its own
// position to Recorded. An earlier shape carried Message alone and re-derived
// the rest, which made a run that suspended report differently from a run that
// did not.
func TestAHeldFailureCrossesWithEverythingItsRaiseNeeds(t *testing.T) {
	t.Parallel()

	t.Run("every field", func(t *testing.T) {
		t.Parallel()

		original := &ErrRunFailed{
			Message:          `step "deploy": task "http" failed (Upstream): 503`,
			Recorded:         `task "http" failed (Upstream): 503`,
			recordedFromTask: true,
			Kind:             v1.ErrorKindUpstream,
		}

		back := heldFrom(heldAcross([]heldFailure{{id: "deploy", err: original}}))
		require.Len(t, back, 1)
		assert.Equal(t, "deploy", back[0].id, "a held failure lost the id a later reference finds it under")

		var run *ErrRunFailed
		require.ErrorAs(t, back[0].err, &run, "a held failure came back as something an enclosing level cannot classify")

		assert.Equal(t, original.Message, run.Message, "the sentence a person reads at the run's failure did not cross")
		assert.Equal(t, original.Recorded, run.Recorded, "the sentence an author's expression compares did not cross")
		assert.Equal(t, original.Kind, run.Kind, "the classification a client reads back did not cross")
		assert.True(t, run.recordedFromTask,
			"a classified task failure came back unclassified, so an enclosing level prepends a position it should absorb")
	})

	t.Run("recordedFromTask false stays false", func(t *testing.T) {
		t.Parallel()

		// The other direction of the same bit, because a carry that hard-coded
		// true would pass the case above.
		back := heldFrom(heldAcross([]heldFailure{{
			id:  "compute",
			err: &ErrRunFailed{Message: "boom", Recorded: `step "compute": boom`},
		}}))
		require.Len(t, back, 1)

		var run *ErrRunFailed
		require.ErrorAs(t, back[0].err, &run)
		assert.False(t, run.recordedFromTask,
			"an unclassified failure came back claiming a task classification, so its position is dropped")
	})

	t.Run("a failure that is not an ErrRunFailed", func(t *testing.T) {
		t.Parallel()

		// Defensive rather than reachable: everything [executor.recordOutcome]
		// propagates is already an [ErrRunFailed], and a cancellation is
		// returned ahead of the hold. What this pins is the fallback's
		// direction — carried by its text and rebuilt as a run failure, rather
		// than dropped, because losing a failure is the defect the whole
		// mechanism exists to prevent.
		back := heldFrom(heldAcross([]heldFailure{{id: "odd", err: errors.New("some other failure")}}))
		require.Len(t, back, 1)

		var run *ErrRunFailed
		require.ErrorAs(t, back[0].err, &run)
		assert.Equal(t, "some other failure", run.Message, "a failure of an unexpected shape crossed the seam without its text")
	})

	t.Run("written order", func(t *testing.T) {
		t.Parallel()

		// Which failure a scope reports is decided by written order, and a
		// scope reports the first one it holds. A carry that reordered would
		// change which failure a suspended run reports and no other assertion
		// here would see it.
		back := heldFrom(heldAcross([]heldFailure{
			{id: "first", err: &ErrRunFailed{Message: "1"}},
			{id: "second", err: &ErrRunFailed{Message: "2"}},
			{id: "third", err: &ErrRunFailed{Message: "3"}},
		}))
		assert.Equal(t, []string{"first", "second", "third"}, heldIDs(back),
			"the order a scope reports its held failures in changed across a seam")
	})

	t.Run("nothing held", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, heldAcross(nil), "an empty hold wrote a frame field")
		assert.Nil(t, heldFrom(nil))
	})
}

// TestAHistoryWithoutTheMarkerStillRefusesTheSeam is the replay guard.
//
// A run recorded before [heldFailureCarryChange] refused every boundary while a
// failure was held, and a worker running this build has to replay to those
// commands: suspending where that history holds an activity wedges the run on a
// nondeterminism error rather than failing it visibly.
func TestAHistoryWithoutTheMarkerStillRefusesTheSeam(t *testing.T) {
	t.Parallel()

	held := []heldFailure{{id: "failing", err: &ErrRunFailed{Message: "boom"}}}

	t.Run("before the marker", func(t *testing.T) {
		t.Parallel()

		// The budget is spent, so every other reason to suspend says yes.
		e := &executor{processed: 5, budget: 1}
		require.True(t, e.shouldSuspend(), "the fixture does not reach the arm under test")

		assert.False(t, e.canSuspendHolding(held),
			"a history recorded before the marker suspended while holding a failure, which replays as nondeterminism")
		assert.True(t, e.canSuspendHolding(nil),
			"a history recorded before the marker stopped suspending when it was holding nothing")
	})

	t.Run("with the marker", func(t *testing.T) {
		t.Parallel()

		e := &executor{processed: 5, budget: 1, carriesHeld: true}
		assert.True(t, e.canSuspendHolding(held),
			"the failure crosses in the frame, so holding one is no longer a reason to refuse")
	})

	t.Run("the refusal is decided before the reasons", func(t *testing.T) {
		t.Parallel()

		// A version 1 history refuses the seam whatever [executor.shouldSuspend]
		// would have said, so the refusal is answered first — which is why this
		// executor, with no workflow context for shouldSuspend's own
		// `ContinueAsNewSuggested` arm to read, answers rather than panics.
		e := &executor{budget: 1}
		assert.False(t, e.canSuspendHolding(held),
			"the version 1 refusal is reached only after the reasons, so a held failure suspends where the recorded history did not")
	})
}

// TestHoldingIsStampedOntoTheFrameThatSuspends pins where the hold is written.
//
// [executor.setFrame] replaces the frame at a depth wholesale on every node of
// the walk, so a hold stamped when the failure was first heard would be erased
// by the next step's position — silently, and only sometimes.
func TestHoldingIsStampedOntoTheFrameThatSuspends(t *testing.T) {
	t.Parallel()

	e := &executor{}
	e.setFrame(0, 3)
	e.holdInFrame(0, []heldFailure{{id: "failing", err: &ErrRunFailed{Message: "boom"}}})

	require.Len(t, e.frames, 1)
	assert.Equal(t, int32(3), e.frames[0].GetNextNode(), "the stamp overwrote the position the segment resumes at")
	require.Len(t, e.frames[0].GetHeldFailures(), 1)
	assert.Equal(t, "failing", e.frames[0].GetHeldFailures()[0].GetStepId())

	// And the next position clears it, which is why the order at the boundary
	// is setFrame first and holdInFrame second.
	e.setFrame(0, 4)
	assert.Empty(t, e.frames[0].GetHeldFailures(), "a stale hold survived the position that replaced it")

	// A depth with no frame is not a panic and not an invention.
	e.holdInFrame(7, []heldFailure{{id: "failing", err: errors.New("boom")}})
	assert.Len(t, e.frames, 1)
}

// TestAHeldStepsRecordSurvivesTheSeamsCompaction is the transcript half.
//
// Nothing remaining mentions a held step — that is why its failure is held
// rather than raised — so the reference walk prunes its outputs. But the
// resumed segment raises that failure, and [v1.PartialTranscript] is where a
// person reads which step it was. Pruning the entry makes a run that suspended
// between the hold and the raise report the failure with the failing step
// missing, while a run that did not suspend reports it with the step present.
func TestAHeldStepsRecordSurvivesTheSeamsCompaction(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name:    "held-step-transcript",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{Id: "failing"},
			{Id: "next"},
		},
	}
	outputs := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"failing":  v1.FailedStepOutputs("task \"http\" failed (Upstream): 503"),
		"unneeded": {},
	}}

	t.Run("held", func(t *testing.T) {
		t.Parallel()

		frames := []*v1.Frame{{
			NextNode:     1,
			HeldFailures: []*v1.HeldFailure{{StepId: "failing", Message: "boom"}},
		}}

		carried := compactOutputsForFrames(spec, frames, outputs)
		require.Contains(t, carried.GetStepValues(), "failing",
			"the failing step's own record was pruned at the seam, so the resumed run raises a failure for a step its transcript never names")
		assert.Equal(t, outputs.GetStepValues()["failing"].GetNamedValues(),
			carried.GetStepValues()["failing"].GetNamedValues(),
			"the record survived as something other than what the step recorded")
	})

	t.Run("not held", func(t *testing.T) {
		t.Parallel()

		// The control: nothing about this keeps an ordinary unreferenced step,
		// or the claim above would hold for a compactor that did nothing.
		carried := compactOutputsForFrames(spec, []*v1.Frame{{NextNode: 1}}, outputs)
		assert.NotContains(t, carried.GetStepValues(), "failing",
			"a step nothing holds and nothing references was carried anyway, so this test would pass without the mechanism")
		assert.NotContains(t, carried.GetStepValues(), "unneeded")
	})
}

// TestACancellationIsNeverHeld is two halves of one claim, and the first half
// is what makes the second load-bearing.
//
// A cancellation reaches [executor.joinAsync]'s caller unwrapped, on purpose:
// [executor.recordOutcome] returns it ahead of `continue_on_error` so Temporal
// reads the run as CANCELED rather than FAILED and the run takes its
// cancellation compensations. Holding one used to be harmless because the error
// object itself was what a later node or the scope end re-raised. The carry
// changed that — what crosses a seam is a rebuilt [ErrRunFailed] — so a held
// cancellation would come back a run failure, and a cancelled run would resume,
// report FAILED, and unwind the wrong way.
func TestACancellationIsNeverHeld(t *testing.T) {
	t.Parallel()

	t.Run("the carry cannot represent one", func(t *testing.T) {
		t.Parallel()

		cancelled := temporal.NewCanceledError()
		require.True(t, temporal.IsCanceledError(cancelled), "the fixture is not a cancellation")

		back := heldFrom(heldAcross([]heldFailure{{id: "outstanding", err: cancelled}}))
		require.Len(t, back, 1)
		assert.False(t, temporal.IsCanceledError(back[0].err),
			"the carry now preserves a cancellation: either the guard below stopped being load-bearing, or a cancellation became a shape [v1.HeldFailure] can hold — decide which, do not delete the guard")
	})

	t.Run("nothing holds one", func(t *testing.T) {
		t.Parallel()

		// Structural, because the guard is one line inside the drain loop and
		// the run that would expose its absence has to be cancelled, holding,
		// and suspending at once. What is checked is the shape that makes the
		// guard work wherever a hold is written: a `heldFailure` is only ever
		// built where a cancellation has already been returned.
		//
		// It also covers the hold site a later change adds, which is the one
		// this cannot be written as a fixture for.
		fset := token.NewFileSet()
		sources, err := filepath.Glob("*.go")
		require.NoError(t, err)

		found := 0
		for _, source := range sources {
			if strings.HasSuffix(source, "_test.go") {
				continue
			}

			file, err := parser.ParseFile(fset, source, nil, 0)
			require.NoError(t, err)

			ast.Inspect(file, func(n ast.Node) bool {
				block, ok := n.(*ast.BlockStmt)
				if !ok {
					return true
				}
				for _, stmt := range block.List {
					if !holdsALiveFailure(stmt) {
						continue
					}
					found++
					assert.Truef(t, blockReturnsCancellationsFirst(block),
						"%s: a failure is held in a block that does not return a cancellation first, so a cancelled run can resume as a failed one",
						fset.Position(stmt.Pos()))
				}

				return true
			})
		}

		require.Positive(t, found, "no production code builds a heldFailure, so this test proves nothing")
	})
}

// holdsALiveFailure reports whether a statement holds a failure this segment
// heard, in the block it is written in.
//
// Nested blocks are not descended into, because a guard belongs to the block
// that holds: without this, every enclosing `for` and `if` up to the function
// body would read as holding a failure its body holds, and the guard written
// beside that hold would be invisible at each of those levels.
//
// A [heldFailure] whose `err` is built in place is a *rebuild* rather than a
// hold — [heldFrom], reading back what crossed a seam — and needs no guard for
// the reason the first half of this test states: what crossed cannot be a
// cancellation, because the carry has no way to represent one.
func holdsALiveFailure(stmt ast.Stmt) bool {
	held := false
	ast.Inspect(stmt, func(n ast.Node) bool {
		if held {
			return false
		}
		if _, ok := n.(*ast.BlockStmt); ok && n != ast.Node(stmt) {
			return false
		}
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if ident, ok := lit.Type.(*ast.Ident); !ok || ident.Name != "heldFailure" {
			return true
		}
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || key.Name != "err" {
				continue
			}
			value := kv.Value
			if unary, ok := value.(*ast.UnaryExpr); ok {
				value = unary.X
			}
			if _, built := value.(*ast.CompositeLit); !built {
				held = true
			}
		}

		return true
	})

	return held
}

// blockReturnsCancellationsFirst reports whether a block guards itself with a
// `temporal.IsCanceledError` that returns, ahead of everything else it does.
//
// Ahead of, rather than merely present: a guard written after the hold would
// read as satisfied and hold the cancellation anyway.
func blockReturnsCancellationsFirst(block *ast.BlockStmt) bool {
	for _, stmt := range block.List {
		if holdsALiveFailure(stmt) {
			return false
		}

		guard, ok := stmt.(*ast.IfStmt)
		if !ok {
			continue
		}
		call, ok := guard.Cond.(*ast.CallExpr)
		if !ok {
			continue
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "IsCanceledError" {
			continue
		}
		if len(guard.Body.List) != 1 {
			continue
		}
		if _, ok := guard.Body.List[0].(*ast.ReturnStmt); ok {
			return true
		}
	}

	return false
}
