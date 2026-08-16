package dst_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The schedule-equivalence property, over the corpus both drivers already
// share.
//
// The cases are deliberately not new ones. pkg/flowstate/v1/internal/conformance is where a
// behaviour lives when both drivers owe it, and every case there already pins
// what a run produces; what this package adds is the question those cases never
// asked, which is whether the answer survives being reached in a different
// order. Writing a separate corpus here would have produced a second set of
// expectations to drift from the first.
//
// Each of these asserts one more thing beyond the property: that the search
// actually reached a scheduling junction. A corpus with no `parallel:` and no
// `async:` in it passes schedule equivalence by having no schedules, and a job
// that reports green by exploring nothing is worse than no job.

// undoPlaceholderBase is the base URL the undo cases are enumerated against
// before a real recording server exists — the same placeholder both drivers'
// own undo tests use, and never contacted.
const undoPlaceholderBase = "http://undo.invalid"

// TestScheduleEquivalenceOverAsyncCases explores every shared `async:` case.
//
// This is the set the property exists for. `async:` is the one marker that lets
// execution depart from written order, and locally the departure is a choice
// this driver makes: run the work where it is written, or hold it until the
// join. Both are legal rehearsals of the durable driver's genuinely overlapping
// coroutine, so an author must not be able to tell which one ran.
func TestScheduleEquivalenceOverAsyncCases(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)

	explored := 0
	for _, test := range conformance.AsyncCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			report := dst.CheckScheduleEquivalence(t, func(ctx context.Context) dst.Result {
				outputs, err := v1.RunWithInputs(ctx, test.Workflow, test.Inputs)

				return dst.Result{Transcript: outputs, Err: err}
			})
			if report.Decisions() > 0 {
				explored++
			}
		})
	}

	require.Positive(t, explored,
		"no async case reached a scheduling junction, so the property proved nothing about any of them")
}

// TestScheduleEquivalenceOverControlFlowCases explores the shared control-flow
// cases, which is where `parallel:` lives.
func TestScheduleEquivalenceOverControlFlowCases(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)

	explored := 0
	for _, test := range conformance.ControlFlowCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			report := dst.CheckScheduleEquivalence(t, func(ctx context.Context) dst.Result {
				outputs, err := v1.RunWithInputs(ctx, test.Workflow, test.Inputs)

				return dst.Result{Transcript: outputs, Err: err}
			})
			if report.Decisions() > 0 {
				explored++
			}
		})
	}

	require.Positive(t, explored,
		"no control-flow case reached a scheduling junction, so the property proved nothing about any of them")
}

// TestScheduleEquivalenceOverUndoCases is the property pointed at the claim it
// was written for: compensations unwind in reverse *written* order, under every
// schedule, including the schedules where completion order is not written order.
//
// The strongest of the three, because its observables are real effects rather
// than the engine's account of itself. A recording server per case, kept across
// every schedule of that case and sliced per run, so each schedule's effects are
// its own while the base URL — which appears in failure text — stays one string
// the comparison is not fooled by.
//
// [conformance.UndoCase.UnorderedPrefix] carries over unchanged and is exactly the
// right line: the concurrent *work* a case does is the schedule's to order, and
// the compensations that follow it are the claim.
func TestScheduleEquivalenceOverUndoCases(t *testing.T) {
	explored := 0
	for index, outline := range conformance.UndoCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCases(base)[index]

			report := dst.CheckScheduleEquivalence(t, func(ctx context.Context) dst.Result {
				before := len(recorded())
				_, err := v1.Run(ctx, test.Workflow)

				return dst.Result{
					Err:             err,
					Effects:         recorded()[before:],
					UnorderedPrefix: test.UnorderedPrefix,
				}
			})
			if report.Decisions() > 0 {
				explored++
			}
		})
	}

	require.Positive(t, explored,
		"no undo case reached a scheduling junction, so nothing here was checked under an alternative schedule")
}

// TestScheduleEquivalenceOverUndoCallCases is the same claim across a `call:`
// boundary: a callee's compensations register onto the caller's stack and unwind
// in reverse written order whatever the schedule did.
func TestScheduleEquivalenceOverUndoCallCases(t *testing.T) {
	for index, outline := range conformance.UndoCallCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCallCases(base)[index]

			dst.CheckScheduleEquivalence(t, func(ctx context.Context) dst.Result {
				before := len(recorded())
				_, err := v1.Run(ctx, test.Workflow)

				return dst.Result{Err: err, Effects: recorded()[before:]}
			})
		})
	}
}
