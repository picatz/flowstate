package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRunWorkflowAsyncUnwind is the local half of [conformance.AsyncUnwindCases]: what
// a scope owes the work it started when it is on its way out failing. The engine
// package runs the identical set against the durable driver.
//
// Run twice, and the second run is the point. Under written order this driver
// runs an async step's work where it is written and its branches in declaration
// order, so both claims hold trivially — which is precisely why neither defect
// these cases pin was visible here for as long as written order was the only
// schedule this driver had. [v1.AdversarialOrder] is the fixed schedule furthest
// from it: every branch advances last-first and every async step's work waits for
// its join. No seed, no search, the same path every run.
//
// Mutation-proved, both directions, in the schedule that reaches them:
//
//   - Removing runNodes' drain of held async work on the failing path fails the
//     first case under AdversarialOrder — `a` never runs, so nothing records and
//     nothing is compensated.
//   - Appending each branch's private undo log where the branch finishes rather
//     than in declaration order fails the second under AdversarialOrder — the
//     unwind comes back as undo-first, undo-second.
//
// Both still pass under written order with either mutation in place, which is the
// whole argument for running the set twice.
func TestRunWorkflowAsyncUnwind(t *testing.T) {
	for _, schedule := range []struct {
		name      string
		scheduler v1.Scheduler
	}{
		{name: "written order", scheduler: nil},
		{name: "adversarial order", scheduler: v1.AdversarialOrder},
	} {
		t.Run(schedule.name, func(t *testing.T) {
			for index, outline := range conformance.AsyncUnwindCases(undoPlaceholderBase) {
				t.Run(outline.Name, func(t *testing.T) {
					base, recorded := conformance.NewUndoServer(t)
					test := conformance.AsyncUnwindCases(base)[index]

					_, err := v1.Run(scheduled(t.Context(), schedule.scheduler), test.Workflow)
					require.Error(t, err, "the run was expected to fail")
					require.Contains(t, err.Error(), test.Summary,
						"the failure does not carry the account of what was compensated")

					conformance.AssertRecorded(t, test, recorded())
				})
			}
		})
	}
}

// TestRunWorkflowUndoUnderAnAdversarialSchedule runs the whole shared saga
// corpus a second time under the one schedule furthest from written order.
//
// The corpus already says what every case must produce; what this adds is that
// it must produce it when nothing runs where the file says it does. It is the
// deterministic form of what the schedule search does over seeds, kept because a
// search proves a claim about the seeds it drew and this proves it about a
// schedule that is reached every run — and because this is the shape in which
// the drain defect first showed up.
func TestRunWorkflowUndoUnderAnAdversarialSchedule(t *testing.T) {
	for index, outline := range conformance.UndoCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCases(base)[index]

			_, err := v1.Run(v1.NewContextWithScheduler(t.Context(), v1.AdversarialOrder), test.Workflow)
			if !test.Fails {
				require.NoError(t, err, "the run was expected to succeed")
			} else {
				require.Error(t, err, "the run was expected to fail")
				require.Contains(t, err.Error(), test.Summary,
					"the failure does not carry the account of what was compensated")
			}

			conformance.AssertRecorded(t, test, recorded())
		})
	}
}

// scheduled injects a scheduler, or leaves the context alone so the run gets the
// [v1.WrittenOrder] default every ordinary run gets.
func scheduled(ctx context.Context, scheduler v1.Scheduler) context.Context {
	if scheduler == nil {
		return ctx
	}

	return v1.NewContextWithScheduler(ctx, scheduler)
}
