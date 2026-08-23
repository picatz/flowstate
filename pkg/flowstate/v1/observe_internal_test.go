package flowstatev1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// panickingObserver is the embedder's mistake: an observer that fails on the
// fact it is handed.
type panickingObserver struct{ saw []string }

func (o *panickingObserver) StepFinished(id string, _ *Node_Outputs, _ error, _ bool) {
	o.saw = append(o.saw, id)
	panic("an observer that cannot handle what it was told")
}

func (o *panickingObserver) StepSkipped(id string) {
	o.saw = append(o.saw, id)
	panic("an observer that cannot handle a skip")
}

func (o *panickingObserver) WaitStarted(id string, _ string, _ time.Duration, _ bool) {
	o.saw = append(o.saw, id)
	panic("an observer that cannot handle a wait")
}

// TestAPanickingObserverDoesNotTakeTheRunWithIt is the rule [RunObserver]
// states, checked rather than asserted in prose: an account of the work must
// never be the reason the work does not happen.
//
// It matters because RunObserver is exported, so the implementation may be an
// embedder's. Without the recover, a run that was succeeding fails — and it
// fails inside recordStepOutcome, so the failure is reported against the step
// that was about to succeed, which is the most misleading place it could
// possibly surface.
//
// The observer is asked to observe on every callback and panics on each, so
// the assertion is that the run still reports success and that every
// observation point was actually reached rather than skipped.
func TestAPanickingObserverDoesNotTakeTheRunWithIt(t *testing.T) {
	t.Parallel()

	observer := &panickingObserver{}
	ctx := NewContextWithRunObserver(t.Context(), observer)

	require.NotPanics(t, func() {
		observeStepFinished(ctx, "build", nil, nil, false)
		observeStepSkipped(ctx, "prod_gate")
		observeWaitStarted(ctx, "approval", "ship-approved", time.Hour, true)
	})

	require.Equal(t, []string{"build", "prod_gate", "approval"}, observer.saw,
		"a panic in one callback stopped a later observation point from being reached")
}
