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
		observeStepFinished(ctx, "build", nil, nil, false, SensitiveValues{})
		observeStepSkipped(ctx, "prod_gate")
		observeWaitStarted(ctx, "approval", "ship-approved", time.Hour, true)
	})

	require.Equal(t, []string{"build", "prod_gate", "approval"}, observer.saw,
		"a panic in one callback stopped a later observation point from being reached")
}

// onlyWithholding is a [WithholdingOnlyRunObserver] that counts what it is
// told.
type onlyWithholding struct{ told int }

func (*onlyWithholding) StepFinished(string, *Node_Outputs, error, bool) {}
func (*onlyWithholding) StepSkipped(string)                              {}
func (*onlyWithholding) WaitStarted(string, string, time.Duration, bool) {}
func (o *onlyWithholding) StepWithheld(string, SensitiveValues)          { o.told++ }

// TestAWithholdingOnlyObserverCostsNoCopy: an observer that reads only what a
// step withholds is told it, and the engine copies no outputs for it, where
// an observer reading the outputs gets its own copy of each (Codex, #2215).
func TestAWithholdingOnlyObserverCostsNoCopy(t *testing.T) {
	named := make(map[string]*Value, 200)
	for i := range 200 {
		named[string(rune('a'+i%26))+string(rune('a'+i/26))] = NewLiteral("value")
	}
	outputs := &Node_Outputs{NamedValues: named}

	only := &onlyWithholding{}
	onlyCtx := NewContextWithRunObserver(t.Context(), only)
	require.True(t, withholdingRead(onlyCtx), "the engine would compute no set for it")
	onlyAllocs := testing.AllocsPerRun(20, func() {
		observeStepFinished(onlyCtx, "step", outputs, nil, false, SensitiveValues{})
	})
	require.Positive(t, only.told, "the observer was not told")

	copyingCtx := NewContextWithRunObserver(t.Context(), &withholdingCounter{})
	copyingAllocs := testing.AllocsPerRun(20, func() {
		observeStepFinished(copyingCtx, "step", outputs, nil, false, SensitiveValues{})
	})

	require.Less(t, onlyAllocs, 5.0, "a withholding-only observer's step copied its outputs")
	require.Greater(t, copyingAllocs, 100.0, "the copying observer's step did not copy, so this proves nothing")
}

// withholdingCounter is a [WithholdingRunObserver] that discards what it is
// told.
type withholdingCounter struct{}

func (withholdingCounter) StepFinished(string, *Node_Outputs, error, bool) {}
func (withholdingCounter) StepSkipped(string)                              {}
func (withholdingCounter) WaitStarted(string, string, time.Duration, bool) {}
func (withholdingCounter) StepFinishedWithholding(string, *Node_Outputs, error, bool, SensitiveValues) {
}
