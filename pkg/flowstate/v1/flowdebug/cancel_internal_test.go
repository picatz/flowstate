package flowdebug

import (
	"context"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestCancellationDuringAConditionIsNotADecline (Codex, #1116).
//
// An errored condition declines the arrival and lets the run carry on, which
// is right for a condition this occurrence cannot answer. Cancellation is not
// that: the operator interrupted the run *while* the condition was evaluating
// — which is exactly when they would, since a costly condition is what makes
// the wait long enough to interrupt.
//
// Swallowed as a decline, `BeforeStep` returns nil and `runNodes` walks into
// the step after the cancel: there is no `ctx.Err()` check between
// `debuggerBeforeStep` and the step's work (`eval.go:1350`). That is the one
// thing an interrupt must not do, and it disagreed with cancellation at the
// prompt, which unwinds immediately.
//
// Tested at `breakpointHolds` rather than through a run, deliberately.
// Cancelling before the run starts makes everything fail for other reasons —
// which is how the first version of this test passed against the defect — and
// cancelling *during* an evaluation is not something a test can time
// deterministically. What decides the behaviour is this branch, so this is
// where it is asked.
func TestCancellationDuringAConditionIsNotADecline(t *testing.T) {
	t.Parallel()

	scope := v1.NewScope(v1.CurrentProfile, nil)

	condition, err := compileCondition("1 == 1", scope)
	if err != nil {
		t.Fatal(err)
	}

	var out strings.Builder
	session, err := New(Options{Out: &out})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	holds, err := session.breakpointHolds(ctx, "body", breakpoint{condition: condition}, scope)

	if err == nil {
		t.Fatalf("a cancelled context must not read as an unanswerable condition (holds=%v)", holds)
	}
	if !isCancellation(err) {
		t.Errorf("want the cancellation propagated, got %v", err)
	}
	if holds {
		t.Error("and it does not hold the run either; the caller unwinds on the error")
	}
	if printed := out.String(); strings.Contains(printed, "could not be evaluated here") {
		t.Errorf("a cancel is not an unanswerable condition and must not be reported as one: %q", printed)
	}
}

// isCancellation is context.Canceled, however the evaluator wrapped it.
func isCancellation(err error) bool {
	return strings.Contains(err.Error(), context.Canceled.Error())
}
