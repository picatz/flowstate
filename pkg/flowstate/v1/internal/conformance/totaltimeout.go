package conformance

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a step's `total_timeout:` does, asked of both drivers at once.
//
// [StepTimeoutTaskDef] beside this file pins the *per-attempt* bound: that a
// step's `timeout:` has become a deadline on the context a task runs under.
// This is the step's other clock — the wall-time budget across every attempt
// and every wait between them — and it is the one an author could not write
// down until #920. The bound itself is older than the key: every step has been
// bounded at [v1.DefaultScheduleToCloseTimeout] on both drivers since the
// defaults were reconciled, and what landed with the key is the ability to
// *say* what the budget is.
//
// Two drivers, two entirely different mechanisms for it. The durable driver
// passes it to Temporal as the activity's ScheduleToCloseTimeout and Temporal
// fails the activity server-side; the local driver wraps its own retry loop in
// a [context.WithTimeoutCause] (eval.go's runStepWithPolicy). Neither knows
// about the other, and the only thing holding them together is that both read
// [v1.StepTimeoutsFor] — which is exactly the "one meaning, written down twice"
// shape CLAUDE.md's both-drivers section is about, one function away from being
// two constants that disagree.
//
// So the case is written as the disagreement it is guarding against. A step
// whose budget is far shorter than its attempt list must end **on the budget**,
// with most of that list unspent — and a driver that quietly ignored the key
// would not fail visibly, it would spend the whole list and record the
// dependency's own failure, which reads exactly like an ordinary exhausted
// retry. So what is counted is attempts.
//
// # What the recorded sentence may not be held to
//
// Not the failure text, and that is deliberate rather than a gap.
// [v1.StepTimeouts]' own doc explains it at length: Temporal times the activity
// out server-side and hands back its own failure, while locally the budget
// arrives as a context deadline carrying the cause runStepWithPolicy named —
// and `engine.durableStepTimeoutError` pointedly leaves the *recorded*
// `${steps.<id>.error}` value as Temporal's own, translating only the
// run-level message. Holding both drivers to one string would mean inventing a
// transport-shaped sentence in the package that exists to keep transports out.
// What both can be held to is what an author can act on: the step fails, a
// `continue_on_error:` tolerates it, the failure is readable, and the budget
// stops the pending retry with most of the fifty-attempt list unspent.

// TotalTimeoutTaskName is the name [TotalTimeoutTaskDef] registers under.
const TotalTimeoutTaskName = "test.total_timeout"

// TotalTimeoutFailure is the text [TotalTimeoutTaskDef] fails every attempt
// with — the shape of a dependency that is down for longer than the step's
// budget, which is the situation `total_timeout:` exists for.
const TotalTimeoutFailure = "the dependency is still refusing"

// TotalTimeoutBudget is the `total_timeout:` [TotalTimeoutWorkflow] declares.
//
// Small enough that the local driver — which spends its backoff on a real
// clock — reaches it in well under a second. A real Temporal server may leave
// the first retry pending until this budget expires (the server, unlike
// testsuite, does not auto-fire this sub-second retry); retry_timeout_e2e_test.go
// pins that its resulting RetryState is TIMEOUT rather than attempt exhaustion.
const TotalTimeoutBudget = 300 * time.Millisecond

// TotalTimeoutAttempt is the `timeout:` beside it: one attempt, well inside the
// budget.
//
// Shorter than [TotalTimeoutBudget] because the compiler refuses the other way
// round — a total that expires inside the first attempt allows none — and this
// fixture must be a file an author could legally write.
const TotalTimeoutAttempt = 50 * time.Millisecond

// TotalTimeoutRetryInterval is the wait between attempts.
//
// With [TotalTimeoutAttempts] attempts allowed, exhausting the retry list takes
// several seconds; the budget is 300ms. That gap is the case: the step must end
// on the budget with most of its attempts unused.
const TotalTimeoutRetryInterval = 100 * time.Millisecond

// TotalTimeoutAttempts is how many attempts the step's `retry:` allows — far
// more than the budget can pay for, which is the whole point.
const TotalTimeoutAttempts = 50

// TotalTimeoutAttemptCeiling is how many attempts a driver honouring the budget
// may have spent by the time it expires.
//
// The arithmetic says four: attempts at 0, 100ms, 200ms and 300ms, the last of
// which is the budget. The ceiling is well above that and far below
// [TotalTimeoutAttempts], because what is being distinguished is "stopped on
// the clock" from "spent the list" — a loaded machine may reach the budget in
// fewer attempts and must not fail for it, while a driver ignoring the key
// overshoots by an order of magnitude.
const TotalTimeoutAttemptCeiling = 10

// TotalTimeoutTaskDef is a [v1.TaskDef] that fails every attempt with a
// retryable error, immediately.
//
// Retryable rather than permanent so the retry loop actually runs: a permanent
// failure ends the step on its first attempt and the budget never fires.
// Immediate rather than blocking so that what is under test is the budget
// across attempts rather than any single attempt's own deadline — a fixture
// that hung would end each attempt on `timeout:` and make the two clocks hard
// to tell apart in the result.
//
// It counts its attempts into the caller's counter, which is the observation the
// case rests on. A counter passed in rather than held in this package because
// two drivers run this fixture in two test binaries, and a package-level one
// would be shared state between whatever else a binary runs concurrently.
func TotalTimeoutTaskDef(attempts *atomic.Int64) v1.TaskDef {
	return v1.TaskDef{
		Name:    TotalTimeoutTaskName,
		Summary: "test fixture failing every attempt, so a step ends on its total_timeout: budget",
		Fn: func(_ context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			attempts.Add(1)
			return nil, v1.NewTaskError(TotalTimeoutTaskName, v1.ErrorKindUpstream, errors.New(TotalTimeoutFailure))
		},
	}
}

// TotalTimeoutWorkflow builds the one-step workflow both drivers run: a step
// declaring both clocks and a retry list it cannot afford, tolerated so the run
// completes and the failure is readable as an ordinary value.
//
// `continue_on_error:` is not decoration here. A budget expiring is an ordinary
// step failure on both drivers rather than a cancellation of the run, and that
// claim is only checkable if a tolerated one lets the run finish and record what
// happened — which is the same thing the durable driver's ScheduleToClose and
// the local driver's caused context deadline each have to get right separately.
func TotalTimeoutWorkflow(workflowName, stepID string) *v1.Workflow {
	return &v1.Workflow{
		Name:    workflowName,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   stepID,
			Kind: &v1.Node_Task{Task: &v1.Task{Name: TotalTimeoutTaskName}},
			Policy: &v1.StepPolicy{
				Timeout:      durationpb.New(TotalTimeoutAttempt),
				TotalTimeout: durationpb.New(TotalTimeoutBudget),
				Retry: &v1.RetryPolicy{
					MaxAttempts:        TotalTimeoutAttempts,
					InitialInterval:    durationpb.New(TotalTimeoutRetryInterval),
					BackoffCoefficient: 1,
					MaxInterval:        durationpb.New(TotalTimeoutRetryInterval),
				},
				ContinueOnError: true,
			},
		}},
	}
}

// TotalTimeoutFailureWorkflow is [TotalTimeoutWorkflow] without tolerance, so
// each driver exposes the classification and run-level message for the same
// budget expiry the tolerated case records as a step output.
func TotalTimeoutFailureWorkflow(workflowName, stepID string) *v1.Workflow {
	wf := TotalTimeoutWorkflow(workflowName, stepID)
	wf.Steps[0].Policy.ContinueOnError = false
	return wf
}

// TotalTimeoutExhaustionWorkflow fails after one attempt, before its generous
// overall budget can lapse. It is the negative case: the dependency failure,
// not total_timeout:, must remain the terminal fact.
func TotalTimeoutExhaustionWorkflow(workflowName, stepID string) *v1.Workflow {
	wf := TotalTimeoutFailureWorkflow(workflowName, stepID)
	wf.Steps[0].Policy.Retry.MaxAttempts = 1
	wf.Steps[0].Policy.TotalTimeout = durationpb.New(5 * time.Second)
	return wf
}

// AssertTotalTimeoutEndedTheStep is the shared assertion over what
// [TotalTimeoutWorkflow]'s step recorded, so both drivers are held to one
// wording of the claim rather than to two.
func AssertTotalTimeoutEndedTheStep(t *testing.T, driver string, outputs *v1.Node_Outputs, attempts int64) {
	t.Helper()

	recorded := outputs.GetNamedValues()[v1.StepErrorOutput].GetLiteral().GetStringValue()
	if recorded == "" {
		t.Fatalf("%s recorded no failure for a step whose %s total_timeout: cannot pay for %d attempts "+
			"%s apart; a tolerated budget expiry has to be readable as ${steps.<id>.%s} on both drivers",
			driver, TotalTimeoutBudget, TotalTimeoutAttempts, TotalTimeoutRetryInterval, v1.StepErrorOutput)
	}
	if !strings.Contains(recorded, TotalTimeoutFailure) {
		t.Errorf("%s discarded the last attempt's dependency failure when the overall budget ended the step: %q",
			driver, recorded)
	}

	if attempts < 1 {
		t.Errorf("%s ended the step without running its dependency once; a timeout before any attempt does not exercise a budget across retries",
			driver)
	}

	if attempts > TotalTimeoutAttemptCeiling {
		t.Errorf("%s spent %d attempts on a step whose %s total_timeout: allows about four; a driver that "+
			"honours the budget stops on the clock with most of its %d-attempt list unspent, and one that "+
			"ignores the key spends the list and looks exactly like an ordinary exhausted retry",
			driver, attempts, TotalTimeoutBudget, TotalTimeoutAttempts)
	}
}

// AssertTotalTimeoutFailure checks the operator-facing half of the same shared
// case: the configured overall budget is the terminal fact and therefore owns
// both the Timeout classification and the run-level sentence. The tolerated
// assertion above independently pins that the dependency failure underneath it
// was preserved rather than replaced.
func AssertTotalTimeoutFailure(t *testing.T, driver string, kind v1.ErrorKind, message string) {
	t.Helper()

	if kind != v1.ErrorKindTimeout {
		t.Errorf("%s classified a lapsed %s total_timeout: as %q, want %q",
			driver, TotalTimeoutBudget, kind, v1.ErrorKindTimeout)
	}
	if !strings.Contains(message, TotalTimeoutBudget.String()) {
		t.Errorf("%s did not report the configured %s overall budget in its terminal message: %q",
			driver, TotalTimeoutBudget, message)
	}
}

// AssertTotalTimeoutLeavesAttemptExhaustionAlone is the negative counterpart:
// a retryable application failure whose attempt limit arrives first is still
// the dependency's Upstream failure, with no overall-budget relabelling.
func AssertTotalTimeoutLeavesAttemptExhaustionAlone(t *testing.T, driver string, kind v1.ErrorKind, message string) {
	t.Helper()

	if kind != v1.ErrorKindUpstream {
		t.Errorf("%s relabelled ordinary attempt exhaustion as %q, want %q", driver, kind, v1.ErrorKindUpstream)
	}
	if !strings.Contains(message, TotalTimeoutFailure) {
		t.Errorf("%s discarded the dependency failure on ordinary attempt exhaustion: %q", driver, message)
	}
	if strings.Contains(message, "overall budget") || strings.Contains(message, "schedule-to-close") {
		t.Errorf("%s reported an overall-budget timeout that did not happen: %q", driver, message)
	}
}

// AssertTotalTimeoutSuppressesWidening pins the one behavioral decision the key
// carries, on whichever driver asks.
//
// [v1.StepTimeoutsFor] widens the overall bound when a declared `timeout:`
// multiplied by the attempts allowed would not fit inside it — otherwise a step
// declaring a long `timeout:` would be cut short by a ceiling derived from
// defaults rather than by its own policy. An explicit `total_timeout:`
// suppresses that: a budget the engine silently extends is not a budget.
//
// Asserted through the function rather than through a run, and asked of both
// drivers anyway, because the function *is* the agreement. Widening only shows
// itself where `timeout:` × attempts exceeds [v1.DefaultScheduleToCloseTimeout],
// which is ten minutes — a run long enough to observe it directly is one the
// local driver would have to spend on a real clock. What both drivers can be
// held to is that each one derives its bound from this call and gets the
// declared number back, which is what a caller per driver records.
func AssertTotalTimeoutSuppressesWidening(t *testing.T, driver string) {
	t.Helper()

	// `timeout:` × attempts is twenty minutes against a ten minute default, so
	// the widening this suppresses is unambiguously in play.
	widening := &v1.StepPolicy{
		Timeout: durationpb.New(2 * time.Minute),
		Retry:   &v1.RetryPolicy{MaxAttempts: 10},
	}

	if got := v1.StepTimeoutsFor(widening, v1.DefaultStepTimeouts()).ScheduleToClose; got <= v1.DefaultScheduleToCloseTimeout {
		t.Fatalf("%s: the fixture no longer exercises widening — a 2m timeout: over 10 attempts left the "+
			"overall bound at %s, so the suppression below would pass without suppressing anything", driver, got)
	}

	declared := &v1.StepPolicy{
		Timeout:      durationpb.New(2 * time.Minute),
		TotalTimeout: durationpb.New(5 * time.Minute),
		Retry:        &v1.RetryPolicy{MaxAttempts: 10},
	}

	if got := v1.StepTimeoutsFor(declared, v1.DefaultStepTimeouts()).ScheduleToClose; got != 5*time.Minute {
		t.Errorf("%s: a step declaring total_timeout: 5m got an overall bound of %s — an explicit budget "+
			"must win outright over the timeout:-times-attempts widening, or the engine is extending a "+
			"deadline its author chose", driver, got)
	}
}
