package conformance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What honoring a rate-limited response's Retry-After does, asked of both
// drivers at once — the fix ratified on #912: `httpExpectationMet` used to
// classify every 4xx, 429 included, as the permanent [v1.ErrorKindInvalidInput],
// so the delay it attached from the header two lines later
// (eval_task_http.go's httpExpectationMet) was parsed, carried, and never
// consulted, because both drivers gate [v1.RetryAfter] on
// [v1.ErrorKind.Retryable]. [v1.ErrorKindRateLimited] is retryable, so this
// fixture exercises the case through the *engine's* retry machinery rather
// than the http task, the same way [TotalTimeoutTaskDef] beside this file
// exercises `total_timeout:` without a task that actually blocks.
//
// The retry policy's own backoff is declared far longer than the delay the
// fixture's failure carries, on purpose: a driver that ignored
// [v1.TaskError.RetryAfter] and fell back to the policy's own interval would
// still retry and still succeed, so attempt count alone cannot tell honoring
// the header apart from ignoring it. What can is which duration the wait
// actually took — [AssertRateLimitDelayHonored] is that assertion, and it is
// meaningful only where a driver's clock is real; see its own doc for why the
// durable driver's case does not call it.

// RateLimitTaskName is the name [RateLimitTaskDef] registers under.
const RateLimitTaskName = "test.rate_limited"

// RateLimitRetryAfter is the delay the fixture's first failure carries as
// [v1.TaskError.RetryAfter] — short enough that a driver honoring it finishes
// this case quickly, and small next to [RateLimitPolicyInterval] so the two
// are easy to tell apart in an elapsed duration.
const RateLimitRetryAfter = 150 * time.Millisecond

// RateLimitPolicyInterval is the step's own `retry:` interval — an order of
// magnitude larger than [RateLimitRetryAfter], so a driver that fell back to
// it instead of the header's delay produces an elapsed duration this case can
// tell apart from one that honored the header.
const RateLimitPolicyInterval = 3 * time.Second

// RateLimitFailure is the text the fixture's first attempt fails with.
const RateLimitFailure = "rate limit exceeded, retry shortly"

// RateLimitTaskDef is a [v1.TaskDef] that fails its first attempt as
// [v1.ErrorKindRateLimited] carrying [RateLimitRetryAfter], then succeeds.
//
// Attempts are counted into the caller's counter, the same shape
// [TotalTimeoutTaskDef] uses and for the same reason: two drivers run this
// fixture in two test binaries, so package-level state would leak between
// whatever else a binary runs concurrently. observedAt receives the
// wall-clock instant the second attempt actually ran, as a Unix nanosecond
// timestamp — a driver whose retry loop blocks on a real clock (the local
// driver's caused context, per eval.go) reaches it at a time the caller can
// compare against when the run began; a driver that time-skips the wait (the
// durable test environment's virtual clock) reaches it almost immediately
// regardless of which delay was asked for, which is exactly why
// [AssertRateLimitDelayHonored] is not asked of that driver.
func RateLimitTaskDef(attempts *atomic.Int64, observedAt *atomic.Int64) v1.TaskDef {
	return v1.TaskDef{
		Name:    RateLimitTaskName,
		Summary: "test fixture failing its first attempt as RateLimited with a Retry-After-shaped delay, then succeeding",
		Fn: func(_ context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			n := attempts.Add(1)
			if n == 1 {
				return nil, &v1.TaskError{
					Task:       RateLimitTaskName,
					Kind:       v1.ErrorKindRateLimited,
					Err:        errors.New(RateLimitFailure),
					RetryAfter: RateLimitRetryAfter,
				}
			}

			observedAt.Store(time.Now().UnixNano())

			return &v1.Node_Outputs{}, nil
		},
	}
}

// RateLimitWorkflow builds the one-step workflow both drivers run: a step
// whose task fails as RateLimited once and whose own `retry:` interval is far
// longer than the delay that failure carries, so the run only completes
// quickly if the delay actually won.
func RateLimitWorkflow(workflowName, stepID string) *v1.Workflow {
	return &v1.Workflow{
		Name:    workflowName,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   stepID,
			Kind: &v1.Node_Task{Task: &v1.Task{Name: RateLimitTaskName}},
			Policy: &v1.StepPolicy{
				Retry: &v1.RetryPolicy{
					MaxAttempts:        3,
					InitialInterval:    durationpb.New(RateLimitPolicyInterval),
					BackoffCoefficient: 1,
				},
			},
		}},
	}
}

// AssertRateLimitRetried is the shared assertion that a rate-limited failure
// was retried rather than treated as permanent — the defect itself, since
// [v1.ErrorKindInvalidInput] would have ended the step on its first attempt
// and left this fixture's second branch unreached.
func AssertRateLimitRetried(t *testing.T, driver string, attempts int64) {
	t.Helper()

	if attempts < 2 {
		t.Fatalf("%s ended the step after %d attempt(s) — a 429 classified RateLimited must be retried, "+
			"not treated as the permanent InvalidInput it used to be classified as (#912)", driver, attempts)
	}
}

// AssertRateLimitDelayHonored is the shared assertion that the wait before
// the second attempt was [RateLimitRetryAfter], not [RateLimitPolicyInterval]
// — meaningful only on a driver whose retry loop blocks on a real clock.
//
// Only the local driver qualifies. The durable driver's retry backoff is a
// Temporal timer, and the test environment both driver callers use
// ([testsuite.WorkflowTestSuite]) auto-skips virtual time for timers, so an
// elapsed wall-clock duration there is near zero whether or not the header's
// delay actually won — the same flakiness this package's other timing-shaped
// assertions ([AssertTotalTimeoutEndedTheStep]) avoid by counting attempts
// instead. The durable half of "did the delay actually win" is asserted at
// the mechanism instead: engine's Test_activityError_retryAfter (extended for
// [v1.ErrorKindRateLimited] alongside its existing Upstream case) pins that
// [v1.RetryAfter] on a RateLimited failure becomes the Temporal
// ApplicationError's NextRetryDelay — the value the real substrate schedules
// the next attempt from, whatever the test environment's own clock does with
// it.
func AssertRateLimitDelayHonored(t *testing.T, driver string, startedAt time.Time, observedAt int64) {
	t.Helper()

	if observedAt == 0 {
		t.Fatalf("%s never reached the fixture's second attempt", driver)
	}

	elapsed := time.Unix(0, observedAt).Sub(startedAt)

	// A generous floor: real scheduling can run a hair early relative to the
	// instant the caller captured before starting the run, but never so early
	// that RateLimitRetryAfter's wait plainly did not happen.
	if floor := RateLimitRetryAfter / 2; elapsed < floor {
		t.Errorf("%s retried after %s, less than %s — that is faster than the header's Retry-After, as though "+
			"the delay were not applied at all", driver, elapsed, floor)
	}

	// A ceiling well under RateLimitPolicyInterval: honoring the header takes
	// roughly RateLimitRetryAfter, and falling back to the step's own `retry:`
	// interval would take RateLimitPolicyInterval instead — twenty times
	// longer, so any reasonable scheduling jitter still lands well inside this
	// ceiling only in the honored case.
	if ceiling := RateLimitPolicyInterval / 2; elapsed > ceiling {
		t.Errorf("%s retried after %s, close to its %s retry: interval rather than the %s the failure's "+
			"Retry-After asked for — the policy's own backoff won instead of the header (#912)",
			driver, elapsed, RateLimitPolicyInterval, RateLimitRetryAfter)
	}
}
