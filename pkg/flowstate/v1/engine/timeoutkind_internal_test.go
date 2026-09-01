package engine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestRecordedStepKindClassifiesEveryTimeout covers [recordedStepKind]'s half
// of #915 directly, which the conformance corpus cannot reach on its own.
//
// `conformance.ErrorKindCases`'s timeout case runs both drivers against a real
// step `timeout:`, and durably that is a race the fixture usually wins: the
// task observes its own deadline and returns, so the failure crosses the
// activity boundary as an ApplicationError already carrying
// [v1.ErrorKindTimeout] from [v1.ClassifyError]. The other outcome of the same
// race — Temporal's own timer firing first and raising a
// [temporal.TimeoutError] at the workflow — is the one an operator meets when a
// task does not watch its context at all, and it is not a shape a test can ask
// for by timing. So it is asked for by construction here, which is what
// [temporal.NewTimeoutError] exists for ("only to support unit testing of
// workflows", per its own doc) and the trade [TestDurableStepTimeoutMessage]
// already makes next door for the identical reason.
//
// Every timeout type, not only the two a step's policy names. #915 reported
// StartToClose; the census behind it asked what the neighbours did, and
// schedule-to-start (a worker that never picked the attempt up) and heartbeat
// (a worker that stopped reporting progress) reached exactly the same default.
// [durableStepTimeoutMessage] declines to translate those two because there is
// no per-step budget value it could quote for them — but that is a fact about
// the sentence, not about the classification, and a kind that varied by timeout
// type would make "was this a defect in Flowstate" depend on which of four
// clocks ran out.
func TestRecordedStepKindClassifiesEveryTimeout(t *testing.T) {
	for _, timeoutType := range []enums.TimeoutType{
		enums.TIMEOUT_TYPE_START_TO_CLOSE,
		enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		enums.TIMEOUT_TYPE_SCHEDULE_TO_START,
		enums.TIMEOUT_TYPE_HEARTBEAT,
		enums.TIMEOUT_TYPE_UNSPECIFIED,
	} {
		t.Run(timeoutType.String(), func(t *testing.T) {
			bare := temporal.NewTimeoutError(timeoutType, nil)
			require.Equal(t, v1.ErrorKindTimeout, recordedStepKind(bare),
				"a %s timeout is the substrate ending the attempt on time, not a defect in Flowstate", timeoutType)

			// And wrapped, because a timeout never reaches this function bare:
			// it arrives inside Temporal's activity envelope, and
			// [durableStepTimeoutError] may be around that again. The SDK
			// exports no constructor for an *ActivityError — only
			// [temporal.NewTimeoutError] is exported for tests — so the
			// wrapping is stood in for by an ordinary `%w`, which is what both
			// of those are to `errors.As`: a chain this has to look through
			// rather than stop at.
			wrapped := fmt.Errorf("activity error: %w", bare)
			require.Equal(t, v1.ErrorKindTimeout, recordedStepKind(wrapped),
				"the envelope a timeout crosses the activity boundary in must not change what it is")
		})
	}
}

// TestRecordedStepKindPrefersTheTimeoutOverAStaleAttempt is the ordering half,
// and the reason the [temporal.TimeoutError] check sits above the
// [temporal.ApplicationError] one.
//
// A schedule-to-close budget that expires after a retryable failure wraps the
// last attempt's classified error as the outer timeout's *cause* — Temporal's
// own documented shape — and errors.As walks straight through the timeout to
// find it. Asking about an application error first therefore answers with the
// prior attempt's classification and hides that the budget is what ended the
// step, which is the identical trap [durableStepTimeoutMessage] documents for
// the message. The two now decide it the same way round, so an operator cannot
// be shown a sentence about the budget beside a kind about the dependency.
func TestRecordedStepKindPrefersTheTimeoutOverAStaleAttempt(t *testing.T) {
	stale := activityError("http", v1.NewTaskError("http", v1.ErrorKindUpstream, errors.New("503")), false)
	err := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, stale)

	require.Equal(t, v1.ErrorKindTimeout, recordedStepKind(err),
		"the budget that ended the step is the classification, not the last attempt it outlived")
}

// TestRecordedStepKindLeavesAClassifiedFailureAlone pins the other side of that
// ordering: nothing above the application-error branch may fire for a failure
// that never involved a Temporal timeout at all. A task's own account of why it
// failed is what [v1.ClassifyError] keeps ahead of any inference, and the
// timeout branch must not have quietly become an exception to that.
func TestRecordedStepKindLeavesAClassifiedFailureAlone(t *testing.T) {
	err := activityError("http", v1.NewTaskError("http", v1.ErrorKindUpstream, errors.New("503")), false)

	require.Equal(t, v1.ErrorKindUpstream, recordedStepKind(err))
}

// TestRecordedStepKindKeepsANestedRunsClassification pins that adding the
// timeout branch did not displace the one that was already first: a
// classification made deeper in the walk — a loop iteration, a parallel branch
// — still wins over anything read off the wrapping around it.
func TestRecordedStepKindKeepsANestedRunsClassification(t *testing.T) {
	inner := &ErrRunFailed{Message: "inner", Kind: v1.ErrorKindPolicyDenied}

	require.Equal(t, v1.ErrorKindPolicyDenied, recordedStepKind(inner))
}

// TestVarsTimeoutIsNotTheRunsExecutionTimeout is [varsFailed]'s reason, asserted
// where it can be: the run's `vars:` activity is the one activity whose timeout
// used to leave this driver untranslated, and the server's fallback reads an
// untranslated timeout as the run's own execution budget expiring.
//
// The claim is therefore two-sided. The message must name `vars:` — an operator
// told "this run exceeded its execution timeout" goes looking for a run budget
// that did not fire — and the classification must stay the activity's, because
// nothing had run yet and the run-level kind says the opposite: that a completed
// prefix may have applied effects an operator has to weigh before restarting.
func TestVarsTimeoutIsNotTheRunsExecutionTimeout(t *testing.T) {
	for _, timeoutType := range []enums.TimeoutType{
		enums.TIMEOUT_TYPE_START_TO_CLOSE,
		enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		enums.TIMEOUT_TYPE_SCHEDULE_TO_START,
		enums.TIMEOUT_TYPE_HEARTBEAT,
	} {
		t.Run(timeoutType.String(), func(t *testing.T) {
			// The envelope stood in for by `%w`, for the reason
			// [TestRecordedStepKindClassifiesEveryTimeout] gives: the SDK exports
			// no *ActivityError constructor, and both are a chain to look through.
			err := varsFailed(fmt.Errorf("activity error: %w",
				temporal.NewTimeoutError(timeoutType, nil)))

			var failed *ErrRunFailed
			require.ErrorAs(t, err, &failed,
				"an untranslated timeout here is what the server reads as the run's own")

			require.Contains(t, failed.Message, "vars:",
				"the sentence must name what timed out, kind=%s", timeoutType)
			require.NotContains(t, failed.Message, "execution timeout",
				"a run budget that did not fire must not be named, kind=%s", timeoutType)

			// And the kind the activity's own policy governs, not the permanent
			// run-level one. Retryable is the assertion that distinguishes them:
			// [v1.ErrorKindRunTimeout] is permanent by construction.
			require.Equal(t, v1.ErrorKindTimeout, failed.errorKind(), "kind=%s", timeoutType)
			require.True(t, failed.errorKind().Retryable(),
				"nothing had run, so this is the activity's timeout and not the run's")
		})
	}
}

// TestVarsFailureKeepsItsOwnAccount is the negative direction, and the reason
// [varsFailed] translates one shape rather than wrapping everything.
//
// A `vars:` expression that failed already crosses the activity boundary as an
// application error carrying its own kind and the sentence [v1.EvalVars] gives
// the local driver for the same file. Wrapping that would prepend a position
// the local driver does not, and re-classify a failure that classified itself —
// so it must reach the client exactly as it arrived.
func TestVarsFailureKeepsItsOwnAccount(t *testing.T) {
	err := activityError("vars", v1.NewTaskError("vars", v1.ErrorKindExpression,
		errors.New("no such key: missing")), false)

	require.Same(t, err, varsFailed(err),
		"a failure that classified itself must travel as itself")
	require.Equal(t, v1.ErrorKindExpression, recordedStepKind(varsFailed(err)))
}

// TestVarsCancellationIsNotAFailure pins that a run somebody stopped while its
// `vars:` were being evaluated still reads as CANCELED.
//
// Temporal decides that from the error's type and [ErrRunFailed] formats a type
// away, so this is the assertion that fails if [varsFailed] is ever widened to
// wrap more than the one shape it translates — [nodeFailed]'s own cancellation
// check being the backstop underneath if it is.
func TestVarsCancellationIsNotAFailure(t *testing.T) {
	canceled := temporal.NewCanceledError()

	require.Same(t, canceled, varsFailed(canceled))
	require.True(t, temporal.IsCanceledError(varsFailed(canceled)),
		"a stopped run must not be reported as one that failed")
}
