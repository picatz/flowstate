package engine

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
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

// TestDurableStepTimeoutTypeOnlyPromotesRetryTimeout is the negative boundary
// around #1163's RetryState arm. These errors are reconstructed through the
// SDK's failure converter from the same wire shape the real-server test pins;
// changing only RetryState must change only the timeout case.
func TestDurableStepTimeoutTypeOnlyPromotesRetryTimeout(t *testing.T) {
	for _, test := range []struct {
		name       string
		retryState enums.RetryState
		kind       v1.ErrorKind
		imposed    bool
	}{
		{name: "schedule-to-close budget", retryState: enums.RETRY_STATE_TIMEOUT, kind: v1.ErrorKindUpstream, imposed: true},
		{name: "maximum attempts", retryState: enums.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, kind: v1.ErrorKindUpstream},
		{name: "non-retryable failure", retryState: enums.RETRY_STATE_NON_RETRYABLE_FAILURE, kind: v1.ErrorKindInvalidInput},
		{name: "unrelated application failure", retryState: enums.RETRY_STATE_UNSPECIFIED, kind: v1.ErrorKindUpstream},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := convertedActivityFailure(test.retryState, test.kind)

			timeoutType, imposed := durableStepTimeoutType(err)
			require.Equal(t, test.imposed, imposed)
			if imposed {
				require.Equal(t, enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutType)
				require.Equal(t, v1.ErrorKindTimeout, recordedStepKind(err))
				return
			}

			require.Equal(t, enums.TIMEOUT_TYPE_UNSPECIFIED, timeoutType)
			require.Same(t, err, durableStepTimeoutMessage(err, &v1.StepPolicy{}),
				"a non-timeout application failure must be left untouched")
			require.Equal(t, test.kind, recordedStepKind(err))
		})
	}
}

func convertedActivityFailure(retryState enums.RetryState, kind v1.ErrorKind) error {
	failure := &failurepb.Failure{
		Message: "activity failed",
		FailureInfo: &failurepb.Failure_ActivityFailureInfo{ActivityFailureInfo: &failurepb.ActivityFailureInfo{
			ActivityType: &commonpb.ActivityType{Name: "Task"},
			ActivityId:   "1",
			RetryState:   retryState,
		}},
		Cause: &failurepb.Failure{
			Message: "the dependency failed",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         kind.String(),
				NonRetryable: !kind.Retryable(),
			}},
		},
	}

	return temporal.GetDefaultFailureConverter().FailureToError(failure)
}

// TestRecordedStepKindKeepsANestedRunsClassification pins that adding the
// timeout branch did not displace the one that was already first: a
// classification made deeper in the walk — a loop iteration, a parallel branch
// — still wins over anything read off the wrapping around it.
func TestRecordedStepKindKeepsANestedRunsClassification(t *testing.T) {
	inner := &ErrRunFailed{Message: "inner", Kind: v1.ErrorKindPolicyDenied}

	require.Equal(t, v1.ErrorKindPolicyDenied, recordedStepKind(inner))
}

// preStepWork is what each of a run's pre-executor activities calls itself when
// it times out, paired with the word an operator would search for.
//
// Written out here rather than derived from the call sites, because that is the
// assertion: the sentence a person reads is the whole product of this
// translation, and a table generated from the code under test would agree with
// whatever the code said.
var preStepWork = []struct {
	name, what, names string
}{
	{
		name:  "vars",
		what:  "the workflow's vars: did not finish evaluating",
		names: "vars:",
	},
	{
		name:  "plugin admission",
		what:  "admitting the workflow's pinned plugins did not finish",
		names: "plugins",
	},
}

// TestPreStepTimeoutIsNotTheRunsExecutionTimeout is [preStepFailed]'s reason,
// asserted where it can be: these are the activities a run executes before its
// first step, and an untranslated timeout from one of them is what the server's
// fallback reads as the run's own execution budget expiring.
//
// The claim is two-sided. The message must name the work — an operator told
// "this run exceeded its execution timeout" goes looking for a run budget that
// did not fire — and the classification must stay the activity's, because
// nothing had run yet and the run-level kind says the opposite: that a completed
// prefix may have applied effects an operator has to weigh before restarting.
func TestPreStepTimeoutIsNotTheRunsExecutionTimeout(t *testing.T) {
	for _, work := range preStepWork {
		for _, timeoutType := range []enums.TimeoutType{
			enums.TIMEOUT_TYPE_START_TO_CLOSE,
			enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			enums.TIMEOUT_TYPE_SCHEDULE_TO_START,
			enums.TIMEOUT_TYPE_HEARTBEAT,
		} {
			t.Run(work.name+"/"+timeoutType.String(), func(t *testing.T) {
				// The envelope stood in for by `%w`, for the reason
				// [TestRecordedStepKindClassifiesEveryTimeout] gives: the SDK
				// exports no *ActivityError constructor, and both are a chain to
				// look through.
				err := preStepFailed(fmt.Errorf("activity error: %w",
					temporal.NewTimeoutError(timeoutType, nil)), work.what)

				var failed *ErrRunFailed
				require.ErrorAs(t, err, &failed,
					"an untranslated timeout here is what the server reads as the run's own")

				require.Contains(t, failed.Message, work.names,
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
}

// TestEveryPreStepActivityTranslatesItsTimeout is the link between the table
// above and the call sites, which the unit test cannot make on its own: a
// sentence asserted here proves nothing if [runWorkflow] or [admitPlugins]
// stopped passing it.
//
// Read syntactically, in the mold of callers_test.go next door in conformance —
// a pre-executor activity dispatched without going through [preStepFailed] is
// the defect this whole translation exists to prevent, and it arrives as a new
// call site rather than as a change to one of these.
func TestEveryPreStepActivityTranslatesItsTimeout(t *testing.T) {
	for _, work := range preStepWork {
		require.True(t,
			strings.Contains(sourceOf(t, "workflow.go")+sourceOf(t, "plugins.go"),
				`preStepFailed(err, "`+work.what+`")`),
			"no pre-step call site passes %q, so the sentence this test asserts is unreachable", work.what)
	}
}

// sourceOf reads one of this package's own files, for the syntactic check above.
func sourceOf(t *testing.T, name string) string {
	t.Helper()

	source, err := os.ReadFile(name)
	require.NoError(t, err)

	return string(source)
}

// TestVarsFailureKeepsItsOwnAccount is the negative direction, and the reason
// [preStepFailed] translates one shape rather than wrapping everything.
//
// A `vars:` expression that failed already crosses the activity boundary as an
// application error carrying its own kind and the sentence [v1.EvalVars] gives
// the local driver for the same file. Wrapping that would prepend a position
// the local driver does not, and re-classify a failure that classified itself —
// so it must reach the client exactly as it arrived.
func TestVarsFailureKeepsItsOwnAccount(t *testing.T) {
	err := activityError("vars", v1.NewTaskError("vars", v1.ErrorKindExpression,
		errors.New("no such key: missing")), false)

	require.Same(t, err, preStepFailed(err, "the workflow's vars: did not finish evaluating"),
		"a failure that classified itself must travel as itself")
	require.Equal(t, v1.ErrorKindExpression, recordedStepKind(preStepFailed(err, "the workflow's vars: did not finish evaluating")))
}

// TestVarsCancellationIsNotAFailure pins that a run somebody stopped while its
// `vars:` were being evaluated still reads as CANCELED.
//
// Temporal decides that from the error's type and [ErrRunFailed] formats a type
// away, so this is the assertion that fails if [preStepFailed] is ever widened to
// wrap more than the one shape it translates — [nodeFailed]'s own cancellation
// check being the backstop underneath if it is.
func TestVarsCancellationIsNotAFailure(t *testing.T) {
	canceled := temporal.NewCanceledError()

	require.Same(t, canceled, preStepFailed(canceled, "the workflow's vars: did not finish evaluating"))
	require.True(t, temporal.IsCanceledError(preStepFailed(canceled, "the workflow's vars: did not finish evaluating")),
		"a stopped run must not be reported as one that failed")
}
