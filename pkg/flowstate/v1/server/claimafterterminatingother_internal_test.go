package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #2061's second review round: the terminate-then-claim split this file
// makes is deliberately not atomic (see [FlowstateServer.claimAfterTerminatingOther]'s
// own doc), and these pin the three properties that keeps that safe —
// checked directly against a scripted client, without a real Temporal
// namespace, since the property under test is which errors this code
// retries and which it does not, not what Temporal itself does with a
// terminate or a start.

// fakeWorkflowRun is the minimal [client.WorkflowRun] [claimStart] and
// [FlowstateServer.claimAfterTerminatingOther] read from: only GetRunID is
// ever called on the value either returns.
type fakeWorkflowRun struct{ runID string }

func (r fakeWorkflowRun) GetID() string                  { return "" }
func (r fakeWorkflowRun) GetRunID() string               { return r.runID }
func (r fakeWorkflowRun) GetFirstExecutionRunID() string { return r.runID }
func (r fakeWorkflowRun) Get(context.Context, any) error { return nil }
func (r fakeWorkflowRun) GetWithOptions(context.Context, any, client.WorkflowRunGetOptions) error {
	return nil
}

// fakeClaimClient scripts [client.Client.TerminateWorkflow] and
// [client.Client.ExecuteWorkflow] by call index: the Nth call answers
// errs[N] (or nil past the end of errs, meaning "succeed from here on").
// Everything else panics through the embedded nil client.Client, which is
// the point — a test that reaches for a call this file does not script is a
// test whose fixture no longer matches what it means to exercise.
type fakeClaimClient struct {
	client.Client

	terminateErrs []error
	terminateRuns []string // runID this call was asked to terminate, in order

	// executeErrs answers the Nth ExecuteWorkflow call by call index — an
	// error, or nil for a success. executeCalls counts every call regardless
	// of outcome; executeSuccessRunIDs answers the Kth *successful* call, in
	// order, separately from call index, since a retried transient failure
	// consumes a call index a success run id must not also be consumed by.
	executeErrs          []error
	executeSuccessRunIDs []string
	executeCalls         int
	executeSuccesses     int
}

func (c *fakeClaimClient) TerminateWorkflow(_ context.Context, _, runID, _ string, _ ...any) error {
	i := len(c.terminateRuns)
	c.terminateRuns = append(c.terminateRuns, runID)
	if i < len(c.terminateErrs) {
		return c.terminateErrs[i]
	}

	return nil
}

func (c *fakeClaimClient) ExecuteWorkflow(context.Context, client.StartWorkflowOptions, any, ...any) (client.WorkflowRun, error) {
	i := c.executeCalls
	c.executeCalls++
	if i < len(c.executeErrs) && c.executeErrs[i] != nil {
		return nil, c.executeErrs[i]
	}

	runID := "run"
	if c.executeSuccesses < len(c.executeSuccessRunIDs) {
		runID = c.executeSuccessRunIDs[c.executeSuccesses]
	}
	c.executeSuccesses++

	return fakeWorkflowRun{runID: runID}, nil
}

// TestClaimStartDoesNotRetryAPermanentFailure is #2061's finding 2: a start
// that fails for a reason no amount of waiting fixes — an invalid argument,
// here — must not be retried for the better part of
// [claimAfterTerminatingOtherTimeout] before surfacing it.
func TestClaimStartDoesNotRetryAPermanentFailure(t *testing.T) {
	t.Parallel()

	fake := &fakeClaimClient{executeErrs: []error{serviceerror.NewInvalidArgument("bad specification")}}

	_, err := claimStart(t.Context(), fake, client.StartWorkflowOptions{}, &v1.RunState{})
	require.Error(t, err)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	require.Equal(t, 1, fake.executeCalls, "a permanent failure was retried instead of surfaced immediately")
}

// TestClaimStartRetriesATransientFailureThenSucceeds is the direction
// TestClaimStartDoesNotRetryAPermanentFailure's fix must not remove: a
// deadline that elapsed on one attempt is still worth trying again.
func TestClaimStartRetriesATransientFailureThenSucceeds(t *testing.T) {
	t.Parallel()

	fake := &fakeClaimClient{
		executeErrs:          []error{serviceerror.NewDeadlineExceeded("slow attempt")},
		executeSuccessRunIDs: []string{"claimed"},
	}

	run, err := claimStart(t.Context(), fake, client.StartWorkflowOptions{}, &v1.RunState{})
	require.NoError(t, err)
	require.Equal(t, "claimed", run.GetRunID())
	require.Equal(t, 2, fake.executeCalls, "the transient failure should have been retried exactly once before succeeding")
}

// TestClaimAfterTerminatingOtherProceedsPastAnAmbiguousTerminateError is
// #2061's finding 1: a terminate call whose own outcome is unknown — the
// per-call deadline elapsing well inside this function's own bound, in
// particular — must not stop the claim that follows. FAIL can only
// discover what still holds the id, never destroy it, so attempting it
// regardless is safe whether or not the terminate actually landed.
func TestClaimAfterTerminatingOtherProceedsPastAnAmbiguousTerminateError(t *testing.T) {
	t.Parallel()

	fake := &fakeClaimClient{
		terminateErrs:        []error{serviceerror.NewDeadlineExceeded("terminate answered late")},
		executeSuccessRunIDs: []string{"claimed"},
	}
	s := mustNew(t, fake)
	submission := submissionFor(t, "", "req", nil)

	resp, err := s.claimAfterTerminatingOther(t.Context(), fake, submission.workflowID, "incumbent-run",
		client.StartWorkflowOptions{}, &v1.RunState{}, submission, true)
	require.NoError(t, err, "an ambiguous terminate outcome stopped the claim rather than attempting it")
	require.Equal(t, "claimed", resp.GetRunId())
	require.False(t, resp.GetReused())
	require.Equal(t, 1, fake.executeCalls, "the claim should have been attempted exactly once, right after the ambiguous terminate")
}

// TestClaimAfterTerminatingOtherAbortsOnAPermanentTerminateError is the
// direction the fix above must not remove: a terminate call refused before
// Temporal ever acted on it — no permission to issue it, here — is this
// server's own failure to report, and the claim that would otherwise follow
// is never attempted.
func TestClaimAfterTerminatingOtherAbortsOnAPermanentTerminateError(t *testing.T) {
	t.Parallel()

	fake := &fakeClaimClient{
		terminateErrs: []error{serviceerror.NewPermissionDenied("not allowed", "")},
	}
	s := mustNew(t, fake)
	submission := submissionFor(t, "", "req", nil)

	_, err := s.claimAfterTerminatingOther(t.Context(), fake, submission.workflowID, "incumbent-run",
		client.StartWorkflowOptions{}, &v1.RunState{}, submission, true)
	require.Error(t, err)
	require.Zero(t, fake.executeCalls, "a permanently failed terminate should never reach the claim attempt")
}

// TestClaimAfterTerminatingOtherRefusesAnAlreadyCancelledCaller is the
// advisory guard: a caller already gone before anything has been terminated
// is refused the ordinary way, rather than this function paying for a
// bounded, uncancellable commitment nobody is waiting to hear the answer to.
func TestClaimAfterTerminatingOtherRefusesAnAlreadyCancelledCaller(t *testing.T) {
	t.Parallel()

	fake := &fakeClaimClient{}
	s := mustNew(t, fake)
	submission := submissionFor(t, "", "req", nil)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := s.claimAfterTerminatingOther(ctx, fake, submission.workflowID, "incumbent-run",
		client.StartWorkflowOptions{}, &v1.RunState{}, submission, true)
	require.Error(t, err)
	require.Empty(t, fake.terminateRuns, "an already-cancelled caller should never reach the terminate call")
	require.Zero(t, fake.executeCalls)
}
