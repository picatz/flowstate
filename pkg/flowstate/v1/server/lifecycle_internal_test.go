package server

import (
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
)

// The classifier is tested here as well as through Temporal, because the two
// checks answer different questions and neither covers the other.
//
// The integration test proves the *assumption*: that Temporal really does refuse
// a finished execution with a NotFound, which is a fact about Temporal that no
// unit test can establish. This proves the *branch*: that a NotFound becomes
// FailedPrecondition and everything else stays Internal.
//
// Splitting them was not the first plan. The integration test claimed to cover
// both directions on the strength of a terminate that succeeds — but a request
// that succeeds never reaches the classifier, so making every error
// FailedPrecondition left it green. An "other direction" test that passes under
// the mutation it names is worth less than none, because it is read as coverage.

// TestActOnRunErrorMapsAFinishedExecution covers the branch that exists.
func TestActOnRunErrorMapsAFinishedExecution(t *testing.T) {
	t.Parallel()

	err := actOnRunError("terminating", "wf-1", "run-1", serviceerror.NewNotFound("workflow execution already completed"))

	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "already finished")
	assert.Contains(t, err.Error(), "retry without it")
	assert.Contains(t, err.Error(), "wf-1", "the refusal does not name the workload it is about")
}

// TestActOnRunErrorLeavesEverythingElseInternal is the direction the integration
// test could not reach.
//
// A classifier that answered FailedPrecondition for everything would turn a real
// server fault — a broken connection, a Temporal outage — into a code that tells
// an operator the request was the problem and retrying will not help. That is the
// expensive direction to be wrong in, and it is the one a happy-path test cannot
// see.
func TestActOnRunErrorLeavesEverythingElseInternal(t *testing.T) {
	t.Parallel()

	for name, cause := range map[string]error{
		"an unclassified failure": errors.New("connection reset"),
		"an unavailable service":  serviceerror.NewUnavailable("frontend is down"),
		"a deadline":              serviceerror.NewDeadlineExceeded("took too long"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := actOnRunError("cancelling", "wf-2", "run-2", cause)

			assert.Equal(t, connect.CodeInternal, connect.CodeOf(err),
				"%s was reported as %s, which tells an operator not to retry something they should",
				name, connect.CodeOf(err))
			assert.ErrorIs(t, err, cause, "the cause is not wrapped, so nothing downstream can inspect it")
		})
	}
}

// TestActOnRunErrorFindsAWrappedNotFound checks the match survives wrapping.
//
// Matched with errors.As rather than a type assertion because the SDK is free to
// wrap, and a classifier that only recognizes the bare type fails open — back to
// a 500 — the first time it does.
func TestActOnRunErrorFindsAWrappedNotFound(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("calling temporal: %w", serviceerror.NewNotFound("workflow execution already completed"))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(actOnRunError("signalling", "wf-3", "run-3", wrapped)))
}

// TestActOnRunErrorDoesNotAdviseOmittingAnIDThatWasNotGiven covers the caller who
// already did the thing the advice describes.
//
// The first version of this message told every caller to omit the run id, because
// the classifier was never given one to look at. A caller who had omitted it was
// therefore instructed to omit it — advice that cannot be followed, about a field
// they did not use, in place of the one fact that was true: the workload is done.
//
// Reproduced against a real Temporal before it was fixed, since it is a message
// only reachable through a run that has genuinely finished.
func TestActOnRunErrorDoesNotAdviseOmittingAnIDThatWasNotGiven(t *testing.T) {
	t.Parallel()

	err := actOnRunError("terminating", "wf-4", "", serviceerror.NewNotFound("workflow execution already completed"))

	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "already finished")
	assert.NotContains(t, err.Error(), "run id",
		"a caller who supplied no run id is told about the run id they did not supply")
	assert.NotContains(t, err.Error(), "continues as new",
		"a caller who pinned nothing is given an explanation about pinning")
}

// TestActOnRunErrorOffersTheRemedyWithoutPromisingIt is the wording, checked.
//
// Omitting the id reaches a current segment only when there *is* one. A workload
// that has genuinely finished answers the same NotFound, and this cannot tell the
// two apart without asking Temporal a second question whose answer would already
// be stale. So the message has to carry both readings — a remedy stated as a
// certainty would send an operator round the same loop believing the tool.
func TestActOnRunErrorOffersTheRemedyWithoutPromisingIt(t *testing.T) {
	t.Parallel()

	err := actOnRunError("terminating", "wf-5", "run-5", serviceerror.NewNotFound("workflow execution already completed"))

	assert.Contains(t, err.Error(), "retry without it", "the message does not say what to try")
	assert.Contains(t, err.Error(), "the workload itself is done",
		"the message promises the retry will work, and it may not")
}
