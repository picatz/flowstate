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

	err := actOnRunError("terminating", "wf-1", serviceerror.NewNotFound("workflow execution already completed"))

	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "already finished")
	assert.Contains(t, err.Error(), "omit the run id")
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

			err := actOnRunError("cancelling", "wf-2", cause)

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
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(actOnRunError("signalling", "wf-3", wrapped)))
}
