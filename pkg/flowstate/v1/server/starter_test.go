package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Who started a run has been recorded on every run since `distinct_from_starter`
// shipped, and no RPC answered with it. So a workflow could declare "the approver
// may not be whoever asked for this", the server could enforce it exactly, and
// nobody outside the server could see the fact being compared against: an
// operator refused a `flow signal` had no way to learn they were being refused
// for being the starter.
//
// These are that fact travelling the whole path a capability has to reach before
// anyone can use it - the memo, the handler, the wire and the schema - with the
// unauthenticated direction beside it, because "no starter recorded" has to stay
// a distinguishable answer rather than becoming a placeholder somebody compares
// against.

// TestGetReportsWhoStartedTheRun is the feature, end to end through the RPC.
//
// The subject is asserted in its qualified `issuer#subject` form on purpose:
// that is the form a `signals:` rule names and the form
// [v1.SignalPolicyCheck] compares, so this pins the decision that the field
// carries the comparable string rather than a rendered display form. A test
// asserting only that the subject appears somewhere in it would pass for a
// prettier answer that no rule could be checked against.
func TestGetReportsWhoStartedTheRun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "requester@example.com",
	})

	started, err := fixture.teamA.Run(ctx, connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	got, err := fixture.teamA.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)

	assert.Equal(t,
		v1.QualifiedSubject("https://issuer.example.com", "requester@example.com"),
		got.Msg.GetStarter(),
		"a run reported a starter that is not the qualified subject a signal policy names, so a "+
			"surface holding this cannot compare it to the rule that would refuse the sender")

	// Terminated rather than left parked for its whole gate timeout, since the
	// fixture's worker is shared with every other test in this package.
	_, err = fixture.teamA.Terminate(ctx, connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)
}

// TestGetReportsTheStarterOnATerminalRun is the other half of "populated in the
// Get handler": the handler answers a run's status from one of four branches,
// and a field set in the running one and forgotten in the others would be a
// starter that disappears the moment the run somebody is asking about finishes.
//
// A starter is a fact about the submission, so it is as true of a finished run
// as of a running one - and a finished run is exactly when an auditor asks.
func TestGetReportsTheStarterOnATerminalRun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "requester@example.com",
	})

	started, err := fixture.teamA.Run(ctx, connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name: "finishes-immediately",
			Steps: []*v1.Node{{
				Id: "say",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("done")},
				}},
			}},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, gerr := fixture.teamA.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if gerr != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp

		return true
	}, 60*time.Second, 200*time.Millisecond, "the run never completed")

	assert.Equal(t,
		v1.QualifiedSubject("https://issuer.example.com", "requester@example.com"),
		final.Msg.GetStarter(),
		"a completed run forgot who started it, so the answer an auditor asks for is exactly the "+
			"one this field stops carrying")
}

// TestGetOnARunWithNoAttestedStarterReportsNoSubject is the negative direction,
// and the reason there is no compatibility arm.
//
// An unauthenticated caller - only possible in a development deployment - is the
// closest a live server gets to a run with nothing to report, and what comes back
// must not be something a reader could mistake for a person. The memo is written
// unconditionally (see starterMemoEntry), so this is the recorded-as-empty case
// rather than the never-recorded one; both are answered the same way here, which
// is the point: a reader may act on neither.
func TestGetOnARunWithNoAttestedStarterReportsNoSubject(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)

	assert.Empty(t, got.Msg.GetStarter(),
		"a run nobody authenticated reported a starter; the qualified form of two empty strings is "+
			"the bare separator, which names nobody and would invite a comparison that can only be wrong")
	assert.False(t, v1.LooksLikeQualifiedSubject(got.Msg.GetStarter()),
		"a run with no attested starter reported a string a signal policy rule would accept as a "+
			"subject, which is the one answer a reader must never be handed here")

	_, err = fixture.teamA.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)
}
