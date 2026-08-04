package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// A workflow id is not a capability.
//
// Ids appear in logs, dashboards, support tickets and URLs, so any RPC that acts
// on a run because a caller named one correctly is an RPC that acts on any run
// whose id has leaked. These tests check both directions, because a check that
// denies everything passes a test that only tries the thing that should work —
// and a check that allows everything passes a test that only tries the thing that
// should not.

// tenantFixture is a dev server, a worker, and two servers standing in for two
// tenants over the same Temporal client.
type tenantFixture struct {
	teamA *server.FlowstateServer
	teamB *server.FlowstateServer

	// The Temporal client the two servers share, for the one question the RPC
	// surface deliberately cannot answer: where in its steps a run has got to.
	// `Get` reports a status, and a status says a run is going rather than what
	// it is doing — so a test that has to act on a run *at a particular step*
	// reads history instead of guessing from a status.
	temporal client.Client
}

// newTenantFixture starts everything needed to run and address real workloads,
// inside a Temporal namespace belonging to this test alone.
func newTenantFixture(t *testing.T) *tenantFixture {
	t.Helper()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	// Two tenants, one cluster. Without an authenticator in front, a caller's
	// namespace is the one the server was configured with, which is what a
	// single-tenant deployment looks like — so two such servers are two tenants
	// as far as the authorization logic is concerned, and that is the logic under
	// test.
	return &tenantFixture{
		teamA:    server.New(temporal, server.WithNamespace("team-a")),
		teamB:    server.New(temporal, server.WithNamespace("team-b")),
		temporal: temporal,
	}
}

// gatedWorkflow is a workload that waits for an approval, which is what makes it
// still addressable while the test asks questions about it.
func gatedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "gated",
		Steps: []*v1.Node{
			{
				Id: "request",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("requesting approval")},
				}},
			},
			{
				Id: "approval",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
					Timeout: durationpb.New(2 * time.Minute),
				}},
			},
			{
				Id:        "deploy",
				Condition: v1.NewExpr("approval.payload.approved"),
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("deploying")},
				}},
			},
		},
	}
}

// TestAnotherTenantCannotAddressARun is the negative direction, and the one worth
// having: a caller in one tenant must not be able to reach another's run even
// knowing its id exactly.
func TestAnotherTenantCannotAddressARun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	require.NotEmpty(t, workflowID)

	t.Run("cannot read it", func(t *testing.T) {
		_, err := fixture.teamB.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		require.Error(t, err, "another tenant read a run by id")

		// Not found rather than permission denied: denied would confirm that a
		// run with this id exists somewhere, which is the one fact a caller in
		// the wrong tenant should not learn.
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("cannot signal it", func(t *testing.T) {
		_, err := fixture.teamB.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
			WorkflowId: workflowID,
			Name:       "deploy-approved",
			Payload: &v1.Node_Outputs{
				NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
			},
		}))
		require.Error(t, err, "another tenant unblocked a run it cannot see")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("its owner still can", func(t *testing.T) {
		// The positive direction, in the same test as the negative one. A check
		// that refused everyone would pass the two subtests above.
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		require.NoError(t, err, "a run's own tenant could not read it")
		require.Equal(t, workflowID, resp.Msg.GetWorkflowId())
	})
}

// TestRunWithNoSuchIdIsNotFound checks that a caller cannot tell a run in another
// tenant from a run that never existed.
func TestRunWithNoSuchIdIsNotFound(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	_, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: "flowstate-workflow-does-not-exist",
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestApprovalGateEndToEnd runs the thing the feature exists for, against a real
// Temporal server: a workload blocks on a human approval, the approval arrives
// over the RPC, and the gated step runs.
func TestApprovalGateEndToEnd(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	// The run is waiting, so it is still running — which is the observable
	// difference between a durable wait and a step that blocks a worker.
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_RUNNING
	}, 30*time.Second, 100*time.Millisecond, "the run never reached a running state")

	// The approval, as `flow signal` would send it.
	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				"by":       v1.NewLiteral("someone@example.com"),
			},
		},
	}))
	require.NoError(t, err)

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp
		return true
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after being approved")

	outputs := final.Msg.GetOutputs().GetStepValues()

	require.NotNil(t, outputs["approval"], "the gate recorded no outputs")
	require.True(t, payloadField(t, outputs["approval"], "approved").GetBoolValue(),
		"what the approver sent did not reach the workload")

	// The #194 fix: the server attests a sender, from what it actually
	// established about the caller, distinct from the payload's self-asserted
	// `by`. No principal was installed on this request's context, so identity
	// reads unauthenticated — but the sender is still the server's own account
	// (not local, timestamped, this tenant's namespace), which is what a real
	// dev deployment with no identity provider in front of it looks like: a
	// signal reached the RPC, and *something* attested it, even if that
	// something has no subject to name.
	require.Equal(t, "team-a", senderField(t, outputs["approval"], "namespace"),
		"the attested sender did not carry the tenant the server itself established")
	require.NotEmpty(t, senderField(t, outputs["approval"], "accepted_at"),
		"the attested sender carries no accepted_at")
	require.NotEqual(t, "someone@example.com", senderField(t, outputs["approval"], "subject"),
		"the payload's self-asserted \"by\" leaked into the attested sender's identity")

	require.NotNil(t, outputs["deploy"], "the gated step did not run after approval")
}

// TestSignalAttestsTheAuthenticatedCallerNotAnythingItClaims is the forged-
// sender direction: proof that a caller cannot make the workload believe
// anything about who sent a signal beyond what the server itself established
// through [server.FlowstateServer.authorizeRun] and identityFor.
//
// Two forgery attempts travel in the same request. The payload — the one part
// of a [v1.SignalRequest] a caller controls — names a key literally spelled
// "sender" carrying a fabricated identity, echoing the exact confusion #194 is
// about. And the request is sent by a principal whose real, verified subject is
// "real-caller@example.com", so the test can tell "the payload's claim" apart
// from "the server's own attestation" rather than both happening to be empty.
func TestSignalAttestsTheAuthenticatedCallerNotAnythingItClaims(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_RUNNING
	}, 30*time.Second, 100*time.Millisecond, "the run never reached a running state")

	// The context this request carries a real, authenticated principal on —
	// exactly what a caller cannot forge, which is the whole point: the
	// server's attestation must come from here, never from the payload below.
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "real-caller@example.com",
	})

	_, err = fixture.teamA.Signal(ctx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				// The forgery: a payload key spelled exactly like the attested
				// output's own name, carrying a fabricated identity. Nothing in
				// [v1.SignalRequest] lets a caller set an actual sender field —
				// the schema itself is the refusal — so this is the only lever
				// available to a hostile caller, and it must not work.
				"sender": v1.NewLiteral("forged-identity@attacker.example.com"),
			},
		},
	}))
	require.NoError(t, err)

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp
		return true
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after being approved")

	approval := final.Msg.GetOutputs().GetStepValues()["approval"]
	require.NotNil(t, approval, "the gate recorded no outputs")

	// The attested sender is the real, authenticated caller — proof the forged
	// direction was refused rather than merely untried.
	require.Equal(t, "real-caller@example.com", senderField(t, approval, "subject"),
		"the attested sender was not the request's authenticated caller")
	require.False(t, senderField(t, approval, "local") == "true",
		"a signal delivered through the server was reported as an unattested local one")

	// The forged claim is still readable, but only inside `payload`, where it
	// is nothing more than a string a sender happened to send — never believed
	// as an identity.
	require.Equal(t, "forged-identity@attacker.example.com",
		payloadField(t, approval, "sender").GetStringValue(),
		"a sender may name a key \"sender\" inside its own payload; it must never be read as the attested one")
}

// senderField reads one entry out of a wait's `sender.identity` mapping, or the
// two sibling fields sitting beside it — see [v1.SenderOutput].
//
// A single lookup that flattens `identity`'s nested map together with `local`
// and `accepted_at`, because every caller of this in this file wants exactly
// one of those four names and none of them cares that two live one level
// deeper than the other two.
func senderField(t *testing.T, outputs *v1.Node_Outputs, name string) string {
	t.Helper()

	sender := outputs.GetNamedValues()[v1.SenderOutput].GetLiteral().GetMapValue()
	require.NotNil(t, sender, "the wait produced no sender mapping")

	for _, entry := range sender.GetEntries() {
		switch entry.GetKey().GetStringValue() {
		case "identity":
			for _, field := range entry.GetValue().GetMapValue().GetEntries() {
				if field.GetKey().GetStringValue() == name {
					return field.GetValue().GetStringValue()
				}
			}
		case name:
			if entry.GetValue().GetBoolValue() {
				return "true"
			}
			return entry.GetValue().GetStringValue()
		}
	}

	t.Fatalf("the sender mapping has no field named %q", name)
	return ""
}

// TestSignalRejectsAMalformedRequest checks that validation runs before anything
// is addressed, so a bad request is a bad request rather than a lookup.
func TestSignalRejectsAMalformedRequest(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	tests := []struct {
		name    string
		request *v1.SignalRequest
	}{
		{
			name:    "no workflow id",
			request: &v1.SignalRequest{Name: "deploy-approved"},
		},
		{
			name:    "no signal name",
			request: &v1.SignalRequest{WorkflowId: "flowstate-workflow-x"},
		},
		{
			name:    "a signal name that is not one",
			request: &v1.SignalRequest{WorkflowId: "flowstate-workflow-x", Name: "not a name!"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := fixture.teamA.Signal(t.Context(), connect.NewRequest(test.request))
			require.Error(t, err)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}
}
