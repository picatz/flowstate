package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// entityWorkflow is the smallest shape #105's design record calls an entity:
// `loop:` carrying state, mutated by `wait_for_signal:`, never expected to
// exhaust in normal operation — retired by a `close` mutation instead.
//
// state (`total`) accumulates the `amount` each `update` signal carries;
// `until:` reads the same signal's `close` field. Both are read through
// `steps.mutation.*`, never bare — only the loop's own [Loop.state] binding
// (`total`) is bare, per [v1.Loop]'s own doc comment.
func entityWorkflow(policy map[string]*v1.SignalPolicy) *v1.Workflow {
	return &v1.Workflow{
		Name:    "entity-order",
		Profile: v1.CurrentProfile,
		Signals: policy,
		Vars: map[string]*v1.Value{
			"kind": v1.NewLiteral("order"),
		},
		Steps: []*v1.Node{
			{
				Id: "lifecycle",
				Kind: &v1.Node_Loop{Loop: &v1.Loop{
					State:         "total",
					Initial:       v1.NewLiteral(int64(0)),
					Update:        v1.NewExpr("total + steps.mutation.payload.amount"),
					Until:         v1.NewExpr("steps.mutation.payload.close"),
					MaxIterations: 20,
					Body: []*v1.Node{
						{
							Id: "mutation",
							Kind: &v1.Node_Wait{Wait: &v1.Wait{
								Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "update"}},
								Timeout: durationpb.New(2 * time.Minute),
							}},
						},
					},
				}},
			},
		},
	}
}

func updatePayload(amount int64, close bool) *v1.Node_Outputs {
	return &v1.Node_Outputs{
		NamedValues: map[string]*v1.Value{
			"amount": v1.NewLiteral(amount),
			"close":  v1.NewLiteral(close),
		},
	}
}

// TestEntityKeyDerivesTheWorkflowIDFromTheCallersOwnNamespace checks
// [RunRequest.entity_key] against [v1.EntityWorkflowID] through the real RPC:
// the id a run comes back with is namespace + separator + key, composed from
// the identity the server itself established — never a bare echo of anything
// the request said about its own namespace, because the request has no
// namespace field to echo.
func TestEntityKeyDerivesTheWorkflowIDFromTheCallersOwnNamespace(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	key := "order-key-1"
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  entityWorkflow(nil),
		EntityKey: &key,
	}))
	require.NoError(t, err)
	require.Equal(t, "flowstate-entity-team-a_order-key-1", started.Msg.GetWorkflowId())

	// Addressable at that id through the ordinary Get path — an entity id is
	// just a workflow id, not a second kind of thing to look up.
	got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: started.Msg.GetWorkflowId(),
	}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, got.Msg.GetStatus())
}

// TestAnotherTenantCannotAddressOrCreateOverAnotherTenantsEntityKey is the
// negative direction CLAUDE.md's tenancy section asks for, applied to entity
// addressing rather than to a secret backend: the same entity_key chosen by
// two different tenants must never name the same run, and a tenant that does
// not own an entity must not be able to reach it — including through
// SignalWithStart's create-if-absent path, which a naive implementation
// could turn into a way to "create into" another tenant's address if the
// namespace half were ever taken from the request instead of the identity.
func TestAnotherTenantCannotAddressOrCreateOverAnotherTenantsEntityKey(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	key := "shared-looking-key"
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  entityWorkflow(nil),
		EntityKey: &key,
	}))
	require.NoError(t, err)
	teamAWorkflowID := started.Msg.GetWorkflowId()

	t.Run("cannot read team A's entity", func(t *testing.T) {
		_, err := fixture.teamB.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: teamAWorkflowID,
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("SignalWithStart under the identical key creates team B's own entity, not team A's", func(t *testing.T) {
		swsResp, err := fixture.teamB.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
			EntityKey: key,
			Workflow:  entityWorkflow(nil),
			Name:      "update",
			Payload:   updatePayload(1, false),
		}))
		require.NoError(t, err)
		require.True(t, swsResp.Msg.GetCreated(), "team B's first call under this key should have created a new entity")

		require.NotEqual(t, teamAWorkflowID, swsResp.Msg.GetWorkflowId(),
			"team A and team B addressing the identical entity_key must never resolve to the same run")
		require.Equal(t, "flowstate-entity-team-b_shared-looking-key", swsResp.Msg.GetWorkflowId())
	})

	t.Run("team A's entity is unaffected", func(t *testing.T) {
		got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: teamAWorkflowID,
		}))
		require.NoError(t, err)
		require.Equal(t, v1.RunResponse_STATUS_RUNNING, got.Msg.GetStatus())
	})
}

// TestSignalWithStartCreatesOnceThenSignalsTheEntityThereafter is the
// create-if-absent path's positive direction: a first call brings the entity
// into existence and reports Created; a second call under the identical key
// finds it already running and delivers to it instead, reporting the same
// thing every ordinary Signal reports — the run kept going, it was not
// replaced.
func TestSignalWithStartCreatesOnceThenSignalsTheEntityThereafter(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	first, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-42",
		Workflow:  entityWorkflow(nil),
		Name:      "update",
		Payload:   updatePayload(5, false),
	}))
	require.NoError(t, err)
	require.True(t, first.Msg.GetCreated())

	second, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-42",
		Workflow:  entityWorkflow(nil),
		Name:      "update",
		Payload:   updatePayload(7, false),
	}))
	require.NoError(t, err)
	require.False(t, second.Msg.GetCreated(), "a second call under the same key should have found the entity already running")
	require.Equal(t, first.Msg.GetWorkflowId(), second.Msg.GetWorkflowId())

	// The entity accumulated both mutations rather than being restarted by
	// the second call — see TestEntityStateReportsCarriedVarsAndLoopState for
	// a direct assertion on the accumulated value; here it is enough that the
	// run is still the same, single, running execution.
	got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: first.Msg.GetWorkflowId(),
	}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, got.Msg.GetStatus())
	require.Equal(t, first.Msg.GetRunId(), got.Msg.GetRunId())
}

// TestSignalWithStartAuthorizesCreateAndDeliverySeparately is the test the
// design record calls out by name: "may this sender CREATE an entity under
// key K" is a strictly stronger question than "may this sender signal this
// run," and a sender who clears the first must not be assumed to have
// cleared the second.
//
// The sender here is fully authorized to create — any authenticated caller
// in their own tenant may, the same floor [FlowstateServer.Run] sets — but
// is not the subject an *already-existing* entity's own declared policy
// names. If SignalWithStart answered "may signal" by re-checking only "may
// create," this sender would reach the wait step; instead the entity's own
// policy — read through the same [authorizeSignal] rule an ordinary
// [FlowstateServer.Signal] enforces — refuses them before Temporal ever sees
// the signal.
func TestSignalWithStartAuthorizesCreateAndDeliverySeparately(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	restricted := entityWorkflow(map[string]*v1.SignalPolicy{
		"update": {
			Allow: []*v1.SignalPolicyRule{
				{Subject: v1.QualifiedSubject("https://issuer.example.com", "owner@example.com")},
			},
		},
	})

	owner := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "owner@example.com",
	})
	created, err := fixture.teamA.SignalWithStart(owner, connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "restricted-order",
		Workflow:  restricted,
		Name:      "update",
		Payload:   updatePayload(1, false),
	}))
	require.NoError(t, err)
	require.True(t, created.Msg.GetCreated())

	// A different, equally-authenticated sender: nothing about them would
	// stop them from creating a *new* entity under a key of their own
	// choosing (see TestSignalWithStartCreatesOnceThenSignalsTheEntityThereafter),
	// which is exactly why "may create" cannot be the question this handler
	// asks when the target already exists.
	stranger := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	})
	_, err = fixture.teamA.SignalWithStart(stranger, connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "restricted-order",
		Workflow:  restricted,
		Name:      "update",
		Payload:   updatePayload(1, true),
	}))
	require.Error(t, err, "a sender the entity's own declared policy does not name delivered a signal to an existing entity")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// And the stranger creating their own, differently-keyed entity is still
	// fine — the refusal above is about *this* entity's declared policy, not
	// about the stranger generally.
	own, err := fixture.teamA.SignalWithStart(stranger, connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "strangers-own-order",
		Workflow:  entityWorkflow(nil),
		Name:      "update",
		Payload:   updatePayload(1, false),
	}))
	require.NoError(t, err, "a sender refused delivery to someone else's entity must still be able to create their own")
	require.True(t, own.Msg.GetCreated())
}

// TestEntityStateReportsCarriedVarsAndLoopState is the reachability proof for
// the query slice: a run that is, by design, always RUNNING must still be
// readable, and readable means the values it is carrying — not only its
// position.
func TestEntityStateReportsCarriedVarsAndLoopState(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	started, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-state",
		Workflow:  entityWorkflow(nil),
		Name:      "update",
		Payload:   updatePayload(3, false),
	}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()

	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "update",
		Payload:    updatePayload(4, false),
	}))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if err != nil || got.Msg.GetStatus() != v1.RunResponse_STATUS_RUNNING {
			return false
		}

		state := got.Msg.GetEntityState()
		if state == nil || state.GetTruncated() {
			return false
		}

		kind := state.GetVars()["kind"]
		if kind == nil || kind.GetLiteral().GetStringValue() != "order" {
			return false
		}

		total := state.GetLoopState()["lifecycle"]
		return total != nil && total.GetLiteral().GetInt64Value() == 3+4
	}, 30*time.Second, 100*time.Millisecond,
		"the entity's carried vars and loop state were never observed through Get, even after two mutations")
}
