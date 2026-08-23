package server_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
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

// entityWorkflowCarryingVar is entityWorkflow with one extra top-level `var:`,
// used to size the state query's answer without touching the loop arithmetic:
// the blob is only ever read back through the state query, never referenced by
// the loop's `update:`/`until:`, so it inflates the [v1.EntityState] a query
// serializes and nothing else.
func entityWorkflowCarryingVar(name string, value *v1.Value) *v1.Workflow {
	wf := entityWorkflow(nil)
	wf.Vars[name] = value
	return wf
}

// TestEntityStateTruncationReachesTheClientThroughGet is the traversal
// [TestEntityStateReportsCarriedVarsAndLoopState] leaves open: that test proves
// the happy path (`!Truncated`, values present) survives QueryWorkflow encoding
// to the RunResponse, and the struct-level unit tests
// (progress_internal_test.go) prove [progress.stateSnapshot] flips Truncated
// when its answer would exceed [engine.entityStateMaxBytes]. Neither drives an
// actually-oversized workload the whole way — SignalWithStart -> the
// StateQuery handler -> [server.entityState]'s QueryWorkflow/encoded.Get ->
// RunResponse.EntityState — and asserts the flag a caller sees. This does.
//
// # Which bound, and why this one
//
// Of the two documented state bounds, only [engine.entityStateMaxBytes]
// (256 KiB) is reachable end-to-end. [engine.entityStateMaxLoopEntries] (64)
// counts *concurrently active* loops, and no submittable spec produces more
// than one: concurrent constructs run their loops with nil progress and record
// nothing, and the one shape that would put a second loop live beside a first —
// a loop reached from inside a loop body, including through a `call:` — is
// refused by [v1.CheckLoopNesting] before it runs. So the count bound sits far
// above its reachable maximum of one and has no end-to-end path, while the byte
// bound does — which is why the byte bound is the one driven here. The engine's
// loop_state_reach_internal_test.go drives that maximum and the refusal; see
// [engine.entityStateMaxLoopEntries]'s comment and #289.
//
// # How the bound is driven, and to both sides
//
// A single top-level var carries a raw blob. ~300 KiB sits comfortably under
// [v1.MaxSpecBytes] (1 MiB, so the spec is accepted) and comfortably over
// [engine.entityStateMaxBytes] (256 KiB, so the serialized answer must be
// refused) — that is the over case. A ~64 KiB blob on the identical shape stays
// well under the byte bound — the under case — and proves the bound is
// *reached* rather than merely never exceeded: same driver, one just below and
// one just above, distinct outcomes. The under case must come back with its
// vars intact and `!Truncated`; the over case must come back `Truncated` with
// an empty body, because a truncated answer reports nothing rather than a
// partial map a reader could mistake for the whole of it.
func TestEntityStateTruncationReachesTheClientThroughGet(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	// One fixture, one worker, two entities under distinct keys: the byte bound
	// is a property of the answer, not of the tenant, so both sides can share a
	// namespace and the slow dev-server setup is paid once.
	underKey := "order-state-under-byte-bound"
	overKey := "order-state-over-byte-bound"

	underStarted, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: underKey,
		Workflow:  entityWorkflowCarryingVar("blob", v1.NewLiteral(strings.Repeat("u", 64*1024))),
		Name:      "update",
		Payload:   updatePayload(1, false),
	}))
	require.NoError(t, err)
	underID := underStarted.Msg.GetWorkflowId()

	overStarted, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: overKey,
		Workflow:  entityWorkflowCarryingVar("blob", v1.NewLiteral(strings.Repeat("o", 300*1024))),
		Name:      "update",
		Payload:   updatePayload(1, false),
	}))
	require.NoError(t, err)
	overID := overStarted.Msg.GetWorkflowId()

	// Both run the same shape and set their vars before the loop body's wait, so
	// once each is parked at its gate the state query's answer is stable.
	waitUntilParkedAtTheGate(t, fixture.temporal, underID)
	waitUntilParkedAtTheGate(t, fixture.temporal, overID)

	// Over the bound: Truncated must reach the client and the oversized body
	// must be refused whole — no partial Vars, no LoopState leaked past the cap.
	require.Eventually(t, func() bool {
		got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: overID,
		}))
		if err != nil || got.Msg.GetStatus() != v1.RunResponse_STATUS_RUNNING {
			return false
		}
		state := got.Msg.GetEntityState()
		return state != nil &&
			state.GetTruncated() &&
			len(state.GetVars()) == 0 &&
			len(state.GetLoopState()) == 0
	}, 30*time.Second, 100*time.Millisecond,
		"an entity whose carried state exceeds entityStateMaxBytes never reported Truncated through Get")

	// Under the bound, same driver: a real answer reaches the client with its
	// vars intact and nothing truncated — the negative pole that makes the
	// assertion above evidence the bound was reached, not merely never crossed.
	require.Eventually(t, func() bool {
		got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: underID,
		}))
		if err != nil || got.Msg.GetStatus() != v1.RunResponse_STATUS_RUNNING {
			return false
		}
		state := got.Msg.GetEntityState()
		if state == nil || state.GetTruncated() {
			return false
		}
		blob := state.GetVars()["blob"]
		return blob != nil && len(blob.GetLiteral().GetStringValue()) == 64*1024
	}, 30*time.Second, 100*time.Millisecond,
		"an entity whose carried state fits under entityStateMaxBytes never returned its untruncated vars through Get")
}

// TestSignalWithStartRefusesANameNothingWaitsFor is the shape this RPC can
// create and then never resolve, and it is a property of moving the initiating
// delivery into `RunState.PendingSignals` rather than sending it as a signal.
//
// A signal for a name nothing waits for is, through [FlowstateServer.Signal],
// wasteful and self-clearing: it lands on a Temporal channel nobody reads and
// is dropped at the run's next Continue-As-New. Carried in RunState it is
// neither. `drainSignals` carries everything already pending forward
// unconditionally and only *adds* from the channels the specification declares,
// so nothing ever removes an entry no `wait_for_signal:` will consume: it holds
// one of [v1.MaxPendingSignals] slots and its share of the state budget for the
// entity's whole life.
//
// The far more common cause is a misspelling, and the run created by one would
// look exactly like a working entity — running, addressable, accumulating
// nothing. So the refusal is synchronous and names the alternatives, and no
// entity is created at all.
func TestSignalWithStartRefusesANameNothingWaitsFor(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	_, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-43",
		Workflow:  entityWorkflow(nil),
		Name:      "updat", // misspelled, on purpose
		Payload:   updatePayload(5, false),
	}))
	require.Error(t, err, "a mutation no step waits for was accepted, and would be carried forever")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "no `wait_for_signal:` in this workflow waits for")
	require.Contains(t, err.Error(), "update", "the diagnostic must name what was meant, not only what was wrong")

	// And nothing was created: the refusal happens before the entity key is
	// claimed, so a misspelling does not leave a parked run behind for the
	// corrected call to collide with.
	workflowID, err := v1.EntityWorkflowID("team-a", "order-43")
	require.NoError(t, err)
	_, err = fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.Error(t, err, "a refused SignalWithStart must not have created an entity")
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestSignalWithStartDeliversTheCreatingMutationAtomicallyWithCreation is
// #692: a caller's first mutation must be persisted with the entity's
// creation, not as a follow-up call that a crash, a cancelled context, or a
// closed run can leave undelivered.
//
// Before [FlowstateServer.SignalWithStart] carried the initiating delivery in
// `RunState.PendingSignals` (see [FlowstateServer.SignalWithStart]'s "Claim
// the entity key" comment), the create path was a Describe-then-
// ExecuteWorkflow-then-SignalWorkflow sequence — Temporal's own
// SignalWithStartWorkflow no longer covered it, and nothing else made the two
// calls indivisible. Between the accepted create and the accepted signal sat
// a window in which the server could crash, the context could be cancelled,
// or the just-created run could close, leaving an entity that exists but
// never received the mutation that was supposed to have started it.
//
// That shape is checkable without injecting a crash: an entity created *with*
// its initiating delivery has no [enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED]
// event in its history at all — the delivery travelled as part of the
// [enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED] event's own input, consumed by
// the engine exactly as a signal that arrived before its wait is (see
// [drainSignals] and [executor.takePendingSignal] in package engine). A
// version that signals after starting always writes a Signaled event — with
// or without a crash in between — so this assertion catches the defect
// structurally, without needing to race a real failure.
//
// Verified against the defect this guards: temporarily dropping
// `PendingSignals` from the created-path `RunState` and issuing an
// unconditional follow-up `SignalWorkflow` call (mirroring the sequence #692
// describes) makes this test fail, because that call always appends a
// Signaled event.
func TestSignalWithStartDeliversTheCreatingMutationAtomicallyWithCreation(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	created, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-atomic",
		Workflow:  entityWorkflow(nil),
		Name:      "update",
		Payload:   updatePayload(5, false),
	}))
	require.NoError(t, err)
	require.True(t, created.Msg.GetCreated())

	events, err := historyOf(t.Context(), fixture.temporal, created.Msg.GetWorkflowId())
	require.NoError(t, err)
	require.NotEmpty(t, events)

	require.False(t,
		slices.ContainsFunc(events, func(event *historypb.HistoryEvent) bool {
			return event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
		}),
		"the creating call's own mutation reached this run as a signal event rather than as "+
			"part of its start input, which is exactly the window #692 was filed for: an accepted "+
			"create that can lose the mutation that initiated it")

	require.True(t,
		slices.ContainsFunc(events, func(event *historypb.HistoryEvent) bool {
			return event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
		}),
		"a created entity must have actually started")

	// The mutation was not merely present in the start input — it was
	// consumed. A second call under the same key delivers a second mutation,
	// and the entity's accumulated total must reflect both: 5 from the
	// creating call above, 7 from this one. (Mirrors
	// TestEntityStateReportsCarriedVarsAndLoopState's read pattern.)
	second, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-atomic",
		Workflow:  entityWorkflow(nil),
		Name:      "update",
		Payload:   updatePayload(7, false),
	}))
	require.NoError(t, err)
	require.False(t, second.Msg.GetCreated())

	require.Eventually(t, func() bool {
		got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: created.Msg.GetWorkflowId(),
		}))
		if err != nil || got.Msg.GetStatus() != v1.RunResponse_STATUS_RUNNING {
			return false
		}

		state := got.Msg.GetEntityState()
		if state == nil || state.GetTruncated() {
			return false
		}

		total := state.GetLoopState()["lifecycle"]
		return total != nil && total.GetLiteral().GetInt64Value() == 5+7
	}, 30*time.Second, 100*time.Millisecond,
		"the entity's accumulated total never reflected both the creating call's own "+
			"mutation (5) and the second call's (7) — the creating call's mutation must "+
			"have been consumed exactly once, not lost and not duplicated")
}
