package server_test

import (
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `concurrency:` end to end, against a real Temporal.
//
// Every test here needs a *live incumbent* — a run that is still open when the
// second submission arrives — because that is the only state a workflow id can
// be contended in. [gatedWorkflow] parks at a signal it is never sent, and
// [waitUntilParkedAtTheGate] is what makes "the first run is still going" a fact
// this test established rather than a race it hopes to win.

// exclusiveWorkflow is [gatedWorkflow] with a `concurrency:` block, keyed on a
// declared input so the key is resolved rather than copied.
//
// An expression rather than a literal in most of these, deliberately: a literal
// key would exercise the composition and none of the resolution, and resolution
// is the half that runs against caller-submitted values.
func exclusiveWorkflow(arm v1.Concurrency_OnConflict) *v1.Workflow {
	wf := gatedWorkflow()
	wf.DeclaredInputs = []*v1.InputDeclaration{{
		Name:     "cluster",
		Type:     v1.InputDeclaration_TYPE_STRING,
		Required: true,
	}}
	wf.Concurrency = &v1.Concurrency{
		Key:        v1.NewExpr("inputs.cluster"),
		OnConflict: arm,
	}

	return wf
}

func clusterInputs(cluster string) map[string]*v1.Value {
	return map[string]*v1.Value{"cluster": v1.NewLiteral(cluster)}
}

// TestConcurrencyRejectsASecondRunHoldingTheSameKey is the default arm, and the
// one an author gets by writing `key:` and nothing else.
func TestConcurrencyRejectsASecondRunHoldingTheSameKey(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT),
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(first.Msg.GetWorkflowId(), "flowstate-lock-"),
		"a concurrency-addressed run is started under its own id namespace, so a key can never "+
			"name a run some other addressing scheme created")
	require.False(t, first.Msg.GetJoined(), "the first run started; nothing was joined")

	waitUntilParkedAtTheGate(t, fixture.temporal, first.Msg.GetWorkflowId())

	_, err = fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT),
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.Error(t, err, "a second run naming the same key must be refused while the first is open")
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err),
		"the caller asked for something the workflow's author said may not happen concurrently, "+
			"which is a refusal they can act on rather than a server fault")
	require.Contains(t, err.Error(), first.Msg.GetRunId(),
		"the refusal names the run that holds the key, which is what makes it actionable")
}

// TestConcurrencyUnspecifiedRejects pins the fail-closed default: an author who
// writes a key and forgets `on_conflict:` gets exclusion, not a second run.
func TestConcurrencyUnspecifiedRejects(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_UNSPECIFIED),
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, fixture.temporal, first.Msg.GetWorkflowId())

	_, err = fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_UNSPECIFIED),
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

// TestConcurrencyJoinReturnsTheIncumbentRun checks the arm whose whole value is
// the run id it hands back — and the flag that says the id is somebody else's.
func TestConcurrencyJoinReturnsTheIncumbentRun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_JOIN),
		Inputs:   clusterInputs("ledger-1"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, fixture.temporal, first.Msg.GetWorkflowId())

	second, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_JOIN),
		Inputs:   clusterInputs("ledger-1"),
	}))
	require.NoError(t, err, "join answers with the incumbent rather than refusing")
	require.True(t, second.Msg.GetJoined(),
		"a join is a fact the server states; a caller cannot tell it from a fresh start by looking")
	require.Equal(t, first.Msg.GetWorkflowId(), second.Msg.GetWorkflowId())
	require.Equal(t, first.Msg.GetRunId(), second.Msg.GetRunId(),
		"the run returned is the one already holding the key, not a second one")
	require.False(t, second.Msg.GetSpecificationAsSubmitted(),
		"the run named is not the one this request would have started, so its specification is "+
			"the incumbent's and a client must not redact against its own copy")

	// And exactly one run exists under that id: the join started nothing.
	got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: first.Msg.GetWorkflowId(),
	}))
	require.NoError(t, err)
	require.Equal(t, first.Msg.GetRunId(), got.Msg.GetRunId())
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, got.Msg.GetStatus())
}

// TestConcurrencyTerminateOtherStopsTheIncumbent checks the destructive arm on
// both sides: the new run exists, and the old one is gone rather than merely
// unreferenced.
func TestConcurrencyTerminateOtherStopsTheIncumbent(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:   clusterInputs("checkout"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, fixture.temporal, first.Msg.GetWorkflowId())

	second, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:   clusterInputs("checkout"),
	}))
	require.NoError(t, err)
	require.False(t, second.Msg.GetJoined(), "terminate_other starts a run of its own")
	require.Equal(t, first.Msg.GetWorkflowId(), second.Msg.GetWorkflowId(),
		"the key is the id, so the replacement holds the same permit")
	require.NotEqual(t, first.Msg.GetRunId(), second.Msg.GetRunId())

	// The incumbent, addressed by its own run id rather than by the workflow id,
	// which now resolves to the replacement.
	incumbent := first.Msg.GetRunId()
	stopped, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: first.Msg.GetWorkflowId(),
		RunId:      &incumbent,
	}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_TERMINATED, stopped.Msg.GetStatus(),
		"terminated rather than cancelled: the incumbent gets no chance to compensate, which is "+
			"the cost this arm's doc comment states")
}

// TestConcurrencyKeysDoNotCrossTenants is the negative direction CLAUDE.md's
// tenancy section asks for, applied to the permit.
//
// "Tenant A takes its own lock" is a functionality test wearing a security
// test's clothes. What is asserted here is that tenant A holding a key does not
// block tenant B from the *identical* key on the *identical* workflow — and that
// B cannot name, join or terminate A's run by choosing an input value, which is
// the only lever B has over the composition.
func TestConcurrencyKeysDoNotCrossTenants(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	// Team A takes the key, and stays holding it for the rest of the test.
	teamARun, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT),
		Inputs:   clusterInputs("shared-looking-key"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, fixture.temporal, teamARun.Msg.GetWorkflowId())

	t.Run("team B is not blocked by team A's key", func(t *testing.T) {
		teamBRun, err := fixture.teamB.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
			Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT),
			Inputs:   clusterInputs("shared-looking-key"),
		}))
		require.NoError(t, err,
			"a key is composed with the tenant, so one tenant's permit is not another's")
		require.NotEqual(t, teamARun.Msg.GetWorkflowId(), teamBRun.Msg.GetWorkflowId(),
			"two tenants naming the identical key must never resolve to one run")
		require.False(t, teamBRun.Msg.GetJoined(), "team B started its own run rather than joining A's")
	})

	t.Run("team B cannot reach team A's run at the id its own key composed to", func(t *testing.T) {
		_, err := fixture.teamB.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: teamARun.Msg.GetWorkflowId(),
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("team B cannot terminate team A's run by asking for terminate_other", func(t *testing.T) {
		// The lever B actually has: choose the arm, choose the input, and submit.
		// If the tenant were taken from anywhere but the attested identity, this
		// is the request that would stop A's run.
		teamBRun, err := fixture.teamB.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
			Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
			Inputs:   clusterInputs("shared-looking-key"),
		}))
		require.NoError(t, err)
		require.NotEqual(t, teamARun.Msg.GetWorkflowId(), teamBRun.Msg.GetWorkflowId())

		stillRunning, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: teamARun.Msg.GetWorkflowId(),
		}))
		require.NoError(t, err)
		require.Equal(t, v1.RunResponse_STATUS_RUNNING, stillRunning.Msg.GetStatus(),
			"team A's run is untouched by anything team B submitted")
		require.Equal(t, teamARun.Msg.GetRunId(), stillRunning.Msg.GetRunId())
	})
}

// TestConcurrencyDifferentKeysDoNotBlockEachOther is the other direction of the
// same claim: exclusion is per key, so it must not serialize work that names
// different resources.
func TestConcurrencyDifferentKeysDoNotBlockEachOther(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT),
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, fixture.temporal, first.Msg.GetWorkflowId())

	second, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT),
		Inputs:   clusterInputs("prod-us"),
	}))
	require.NoError(t, err, "a different key is a different permit")
	require.NotEqual(t, first.Msg.GetWorkflowId(), second.Msg.GetWorkflowId())
}

// TestConcurrencyReleasesTheKeyWhenTheRunEnds is the difference between a permit
// and a dedupe key, and the reason the reuse policy is left at Temporal's
// default rather than set to REJECT_DUPLICATE the way a webhook delivery's is.
func TestConcurrencyReleasesTheKeyWhenTheRunEnds(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	// A workload that finishes on its own rather than parking.
	finishing := func() *v1.Workflow {
		return &v1.Workflow{
			Name: "exclusive-and-brief",
			DeclaredInputs: []*v1.InputDeclaration{{
				Name:     "cluster",
				Type:     v1.InputDeclaration_TYPE_STRING,
				Required: true,
			}},
			Concurrency: &v1.Concurrency{Key: v1.NewExpr("inputs.cluster")},
			Steps: []*v1.Node{{
				Id: "work",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("done")},
				}},
			}},
		}
	}

	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: finishing(),
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: first.Msg.GetWorkflowId(),
		}))

		return err == nil && got.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 30*time.Second, 100*time.Millisecond, "the first run did not finish, so nothing released the key")

	second, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: finishing(),
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.NoError(t, err,
		"a finished run has released the resource, so the next submission naming the key is a "+
			"new run rather than a duplicate of the old one")
	require.Equal(t, first.Msg.GetWorkflowId(), second.Msg.GetWorkflowId())
	require.NotEqual(t, first.Msg.GetRunId(), second.Msg.GetRunId())
}

// TestConcurrencyRefusesAnEntityKeyToo covers the third addressing scheme: both
// compose the run's workflow id, and there is no correct precedence between
// them, so the pair is refused rather than ordered.
func TestConcurrencyRefusesAnEntityKeyToo(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	key := "order-9"
	_, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT),
		Inputs:    clusterInputs("prod-eu"),
		EntityKey: &key,
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "entity_key")
}

// TestConcurrencyRefusesAKeyThatDoesNotResolve is the caller-facing half of
// resolution: what the key evaluates to is decided by the inputs the caller
// submitted, so a key that resolves to something other than a string is their
// mistake and is reported as one, before anything durable exists.
func TestConcurrencyRefusesAKeyThatDoesNotResolve(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	wf := exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT)
	wf.Concurrency.Key = v1.NewExpr("size(inputs.cluster)")

	_, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: wf,
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "must evaluate to a string")
}

// TestConcurrencyRefusedAlongsideAWebhookTrigger and its schedule sibling are the
// two combinations a workflow id cannot serve twice, refused at the server as
// well as at the compiler — a hand-built specification arrives without the
// compiler in front of it.
func TestConcurrencyRefusedAlongsideAWebhookTrigger(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	wf := exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT)
	wf.Triggers = &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
		Name:           "payments",
		IdempotencyKey: v1.NewExpr("event.id"),
		Verify:         map[string]*v1.Value{"stripe": v1.NewLiteral("webhook-secret")},
	}}}

	_, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: wf,
		Inputs:   clusterInputs("prod-eu"),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "webhook trigger")
}

// TestConcurrencyWorkflowIDIsTenantScopedAndItsOwnNamespace is the derivation on
// its own terms, without a server: the tenant and the workflow name are inside
// the digest, and the prefix keeps a permit out of every other scheme's address
// space.
func TestConcurrencyWorkflowIDIsTenantScopedAndItsOwnNamespace(t *testing.T) {
	t.Parallel()

	teamA := v1.ConcurrencyWorkflowID("team-a", "drain", "prod-eu")
	teamB := v1.ConcurrencyWorkflowID("team-b", "drain", "prod-eu")
	otherWorkflow := v1.ConcurrencyWorkflowID("team-a", "deploy", "prod-eu")
	otherKey := v1.ConcurrencyWorkflowID("team-a", "drain", "prod-us")

	require.NotEqual(t, teamA, teamB, "the tenant is inside the digest")
	require.NotEqual(t, teamA, otherWorkflow, "the workflow name is inside the digest")
	require.NotEqual(t, teamA, otherKey, "the key is inside the digest")
	require.Equal(t, teamA, v1.ConcurrencyWorkflowID("team-a", "drain", "prod-eu"),
		"the same three halves compose the same permit, which is the whole mechanism")

	// The separator does its job: without it, ("a", "bc") and ("ab", "c") would
	// digest alike and one tenant would hold another's permit.
	require.NotEqual(t,
		v1.ConcurrencyWorkflowID("a", "bc", "k"),
		v1.ConcurrencyWorkflowID("ab", "c", "k"))

	require.True(t, strings.HasPrefix(teamA, "flowstate-lock-"))
	require.NotContains(t, teamA, "prod-eu",
		"the key is digested rather than interpolated: a workflow id is durable and broadly "+
			"readable, and a key is frequently a customer's or a cluster's name")
}
