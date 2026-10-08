package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestSignalPolicyRefusalNamesTheClaimsTheSenderCarried proves a refusal of a
// claims-reading predicate says which claim names the sender identity carried
// (so an empty projection reads differently from a wrong value) and names the
// flag that projects them, never a value; a projected claim still admits; and
// a predicate that does not read claims is refused without the hint.
func TestSignalPolicyRefusalNamesTheClaimsTheSenderCarried(t *testing.T) {
	t.Parallel()

	const hint = "carry_claims"
	policy := predicatePolicy(`sender.identity.claims.team == "platform"`)
	check := func(claims map[string]string) error {
		sender := &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://i", Subject: "alice", Claims: v1.StringClaimValues(claims)}}
		return v1.SignalPolicyCheck(context.Background(), policy, sender, nil, false, nil)
	}

	require.NoError(t, check(map[string]string{"team": "platform"}), "a projected claim still admits")

	err := check(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "carried no claims")
	assert.Contains(t, err.Error(), hint)

	err = check(map[string]string{"email": "secret-value@example.com"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only the claims email")
	assert.NotContains(t, err.Error(), "secret-value", "claim values are never quoted")

	err = check(map[string]string{"team": "wrong-value"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only the claims team")
	assert.NotContains(t, err.Error(), "wrong-value")

	err = v1.SignalPolicyCheck(context.Background(), predicatePolicy(`sender.identity.subject == "bob"`),
		&v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://i", Subject: "alice"}}, nil, false, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), hint)
}

// TestSignalPolicyPredicateReadsListAndNestedClaims proves a predicate reads a
// list claim (`"sre" in sender.identity.claims.groups`) and a nested one
// (`sender.identity.claims.slack.user`) of the sender and of the run's starter,
// and that a claim the caller lacks denies rather than permitting: a group
// list without the group, a nested key that is absent, and a claim the sender
// never carried. Both walkers see the read: `claims` marks the narrowing rule
// satisfied and the run read is recorded for delivery.
func TestSignalPolicyPredicateReadsListAndNestedClaims(t *testing.T) {
	t.Parallel()

	claims := func(groups []any, slack map[string]any) map[string]*structpb.Value {
		in := map[string]any{}
		if groups != nil {
			in["groups"] = groups
		}
		if slack != nil {
			in["slack"] = slack
		}
		s, err := structpb.NewStruct(in)
		require.NoError(t, err)

		return s.GetFields()
	}
	sender := func(c map[string]*structpb.Value) *v1.WorkloadIdentity {
		return &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://i", Subject: "alice",
			Kind: v1.PrincipalKind_PRINCIPAL_KIND_HUMAN, Claims: c}}
	}
	check := func(expr string, who *v1.WorkloadIdentity) error {
		return v1.SignalPolicyCheck(context.Background(), predicatePolicy(expr), who, who, true, nil)
	}

	const rule = `sender.identity.kind == "human" && "sre" in sender.identity.claims.groups && sender.identity.claims.slack.user == "U1"`

	require.NoError(t, check(rule, sender(claims([]any{"dev", "sre"}, map[string]any{"user": "U1"}))))
	require.Error(t, check(rule, sender(claims([]any{"dev"}, map[string]any{"user": "U1"}))), "a list without the group")
	require.Error(t, check(rule, sender(claims([]any{"sre"}, map[string]any{"user": "U2"}))), "a nested value that differs")
	require.Error(t, check(rule, sender(claims([]any{"sre"}, map[string]any{"other": "x"}))), "an absent nested key errors, which denies")
	require.Error(t, check(rule, sender(claims([]any{"sre"}, nil))), "an absent object errors, which denies")
	require.Error(t, check(rule, sender(nil)), "no claims at all denies")

	// The starter's claims read the same way, and `has` guards an absent one.
	require.NoError(t, check(`"sre" in run.identity.claims.groups`, sender(claims([]any{"sre"}, nil))))
	require.NoError(t, check(`!has(sender.identity.claims.nothing) && sender.identity.principal != ""`, sender(nil)))
}

// TestSignalPolicyPredicateReadsTheActorChain proves a predicate reads who is
// acting for the sender and for the run's starter, and that the guard an author
// is told to write holds in both directions: a delegated sender through the
// named actor is admitted, one through another actor or none is refused, and
// `!sender.identity.delegated` refuses every delegated sender.
func TestSignalPolicyPredicateReadsTheActorChain(t *testing.T) {
	t.Parallel()

	sender := func(actors ...*v1.Actor) *v1.WorkloadIdentity {
		return &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://i", Subject: "alice", Actors: actors}}
	}
	bot := &v1.Actor{Issuer: "https://agents.example", Subject: "triage-bot"}
	other := &v1.Actor{Issuer: "https://agents.example", Subject: "other-bot"}

	named := predicatePolicy(`sender.identity.delegated && sender.identity.actors[0].issuer == "https://agents.example" && sender.identity.actors[0].subject == "triage-bot"`)
	require.NoError(t, v1.SignalPolicyCheck(context.Background(), named, sender(bot), nil, false, nil))
	require.Error(t, v1.SignalPolicyCheck(context.Background(), named, sender(other), nil, false, nil), "another actor")
	require.Error(t, v1.SignalPolicyCheck(context.Background(), named, sender(), nil, false, nil), "no actor")

	guarded := predicatePolicy(`!sender.identity.delegated || sender.identity.actors[0].subject == "triage-bot"`)
	require.NoError(t, v1.SignalPolicyCheck(context.Background(), guarded, sender(), nil, false, nil), "acting alone passes the guard")
	require.NoError(t, v1.SignalPolicyCheck(context.Background(), guarded, sender(bot), nil, false, nil))
	require.Error(t, v1.SignalPolicyCheck(context.Background(), guarded, sender(other), nil, false, nil))

	noAgents := predicatePolicy(`!sender.identity.delegated`)
	require.NoError(t, v1.SignalPolicyCheck(context.Background(), noAgents, sender(), nil, false, nil))
	require.Error(t, v1.SignalPolicyCheck(context.Background(), noAgents, sender(bot), nil, false, nil))

	// The run's starter reads the same way: a rule about who started the run.
	starter := predicatePolicy(`run.identity.actors[0].subject == "triage-bot"`)
	require.NoError(t, v1.SignalPolicyCheck(context.Background(), starter, sender(), sender(bot), true, nil))
	require.Error(t, v1.SignalPolicyCheck(context.Background(), starter, sender(), sender(other), true, nil))
}
