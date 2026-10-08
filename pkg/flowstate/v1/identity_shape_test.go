package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

func TestPrincipal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, issuer, subject, want string }{
		{"both", "https://idp.example", "alice", "https://idp.example#alice"},
		{"neither", "", "", ""},
		{"no issuer", "", "alice", ""},
		{"no subject", "https://idp.example", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, principal.Qualified(tc.issuer, tc.subject))
		})
	}
}

// TestIdentityShape pins the one shape `run.identity` and `sender.identity`
// share: exactly these keys, a nil identity renders empty rather than panicking,
// and principal agrees with [v1.QualifiedSubject] whenever it is non-empty.
func TestIdentityShape(t *testing.T) {
	t.Parallel()

	keys := func(m map[string]any) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}

		return out
	}

	nilShape := v1.IdentityShape(nil)
	require.ElementsMatch(t, []string{"subject", "issuer", "namespace", "claims", "principal", "kind", "actions", "actors", "delegated"}, keys(nilShape))
	require.Equal(t, false, nilShape["delegated"])
	require.Empty(t, nilShape["actors"])
	require.Equal(t, "", nilShape["principal"])
	require.Empty(t, nilShape["claims"])

	id := &v1.WorkloadIdentity{Principal: &v1.Principal{Subject: "alice", Issuer: "https://idp.example", Namespace: "team-a", Claims: v1.StringClaimValues(map[string]string{"team": "sre"})}, Deployment: "prod"}
	shape := v1.IdentityShape(id)
	require.Equal(t, v1.QualifiedSubject("https://idp.example", "alice"), shape["principal"])
	require.Equal(t, map[string]any{"team": "sre"}, shape["claims"])
	require.Equal(t, "", shape["kind"], "a policy that assigned no kind renders none, never workload")

	for kind, want := range map[v1.PrincipalKind]string{
		v1.PrincipalKind_PRINCIPAL_KIND_HUMAN:    "human",
		v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD: "workload",
		v1.PrincipalKind_PRINCIPAL_KIND_AGENT:    "agent",
		v1.PrincipalKind(99):                     "",
	} {
		require.Equal(t, want, v1.IdentityShape(&v1.WorkloadIdentity{Principal: &v1.Principal{Kind: kind}})["kind"])
		if want != "" {
			require.Equal(t, kind, v1.PrincipalKindNamed(want))
		}
	}
	require.Equal(t, v1.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED, v1.PrincipalKindNamed("humen"), "a misspelling is none, not a guess")
	require.Equal(t, v1.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED, v1.PrincipalKindNamed("Human"), "a trust policy takes the lowercase spelling only, so a rehearsal must too")
	require.NotContains(t, shape, "deployment", "deployment is the sender's addition, not part of the shared shape")
}

// TestIdentityShapeActors pins how the act chain renders where an expression
// reads a plain map (`run.identity`, `sender.identity`) and round trips the wire
// form, so the chain a rule reads is the chain the server attested: current
// actor first, issuer and subject only.
func TestIdentityShapeActors(t *testing.T) {
	t.Parallel()

	id := &v1.WorkloadIdentity{Principal: &v1.Principal{
		Subject: "alice", Issuer: "https://idp.example",
		Actors: []*v1.Actor{
			{Issuer: "https://agents.example", Subject: "triage-bot"},
			{Issuer: "https://platform.example", Subject: "orchestrator"},
		},
	}}

	shape := v1.IdentityShape(id)
	require.Equal(t, true, shape["delegated"])
	require.Equal(t, []any{
		map[string]any{"issuer": "https://agents.example", "subject": "triage-bot"},
		map[string]any{"issuer": "https://platform.example", "subject": "orchestrator"},
	}, shape["actors"])

	caller := v1.CallerOf(id)
	require.True(t, caller.Delegated)
	require.Equal(t, []principal.Actor{
		{Issuer: "https://agents.example", Subject: "triage-bot"},
		{Issuer: "https://platform.example", Subject: "orchestrator"},
	}, caller.Actors)

	// Wire -> engine -> wire loses nothing and invents nothing.
	back := v1.ProtoPrincipal(v1.AuthIdentity(id))
	require.Len(t, back.GetActors(), 2)
	for i, want := range id.GetPrincipal().GetActors() {
		require.Equal(t, want.GetIssuer(), back.GetActors()[i].GetIssuer())
		require.Equal(t, want.GetSubject(), back.GetActors()[i].GetSubject())
	}

	// A caller acting for themselves round trips as none, not as an empty
	// chain that reads as delegated, and a chain alone keeps the principal.
	require.Nil(t, v1.ProtoPrincipal(v1.AuthIdentity(&v1.WorkloadIdentity{Principal: &v1.Principal{Subject: "alice", Issuer: "i"}})).GetActors())
	require.NotNil(t, v1.ProtoPrincipal(auth.WorkloadIdentity{Actors: []principal.Actor{{Issuer: "a", Subject: "b"}}}))
}

// TestPrincipalActorsAreBoundedOnTheWire holds the schema half of the chain's
// bounds: at most two actors, each with a non-empty issuer and subject.
func TestPrincipalActorsAreBoundedOnTheWire(t *testing.T) {
	t.Parallel()

	actor := &v1.Actor{Issuer: "https://agents.example", Subject: "triage-bot"}
	valid := func(actors ...*v1.Actor) error {
		return v1.Validate(&v1.WorkloadIdentity{Principal: &v1.Principal{Subject: "alice", Issuer: "i", Actors: actors}})
	}

	require.NoError(t, valid())
	require.NoError(t, valid(actor))
	require.NoError(t, valid(actor, actor))
	require.Error(t, valid(actor, actor, actor), "a third actor is over the depth bound")
	require.Error(t, valid(&v1.Actor{Issuer: "https://agents.example"}), "an actor with no subject")
	require.Error(t, valid(&v1.Actor{Subject: "triage-bot"}), "an actor with no issuer")
	require.Error(t, valid(&v1.Actor{Issuer: strings.Repeat("i", 1025), Subject: "s"}), "an oversized issuer")
	require.Error(t, valid(&v1.Actor{Issuer: "i", Subject: strings.Repeat("s", 1025)}), "an oversized subject")
}
