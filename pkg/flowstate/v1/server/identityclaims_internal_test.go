package server

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestIdentityForCarriesThePrincipalsClaims exercises the handoff the auth
// package's federation tests cannot see: the verified principal's claims
// through FlowstateServer.identityFor into the durable identity. Those tests
// prove a CI-issued token becomes a Principal whose claims are the entry's
// carry_claims; this proves the server copies exactly the principal's claims
// and nothing else.
//
// The principal is shaped like the one ci_federation_test.go verifies out of
// a CI-issued token, built directly here because the join under test begins
// after verification.
func TestIdentityForCarriesThePrincipalsClaims(t *testing.T) {
	t.Parallel()

	principal := auth.Principal{
		Issuer:    "https://token.actions.githubusercontent.com",
		Subject:   "repo:example/service:ref:refs/heads/main",
		Namespace: "team-a",
		Claims: map[string]any{
			"repository": "example/service",
			"ref":        "refs/heads/main",
			"groups":     []any{"sre"},
		},
	}
	ctx := auth.ContextWithPrincipal(context.Background(), principal)

	s := mustNew(t, nil,
		WithNamespace("fallback-tenant"),
		WithDeployment("prod"),
	)

	id := s.identityFor(ctx)
	if id.GetPrincipal().GetSubject() != principal.Subject {
		t.Fatalf("subject = %q, want %q", id.GetPrincipal().GetSubject(), principal.Subject)
	}
	if id.GetPrincipal().GetIssuer() != principal.Issuer {
		t.Fatalf("issuer = %q, want %q", id.GetPrincipal().GetIssuer(), principal.Issuer)
	}
	// The verified caller's namespace wins over the server's fallback; the
	// other order would make the tenant boundary decorative.
	if id.GetPrincipal().GetNamespace() != "team-a" {
		t.Fatalf("namespace = %q, want the principal's %q", id.GetPrincipal().GetNamespace(), "team-a")
	}
	for claim, want := range map[string]string{
		"repository": "example/service",
		"ref":        "refs/heads/main",
	} {
		if got := id.GetPrincipal().GetClaims()[claim].GetStringValue(); got != want {
			t.Errorf("claim %q = %q, want %q", claim, got, want)
		}
	}
	if got := id.GetPrincipal().GetClaims()["groups"].GetListValue().GetValues(); len(got) != 1 || got[0].GetStringValue() != "sre" {
		t.Errorf("groups = %v, want [sre]", got)
	}
	if len(id.GetPrincipal().GetClaims()) != 3 {
		t.Errorf("claims = %v, want exactly the principal's three", id.GetPrincipal().GetClaims())
	}
}

// TestIdentityForWithNoCarriedClaims pins the default: a principal whose entry
// carried none yields an identity with none.
func TestIdentityForWithNoCarriedClaims(t *testing.T) {
	t.Parallel()

	ctx := auth.ContextWithPrincipal(context.Background(), auth.Principal{
		Issuer:  "https://token.actions.githubusercontent.com",
		Subject: "repo:example/service:ref:refs/heads/main",
	})

	id := mustNew(t, nil, WithNamespace("solo")).identityFor(ctx)
	if len(id.GetPrincipal().GetClaims()) != 0 {
		t.Fatalf("claims = %v, want none", id.GetPrincipal().GetClaims())
	}
	if id.GetPrincipal().GetNamespace() != "solo" {
		t.Fatalf("namespace = %q, want the server fallback %q for a principal naming none", id.GetPrincipal().GetNamespace(), "solo")
	}
}

// TestIdentityForCarriesThePolicyAssignedKind proves the kind the admitting trust
// policy entry assigned reaches the durable identity, and that a caller with none
// stays unspecified rather than becoming a workload.
func TestIdentityForCarriesThePolicyAssignedKind(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil, WithNamespace("solo"))

	for kind, want := range map[auth.PrincipalKind]v1.PrincipalKind{
		auth.PrincipalKindHuman:    v1.PrincipalKind_PRINCIPAL_KIND_HUMAN,
		auth.PrincipalKindWorkload: v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD,
		auth.PrincipalKindAgent:    v1.PrincipalKind_PRINCIPAL_KIND_AGENT,
		"":                         v1.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED,
	} {
		ctx := auth.ContextWithPrincipal(context.Background(), auth.Principal{
			Issuer: "https://idp.example", Subject: "alice", Kind: kind,
		})
		if got := s.identityFor(ctx).GetPrincipal().GetKind(); got != want {
			t.Errorf("kind %q became %s, want %s", kind, got, want)
		}
	}
}
