package server

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestIdentityForCarriesConfiguredClaims exercises the handoff the auth
// package's federation tests cannot see: WithIdentityClaims through
// FlowstateServer.identityFor. Those tests prove a CI-issued token becomes a
// Principal with its claims intact, and prove IdentityFromPrincipal copies
// whatever names it is handed; neither would notice this server option
// dropping or misrouting the names on the way through. This is the join, so
// it gets its own test.
//
// The principal is shaped like the one ci_federation_test.go verifies out of
// a CI-issued token, built directly here because the join under test begins
// after verification.
func TestIdentityForCarriesConfiguredClaims(t *testing.T) {
	t.Parallel()

	principal := auth.Principal{
		Issuer:    "https://token.actions.githubusercontent.com",
		Subject:   "repo:example/service:ref:refs/heads/main",
		Namespace: "team-a",
		Claims: map[string]any{
			"repository":       "example/service",
			"ref":              "refs/heads/main",
			"workflow":         "deploy",
			"repository_owner": "example",
		},
	}
	ctx := auth.ContextWithPrincipal(context.Background(), principal)

	s := mustNew(t, nil,
		WithNamespace("fallback-tenant"),
		WithDeployment("prod"),
		WithIdentityClaims("repository", "ref"),
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
			t.Errorf("configured claim %q = %q, want %q", claim, got, want)
		}
	}
	// Only named claims are carried: the identity is persisted in workflow
	// history, so an unconfigured claim leaking through is a disclosure, not
	// a convenience.
	for _, claim := range []string{"workflow", "repository_owner"} {
		if got, ok := id.GetPrincipal().GetClaims()[claim]; ok {
			t.Errorf("unconfigured claim %q carried into the identity as %q", claim, got)
		}
	}
}

// TestIdentityForWithNoConfiguredClaims pins the default: a server configured
// with no identity claims carries none, however many the verified token held.
func TestIdentityForWithNoConfiguredClaims(t *testing.T) {
	t.Parallel()

	ctx := auth.ContextWithPrincipal(context.Background(), auth.Principal{
		Issuer:  "https://token.actions.githubusercontent.com",
		Subject: "repo:example/service:ref:refs/heads/main",
		Claims:  map[string]any{"repository": "example/service"},
	})

	id := mustNew(t, nil, WithNamespace("solo")).identityFor(ctx)
	if len(id.GetPrincipal().GetClaims()) != 0 {
		t.Fatalf("claims = %v, want none carried by default", id.GetPrincipal().GetClaims())
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
