package server_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authz"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestWhoamiAnswersAnyVerifiedCaller proves the two properties the RPC is for:
// a verified caller is told its own principal, and it is told so even when its
// policy entry grants no action at all, which is the case a diagnostic for
// "why was I refused" most needs. Every other RPC refuses that caller.
func TestWhoamiAnswersAnyVerifiedCaller(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)

	// The claims the admitting entry carries, as [auth.MapClaims] produces them:
	// the token below also holds "email" and "sub_secret", which the entry does
	// not name.
	entry := auth.TrustedIssuer{
		Name: "ci",
		CarryClaims: []auth.CarryClaim{
			{Claim: "repository", Type: auth.ClaimTypeString},
			{Claim: "groups", Type: auth.ClaimTypeStringList},
		},
	}
	carried, err := auth.MapClaims(entry, map[string]any{
		"repository": "picatz/flowstate",
		"groups":     []any{"platform", "oncall"},
		"email":      "person@example.com",
		"sub_secret": "must-not-travel",
	})
	require.NoError(t, err)

	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:     "https://issuer.example",
		Subject:    "runner-7",
		IssuerName: "ci",
		Namespace:  "acme",
		Kind:       auth.PrincipalKindWorkload,
		Actions:    nil, // the entry lists no action: nothing but Whoami is held
		Claims:     carried,
	})

	// The control: this caller holds nothing, so an ordinary RPC refuses it.
	_, err = s.GetCatalog(ctx, connect.NewRequest(&v1.GetCatalogRequest{}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err),
		"the caller was meant to hold no action, so this proves Whoami is not merely allowed by a permissive fixture")

	resp, err := s.Whoami(ctx, connect.NewRequest(&v1.WhoamiRequest{}))
	require.NoError(t, err)
	require.True(t, resp.Msg.GetAuthenticated())

	principal := resp.Msg.GetPrincipal()
	require.Equal(t, "https://issuer.example", principal.GetIssuer())
	require.Equal(t, "runner-7", principal.GetSubject())
	require.Equal(t, "acme", principal.GetNamespace())
	require.Equal(t, "ci", principal.GetIssuerEntry())
	require.Equal(t, v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD, principal.GetKind())

	claims := principal.GetClaims()
	require.Equal(t, "picatz/flowstate", claims["repository"].GetStringValue())
	require.Len(t, claims["groups"].GetListValue().GetValues(), 2)
	require.NotContains(t, claims, "email", "a claim the entry does not carry reached the answer")
	require.NotContains(t, claims, "sub_secret", "a claim the entry does not carry reached the answer")
}

// TestWhoamiReportsTheActionsAVerifiedCallerHolds shows the answer is the
// caller's own list, not a widened one.
func TestWhoamiReportsTheActionsAVerifiedCallerHolds(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example",
		Subject: "reader",
		Actions: auth.ActionScopes{"workload.read"},
	})

	resp, err := s.Whoami(ctx, connect.NewRequest(&v1.WhoamiRequest{}))
	require.NoError(t, err)
	require.Equal(t, []string{"workload.read"}, resp.Msg.GetPrincipal().GetActions())
}

// TestWhoamiAnswersAnUnauthenticatedCaller: the insecure development server and
// a deployment with no authentication are answered, never refused.
func TestWhoamiAnswersAnUnauthenticatedCaller(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)

	t.Run("anonymous principal", func(t *testing.T) {
		t.Parallel()

		ctx := auth.ContextWithPrincipal(t.Context(), auth.AnonymousPrincipal())

		resp, err := s.Whoami(ctx, connect.NewRequest(&v1.WhoamiRequest{}))
		require.NoError(t, err)
		require.False(t, resp.Msg.GetAuthenticated())
		require.Equal(t, auth.AnonymousIssuer, resp.Msg.GetPrincipal().GetIssuer())
		require.Empty(t, resp.Msg.GetPrincipal().GetClaims())
	})

	t.Run("no principal at all", func(t *testing.T) {
		t.Parallel()

		resp, err := s.Whoami(t.Context(), connect.NewRequest(&v1.WhoamiRequest{}))
		require.NoError(t, err)
		require.False(t, resp.Msg.GetAuthenticated())
		require.NotNil(t, resp.Msg.GetPrincipal(), "nobody is stated as an empty principal, not left absent")
		require.Empty(t, resp.Msg.GetPrincipal().GetSubject())
	})
}

// TestWhoamiIsStillRefusedByAnEmbedderDecider: held-by-every-caller removes the
// policy list from the question, not the embedder's own veto, which can only
// narrow.
func TestWhoamiIsStillRefusedByAnEmbedderDecider(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil, server.WithDecider(authz.DeciderFunc(func(_ context.Context, _ authz.Request) authz.Decision {
		return authz.Decision{}
	})))
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{Issuer: "https://issuer.example", Subject: "x"})

	_, err := s.Whoami(ctx, connect.NewRequest(&v1.WhoamiRequest{}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}
