package netpolicy

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckConnectionRequiresExplicitProtocolAndChecksResolvedAddress(t *testing.T) {
	addr := netip.MustParseAddrPort("8.8.8.8:5432")

	defaults, err := New()
	require.NoError(t, err)
	require.ErrorIs(t, defaults.CheckConnection(t.Context(), "postgres", "database.example", addr), ErrDenied)

	postgres, err := New(WithSchemes("postgres"))
	require.NoError(t, err)
	require.NoError(t, postgres.CheckConnection(t.Context(), "postgres", "database.example", addr))
	require.ErrorIs(t,
		postgres.CheckConnection(t.Context(), "postgres", "database.example", netip.MustParseAddrPort("127.0.0.1:5432")),
		ErrDenied,
	)
}

func TestCheckConnectionUsesExistingCELVocabulary(t *testing.T) {
	policy, err := New(
		WithSchemes("postgres"),
		WithAllowRules(`scheme == "postgres" && host == "database.example" && port == 5432 && method == "CONNECT" && credentials`),
		WithDenyRules(`ip == "203.0.113.9" || identity.namespace != "team-a"`),
	)
	require.NoError(t, err)

	ctx := ContextWithCredentials(t.Context(), true)
	ctx = ContextWithIdentity(ctx, Identity{Namespace: "team-a"})
	require.NoError(t, policy.CheckConnection(ctx, "postgres", "database.example", netip.MustParseAddrPort("8.8.8.8:5432")))
	require.ErrorIs(t,
		policy.CheckConnection(ctx, "postgres", "database.example", netip.MustParseAddrPort("203.0.113.9:5432")),
		ErrDenied,
	)
}
