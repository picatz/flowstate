package authz_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authz"
)

// TestIdentityReadIsHeldByEveryCaller: identity.read is the one action no
// policy list is needed for, in either mode, and no neighbouring action
// inherits that.
func TestIdentityReadIsHeldByEveryCaller(t *testing.T) {
	t.Parallel()

	const identityRead = v1.AuthorizationAction_AUTHORIZATION_ACTION_IDENTITY_READ

	verified := auth.Principal{Subject: "s", Issuer: "https://idp.example", Actions: auth.ActionScopes{}}

	for _, mode := range []authz.Mode{authz.Implied, authz.Explicit} {
		require.True(t, authz.DecidePrincipal(verified, true, identityRead, mode).Allowed, "verified, empty list, mode %d", mode)
		require.True(t, authz.DecidePrincipal(auth.AnonymousPrincipal(), true, identityRead, mode).Allowed, "anonymous, mode %d", mode)
		require.True(t, authz.DecidePrincipal(auth.Principal{}, false, identityRead, mode).Allowed, "unauthenticated, mode %d", mode)
	}

	require.False(t, authz.DecidePrincipal(verified, true, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ, authz.Implied).Allowed,
		"holding identity.read for everyone must not widen any other action")

	var held []v1.AuthorizationAction
	for number := range v1.AuthorizationAction_name {
		if action := v1.AuthorizationAction(number); v1.AuthorizationActionHeldByEveryCaller(action) {
			held = append(held, action)
		}
	}
	require.Equal(t, []v1.AuthorizationAction{identityRead}, held)
}
