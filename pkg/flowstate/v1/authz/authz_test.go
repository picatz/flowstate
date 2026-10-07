package authz_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authz"
)

func TestDecidePrincipal(t *testing.T) {
	const signal = v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL
	const other = v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_DEBUG

	scope := v1.AuthorizationActionScope(signal)
	verified := auth.Principal{Subject: "s", Issuer: "https://idp.example"}

	tests := []struct {
		name          string
		principal     auth.Principal
		authenticated bool
		mode          authz.Mode
		action        v1.AuthorizationAction
		want          bool
	}{
		{"no principal holds an implied action", auth.Principal{}, false, authz.Implied, signal, true},
		{"no principal holds no explicit action", auth.Principal{}, false, authz.Explicit, signal, false},
		{"a verified caller with no list holds nothing implied", verified, true, authz.Implied, signal, false},
		{"a verified caller with no list holds nothing explicit", verified, true, authz.Explicit, signal, false},
		{"the anonymous caller of an insecure server holds every implied action", auth.AnonymousPrincipal(), true, authz.Implied, signal, true},
		{"the anonymous caller holds no explicit action", auth.AnonymousPrincipal(), true, authz.Explicit, signal, false},
		{"an empty list grants nothing implied", auth.Principal{Subject: "s", Issuer: "https://idp.example", Actions: []string{}}, true, authz.Implied, signal, false},
		{"a listed action is held", auth.Principal{Subject: "s", Issuer: "https://idp.example", Actions: []string{scope}}, true, authz.Implied, signal, true},
		{"a listed action is held explicitly", auth.Principal{Subject: "s", Issuer: "https://idp.example", Actions: []string{scope}}, true, authz.Explicit, signal, true},
		{"an unlisted action is refused", auth.Principal{Subject: "s", Issuer: "https://idp.example", Actions: []string{scope}}, true, authz.Implied, other, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := authz.DecidePrincipal(test.principal, test.authenticated, test.action, test.mode)
			require.Equal(t, test.want, decision.Allowed)
			require.Equal(t, v1.AuthorizationActionScope(test.action), decision.Scope)
		})
	}
}

func TestRefusalNamesTheScopeAndChallenges(t *testing.T) {
	const action = v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL

	scope := v1.AuthorizationActionScope(action)

	require.Nil(t, authz.DecidePrincipal(auth.Principal{}, false, action, authz.Implied).Refusal())

	refusal := authz.DecidePrincipal(auth.Principal{Subject: "s", Issuer: "https://idp.example", Actions: []string{}}, true, action, authz.Implied).Refusal()
	require.NotNil(t, refusal)
	require.Equal(t, connect.CodePermissionDenied, refusal.Code())
	require.Contains(t, refusal.Message(), scope)
	require.Equal(t, `Bearer error="insufficient_scope", scope="`+scope+`"`, refusal.Meta().Get("WWW-Authenticate"))
}

func TestRestrictOnlyNarrowsThePolicy(t *testing.T) {
	t.Parallel()

	const run = v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN
	holder := auth.Principal{Issuer: "https://issuer.example", Subject: "a", Actions: auth.ActionScopes{"workload.run"}}
	stranger := auth.Principal{Issuer: "https://issuer.example", Subject: "b", Actions: auth.ActionScopes{}}

	allowAll := authz.DeciderFunc(func(context.Context, authz.Request) authz.Decision {
		return authz.Decision{Allowed: true, Scope: "workload.run"}
	})
	denyAll := authz.DeciderFunc(func(context.Context, authz.Request) authz.Decision {
		return authz.Decision{Scope: "freeze"}
	})
	panics := authz.DeciderFunc(func(context.Context, authz.Request) authz.Decision { panic("boom") })

	ask := func(d authz.Decider, p auth.Principal) authz.Decision {
		return d.Decide(t.Context(), authz.Request{Principal: p, Authenticated: true, Action: run, Mode: authz.Implied})
	}

	require.True(t, ask(authz.Restrict(nil, allowAll), holder).Allowed, "an agreeing extra check changes nothing")
	require.False(t, ask(authz.Restrict(nil, allowAll), stranger).Allowed,
		"an extra check that allows must not grant what the trust policy withholds")
	require.False(t, ask(authz.Restrict(nil, denyAll), holder).Allowed, "an extra check can refuse what the policy grants")
	refused := ask(authz.Restrict(nil, denyAll), holder)
	require.True(t, refused.Embedder)
	require.Equal(t, "workload.run", refused.Scope, "a refusal names the action needed, not the embedder's own label")
	require.Empty(t, refused.Refusal().Meta().Get("WWW-Authenticate"))
	require.False(t, ask(authz.Restrict(nil, allowAll), stranger).Embedder, "the policy's own refusal is not an embedder's")
	require.False(t, ask(authz.Restrict(nil, panics), holder).Allowed, "a panicking extra check is a refusal")
	require.Equal(t, authz.PolicyDecider{}, authz.Restrict(nil, nil))

	calls := 0
	counting := authz.DeciderFunc(func(context.Context, authz.Request) authz.Decision {
		calls++
		return authz.Decision{Allowed: true}
	})
	ask(authz.Restrict(nil, counting), stranger)
	require.Zero(t, calls, "the extra check is consulted only once the policy has allowed")
}
