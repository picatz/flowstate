package authz_test

import (
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
		{"an entry listing no actions grants every implied action", auth.Principal{Subject: "s"}, true, authz.Implied, signal, true},
		{"an entry listing no actions grants no explicit action", auth.Principal{Subject: "s"}, true, authz.Explicit, signal, false},
		{"an empty list grants nothing implied", auth.Principal{Subject: "s", Actions: []string{}}, true, authz.Implied, signal, false},
		{"a listed action is held", auth.Principal{Subject: "s", Actions: []string{scope}}, true, authz.Implied, signal, true},
		{"a listed action is held explicitly", auth.Principal{Subject: "s", Actions: []string{scope}}, true, authz.Explicit, signal, true},
		{"an unlisted action is refused", auth.Principal{Subject: "s", Actions: []string{scope}}, true, authz.Implied, other, false},
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

	refusal := authz.DecidePrincipal(auth.Principal{Subject: "s", Actions: []string{}}, true, action, authz.Implied).Refusal()
	require.NotNil(t, refusal)
	require.Equal(t, connect.CodePermissionDenied, refusal.Code())
	require.Contains(t, refusal.Message(), scope)
	require.Equal(t, `Bearer error="insufficient_scope", scope="`+scope+`"`, refusal.Meta().Get("WWW-Authenticate"))
}
