package flowstatev1

import (
	"net/http"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/stretchr/testify/require"
)

// Test_httpTask_egressIdentity proves the bridge #240 adds: the http task renders
// the run's WorkloadIdentity into the egress policy, so an identity-scoped rule
// gates the request by tenant. The netpolicy tests prove the rule mechanism; this
// proves the wiring from a run's scope to it, which is what makes the capability
// reachable rather than merely present.
//
// The negative direction is the point (#240): the same policy that admits team-a
// must deny team-b on the same worker. A run with no identity — a local run's
// empty scope, or a nil scope — is denied too, the fail-closed reading.
func Test_httpTask_egressIdentity(t *testing.T) {
	server, _ := httpTaskServer(t, http.StatusOK, "ok", nil)

	// One shared policy, identity-scoped: only team-a may egress.
	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithTimeout(5*time.Second),
		netpolicy.WithAllowRules(`identity.namespace == "team-a"`),
	)
	require.NoError(t, err)
	fn := taskFuncHTTP(policy)

	inputs := NewNamedValues(map[string]any{"url": server.URL})

	scopeFor := func(namespace string) *Scope {
		if namespace == "" {
			return nil
		}
		return &Scope{Identity: &WorkloadIdentity{Namespace: namespace, Subject: "spiffe://acme/" + namespace}}
	}

	t.Run("the admitted tenant reaches the host", func(t *testing.T) {
		out, err := fn(t.Context(), inputs, scopeFor("team-a"))
		require.NoError(t, err)
		require.NotNil(t, out)
	})

	t.Run("another tenant is denied the same host", func(t *testing.T) {
		_, err := fn(t.Context(), inputs, scopeFor("team-b"))
		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindPolicyDenied, taskErr.Kind)
	})

	t.Run("a run without an identity is denied", func(t *testing.T) {
		_, err := fn(t.Context(), inputs, scopeFor(""))
		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindPolicyDenied, taskErr.Kind)
	})
}
