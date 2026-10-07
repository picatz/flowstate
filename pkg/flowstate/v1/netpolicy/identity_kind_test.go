package netpolicy

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// Test_Policy_rules_identityKindAndActions proves an egress rule can key on the
// shared principal.Caller's kind and actions, in both the allowing and the
// denying direction.
func Test_Policy_rules_identityKindAndActions(t *testing.T) {
	server, _ := testServer(t, "ok")

	human := principal.Caller{Namespace: "team-a", Kind: "human", Actions: []string{"run.start"}}
	workload := principal.Caller{Namespace: "team-a", Kind: "workload"}

	allow, err := New(WithAllowLoopback(), WithAllowRules(`identity.kind == "workload"`))
	require.NoError(t, err)

	resp, err := getAs(t, allow, server.URL, workload)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, err = getAs(t, allow, server.URL, human)
	requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
	_, err = getAs(t, allow, server.URL, principal.Caller{})
	requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")

	deny, err := New(WithAllowLoopback(), WithDenyRules(`identity.kind == "human"`))
	require.NoError(t, err)
	_, err = getAs(t, deny, server.URL, human)
	requireDenied(t, err, ReasonDenyRule, `identity.kind == "human"`)
	resp, err = getAs(t, deny, server.URL, workload)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// actions is readable, and a caller carrying none is a non-match, not an error.
	byAction, err := New(WithAllowLoopback(), WithAllowRules(`"run.start" in identity.actions`))
	require.NoError(t, err)
	resp, err = getAs(t, byAction, server.URL, human)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = getAs(t, byAction, server.URL, workload)
	requireDenied(t, err, ReasonNoAllowRule, "no allow rule matched")
}
