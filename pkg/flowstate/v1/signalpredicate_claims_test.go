package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestSignalPolicyRefusalNamesTheClaimsTheSenderCarried proves a refusal of a
// claims-reading predicate says which claim names the sender identity carried
// (so an empty projection reads differently from a wrong value) and names the
// flag that projects them, never a value; a projected claim still admits; and
// a predicate that does not read claims is refused without the hint.
func TestSignalPolicyRefusalNamesTheClaimsTheSenderCarried(t *testing.T) {
	t.Parallel()

	const hint = "--identity-claim"
	policy := predicatePolicy(`sender.identity.claims.team == "platform"`)
	check := func(claims map[string]string) error {
		sender := &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://i", Subject: "alice", Claims: v1.StringClaimValues(claims)}}
		return v1.SignalPolicyCheck(context.Background(), policy, sender, nil, false, nil)
	}

	require.NoError(t, check(map[string]string{"team": "platform"}), "a projected claim still admits")

	err := check(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "carried no claims")
	assert.Contains(t, err.Error(), hint)

	err = check(map[string]string{"email": "secret-value@example.com"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only the claims email")
	assert.NotContains(t, err.Error(), "secret-value", "claim values are never quoted")

	err = check(map[string]string{"team": "wrong-value"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only the claims team")
	assert.NotContains(t, err.Error(), "wrong-value")

	err = v1.SignalPolicyCheck(context.Background(), predicatePolicy(`sender.identity.subject == "bob"`),
		&v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://i", Subject: "alice"}}, nil, false, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), hint)
}
