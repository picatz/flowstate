package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// An identity written outside a test file is held to the rule a test file's own
// are, by the same function: the exported check is not a second copy.
func TestScriptedIdentityCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity *flowtest.ScriptedIdentity
		want     string
	}{
		{"absent is the absence of an identity, not a malformed one", nil, ""},
		{"empty is anonymous, not malformed", &flowtest.ScriptedIdentity{}, ""},
		{"both halves", &flowtest.ScriptedIdentity{Subject: "s", Issuer: "i"}, ""},
		{"subject alone", &flowtest.ScriptedIdentity{Subject: "s"}, "x names a subject or an issuer without the other"},
		{"issuer alone", &flowtest.ScriptedIdentity{Issuer: "i"}, "x names a subject or an issuer without the other"},
		{"empty claim value", &flowtest.ScriptedIdentity{Claims: map[string]string{"team": ""}}, "x declares a claim with an empty value"},
		{"empty claim name", &flowtest.ScriptedIdentity{Claims: map[string]string{"": "v"}}, "x declares a claim with an empty name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.identity.Check("x")
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestScriptedIdentityWorkloadIdentity(t *testing.T) {
	t.Parallel()

	var absent *flowtest.ScriptedIdentity
	require.NotNil(t, absent.WorkloadIdentity(), "an absent identity renders empty, never nil")
	require.Empty(t, absent.WorkloadIdentity().GetSubject())

	got := (&flowtest.ScriptedIdentity{Subject: "s", Issuer: "i", Namespace: "n", Claims: map[string]string{"a": "b"}}).WorkloadIdentity()
	require.Equal(t, "s", got.GetSubject())
	require.Equal(t, "i", got.GetIssuer())
	require.Equal(t, "n", got.GetNamespace())
	require.Equal(t, map[string]string{"a": "b"}, got.GetClaims())
}
