package auth_test

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

func TestProfileInventoryIsDefensive(t *testing.T) {
	profiles := auth.AuthProfiles()
	require.NotEmpty(t, profiles)
	profiles[0].Id = "mutated"
	require.NotEqual(t, "mutated", auth.AuthProfiles()[0].GetId())
}

func TestTargetProfilesFailClosed(t *testing.T) {
	base := `issuer: https://flowstate.example.com
targets:
  - name: partner
    token_exchange:
      token_url: https://as.example.com/token
      audience: https://as.example.com
`
	_, err := auth.ParseFederationPolicy([]byte(base))
	require.ErrorContains(t, err, "profile is required")

	_, err = auth.ParseFederationPolicy([]byte(`issuer: https://flowstate.example.com
targets:
  - name: partner
    profile: oauth-token-exchange-draft-99
    token_exchange:
      token_url: https://as.example.com/token
      audience: https://as.example.com
`))
	require.ErrorContains(t, err, "unknown auth profile revision")
}
