package auth_test

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

// TestNewProtectedResourceRefusalsNeverQuoteCredentials holds every refusal
// that quotes a resource identifier or an authorization server to the
// package's one answer for what a diagnostic may echo (picatz/flowstate#2039).
//
// Only the fragment refusal runs before ValidateHTTPSURL has seen the
// userinfo, so it is the one that echoed a credential whole before the fix.
// The digit-leading shapes are the "host:port" misread (url.Parse takes
// "acct9:2024" for a host and port and leaves the rest of the credential in
// the query or path); ValidateHTTPSURL now refuses them first
// (picatz/flowstate#2038), and those cases pin that nothing downstream of it
// can quote them either, whichever layer ends up doing the refusing.
func TestNewProtectedResourceRefusalsNeverQuoteCredentials(t *testing.T) {
	t.Parallel()

	const secret = "s3cr3t"

	for _, tc := range []struct {
		name     string
		resource string
		want     string
	}{
		{"fragment after userinfo", "https://acct9:" + secret + "@host.example.com/mcp#x", "must not include a fragment"},
		{"fragment after misread userinfo", "https://acct9:2024/" + secret + "@host.example.com/mcp#x", "must not include a fragment"},
		{"query", "https://acct9:2024?" + secret + "@host.example.com/mcp", "must not include credentials"},
		{"trailing slash", "https://acct9:2024/" + secret + "@host.example.com/", "must not include credentials"},
		{"brace", "https://acct9:2024/" + secret + "@host.example.com/{x}", "must not include credentials"},
		{"non-canonical path", "https://acct9:2024/" + secret + "@host.example.com//x", "must not include credentials"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
				Resource:             tc.resource,
				AuthorizationServers: []string{"https://trusted.example.com"},
			}, trustingPolicy("https://trusted.example.com"))

			require.Error(t, err)
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, err, "host.example.com", "the refusal must still name the host")
			require.NotContains(t, err.Error(), secret)
		})
	}

	t.Run("untrusted authorization server", func(t *testing.T) {
		t.Parallel()

		_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
			Resource:             "https://flowstate.example.com/mcp",
			AuthorizationServers: []string{"https://acct9:2024/" + secret + "@rogue.example.com"},
		}, trustingPolicy("https://trusted.example.com"))

		require.Error(t, err)
		require.ErrorContains(t, err, "rogue.example.com")
		require.NotContains(t, err.Error(), secret)
	})
}

// TestPolicyValidateDisagreeingKeySourceDoesNotQuoteCredentials pins that
// policy validation never quotes an issuer's credential, on the path that
// reaches the "disagree on signing-key source" refusal as on the ones before
// it (picatz/flowstate#2039).
func TestPolicyValidateDisagreeingKeySourceDoesNotQuoteCredentials(t *testing.T) {
	t.Parallel()

	const secret = "s3cr3t"

	entry := func(name, jwks string) auth.TrustedIssuer {
		return auth.TrustedIssuer{
			Name:      name,
			Issuer:    "https://acct9:2024/" + secret + "@host.example.com",
			JWKSURL:   jwks,
			Audiences: []string{"https://flowstate.example.com/mcp"},
			Actions:   []string{},
		}
	}

	err := auth.Policy{Issuers: []auth.TrustedIssuer{
		entry("a", "https://keys.example.com/a"),
		entry("b", "https://keys.example.com/b"),
	}}.Validate()

	require.Error(t, err)
	require.ErrorContains(t, err, "host.example.com")
	require.NotContains(t, err.Error(), secret)
}
