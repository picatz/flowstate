package flowstatev1

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_isLoopbackHost pins the exemption's boundary directly, without a
// network round trip: exactly what secrets/vault/vault.go's isLoopback
// accepts, and nothing a hostname's eventual DNS answer could widen it to.
func Test_isLoopbackHost(t *testing.T) {
	loopback := []string{
		"localhost",
		"127.0.0.1",
		"127.0.0.53",
		"::1",
	}
	for _, host := range loopback {
		t.Run(host+" is loopback", func(t *testing.T) {
			require.True(t, isLoopbackHost(host))
		})
	}

	notLoopback := []string{
		// A public IP literal: the plain case the exemption must not reach.
		"93.184.216.34",
		// A private, non-loopback range (RFC 1918) — "not on the public
		// internet" is not the rule; "is this machine" is.
		"10.0.0.5",
		"192.168.1.1",
		// A hostname that is not the literal string "localhost". Nothing here
		// resolves DNS, so a name that *would* resolve to 127.0.0.1 today is
		// still refused: trusting a name because of what it might resolve to
		// is exactly the ambiguity CLAUDE.md's tenancy-boundary lesson warns
		// against, applied to a different boundary.
		"internal.example.corp",
		"loopback.example.com",
		// Not an address at all.
		"",
	}
	for _, host := range notLoopback {
		t.Run(host+" is not loopback", func(t *testing.T) {
			require.False(t, isLoopbackHost(host))
		})
	}
}

// Test_refuseCleartextCredential pins the refusal itself: which inputs
// trigger it, which destinations exempt it, and the exact wording a workflow
// author sees, mirroring secrets/vault/vault.go:751-766's idiom for the same
// situation.
func Test_refuseCleartextCredential(t *testing.T) {
	bearerInputs := &Task_HTTP_Inputs{
		Bearer: &Value{Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}
	partnerAPI := "partner-api"
	credentialInputs := &Task_HTTP_Inputs{Credential: &partnerAPI}
	plainInputs := &Task_HTTP_Inputs{}

	mustParse := func(t *testing.T, raw string) *url.URL {
		t.Helper()
		u, err := url.Parse(raw)
		require.NoError(t, err)
		return u
	}

	t.Run("a bearer credential to a public http destination is refused", func(t *testing.T) {
		err := refuseCleartextCredential(bearerInputs, mustParse(t, "http://example.com/webhook"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "http://example.com/webhook")
		require.Contains(t, err.Error(), "would send a credential in cleartext")
		require.Contains(t, err.Error(), "use https")
		require.Contains(t, err.Error(), "loopback")

		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindPolicyDenied, taskErr.Kind)
	})

	t.Run("a JIT credential target to a public http destination is refused", func(t *testing.T) {
		err := refuseCleartextCredential(credentialInputs, mustParse(t, "http://example.com/webhook"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "would send a credential in cleartext")
	})

	t.Run("a bearer credential to a private, non-loopback http destination is still refused", func(t *testing.T) {
		// The boundary CLAUDE.md's own worked examples keep re-finding: "not
		// on the public internet" is not the exemption, "is this machine" is.
		err := refuseCleartextCredential(bearerInputs, mustParse(t, "http://10.0.0.5/webhook"))
		require.Error(t, err)
	})

	t.Run("a bearer credential to https is not refused", func(t *testing.T) {
		require.NoError(t, refuseCleartextCredential(bearerInputs, mustParse(t, "https://example.com/webhook")))
	})

	t.Run("a bearer credential to loopback by IP is not refused", func(t *testing.T) {
		require.NoError(t, refuseCleartextCredential(bearerInputs, mustParse(t, "http://127.0.0.1:9200/webhook")))
	})

	t.Run("a bearer credential to loopback by name is not refused", func(t *testing.T) {
		require.NoError(t, refuseCleartextCredential(bearerInputs, mustParse(t, "http://localhost:9200/webhook")))
	})

	t.Run("a bearer credential to IPv6 loopback is not refused", func(t *testing.T) {
		require.NoError(t, refuseCleartextCredential(bearerInputs, mustParse(t, "http://[::1]:9200/webhook")))
	})

	t.Run("a request carrying no credential is never refused, on http or https", func(t *testing.T) {
		require.NoError(t, refuseCleartextCredential(plainInputs, mustParse(t, "http://example.com/webhook")))
		require.NoError(t, refuseCleartextCredential(plainInputs, mustParse(t, "https://example.com/webhook")))
	})
}
