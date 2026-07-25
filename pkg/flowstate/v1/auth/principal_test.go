package auth_test

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

// TestPrincipalIdentity checks the identity authorization decisions are keyed
// on, including that the zero value cannot be mistaken for a real caller.
func TestPrincipalIdentity(t *testing.T) {
	tests := []struct {
		name          string
		principal     auth.Principal
		wantID        string
		wantString    string
		wantZero      bool
		wantAnonymous bool
	}{
		{
			name:       "a workload from a CI provider",
			principal:  auth.Principal{Issuer: "https://token.actions.githubusercontent.com", Subject: "repo:picatz/flowstate:ref:refs/heads/main", Role: "deployer"},
			wantID:     "https://token.actions.githubusercontent.com#repo:picatz/flowstate:ref:refs/heads/main",
			wantString: "https://token.actions.githubusercontent.com#repo:picatz/flowstate:ref:refs/heads/main (deployer)",
		},
		{
			name:       "a caller with no role",
			principal:  auth.Principal{Issuer: "https://issuer.example.com", Subject: "runner"},
			wantID:     "https://issuer.example.com#runner",
			wantString: "https://issuer.example.com#runner",
		},
		{
			name:       "the zero value, which is nobody",
			principal:  auth.Principal{},
			wantID:     "",
			wantString: "unauthenticated",
			wantZero:   true,
		},
		{
			name:          "the anonymous caller",
			principal:     auth.AnonymousPrincipal(),
			wantID:        auth.AnonymousIssuer + "#" + auth.AnonymousSubject,
			wantString:    auth.AnonymousIssuer + "#" + auth.AnonymousSubject + " (" + auth.AnonymousRole + ")",
			wantAnonymous: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantID, test.principal.ID())
			require.Equal(t, test.wantString, test.principal.String())
			require.Equal(t, test.wantZero, test.principal.IsZero())
			require.Equal(t, test.wantAnonymous, test.principal.IsAnonymous())
		})
	}
}

// TestPrincipalIdentityDistinguishesIssuers checks that the same subject from two
// issuers is two different callers, which is why the identity includes both.
func TestPrincipalIdentityDistinguishesIssuers(t *testing.T) {
	first := auth.Principal{Issuer: "https://a.example.com", Subject: "runner"}
	second := auth.Principal{Issuer: "https://b.example.com", Subject: "runner"}

	require.NotEqual(t, first.ID(), second.ID())
}

// TestPrincipalClaims checks the accessors that spare handlers from
// type-switching on decoded JSON.
func TestPrincipalClaims(t *testing.T) {
	principal := auth.Principal{
		Issuer:   "https://issuer.example.com",
		Subject:  "runner",
		Audience: []string{"flowstate", "https://flowstate.example.com"},
		Claims: map[string]any{
			"email":          "someone@example.com",
			"email_verified": true,
			"groups":         []any{"platform", "sre"},
		},
	}

	email, ok := principal.StringClaim("email")
	require.True(t, ok)
	require.Equal(t, "someone@example.com", email)

	_, ok = principal.StringClaim("email_verified")
	require.False(t, ok, "a boolean claim is not a string claim")

	_, ok = principal.StringClaim("missing")
	require.False(t, ok)

	verified, ok := principal.Claim("email_verified")
	require.True(t, ok)
	require.Equal(t, true, verified)

	_, ok = principal.Claim("missing")
	require.False(t, ok)

	require.True(t, principal.HasAudience("flowstate"))
	require.True(t, principal.HasAudience("https://flowstate.example.com"))
	require.False(t, principal.HasAudience("some-other-service"))

	// Accessors on the zero value report absence rather than panicking.
	var zero auth.Principal
	_, ok = zero.Claim("anything")
	require.False(t, ok)
	require.False(t, zero.HasAudience("flowstate"))
}

// TestPrincipalLogValue checks that logging a caller records who it is without
// spilling its claims, which routinely carry personal data.
func TestPrincipalLogValue(t *testing.T) {
	principal := auth.Principal{
		Issuer:     "https://issuer.example.com",
		IssuerName: "idp",
		Subject:    "runner",
		Role:       "operator",
		ExpiresAt:  time.Date(2026, time.July, 25, 13, 0, 0, 0, time.UTC),
		Claims: map[string]any{
			"email":         "someone@example.com",
			"phone_number":  "+15555550123",
			"custom_secret": "do-not-log-me",
		},
	}

	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil))
	logger.Info("authenticated", "principal", principal)

	logged := buffer.String()

	require.Contains(t, logged, "https://issuer.example.com#runner")
	require.Contains(t, logged, "idp")
	require.Contains(t, logged, "operator")

	for _, sensitive := range []string{"someone@example.com", "+15555550123", "do-not-log-me"} {
		require.NotContains(t, logged, sensitive, "claims must not reach the logs")
	}

	// An unauthenticated caller logs as such rather than as an empty group.
	buffer.Reset()
	logger.Info("rejected", "principal", auth.Principal{})
	require.Contains(t, buffer.String(), "unauthenticated")
}
