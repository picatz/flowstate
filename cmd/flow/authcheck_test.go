package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

func authCheckIssuer(t *testing.T) *authtest.Issuer {
	t.Helper()
	issuer := authtest.NewIssuer()
	t.Cleanup(func() { require.NoError(t, issuer.Close()) })
	return issuer
}

func writeAuthCheckPolicy(t *testing.T, issuers ...auth.TrustedIssuer) string {
	t.Helper()
	data, err := json.Marshal(auth.Policy{
		Issuers: issuers,
		Egress: &netpolicy.EgressConfig{
			AllowLoopback: true,
			Schemes:       []string{"http", "https"},
		},
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "trust.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func writeAuthCheckToken(t *testing.T, token string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(token+"\n"), 0o600))
	return path
}

func authCheckEntry(name, issuer string, rules ...auth.ClaimRule) auth.TrustedIssuer {
	return auth.TrustedIssuer{
		Name:      name,
		Issuer:    issuer,
		Audiences: []string{"flowstate"},
		Require:   rules,
		Namespace: "acme",
	}
}

// TestAuthCheckDiagnosesAPartialOverlap drives the exact shape #1192 names:
// neither entry contains the other, callers exist that reach each alone, and a
// caller in their intersection is refused with both source rows attributed.
func TestAuthCheckDiagnosesAPartialOverlap(t *testing.T) {
	t.Parallel()
	issuer := authCheckIssuer(t)
	policy := writeAuthCheckPolicy(t,
		authCheckEntry("repository", issuer.URL(), auth.RequireClaim("repository", "acme/app")),
		authCheckEntry("main-branch", issuer.URL(), auth.RequireClaim("ref", "refs/heads/main")),
	)

	for _, test := range []struct {
		name       string
		claims     map[string]any
		wantOutput string
		wantCode   int
	}{
		{
			name:       "repository entry alone",
			claims:     map[string]any{"repository": "acme/app", "ref": "refs/heads/topic"},
			wantOutput: `accepted by issuers[0] ("repository")`,
		},
		{
			name:       "branch entry alone",
			claims:     map[string]any{"repository": "somebody/else", "ref": "refs/heads/main"},
			wantOutput: `accepted by issuers[1] ("main-branch")`,
		},
		{
			name:       "intersection is ambiguous",
			claims:     map[string]any{"repository": "acme/app", "ref": "refs/heads/main"},
			wantOutput: `issuers[0] ("repository"), issuers[1] ("main-branch")`,
			wantCode:   exitCodeFailure,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			token := issuer.MintToken(test.claims, authtest.WithAudience("flowstate"))
			res := runFlow(t, "auth", "check", "--auth-policy", policy, "--token-file", writeAuthCheckToken(t, token))
			assert.Equal(t, test.wantCode, res.ExitCode, res.Output())
			assert.Contains(t, res.Output(), test.wantOutput)
			assert.NotContains(t, res.Output(), token)
		})
	}
}

// TestAuthCheckAcceptsDisjointNoneOfTiers is the negative direction: the
// canonical any_of/none_of pair produces no ambiguity, and each concrete token
// is attributed to exactly its own policy row.
func TestAuthCheckAcceptsDisjointNoneOfTiers(t *testing.T) {
	t.Parallel()
	issuer := authCheckIssuer(t)
	policy := writeAuthCheckPolicy(t,
		authCheckEntry("main", issuer.URL(),
			auth.RequireClaim("repository", "acme/app"),
			auth.RequireClaim("ref", "refs/heads/main")),
		authCheckEntry("other", issuer.URL(),
			auth.RequireClaim("repository", "acme/app"),
			auth.RequireClaimNoneOf("ref", "refs/heads/main")),
	)

	for _, test := range []struct {
		ref  string
		want string
	}{
		{ref: "refs/heads/main", want: `issuers[0] ("main")`},
		{ref: "refs/heads/topic", want: `issuers[1] ("other")`},
	} {
		token := issuer.MintToken(map[string]any{"repository": "acme/app", "ref": test.ref},
			authtest.WithAudience("flowstate"))
		res := runFlowStdin(t, token, "auth", "check", "--auth-policy", policy, "--token-file", "-")
		require.NoError(t, res.Err, res.Output())
		assert.Contains(t, res.Stdout, test.want)
		assert.Empty(t, res.Stderr)
	}
}

// TestAuthCheckRedactsEveryCredentialDerivedRefusal checks both a verified but
// non-comparable claim and malformed input. The former could have printed a
// signed claim through ClaimMismatchError; the latter could have printed parser
// text. The command emits only PublicReason's fixed classifications.
func TestAuthCheckRedactsEveryCredentialDerivedRefusal(t *testing.T) {
	t.Parallel()
	issuer := authCheckIssuer(t)
	policy := writeAuthCheckPolicy(t,
		authCheckEntry("repository", issuer.URL(), auth.RequireClaim("repository", "acme/app")),
	)

	secretish := "do-not-print-this-credential-value"
	tests := []struct {
		name  string
		token string
		want  string
	}{
		{
			name: "non-comparable claim",
			token: issuer.MintToken(map[string]any{
				"repository": map[string]any{"sensitive": secretish},
			}, authtest.WithAudience("flowstate")),
			want: "token is not accepted by the trust policy",
		},
		{name: "malformed token", token: "not-a-jwt." + secretish, want: "malformed token"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			res := runFlowStdin(t, test.token, "auth", "check", "--auth-policy", policy, "--token-file", "-")
			assert.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
			assert.Contains(t, res.Stderr, test.want)
			assert.NotContains(t, res.Output(), test.token)
			assert.NotContains(t, res.Output(), secretish)
		})
	}
}

func TestAuthCheckMalformedPolicyAndNoMatchFail(t *testing.T) {
	t.Parallel()
	t.Run("malformed policy", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "trust.yaml")
		require.NoError(t, os.WriteFile(path, []byte("issuers: [not-an-entry]"), 0o600))
		res := runFlowStdin(t, "not-reached", "auth", "check", "--auth-policy", path, "--token-file", "-")
		assert.Equal(t, exitCodeFailure, res.ExitCode)
		assert.Contains(t, res.Stderr, "parsing auth policy")
	})

	t.Run("no entry matches", func(t *testing.T) {
		t.Parallel()
		issuer := authCheckIssuer(t)
		policy := writeAuthCheckPolicy(t,
			authCheckEntry("repository", issuer.URL(), auth.RequireClaim("repository", "acme/app")),
		)
		token := issuer.MintToken(map[string]any{"repository": "other/app"}, authtest.WithAudience("flowstate"))
		res := runFlowStdin(t, token, "auth", "check", "--auth-policy", policy, "--token-file", "-")
		assert.Equal(t, exitCodeFailure, res.ExitCode)
		assert.Contains(t, res.Stderr, "token is not accepted by the trust policy")
	})
}

// TestAuthCheckNeverEchoesAPositionalToken protects the input boundary itself.
// Even misuse is redacted: the custom Args check does not let Cobra quote the
// unexpected argument in the usage error it returns.
func TestAuthCheckNeverEchoesAPositionalToken(t *testing.T) {
	t.Parallel()
	secret := "raw-token-that-must-not-reach-diagnostics"
	res := runFlow(t, "auth", "check", secret)
	assert.Equal(t, exitCodeUsage, res.ExitCode, res.Output())
	assert.NotContains(t, res.Output(), secret)
	assert.Contains(t, res.Stderr, "takes no positional arguments")

	cmd := flowCommand(t, "auth", "check")
	assert.Nil(t, cmd.Flags().Lookup("token"), "a raw-token flag would put the credential in argv and completion")
	assert.NotContains(t, strings.Fields(cmd.Use), "<token>")
}

// TestAuthCheckNeverEchoesATokenMistakenForAFilePath is the less obvious argv
// misuse: --token-file is a legitimate flag, so its value reaches the file
// source, whose ordinary error names the path. Here that path may itself be the
// credential, and the command must discard the underlying error rather than
// print it.
func TestAuthCheckNeverEchoesATokenMistakenForAFilePath(t *testing.T) {
	t.Parallel()
	issuer := authCheckIssuer(t)
	policy := writeAuthCheckPolicy(t, authCheckEntry("repository", issuer.URL()))
	token := issuer.MintToken(nil, authtest.WithAudience("flowstate"))

	res := runFlow(t, "auth", "check", "--auth-policy", policy, "--token-file", token)
	assert.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	assert.Contains(t, res.Stderr, "credential could not be read")
	assert.NotContains(t, res.Output(), token)
}

// TestAuthCheckNeverEchoesATokenMistakenForAPolicyPath covers the corresponding
// flag-value swap. os.ReadFile errors include their path, so the command must
// not wrap the underlying error when that "path" may be the raw credential.
func TestAuthCheckNeverEchoesATokenMistakenForAPolicyPath(t *testing.T) {
	t.Parallel()
	issuer := authCheckIssuer(t)
	token := issuer.MintToken(nil, authtest.WithAudience("flowstate"))

	res := runFlow(t, "auth", "check", "--auth-policy", token, "--token-file", writeAuthCheckToken(t, token))
	assert.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	assert.Contains(t, res.Stderr, "policy could not be read")
	assert.NotContains(t, res.Output(), token)
}

// TestAuthCheckNeverEchoesATokenReadAsPolicy covers a swapped pair of valid
// paths. Policy parsers can quote the malformed source, which in this case is
// the credential itself, so their detailed error must not reach output.
func TestAuthCheckNeverEchoesATokenReadAsPolicy(t *testing.T) {
	t.Parallel()
	issuer := authCheckIssuer(t)
	token := issuer.MintToken(nil, authtest.WithAudience("flowstate"))
	tokenPath := writeAuthCheckToken(t, token)
	policyPath := writeAuthCheckPolicy(t, authCheckEntry("repository", issuer.URL()))

	res := runFlow(t, "auth", "check", "--auth-policy", tokenPath, "--token-file", policyPath)
	assert.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	assert.Contains(t, res.Stderr, "policy is malformed")
	assert.NotContains(t, res.Output(), token)
}
