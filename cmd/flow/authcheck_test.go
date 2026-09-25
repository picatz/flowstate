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

// TestAuthCheckDoesNotEchoACredentialWrittenIntoAnIssuerURL is the other half
// of what #1693's syntax-versus-validation split assumed.
//
// It reports the loader's own sentence for a policy that decodes, on the
// reasoning that a validation failure "quotes at most a value the policy
// declared". One validation failure quotes a value the policy declared that is
// itself a credential: an issuer written `https://user:password@host` is
// refused for carrying credentials, and the refusal used to quote the URL to
// say so. The operator holds the file either way; whoever reads the CI or
// support transcript this command's stderr lands in does not (Codex).
//
// Asserted through the command rather than against [auth.ValidateHTTPSURL],
// which has its own case, because the disclosure is the whole rendered
// sentence reaching a stream — and this command is the one that renders it.
func TestAuthCheckDoesNotEchoACredentialWrittenIntoAnIssuerURL(t *testing.T) {
	t.Parallel()

	const password = "hunter2-not-a-real-password"

	for name, tt := range map[string]struct {
		issuer  string
		wantErr string
		absent  []string
	}{
		"userinfo url.Parse reads as userinfo": {
			issuer:  "https://acct9:" + password + "@issuer.example.com",
			wantErr: "must not include credentials",
			absent:  []string{password, "acct9"},
		},
		// A password whose leading run is all digits parses as a *port*, so
		// the URL is well formed and carries no userinfo by url.Parse's
		// reading. Past picatz/flowstate#2038, auth.ValidateHTTPSURL's own
		// credentials check catches this directly — the authority is
		// ambiguous (a colon that could introduce a port is present) and an
		// `@` survives in the query — rather than deferring to
		// validateIssuerURL, which used to be the refusal an operator saw
		// here and carried the whole credential doing it (flowstate-reviewer).
		"a credential url.Parse reads as a port": {
			issuer:  "https://acct9:2024?" + password + "@issuer.example.com",
			wantErr: "must not include credentials",
			absent:  []string{password, "acct9"},
		},
		// Both misreads at once: digit-leading password (so url.Parse calls it
		// a port and the URL is well formed) and a percent-encoded delimiter.
		// auth.ValidateHTTPSURL's search recognizes `%40` the same as a
		// literal `@`.
		"a port misread whose delimiter is percent-encoded": {
			issuer:  "https://acct9:2024?" + password + "%40issuer.example.com",
			wantErr: "must not include credentials",
			absent:  []string{password, "acct9"},
		},
		// The delimiters mixed: the slash keeps the URL parseable, so nothing
		// upstream calls it malformed, and it puts the rest of the credential
		// past where a before-first-slash read used to stop. Past
		// picatz/flowstate#2038, auth.ValidateHTTPSURL's own credentials
		// check searches the whole of the path, query and fragment together
		// once the authority is ambiguous, so this reaches it directly too —
		// see the comment on that check.
		"a port misread whose credential spans a slash and a query": {
			issuer:  "https://acct9:2024/s3c?" + password + "@issuer.example.com",
			wantErr: "must not include credentials",
			absent:  []string{password, "acct9"},
		},
		"the same misread with a fragment": {
			issuer:  "https://acct9:007#" + password + "@issuer.example.com",
			wantErr: "must not include credentials",
			absent:  []string{password, "acct9"},
		},
		// A colon with nothing after it: url.Parse reads Host as `acct9:`
		// and Port() as "", the empty string being a valid (if useless)
		// port — a shape url.Parse's own Port() cannot distinguish from a
		// host with no port at all, so auth.ValidateHTTPSURL keys on the
		// colon itself rather than on Port() != "" (flowstate-reviewer,
		// urlprobe).
		"a port misread behind an empty port": {
			issuer:  "https://acct9:/" + password + "@issuer.example.com",
			wantErr: "must not include credentials",
			absent:  []string{password, "acct9"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			policy := "issuers:\n  - name: vendor\n    issuer: " + tt.issuer +
				"\n    audiences: [flowstate]\n"
			path := filepath.Join(t.TempDir(), "trust.yaml")
			require.NoError(t, os.WriteFile(path, []byte(policy), 0o600))

			res := runFlowStdin(t, "not-reached", "auth", "check", "--auth-policy", path, "--token-file", "-")
			assert.Equal(t, exitCodeFailure, res.ExitCode)

			// The refusal still arrives, and still says what is wrong: this is
			// a redaction, not a withholding, for the reason every other one
			// in this repository gives — the sentence is the only thing that
			// says why.
			unwrapped := strings.Join(strings.Fields(res.Stderr), " ")
			assert.Contains(t, unwrapped, tt.wantErr)
			assert.Contains(t, unwrapped, "issuer.example.com",
				"the operator cannot find the entry this is about")

			for _, secret := range tt.absent {
				assert.NotContains(t, res.Output(), secret,
					"credential material reached the command's output")
			}
		})
	}
}

// TestAuthCheckRedactsAQueryBearingIssuerEvenWithoutACredential records the
// cost of reading a query- or fragment-bearing issuer greedily, so that the
// choice is a decision rather than something a later reader discovers.
//
// An issuer carrying a query is not a usable issuer whatever else is right
// about it, so validateIssuerURL has no well-formed reading to protect and
// takes the wider one. The price is this: a path `@` in such an issuer takes
// the host with it, and the operator reads `[redacted]@handler` where no
// credential was. That is the same trade the redaction makes everywhere it
// cannot tell which `@` a person meant, and the entry is still addressed by
// the `issuers[0]:` frame the loader wraps around it.
func TestAuthCheckRedactsAQueryBearingIssuerEvenWithoutACredential(t *testing.T) {
	t.Parallel()

	policy := "issuers:\n  - name: vendor\n    issuer: https://issuer.example.com/callback@handler?tenant=a" +
		"\n    audiences: [flowstate]\n"
	path := filepath.Join(t.TempDir(), "trust.yaml")
	require.NoError(t, os.WriteFile(path, []byte(policy), 0o600))

	res := runFlowStdin(t, "not-reached", "auth", "check", "--auth-policy", path, "--token-file", "-")
	assert.Equal(t, exitCodeFailure, res.ExitCode)

	unwrapped := strings.Join(strings.Fields(res.Stderr), " ")
	assert.Contains(t, unwrapped, "must not include a query string or fragment")
	assert.Contains(t, unwrapped, "[redacted]@handler",
		"the greedy read stopped somewhere else")
	assert.Contains(t, unwrapped, "issuers[0]",
		"the loader's frame is what still addresses the entry")
}

// TestAuthCheckReportsTheLoadersOwnRefusal is #1693: a policy that decodes and
// fails validation is refused with the loader's own sentence — the field and
// the rule `flow server` would print for the same bytes — rather than the fixed
// "policy is malformed" that hid it. The redaction is kept for what it exists
// for, a document the decoder could not read, whose diagnostic quotes source
// (see TestAuthCheckNeverEchoesATokenReadAsPolicy).
func TestAuthCheckReportsTheLoadersOwnRefusal(t *testing.T) {
	t.Parallel()

	issuer := authCheckIssuer(t)
	nameless := "issuers:\n  - issuer: " + issuer.URL() + "\n    audiences: [flowstate]\n"
	path := filepath.Join(t.TempDir(), "trust.yaml")
	require.NoError(t, os.WriteFile(path, []byte(nameless), 0o600))

	// The sentence the server's loader gives the same bytes, with no path in it.
	_, loaderErr := auth.ParsePolicy([]byte(nameless))
	require.Error(t, loaderErr)
	require.NotErrorIs(t, loaderErr, auth.ErrPolicySyntax, "the fixture decodes; it fails validation, which is the case under test")

	res := runFlowStdin(t, "not-reached", "auth", "check", "--auth-policy", path, "--token-file", "-")
	assert.Equal(t, exitCodeFailure, res.ExitCode)

	// The terminal renderer wraps a long sentence at the column width, so the
	// comparison is over words rather than bytes.
	unwrapped := strings.Join(strings.Fields(res.Stderr), " ")
	assert.Contains(t, unwrapped, "issuers[0]: name is required", "the loader's refusal was hidden: %s", res.Stderr)
	assert.Contains(t, unwrapped, "parsing auth policy: "+loaderErr.Error(),
		"auth check and the loader disagree about the same bytes")
	assert.NotContains(t, unwrapped, "policy is malformed")
}
