package auth_test

import (
	"slices"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// TestPolicyValidate checks that unusable configuration is refused, since a
// policy that does not mean what its author thought is a security problem rather
// than an inconvenience.
func TestPolicyValidate(t *testing.T) {
	valid := auth.TrustedIssuer{
		Name:      "idp",
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"flowstate"},
	}

	// spoil returns a copy of the valid issuer with one thing changed.
	spoil := func(change func(*auth.TrustedIssuer)) auth.Policy {
		issuer := valid
		change(&issuer)
		return auth.Policy{Issuers: []auth.TrustedIssuer{issuer}}
	}

	tests := []struct {
		name    string
		policy  auth.Policy
		wantErr bool
	}{
		{
			name:   "a single issuer with an audience",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{valid}},
		},
		{
			name: "several entries for one issuer",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "main", Issuer: valid.Issuer, Audiences: []string{"flowstate"}, Require: []auth.ClaimRule{auth.RequireClaim("ref", "refs/heads/main")}},
				{Name: "other", Issuer: valid.Issuer, Audiences: []string{"flowstate"}},
			}},
		},
		{
			name:    "no issuers, which would trust nobody",
			policy:  auth.Policy{},
			wantErr: true,
		},
		{
			name:    "an issuer with no name to audit against",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Name = "" }),
			wantErr: true,
		},
		{
			name:    "no issuer URL",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Issuer = "" }),
			wantErr: true,
		},
		{
			name:    "an issuer that is not a URL",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Issuer = "issuer.example.com" }),
			wantErr: true,
		},
		{
			name:    "an issuer reachable only over plain HTTP",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Issuer = "http://issuer.example.com" }),
			wantErr: true,
		},
		{
			name:   "a loopback issuer over plain HTTP, for local development",
			policy: spoil(func(i *auth.TrustedIssuer) { i.Issuer = "http://127.0.0.1:8080/realms/flowstate" }),
		},
		{
			name:   "a localhost issuer over plain HTTP",
			policy: spoil(func(i *auth.TrustedIssuer) { i.Issuer = "http://localhost:8080/realms/flowstate" }),
		},
		{
			name:    "an internal hostname over plain HTTP, which is not loopback",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Issuer = "http://issuer.internal:8080" }),
			wantErr: true,
		},
		{
			name:    "an issuer that is not served over HTTP at all",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Issuer = "ftp://issuer.example.com" }),
			wantErr: true,
		},
		{
			name:    "an issuer with a query string",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Issuer = "https://issuer.example.com?tenant=a" }),
			wantErr: true,
		},
		{
			name:    "no audience, which would accept tokens minted for anything",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Audiences = nil }),
			wantErr: true,
		},
		{
			name:    "an empty audience",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Audiences = []string{""} }),
			wantErr: true,
		},
		{
			name:    "the none algorithm",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Algorithms = []jwa.Algorithm{jwa.None} }),
			wantErr: true,
		},
		{
			name:    "an HMAC algorithm, which has no published key",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Algorithms = []jwa.Algorithm{jwa.HS256} }),
			wantErr: true,
		},
		{
			name:    "an algorithm this package cannot verify",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Algorithms = []jwa.Algorithm{jwa.ES384} }),
			wantErr: true,
		},
		{
			name:    "an unknown algorithm",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Algorithms = []jwa.Algorithm{"RS255"} }),
			wantErr: true,
		},
		{
			name:   "an explicit algorithm allowlist",
			policy: spoil(func(i *auth.TrustedIssuer) { i.Algorithms = []jwa.Algorithm{jwa.RS256, jwa.ES256} }),
		},
		{
			name:    "a claim rule with no claim",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Require = []auth.ClaimRule{{AnyOf: []string{"x"}}} }),
			wantErr: true,
		},
		{
			name:    "a claim rule with no accepted values",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Require = []auth.ClaimRule{{Claim: "sub"}} }),
			wantErr: true,
		},
		{
			name:    "a claim rule accepting an empty value",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Require = []auth.ClaimRule{auth.RequireClaim("sub", "")} }),
			wantErr: true,
		},
		{
			name: "a claim rule on the issuer, which is already matched exactly",
			policy: spoil(func(i *auth.TrustedIssuer) {
				i.Require = []auth.ClaimRule{auth.RequireClaim("iss", "https://issuer.example.com")}
			}),
			wantErr: true,
		},
		{
			name:    "a claim rule on a timestamp",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Require = []auth.ClaimRule{auth.RequireClaim("exp", "1234567890")} }),
			wantErr: true,
		},
		{
			name:   "a claim rule on the audience",
			policy: spoil(func(i *auth.TrustedIssuer) { i.Require = []auth.ClaimRule{auth.RequireClaim("aud", "flowstate")} }),
		},
		{
			name:    "a negative maximum token age",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.MaxTokenAge = -time.Minute }),
			wantErr: true,
		},
		{
			name:    "a key set URL that is not a URL",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.JWKSURL = "keys" }),
			wantErr: true,
		},
		{
			name:    "a key set URL served over plain HTTP",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.JWKSURL = "http://keys.example.com/jwks" }),
			wantErr: true,
		},
		{
			name:   "an explicit key set URL",
			policy: spoil(func(i *auth.TrustedIssuer) { i.JWKSURL = "https://issuer.example.com/keys" }),
		},
		{
			name: "two entries with the same name",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "idp", Issuer: valid.Issuer, Audiences: []string{"flowstate"}},
				{Name: "idp", Issuer: "https://other.example.com", Audiences: []string{"flowstate"}},
			}},
			wantErr: true,
		},
		{
			name: "entries for one issuer that disagree about where its keys are",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "a", Issuer: valid.Issuer, Audiences: []string{"flowstate"}, JWKSURL: "https://issuer.example.com/keys"},
				{Name: "b", Issuer: valid.Issuer, Audiences: []string{"flowstate"}, JWKSURL: "https://issuer.example.com/other-keys"},
			}},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.policy.Validate()

			if !test.wantErr {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)

			// A policy that does not validate must not produce a verifier either.
			verifier, err := auth.NewOIDCVerifier(test.policy)
			require.Error(t, err)
			require.Nil(t, verifier)
		})
	}
}

// TestParsePolicy checks that a policy can live in a file an operator reviews.
func TestParsePolicy(t *testing.T) {
	t.Run("YAML", func(t *testing.T) {
		policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: github-actions-main
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    algorithms: [RS256]
    role: deployer
    max_token_age: 10m
    require:
      - claim: repository
        any_of: [picatz/flowstate]
      - claim: ref
        any_of: [refs/heads/main, refs/tags/v1]
  - name: cluster
    issuer: https://kubernetes.default.svc.cluster.local
    audiences: [flowstate]
    jwks_url: https://kubernetes.default.svc.cluster.local/openid/v1/jwks
    role: runner
`))
		require.NoError(t, err)
		require.Len(t, policy.Issuers, 2)

		actions := policy.Issuers[0]
		require.Equal(t, "github-actions-main", actions.Name)
		require.Equal(t, "https://token.actions.githubusercontent.com", actions.Issuer)
		require.Equal(t, []string{"flowstate"}, actions.Audiences)
		require.Equal(t, []jwa.Algorithm{jwa.RS256}, actions.Algorithms)
		require.Equal(t, "deployer", actions.Role)
		require.Equal(t, 10*time.Minute, actions.MaxTokenAge)
		require.Equal(t, []auth.ClaimRule{
			{Claim: "repository", AnyOf: []string{"picatz/flowstate"}},
			{Claim: "ref", AnyOf: []string{"refs/heads/main", "refs/tags/v1"}},
		}, actions.Require)

		cluster := policy.Issuers[1]
		require.Equal(t, "https://kubernetes.default.svc.cluster.local/openid/v1/jwks", cluster.JWKSURL)
		require.Empty(t, cluster.Algorithms, "an issuer without an allowlist uses the default one")
	})

	t.Run("JSON", func(t *testing.T) {
		policy, err := auth.ParsePolicy([]byte(`{
			"issuers": [{
				"name": "idp",
				"issuer": "https://issuer.example.com",
				"audiences": ["flowstate"],
				"require": [{"claim": "sub", "any_of": ["runner"]}]
			}]
		}`))
		require.NoError(t, err)
		require.Len(t, policy.Issuers, 1)
		require.Equal(t, auth.RequireClaim("sub", "runner"), policy.Issuers[0].Require[0])
	})

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "a misspelled field, which would silently drop a restriction",
			input: "issuers:\n  - name: idp\n    issuer: https://issuer.example.com\n    audiences: [flowstate]\n    requires:\n      - claim: sub\n        any_of: [runner]\n",
		},
		{
			name:  "a policy that parses but does not validate",
			input: "issuers:\n  - name: idp\n    issuer: https://issuer.example.com\n",
		},
		{
			name:  "an empty document",
			input: "",
		},
		{
			name:  "not YAML at all",
			input: "\t\tissuers: [",
		},
		{
			name:  "an unparseable duration",
			input: "issuers:\n  - name: idp\n    issuer: https://issuer.example.com\n    audiences: [flowstate]\n    max_token_age: soon\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := auth.ParsePolicy([]byte(test.input))
			require.Error(t, err)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Empty(t, policy.Issuers)
		})
	}
}

// TestDefaultAlgorithms checks that the default allowlist cannot be talked into
// accepting an unsigned or symmetric token, and that a caller cannot change it
// for everyone else.
func TestDefaultAlgorithms(t *testing.T) {
	algorithms := auth.DefaultAlgorithms()

	require.NotEmpty(t, algorithms)

	for _, forbidden := range []jwa.Algorithm{jwa.None, jwa.HS256, jwa.HS384, jwa.HS512} {
		require.False(t, slices.Contains(algorithms, forbidden), "%q must never be allowed by default", forbidden)
	}

	require.True(t, slices.Contains(algorithms, jwa.RS256))
	require.True(t, slices.Contains(algorithms, jwa.ES256))
	require.True(t, slices.Contains(algorithms, jwa.EdDSA))

	// ES384 is left out deliberately: the underlying JOSE library cannot verify
	// SHA-384 ECDSA signatures.
	require.False(t, slices.Contains(algorithms, jwa.ES384))

	// The returned slice is a copy, so a caller sorting or truncating it does not
	// change what every other issuer accepts.
	algorithms[0] = jwa.None
	require.False(t, slices.Contains(auth.DefaultAlgorithms(), jwa.None))
}

// TestRequireClaimHelpers checks the constructors operators reach for first.
func TestRequireClaimHelpers(t *testing.T) {
	require.Equal(t, auth.ClaimRule{Claim: "sub", AnyOf: []string{"runner"}}, auth.RequireClaim("sub", "runner"))
	require.Equal(t, auth.ClaimRule{Claim: "ref", AnyOf: []string{"main", "release"}}, auth.RequireClaimAnyOf("ref", "main", "release"))
}
