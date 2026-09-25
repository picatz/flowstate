package auth_test

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
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
			name:   "an explicit key set file",
			policy: spoil(func(i *auth.TrustedIssuer) { i.JWKSFile = "/etc/flowstate/issuer.jwks" }),
		},
		{
			name: "two key set sources",
			policy: spoil(func(i *auth.TrustedIssuer) {
				i.JWKSURL = "https://issuer.example.com/keys"
				i.JWKSFile = "/etc/flowstate/issuer.jwks"
			}),
			wantErr: true,
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
		{
			name: "entries for one issuer that disagree between URL and file keys",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "a", Issuer: valid.Issuer, Audiences: []string{"flowstate"}, JWKSURL: "https://issuer.example.com/keys"},
				{Name: "b", Issuer: valid.Issuer, Audiences: []string{"flowstate"}, JWKSFile: "/etc/flowstate/keys.jwks"},
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
    actions: [workload.run, workload.read]
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
		require.Equal(t, auth.ActionScopes{"workload.run", "workload.read"}, actions.Actions)
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

	t.Run("action presence is preserved", func(t *testing.T) {
		policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: unrestricted
    issuer: https://issuer.example.com
    audiences: [flowstate]
  - name: denied
    issuer: https://other.example.com
    audiences: [flowstate]
    actions: []
`))
		require.NoError(t, err)
		require.Nil(t, policy.Issuers[0].Actions)
		require.NotNil(t, policy.Issuers[1].Actions)
		require.Empty(t, policy.Issuers[1].Actions)

		encoded, err := json.Marshal(policy)
		require.NoError(t, err)
		roundTrip, err := auth.ParsePolicy(encoded)
		require.NoError(t, err)
		require.Nil(t, roundTrip.Issuers[0].Actions)
		require.NotNil(t, roundTrip.Issuers[1].Actions)

		encoded, err = yaml.Marshal(policy)
		require.NoError(t, err)
		roundTrip, err = auth.ParsePolicy(encoded)
		require.NoError(t, err)
		require.Nil(t, roundTrip.Issuers[0].Actions)
		require.NotNil(t, roundTrip.Issuers[1].Actions)
	})

	t.Run("null actions are refused rather than treated as omitted", func(t *testing.T) {
		_, err := auth.ParsePolicy([]byte(`
issuers:
  - name: idp
    issuer: https://issuer.example.com
    audiences: [flowstate]
    actions: null
`))
		require.Error(t, err)
		require.ErrorContains(t, err, "actions is present but null")
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

// TestValidateHTTPSURL checks the host requirement against the shape url.Parse
// actually produces, not the shape it looks like it produces. url.Parse keeps a
// bare port in Host without a hostname (url.Parse("https://:443/x").Host ==
// ":443" while Hostname() == ""), so a check against Host rather than Hostname
// accepts a URL that names no host at all — see #971.
func TestValidateHTTPSURL(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantErr    string   // substring expected in the error, "" if no error expected
		wantAbsent []string // substrings the error must NOT carry, for a URL holding a credential
	}{
		{
			name: "a normal https URL",
			url:  "https://issuer.example.com",
		},
		{
			name: "an https URL with a path",
			url:  "https://issuer.example.com/.well-known/jwks.json",
		},
		{
			name: "http against loopback by name",
			url:  "http://localhost:8080",
		},
		{
			name: "http against loopback by address",
			url:  "http://127.0.0.1:8080",
		},
		{
			name:    "a host-free URL with a bare port",
			url:     "https://:443/x",
			wantErr: "must name a host",
		},
		{
			name:       "a host-free URL with credentials and a bare port",
			url:        "https://acct9@:443/x",
			wantErr:    "must name a host",
			wantAbsent: []string{"acct9"},
		},
		{
			name:    "no host at all",
			url:     "https:///x",
			wantErr: "must name a host",
		},
		{
			name:    "http against a non-loopback host",
			url:     "http://issuer.example.com",
			wantErr: "must use https",
		},
		{
			name:       "credentials in the URL",
			url:        "https://acct9:s3cr3t@issuer.example.com",
			wantErr:    "must not include credentials",
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// Both halves of the credential are gone and the host stays, so
			// the operator can still find the entry this is about.
			name:       "the refusal names the host it redacted the credentials out of",
			url:        "https://acct9:s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The shape with nothing to call url.URL.Redacted on: url.Parse
			// refuses a space in the userinfo, and its own error renders the
			// whole URL in front of the reason.
			name:       "credentials in a URL url.Parse will not read",
			url:        "https://acct9:s3c r3t@issuer.example.com",
			wantErr:    "is not a valid URL",
			wantAbsent: []string{"acct9", "s3c r3t"},
		},
		{
			// url.Parse's own reason quotes a piece of what it refused: a bad
			// percent escape in the password renders as `invalid URL escape
			// "%zz"`, which is three characters of that password arriving
			// after the URL around them was cleaned. So the reason is dropped
			// whenever anything was redacted.
			name:       "a credential holding a bad percent escape",
			url:        "https://acct9:hunter%zz@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" is not a valid URL`,
			wantAbsent: []string{"acct9", "hunter", "%zz"},
		},
		{
			// And the credential-free direction: with nothing redacted there
			// is nothing for the reason to be a fragment of, so it survives
			// and the diagnostic keeps saying what is actually wrong.
			name:    "a malformed URL with no credentials keeps its reason",
			url:     "https://issuer.example.com/%zz",
			wantErr: `invalid URL escape "%zz"`,
		},
		{
			// A mistyped authority delimiter. url.Parse reads both of these as
			// a URL with no host, so they are refused by the branch above the
			// credentials check — and a search for a literal `//` finds no
			// authority in the first and an empty one in the second.
			name:       "credentials after a single-slash delimiter",
			url:        "https:/acct9:s3cr3t@issuer.example.com",
			wantErr:    "must name a host",
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			name:       "credentials after a three-slash delimiter",
			url:        "https:///acct9:s3cr3t@issuer.example.com",
			wantErr:    "must name a host",
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// What holds isURLScheme up. Here the first colon is inside the
			// userinfo rather than after a scheme, and a guard that took
			// whatever precedes it as one would consume `//acct9:`, find no
			// slashes left, and hand the whole credential back — while
			// url.Parse reads the same string as an authority and refuses it
			// for carrying one. That is the fail-open direction, and without
			// this case deleting the validation from the guard leaves the
			// suite green (flowstate-reviewer).
			name:       "credentials after a scheme-relative delimiter",
			url:        "//acct9:s3cr3t@issuer.example.com",
			wantErr:    `issuer "//[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// A password holding an unescaped URL delimiter. The strict
			// reading stops the authority at the first `/`, `?` or `#`, so
			// `acct9:s3c` holds no `@` and the whole credential survived into
			// the refusal — which is why a string url.Parse has rejected is
			// searched to its end instead (Codex).
			name:       "a credential holding an unescaped slash",
			url:        "https://acct9:s3c/r3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" is not a valid URL`,
			wantAbsent: []string{"acct9", "s3c", "r3t"},
		},
		{
			name:       "a credential holding an unescaped question mark",
			url:        "https://acct9:s3c?r3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" is not a valid URL`,
			wantAbsent: []string{"acct9", "s3c", "r3t"},
		},
		{
			name:       "a credential holding an unescaped hash",
			url:        "https://acct9:s3c#r3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" is not a valid URL`,
			wantAbsent: []string{"acct9", "s3c", "r3t"},
		},
		{
			// A password whose leading run is all digits: url.Parse calls it a
			// *port*, so the URL is well formed and there is no userinfo, and
			// the rest of the credential lands past the delimiter where the
			// strict reading stopped, in the query this time rather than the
			// path. Past picatz/flowstate#2038's widened search, this
			// function's own credentials check catches it directly — the
			// authority is ambiguous (a port is present) and the query holds
			// an `@` — rather than deferring to validateIssuerURL or falling
			// through to the scheme check.
			name:       "a credential read as a port, in the query, under plain http",
			url:        "http://acct9:2024?s3cr3t@issuer.example.com",
			wantErr:    `issuer "http://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The same shape under https, so the credentials check is not
			// merely arriving ahead of a scheme refusal that would have
			// caught it anyway: this used to pass every check in this
			// function and reach validateIssuerURL, which refused it for the
			// query rather than the credential.
			name:       "a credential read as a port, in the query, under https",
			url:        "https://acct9:2024?s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The delimiter percent-encoded, which templating produces. Only a
			// malformed string reaches this: url.Parse refuses the port, the
			// greedy search finds no literal `@`, and without the encoded
			// spelling the credential stayed whole *and* the parse reason
			// quoted it a second time.
			name:       "a credential whose at sign is percent-encoded",
			url:        "https://acct9:s3cr3t%40issuer.example.com",
			wantErr:    `issuer "https://[redacted]%40issuer.example.com" is not a valid URL`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// Both misreads at once, which is the shape that slipped between
			// two searches looking for one spelling each: a digit-leading
			// password makes this well formed, so the greedy branch never
			// ran under the pre-picatz/flowstate#2038 code, and the delimiter
			// is encoded, so the strict one found no `@`. Past that fix, the
			// credentials check's own widened search recognizes `%40` the
			// same as a literal `@` and catches it directly.
			name:       "a percent-encoded delimiter behind a port misread, in the query",
			url:        "http://acct9:2024?s3cr3t%40issuer.example.com",
			wantErr:    `issuer "http://[redacted]%40issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// A username carrying an unescaped `@` moves url.Parse's split:
			// this is userinfo `ac`, host `t9`, port 2024, query. A search that
			// stopped at the authority found that first `@`, was satisfied, and
			// left the password behind (flowstate-reviewer). One region past
			// the authority is what closes it.
			name:       "a username holding an at sign in front of a port misread",
			url:        "https://ac@t9:2024?s3cr3t@issuer.example.com",
			wantErr:    "must not include credentials",
			wantAbsent: []string{"s3cr3t"},
		},
		{
			// The same username-`@` shape with a slash rather than a query,
			// which used to leak the password: url.Parse still splits
			// userinfo `ac`, host `t9`, port `2024`, path
			// `/s3cr3t@keys.example.com`, and the redaction this refusal
			// quotes was built from the region *before* the first slash — so
			// it found the early `@`, cut there, and then appended the
			// unbounded remainder of the raw string, password and all, past
			// the marker: `issuer "https://[redacted]@t9:2024/s3cr3t@keys.example.com"
			// must not include credentials` (flowstate-reviewer). Pre-existing
			// in the code this touches, reached only once
			// [hostCarriesPortDelimiter] made this URL's host ambiguous in
			// the first place, so fixed and pinned here rather than filed
			// separately.
			name:       "a username holding an at sign in front of a port misread, with a slash",
			url:        "https://ac@t9:2024/s3cr3t@keys.example.com",
			wantErr:    `issuer "https://[redacted]@keys.example.com" must not include credentials`,
			wantAbsent: []string{"t9", "s3cr3t"},
		},
		{
			// The cost of reading past the authority, with a real credential
			// present: the later `@` is the one cut at, so the host goes too.
			// Pinned so that narrowing the region later is a decision.
			name:       "a credential and a later at sign lose the host together",
			url:        "http://acct9:s3cr3t@issuer.example.com?cb=a@b",
			wantErr:    `issuer "http://[redacted]@b" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// picatz/flowstate#2038: a password whose leading run is all
			// digits parses as a *port*, so this URL is well formed, carries
			// no userinfo by url.Parse's reading, and used to be accepted —
			// dialing host `acct9`, never `issuer.example.com`, with the rest
			// of the credential riding the request path to it. Refused now,
			// ahead of the scheme check, because the port that made the read
			// well formed is exactly the marker that a slash-delimited read
			// cannot be trusted here.
			name:       "a credential url.Parse reads as a port and a path",
			url:        "http://acct9:2024/s3cr3t@issuer.example.com",
			wantErr:    `issuer "http://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The same shape under https, so the fix is not merely the
			// scheme check firing first: this used to pass every check in
			// this function and reach validateIssuerURL, where it had no
			// query or fragment to be refused for either — see
			// picatz/flowstate#2039's note on this exact entry.
			name:       "a credential url.Parse reads as a port and a path, under https",
			url:        "https://acct9:2024/s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The cost of the before-first-slash fallback, pinned so that
			// narrowing it later is a decision rather than an accident: this
			// URL carries no credential, and it is redacted anyway, because
			// with no path slash the region searched is the whole remainder
			// by default (see urlWithoutCredentials) — the same fallback the
			// port-misread cases above rely on, minus the port gate this one
			// never trips.
			name:    "a credential-free query holding an at sign is redacted too",
			url:     "http://issuer.example.com?tenant=a@b",
			wantErr: `issuer "http://[redacted]@b" must use https`,
		},
		{
			// No scheme at all, which an unexpanded `${SCHEME}` leaves behind.
			// The leading colon used to stop the slash count before it began.
			name:       "credentials after an empty scheme",
			url:        "://acct9:s3cr3t@issuer.example.com",
			wantErr:    "is not a valid URL",
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The other direction: a well-formed URL whose *path* holds an
			// `@` keeps the strict reading, so the host an operator needs in
			// order to find the entry is still in the sentence. No port here,
			// so nothing could have swallowed a digit-leading password into
			// one — the port gate on the check above is what keeps this
			// accepted rather than refused like the pair below.
			name:    "an at sign in the path of a valid URL is not a credential",
			url:     "http://issuer.example.com/a@b",
			wantErr: `issuer "http://issuer.example.com/a@b" must use https`,
		},
		{
			// The accepted cost of the port gate, pinned as a decision: a
			// real port followed by a path `@` that carries no credential at
			// all is textually identical to the misread above —
			// `scheme://word:digits/…@…` either way — and nothing past the
			// string says which the author meant. picatz/flowstate#2038's own
			// acceptance criteria asked for this exact shape to stay
			// accepted; it cannot, without reopening the same door the
			// reported bug used, so this refuses both rather than guess.
			// `must use https` on this scheme's twin above shows the port
			// alone was never going to save it.
			name:       "a real port and a credential-free path at sign are refused together",
			url:        "https://issuer.example.com:8443/path@thing",
			wantErr:    `issuer "https://[redacted]@thing" must not include credentials`,
			wantAbsent: []string{"path"},
		},
		{
			// An earlier version of this check searched only the first path
			// segment, on the reasoning that a `/` written inside a password
			// splits its remainder into that one segment and no further. It
			// does not: this credential's tail is in the *second* segment
			// (Copilot), one slash past where that version stopped looking,
			// and passed every check with it. The whole path is searched now
			// instead — see the comment on the credentials check.
			name:       "a credential whose tail is a second path segment past the port",
			url:        "https://acct9:2024/s3cr3t/foo@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t", "foo"},
		},
		{
			// The other way a bounded search missed the tail: a password
			// that itself began with the slash that split it leaves an
			// *empty* first path segment (Codex) — `//s3cr3t@host`, not
			// `/s3cr3t@host` — which a first-segment-only search reads as
			// "no segment, nothing to check" and lets straight through.
			name:       "a credential behind a doubled path slash",
			url:        "https://acct9:2024//s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// A colon with nothing after it: url.Parse reads Host as
			// `acct9:` and Port() as "", the empty string being a valid (if
			// useless) port, so testing Port() != "" missed this shape
			// entirely — the authority is exactly as ambiguous as
			// `acct9:2024`, just with no digits after the colon
			// (flowstate-reviewer, urlprobe).
			name:       "a credential behind an empty port",
			url:        "https://acct9:/s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The empty-port shape again, with the credential's tail in the
			// query instead of the path.
			name:       "a credential behind an empty port, in the query",
			url:        "https://acct9:?s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The credential's tail split across a path segment and a query,
			// so neither half alone contains an `@`: `s3` is the path, and
			// `cr3t@host` is the query. A search bounded to only the path
			// misses this the same way one bounded to the query alone would.
			name:       "a credential whose tail crosses from the path into the query",
			url:        "https://acct9:2024/s3?cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t", "cr3t"},
		},
		{
			// The credential's tail in the fragment, past a `#` this time
			// rather than a `?` — the same shape, the third delimiter.
			name:       "a credential whose tail is in the fragment",
			url:        "https://acct9:2024#s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// An IPv6 authority with a real, non-loopback port and the same
			// path-based misread: [hostCarriesPortDelimiter] has to look
			// past the literal's own brackets rather than finding the first
			// ":" in Host, or every bracketed IPv6 host would read as
			// ambiguous regardless of whether a port follows the brackets.
			name:       "a credential behind a port on an IPv6 authority",
			url:        "https://[2001:db8::1]:2024/s3cr3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"2001:db8::1", "s3cr3t"},
		},
		{
			// The negative direction of the same case: a bracketed IPv6
			// authority with no port at all is not ambiguous — there is no
			// "host:port" reading of a bare "[2001:db8::1]" to confuse with
			// a misread userinfo — so a path `@` past it is the ordinary
			// kind, like the plain-hostname case above.
			name: "an IPv6 authority with no port keeps an at sign in its path",
			url:  "https://[2001:db8::1]/a@b",
		},
		{
			// An IPv6 loopback authority with a real port: the loopback
			// exemption applies here exactly as it does for a IPv4 or named
			// loopback host.
			name: "the loopback exemption covers an IPv6 loopback authority too",
			url:  "https://[::1]:2024/s3cr3t@issuer.example.com",
		},
		{
			// The shape both the segment-scoped cases above exist to
			// preserve, and the reason the port gate is not simply "any port
			// and any path `@`": Google's IAM Credentials API builds exactly
			// this path
			// (`/v1/projects/-/serviceAccounts/<email>:generateAccessToken`,
			// see gcpExchanger.impersonate), and a deployment reaching it
			// through a non-default port would otherwise be unable to
			// impersonate a service account at all. Accepted here because
			// there is no port — production's real endpoint,
			// https://iamcredentials.googleapis.com, has none either, so
			// this check never runs for it regardless of path shape.
			name: "Google's real service-account impersonation path, no explicit port",
			url:  "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/name@project.iam.gserviceaccount.com:generateAccessToken",
		},
		{
			// The same path shape *with* an explicit, non-loopback port is
			// refused: nothing past the parsed string distinguishes this
			// from the port-misread cases above, so AGENTS.md invariant 6's
			// fail-closed answer applies here too, deliberately, per
			// picatz/flowstate#2038's review thread. An operator who needs a
			// custom IAM endpoint configures one without a port.
			name:       "the same Google-shaped path with a non-loopback port is refused",
			url:        "https://iam.internal.example.com:8443/v1/projects/-/serviceAccounts/name@project.iam.gserviceaccount.com:generateAccessToken",
			wantErr:    `issuer "https://[redacted]@project.iam.gserviceaccount.com:generateAccessToken" must not include credentials`,
			wantAbsent: []string{"iam.internal.example.com", "v1", "projects", "serviceAccounts", "name"},
		},
		{
			// The loopback exemption, on the same footing as the plain-http
			// loopback exemption already in this table: a target dialed at
			// its own literal address cannot be "the wrong host" this check
			// exists to prevent, and this exact shape — a local relying
			// party standing in for Google's IAM Credentials API, reached
			// through the ephemeral port a test listener binds to — is what
			// TestGCPExchanger and TestGCPExchangerBoundsServiceAccountExpiry
			// need accepted to run at all.
			name: "the Google-shaped path is accepted against a loopback port",
			url:  "https://127.0.0.1:8443/v1/projects/-/serviceAccounts/name@project.iam.gserviceaccount.com:generateAccessToken",
		},
		{
			// What holds isURLScheme's *character set* up, which is the
			// symmetric half of the case above: a scheme may carry digits and
			// `+`, `-`, `.` after its first letter, and accepting only letters
			// would stop `s3://` being read as a scheme at all — so the colon
			// would look like a password's, nothing would be redacted, and the
			// credentials branch would print it. Every other scheme in this
			// table is pure letters, so without this case that clause can be
			// deleted with the suite still green.
			name:       "credentials under a scheme holding a digit",
			url:        "s3://acct9:s3cr3t@issuer.example.com",
			wantErr:    `issuer "s3://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The opaque URL the redaction deliberately leaves alone, so that
			// `mailto:a@b` keeps its meaning. Asserted so the exemption is a
			// decision on the record rather than an oversight; picatz/flowstate#2028
			// holds the judgement about closing it.
			name:    "an opaque URL keeps its at sign",
			url:     "mailto:someone@example.com",
			wantErr: `issuer "mailto:someone@example.com" must name a host`,
		},
		{
			// A second `@` in the authority: the host is what follows the
			// last one, so a cut at the first would leave `r3t@` behind.
			//
			// The whole sentence rather than a fragment of it, and `r3t` in
			// wantAbsent beside the whole password, because neither of those
			// alone can fail: cutting at the first `@` yields
			// `https://[redacted]@r3t@issuer.example.com`, which still
			// contains "must not include credentials" and still contains
			// neither `acct9` nor `s3c@r3t`.
			name:       "credentials holding an at sign",
			url:        "https://acct9:s3c@r3t@issuer.example.com",
			wantErr:    `issuer "https://[redacted]@issuer.example.com" must not include credentials`,
			wantAbsent: []string{"acct9", "s3c@r3t", "r3t"},
		},
		{
			name:    "an unsupported scheme",
			url:     "ftp://issuer.example.com",
			wantErr: "must use https",
		},
		{
			name:    "not a URL at all",
			url:     "://not a url",
			wantErr: "is not a valid URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := auth.ValidateHTTPSURL(tt.url, "issuer")

			if tt.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, parsed)
				return
			}

			require.Error(t, err)
			require.Nil(t, parsed)
			require.ErrorContains(t, err, tt.wantErr)
			// The diagnostic names the field the operator configured, in the
			// caller's own vocabulary, not the function's name.
			require.ErrorContains(t, err, "issuer")

			// And it does not repeat the credential the URL carried. Every
			// refusal here quotes the URL, this one is reached *because* the
			// URL holds a credential, and the caller printing the sentence is
			// `flow auth check`, whose stderr a CI or support transcript
			// keeps (Codex).
			for _, secret := range tt.wantAbsent {
				require.NotContains(t, err.Error(), secret,
					"the refusal repeats the credential it was given")
			}
		})
	}
}
