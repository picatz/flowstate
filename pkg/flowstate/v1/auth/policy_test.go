package auth_test

import (
	"encoding/json"
	"slices"
	"strings"
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

// TestNamespaceMapYAMLRoundTrips is #1949: the weekly deep fuzz tier's
// FuzzNamespaceMap found that a namespace_map key ending in YAML's merge-key
// indicator (`<<`) decodes once but cannot be marshaled back out and decoded
// again, because goccy/go-yaml treats an unquoted key ending in `<<` as the
// merge directive on the way back in regardless of how the source document
// spelled it. `namespace_map:\n  0<<:`, with no trailing newline, is the
// shape the fuzzer minimized to: the decoder's merge-key type check only
// runs once it has a value to examine, and at end of input there is none
// yet, so the key slips through as a literal `"0<<"` — a state the plain
// YAML mapping [NamespaceMap.MarshalYAML] used to write then could not
// reproduce.
//
// A refusal on that one suffix shape was the first fix, and a differential
// run against the decoder found it incomplete: a trailing tab, or a leading
// `? ` (YAML's own explicit-key indicator), silently changes a key across
// this same unquoted round trip with no error to catch it either — nine
// distinct breaks in three million random keys drawn from YAML's own
// punctuation, none of them a `<<` suffix (flowstate-reviewer).
// [NamespaceMap.MarshalYAML] now writes every key as a quoted JSON string
// instead, which the same run found zero breaks for, so this test asserts
// the round trip directly rather than refusing the one shape found first —
// see the comment on [NamespaceMap.MarshalYAML] for why quoting is the fix
// and refusing is not, and for the separate, already-tracked class (#2077)
// that run's alphabet did not reach and this test does not claim to cover.
func TestNamespaceMapYAMLRoundTrips(t *testing.T) {
	tests := []struct {
		name string
		doc  string
	}{
		{
			name: "the exact fuzz-found input: no trailing newline",
			doc:  "0<<:",
		},
		{
			name: "the literal merge-key indicator itself, no trailing newline",
			doc:  "<<:",
		},
		{
			name: "the same key, quoted and with an ordinary value",
			doc:  "\"<<\": ok\n",
		},
		{
			name: "several trailing angle brackets",
			doc:  "prod<<<<:",
		},
		{
			name: "a leading, non-suffix angle-bracket pair",
			doc:  "<<prod: ok\n",
		},
		{
			name: "an ordinary key",
			doc:  "prod: ok\n",
		},
		{
			// The second, wider class the suffix refusal never covered: a
			// trailing tab silently became a truncated key on the old
			// unquoted round trip (flowstate-reviewer).
			name: "a key holding a trailing tab",
			doc:  "\"prod\t\": ok\n",
		},
		{
			// And the other member of that class: YAML's explicit-key
			// indicator, `? `, silently vanished from the front of a key on
			// the old unquoted round trip.
			name: "a key starting with YAML's explicit-key indicator",
			doc:  "\"? prod\": ok\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m auth.NamespaceMap
			require.NoError(t, m.UnmarshalYAML([]byte(tt.doc)))

			// The property FuzzNamespaceMap checks: what decodes must
			// encode, and decode again to the same map.
			encoded, err := m.MarshalYAML()
			require.NoError(t, err)
			var again auth.NamespaceMap
			require.NoError(t, again.UnmarshalYAML(encoded), "the YAML %q encoded to did not decode", encoded)
			require.Equal(t, m, again)
		})
	}
}

// TestNamespaceMapJSONRoundTrips is the JSON half of
// TestNamespaceMapYAMLRoundTrips: [NamespaceMap.MarshalJSON] writes the same
// HTML-unescaped compact JSON [NamespaceMap.MarshalYAML] does, so the same
// keys round-trip through [encoding/json] too.
func TestNamespaceMapJSONRoundTrips(t *testing.T) {
	for _, key := range []string{"0<<", "<<", "prod<<<<", "<<prod", "prod", "prod\t", "? prod"} {
		t.Run(key, func(t *testing.T) {
			m := auth.NamespaceMap{key: "ns"}

			encoded, err := m.MarshalJSON()
			require.NoError(t, err)
			if strings.Contains(key, "<") {
				require.Contains(t, string(encoded), "<",
					"HTML-safe escaping should be off, so a literal angle bracket should survive: %s", encoded)
			}
			require.NotContains(t, string(encoded), `\u00`,
				"HTML-safe escaping should be off: %s", encoded)

			var again auth.NamespaceMap
			require.NoError(t, again.UnmarshalJSON(encoded))
			require.Equal(t, m, again)
		})
	}
}

// TestParsePolicyRoundTripsAMergeKeyShapedNamespaceMapKey is the whole-Policy
// version of TestNamespaceMapYAMLRoundTrips and TestNamespaceMapJSONRoundTrips:
// a namespace_map field does not decode in isolation in production, it decodes
// as one field of a [Policy] [ParsePolicy] loads, so the round trip that
// matters is marshaling a whole policy and loading it back, not marshaling the
// map alone.
func TestParsePolicyRoundTripsAMergeKeyShapedNamespaceMapKey(t *testing.T) {
	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{
			{
				Name:           "idp",
				Issuer:         "https://issuer.example.com",
				Audiences:      []string{"flowstate"},
				NamespaceClaim: "repository",
				NamespaceMap: auth.NamespaceMap{
					"0<<":        "team-a",
					"<<":         "team-b",
					"prod<<<<":   "team-c",
					"has\ttab":   "team-d",
					"? explicit": "team-e",
				},
			},
		},
	}

	t.Run("YAML", func(t *testing.T) {
		encoded, err := yaml.Marshal(policy)
		require.NoError(t, err)

		decoded, err := auth.ParsePolicy(encoded)
		require.NoError(t, err, "the YAML this policy marshaled to did not parse: %s", encoded)
		require.Equal(t, policy.Issuers[0].NamespaceMap, decoded.Issuers[0].NamespaceMap)
	})

	t.Run("JSON", func(t *testing.T) {
		// [NamespaceMap.MarshalJSON]'s own bytes embedded directly in a hand
		// -assembled document, the way an operator's or a tool's JSON policy
		// actually carries one — not [encoding/json.Marshal] on the whole
		// [auth.Policy], which HTML-escapes a nested [json.Marshaler]'s
		// bytes regardless of that Marshaler's own escaping choice (a stdlib
		// property, not a policy loading one: [encoding/json.Marshal]
		// recompacts and re-escapes every nested Marshaler's raw output
		// through its own default-on HTML escaping, which is exactly what
		// [NamespaceMap.MarshalJSON] turns off for itself and cannot turn
		// off for a caller that wraps it this way). No code in this
		// repository marshals a whole Policy back out, so that stdlib
		// interaction is not this test's concern; what a real JSON policy
		// document carries is [NamespaceMap.MarshalJSON]'s own bytes,
		// spliced in as any JSON producer would.
		mapJSON, err := policy.Issuers[0].NamespaceMap.MarshalJSON()
		require.NoError(t, err)

		doc := `{"issuers":[{"name":"idp","issuer":"https://issuer.example.com",` +
			`"audiences":["flowstate"],"namespace_claim":"repository","namespace_map":` +
			string(mapJSON) + `}]}`

		decoded, err := auth.ParsePolicy([]byte(doc))
		require.NoError(t, err, "the JSON namespace_map marshaled to did not parse inside a policy: %s", doc)
		require.Equal(t, policy.Issuers[0].NamespaceMap, decoded.Issuers[0].NamespaceMap)
	})
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
			// strict reading stopped. Under https this
			// function accepts it and validateIssuerURL refuses the query —
			// see TestAuthCheckDoesNotEchoACredentialWrittenIntoAnIssuerURL,
			// which drives that path end to end. Here it is refused a step
			// earlier, for its scheme, on one of the lines this change touches.
			name:       "a credential read as a port under plain http",
			url:        "http://acct9:2024?s3cr3t@issuer.example.com",
			wantErr:    "must use https",
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
			// runs, and the delimiter is encoded, so the strict one found no
			// `@`. Refused a layer up for its query —
			// see the cmd/flow test for that path — and here for its scheme.
			name:       "a percent-encoded delimiter behind a port misread",
			url:        "http://acct9:2024?s3cr3t%40issuer.example.com",
			wantErr:    "must use https",
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
			// The cost of reading past the authority, with a real credential
			// present: the later `@` is the one cut at, so the host goes too.
			// Pinned so that narrowing the region later is a decision.
			name:       "a credential and a later at sign lose the host together",
			url:        "http://acct9:s3cr3t@issuer.example.com?cb=a@b",
			wantErr:    `issuer "http://[redacted]@b" must not include credentials`,
			wantAbsent: []string{"acct9", "s3cr3t"},
		},
		{
			// The residual this deliberately does not close, pinned so it is a
			// decision on the record: the port misread with the rest of the
			// credential in what url.Parse calls the path. It is textually
			// identical to `https://host:8443/path@thing`, an ordinary URL
			// whose host a refusal must keep, so redacting past the slash
			// would erase the host from every one of those. picatz/flowstate#2038
			// holds the repair, which is to stop reading this as host, port
			// and path at all.
			name:    "a credential in what url.Parse calls the path is left alone",
			url:     "http://acct9:2024/s3cr3t@issuer.example.com",
			wantErr: `issuer "http://acct9:2024/s3cr3t@issuer.example.com" must use https`,
		},
		{
			// The cost of the before-first-slash fallback, pinned so that
			// narrowing it later is a decision rather than an accident: this
			// URL carries no credential, and it is redacted anyway, because it
			// is textually the same shape as the port misread above.
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
			// order to find the entry is still in the sentence.
			name:    "an at sign in the path of a valid URL is not a credential",
			url:     "http://issuer.example.com/a@b",
			wantErr: `issuer "http://issuer.example.com/a@b" must use https`,
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
